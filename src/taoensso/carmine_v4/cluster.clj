(ns ^:no-doc taoensso.carmine-v4.cluster
  "Private Redis Cluster protocol implementation. See
  <https://redis.io/docs/reference/cluster-spec/>."
  (:require
   [clojure.string :as str]
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.carmine-v4.utils :as utils]
   [taoensso.carmine-v4.opts  :as opts]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.resp  :as resp]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write]
   )

  (:import
   [java.nio.charset StandardCharsets]
   [java.util Arrays]
   [java.util Locale]
   [java.util.concurrent ExecutionException]
   [java.util.concurrent.atomic AtomicInteger]
   [taoensso.carmine_v4.classes RawRedisArg]))

(comment (remove-ns 'taoensso.carmine-v4.cluster))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*conn-cbs*)

(alias 'core 'taoensso.carmine-v4)

(def ^:private ^:const num-key-slots 16384)
(def ^:private ^:dynamic *refreshing-specs* #{})
(def ^:private ^:dynamic *mgr-conn-opts*
  "Connection options of the manager that triggered a topology refresh."
  nil)

;;;; ClusterSpec

(declare ^:private refresh-topology* public-topology)

(defprotocol ^:private IClusterSpec
  "Internal protocol, not for public use or extension."
  (cluster-spec-opts [spec])
  (record-exec-stats! [spec counts])
  (mark-topology-stale! [spec])
  (cached-topology [spec]
   "Returns the current complete topology cache, or nil before discovery or
   while the cache is not trusted (e.g. after a node transport failure or a
   MOVED or CLUSTERDOWN reply that exhausted the flush retry budget).")
  (cached-topology+ts [spec]
   "Returns `[topology updated-at-ms]` from one state snapshot, or nil. See
   `cached-topology`.")
  (refresh-topology! [spec]
   "Refreshes and returns the complete topology synchronously.

   Concurrent calls on one spec join one active sweep. The constructor fixes the
   discovery policy. After a refresh failure, calls `:on-refresh-error`. It then
   returns the stale last-known complete topology, if available; otherwise, it
   throws `:carmine.cluster/refresh-error`.

   A refresh callback must not transitively refresh the same spec. Such a
   reentrant call throws `:carmine.cluster/reentrant-refresh`."))

(deftype ClusterSpec [spec-opts seed-addrs state_ refresh_]
  Object
  (toString [this]
    (let [{:keys [known-addrs topology]} @state_]
      (enc/str-impl this "taoensso.carmine.ClusterSpec"
        {:n-seeds       (count seed-addrs)
         :n-known-addrs (count known-addrs)
         :topology?     (boolean topology)})))

  clojure.lang.IDeref
  (deref [_]
    (let [{:keys [known-addrs topology topology-stale? last-success-addr updated-at stats]} @state_]
      {:cluster-spec-opts (utils/redact-secrets spec-opts)
       :seed-addrs   seed-addrs
       :known-addrs  known-addrs
       :topology     (public-topology topology updated-at)
       :topology-stale? (boolean topology-stale?)
       :last-success-addr last-success-addr
       :stats        stats}))

  IClusterSpec
  (cluster-spec-opts [_] spec-opts)
  (record-exec-stats! [_ counts]
    (swap! state_
      (fn [state]
        (reduce-kv
          (fn [state k n]
            (update-in state [:stats :execution-stats k]
              (fn [?n] (+ (long (or ?n 0)) (long n)))))
          state counts))))
  (mark-topology-stale! [_]
    ;; A dead/unreachable node cannot reply MOVED, so transport failures
    ;; must distrust the cached topology to un-pin its slots. The next
    ;; flush then refreshes (single-flight, stale-serve fallback).
    (swap! state_ (fn [state] (if (:topology state) (assoc state :topology-stale? true) state)))
    nil)
  (cached-topology [_]
    (let [state @state_]
      (when-not (:topology-stale? state) (:topology state))))

  (cached-topology+ts [_]
    ;; Single state read so callers can pair a topology with the
    ;; `:updated-at` of exactly the refresh that produced it
    (let [state @state_]
      (when-not (:topology-stale? state)
        (when-let [topology (:topology state)]
          [topology (:updated-at state)]))))
  (refresh-topology! [this]
    (refresh-topology* this spec-opts seed-addrs state_ refresh_)))

(enc/def-print-impl [cs ClusterSpec] (str "#" cs))

(defn ^:public cluster-spec?
  "Returns true iff the given `x` is a Carmine `ClusterSpec`."
  [x] (instance? ClusterSpec x))

(defn ^:public cluster-spec
  "Returns a new stateful `ClusterSpec` for the given `[host port]` addresses
  of one or more cluster nodes:

    (def my-spec (cluster-spec [[\"127.0.0.1\" 7000] [\"127.0.0.1\" 7001]]))

  The spec fetches and keeps track of the cluster's topology. Share one spec
  between all the managers that use the same cluster.

  Topology is fetched with the auth and TLS options of the manager in use,
  so managers that share a spec should have the same TLS setting.

  Deref the spec for diagnostics, incl. `:topology` (same shape as
  `cluster-cached-topology`), `:topology-stale?`, and `:stats`.

  See `default-cluster-spec-opts` for spec options, and `conn-manager` for
  use."
  ([seed-addrs             ] (cluster-spec seed-addrs nil))
  ([seed-addrs cluster-spec-opts]
   (try
     (truss/have? sequential? seed-addrs)
     (truss/have? [:or nil? map?] cluster-spec-opts)
     (let [seed-addrs (into [] (comp (map opts/parse-sock-addr) (distinct)) seed-addrs)
           spec-opts (opts/parse-cluster-spec-opts cluster-spec-opts)]
       (when-not (seq seed-addrs)
         (truss/ex-info! "[Carmine] At least one Redis Cluster seed address is required"
           {:eid :carmine.cluster/no-seed-addrs}))
       (ClusterSpec. spec-opts seed-addrs
         (atom
           {:known-addrs seed-addrs
            :topology    nil
            :updated-at  nil
            :stats
            {:refresh-stats {}
             :node-stats    {}}})
         (enc/latom nil)))
     (catch Throwable t
       (truss/ex-info! (opts/invalid-opts-msg "[Carmine] Invalid Redis Cluster specification" t)
         {:eid        :carmine.cluster/invalid-spec
          :seed-addrs (utils/redact-secrets (enc/typed-val seed-addrs))
          :cluster-spec-opts (utils/redact-secrets (enc/typed-val cluster-spec-opts))}
         (opts/sanitized-opts-cause t))))))

;;;; Topology replies

(defn- invalid-topology! [problem data]
  (truss/ex-info! "[Carmine] Invalid Redis Cluster topology reply"
    (assoc data
      :eid     :carmine.cluster/invalid-topology
      :problem problem)))

(defn- reply-map [kind x]
  (cond
    (map? x) x
    (and (sequential? x) (even? (count x)))
    (into {} (map vec) (partition 2 x))

    :else
    (invalid-topology! :expected-map
      {:kind kind, :value (enc/typed-val x)})))

(defn- reply-field [m k]
  (if (contains? m k)
    (get m k)
    (get m (name k))))

(defn- normalize-attrs [kind x]
  (persistent!
    (reduce-kv
      (fn [m k v]
        (assoc! m (if (string? k) (keyword k) k) v))
      (transient {})
      (reply-map kind x))))

(defn- valid-port? [x]
  (and (integer? x) (<= 1 x 65535)))

(defn- normalize-node
  [source-host role raw-attrs]
  (let [attrs (normalize-attrs :node raw-attrs)
        port     (reply-field attrs :port)
        tls-port (reply-field attrs :tls-port)
        _valid-port (when (and (some? port) (not (valid-port? port)))
            (invalid-topology! :invalid-node-port
              {:node attrs, :port port}))
        _valid-tls-port (when (and (some? tls-port) (not (valid-port? tls-port)))
            (invalid-topology! :invalid-node-port
              {:node attrs, :tls-port tls-port}))
        _some-port (when-not (or (some? port) (some? tls-port))
            (invalid-topology! :missing-node-port {:node attrs}))

        endpoint (reply-field attrs :endpoint)
        host
        (cond
          (= endpoint "?") nil
          (or (nil? endpoint) (= endpoint "")) source-host
          (string? endpoint) endpoint
          :else
          (invalid-topology! :invalid-node-endpoint
            {:node attrs, :endpoint endpoint}))]

    {:id                 (reply-field attrs :id)
     :addr               (when (and host port)     [host port])
     :tls-addr           (when (and host tls-port) [host tls-port])
     :endpoint           endpoint
     :ip                 (reply-field attrs :ip)
     :hostname           (reply-field attrs :hostname)
     :port               port
     :tls-port           tls-port
     :role               role
     :attrs              attrs}))

(defn- normalize-slot-range [lo hi]
  (if (and (integer? lo) (integer? hi)
        (<= 0 lo hi (dec num-key-slots)))
    [lo hi]
    (invalid-topology! :invalid-slot-range
      {:slot-range [lo hi]})))

(defn- node-routable? [{:keys [addr tls-addr]}]
  (boolean (or addr tls-addr)))

(defn- missing-slot-ranges [slot-ranges]
  (loop [expected 0, ranges (seq (keys slot-ranges)), missing []]
    (if-let [[[lo hi] & more] ranges]
      (recur (inc ^long hi) more
        (if (< expected ^long lo)
          (conj missing [expected (dec ^long lo)])
          missing))
      (if (< expected num-key-slots)
        (conj missing [expected (dec num-key-slots)])
        missing))))

(defn- topology [shard-ranges]
  (let [entries
        (sort-by (fn [[[lo _hi] _shard]] lo)
          (mapcat
            (fn [{:keys [slots] :as shard}]
              (map (fn [slot-range] [slot-range (dissoc shard :slots)]) slots))
            shard-ranges))]

    (loop [previous-hi nil, entries entries, slot-ranges (sorted-map)]
      (if-let [[[lo hi :as slot-range] shard] (first entries)]
        (if (and previous-hi (<= ^long lo ^long previous-hi))
          (invalid-topology! :overlapping-slot-ranges
            {:previous-slot-end previous-hi, :slot-range slot-range})
          (recur hi (next entries) (assoc slot-ranges slot-range shard)))

        (let [missing     (missing-slot-ranges slot-ranges)
              unroutable (into []
                           (keep
                             (fn [[slot-range {:keys [master]}]]
                               (when-not (node-routable? master) slot-range)))
                           slot-ranges)
              coverage-complete? (empty? missing)
              routable?          (and (not (empty? slot-ranges)) (empty? unroutable))]
          {:shards                  shard-ranges
           :slot-ranges             slot-ranges
           :n-slots                 (reduce-kv
                                      (fn [n [lo hi] _]
                                        (+ n (inc (- ^long hi ^long lo))))
                                      0 slot-ranges)
           :missing-slot-ranges     missing
           :unroutable-slot-ranges  unroutable
           :slot-coverage-complete? coverage-complete?
           :routable?               routable?
           :complete?               (and coverage-complete? routable?)})))))

(defn- slots-node [source-host role tls? raw-node]
  (when-not (and (sequential? raw-node) (<= 2 (count raw-node)))
    (invalid-topology! :invalid-node
      {:node (enc/typed-val raw-node)}))

  (let [[endpoint port id ?metadata] raw-node
        metadata (if (nil? ?metadata) {} (reply-map :node-metadata ?metadata))]
    (normalize-node source-host role
      (cond-> (assoc metadata :endpoint endpoint, :id id)
        tls?       (assoc :tls-port port)
        (not tls?) (assoc :port port)))))

(defn parse-cluster-slots
  "Normalizes a `CLUSTER SLOTS` reply into Carmine's Cluster topology model.

  `source-addr` identifies the node that returned the reply. Its host is used
  when Redis reports an empty or nil preferred endpoint. The model keeps
  missing slot ranges and unknown endpoints as explicit incomplete topology.
  Malformed or overlapping ranges throw `:carmine.cluster/invalid-topology`.

  Topology `:routable?` is independent of the transport. It means that each
  present range has a master with an advertised address. `CLUSTER SLOTS`
  supplies one preferred endpoint for the discovery transport. The three-arity
  form records it as `:tls-addr` when `tls?` is truthy, otherwise as `:addr`."
  ([reply] (parse-cluster-slots nil false reply))
  ([source-addr reply] (parse-cluster-slots source-addr false reply))
  ([source-addr tls? reply]
   (let [source-host (first source-addr)
         _ (when-not (sequential? reply)
             (invalid-topology! :expected-array
               {:value (enc/typed-val reply)}))
         ranges
         (mapv
           (fn [raw-range]
             (when-not (and (sequential? raw-range) (<= 3 (count raw-range)))
               (invalid-topology! :invalid-shard
                 {:shard (enc/typed-val raw-range)}))

             (let [[lo hi raw-master & raw-replicas] raw-range
                   master   (slots-node source-host :master tls? raw-master)
                   replicas (mapv #(slots-node source-host :replica tls? %) raw-replicas)]
               {:slots    [(normalize-slot-range lo hi)]
                :master   master
                :replicas replicas
                :nodes    (into [master] replicas)}))
           reply)]
     (topology ranges))))

;;;; Topology discovery/cache

(defn- raw-topology-reply!
  "Issues `CLUSTER SLOTS` to one address. Private test seam."
  [conn-opts addr]
  (let [[host port] addr]
    (conns/with-new-conn conn-opts host port nil
      (fn [_ in out]
        (first
          (resp/with-replies in out
            {:natural-replies? true, :as-vec? true, :error-mode :return} nil
            #(resp/rcmd* ["CLUSTER" "SLOTS"])))))))

(defn- incomplete-topology-error [addr topology]
  (truss/ex-info "[Carmine] Redis Cluster topology is incomplete"
    {:eid         :carmine.cluster/incomplete-topology
     :source-addr addr
     :n-slots     (:n-slots topology)
     :missing-slot-ranges    (:missing-slot-ranges topology)
     :unroutable-slot-ranges (:unroutable-slot-ranges topology)}))

(defn- refresh-timeout-error [addr]
  (truss/ex-info "[Carmine] Redis Cluster topology refresh deadline exhausted"
    {:eid :carmine.cluster/refresh-timeout
     :source-addr addr}))

(defn- attempt-candidate [conn-opts addr deadline-nanos]
  (try
    (let [attempt-conn-opts
          (or (utils/conn-opts-before-deadline conn-opts deadline-nanos)
            (throw (refresh-timeout-error addr)))
          reply
          (utils/call-before-deadline deadline-nanos
            #(raw-topology-reply! attempt-conn-opts addr))
          _ (when (identical? reply utils/deadline-exhausted)
              (throw (refresh-timeout-error addr)))
          _ (when (com/reply-error? reply) (throw reply))
          tls? (boolean (utils/get-at conn-opts :socket-opts :ssl))
          ;; NB `:refresh-timeout-ms` bounds discovery I/O: a fully-received
          ;; topology is deliberately kept even when the deadline expires
          ;; during parsing
          topology (parse-cluster-slots addr tls? reply)]

      (if (:complete? topology)
        {:attempt {:addr addr, :outcome :success}
         :topology topology}
        (throw (incomplete-topology-error addr topology))))

    (catch Throwable t
      (when (instance? InterruptedException t)
        (.interrupt (Thread/currentThread))
        (throw t))
      {:attempt
       {:addr    addr
        :outcome
        (case (:eid (ex-data t))
          :carmine.cluster/refresh-timeout     :timeout
          :carmine.cluster/invalid-topology    :invalid-topology
          :carmine.cluster/incomplete-topology :incomplete-topology
          :error)
        :error t}
       :timed-out? (= (:eid (ex-data t)) :carmine.cluster/refresh-timeout)})))

(defn- node-route-addr [tls? node]
  (if tls?
    (:tls-addr node)
    (:addr node)))

(defn- topology-known-addrs [topology tls? seed-addrs]
  (let [addr-key (if tls? :tls-addr :addr)]
    (into [] (distinct)
      (concat
        (keep addr-key (mapcat :nodes (:shards topology)))
        seed-addrs))))

(defn- topology-routing-view [topology]
  (mapv
    (fn [[slot-range {:keys [master replicas]}]]
      [slot-range
       (select-keys master [:id :addr :tls-addr :role])
       (mapv #(select-keys % [:id :addr :tls-addr :role]) replicas)])
    (:slot-ranges topology)))

(defn- add-count [m path n]
  (update-in m path (fn [?n] (+ (long (or ?n 0)) (long n)))))

(defn- record-sweep-stats [state attempts success?]
  (let [state
        (-> state
          (add-count [:stats :refresh-stats :n-attempts] (count attempts))
          (add-count [:stats :refresh-stats
                      (if success? :n-successes :n-errors)] 1))]
    (reduce
      (fn [state {:keys [addr outcome]}]
        (-> state
          (add-count [:stats :node-stats addr :n-attempts] 1)
          (add-count [:stats :node-stats addr
                      (if (= outcome :success) :n-successes :n-errors)] 1)))
      state attempts)))

(defn- refresh-success!
  [spec cluster-spec-opts seed-addrs state_ t0 attempts topology]
  (let [tls?        (boolean (get-in cluster-spec-opts [:conn-opts :socket-opts :ssl]))
        known-addrs (topology-known-addrs topology tls? seed-addrs)
        source-addr (get-in attempts [(dec (count attempts)) :addr])
        [old-state new-state]
        (swap-vals! state_
          (fn [state]
            (-> state
              (record-sweep-stats attempts true)
              (dissoc :timed-out-addrs)
              (assoc
                :topology topology
                :topology-stale? false
                :known-addrs known-addrs
                :last-success-addr source-addr
                :updated-at (System/currentTimeMillis)))))
        old-topology (:topology old-state)
        changed?     (not= (some-> old-topology topology-routing-view)
                       (topology-routing-view topology))
        cbs          (:cbs cluster-spec-opts)
        elapsed-ms   (- (System/currentTimeMillis) t0)]

    (utils/cb-notify!
      (get core/*conn-cbs* :on-refresh-success)
      (get cbs              :on-refresh-success)
      (delay
        {:cbid          :on-refresh-success
         :via           'refresh-topology!
         :cluster-spec  spec
         :cluster-spec-opts (utils/redact-secrets cluster-spec-opts)
         :source-addr   source-addr
         :topology      (public-topology topology (:updated-at new-state))
         :n-attempts    (count attempts)
         :elapsed-ms    elapsed-ms}))

    (when changed?
      (utils/cb-notify!
        (get core/*conn-cbs* :on-changed-topology)
        (get cbs              :on-changed-topology)
        (delay
          {:cbid          :on-changed-topology
           :via           'refresh-topology!
           :cluster-spec  spec
           :cluster-spec-opts (utils/redact-secrets cluster-spec-opts)
           :source-addr   source-addr
           :changed       {:old (public-topology old-topology (:updated-at old-state))
                           :new (public-topology     topology (:updated-at new-state))}
           :elapsed-ms    elapsed-ms})))
    topology))

(defn- refresh-failed!
  [spec cluster-spec-opts state_ t0 attempts timed-out?]
  (let [elapsed-ms (- (System/currentTimeMillis) t0)
        error
        (truss/ex-info "[Carmine] Failed to refresh Redis Cluster topology"
          {:eid          :carmine.cluster/refresh-error
           :via          'refresh-topology!
           :cluster-spec spec
           :cluster-spec-opts (utils/redact-secrets cluster-spec-opts)
           :attempts     attempts
           :n-attempts   (count attempts)
           :timed-out?   (boolean timed-out?)
           :stale-served? false
           :elapsed-ms   elapsed-ms}
          (enc/rsome :error attempts))
        ?timed-out-addr ; Addr that exhausted the sweep's deadline
        (when timed-out? (:addr (peek attempts)))

        state
        (swap! state_
          (fn [state]
            (cond-> (record-sweep-stats state attempts false)
              ?timed-out-addr
              (update :timed-out-addrs ; Most recent last
                #(conj (into [] (remove #{?timed-out-addr}) %) ?timed-out-addr)))))
        cached (:topology state)
        cbs    (:cbs cluster-spec-opts)]

    (if cached
      (do
        (utils/cb-notify!
          (get core/*conn-cbs* :on-refresh-error)
          (get cbs              :on-refresh-error)
          (delay
            (assoc (ex-data error)
              :cbid :on-refresh-error
              :stale-served? true
              :topology (public-topology cached (:updated-at state)))))
        cached)
      (utils/cb-notify-and-throw! :on-refresh-error
        (get core/*conn-cbs* :on-refresh-error)
        (get cbs              :on-refresh-error)
        error))))

(defn- refresh-sweep!
  [spec cluster-spec-opts seed-addrs state_]
  (let [t0         (System/currentTimeMillis)
        deadline-nanos
        (utils/timeout-deadline-nanos (:refresh-timeout-ms cluster-spec-opts))
        conn-opts  (utils/with-mgr-node-opts (:conn-opts cluster-spec-opts) *mgr-conn-opts*)
        cluster-spec-opts (assoc cluster-spec-opts :conn-opts conn-opts) ; For TLS addrs, etc.
        state      @state_
        candidates (into [] (distinct)
                     (concat
                       (when-let [addr (:last-success-addr state)] [addr])
                       (:known-addrs state)
                       seed-addrs))
        candidates ; Addrs that exhausted a recent sweep's deadline go last
        (if-let [timed-out (not-empty (:timed-out-addrs state))]
          (into (filterv (complement (set timed-out)) candidates)
            (filter (set candidates)) timed-out)
          candidates)]
    (loop [candidates candidates, attempts []]
      (if-not (utils/remaining-timeout-ms deadline-nanos)
        (refresh-failed! spec cluster-spec-opts state_ t0 attempts true)
        (if-let [addr (first candidates)]
          (let [{:keys [attempt topology timed-out?]}
                (attempt-candidate conn-opts addr deadline-nanos)
                attempts (conj attempts attempt)]
            (cond
              topology
              (refresh-success! spec cluster-spec-opts seed-addrs state_ t0
                attempts topology)

              timed-out?
              (refresh-failed! spec cluster-spec-opts state_ t0 attempts true)

              :else
              (recur (next candidates) attempts)))
          (refresh-failed! spec cluster-spec-opts state_ t0 attempts false))))))

(defn- refresh-topology*
  [spec cluster-spec-opts seed-addrs state_ refresh_]
  (when (contains? *refreshing-specs* spec)
    (truss/ex-info! "[Carmine] Reentrant Redis Cluster topology refresh"
      {:eid          :carmine.cluster/reentrant-refresh
       :via          'refresh-topology!
       :cluster-spec spec}))
  (loop []
    (if-let [in-flight @refresh_]
      (force in-flight)
      (let [sweep-fn
            (bound-fn []
              (binding [resp/*ctx* nil
                        *refreshing-specs* (conj *refreshing-specs* spec)]
                (refresh-sweep! spec cluster-spec-opts seed-addrs state_)))
            in-flight (delay (sweep-fn))]
        (if (compare-and-set! refresh_ nil in-flight)
          (try
            (force in-flight)
            (finally (compare-and-set! refresh_ in-flight nil)))
          (recur))))))

;;;; Public topology projection

(defn- have-cluster-spec [via spec]
  (when-not (cluster-spec? spec)
    (truss/ex-info! "[Carmine] Expected a Redis Cluster `ClusterSpec`"
      {:eid :carmine.cluster/invalid-spec
       :via via
       :cluster-spec (enc/typed-val spec)}))
  spec)

(defn- public-node [node]
  (when node
    {:node-id  (:id       node)
     :addr     (:addr     node)
     :tls-addr (:tls-addr node)}))

(defn- public-topology
  "Returns the stable public projection of an internal topology map. See
  `cluster-cached-topology` for its contract."
  [topology updated-at]
  (when topology
    {:shards ; One per master, `CLUSTER SLOTS` gives one row per slot range
     (let [rows     (sort-by #(first (:slots %)) (:shards topology))
           grouped (group-by :master rows)]
       (mapv
         (fn [master]
           (let [rows (get grouped master)]
             {:slot-ranges (into [] (mapcat :slots) rows)
              :master      (public-node master)
              :replicas    (mapv public-node (:replicas (first rows)))}))
         (distinct (map :master rows))))
     :updated-at-ms updated-at}))

(defn ^:public cluster-cached-topology
  "Returns the last known topology of `cluster-spec`, or nil if none is
  currently known, with one shard per master. For diagnostics, and for
  targeting specific nodes:

    {:shards [{:slot-ranges [[0 5460]]
               :master   {:node-id \"…\" :addr [\"10.0.1.1\" 7000] :tls-addr nil}
               :replicas [{…}]}]
     :updated-at-ms 1731400000000}

  E.g. to send a command to every master (once the manager has made its
  first request or `cluster-refresh-topology!` call, the topology is nil
  before then):

    (wcar mgr :as-vec
      (doseq [shard (:shards (cluster-cached-topology my-spec))]
        (with-cluster-target {:node-id (get-in shard [:master :node-id])}
          (dbsize))))

  See also `cluster-refresh-topology!`, `with-cluster-target`."
  [cluster-spec]
  (have-cluster-spec 'cluster-cached-topology cluster-spec)
  (when-let [[topology updated-at] (cached-topology+ts cluster-spec)]
    (public-topology topology updated-at)))

(defn ^:public cluster-refresh-topology!
  "Fetches the current topology of Cluster connection manager `mgr`, and
  returns it as `cluster-cached-topology` does. Rarely needed since managers
  refresh automatically, but useful to have a topology before the manager's
  first request.

  If the fetch fails, returns the last known topology or throws if there's
  none."
  [mgr]
  (let [conn-opts (when (conns/conn-manager? mgr) (conns/mgr-conn-opts mgr))
        spec      (get (opts/get-cluster-server conn-opts) :cluster-spec)]
    (when-not spec
      (truss/ex-info! "[Carmine] Expected a Redis Cluster connection manager"
        {:eid :carmine.cluster/invalid-manager
         :via 'cluster-refresh-topology!
         :mgr (enc/typed-val mgr)}))
    (binding [*mgr-conn-opts* conn-opts] ; For the manager's auth and TLS options
      (refresh-topology! spec))
    (:topology @spec)))

;;;; Routing foundation

(declare slot->shard ^:private no-route! ba->key-slot)

(def ^:private ^:dynamic *cluster-target* nil)

(defn- invalid-target!
  ([problem target] (invalid-target! problem target nil))
  ([problem target hint]
   (truss/ex-info! "[Carmine] Invalid Redis Cluster target"
     (enc/assoc-some
       {:eid :carmine.cluster/invalid-target
        :problem problem
        :target (enc/typed-val target)}
       :hint hint))))

(defn- normalize-cluster-target [target]
  (cond
    (= target :any) {:kind :any}

    (contains? #{:masters :nodes} target)
    (invalid-target! :broadcast-not-supported target
      "To run a command on several nodes, loop over the nodes from `cluster-cached-topology` and target each with `{:node-id id}`.")

    (map? target)
    (do
      (when-not (= (count target) 1)
        (invalid-target! :expected-one-selector target))
      (let [[selector value] (first target)]
        (case selector
          :key
          {:kind :slot, :slot (ba->key-slot
                                (write/arg-payload-bytes
                                  (write/prepare-arg value)))}

          :slot
          (if (and (integer? value) (<= 0 value (dec num-key-slots)))
            {:kind :slot, :slot (long value)}
            (invalid-target! :invalid-slot target))

          :addr
          {:kind :addr, :addr (opts/parse-sock-addr value)}

          :node-id
          (if (or (string? value) (keyword? value))
            {:kind :node-id, :node-id (enc/as-qname value)}
            (invalid-target! :invalid-node-id target))

          (invalid-target! :unknown-selector target))))

    :else
    (invalid-target! :unexpected-target target)))

(defn ^:no-doc with-cluster-target*
  "Internal implementation for the public `taoensso.carmine-v4/with-cluster-target` macro."
  [target body-fn]
  (binding [*cluster-target* (normalize-cluster-target target)]
    (body-fn)))

(defn ^:no-doc without-request-context*
  "Calls `body-fn` without any request-scoped bindings (e.g. from a `wcar`
  body), for threads that mustn't affect their creating thread's requests."
  [body-fn]
  (binding [resp/*ctx*              nil
            write/*conn-reusable?_ nil
            com/*tx-effects?*       false
            com/*natural-replies?*  false
            com/*read-mode*         nil
            com/*parser*            nil
            *cluster-target*        nil]
    (body-fn)))

(defn ^:no-doc request-target
  "Captures and validates one request's Cluster target at enqueue time."
  [inferred-slot cluster-routing]
  (let [explicit *cluster-target*
        inferred (when (some? inferred-slot)
                   {:kind :slot, :slot (long inferred-slot)})]
    (cond
      (= cluster-routing :unsupported) nil

      (and inferred explicit)
      (if (and (= (:kind explicit) :slot)
            (= (:slot explicit) inferred-slot))
        inferred
        (truss/ex-info! "[Carmine] Explicit Cluster target conflicts with inferred command keys"
          {:eid :carmine.cluster/target-conflict
           :inferred-target inferred
           :explicit-target explicit}))

      inferred inferred
      explicit explicit
      (= cluster-routing :single-node) {:kind :any}

      :else
      (truss/ex-info! "[Carmine] Redis Cluster command requires an explicit target"
        {:eid :carmine.cluster/target-required
         :cluster-routing cluster-routing
         :hint "Use with-cluster-target or wrap a routing key with cluster-key."}))))

(defn- topology-nodes [topology]
  (into []
    (comp
      (mapcat :nodes)
      (distinct))
    (:shards topology)))

(defn- topology-masters [topology]
  (into [] (comp (keep :master) (distinct)) (:shards topology)))

(defn- target-addr
  "Returns the `[host port]` address for a normalized Cluster target, or throws
  `:carmine.cluster/no-route`."
  [topology tls? target]
  (let [transport (if tls? :tls :plain)
        node-addr
        (fn [node]
          (or (node-route-addr tls? node)
            (no-route! :transport-unavailable
              {:target target, :transport transport, :node-id (:id node)})))]
    (case (:kind target)
      :slot
      (let [slot (:slot target)]
        (if-let [shard (slot->shard topology slot)]
          (node-addr (:master shard))
          (no-route! :unmapped-slot {:slot slot, :target target})))

      :addr
      (let [addr (:addr target)]
        (if (some #(= (node-route-addr tls? %) addr) (topology-nodes topology))
          addr
          (no-route! :unknown-address {:target target, :transport transport})))

      :node-id
      (if-let [node (some #(when (= (str (:id %)) (:node-id target)) %)
                      (topology-nodes topology))]
        (node-addr node)
        (no-route! :unknown-node-id {:target target}))

      :any
      (or (some #(node-route-addr tls? %) (topology-masters topology))
        (no-route! :transport-unavailable {:target target, :transport transport}))

      (invalid-target! :unexpected-normalized-target target))))

(defn slot->shard
  "Returns the normalized shard covering `slot`, or nil when the topology does
  not cover it. Returns a covered but unroutable shard unchanged, so callers can
  distinguish missing coverage from temporarily unusable topology.

  Throws for a slot outside Redis Cluster's inclusive [0, 16383] range."
  [topology slot]
  (when-not (and (integer? slot) (<= 0 slot (dec num-key-slots)))
    (truss/ex-info! "[Carmine] Invalid Redis Cluster slot"
      {:eid :carmine.cluster/invalid-slot
       :slot (enc/typed-val slot)}))

  (reduce-kv
    (fn [_ [lo hi] shard]
      (when (<= ^long lo ^long slot ^long hi) (reduced shard)))
    nil (:slot-ranges topology)))

(defn- no-route! [problem data]
  (truss/ex-info! "[Carmine] No Redis Cluster route available"
    (assoc data :eid :carmine.cluster/no-route, :problem problem)))

;;;; Execution

(declare parse-cluster-error)

(defn- write-request! [out ^taoensso.carmine_v4.resp.Req req]
  (write/write-command out (.-args req)
    (.-encoded-prefix req) (.-n-prefix-args req)))

(defn- read-cluster-reply! [read-opts push-fn input-state]
  (let [reply (read/read-reply-from-state (com/with-push-fn read-opts push-fn) input-state)]
    (if (com/reply-error? {:eid :carmine.read/unexpected-read-error} reply)
      (throw reply) ; Invalidate the borrowed connection while still in scope
      reply)))

(defn- borrow-addr!
  "Internal execution seam for deterministic Cluster tests."
  [mgr addr f]
  (conns/mgr-borrow-addr! mgr addr f))

(defn- refresh-for-execution!
  "Refreshes topology using the manager's auth and TLS options."
  [mgr spec]
  (binding [*mgr-conn-opts* (conns/mgr-conn-opts mgr)]
    (refresh-topology! spec)))

(defn- tasks->partitions
  "Groups tasks by address, preserving first-seen address order and task order."
  [tasks]
  (let [partitions (java.util.ArrayList.)] ; [addr entries] pairs
    (reduce
      (fn [addr->entries task]
        (let [addr (get task :addr)]
          (if-let [^java.util.ArrayList entries (get addr->entries addr)]
            (do (.add entries task) addr->entries)
            (let [entries (doto (java.util.ArrayList.) (.add task))]
              (.add partitions [addr entries])
              (assoc addr->entries addr entries)))))
      {} tasks)
    (mapv (fn [[addr entries]] {:addr addr, :entries (vec entries)}) partitions)))

(defn- execute-partition!
  [mgr {:keys [addr entries]}]
  (borrow-addr! mgr addr
    (fn [_conn in out]
      (let [push-fn     (conns/mgr-push-fn mgr addr)
            input-state (com/input-state in)] ; Once per partition
        (doseq [{:keys [request asking?]} entries]
          (when asking?
            (write/write-array-len out 1)
            (write/write-bulk-arg "ASKING" out))
          (write-request! out request))
        (.flush ^java.io.BufferedOutputStream out)
        (mapv
          (fn [{:keys [request asking?] :as entry}]
            (let [asking-reply (when asking?
                                 (read-cluster-reply! com/read-opts-natural push-fn input-state))
                  reply        (read-cluster-reply!
                                 (.-read-opts ^taoensso.carmine_v4.resp.Req request) push-fn input-state)]
              (assoc entry :reply
                (if (and asking? (com/reply-error? asking-reply))
                  asking-reply
                  reply))))
          entries)))))

(defn- throw-if-interrupted!
  []
  (when (Thread/interrupted)
    (let [t (InterruptedException. "[Carmine] Cluster execution interrupted")]
      (.interrupt (Thread/currentThread))
      (throw t))))

(defn- await-workers!
  "Waits for all workers to settle and returns unexpected worker errors. If the
  caller is interrupted, stops new work and waits for active workers. Then it
  restores the interrupt status and rethrows."
  [workers stop?_]
  (let [interrupted_ (volatile! nil)
        remember-interrupt!
        (fn [t]
          (reset! stop?_ true)
          (when-not @interrupted_
            (vreset! interrupted_ t)))
        sample-interrupt!
        (fn []
          (when (Thread/interrupted)
            (remember-interrupt!
              (InterruptedException.
                "[Carmine] Cluster execution interrupted"))))
        worker-errors
        (mapv
          (fn [worker]
            (loop []
              ;; A completed `Future.get` need not inspect a pending interrupt.
              (sample-interrupt!)
              (let [[kind value]
                    (try
                      [:ok @worker]
                      (catch InterruptedException t [:interrupted t])
                      (catch ExecutionException t [:error (or (.getCause t) t)])
                      (catch Throwable t [:error t]))]
                (case kind
                  :ok
                  (do (sample-interrupt!) nil)

                  :error
                  (do
                    (reset! stop?_ true)
                    (sample-interrupt!)
                    value)

                  :interrupted
                  (do
                    ;; Do not abandon workers with unread Redis replies.
                    (remember-interrupt! value)
                    (recur))))))
          workers)
        ;; Final sample closes the last join/throw race.
        _ (sample-interrupt!)]
    (when-let [t @interrupted_]
      (.interrupt (Thread/currentThread))
      (throw t))
    worker-errors))

(defn- execute-round!
  [mgr tasks max-concurrent-nodes]
  (throw-if-interrupted!)
  (let [partitions (tasks->partitions tasks)
        n-partitions (count partitions)
        n-workers (int (min max-concurrent-nodes n-partitions))]
    (if (<= n-workers 1)
      (let [completed
            (try
              (reduce
                (fn [completed partition]
                  (throw-if-interrupted!)
                  (into completed (execute-partition! mgr partition)))
                [] partitions)
              (catch InterruptedException t
                (.interrupt (Thread/currentThread))
                (throw t)))]
        (throw-if-interrupted!)
        completed)

      ;; `future` conveys dynamic bindings. Each worker owns at most one
      ;; borrowed node connection at a time; the keyed pool remains the global
      ;; connection-capacity/backpressure authority across concurrent callers.
      (let [next-idx*   (AtomicInteger. 0)
            stop?_     (atom false)
            results    (object-array n-partitions)
            worker-fn
            (fn []
              (try
                (loop []
                  (when-not @stop?_
                    (let [idx (.getAndIncrement next-idx*)]
                      (when (and (< idx n-partitions) (not @stop?_))
                        (try
                          (aset results idx
                            {:value (execute-partition! mgr (nth partitions idx))})
                          (catch Throwable t
                            (aset results idx {:error t})
                            ;; Best effort: siblings already past their last stop
                            ;; check may still start additional partitions.
                            (reset! stop?_ true)))
                        (recur)))))
                (catch Throwable t
                  ;; Defense for failures outside the guarded partition body.
                  (reset! stop?_ true)
                  (throw t))))
            workers (mapv (fn [_] (future (worker-fn))) (range n-workers))
            ;; Defense in depth for failures outside `execute-partition!`'s
            ;; guarded body (the ordinary partition error path uses `results`).
            unexpected-worker-errors (await-workers! workers stop?_)]
        (when-let [t (or (some :error results)
                       (first (remove nil? unexpected-worker-errors)))]
          ;; A parser can be interrupted on a worker rather than this caller.
          ;; Match serial execution when propagating that interruption.
          (when (instance? InterruptedException t)
            (.interrupt (Thread/currentThread)))
          (throw t))
        (into [] (mapcat :value) results)))))

(defn- manager-tls? [mgr]
  (boolean (get-in (conns/mgr-conn-opts mgr) [:socket-opts :ssl])))

(defn- current-task-addr [mgr spec request]
  (let [topology (or (cached-topology spec) (refresh-for-execution! mgr spec))
        target   (.-cluster-target ^taoensso.carmine_v4.resp.Req request)]
    (target-addr topology (manager-tls? mgr) target)))

(defn- retry-task-addr
  "Returns the current address for a retried slot-targeted task. Returns nil for
  any `:carmine.cluster/no-route` failure, such as a temporarily unmapped slot
  during resharding. An isolated routing gap must not abort the flush and
  discard completed replies. When this function returns nil, callers keep the
  previous address and ASKING state for the retry."
  [mgr spec request]
  (try
    (current-task-addr mgr spec request)
    (catch Throwable t
      (if (= (:eid (ex-data t)) :carmine.cluster/no-route)
        nil
        (throw t)))))

(defn- parsed-cluster-error [addr reply]
  (try
    (parse-cluster-error addr reply)
    (catch Throwable t
      (if (= (:eid (ex-data t)) :carmine.cluster/invalid-redirection)
        nil ; Preserve malformed Redis reply as this request's final result
        (throw t)))))

(defn- transport-error?
  "Returns true iff the cause chain of `t` shows a connection or transport
  failure (e.g. an unreachable or hung node). Returns false for interruption,
  manager closure, pool exhaustion, and application errors."
  [t]
  (loop [t t, n 0]
    (cond
      (nil? t)                             false
      (>= n 64)                            false ; Cycle guard
      (instance? java.io.IOException t)    true
      (instance? InterruptedException t)   false
      :else
      (let [cause (.getCause ^Throwable t)]
        (recur (if (identical? cause t) nil cause) (inc n))))))

(defn- execute-with-retries!
  [mgr spec cluster-opts initial-tasks]
  (let [max-rounds (long (:max-retry-rounds cluster-opts))
        backoff-ms (long (:retry-backoff-ms cluster-opts))
        max-concurrent-nodes (:max-concurrent-nodes cluster-opts)
        counts_    (volatile! {})
        add-stat!  (fn [k] (vswap! counts_ update k (fnil inc 0)))]
    (try
      (loop [round 0, tasks initial-tasks, final {}]
        (let [completed
              (try
                (execute-round! mgr tasks max-concurrent-nodes)
                (catch Throwable t
                  ;; A dead node can't reply MOVED: distrust the cached topology
                  ;; so the next flush re-discovers. NB commands that may have
                  ;; executed are NOT auto-replayed; the error propagates.
                  (when (transport-error? t) (mark-topology-stale! spec))
                  (throw t)))
              retry?   (< round max-rounds)
              observed ; [completion parsed-error] pairs, usually none
              (into []
                (keep
                  (fn [{:keys [addr reply] :as completion}]
                    (when-let [parsed (parsed-cluster-error addr reply)]
                      (when (contains? #{:moved :ask :try-again :cluster-down} (:kind parsed))
                        [completion parsed]))))
                completed)
              retryable
              (when retry? observed)
              dirty? (some (fn [[_ parsed]]
                             (contains? #{:moved :cluster-down} (:kind parsed)))
                       observed)
              delayed? (some (fn [[_ parsed]]
                               (contains? #{:try-again :cluster-down} (:kind parsed)))
                         retryable)
              _
              (doseq [[_ parsed] observed]
                (add-stat!
                  (case (:kind parsed)
                    :moved        :n-moved
                    :ask          :n-ask
                    :try-again    :n-try-again
                    :cluster-down :n-cluster-down)))
              _
              (when dirty?
                (if retry?
                  (refresh-for-execution! mgr spec)
                  ;; The reply is final for this flush, but the observed topology
                  ;; change must unpin later requests from the same stale route.
                  (mark-topology-stale! spec)))
              retry-indexes (into #{} (map (comp :index first)) retryable)
              final
              (persistent!
                (reduce
                  (fn [final {:keys [index reply]}]
                    (if (contains? retry-indexes index)
                      final
                      (assoc! final index reply)))
                  (transient final) completed))
              next-tasks
              (mapv
                (fn [[{:keys [addr request] :as completion} parsed]]
                  (case (:kind parsed)
                    :moved (assoc completion :addr (:addr parsed), :asking? false)
                    :ask   (assoc completion :addr (:addr parsed), :asking? true)

                    ;; Retain the attempted route. In particular, an ASK-routed
                    ;; retry must stay on its importing node and send ASKING
                    ;; again (the flag applies to one command only).
                    :try-again completion

                    :cluster-down
                    (if (= (:kind (.-cluster-target ^taoensso.carmine_v4.resp.Req request))
                          :slot)
                      (if-let [new-addr (retry-task-addr mgr spec request)]
                        (assoc completion :addr new-addr, :asking? false)
                        ;; Transient no-route: retry at the previous address,
                        ;; retaining ASKING state (an ASK-routed retry must
                        ;; re-ASK its importing node):
                        completion)
                      (assoc completion :addr addr, :asking? false))))
                retryable)]
          (if (seq next-tasks)
            (do
              (add-stat! :n-retry-rounds)
              (when (and delayed? (pos? backoff-ms))
                (try
                  (Thread/sleep backoff-ms)
                  (catch InterruptedException t
                    (.interrupt (Thread/currentThread))
                    (throw t))))
              (recur (inc round) next-tasks final))
          final)))
      ;; Incl. counts from a flush that threw in a later round
      (finally (when (seq @counts_) (record-exec-stats! spec @counts_))))))

(defn execute-flush!
  "Consumes pending Cluster requests and appends replies in request order. Does
  not replay transport failures. Internal."
  [{:keys [mgr cluster-server]} ^java.util.LinkedList pending-reqs*
   ^java.util.LinkedList pending-replies*]
  (let [n (.size pending-reqs*)]
    (when (pos? n)
      (let [{:keys [cluster-spec cluster-opts]} cluster-server
            reqs
            (loop [idx 0, acc (transient [])]
              (if (< idx n)
                (recur (inc idx) (conj! acc (.removeFirst pending-reqs*)))
                (persistent! acc)))

            network-req? #(instance? taoensso.carmine_v4.resp.Req %)
            unsupported
            (into []
              (keep-indexed
                (fn [idx req]
                  (when (and (network-req? req)
                          (false? (.-supports-cluster? ^taoensso.carmine_v4.resp.Req req)))
                    idx)))
              reqs)
            _ (when (seq unsupported)
                (truss/ex-info! "[Carmine] Redis command does not support Cluster"
                  {:eid     :carmine.cluster/command-not-supported
                   :indexes unsupported}))

            tasks ; One per network request, in request order
            (when (some network-req? reqs)
              (let [topology (or (cached-topology cluster-spec)
                               (refresh-for-execution! mgr cluster-spec))
                    tls?     (manager-tls? mgr)
                    addrs_   (volatile! {}) ; Resolve each distinct target once
                    target->addr
                    (fn [target]
                      (or (get @addrs_ target)
                        (let [addr (target-addr topology tls? target)]
                          (vswap! addrs_ assoc target addr)
                          addr)))]
                (into []
                  (keep-indexed
                    (fn [idx req]
                      (when (network-req? req)
                        {:index idx, :request req, :asking? false
                         :addr (target->addr
                                 (.-cluster-target ^taoensso.carmine_v4.resp.Req req))})))
                  reqs)))

            network-replies
            (if (seq tasks)
              (execute-with-retries! mgr cluster-spec cluster-opts tasks)
              {})
            missing (Object.)]

        (dotimes [idx n]
          (let [req (nth reqs idx)
                reply
                (if (network-req? req)
                  (let [reply (get network-replies idx missing)]
                    (if (identical? reply missing)
                      (truss/ex-info! "[Carmine] Redis Cluster replies do not match requests"
                        {:eid :carmine.cluster/reply-mismatch, :index idx})
                      reply))
                  (let [^taoensso.carmine_v4.resp.LocalEchoReq req req]
                    (read/complete-reply (.-read-opts req) (.-reply req))))]
            (when-not (identical? reply com/sentinel-skipped-reply)
              (.add pending-replies* reply))))
        n))))

(defn- invalid-redirection! [problem source-addr error]
  (let [{:keys [code message]} (ex-data error)]
    (truss/ex-info! "[Carmine] Invalid Redis Cluster redirection"
      {:eid         :carmine.cluster/invalid-redirection
       :problem     problem
       :code        code
       :message     message
       :source-addr source-addr}
      error)))

(defn- parse-uint [s]
  (when (and (string? s) (re-matches #"\d+" s))
    (try
      (Long/parseLong s)
      (catch NumberFormatException _ nil))))

(defn- parse-redirection-endpoint [source-addr endpoint error]
  (let [[host port-str]
        (if (str/starts-with? endpoint "[")
          (if-let [[_ host port] (re-matches #"^\[([^\]]+)\]:(.*)$" endpoint)]
            [host port]
            (invalid-redirection! :invalid-endpoint source-addr error))
          (let [colon-idx (.lastIndexOf ^String endpoint ":")]
            (if (neg? colon-idx)
              (invalid-redirection! :invalid-endpoint source-addr error)
              [(subs endpoint 0 colon-idx) (subs endpoint (inc colon-idx))])))
        port (parse-uint port-str)
        host (if (= host "") (first source-addr) host)]
    (when-not (and port (valid-port? port))
      (invalid-redirection! :invalid-port source-addr error))
    {:host host
     :port port
     :addr (when host [host port])}))

(defn parse-cluster-error
  "Normalizes a recognized Redis Cluster reply error; otherwise returns nil.

  MOVED requests one redirect and a full topology refresh. ASK requests one
  redirect after ASKING and does not change the topology cache. TRYAGAIN permits
  a bounded delayed retry. CLUSTERDOWN permits a bounded refresh and retry.
  CROSSSLOT is permanent. This function only classifies the reply. The executor
  controls retry budgets.

  An empty redirect host uses the source host. Without `source-addr`, it is
  still valid but returns nil `:host` and `:addr`. A malformed recognized MOVED
  or ASK reply throws `:carmine.cluster/invalid-redirection`. The executor
  isolates this failure to that reply."
  ([error] (parse-cluster-error nil error))
  ([source-addr error]
   (when (com/reply-error? {:eid :carmine.read/redis-error-reply} error)
     ;; Nb unwrap any attributed reply so that classification always sees
     ;; the underlying error's data (attributed redirects included):
     (let [{:keys [code message]} (ex-data (com/reply-content error))
           base {:message message, :error error}]
       (case code
         ("MOVED" "ASK")
         (let [parts (when (string? message) (str/split message #" " -1))]
           (when-not (and (== (count parts) 3) (= (first parts) code))
             (invalid-redirection! :invalid-message source-addr error))
           (let [[_ slot-str endpoint] parts
                 slot (parse-uint slot-str)]
             (when-not (and slot (< slot num-key-slots))
               (invalid-redirection! :invalid-slot source-addr error))
             (merge base
               {:kind       (if (= code "MOVED") :moved :ask)
                :transient? (= code "ASK")
                :slot       slot}
               (parse-redirection-endpoint source-addr endpoint error))))

         "TRYAGAIN"    (assoc base :kind :try-again,   :transient? true)
         "CLUSTERDOWN" (assoc base :kind :cluster-down, :transient? true)
         "CROSSSLOT"   (assoc base :kind :cross-slot,   :transient? false)
         nil)))))

;;;; Key slots
(let [xmodem-crc16-lookup
      (long-array
        [0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
         0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
         0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
         0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
         0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
         0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
         0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
         0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
         0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
         0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
         0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
         0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
         0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
         0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
         0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
         0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
         0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
         0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
         0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
         0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
         0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
         0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
         0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
         0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
         0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
         0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
         0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
         0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
         0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
         0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
         0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
         0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0])]

  (defn- crc16
    "Returns the Redis Cluster CRC16 hash for the given bytes. See Appendix A of
    <https://redis.io/docs/reference/cluster-spec/>.

    Thanks to @bpoweski for this implementation."
    ([^bytes ba] (crc16 ba 0 (alength ba)))
    ([^bytes ba offset len]
     (let [end (+ (long offset) (long len))]
      (loop [n   (long offset)
             crc 0] ; Inlines faster than `enc/reduce-n`
        (if (>= n end)
          crc
          (recur (unchecked-inc n)
            (bit-xor (bit-and (bit-shift-left crc 8) 0xffff)
              (aget xmodem-crc16-lookup
                (-> (bit-shift-right crc 8)
                  (bit-xor (aget ba n))
                  (bit-and 0xff)))))))))))

(defn- ba->key-slot
  "Returns the Redis Cluster slot for the exact key bytes. Applies the Cluster hash-tag rules."
  [^bytes ba]
  (let [len (alength ba)
        open-idx
        (loop [idx 0]
          (when (< idx len)
            (if (== (aget ba idx) (byte (int \{)))
              idx
              (recur (unchecked-inc idx)))))

        close-idx
        (when open-idx
          (loop [idx (unchecked-inc ^long open-idx)]
            (when (< idx len)
              (if (== (aget ba idx) (byte (int \})))
                idx
                (recur (unchecked-inc idx))))))]

    (if (and close-idx (> ^long close-idx (unchecked-inc ^long open-idx)))
      (mod (crc16 ba (unchecked-inc ^long open-idx)
             (- ^long close-idx (unchecked-inc ^long open-idx)))
        num-key-slots)
      (mod (crc16 ba) num-key-slots))))

(defprotocol IClusterKey
  (^:public cluster-key [redis-key]
    "Marks a string or byte array as a Redis key, so that Cluster can route
    an `rcmd` request:
      (wcar mgr (rcmd :GET (cluster-key \"k\")))

    A byte array key is sent exactly as is, as with `bytes`."))
(deftype      ClusterKey [^bytes ba ^long slot]
  RawRedisArg          (redisBytes  [_] ba)
  clojure.lang.IDeref (deref       [this] slot) ; For tests
  IClusterKey         (cluster-key [this] this))

(extend-type (Class/forName "[B")
  IClusterKey
  (cluster-key [ba]
    (let [ba-copy (Arrays/copyOf ^bytes ba (alength ^bytes ba))]
      (ClusterKey. ba-copy (ba->key-slot ba-copy)))))

(extend-type String
  IClusterKey
  (cluster-key [s]
    (let [s-ba (enc/str->utf8-ba s)]
      (ClusterKey. s-ba (ba->key-slot s-ba)))))

(defn- ?cluster-key-slot
  "Returns the Cluster slot for a `cluster-key` value, or nil for other values."
  [x]
  (let [x (write/prepared-logical-value x)]
    (when (instance? ClusterKey x) (.-slot ^ClusterKey x))))

(defn ^:public cluster-slot
  "Returns the Redis Cluster slot (0 to 16383) that the given key is routed
  to: a string, byte array, or `cluster-key`. Keys with the same hash tag
  share a slot:
    (= (cluster-slot \"{user1}:a\") (cluster-slot \"{user1}:b\")) => true"
  [redis-key]
  (or (?cluster-key-slot redis-key)
    (cond
      (string?    redis-key) (ba->key-slot (enc/str->utf8-ba redis-key))
      ;; As routed, i.e. incl. any byte-array marker (see `*auto-freeze?*`)
      (enc/bytes? redis-key) (ba->key-slot (write/arg-payload-bytes (write/prepare-arg redis-key)))
      :else
      (truss/ex-info! "[Carmine] Unexpected Redis Cluster key type"
        {:eid :carmine.cluster/invalid-key
         :via 'cluster-slot
         :redis-key (enc/typed-val redis-key)}))))

(defn- redis-arg-slot [x]
  (or (?cluster-key-slot x)
      (ba->key-slot (write/arg-payload-bytes x))))

(defn- command-token [x]
  (-> (String. ^bytes (write/arg-payload-bytes x) StandardCharsets/US_ASCII)
    (.toUpperCase Locale/ROOT)))

(defn- empty-redis-arg? [x]
  (zero? (alength ^bytes (write/arg-payload-bytes x))))

(defn- migrate-key-indexes [args]
  ;; MIGRATE supports either one key at index 3, or an empty placeholder plus
  ;; a final `KEYS key [key ...]` clause. Parse the valid option grammar rather
  ;; than using Redis's incomplete backward-keyword spec, which can mistake an
  ;; AUTH password equal to "KEYS" for the clause token.
  (if-not (and (< 3 (count args)) (empty-redis-arg? (get args 3)))
    (when (< 3 (count args)) [3])
    (let [argc (count args)]
      (loop [idx 6]
        (when (< idx argc)
          (case (command-token (get args idx))
            ("COPY" "REPLACE") (recur (inc idx))
            "AUTH"  (recur (+ idx 2))
            "AUTH2" (recur (+ idx 3))
            "KEYS"  (range (inc idx) argc)
            nil))))))

(defn- parse-command-count [x]
  (try
    (let [n (cond
              (integer? x) (long x)
              (string?  x) (Long/parseLong x)
              :else nil)]
      (when (and n (not (neg? n))) n))
    (catch NumberFormatException _ nil)))

(defn- key-spec-indexes [args key-spec]
  (let [argc (count args)
        begin-search (:begin_search key-spec)
        begin-type   (:type begin-search)
        begin-spec   (:spec begin-search)
        begin
        (case begin-type
          "index" (get begin-spec :index)
          "keyword"
          (let [target (str/upper-case (get begin-spec :keyword))
                start0 (long (get begin-spec :startfrom 0))
                backward? (neg? start0)
                start  (if backward? (max 0 (+ argc start0)) start0)
                indexes (if backward? (range start 0 -1) (range start argc))]
            (some
              (fn [idx]
                (when (= (command-token (get args idx)) target) (inc idx)))
              indexes))
          nil)
        find-keys (:find_keys key-spec)]
    (when (and (integer? begin) (<= 0 begin) (< begin argc))
      (case (:type find-keys)
        "range"
        (let [{:keys [lastkey keystep limit]} (:spec find-keys)
              step (long keystep)
              ;; A positive `limit` stops the search a factor into the span of
              ;; remaining arguments: `begin + (argc - begin)/limit + lastkey`,
              ;; matching Redis (which uses it only with `lastkey` -1).
              last (if (and (neg? ^long lastkey) (pos? ^long limit))
                     (+ begin (quot (- argc begin) (long limit)) (long lastkey))
                     (if (neg? ^long lastkey)
                       (+ argc (long lastkey))
                       (+ begin (long lastkey))))]
          (when (and (pos? step) (<= begin last) (< last argc))
            (range begin (inc last) step)))

        "keynum"
        (let [{:keys [keynumidx firstkey keystep]} (:spec find-keys)
              n (parse-command-count (get args (+ begin (long keynumidx))))
              first-idx (+ begin (long firstkey))
              step (long keystep)]
          (when (and n (pos? step)
                  (<= (+ first-idx (* (max 0 (dec n)) step)) (dec argc)))
            (take n (iterate #(+ ^long % step) first-idx))))
        nil))))

(defn- single-key-slot
  "Returns the slot of the key at `idx`, or nil if `command-slot` must take
  its general path: no arg at `idx`, or another arg is a `cluster-key`."
  [args ^long idx]
  (let [argc (count args)]
    (when (< idx argc)
      (loop [i 0]
        (if (< i argc)
          (when (or (== i idx) (nil? (?cluster-key-slot (nth args i))))
            (recur (inc i)))
          (redis-arg-slot (nth args idx)))))))

(declare ^:private general-command-slot)

(defn ^:no-doc command-slot
  "Returns the single Cluster slot identified by Redis command key specs and
  explicit `cluster-key` arguments. Throws before I/O if known keys use
  different slots.

  A route map may include `:single-key-idx` for the common case of key specs
  that always identify exactly one key, at that fixed arg index."
  [args route]
  (or
    (when-let [idx (when (map? route) (get route :single-key-idx))]
      (single-key-slot args idx))
    (general-command-slot args route)))

(defn- general-command-slot [args route]
  (let [{:keys [kind key-specs]}
        (if (map? route) route {:kind nil, :key-specs route})
        indexes
        (case kind
          :migrate (migrate-key-indexes args)
          (when (seq key-specs)
            (distinct (mapcat #(or (key-spec-indexes args %) []) key-specs))))
        route-slots (keep #(redis-arg-slot (get args %)) indexes)
        explicit-slots (keep ?cluster-key-slot args)
        slots (vec (distinct (concat route-slots explicit-slots)))]
    (when (> (count slots) 1)
      (truss/ex-info! "[Carmine] Redis Cluster command keys span multiple slots"
        {:eid :carmine.cluster/cross-slot-keys
         :slots slots
         :command (first args)}))
    (first slots)))

(comment
  (enc/qb 1e5 ; [7.59 22.92]
    (cluster-key        "foo")
    (cluster-key "ignore{foo}")))
