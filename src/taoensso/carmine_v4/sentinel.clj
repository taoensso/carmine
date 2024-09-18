(ns ^:no-doc taoensso.carmine-v4.sentinel
  "Private ns, implementation detail.
  Implementation of the Redis Sentinel protocol,
  Ref. <https://redis.io/docs/reference/sentinel-clients/>"
  (:require
   [taoensso.encore :as enc :refer [have have?]]
   [taoensso.carmine-v4.utils :as utils]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.resp  :as resp]
   [taoensso.carmine-v4.opts  :as opts])

  (:import [java.util.concurrent.atomic AtomicLong]))

(comment (remove-ns 'taoensso.carmine-v4.sentinel))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*conn-cbs*)

(alias 'core 'taoensso.carmine-v4)

;;;; Dev/test config

(defn- spit-sentinel-test-config
  [{:keys [n-sentinels first-sentinel-port master-name master-addr quorum]
    :or
    {n-sentinels 2
     first-sentinel-port 26379
     master-name "my-master"
     master-addr ["127.0.0.1" 6379]
     quorum n-sentinels}}]

  (dotimes [idx n-sentinels]
    (let [[master-host master-port] master-addr
          sentinel-port (+ ^long first-sentinel-port idx)
          fname (str "sentinel" (inc idx) ".conf")

          content
          (format
            "# Redis Sentinel test config generated by Carmine
# Start Sentinel server with `redis-sentinel %1$s`

port %2$s

# sentinel monitor <master-group-name> <host> <port> <quorum>
sentinel monitor %3$s %4$s %5$s %6$s
sentinel down-after-milliseconds %3$s 60000"

            fname
            sentinel-port
            master-name master-host master-port
            quorum)]

      (spit fname content ))))

(comment (spit-sentinel-test-config {}))

;;;; Node adresses
;; - Node    => Redis master, Redis read replica, or Sentinel server
;; - Address => [<node-host> <node-port>]

(defn- remove-addr [old-addrs addr]
  (let [addr (opts/parse-sock-addr addr)]
    (transduce (remove #(= % addr)) conj [] old-addrs)))

(defn- add-addr->front [old-addrs addr]
  (let [addr (opts/parse-sock-addr addr)]
    (if (= (get old-addrs 0) addr)
      (do                                         old-addrs)
      (transduce (remove #(= % addr)) conj [addr] old-addrs))))

(defn- add-addrs->back [old-addrs addrs]
  (if (empty? addrs)
    old-addrs
    (let [old-addrs   (or  old-addrs [])
          old-addr?   (set old-addrs)]
      (transduce (comp (map opts/parse-sock-addr) (remove old-addr?))
        conj old-addrs addrs))))

(defn- reset-addrs [addrs]
  (transduce (comp (map opts/parse-sock-addr) (distinct))
    conj [] addrs))

;;;; SentinelSpec

(defprotocol ^:private ISentinelSpec
  "Internal protocol, not for public use or extension."
  (sentinel-opts  [spec])
  (update-addrs!  [spec master-name cbs               kind f])
  (resolve-addr!  [spec master-name sentinel-opts use-cache?])
  (resolved-addr? [spec master-name sentinel-opts use-cache? addr]))

(def ^:dynamic *mgr-cbs*
  "Private, implementation detail.
  Mechanism to support `ConnManager` callbacks (cbs)."
  nil)

(enc/defn-cached ^:private unique-addrs {:size 128 :gc-every 100}
  [addrs-state]
  (let [vs (vals addrs-state)]
    {:masters   (into #{}       (map :master)         vs)
     :replicas  (into #{} (comp (map :replicas)  cat) vs)
     :sentinels (into #{} (comp (map :sentinels) cat) vs)}))

(let [kvs->map (fn [x] (if (map? x) x (into {} (comp (partition-all 2)) x)))]
  (defn- parse-nodes-info->addrs [info-seq]
    (when info-seq ; [<node1-info> ...]
      (not-empty
        (reduce
          (fn [acc in] ; Info elements may be map (RESP3) or kvseq (RESP2)
            (let [in (kvs->map in)]
              (enc/if-let [host (get in "host")
                           port (get in "port")]
                (conj acc [host port])
                (do   acc))))
          []
          info-seq)))))

(defn- get-rand [coll] (if (empty? coll) nil (get coll (rand-int (count coll)))))
(defn- members= [c1 c2] (or (= c1 c2) (and (= (count c1) (count c2)) (= (set c1) (set c2)))))

(defn- inc-stat! [stats_ k1 k2] (swap! stats_ (fn [m] (enc/update-in m [k1 k2] (fn [?n] (inc (long (or ?n 0))))))))
(comment (inc-stat! (atom {}) "foo" :k1))

(deftype SentinelSpec
  [sentinel-opts
   addrs-state_    ; Delayed {<master-name>   {:master <addr>, :replicas [<addr>s], :sentinels [<addr>s]}}
   resolve-stats_  ;         {<master-name>   {:keys [n-requests n-attempts n-successes n-errors n-resolved-to-X n-changes-to-X]}
   sentinel-stats_ ;         {<sentinel-addr> {:keys [           n-attempts n-successes n-errors n-ignorant n-unreachable n-misidentified]}
   ]

  Object
  (toString [this]
    ;; "taoensso.carmine.SentinelSpec[masters=3 replicas=4 sentinels=2 0x7b9f6831]"
    (let [{:keys [masters replicas sentinels]} (unique-addrs (force @addrs-state_))]
      (str
        "taoensso.carmine.SentinelSpec["
        "masters="   (count masters)   " "
        "replicas="  (count replicas)  " "
        "sentinels=" (count sentinels) " "
        (enc/ident-hex-str this) "]")))

  clojure.lang.IDeref
  (deref [this]
    (let [addrs-state (force @addrs-state_)]
      {:sentinel-opts sentinel-opts
       :nodes-addrs   addrs-state
       :stats
       (let [{:keys [masters replicas sentinels]} (unique-addrs addrs-state)]
         {:node-counts
          {:masters   (count masters)
           :replicas  (count replicas)
           :sentinels (count sentinels)}

          :resolve-stats  @resolve-stats_
          :sentinel-stats @sentinel-stats_})}))

  ISentinelSpec
  (sentinel-opts [_] sentinel-opts)
  (update-addrs! [this master-name cbs kind f]
    (have? [:el #{:master :replicas :sentinels}] kind)
    (let [master-name (enc/as-qname master-name)
          master?     (identical? kind :master)]

      (if-let [[old-val new-val]
               (let [swap-result_ (volatile! nil)
                     new-state_
                     (swap! addrs-state_
                       (fn  [old-state_]
                         (delay ; Minimize contention during (sometimes expensive) updates
                           (let [old-state (force        old-state_)
                                 old-val   (utils/get-at old-state master-name kind)
                                 new-val   (f old-val)]

                             (if-let [unchanged?
                                      (if master?
                                        (=        old-val new-val)
                                        (members= old-val new-val))]

                               old-state
                               (do
                                 (vreset! swap-result_ [old-val new-val])
                                 (assoc-in old-state [master-name kind] new-val)))))))]

                 @new-state_
                 @swap-result_)]

        (let [cbid
              (case kind
                :master    (do (inc-stat! resolve-stats_ master-name :n-changes-to-master)    :on-changed-master)
                :replicas  (do (inc-stat! resolve-stats_ master-name :n-changes-to-replicas)  :on-changed-replicas)
                :sentinels (do (inc-stat! resolve-stats_ master-name :n-changes-to-sentinels) :on-changed-sentinels))]

          (utils/cb-notify!
            (get core/*conn-cbs* cbid)
            (get       *mgr-cbs* cbid)
            (get            cbs  cbid)
            (delay
              (assoc
                {:cbid          cbid
                 :master-name   master-name
                 :sentinel-spec this
                 :sentinel-opts sentinel-opts
                 :changed       {:old old-val, :new new-val}})))
          true)

        false)))

  (resolve-addr! [this master-name sentinel-opts use-cache?]
    (let [master-name (enc/as-qname      master-name)
          node-addrs  (get @addrs-state_ master-name)
          {:keys [prefer-read-replica?]} sentinel-opts]

      (if use-cache?
        (or
          (when prefer-read-replica? (get-rand (get node-addrs :replicas)))
          (do                                  (get node-addrs :master))))

      (let [t0 (System/currentTimeMillis)
            sentinel-addrs (get node-addrs :sentinels)

            {:keys [conn-opts cbs update-replicas? update-sentinels?]}
            sentinel-opts]

        (if (empty? sentinel-addrs)
          (do
            (inc-stat! resolve-stats_ master-name :n-errors)
            (utils/cb-notify-and-throw! :on-resolve-error
              (get core/*conn-cbs*      :on-resolve-error)
              (get       *mgr-cbs*      :on-resolve-error)
              (get            cbs       :on-resolve-error)
              (ex-info "[Carmine] [Sentinel] No Sentinel server addresses configured for requested master"
                {:eid :carmine.sentinel/no-sentinel-addrs-in-spec
                 :master-name   master-name
                 :sentinel-spec this
                 :sentinel-opts sentinel-opts}
                (Exception. "No Sentinel server addresses in spec"))))

          (let [n-attempts*   (java.util.concurrent.atomic.AtomicLong. 0)
                attempt-log_  (volatile! []) ; [<debug-entry> ...]
                error-counts_ (volatile! {}) ; {<sentinel-addr> {:keys [unreachable ignorant misidentified]}}
                record-error!
                (fn [sentinel-addr t0-attempt error-kind ?data]

                  (inc-stat! sentinel-stats_ sentinel-addr :n-errors)
                  (inc-stat! sentinel-stats_ sentinel-addr
                    (case error-kind
                      :ignorant      :n-ignorant
                      :unreachable   :n-unreachable
                      :misidentified :n-misidentified
                                     :n-other-errors))

                  ;; Add entry to attempt log
                  (let [attempt-ms (- (System/currentTimeMillis) ^long t0-attempt)]
                    (vswap! attempt-log_ conj
                      (assoc
                        (conj
                          {:attempt       (.get n-attempts*)
                           :sentinel-addr sentinel-addr
                           :error         error-kind}
                          ?data)
                        :attempt-ms attempt-ms)))

                  ;; Increment counter for error kind
                  (vswap! error-counts_
                    (fn [m]
                      (enc/update-in m [sentinel-addr error-kind]
                        (fn [?n] (inc (long (or ?n 0))))))))

                ;; Node addrs reported during resolution
                reported-sentinel-addrs_ (volatile! #{})
                reported-replica-addrs_  (volatile! #{})

                complete-resolve!
                (fn
                  ([error]
                   (inc-stat! resolve-stats_ master-name :n-errors)

                   (when-let [addrs @reported-sentinel-addrs_]
                     (update-addrs! this master-name cbs :sentinels
                       (fn [old] (add-addrs->back old addrs))))

                   (utils/cb-notify-and-throw! :on-resolve-error
                     (get core/*conn-cbs*      :on-resolve-error)
                     (get       *mgr-cbs*      :on-resolve-error)
                     (get            cbs       :on-resolve-error)
                     error))

                  ([reporting-sentinel-addr resolved-addr confirmed-role]
                   (let [reporting-sentinel-addr (opts/parse-sock-addr reporting-sentinel-addr)
                         resolved-addr           (opts/parse-sock-addr resolved-addr)]

                     (when-let [addrs @reported-replica-addrs_]
                       (update-addrs! this master-name cbs :replicas
                         (fn [old] (reset-addrs addrs))))

                     (when-let [addrs @reported-sentinel-addrs_]
                       (update-addrs! this master-name cbs :sentinels
                         (fn [old] (add-addrs->back old addrs))))

                     (inc-stat! sentinel-stats_ reporting-sentinel-addr :n-successes)
                     (inc-stat! resolve-stats_  master-name             :n-successes)
                     (inc-stat! resolve-stats_  master-name
                       (case confirmed-role
                         :master  :n-resolved-to-master
                         :replica :n-resolved-to-replica))

                     (utils/cb-notify!
                       (get core/*conn-cbs* :on-resolve-success)
                       (get       *mgr-cbs* :on-resolve-success)
                       (get            cbs  :on-resolve-success)
                       (delay
                         {:cbid          :on-resolve-success
                          :master-name   master-name
                          :resolved-to   {:addr resolved-addr :role confirmed-role}
                          :sentinel-spec this
                          :sentinel-opts sentinel-opts
                          :ms-elapsed (- (System/currentTimeMillis) t0)}))

                     (when (identical? confirmed-role :master)
                       (update-addrs! this master-name cbs :master
                         (fn [_old] resolved-addr)))

                     resolved-addr)))]

            (loop [n-retries 0]
              (let [t0-attempt (System/currentTimeMillis)
                    [?reporting-sentinel-addr ?reported-master-addr] ; ?[<addr> <addr>]
                    (reduce
                      ;; Try each known sentinel addr, sequentially
                      (fn [acc sentinel-addr]
                        (.incrementAndGet n-attempts*)
                        (inc-stat! resolve-stats_  master-name   :n-attempts)
                        (inc-stat! sentinel-stats_ sentinel-addr :n-attempts)
                        (let [[host port] sentinel-addr
                              [?master-addr ?replicas-info ?sentinels-info]
                              (case host ; Simulated errors for tests
                                "unreachable"   [::unreachable                 nil nil]
                                "misidentified" [["simulated-misidentified" 0] nil nil]
                                "ignorant"      nil
                                (try
                                  (conns/with-new-conn conn-opts host port master-name
                                    (fn [_ in out]
                                      (resp/with-replies in out :natural-replies :as-vec
                                        (fn []
                                          ;; Always ask about master (may be used as fallback when no replicas)
                                          (resp/rcall "SENTINEL" "get-master-addr-by-name" master-name)

                                          (if (or prefer-read-replica? update-replicas?)
                                            ;; Ask about replica nodes
                                            (resp/rcall "SENTINEL" "replicas" master-name)
                                            (resp/local-echo nil))

                                          (when update-sentinels?
                                            ;; Ask about sentinel nodes
                                            (resp/rcall "SENTINEL" "sentinels" master-name))))))

                                  (catch Throwable _
                                    [::unreachable nil nil])))]

                          (when-let [addrs (parse-nodes-info->addrs  ?replicas-info)] (vreset! reported-replica-addrs_       addrs))
                          (when-let [addrs (parse-nodes-info->addrs ?sentinels-info)] (vswap!  reported-sentinel-addrs_ into addrs))

                          (enc/cond
                            (vector?    ?master-addr)                        (reduced [sentinel-addr (opts/parse-sock-addr ?master-addr)])
                            (nil?       ?master-addr)               (do (record-error! sentinel-addr t0-attempt :ignorant    nil) acc)
                            (identical? ?master-addr ::unreachable) (do (record-error! sentinel-addr t0-attempt :unreachable nil) acc))))

                      nil sentinel-addrs)]

                (if-let [[reporting-sentinel-addr resolved-addr confirmed-role]
                         (enc/when-let [sentinel-addr ?reporting-sentinel-addr
                                        master-addr   ?reported-master-addr]

                           (let [[target-addr expected-role]
                                 (or
                                   (when prefer-read-replica?
                                     (when-let [replica-addr (get-rand @reported-replica-addrs_)]
                                       [replica-addr :replica]))

                                   [master-addr :master])

                                 actual-role
                                 (let [[host port] target-addr
                                       reply
                                       (try
                                         (conns/with-new-conn conn-opts host port master-name
                                           (fn [_ in out]
                                             (resp/with-replies in out :natural-replies false
                                               (fn [] (resp/rcall "ROLE")))))
                                         (catch Throwable _ nil))]

                                   (when (vector? reply) (get reply 0)))]

                             ;; Confirm that node and sentinel agree on node's role
                             (if (= actual-role (name expected-role))
                               [sentinel-addr target-addr expected-role]
                               (do
                                 (record-error! sentinel-addr t0-attempt :misidentified
                                   {:resolved-to
                                    {:addr target-addr
                                     :role {:expected        expected-role
                                            :actual (keyword actual-role)}}})
                                 nil))))]

                  (complete-resolve! reporting-sentinel-addr resolved-addr confirmed-role)

                  (let [{:keys [resolve-timeout-ms retry-delay-ms]} sentinel-opts
                        elapsed-ms  (- (System/currentTimeMillis) t0)
                        retry-at-ms (+ elapsed-ms (long (or retry-delay-ms 0)))]

                    (if (> retry-at-ms (long (or resolve-timeout-ms 0)))
                      (do
                        (vswap! attempt-log_ conj
                          [:timeout
                           (str
                             "(" elapsed-ms " elapsed + " retry-delay-ms " delay = " retry-at-ms
                             ") > " resolve-timeout-ms " timeout")])

                        (complete-resolve!
                          (ex-info "[Carmine] [Sentinel] Timed out while trying to resolve requested master"
                            {:eid :carmine.sentinel/resolve-timeout
                             :master-name     master-name
                             :sentinel-spec   this
                             :sentinel-opts   sentinel-opts
                             :sentinel-errors @error-counts_
                             :n-attempts      (.get n-attempts*)
                             :n-retries       n-retries
                             :ms-elapsed      (- (System/currentTimeMillis) t0)
                             :attempt-log     @attempt-log_})))
                      (do
                        (vswap! attempt-log_ conj [:retry-after-sleep retry-delay-ms])
                        (Thread/sleep (int retry-delay-ms))
                        (recur (inc n-retries)))))))))))))

  (resolved-addr? [this master-name sentinel-opts use-cache? addr]
    (when-not use-cache? ; Update cache
      (resolve-addr! this master-name sentinel-opts false))

    (let [addr        (opts/parse-sock-addr addr)
          master-name (enc/as-qname      master-name)
          node-addrs  (get @addrs-state_ master-name)]
      (or
        (when (= addr (get node-addrs :master)) :master)
        (when (and (get sentinel-opts :prefer-read-replica?)
                (enc/rfirst #(= % addr) (get node-addrs :replicas)))
          :replica)))))

(enc/def-print-impl [ss SentinelSpec] (str "#" ss))

(defn ^:public sentinel-spec?
  "Returns true iff given argument is a Carmine `SentinelSpec`."
  [x] (instance? SentinelSpec x))

(defn ^:public sentinel-spec
  "Given a Redis Sentinel server addresses map of form
    {<master-name> [[<sentinel-server-host> <sentinel-server-port>] ...]},
  returns a new stateful `SentinelSpec` for use in `conn-opts`.

    (def my-sentinel-spec
      \"Stateful Redis Sentinel server spec. Will be kept
       automatically updated by Carmine.\"
      (sentinel-spec
        {:caching       [[\"192.158.1.38\" 26379] ...]
         :message-queue [[\"192.158.1.38\" 26379] ...]}))
      => stateful `SentinelSpec`

  For options docs, see `*default-sentinel-opts*` docstring.
  See also `get-env` for a util to load `sentinel-addrs-map`
  from environmental config."
  ([sentinel-addrs-map              ] (sentinel-spec sentinel-addrs-map nil))
  ([sentinel-addrs-map sentinel-opts]
   (let [addrs-state
         (reduce-kv
           (fn [m master-name addrs]
             (assoc m (enc/as-qname master-name)
               {:sentinels (reset-addrs addrs)}))
           {} (have map? sentinel-addrs-map))]

     (SentinelSpec.
       (have [:or nil? map?] sentinel-opts)
       (atom addrs-state)
       (atom {})
       (atom {})))))

(comment
  (resolve-addr!
    ;; Use host e/o #{"unreachable" "ignorant" "misidentified"} to simulate errors
    (sentinel-spec {"my-master" [[#_"ignorant" #_"misidentified" "127.0.0.1" 26379]]})
    "my-master" {} (not :use-cache))

  (conns/with-new-conn {} "127.0.0.1" 26379 #_6379 nil
    (fn [_ in out]
      (resp/with-replies in out false false
        (fn []
          (resp/rcall "ROLE")
          #_(resp/rcall "SENTINEL" "get-master-addr-by-name" "my-master")
          #_(resp/rcall "SENTINEL" "replicas"                "my-master")
          #_(core/rcall "SENTINEL" "sentinels"               "my-master"))))))
