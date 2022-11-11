(ns taoensso.carmine.impl.sentinel
  "Implementation of the Redis Sentinel protocol,
  Ref. https://redis.io/docs/reference/sentinel-clients/

  A set of Sentinel servers (usu. >= 3) basically provides a
  quorum mechanism to resolve the current master Redis server
  for a given \"master name\" (service name):

    (fn resolve-redis-master
      [master-name stateful-sentinel-addresses]) -> <redis-master-address>

  Requests to the service then go through an indirection step:
    request -> resolve-redis-master -> redis-master"

  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [have have? throws?]]))

(comment
  (remove-ns      'taoensso.carmine.impl.sentinel)
  (test/run-tests 'taoensso.carmine.impl.sentinel))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*
            taoensso.carmine-v4/with-carmine
            taoensso.carmine-v4/redis-call)

(alias 'core 'taoensso.carmine-v4)

;;;; TODO
;; - Conn req timeouts
;; - Re-check Sentinel client docs
;; - Sketch API for plugging into wcar
;; - API for interactions with Pub/Sub and connection pools?

;; - Proper opts
;; - Testing env (local + GitHub)
;;   - `redis-sentinel sentinel1.conf` (port 26379)
;;   - `redis-sentinel sentinel2.conf` (port 26380)

;;;; Sentinel spec maps
;; {<master-name> [[<sentinel-ip> <sentinel-port> ...]]}

(defn- address-pair
  "Returns [<ip-string> <port-int>], or throws."
  ( [ip port]  [(have string? ip) (enc/as-int port)])
  ([[ip port]] [(have string? ip) (enc/as-int port)]))

(comment (address-pair "ip" 80))

(defn- remove-sentinel [spec-map master-name addr]
  (let [master-name (enc/as-qname master-name)
        old-addrs   (get spec-map master-name)
        addr        (address-pair addr)]
    (assoc spec-map master-name
      (transduce (remove #(= % addr)) conj [] old-addrs))))

(defn- add-sentinel->front [spec-map master-name addr]
  (let [master-name (enc/as-qname master-name)
        old-addrs   (get spec-map master-name)
        addr        (address-pair addr)]

    (if (= (get old-addrs 0) addr)
      (do    spec-map) ; Common case
      (assoc spec-map master-name
        (transduce (remove #(= % addr)) conj [addr] old-addrs)))))

(defn- add-sentinels->back [spec-map master-name addrs]
  (if (empty? addrs)
    spec-map
    (let [master-name (enc/as-qname master-name)
          old-addrs   (get spec-map master-name [])
          old-addr?   (set old-addrs)]

      (assoc spec-map master-name
        (transduce (comp (map address-pair) (remove old-addr?))
          conj old-addrs addrs)))))

(defn- clean-sentinel-spec [spec-map]
  (reduce-kv
    (fn [m master-name addrs]
      (assoc m (enc/as-qname master-name)
        (transduce (comp (map address-pair) (distinct))
          conj [] addrs)))
    {} spec-map))

(deftest ^:private _spec-map-utils
  [(= (-> {}
        (add-sentinels->back :m1 [["ip1" 1] ["ip2" "2"] ["ip3" 3]])
        (add-sentinels->back :m2 [["ip4" 4] ["ip5" "5"]])
        (add-sentinel->front :m1 ["ip2" 2])
        (add-sentinels->back :m1 [["ip3" 3] ["ip6" 6]])
        (remove-sentinel     :m2 ["ip4" 4]))

     {"m1" [["ip2" 2] ["ip1" 1] ["ip3" 3] ["ip6" 6]],
      "m2" [["ip5" 5]]})

   (is (= (clean-sentinel-spec {:m1 [["ip1" 1] ["ip1" "1"] ["ip2" "2"]]})
         {"m1" [["ip1" 1] ["ip2" 2]]}))])

;;;; Sentinel Specs
;; Simple stateful agent wrapper around a sentinel spec map

(defprotocol ISentinelSpec
  (remove-sentinel!     [sentinel-spec master-name addr]  "Removes given [ip port] Sentinel server address from `master-name`'s addresses in SentinelSpec.")
  (add-sentinel->front! [sentinel-spec master-name addr]  "Adds given [ip port] Sentinel server address to the front of `master-name`'s addresses in SentinelSpec.")
  (add-sentinels->back! [sentinel-spec master-name addrs] "Adds given [[ip port] ...] Sentinel server addresses to the back of `master-name`'s addresses in SentinelSpec.")
  (update-sentinels!    [spec f] "Sets SentinelSpec's state map to (f current-state-map), for given update function `f`."))

(deftype SentinelSpec [spec-map_]
  ;; Using delays to minimize contention during updates
  clojure.lang.IDeref (deref [_] (force @spec-map_))
  clojure.lang.IRef
  (addWatch     [_ k f] (add-watch      spec-map_ k f))
  (removeWatch  [_ k  ] (remove-watch   spec-map_ k))
  (getValidator [_    ] (get-validator  spec-map_))
  (setValidator [_   f] (set-validator! spec-map_ f))

  ISentinelSpec
  (remove-sentinel!     [spec master-name addr]  (swap! spec-map_ (fn [old_] (delay (remove-sentinel     (force old_) master-name addr))))  spec)
  (add-sentinel->front! [spec master-name addr]  (swap! spec-map_ (fn [old_] (delay (add-sentinel->front (force old_) master-name addr))))  spec)
  (add-sentinels->back! [spec master-name addrs] (swap! spec-map_ (fn [old_] (delay (add-sentinels->back (force old_) master-name addrs)))) spec)
  (update-sentinels!    [spec f]                 (swap! spec-map_ (fn [old_] (delay (f                   (force old_)))))                   spec))

(defn sentinel-spec
  "Given a Redis Sentinel server spec map of form
    {<master-name> [[<sentinel-ip> <sentinel-port>] ...]},
  returns a stateful SentinelSpec wrapper that supports
  deref, watching, validation, and the ISentinelSpec utils.

    (def my-sentinel-spec
      \"Stateful Redis Sentinel server spec. Will be kept
       automatically updated by Carmine.\"
      (sentinel-spec
        {:caching       [[\"192.158.1.38\" 26379] ...]
         :message-queue [[\"192.158.1.38\" 26379] ...]}))
      => stateful SentinelSpec

     (add-watch my-sentinel-spec :my-watch
       (fn [_ _ old new]
         (when (not= old new)
           (println \"Sentinel spec changed!\" [old new]))))"
  [spec-map]
  (SentinelSpec. (atom (clean-sentinel-spec spec-map))))

(deftest ^:private _spec-protocol
  [(is (= @(sentinel-spec {:m1 [["ip1" "1"] ["ip1" "1"] ["ip2" "2"]]})
         {"m1" [["ip1" 1] ["ip2" 2]]}))

   (is (=
         (->
           {:m1 [["ip1" "1"] ["ip1" "1"] ["ip2" "2"]]}
           (sentinel-spec)
           (add-sentinel->front! :m1 ["ip3" "3"])
           (add-sentinels->back! :m1 [["ip4" "4"] ["ip5" "5"]])
           (remove-sentinel!     :m1 ["ip5" "5"])
           (deref))
         {"m1" [["ip3" 3] ["ip1" 1] ["ip2" 2] ["ip4" 4]]}))])

(defn- kvs->map [x] (if (map? x) x (into {} (comp (partition-all 2)) x)))
(comment [(kvs->map {"a" "A" "b" "B"}) (kvs->map ["a" "A" "b" "B"])])

;; TODO opts (incl. timeouts, auth, etc.)
(defn resolve-master
  "Given `master-name`, returns [<master-ip> <master-port>] reported by
  sentinel consensus, or throws.
  TODO Opts, etc.
  Follows procedure as per Sentinel spec,
  Ref. https://redis.io/docs/reference/sentinel-clients/"
  [opts sentinel-spec master-name]
  (let [spec-map    (enc/force-ref sentinel-spec)
        master-name (enc/as-qname master-name)
        sentinels   (get spec-map master-name)]

    (if (empty? sentinels)
      (throw
        (ex-info "[Carmine] [Sentinel] No Sentinel servers configured for requested master"
          {:eid :carmine.sentinel/no-sentinels
           :master-name  master-name
           :current-spec spec-map}))

      (let [t0 (System/currentTimeMillis)
            n-attempts* (java.util.concurrent.atomic.AtomicLong. 0)

            {:keys [add-missing-sentinels?]
             :or   {add-missing-sentinels? true}} opts

            mutable-spec? (instance? SentinelSpec sentinel-spec)
            add-missing-sentinels? (and mutable-spec? add-missing-sentinels?)

            attempt-log_  (volatile! []) ; [[<sentinel> <kind>] ...]
            error-counts_ (volatile! {}) ; {<sentinel> {:keys [unreachable ignorant misidentified]}}
            error!
            (fn [sentinel t0-attempt error-kind ?data]
              (let [attempt-msecs (- (System/currentTimeMillis) t0-attempt)]
                (vswap! attempt-log_ conj
                  (assoc
                    (conj
                      {:attempt (.get n-attempts*)
                       :sentinel      sentinel
                       :error         error-kind}
                      ?data)
                    :attempt-msecs attempt-msecs)))

              (vswap! error-counts_
                (fn [m]
                  (enc/update-in m [sentinel error-kind]
                    (fn [?n] (inc ^long (or ?n 0)))))))

            ;; All sentinels reported during resolution process
            reported-sentinels_ (volatile! #{}) ; #{[ip port]}
            complete!
            (fn [?master ?error]

              (when mutable-spec?
                (update-sentinels! sentinel-spec
                  (fn [spec-map]
                    (cond-> spec-map
                      ?master                (add-sentinel->front master-name ?master)
                      add-missing-sentinels? (add-sentinels->back master-name @reported-sentinels_)))))

              (if-let [error ?error] (throw error) ?master))]

        (loop [n-loops 1]

          (enc/cond
            :let [t0-attempt (System/currentTimeMillis)
                  [?sentinel ?master] ; ?[<addr> <addr>]
                  (reduce
                    ;; Try each known sentinel, sequentially
                    (fn [acc sentinel]
                      (.incrementAndGet n-attempts*)
                      (let [[ip port] (address-pair sentinel)
                            [?master ?sentinels-info]
                            (try
                              ;; TODO Opts, auth, etc. ~100 msecs timeout, no pool (?)
                              (core/with-carmine {:host ip :port port} :as-vec
                                (fn []
                                  ;; Does this sentinel know the master?
                                  (core/redis-call "SENTINEL" "get-master-addr-by-name"
                                    master-name)

                                  (when add-missing-sentinels?
                                    ;; Ask sentinel to report on other known sentinels
                                    (binding [core/*keywordize-maps?* false]
                                      (core/redis-call "SENTINEL" "sentinels" master-name)))))

                              (catch Throwable _
                                [::unreachable nil]))]

                        (when-let [sentinels-info ?sentinels-info] ; [<sentinel1-info> ...]
                          (enc/run!
                            (fn [info] ; Info may be map (RESP3) or kvseq (RESP2)
                              (let [info (kvs->map info)]
                                (enc/when-let [ip   (get info "ip")
                                               port (get info "port")]
                                  (vswap! reported-sentinels_ conj [ip port]))))
                            sentinels-info))

                        (enc/cond
                          (vector?    ?master)               (reduced [sentinel ?master])
                          (nil?       ?master)               (do (error! sentinel t0-attempt :ignorant    nil) acc)
                          (identical? ?master ::unreachable) (do (error! sentinel t0-attempt :unreachable nil) acc))))

                    nil sentinels)

                  error-data_
                  (delay
                    {:master-name     master-name
                     :current-spec    spec-map
                     :n-loops         n-loops
                     :n-attempts      (.get n-attempts*)
                     :msecs-elapsed   (- (System/currentTimeMillis) t0)
                     :sentinel-errors @error-counts_
                     :attempt-log     @attempt-log_})]

            (nil? ?master)
            (complete! nil
              (ex-info "[Carmine] [Sentinel] Failed to resolve requested master"
                (assoc @error-data_ :eid :carmine.sentinel/resolve-failure)))

            :if-let [master ?master]
            (let [sentinel ?sentinel
                  master (address-pair master)
                  [ip port] master
                  reply
                  (try
                    ;; Opts, auth, etc., ~250msecs timeout, no pool (?)
                    (core/with-carmine {:host ip :port port} false
                      (fn [] (core/redis-call "ROLE")))
                    (catch Throwable _ nil))

                  ?role (when (vector? reply) (get reply 0))]

              ;; Confirm that master designated by sentinel actually identifies itself as master
              (if (= ?role "masterNO")
                (complete! master nil)

                (let [{:keys [timeout-msecs retry-delay-msecs]
                       :or   {timeout-msecs 2000
                              retry-delay-msecs 250}}
                      opts

                      elapsed-msecs  (- (System/currentTimeMillis) t0)
                      retry-at-msecs (+ elapsed-msecs retry-delay-msecs)]

                  (error! sentinel t0-attempt :misidentified
                    {:identified master :actual-role ?role})

                  (if (> retry-at-msecs timeout-msecs)
                    (do
                      (vswap! attempt-log_ conj
                        [:timeout
                         (str
                           "(" elapsed-msecs " elapsed + " retry-delay-msecs " delay = " retry-at-msecs
                           ") > " timeout-msecs " timeout")])

                      (complete! nil
                        (ex-info "[Carmine] [Sentinel] Timed out while trying to resolve requested master"
                          (assoc @error-data_ :eid :carmine.sentinel/resolve-timeout))))
                    (do
                      (vswap! attempt-log_ conj [:sleep retry-delay-msecs])
                      (Thread/sleep retry-delay-msecs)
                      (recur (inc n-loops)))))))))))))

(comment
  (resolve-master {}
    #_(sentinel-spec {"mymaster" [["127.0.0.1" 26379]]})
    {"mymaster" [["127.0.0.1" 26379]]}
    "mymaster"))

;;;;;

(comment
  (core/wcar {:host "127.0.0.1" :port "6379"}
    (core/redis-call "ROLE"))

  (core/wcar {:port 26379}
    (core/redis-call "SENTINEL" "get-master-addr-by-name" "mymaster"))

  (core/wcar {:port 26379}
    (core/redis-call "HELLO" 3)
    (binding [core/*keywordize-maps?* false]
      (core/redis-call "SENTINEL" "sentinels" "mymaster"))))
