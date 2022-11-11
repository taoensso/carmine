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
            taoensso.carmine-v4/wcar
            taoensso.carmine-v4/redis-call)

(alias 'core 'taoensso.carmine-v4)

;;;; TODO
;; x Initial sketch
;; - Proper errors
;; - Proper opts
;; - How to test locally? Simulated?
;; - How to test on GitHub?
;;   - sentinel1.conf (port 26379), sentinel2.conf (port 26380)
;; - API for interactions with Pub/Sub and connection pools?

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

(deftest ^:private _spec-map-utils
  [(= (-> {}
        (add-sentinels->back :m1 [["ip1" 1] ["ip2" "2"] ["ip3" 3]])
        (add-sentinels->back :m2 [["ip4" 4] ["ip5" "5"]])
        (add-sentinel->front :m1 ["ip2" 2])
        (add-sentinels->back :m1 [["ip3" 3] ["ip6" 6]])
        (remove-sentinel     :m2 ["ip4" 4]))

     {"m1" [["ip2" 2] ["ip1" 1] ["ip3" 3] ["ip6" 6]],
      "m2" [["ip5" 5]]})])

;;;; Sentinel Specs
;; Simple stateful agent wrapper around a sentinel spec map

(defprotocol ISentinelSpec
  (remove-sentinel!     [sentinel-spec master-name addr]  "Removes given [ip port] Sentinel server address from `master-name`'s addresses in SentinelSpec.")
  (add-sentinel->front! [sentinel-spec master-name addr]  "Adds given [ip port] Sentinel server address to the front of `master-name`'s addresses in SentinelSpec.")
  (add-sentinels->back! [sentinel-spec master-name addrs] "Adds given [[ip port] ...] Sentinel server addresses to the back of `master-name`'s addresses in SentinelSpec.")
  (update-sentinels!    [spec f] "Sets SentinelSpec's state map to (f current-state-map), for given update function `f`."))

(deftype SentinelSpec [agent__]
  clojure.lang.IDeref (deref [_] @@agent__)
  clojure.lang.IRef
  (addWatch     [_ k f] (add-watch      @agent__ k f))
  (removeWatch  [_ k  ] (remove-watch   @agent__ k))
  (getValidator [_    ] (get-validator  @agent__))
  (setValidator [_   f] (set-validator! @agent__ f))

  ISentinelSpec
  (remove-sentinel!     [_ master-name addr]  (send @agent__ remove-sentinel     master-name addr))
  (add-sentinel->front! [_ master-name addr]  (send @agent__ add-sentinel->front master-name addr))
  (add-sentinels->back! [_ master-name addrs] (send @agent__ add-sentinels->back master-name addrs))
  (update-sentinels!    [_ f]                 (send @agent__ f)))

(defn sentinel-spec
  "Given a Redis Sentinel server spec map of form
    {<master-name> [[<sentinel-ip> <sentinel-port>] ...]},
  returns a stateful SentinelSpec wrapper that supports
  deref, watching, validation, and the ISentinelSpec utils.

  Uses an agent for state management, consider calling
  (shutdown-agents) as part of your application shutdown."
  [spec-map]
  (let [spec-map
        (reduce-kv
          (fn [m master-name addrs]
            (assoc m (enc/as-qname master-name)
              (transduce (comp (map address-pair) (distinct))
                conj [] addrs)))
          {} spec-map)]

    (SentinelSpec.
      (delay (agent spec-map :error-mode :continue)))))

(comment @(sentinel-spec {:m1 [["ip1" "1"] ["ip1" "1"] ["ip2" "2"]]}))

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
  (let [master-name (enc/as-qname                      master-name)
        sentinels   (get (enc/force-ref sentinel-spec) master-name)]

    (if (empty? sentinels)
      (throw
        (ex-info "TODO" {}))

      (let [t0 (System/currentTimeMillis)
            {:keys [add-missing-sentinels?]
             :or   {add-missing-sentinels? true}} opts

            n-attempts* (java.util.concurrent.atomic.AtomicLong. 0)
            mutable-spec? (instance? SentinelSpec sentinel-spec)
            add-missing-sentinels? (and mutable-spec? add-missing-sentinels?)

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
            :let [unreachable_ (volatile! #{}) ; #{[ip port]}
                  ignorant_    (volatile! #{}) ; ''

                  ?master ; Declared ?[ip port]
                  (reduce
                    ;; Try each known sentinel, sequentially
                    (fn [acc sentinel]
                      (.incrementAndGet n-attempts*)
                      (let [[ip port] sentinel
                            [?master ?sentinels-info]
                            (try
                              ;; TODO Opts, auth, etc. ~100 msecs timeout, no pool (?)
                              (core/wcar {:host ip :port port} :as-vec
                                ;; Does this sentinel know the master?
                                (core/redis-call "SENTINEL" "get-master-addr-by-name"
                                  master-name)

                                (when add-missing-sentinels?
                                  ;; Ask sentinel to report on other known sentinels
                                  (binding [core/*keywordize-maps?* false]
                                    (core/redis-call "SENTINEL" "sentinels" master-name))))

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
                          (vector?    ?master)               (reduced ?master) ; [ip port]
                          (nil?       ?master)               (do (vswap! ignorant_    conj sentinel) acc)
                          (identical? ?master ::unreachable) (do (vswap! unreachable_ conj sentinel) acc))))

                    nil sentinels)]

            (nil? ?master)
            (complete! nil
              (ex-info "TODO"
                {:n-attempts    (.get n-attempts*)
                 :msecs-elapsed (- (System/currentTimeMillis) t0)
                 :sentinel-servers
                 {:unreachable  (let [s @unreachable_] {:count (count s) :addresses s})
                  :ignorant     (let [s @ignorant_]    {:count (count s) :addresses s})}}))

            :if-let [master ?master]
            (let [[ip port] master
                  reply
                  (try
                    ;; Opts, auth, etc., ~250msecs timeout, no pool (?)
                    (core/wcar {:host ip :port port} (core/redis-call "ROLE"))
                    (catch Throwable _ nil))]

              ;; Confirm that master designated by sentinel actually identifies itself as master
              (if (= reply "master")
                (complete! master nil)

                (let [{:keys [timeout-msecs retry-delay-msecs]
                       :or   {timeout-msecs 2000
                              retry-delay-msecs 250}}
                      opts

                      tnow (System/currentTimeMillis)]

                  (if (> (- (+ tnow retry-delay-msecs) t0) timeout-msecs)
                    (complete! nil
                      (ex-info "TODO timeout" {}))
                    (do
                      (Thread/sleep retry-delay-msecs)
                      (recur (inc n-loops)))))))))))))

(comment
  (resolve-master {}
    {"mymaster" [["127.0.0.1" 26379]]}
    "mymaster"))

;;;;;

(comment
  (core/wcar {:port 26379}
    (core/redis-call "SENTINEL" "get-master-addr-by-name" "mymaster"))

  (core/wcar {:port 26379}
    (core/redis-call "HELLO" 3)
    (binding [core/*keywordize-maps?* false]
      (core/redis-call "SENTINEL" "sentinels" "mymaster"))))
