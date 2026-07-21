(ns taoensso.carmine-v4.tests.cluster
  "Live Redis Cluster acceptance tests, selected explicitly by CI."
  (:require
   [clojure.test                     :refer [deftest testing is]]
   [taoensso.encore                  :as enc]
   [taoensso.truss                   :as truss]
   [taoensso.carmine-v4              :as car]
   [taoensso.carmine-v4.opts         :as opts]
   [taoensso.carmine-v4.conns        :as conns]
   [taoensso.carmine-v4.cluster      :as cluster]
   [taoensso.carmine-v4.message-queue :as mq]
   [taoensso.carmine-v4.resp         :as resp]
   [taoensso.carmine-v4.resp.common  :as com]
   [taoensso.carmine-v4.tests.mq-soak :as mq-soak]))

(defn- queue-keys [queue] (#'mq/queue-keys queue))

(defn- get-env! [k]
  (or (System/getenv k)
    (throw (ex-info "Missing Redis Cluster test environment variable" {:key k}))))

(defn- cluster-addrs []
  (let [host (get-env! "CARMINE_TEST_CLUSTER_HOST")]
    (mapv
      (fn [k] [host (Long/parseLong (get-env! k))])
      ["CARMINE_TEST_CLUSTER_PORT_1"
       "CARMINE_TEST_CLUSTER_PORT_2"
       "CARMINE_TEST_CLUSTER_PORT_3"])))

(defn- call-redis!
  ([addr resp3? as-vec? command]
   (call-redis! addr resp3? as-vec? :throw command))
  ([addr resp3? as-vec? error-mode command]
   (conns/with-new-conn
     (opts/parse-conn-opts :redis
       {:server addr, :init {:resp3? resp3?, :client-name nil}})
     (fn [_ in out]
       (resp/with-replies in out
         {:natural-replies? false, :as-vec? as-vec?, :error-mode error-mode} nil
         #(resp/rcmd* command))))))

(defn- call-redis-commands!
  [addr resp3? commands]
  (conns/with-new-conn
    (opts/parse-conn-opts :redis
      {:server addr, :init {:resp3? resp3?, :client-name nil}})
    (fn [_ in out]
      (resp/with-replies in out false true
        #(resp/rcmds* commands)))))

(defn- await!
  [timeout-ms description f]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (let [value (try (f) (catch Throwable t t))]
        (cond
          (and value (not (instance? Throwable value))) value
          (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 100) (recur))
          :else
          (throw
            (ex-info (str "Timed out waiting for " description)
              {:timeout-ms timeout-ms, :last-value value}
              (when (instance? Throwable value) value))))))))

(defn- call-cluster-replica!
  [addr & commands]
  (let [[readonly & replies]
        (call-redis-commands! addr true (into [["READONLY"]] commands))]
    (when-not (= readonly "OK")
      (throw (ex-info "Redis Cluster replica rejected READONLY" {:addr addr, :reply readonly})))
    replies))

(defn- reply-value! [addr resp3? command]
  (first (call-redis! addr resp3? true command)))

(defn- reply-result! [addr resp3? command]
  (first (call-redis! addr resp3? true :return command)))

(defn- thrown-reply! [addr resp3? command]
  (try
    (call-redis! addr resp3? false command)
    (catch Throwable t
      (or (truss/matching-error com/reply-error?
            {:eid :carmine.read/redis-error-reply} t)
        t))))

(defn- slot-master-addr [topology slot]
  (get-in (cluster/slot->shard topology slot) [:master :addr]))

(defn- master-id-for-addr [topology addr]
  (some
    (fn [[_ shard]]
      (when (= (get-in shard [:master :addr]) addr)
        (get-in shard [:master :id])))
    (:slot-ranges topology)))

(defn- topology-routes [topology]
  (mapv #(slot-master-addr topology %) (range 16384)))

(defn- direct-shards-topology! [addr]
  (cluster/parse-cluster-shards addr
    (reply-value! addr true ["CLUSTER" "SHARDS"])))

(defn- find-key-for-slot [prefix target-slot]
  (loop [idx 0]
    (when (>= idx 1000000)
      (throw
        (ex-info "Failed to find key for Redis Cluster slot"
          {:target-slot target-slot, :prefix prefix})))
    (let [key (str prefix idx)
          ck  (cluster/cluster-key key)]
      (if (= (cluster/cluster-slot ck) target-slot)
        ck
        (recur (inc idx))))))

(defn- find-owner-keys [topology prefix]
  (let [all-addrs
        (into #{}
          (keep (fn [[_ shard]] (get-in shard [:master :addr])))
          (:slot-ranges topology))]
    (loop [idx 0, found {}]
      (if (= (count found) (count all-addrs))
        found
        (do
          (when (>= idx 1000000)
            (throw
              (ex-info "Failed to find keys spanning Redis Cluster owners"
                {:owner-addrs all-addrs, :found-addrs (set (keys found))})))
          (let [ck   (cluster/cluster-key (str "{owner-" idx "}" prefix))
                addr (slot-master-addr topology (cluster/cluster-slot ck))]
            (recur (inc idx) (if (contains? found addr) found (assoc found addr ck)))))))))

(defn- find-owner-plain-keys [topology prefix]
  (into {}
    (map
      (fn [[addr ck]]
        ;; Generate again from the wrapper's exact UTF-8 bytes as a plain key.
        [addr (enc/utf8-ba->str (.redisBytes
                                 ^taoensso.carmine_v4.classes.RawRedisArg ck))]))
    (find-owner-keys topology prefix)))

(defn- delete-keys! [owned-keys]
  (doseq [[addr ck] owned-keys]
    (try
      (reply-value! addr true ["DEL" ck])
      (catch Throwable _))))

(deftest ^:cluster-integration _live-cluster
  (let [addrs      (cluster-addrs)
        seed-addr  (first addrs)
        discoveries
        (into {}
          (for [resp3? [false true]
                source [:slots :shards]]
            (let [reply
                  (reply-value! seed-addr resp3?
                    ["CLUSTER" (if (= source :slots) "SLOTS" "SHARDS")])
                  topology
                  ((if (= source :slots)
                     cluster/parse-cluster-slots
                     cluster/parse-cluster-shards)
                   seed-addr reply)]
              [[source resp3?] topology])))
        topology (get discoveries [:shards true])
        run-id   (str (java.util.UUID/randomUUID))
        prefix   (str ":carmine:v4:cluster:" run-id)
        owned_   (atom [])]

    (try
      (testing "RESP2 and RESP3 topology discovery"
        (doseq [[[source resp3?] topology] discoveries]
          [(is (:complete? topology) (str source " RESP3=" resp3?))
           (is (= (:n-slots topology) 16384))
           (is (= (count
                    (into #{}
                      (map #(get-in % [:master :addr]))
                      (vals (:slot-ranges topology))))
                  3))])

        (let [routes (mapv (comp topology-routes val) discoveries)]
          (is (apply = routes)
            "SLOTS and SHARDS agree on every slot-to-master route"))

        (is (every? #(= :online (get-in % [:master :health]))
              (vals (:slot-ranges topology)))
          "Live SHARDS reports every master online"))

      (testing "Cluster key hashing matches the Redis oracle"
        (let [string-keys
              (into
                ["foo" "foo{}{bar}" "foo{{bar}}" "foo{bar}{zap}"
                 (str "{" run-id "}" prefix)]
                (map #(str prefix ":oracle:" %) (range 96)))
              binary-keys
              [(byte-array [(byte 0) (byte 123) (byte 97) (byte 125) (byte 1)])
               (enc/str->utf8-ba (str prefix "{binary}"))]]
          (doseq [key (concat string-keys binary-keys)]
            (let [ck (cluster/cluster-key key)]
              (is (= (reply-value! seed-addr true ["CLUSTER" "KEYSLOT" ck])
                     (cluster/cluster-slot ck)))))))

      (testing "Owner routing, boundary slots, and binary keys"
        (let [owner-keys (find-owner-keys topology prefix)
              edge-keys
              (mapv
                (fn [slot]
                  (let [ck (find-key-for-slot (str prefix ":edge:" slot ":") slot)]
                    [(slot-master-addr topology slot) ck]))
                [0 16383])
              binary-key (cluster/cluster-key (enc/str->utf8-ba (str "{binary}" prefix)))
              binary-owned
              [(slot-master-addr topology (cluster/cluster-slot binary-key)) binary-key]
              owned-keys
              (into (mapv (fn [[addr ck]] [addr ck]) owner-keys)
                (conj edge-keys binary-owned))]

          (reset! owned_ owned-keys)
          (doseq [[addr ck] owned-keys]
            [(is (= (reply-value! addr true ["SET" ck "value"]) "OK"))
             (is (= (reply-value! addr false ["GET" ck]) "value"))])

          (is (= (into #{} (map first) owner-keys) (set addrs))
            "Generated keys cover every configured master")))

      (testing "Real MOVED and CROSSSLOT errors"
        (let [[owner-addr ck] (first @owned_)
              wrong-addr      (first (remove #(= % owner-addr) addrs))
              command         ["GET" ck]
              moved-value     (reply-result! wrong-addr true command)
              moved-thrown    (thrown-reply! wrong-addr true command)
              parsed-value    (cluster/parse-cluster-error wrong-addr moved-value)
              parsed-thrown   (cluster/parse-cluster-error wrong-addr moved-thrown)]
          [(is (com/reply-error? moved-value))
           (is (com/reply-error? moved-thrown))
           (is (= (dissoc parsed-value :error) (dissoc parsed-thrown :error)))
           (is (truss/submap? parsed-value
                 {:kind :moved
                  :slot (cluster/cluster-slot ck)
                  :addr owner-addr}))])

        (let [[[_ ck1] [_ ck2]] (take 2 @owned_)
              error  (reply-result! seed-addr true ["MGET" ck1 ck2])
              parsed (cluster/parse-cluster-error seed-addr error)]
          [(is (com/reply-error? error))
           (is (truss/submap? parsed
                 {:kind :cross-slot, :transient? false}))]))

      (finally
        (delete-keys! @owned_)))))

(deftest ^:cluster-integration _live-cluster-spec
  (let [addrs           (cluster-addrs)
        changed-events_ (atom [])
        spec            (cluster/cluster-spec addrs
                          {:cbs {:on-changed-topology #(swap! changed-events_ conj %)}})
        topology        (cluster/refresh-topology! spec)
        master-addrs
        (into #{}
          (keep #(get-in % [:master :addr]))
          (vals (:slot-ranges topology)))]

    [(is (:complete? topology))
     (is (= (:n-slots topology) 16384))
     (is (= master-addrs (set addrs)))
     (is (every? (set (:known-addrs @spec)) addrs))
     (is (identical? topology (cluster/cached-topology spec)))
     (is (= (count @changed-events_) 1))]

    (cluster/refresh-topology! spec)
    (is (= (count @changed-events_) 1)
      "An unchanged live routing projection emits no change callback")

    (testing "Cluster manager executes and stitches cross-node pipelines"
      (let [owned (find-owner-plain-keys topology
                    (str ":carmine:v4:executor:" (java.util.UUID/randomUUID)))
            pairs (vec owned)]
        (try
          (with-open [mgr
                      (car/conn-manager-clustered
                        {:conn-opts
                         {:server {:cluster-spec spec
                                   :cluster-opts {:max-concurrent-partitions 3}}}
                         :pool-opts {:test-while-idle? false}})]
            (let [set-replies
                  (car/wcar mgr :as-vec
                    (doseq [[_ key] pairs]
                      (car/set key "value")))
                  get-replies
                  (car/wcar mgr :as-vec
                    (doseq [[_ key] (reverse pairs)]
                      (car/get key))
                    (resp/local-echo :local))]
              [(is (= set-replies (vec (repeat (count pairs) "OK"))))
               (is (= get-replies
                     (conj (vec (repeat (count pairs) "value")) :local)))
               (is (pos? (get-in @mgr [:stats :counts :created])))]))
          (finally (delete-keys! pairs)))))

    (testing "Lua fallback is routed, node-local, and broadcast-safe"
      (let [owned (find-owner-plain-keys topology
                    (str ":carmine:v4:lua:" (java.util.UUID/randomUUID)))
            pairs (vec owned)
            keys  (mapv second pairs)
            script "return {KEYS[1], ARGV[1]}"
            expected (mapv (fn [key] [key "value"]) keys)]
        (doseq [addr addrs]
          (is (= (reply-value! addr true ["SCRIPT" "FLUSH"]) "OK")))
        (with-open [mgr
                    (car/conn-manager-clustered
                      {:conn-opts {:server {:cluster-spec spec}}
                       :pool-opts {:test-while-idle? false}})]
          [(is (= (mapv
                    (fn [key]
                      (car/wcar mgr (car/lua script [key] ["value"])))
                    keys)
                  expected)
             "The same cold script falls back independently on every owner")
           (is (= (mapv
                    (fn [key]
                      (car/wcar mgr (car/lua script [key] ["value"])))
                    keys)
                  expected)
             "Warm scripts execute through EVALSHA on every owner")]

          (testing "Single-key CAS helpers pipeline and route across every owner"
            (doseq [key keys]
              [(is (= (car/wcar mgr :as-vec
                        (car/del key)
                        (car/compare-and-set key nil :unexpected)
                        (car/set key {:owner key})
                        (car/compare-and-set key {:owner key} [:updated key])
                        (car/compare-and-delete key {:owner key})
                        (car/compare-and-delete key [:updated key]))
                      [0 false "OK" true false true]))
               (is (= (car/wcar mgr :as-vec
                        (car/compare-and-hset key "field" nil :unexpected)
                        (car/hset key "field" {:owner key})
                        (car/compare-and-hset key "field" {:owner key} [:updated key])
                        (car/compare-and-hdel key "field" {:owner key})
                        (car/compare-and-hdel key "field" [:updated key])
                        (car/exists key))
                      [false 1 true false true 0]))]))

          (testing "Atomic swap helpers route per key"
            (doseq [key keys]
              (let [hash-key (str "{" key "}:hash")]
                [(is (= (car/swap mgr key
                          (fn [old missing?]
                            (is (nil? old))
                            (is missing?)
                            {:owner key, :version 1}))
                        {:owner key, :version 1}))
                 (is (= (car/swap mgr key
                          (fn [old _] (update old :version inc)))
                        {:owner key, :version 2}))
                (is (= (car/hswap mgr hash-key "field"
                          (fn [old missing?]
                            (is (nil? old))
                            (is missing?)
                            [:field key]))
                        [:field key]))
                (is (= (car/wcar mgr :as-vec
                          (car/get key)
                          (car/hget hash-key "field"))
                        [{:owner key, :version 2} [:field key]]))])))

          (let [[flushed-addr flushed-key] (first pairs)]
            (is (= (reply-value! flushed-addr true ["SCRIPT" "FLUSH"]) "OK"))
            (is (= (car/wcar mgr (car/lua script [flushed-key] ["value"]))
                  [flushed-key "value"])
              "A per-node flush triggers fallback where the script is absent")
            (let [[_ warm-key] (second pairs)]
              (is (= (car/wcar mgr (car/evalsha* script 1 warm-key "value"))
                    [warm-key "value"])
                "An unflushed owner retains its independently loaded script")))

          (let [[key-a key-b] (take 2 keys)]
            (is (truss/throws? :ex-info {:eid :carmine.cluster/cross-slot-keys}
                  (car/wcar mgr
                    (car/lua "return {KEYS[1], KEYS[2]}"
                      [key-a key-b] [])))))

          [(is (truss/throws? :ex-info {:eid :carmine.cluster/target-required}
                 (car/wcar mgr (car/eval* "return 'targeted'" 0))))
           (is (= (car/wcar mgr
                    (car/with-cluster-target :any
                      (car/eval* "return 'targeted'" 0)))
                 "targeted"))
           (is (truss/throws? :ex-info
                 {:eid :carmine.script/cluster-broadcast-not-supported}
                 (car/wcar mgr
                   (car/with-cluster-target :masters
                     (car/eval* "return 'broadcast'" 0)))))
           (let [replies
                 (car/wcar mgr
                   (car/with-cluster-target :masters
                     (car/eval "return 'broadcast'" 0)))]
             [(is (= (count replies) (count addrs)))
              (is (every? #(= (:reply %) "broadcast") replies))])
           (is (= (car/wcar mgr
                    (car/lua-ro "return KEYS[1]" [(first keys)] []))
                 (first keys)))])))

    (testing "Concurrent cross-node pipelines survive repeated pool clears"
      (let [owned (find-owner-plain-keys topology
                    (str ":carmine:v4:stress:" (java.util.UUID/randomUUID)))
            pairs (vec owned)
            keys  (mapv second pairs)
            expected (vec (repeat (count keys) "value"))
            n-workers 8
            n-iterations 20
            n-clears 10
            ready (java.util.concurrent.CountDownLatch. n-workers)
            start (java.util.concurrent.CountDownLatch. 1)]
        (try
          (with-open [mgr
                      (car/conn-manager-clustered
                        {:conn-opts
                         {:server {:cluster-spec spec
                                   :cluster-opts {:max-concurrent-partitions 3}}}
                         :pool-opts
                         {:test-while-idle? false
                          :max-total 6, :max-total-per-key 2
                          :max-wait-ms 10000}})]
            (is (= (car/wcar mgr :as-vec
                     (doseq [key keys] (car/set key "value")))
                  (vec (repeat (count keys) "OK"))))
            (let [workers
                  (mapv
                    (fn [_]
                      (future
                        (.countDown ready)
                        (.await start)
                        (try
                          (dotimes [_ n-iterations]
                            (let [replies
                                  (car/wcar mgr :as-vec
                                    (doseq [key keys] (car/get key)))]
                              (when-not (= replies expected)
                                (throw
                                  (ex-info "Unexpected replies during Cluster stress"
                                    {:expected expected, :actual replies})))))
                          :done
                          (catch Throwable t t))))
                    (range n-workers))
                  _ (is (.await ready 5 java.util.concurrent.TimeUnit/SECONDS))
                  _ (.countDown start)
                  clear-f
                  (future
                    (mapv
                      (fn [_]
                        (Thread/sleep 5)
                        (car/conn-manager-clear! mgr 5000))
                      (range n-clears)))
                  clear-results (deref clear-f 30000 ::timeout)
                  worker-results (mapv #(deref % 30000 ::timeout) workers)]
              [(is (not= clear-results ::timeout) "Live clears finish within their bound")
               (is (= clear-results
                     (vec (repeat n-clears {:action :cleared, :graceful? true}))))
               (is (not-any? #{::timeout} worker-results))
               (is (every? #{:done} worker-results))
               (is (<= n-clears (get-in @mgr [:stats :counts :cleared])))
               (is (car/conn-manager-open? mgr))]))
          (finally (delete-keys! pairs)))))

    (testing "A dropped node connection is replaced on the next routed borrow"
      (let [[owner-addr key]
            (first
              (find-owner-plain-keys topology
                (str ":carmine:v4:dropped:" (java.util.UUID/randomUUID))))
            pair [owner-addr key]]
        (try
          (with-open [mgr
                      (car/conn-manager-clustered
                        {:conn-opts {:server {:cluster-spec spec}}
                         :pool-opts {:test-while-idle? false
                                     :ready-check-after-idle-ms 0}})]
            (is (= (car/wcar mgr (car/set key "value")) "OK"))
            (let [client-id
                  (conns/mgr-borrow-addr! mgr owner-addr
                    (fn [_ in out]
                      (resp/with-replies in out false false
                        #(resp/rcmd "CLIENT" "ID"))))
                  created-before (get-in @mgr [:stats :counts :created])]
              [(is (= (reply-value! owner-addr true
                        ["CLIENT" "KILL" "ID" client-id]) 1))
               (is (= (car/wcar mgr (car/get key)) "value"))
               (is (< created-before (get-in @mgr [:stats :counts :created]))
                 "Borrow validation replaces the server-closed node connection")]))
          (finally (delete-keys! [pair])))))

    (testing "A real MOVED reply repairs stale routing and retries safely"
      (let [[owner-addr key]
            (first
              (find-owner-keys topology
                (str ":carmine:v4:moved:" (java.util.UUID/randomUUID))))
            wrong-addr (first (remove #{owner-addr} addrs))
            stale-spec (cluster/cluster-spec addrs
                         {:topology-source :cluster-slots})]
        (try
          (is (= (reply-value! owner-addr true ["SET" key "moved-value"]) "OK"))
          (with-redefs-fn
            {#'cluster/raw-topology-reply!
             (fn [& _]
               [[0 16383 [(first wrong-addr) (second wrong-addr) "stale"]]])}
            #(cluster/refresh-topology! stale-spec))
          (is (= (get-in (cluster/slot->shard
                           (cluster/cached-topology stale-spec)
                           (cluster/cluster-slot key))
                     [:master :addr])
                wrong-addr))
          (with-open [mgr
                      (car/conn-manager-clustered
                        {:conn-opts
                         {:server
                          {:cluster-spec stale-spec
                           :cluster-opts
                           {:max-retry-rounds 2, :retry-backoff-ms 0}}}
                         :pool-opts {:test-while-idle? false}})]
            (is (= (car/wcar mgr (resp/rcmd "GET" key)) "moved-value")))
          [(is (= (get-in (cluster/slot->shard
                            (cluster/cached-topology stale-spec)
                            (cluster/cluster-slot key))
                      [:master :addr])
                 owner-addr))
           (is (= (get-in @stale-spec
                    [:stats :execution-stats :n-moved]) 1))
           (is (= (get-in @stale-spec
                    [:stats :execution-stats :n-retry-rounds]) 1))]
          (finally
            (try (reply-value! owner-addr true ["DEL" key])
              (catch Throwable _))))))

    (testing "A real ASK reply preserves cached ownership and retries with ASKING"
      (let [[source-addr key]
            (first
              (find-owner-keys topology
                (str ":carmine:v4:ask:" (java.util.UUID/randomUUID))))
            target-addr (first (remove #{source-addr} addrs))
            slot        (cluster/cluster-slot key)
            source-id   (master-id-for-addr topology source-addr)
            target-id   (master-id-for-addr topology target-addr)
            ask-before  (long (or (get-in @spec [:stats :execution-stats :n-ask]) 0))]
        (try
          [(is (and source-id target-id))
           (is (= (reply-value! source-addr true ["SET" key "ask-value"]) "OK"))
           (is (= (reply-value! target-addr true
                    ["CLUSTER" "SETSLOT" slot "IMPORTING" source-id]) "OK"))
           (is (= (reply-value! source-addr true
                    ["CLUSTER" "SETSLOT" slot "MIGRATING" target-id]) "OK"))
           (is (= (reply-value! source-addr true
                    ["MIGRATE" (first target-addr) (second target-addr)
                     key 0 5000 "REPLACE"])
                  "OK"))]

          (let [ask-error (reply-result! source-addr true ["GET" key])
                parsed    (cluster/parse-cluster-error source-addr ask-error)]
            [(is (truss/submap? parsed
                   {:kind :ask, :slot slot, :addr target-addr}))
             (with-open [mgr
                         (car/conn-manager-clustered
                           {:conn-opts
                            {:server
                             {:cluster-spec spec
                              :cluster-opts
                              {:max-retry-rounds 2, :retry-backoff-ms 0}}}
                            :pool-opts {:test-while-idle? false}})]
               (is (= (car/wcar mgr (car/get key)) "ask-value")))
             (is (= (slot-master-addr (cluster/cached-topology spec) slot)
                    source-addr)
               "ASK does not rewrite cached ownership")
             (is (< ask-before
                    (long (get-in @spec [:stats :execution-stats :n-ask]))))])

          (finally
            ;; Delete the imported key before restoring the slot's stable state.
            (try (call-redis-commands! target-addr true [["ASKING"] ["DEL" key]])
              (catch Throwable _))
            (try (reply-value! source-addr true ["CLUSTER" "SETSLOT" slot "STABLE"])
              (catch Throwable _))
            (try (reply-value! target-addr true ["CLUSTER" "SETSLOT" slot "STABLE"])
              (catch Throwable _))
            (try (reply-value! source-addr true ["DEL" key])
              (catch Throwable _))))))

    (let [resp2-spec (cluster/cluster-spec addrs
                       {:conn-opts {:init {:resp3? false}}})
          resp2-topology (cluster/refresh-topology! resp2-spec)
          slots-spec (cluster/cluster-spec addrs
                       {:topology-source :cluster-slots})
          slots-topology (cluster/refresh-topology! slots-spec)]
      [(is (= (topology-routes resp2-topology) (topology-routes topology))
         "RESP2 discovery agrees with RESP3")
       (is (= (topology-routes slots-topology) (topology-routes topology))
         "Pinned CLUSTER SLOTS discovery agrees with CLUSTER SHARDS")])

    (let [mixed-spec (cluster/cluster-spec
                       (into [[(first (first addrs)) 1]] addrs))
          mixed-topology
          (cluster/refresh-topology! mixed-spec)]
      [(is (:complete? mixed-topology))
       (is (= (get-in @mixed-spec [:stats :node-stats [(first (first addrs)) 1] :n-errors]) 1)
         "A refused seed advances discovery to the next candidate")])

    (let [host     (first (first addrs))
          bad-spec (cluster/cluster-spec [[host 1] [host 2]])
          error
          (try
            (cluster/refresh-topology! bad-spec)
            (catch Throwable t t))]
      [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-failed))
       (is (= (:n-attempts (ex-data error)) 2))])))

(deftest ^:cluster-integration _live-cluster-message-queue
  (doseq [resp3? [false true]]
    (let [qname (str "cluster-mq-v4-" (java.util.UUID/randomUUID))
          spec (cluster/cluster-spec (cluster-addrs))]
      (with-open [mgr
                  (car/conn-manager-clustered
                    {:conn-opts {:server {:cluster-spec spec}
                                 :init {:resp3? resp3?}}})]
        (let [queue (mq/queue mgr qname
                      {:retry-base-ms 0, :retry-max-ms 0, :on-duplicate :coalesce})
              handled (promise)
              worker (mq/worker-create queue
                       (fn [claim]
                         (deliver handled claim)
                         (mq/outcome:ack))
                       {:idle-max-ms 1000})]
          (try
            (mq/worker-start! worker)
            [(is (= (await! 5000 "the MQ worker to enter its blocking wake"
                      #(when (pos? (long
                                     (get-in @worker [:stats :counts :wake-waits] 0)))
                         true))
                    true))
             (is (= (:action (mq/msg-enqueue! queue {:payload :old}
                               {:mid "mid", :delay-ms 10000})) :added))
             (is (= (:action (mq/msg-enqueue! queue {:payload :cluster}
                               {:mid "mid", :delay-ms 0})) :coalesced))
             (is (truss/submap? (deref handled 5000 nil)
                   {:mid "mid", :msg {:payload :cluster}}))
             (is (= (await! 5000 "the Cluster MQ message to settle"
                      #(when (nil? (mq/msg-status queue "mid")) true))
                    true))
             (is (pos? (long
                         (get-in @worker [:stats :counts :wake-signals] 0))))
             (is (mq/worker-stop! worker))
             (is (mq/worker-await-stopped! worker 5000))
             (is (nil? (mq/msg-status queue "mid")))]
            (finally
              (.close ^java.io.Closeable worker)
              (mq/queue-clear!! queue))))))))

(deftest ^:cluster-integration _live-cluster-message-queue-move
  (let [spec (cluster/cluster-spec (cluster-addrs))]
    (with-open [mgr
                (car/conn-manager-clustered
                  {:conn-opts {:server {:cluster-spec spec}}})]
      (let [topology (cluster/refresh-topology! spec)
            source-name (str "cluster-mq-v4-move-source-" (java.util.UUID/randomUUID))
            source (mq/queue mgr source-name
                     {:on-duplicate :coalesce
                      :retry-base-ms 0, :retry-max-ms 0})
            source-slot
            (cluster/cluster-slot
              (cluster/cluster-key (get (queue-keys source) :config)))
            source-master (slot-master-addr topology source-slot)
            [target-name target-slot target-master]
            (loop [idx 0]
              (when (= idx 4096)
                (throw
                  (ex-info "Cluster move test requires at least two primaries"
                    {:source-slot source-slot, :source-master source-master})))
              (let [candidate
                    (str "cluster-mq-v4-move-target-" idx "-"
                      (java.util.UUID/randomUUID))
                    slot
                    (cluster/cluster-slot
                      (cluster/cluster-key (get (#'mq/qkeys candidate) :config)))
                    master (slot-master-addr topology slot)]
                (if (not= master source-master)
                  [candidate slot master]
                  (recur (inc idx)))))
            target_ (volatile! nil)]
        (try
          (mq/msg-enqueue! source :dead {:mid "dead"})
          (let [dead (#'mq/claim! source 64)]
            (#'mq/settle! source "dead" (:token dead)
              (mq/outcome:dead {:reason "cluster-move"})))
          (mq/msg-enqueue! source :leased {:mid "leased"})
          (let [leased (#'mq/claim! source 64)]
            (mq/msg-enqueue! source :successor {:mid "leased"})
            (mq/msg-enqueue! source :scheduled {:mid "scheduled", :delay-ms 60000})
            (let [before (:counts (mq/queue-status source))
                  moved (mq/queue-move! source target-name)
                  target (:queue moved)]
              (vreset! target_ target)
              [(is (not= source-slot target-slot))
               (is (not= source-master target-master)
                 "Move crosses physical Cluster primaries")
               (is (= (:counts (mq/queue-status target)) before))
               (is (truss/submap? (mq/dead-info target "dead")
                     {:status :dead, :msg :dead, :reason "cluster-move"}))
               (is (zero? (car/wcar mgr
                            (apply car/exists (vals (queue-keys source))))))
               (is (= (:action (#'mq/settle! target "leased" (:token leased)
                                 (mq/outcome:ack)))
                     :acked))]
              (let [successor (#'mq/claim! target 64)]
                [(is (= (:msg successor) :successor))
                 (is (= (:action (#'mq/settle! target "leased" (:token successor)
                                   (mq/outcome:ack)))
                       :acked))])))
          (finally
            (when-let [target @target_]
              (try
                (car/wcar mgr (apply car/unlink (vals (queue-keys target))))
                (catch Throwable _)))
            (try
              (car/wcar mgr (apply car/unlink (vals (queue-keys source))))
              (catch Throwable _))))))))

(deftest ^:cluster-integration _live-cluster-message-queue-failover
  (let [spec (cluster/cluster-spec (cluster-addrs))
        qname (str "cluster-mq-v4-failover-" (java.util.UUID/randomUUID))]
    (with-open [mgr
                (car/conn-manager-clustered
                  {:conn-opts {:server {:cluster-spec spec}}})]
      (let [queue (mq/queue mgr qname
                    {:lease-ms 120000, :retry-base-ms 0, :retry-max-ms 0})
            slot (cluster/cluster-slot
                   (cluster/cluster-key (get (queue-keys queue) :payloads)))
            initial-topology (cluster/refresh-topology! spec)
            initial-shard (cluster/slot->shard initial-topology slot)
            initial-master (get-in initial-shard [:master :addr])
            failover-replica (get-in initial-shard [:replicas 0 :addr])
            initial-routes (topology-routes initial-topology)
            observer-addrs
            (into []
              (comp (mapcat :nodes) (keep :addr) (distinct))
              (:shards initial-topology))
            soak-qname
            (loop [idx 0]
              (let [candidate (str "cluster-mq-v4-soak-" idx "-" (java.util.UUID/randomUUID))
                    candidate-slot
                    (cluster/cluster-slot
                      (cluster/cluster-key (get (#'mq/qkeys candidate) :payloads)))
                    candidate-master
                    (get-in (cluster/slot->shard initial-topology candidate-slot)
                      [:master :addr])]
                (if (= candidate-master initial-master) candidate (recur (inc idx)))))
            claimed_ (promise)
            continue_ (promise)
            settled_ (promise)
            successor-handled_ (promise)
            handler-calls_ (atom 0)
            worker
            (mq/worker-create queue
              (fn [claim]
                (swap! handler-calls_ inc)
                (if (= (:msg claim) {:topology :cluster})
                  (do (deliver claimed_ claim) @continue_)
                  (deliver successor-handled_ claim))
                (mq/outcome:ack))
              {:idle-max-ms 1000
               :on-event #(when (= (:event %) :settled) (deliver settled_ %))})
            current-master!
            (fn []
              (-> (cluster/refresh-topology! spec)
                (cluster/slot->shard slot)
                (get-in [:master :addr])))
            soak (mq-soak/start! mgr "cluster-short"
                   {:queue-name soak-qname, :mid-count 4
                    :producer-count 2, :concurrency 2})]
        (try
          [(is (some? initial-master))
           (is (some? failover-replica))
           (is (not= initial-master failover-replica))
           (is (mq/worker-start! worker))
           (is (= (await! 5000 "the MQ failover worker to enter its blocking wake"
                    #(when (pos? (long
                                   (get-in @worker [:stats :counts :wake-waits] 0)))
                       true))
                  true))
           (is (= (:action (mq/msg-enqueue! queue {:topology :cluster} {:mid "mid"})) :added))]

          (let [{:keys [mid] :as claim}
                (await! 5000 "the MQ worker to claim its message"
                  #(deref claimed_ 10 nil))
                token (car/wcar mgr
                        (car/hget (get (queue-keys queue) :lease-tokens) mid))]
            [(is (truss/submap? claim
                   {:mid "mid", :msg {:topology :cluster}}))
             (is (= (await! 10000 "the active MQ lease to reach its Cluster replica"
                      #(let [[score replica-token]
                             (call-cluster-replica! failover-replica
                               ["ZSCORE" (get (queue-keys queue) :leased) mid]
                               ["HGET" (get (queue-keys queue) :lease-tokens) mid])]
                         (when (and score (= replica-token token)) true)))
                    true))
             (is (= (:action (mq/msg-enqueue! queue {:topology :successor} {:mid "mid"
                                                                         :on-duplicate :coalesce}))
                    :coalesced-successor))
             (is (= (await! 10000 "the coalesced MQ successor to reach its Cluster replica"
                      #(when (some? (first
                                      (call-cluster-replica! failover-replica
                                        ["HGET" (get (queue-keys queue) :successor-payloads) mid])))
                         true))
                    true))])

          (testing "A replicated active lease settles through the promoted replica"
            [(is (= (reply-value! failover-replica true ["CLUSTER" "FAILOVER"]) "OK"))
             (is (= (await! 30000 "the MQ slot replica to become primary"
                      #(when (= (current-master!) failover-replica) failover-replica))
                    failover-replica))

             (deliver continue_ true)
             (let [event (deref settled_ 15000 nil)]
               [(is (= (get-in event [:result :action]) :acked))
                (is (get-in event [:result :successor-promoted?]))])
             (is (= (:msg (deref successor-handled_ 15000 nil))
                    {:topology :successor}))
             (is (= (await! 10000 "the promoted successor to settle"
                      #(when (and (= @handler-calls_ 2)
                              (nil? (mq/msg-status queue "mid"))) true))
                    true))
             (is (pos? (long
                         (get-in @worker [:stats :counts :wake-signals] 0))))

             (is (= (await! 10000 "the settlement to replicate to the original primary"
                      #(let [[payload]
                             (call-cluster-replica! initial-master
                               ["HGET" (get (queue-keys queue) :payloads) "mid"])]
                         (when (nil? payload) true)))
                    true))

             (let [report (mq-soak/finish! soak 60000)]
               [(is (:no-phantoms? report))
                (is (:drained? report))
                (is (:successors-exercised? report))
                (is (:successors-bounded? report))])])

          (finally
            (deliver continue_ true)
            (mq-soak/close! soak)
            (.close ^java.io.Closeable worker)
            (try (mq/queue-clear!! queue) (catch Throwable _))

            ;; Restore and verify the captured topology so sibling tests never
            ;; depend on namespace/test-map iteration order.
            (try
              (await! 30000 "the original MQ slot primary to be restored"
                #(if (= (current-master!) initial-master)
                   initial-master
                   (do
                     ;; A just-demoted primary may briefly reject FAILOVER while
                     ;; its replication link becomes ready. Reissue until the
                     ;; live topology confirms restoration.
                     (when (= (reply-value! initial-master true
                                ["CLUSTER" "FAILOVER"])
                              "OK")
                       (when (= (current-master!) initial-master)
                         initial-master)))))

              ;; A single node can observe the restored primary before Cluster
              ;; gossip has converged elsewhere. Query every captured node
              ;; directly so the following test cannot inherit an ambiguous
              ;; two-master SHARDS view from this failover.
              (await! 30000 "every Cluster node to observe the restored topology"
                #(let [topologies (mapv direct-shards-topology! observer-addrs)]
                   (when
                     (every?
                       (fn [topology]
                         (and (:complete? topology)
                           (= (topology-routes topology) initial-routes)))
                       topologies)
                     initial-master)))
              (catch Throwable t
                (throw
                  (ex-info "Failed to restore the original Cluster primary"
                    {:slot slot, :initial-master initial-master
                     :failover-replica failover-replica}
                    t))))))))))
