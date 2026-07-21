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
   [taoensso.carmine-v4.resp         :as resp]
   [taoensso.carmine-v4.resp.common  :as com]))

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
          (for [resp3? [false true]]
            [resp3?
             (cluster/parse-cluster-slots seed-addr
               (reply-value! seed-addr resp3? ["CLUSTER" "SLOTS"]))]))
        topology (get discoveries true)
        run-id   (str (java.util.UUID/randomUUID))
        prefix   (str ":carmine:v4:cluster:" run-id)
        owned_   (atom [])]

    (try
      (testing "RESP2 and RESP3 topology discovery"
        (doseq [[resp3? topology] discoveries]
          [(is (:complete? topology) (str "RESP3=" resp3?))
           (is (= (:n-slots topology) 16384))
           (is (= (count
                    (into #{}
                      (map #(get-in % [:master :addr]))
                      (vals (:slot-ranges topology))))
                  3))])

        (let [routes (mapv (comp topology-routes val) discoveries)]
          (is (apply = routes)
            "RESP2 and RESP3 agree on every slot-to-master route")))

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

(deftest ^:cluster-integration _live-cluster-refresh-topology
  (let [addrs (cluster-addrs)
        spec  (cluster/cluster-spec addrs)]
    (with-open [mgr (car/conn-manager-clustered
                      {:conn-opts {:server {:cluster-spec spec}}})]
      (let [before   (car/cluster-cached-topology spec)
            topology (car/cluster-refresh-topology! mgr)]
        [(is (nil? before) "No topology before the manager's first request")
         (is (= (into #{} (map #(get-in % [:master :addr])) (:shards topology))
                (set addrs)))
         (is (= topology (car/cluster-cached-topology spec)))]))))

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
                                   :cluster-opts {:max-concurrent-nodes 3}}}
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

    (testing "Lua fallback is routed and node-local"
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
                 {:eid :carmine.cluster/invalid-target
                  :problem :broadcast-not-supported}
                 (car/wcar mgr
                   (car/with-cluster-target :masters
                     (car/eval "return 'broadcast'" 0)))))
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
                                   :cluster-opts {:max-concurrent-nodes 3}}}
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
                        (car/conn-manager-clear! mgr {:timeout-ms 5000}))
                      (range n-clears)))
                  clear-results (deref clear-f 30000 ::timeout)
                  worker-results (mapv #(deref % 30000 ::timeout) workers)]
              [(is (not= clear-results ::timeout) "Live clears finish within their bound")
               (is (= clear-results
                     (vec (repeat n-clears true))))
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
            stale-spec (cluster/cluster-spec addrs)]
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
          resp2-topology (cluster/refresh-topology! resp2-spec)]
      (is (= (topology-routes resp2-topology) (topology-routes topology))
        "RESP2 discovery agrees with RESP3"))

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
      [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-error))
       (is (= (:n-attempts (ex-data error)) 2))])))
