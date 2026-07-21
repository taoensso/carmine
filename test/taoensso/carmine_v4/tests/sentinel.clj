(ns taoensso.carmine-v4.tests.sentinel
  "Live Redis Sentinel acceptance tests, selected explicitly by CI."
  (:require
   [clojure.test                :refer [deftest testing is]]
   [taoensso.truss              :as truss]
   [taoensso.carmine-v4         :as car :refer [wcar]]
   [taoensso.carmine-v4.conns   :as conns]
   [taoensso.carmine-v4.opts    :as opts]
   [taoensso.carmine-v4.resp    :as resp]
   [taoensso.carmine-v4.message-queue :as mq]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.tests.mq-soak :as mq-soak]))

(defn- queue-keys [queue] (#'mq/queue-keys queue))

(defn- get-env! [k]
  (or (System/getenv k)
    (throw (ex-info "Missing Redis Sentinel test environment variable" {:key k}))))

(defn- env-addr [host-k port-k] [(get-env! host-k) (Long/parseLong (get-env! port-k))])

(defn- sentinel-addrs []
  (let [host (get-env! "CARMINE_TEST_SENTINEL_HOST")]
    (mapv
      (fn [port] [host (Long/parseLong port)])
      (keep #(System/getenv %)
        ["CARMINE_TEST_SENTINEL_PORT"
         "CARMINE_TEST_SENTINEL_PORT_2"
         "CARMINE_TEST_SENTINEL_PORT_3"]))))

(defn- call-redis!
  [addr & command]
  (conns/with-new-conn
    (opts/parse-conn-opts :redis {:server addr, :init {:resp3? false, :client-name nil}})
    (fn [_ in out] (resp/with-replies in out false false #(resp/rcmd* command)))))

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

(defn- sentinel-master-addr [sentinel-addr master-name]
  (when-let [[host port] (call-redis! sentinel-addr "SENTINEL" "get-master-addr-by-name" master-name)]
    [host (Long/parseLong port)]))

(defn- connected-addr [mgr]
  (conns/mgr-borrow! mgr
    (fn [conn _in _out]
      (let [{:keys [host port]} @conn]
        [host port]))))

(deftest ^:sentinel-integration _live-failover
  (let [master-name     (get-env! "CARMINE_TEST_SENTINEL_MASTER_NAME")
        sentinel-addrs  (sentinel-addrs)
        sentinel-addr   (first sentinel-addrs)
        surviving-sentinel-addr (or (second sentinel-addrs) sentinel-addr)
        initial-master  (env-addr "CARMINE_TEST_SENTINEL_INITIAL_MASTER_HOST"  "CARMINE_TEST_SENTINEL_INITIAL_MASTER_PORT")
        initial-replica (env-addr "CARMINE_TEST_SENTINEL_INITIAL_REPLICA_HOST" "CARMINE_TEST_SENTINEL_INITIAL_REPLICA_PORT")
        master-events_  (atom [])
        spec
        (sentinel/sentinel-spec {master-name sentinel-addrs}
          {:retry-delay-ms       50
           :resolve-timeout-ms 10000
           :update-replicas?    true
           :cbs {:on-changed-master #(swap! master-events_ conj %)}})

        sentinel-opts
        {:clear-timeout-ms 5000}

        server
        {:master-name master-name
         :sentinel-spec spec
         :sentinel-opts sentinel-opts}

        key (str "carmine:v4:sentinel-acceptance:" (java.util.UUID/randomUUID))]

    (with-open [mgr (car/conn-manager {:conn-opts {:server server}})]
      (let [queue (mq/queue mgr (str "sentinel-mq-v4-" (java.util.UUID/randomUUID))
                    {:lease-ms 120000, :retry-base-ms 0, :retry-max-ms 0
                     :durability {:replicas 1, :timeout-ms 5000}})
            claimed_ (promise)
            continue_ (promise)
            settled_ (promise)
            successor-handled_ (promise)
            handler-calls_ (atom 0)
            worker
            (mq/worker-create queue
              (fn [claim]
                (swap! handler-calls_ inc)
                (if (= (:msg claim) {:topology :sentinel})
                  (do (deliver claimed_ claim) @continue_)
                  (deliver successor-handled_ claim))
                (mq/outcome:ack))
              {:idle-max-ms 1000
               :on-event #(when (= (:event %) :settled) (deliver settled_ %))})
            soak (mq-soak/start! mgr "sentinel-short"
                   {:mid-count 4, :producer-count 2, :concurrency 2})]
        (try
          (testing "Initial master, preferred replica, and replicated MQ lease"
            [(is (= (connected-addr mgr) initial-master))
             (is (= (wcar mgr (resp/rcmd "SET" key "before-failover")) "OK"))

             (let [channel (str key ":pubsub")
                   message_ (promise)]
               (with-open [^java.io.Closeable listener
                           (car/pubsub-listener
                             {:conn-opts {:server server}
                              :handler-fn
                              #(when (= (:kind %) :message)
                                 (deliver message_ %))
                              :init-subs {:channels [channel]}})]
                 (is (= (wcar mgr (car/publish channel "sentinel-pubsub")) 1))
                 (is (= (:payload (deref message_ 5000 nil)) "sentinel-pubsub"))))

             (with-open [replica-mgr
                         (car/conn-manager-unpooled
                           {:conn-opts
                            {:server
                             (assoc server :sentinel-opts
                               (assoc sentinel-opts :prefer-read-replica? true))}})]

               [(is (= (connected-addr replica-mgr) initial-replica))
                (is (= (await! 10000 "replication to the preferred replica"
                         #(when (= (wcar replica-mgr (resp/rcmd "GET" key)) "before-failover") true))
                      true))])

             (is (mq/worker-start! worker))
             (is (= (await! 5000 "the MQ worker to enter its blocking wake"
                      #(when (pos? (long
                                     (get-in @worker [:stats :counts :wake-waits] 0)))
                         true))
                    true))
             (let [enqueued (mq/msg-enqueue! queue {:topology :sentinel} {:mid "mid"})]
               [(is (= (:action enqueued) :added))
                (is (true? (get-in enqueued [:durability :satisfied?])))])
             (let [{:keys [mid] :as claim}
                   (await! 5000 "the MQ worker to claim its message"
                     #(deref claimed_ 10 nil))
                   token (wcar mgr
                           (car/hget (get (queue-keys queue) :lease-tokens) mid))]
               [(is (truss/submap? claim
                      {:mid "mid", :msg {:topology :sentinel}}))
                (is (= (await! 10000 "the active MQ lease to reach the replica"
                         #(when (and
                                  (some? (call-redis! initial-replica "ZSCORE"
                                           (get (queue-keys queue) :leased) mid))
                                  (= (call-redis! initial-replica "HGET"
                                       (get (queue-keys queue) :lease-tokens) mid)
                                    token))
                            true))
                      true))
                (let [coalesced (mq/msg-enqueue! queue {:topology :successor}
                                  {:mid "mid", :on-duplicate :coalesce})]
                  [(is (= (:action coalesced) :coalesced-successor))
                   (is (true? (get-in coalesced [:durability :satisfied?])))])
                (is (= (await! 10000 "the coalesced MQ successor to reach the replica"
                         #(when (some? (call-redis! initial-replica "HGET"
                                        (get (queue-keys queue) :successor-payloads) mid))
                            true))
                      true))])])

          (let [counts-before (get-in @mgr [:stats :counts])
                channel (str key ":recovering-pubsub")
                post-message_ (promise)
                recovered_ (promise)]
            (with-open [^java.io.Closeable listener
                        (car/pubsub-listener
                          {:conn-opts {:server server}
                           :handler-fn
                           #(when (and (= (:kind %) :message)
                                       (= (:payload %) "after-failover"))
                              (deliver post-message_ %))
                           :init-subs {:channels [channel]}
                           :recovery {:check-ms 100, :backoff-ms 50}
                           :listener-fn
                           #(when (= (:kind %) :recovered)
                              (deliver recovered_ %))})]
              (testing "Master failure, promotion, and in-flight MQ settlement"
                (when (< 1 (count sentinel-addrs))
                  ;; Prove resolution and failover continue after the first
                  ;; configured Sentinel disappears.
                  (try
                    (call-redis! sentinel-addr "SHUTDOWN" "NOSAVE")
                    (catch Throwable _))
                  (is (= (await! 10000 "first Sentinel shutdown"
                           #(try
                              (call-redis! sentinel-addr "PING")
                              nil
                              (catch Throwable _ true)))
                        true))
                  (is (= (await! 10000 "surviving Sentinel availability"
                           #(when (= (call-redis! surviving-sentinel-addr "PING") "PONG") true))
                        true)))

                ;; Redis closes the socket without replying to SHUTDOWN.
                (try
                  (call-redis! initial-master "SHUTDOWN" "NOSAVE")
                  (catch Throwable _))

                [(is (= (await! 30000 "Sentinel promotion"
                          #(when (= (sentinel-master-addr surviving-sentinel-addr master-name) initial-replica)
                             initial-replica))
                       initial-replica))

                 (is (= (await! 15000 "a write through the promoted master"
                          #(when (= (wcar mgr (resp/rcmd "SET" key "after-failover")) "OK") true))
                       true))

                 (is (= (connected-addr mgr) initial-replica))
                 (is (= (wcar mgr (resp/rcmd "GET" key)) "after-failover"))

                 (is (= (:kind (deref recovered_ 15000 nil)) :recovered))
                 (is (= (wcar mgr (car/publish channel "after-failover")) 1))
                 (is (= (:payload (deref post-message_ 5000 nil)) "after-failover")
                   "The same supervised listener receives after promotion")

                 (is (= (:replies
                          (car/transact! mgr #(car/set (str key ":tx") "committed")))
                        ["OK"]))

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

                 (let [report (mq-soak/finish! soak 60000)]
                   [(is (:no-phantoms? report))
                    (is (:drained? report))
                    (is (:successors-exercised? report))
                    (is (:successors-bounded? report))])

                 (is (mq/worker-stop! worker))
                 (is (mq/worker-await-stopped! worker 5000))

                 (let [master-transitions (mapv #(get-in % [:changed :new]) @master-events_)
                       counts-after (get-in @mgr [:stats :counts])
                       increased?
                       (fn [counter]
                         (< (long (or (get counts-before counter) 0))
                            (long (or (get counts-after  counter) 0))))]
                   [(is (= master-transitions [initial-master initial-replica]))
                    (is (or (increased? :cleared) (increased? :created))
                      "The manager clears or replaces stale pooled connections")])])))

          (finally
            (deliver continue_ true)
            (mq-soak/close! soak)
            (.close ^java.io.Closeable worker)
            (mq/queue-clear!! queue)))))))
