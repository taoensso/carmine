(ns taoensso.carmine-v4.tests.sentinel
  "Live Redis Sentinel acceptance tests, selected explicitly by CI."
  (:require
   [clojure.test                :refer [deftest testing is]]
   [taoensso.carmine-v4         :as car :refer [wcar]]
   [taoensso.carmine-v4.conns   :as conns]
   [taoensso.carmine-v4.opts    :as opts]
   [taoensso.carmine-v4.resp    :as resp]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.message-queue :as mq]))

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
      (:addr @conn))))

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
          {:retry-backoff-ms       50
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

    (with-open [mgr (car/conn-manager {:conn-opts {:server server}})
                replica-mgr
                (car/conn-manager
                  {:conn-opts
                   {:server
                    (assoc server :sentinel-opts
                      (assoc sentinel-opts :prefer-read-replica? true))}})]
      (testing "Initial master and preferred replica"
        [(is (= (connected-addr mgr) initial-master))
         (is (= (wcar mgr (resp/rcmd "SET" key "before-failover")) "OK"))

         (let [channel (str key ":pubsub")
               message_ (promise)]
           (with-open [^java.io.Closeable listener
                       (car/pubsub-listener
                         {:conn-opts {:server server}
                          :on-message
                          #(when (= (:kind %) :message)
                             (deliver message_ %))
                          :init-subs {:channels [channel]}})]
             (is (= (wcar mgr (car/publish channel "sentinel-pubsub")) 1))
             (is (= (:payload (deref message_ 5000 nil)) "sentinel-pubsub"))))

         (is (= (connected-addr replica-mgr) initial-replica))
         (is (= (await! 10000 "replication to the preferred replica"
                  #(when (= (wcar replica-mgr (resp/rcmd "GET" key)) "before-failover") true))
               true))])

      (testing "Orderly failover: the old master stays up as a replica"
        ;; Unlike a crash, this leaves connections to the old master open
        (let [channel    (str key ":orderly-pubsub")
              message_   (promise)
              recovered_ (promise)
              held_      (promise)
              release_   (promise)]
          (with-open [mgr2 (car/conn-manager {:conn-opts {:server server}}) ; Shares the spec
                      ;; Own spec without caching, so that its return check is
                      ;; what finds the new master
                      mgr3
                      (car/conn-manager
                        {:pool-opts {:test-on-return? true}
                         :conn-opts
                         {:server
                          (assoc server :sentinel-spec
                            (sentinel/sentinel-spec {master-name sentinel-addrs}
                              {:retry-backoff-ms 50, :resolve-timeout-ms 10000
                               :resolve-cache-ttl-ms 0}))}})
                      listener
                      (car/pubsub-listener
                        {:conn-opts {:server server}
                         :on-message #(when (= (:kind %) :message) (deliver message_ %))
                         :init-subs {:channels [channel]}
                         :recovery-opts {:check-ms 100, :backoff-ms 50}
                         :on-event #(when (= (:kind %) :recovered) (deliver recovered_ %))})]

           (let [held ; A borrow across the failover, its reply already read
                 (future
                   (wcar mgr3
                     (let [reply (car/with-replies (resp/rcmd "SET" key "before-orderly"))]
                       (deliver held_ true)
                       @release_
                       (resp/local-echo reply))))]
            (try
            [(is (= (wcar mgr2 (resp/rcmd "SET" key "before-orderly")) "OK")
               "Pools a connection to the old master")
             (is (true? (deref held_ 5000 nil)))
             (is (= (call-redis! sentinel-addr "SENTINEL" "FAILOVER" master-name) "OK"))
             (is (= (await! 30000 "orderly promotion"
                      #(when (= (sentinel-master-addr sentinel-addr master-name) initial-replica) true))
                   true))
             ;; Sentinel can take ~20s to reconfigure the old master
             (is (= (await! 45000 "the old master to follow the new one"
                      #(let [info (call-redis! initial-master "INFO" "replication")]
                         (when (and (re-find #"role:slave" info) (re-find #"master_link_status:up" info)) true)))
                   true))

             (let [before (get-in @mgr3 [:stats :counts])
                   t0     (System/currentTimeMillis)
                   _      (deliver release_ true)
                   reply  (deref held 10000 ::timeout)
                   after  (get-in @mgr3 [:stats :counts])]
               [(is (= reply "OK") "A return check that finds the new master keeps the result")
                (is (< (- (System/currentTimeMillis) t0) 2500)
                  "And doesn't wait on its own conn (`:clear-timeout-ms` is 5000)")
                (is (> (:cleared after) (:cleared before)) "The check cleared the manager")
                (is (> (get-in after [:destroyed :total]) (get-in before [:destroyed :total])))
                (is (= (connected-addr mgr3) initial-replica))])

             (doseq [m [mgr mgr2]]
               [(is (= (await! 15000 "a write through the new master"
                         #(when (= (wcar m (resp/rcmd "SET" key "after-orderly")) "OK") true))
                      true))
                (is (= (connected-addr m) initial-replica))
                (is (every? #(= % "OK") (repeatedly 20 #(wcar m (resp/rcmd "SET" key "after-orderly"))))
                  "Connections to the old master aren't reused")])

             (is (= (await! 15000 "the replica-preferring manager to leave the new master"
                      #(when (= (connected-addr replica-mgr) initial-master) true))
                   true)
               "Its pooled connection was to the node that's now the master")
             (is (= (await! 10000 "replication to the new replica"
                      #(when (= (wcar replica-mgr (resp/rcmd "GET" key)) "after-orderly") true))
                   true))
             (is (every? #(= % initial-master) (repeatedly 20 #(connected-addr replica-mgr)))
               "It stays on the replica")

             (is (= (:new-addr (deref recovered_ 15000 nil)) initial-replica)
               "The listener follows the master, though its connection stayed open")
             (is (true? (car/pubsub-await-synced! listener {:timeout-ms 5000})))
             (is (= (wcar mgr (car/publish channel "after-orderly")) 1))
             (is (= (:payload (deref message_ 5000 nil)) "after-orderly"))]
            (finally (deliver release_ true)))))))

      ;; The roles are now swapped
      (let [[master replica] [initial-replica initial-master]
            counts-before (get-in @mgr [:stats :counts])
            channel (str key ":recovering-pubsub")
            post-message_ (promise)
            recovered_ (promise)
            mq-qname (str key ":mq")
            mq-handled_ (promise)]
        (with-open [^java.io.Closeable listener
                    (car/pubsub-listener
                      {:conn-opts {:server server}
                       :on-message
                       #(when (and (= (:kind %) :message)
                                   (= (:payload %) "after-failover"))
                          (deliver post-message_ %))
                       :init-subs {:channels [channel]}
                       :recovery-opts {:check-ms 100, :backoff-ms 50}
                       :on-event
                       #(when (= (:kind %) :recovered)
                          (deliver recovered_ %))})]
          (with-open [^java.io.Closeable mq-worker
                      ;; Idle in BRPOP across the failover
                      (mq/worker mgr mq-qname
                        {:handler (fn [{:keys [message]}] (deliver mq-handled_ message) {:status :success})
                         :throttle-ms 10, :eoq-backoff-ms 500})]
            (testing "Master failure and promotion"
              (is (await! 10000 "MQ worker idle sleep"
                    #(when (pos? (long (get-in @mq-worker [:stats :counts :sleep/end-of-circle] 0))) true)))

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
                (call-redis! master "SHUTDOWN" "NOSAVE")
                (catch Throwable _))

              [(is (= (await! 30000 "Sentinel promotion"
                        #(when (= (sentinel-master-addr surviving-sentinel-addr master-name) replica)
                           replica))
                     replica))

               (is (= (await! 15000 "a write through the promoted master"
                        #(when (= (wcar mgr (resp/rcmd "SET" key "after-failover")) "OK") true))
                     true))

               (is (= (connected-addr mgr) replica))
               (is (= (wcar mgr (resp/rcmd "GET" key)) "after-failover"))

               (is (= (await! 15000 "a read with no replica left"
                        #(when (= (wcar replica-mgr (resp/rcmd "GET" key)) "after-failover") true))
                     true))
               (is (= (connected-addr replica-mgr) replica)
                 "A replica-preferring manager reads from the master when there's no replica")

               (is (:running? @mq-worker) "MQ worker survives failover")
               (is (:success? (wcar mgr (mq/enqueue mq-qname :after-failover))))
               (is (= (deref mq-handled_ 15000 :timeout) :after-failover)
                 "MQ worker handles messages via the promoted master")

               (is (= (:kind (deref recovered_ 15000 nil)) :recovered))
               (is (= (wcar mgr (car/publish channel "after-failover")) 1))
               (is (= (:payload (deref post-message_ 5000 nil)) "after-failover")
                 "The same supervised listener receives after promotion")

               (is (= (:replies
                        (car/transact! mgr #(car/set (str key ":tx") "committed")))
                      ["OK"]))

               (let [master-transitions (mapv #(get-in % [:changed :new]) @master-events_)
                     counts-after (get-in @mgr [:stats :counts])
                     increased?
                     (fn [counter]
                       (< (long (or (get counts-before counter) 0))
                          (long (or (get counts-after  counter) 0))))]
                 [(is (= master-transitions [initial-master initial-replica initial-master]))
                  (is (or (increased? :cleared) (increased? :created))
                    "The manager clears or replaces stale pooled connections")])])))))))
