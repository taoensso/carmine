(ns taoensso.carmine-v4.tests.cluster-unit
  "Deterministic Carmine v4 Cluster topology, routing, and executor tests."
  (:require
   [clojure.test        :refer [deftest is]]
   [taoensso.encore     :as enc]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine-v4          :as car  :refer [wcar]]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.utils    :as utils]
   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.cluster  :as cluster]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tc+ "Parsed test conn-opts" support/parsed-test-conn-opts)

(support/use-clean-redis-fixture!)

;;;; Cluster

(deftest _cluster-spec
  (let [spec (cluster/cluster-spec
               [["seed-a.redis" "7000"]
                ["seed-b.redis" 7001]
                ["seed-a.redis" 7000]])
        state @spec]
    [(is (cluster/cluster-spec? spec))
     (is (car/cluster-spec? spec))
     (is (= (:seed-addrs state)
           [["seed-a.redis" 7000] ["seed-b.redis" 7001]]))
     (is (= (:known-addrs state) (:seed-addrs state)))
     (is (= (get-in state [:cluster-spec-opts :refresh-timeout-ms]) 2000))
     (is (nil? (:topology state)))
     (is (= (set (keys state))
           #{:cluster-spec-opts :seed-addrs :known-addrs :topology :topology-stale?
             :last-success-addr :stats}))])

  [(let [error (truss/throws (cluster/cluster-spec []))]
     [(is (= (:eid (ex-data error)) :carmine.cluster/invalid-spec))
      (is (= (:eid (ex-data (ex-cause error))) :carmine.cluster/no-seed-addrs))])
   (is (->> (cluster/cluster-spec [["seed.redis" "not-a-port"]])
         (throws? :ex-info {:eid :carmine.cluster/invalid-spec})))
   (is (->> (cluster/cluster-spec [["seed.redis" 7000]]
              {:topology-source :cluster-slots})
         (throws? :ex-info {:eid :carmine.cluster/invalid-spec})))
   (is (->> (cluster/cluster-spec [["seed.redis" 7000]]
              {:refresh-timeout-ms -1})
         (throws? :ex-info {:eid :carmine.cluster/invalid-spec})))
   (is (not (cluster/cluster-spec? {})))])

(deftest _cluster-conn-guard
  (let [spec (cluster/cluster-spec [["seed.redis" 7000]])
        conn-opts
        (opts/parse-conn-opts :redis
          {:server {:cluster-spec spec}})]
    (is (->> (#'conns/new-conn conn-opts)
          (throws? :ex-info {:eid :carmine.cluster/manager-required})))))

(deftest _cluster-conn-manager
  (let [spec      (cluster/cluster-spec [["seed.redis" 7000]])
        conn-opts {:server {:cluster-spec spec}}
        expected  {:eid :carmine.cluster/manager-required}]
    [(is (->> (conns/conn-manager-unpooled {:conn-opts conn-opts})
           (throws? :ex-info expected)))
     (is (->> (conns/conn-manager-pooled {:conn-opts conn-opts})
           (throws? :ex-info expected)))
     (with-open [mgr (car/conn-manager
                       {:conn-opts conn-opts
                        :pool-opts {:test-while-idle? false}})]
       (is (instance? taoensso.carmine_v4.conns.ConnManagerClustered mgr)
         "`conn-manager` returns a Cluster manager for a Cluster server"))
     (with-open [mgr (conns/conn-manager-clustered
                       {:conn-opts conn-opts
                        :pool-opts {:test-while-idle? false}})]
       [(is (car/conn-manager-open? mgr))
        (support/assert-manager-stats-schema! :clustered mgr)
        (is (truss/submap? (conns/mgr-cluster-server mgr)
              {:cluster-spec spec}))
        (is (= (conns/mgr-borrow-addr! mgr ["127.0.0.1" 6379]
                 (fn [_ in out]
                   (resp/with-replies in out false false resp/ping)))
              "PONG"))
        (is (= (get-in @mgr [:stats :counts :created]) 1))
        (is (= (:nodes (car/conn-manager-stats mgr))
              [{:addr ["127.0.0.1" 6379], :active 0, :idle 1}]))
        (is (= (car/conn-manager-clear! mgr {:timeout-ms 1000})
              true))])
     (with-open [mgr (conns/conn-manager-clustered
                       {:conn-opts
                        {:server {:cluster-spec spec}
                         :socket-opts {:connect-timeout-ms 50}}
                        :pool-opts {:test-while-idle? false}})]
       (let [failure (truss/throws
                       (conns/mgr-borrow-addr! mgr ["127.0.0.1" 1]
                         (fn [& _])))]
         [(is (= (:eid (ex-data failure)) :carmine.conns/borrow-conn-error))
          (is (not= (:eid (ex-data (ex-cause failure)))
                :carmine.conns/borrow-conn-error))
          (is (= (get-in @mgr [:stats :counts :failed]) 1))]))]))

(deftest _cluster-closed-pool-keeps-no-late-replacement
  (let [spec        (cluster/cluster-spec [["seed.redis" 7000]])
        addr        ["127.0.0.1" 6379]
        borrowed    (promise)
        release     (promise)
        gate-armed_ (atom false)
        creating    (promise)
        resume      (promise)
        original-new-conn @#'conns/new-conn
        mgr
        (conns/conn-manager-clustered
          {:conn-opts {:server {:cluster-spec spec}}
           :pool-opts {:max-total 1, :max-total-per-key 1, :max-wait-ms 2000
                       :test-while-idle? false}})]
    (try
      (with-redefs-fn
        {#'conns/new-conn
         (fn [& args]
           (when @gate-armed_ (deliver creating true) @resume)
           (apply original-new-conn args))}
        (fn []
          (let [failing
                (future
                  (truss/throws
                    (conns/mgr-borrow-addr! mgr addr
                      (fn [& _]
                        (deliver borrowed true)
                        @release
                        (throw (ex-info "Request failed" {}))))))
                _ (deref borrowed 2000 ::timeout)
                _ (reset! gate-armed_ true)
                waiter (future (truss/throws (conns/mgr-borrow-addr! mgr addr (fn [& _]))))]
            (support/await-pred! 2000 "a borrower to wait"
              #(pos? (get-in @mgr [:stats :connections :waiting])))
            (deliver release true) ; Invalidated, replacement creation blocks
            [(is (= (deref creating 2000 ::timeout) true))
             (is (= (:eid (ex-data (deref waiter 4000 nil))) :carmine.conns/borrow-conn-error)
               "The waiter gives up")
             (is (true? (car/conn-manager-close! mgr {:timeout-ms 0})))
             (do
               (deliver resume true)
               (is (= (ex-message (deref failing 2000 nil)) "Request failed")
                 "The request's own error stands"))
             (is (zero? (get-in @mgr [:stats :connections :idle]))
               "A replacement created across close doesn't stay idle")])))
      (finally
        (deliver release true)
        (deliver resume true)
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

(deftest _cluster-conn-manager-active-clear
  (let [spec         (cluster/cluster-spec [["seed.redis" 7000]])
        borrowed     (promise)
        release      (promise)
        close-events_ (atom [])
        mgr
        (conns/conn-manager-clustered
          {:conn-opts
           {:server {:cluster-spec spec}
            :cbs {:on-conn-close #(swap! close-events_ conj %)}}
           :pool-opts {:test-while-idle? false}})]
    (try
      (let [borrow-f
            (future
              (conns/mgr-borrow-addr! mgr ["127.0.0.1" 6379]
                (fn [_ _ _]
                  (deliver borrowed :borrowed)
                  @release
                  :done)))
            _ (is (= (deref borrowed 2000 ::timeout) :borrowed))
            before (get-in @mgr [:stats :counts])
            clear-f (future (car/conn-manager-clear! mgr {:timeout-ms 2000}))]
        (support/await-pred! 2000 "the clear to start"
          #(pos? (get-in @mgr [:stats :counts :cleared])))
        (deliver release :release)
        [(is (= (deref borrow-f 2000 ::timeout) :done))
         (is (= (deref clear-f 2000 ::timeout)
               true))
         (is (zero? (get-in @mgr [:stats :connections :idle])))
         (is (= (get-in (first @close-events_) [:data :via]) 'mgr-clear!))
         (is (= (conns/mgr-borrow-addr! mgr ["127.0.0.1" 6379]
                  (fn [_ in out]
                    (resp/with-replies in out false false resp/ping)))
                "PONG"))
         (is (> (get-in @mgr [:stats :counts :created]) (:created before)))])
      (finally
        (deliver release :release)
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

(deftest _cluster-conn-manager-exhaustion
  (let [spec    (cluster/cluster-spec [["seed.redis" 7000]])
        release (promise)
        first-borrowed (promise)]
    (with-open [mgr
                (conns/conn-manager-clustered
                  {:conn-opts {:server {:cluster-spec spec}}
                   :pool-opts
                   {:test-while-idle? false
                    :max-total 1, :max-total-per-key 1, :max-wait-ms 15000}})]
      (let [borrow-f1
            (future
              (conns/mgr-borrow-addr! mgr ["127.0.0.1" 6379]
                (fn [& _]
                  (deliver first-borrowed :borrowed)
                  @release
                  :first)))
            _ (is (= (deref first-borrowed 5000 ::timeout) :borrowed))
            borrow-f2
            (future
              (conns/mgr-borrow-addr! mgr ["127.0.0.1" 6379]
                (fn [& _] :second)))]
        (support/await-pred! 5000 "the second borrower to reach pool exhaustion"
          #(pos? (get-in @mgr [:stats :connections :waiting])))
        (deliver release :release)
        [(is (= (deref borrow-f1 5000 ::timeout) :first)
           "A completed borrower can return while another waits at exhaustion")
         (is (= (deref borrow-f2 5000 ::timeout) :second))]))))

(deftest _cluster-conn-manager-cross-clear-acquisition
  (let [spec          (cluster/cluster-spec [["seed.redis" 7000]])
        creating      (promise)
        release       (promise)
        calls_        (atom 0)
        created_      (atom [])
        used_         (atom [])
        real-new-conn @#'conns/new-conn]
    (with-redefs-fn
      {#'conns/new-conn
       (fn
         ([conn-opts]
          (let [call (swap! calls_ inc)]
            (when (= call 1)
              (deliver creating true)
              @release)
            (let [conn (real-new-conn
                         (assoc conn-opts :server ["127.0.0.1" 6379]))]
              (swap! created_ conj conn)
              conn)))
         ([conn-opts t0 master-name host port]
          (real-new-conn conn-opts t0 master-name host port)))}
      (fn []
        (with-open [mgr
                    (conns/conn-manager-clustered
                      {:conn-opts {:server {:cluster-spec spec}}
                       :pool-opts
                       {:test-while-idle? false
                        :max-total 1, :max-total-per-key 1
                        :max-wait-ms 5000}})]
          (let [borrow-f
                (future
                  (conns/mgr-borrow-addr! mgr ["node.redis" 7000]
                    (fn [conn _ _]
                      (swap! used_ conj conn)
                      :used)))]
            (try
              (is (= (deref creating 5000 ::timeout) true))
              (is (= (car/conn-manager-clear! mgr {:timeout-ms 5000})
                    true))
              (finally (deliver release true)))
            [(is (= (deref borrow-f 10000 ::timeout) :used))
             (is (= (count @created_) 2))
             (is (identical? (first @used_) (second @created_))
               "Only the post-clear connection enters user code")
             (is (false? (:open? @(first @created_)))
               "The acquisition spanning the clear is invalidated")]))))))

(deftest ^:v4-canary
  _cluster-conn-manager-clear-stress
  (let [n-workers       12
        n-iterations    30
        n-clears        12
        ready           (java.util.concurrent.CountDownLatch. n-workers)
        start           (java.util.concurrent.CountDownLatch. 1)
        completed-clears_ (atom 0)
        spec            (cluster/cluster-spec [["seed.redis" 7000]])
        node-addrs      [["node-a.redis" 7000] ["node-b.redis" 7001]]
        real-new-conn   @#'conns/new-conn]
    (with-redefs-fn
      {#'conns/new-conn
       (fn
         ([conn-opts]
          ;; Exercise distinct keyed-pool addresses against the one Redis server
          ;; configured for the test suite, including its protocol/auth options.
          (real-new-conn (assoc conn-opts :server (:server tc+) :init (:init tc+))))
         ([conn-opts t0 master-name host port]
          (real-new-conn conn-opts t0 master-name host port)))}
      (fn []
        (with-open [mgr
                    (conns/conn-manager-clustered
                      {:conn-opts {:server {:cluster-spec spec}}
                       :pool-opts
                       {:test-while-idle? false
                        :max-total 3, :max-total-per-key 1
                        :max-wait-ms 5000}})]
          (let [workers
                (mapv
                  (fn [worker-idx]
                    (future
                      (.countDown ready)
                      (.await start)
                      (dotimes [iteration n-iterations]
                        (let [addr (nth node-addrs (mod (+ worker-idx iteration) 2))]
                          (conns/mgr-borrow-addr! mgr addr
                            (fn [_conn in out]
                              (when (zero? (mod (+ worker-idx iteration) 7))
                                (Thread/yield))
                              (is (= (resp/with-replies in out false false resp/ping) "PONG"))))))
                      n-iterations))
                  (range n-workers))
                _ (is (.await ready 5 java.util.concurrent.TimeUnit/SECONDS))
                _ (.countDown start)
                clear-f
                (future
                  (loop [remaining n-clears, results []]
                    (if (zero? remaining)
                      results
                      (do
                        (Thread/sleep 2)
                        (let [result (car/conn-manager-clear! mgr {:timeout-ms 5000})]
                          (swap! completed-clears_ inc)
                          (recur (dec remaining) (conj results result)))))))
                clear-results (deref clear-f 30000 ::timeout)
                worker-results (mapv #(deref % 30000 ::timeout) workers)]
            [(is (not= clear-results ::timeout) "Bounded clears never deadlock")
             (is (= clear-results
                   (vec (repeat n-clears true))))
             (is (not-any? #{::timeout} worker-results) "Workers finish after clears cease")
             (is (= worker-results (vec (repeat n-workers n-iterations))))
             (is (= @completed-clears_ n-clears))
             (is (car/conn-manager-open? mgr))
             (is (zero? (get-in @mgr [:stats :connections :active])))
             (is (zero? (get-in @mgr [:stats :connections :waiting])))
             (is (<= n-clears (get-in @mgr [:stats :counts :cleared])))]))))))

(defn- test-slots-reply
  ([] (test-slots-reply "master.redis"))
  ([master-host]
   [[0 16383 [master-host 7000 "master"] ["replica.redis" 7001 "replica"]]]))

(deftest _cluster-refresh-uses-manager-opts
  (let [spec  (cluster/cluster-spec [["seed.redis" 7000]])
        seen_ (atom nil)
        fail?_ (atom true)]
    (with-open [mgr
                (car/conn-manager
                  {:conn-opts
                   {:server      {:cluster-spec spec}
                    :init        {:auth {:username "mgr-user", :password "mgr-pass"}}
                    :socket-opts {:ssl true}}
                   :pool-opts {:test-while-idle? false}})]
      (with-redefs-fn
        {#'cluster/raw-topology-reply!
         (fn [conn-opts _addr]
           (reset! seen_
             [(get-in conn-opts [:init :auth :username])
              (get-in conn-opts [:socket-opts :ssl])])
           (if @fail?_
             (throw (ex-info "Node auth failed" {}))
             (test-slots-reply)))}
        (fn []
          [(let [err (truss/throws (#'cluster/refresh-for-execution! mgr spec))]
             [(is (= (:eid (ex-data err)) :carmine.cluster/refresh-error))
              (is (= (ex-message (ex-cause err)) "Node auth failed")
                "The error connecting to the node is kept as cause")])
           (is (= @seen_ ["mgr-user" true])
             "Topology is fetched with the manager's auth and TLS options")
           (do
             (reset! fail?_ false)
             (#'cluster/refresh-for-execution! mgr spec)
             (is (= (:known-addrs @spec)
                   [["master.redis" 7000] ["replica.redis" 7001] ["seed.redis" 7000]])
               "Nodes are known by their TLS addresses when the manager uses TLS"))])))))

(deftest _cluster-refresh-success
  (let [success-events_ (atom [])
        changed-events_ (atom [])
        master-host_     (atom "master.redis")
        calls_           (atom [])
        spec             (cluster/cluster-spec [["seed.redis" 7000]]
                           {:cbs
                            {:on-refresh-success  #(swap! success-events_ conj %)
                             :on-changed-topology #(swap! changed-events_ conj %)}})
        refresh
        (fn []
          (with-redefs-fn
            {#'cluster/raw-topology-reply!
             (fn [conn-opts addr]
               (swap! calls_ conj addr)
               (test-slots-reply @master-host_))}
            #(cluster/refresh-topology! spec)))]

    (let [topology-1 (refresh)]
      [(is (:complete? topology-1))
       (is (identical? topology-1 (cluster/cached-topology spec)))
       (is (= (:known-addrs @spec)
             [["master.redis" 7000]
              ["replica.redis" 7001]
              ["seed.redis" 7000]]))
       (is (= @calls_ [["seed.redis" 7000]]))])

    (let [_ (refresh)
          stats (:stats @spec)]
      [(is (= (count @success-events_) 2))
       (is (= (count @changed-events_) 1)
         "An unchanged topology does not report a routing change")
       (is (= (mapv :cbid @success-events_)
             [:on-refresh-success :on-refresh-success]))
       (is (= (:cbid (first @changed-events_)) :on-changed-topology))
       (is (= (:refresh-stats stats)
             {:n-attempts 2, :n-successes 2}))
       (is (= (get-in stats [:node-stats ["seed.redis" 7000]])
             {:n-attempts 2, :n-successes 2}))])

    (reset! master-host_ "moved.redis")
    (refresh)
    (let [{:keys [old new]} (:changed (second @changed-events_))]
      [(is (= (count @success-events_) 3))
       (is (= (count @changed-events_) 2))
       (is (= (get-in old [:shards 0 :master :addr]) ["master.redis" 7000]))
       (is (= (get-in new [:shards 0 :master :addr]) ["moved.redis" 7000]))
       (is (= new (cluster/cluster-cached-topology spec))
         "Callbacks get the public topology projection")
       (is (= (:topology (peek @success-events_))
              (cluster/cluster-cached-topology spec)))])))

(deftest _cluster-refresh-candidates
  (let [calls_ (atom [])
        spec   (cluster/cluster-spec [["stale.redis" 7000] ["good.redis" 7001]])
        topology
        (with-redefs-fn
          {#'cluster/raw-topology-reply!
           (fn [_ addr]
             (swap! calls_ conj addr)
             (if (= addr ["stale.redis" 7000])
               []
               [[0 16383 ["good.redis" 7001 "master"]]]))}
          #(cluster/refresh-topology! spec))
        stats (:stats @spec)]

    [(is (:complete? topology))
     (is (= @calls_ [["stale.redis" 7000] ["good.redis" 7001]]))
     (is (= (:refresh-stats stats) {:n-attempts 2, :n-successes 1}))
     (is (= (get-in stats [:node-stats ["stale.redis" 7000] :n-errors]) 1))])

  (let [calls_ (atom [])
        spec   (cluster/cluster-spec [["loading.redis" 7000] ["good.redis" 7001]])]
    (with-redefs-fn
      {#'cluster/raw-topology-reply!
       (fn [_ addr]
         (swap! calls_ conj addr)
         (if (= addr ["loading.redis" 7000])
           (support/redis-error-reply "LOADING Redis is loading the dataset in memory")
           (test-slots-reply)))}
      #(cluster/refresh-topology! spec))
    (is (= @calls_ [["loading.redis" 7000] ["good.redis" 7001]])
      "An error reply advances to the next candidate")))

(deftest _cluster-refresh-learned-candidate
  (let [calls_       (atom [])
        mode_        (atom :initial)
        seed-addr    ["seed.redis" 7000]
        learned-addr ["master.redis" 7000]
        spec         (cluster/cluster-spec [seed-addr])
        raw-fn
        (fn [_ addr]
          (swap! calls_ conj addr)
          (when (and (= @mode_ :learned-success) (= addr seed-addr))
            (throw (Exception. "Seed unavailable")))
          (test-slots-reply))]
    (with-redefs-fn {#'cluster/raw-topology-reply! raw-fn}
      #(cluster/refresh-topology! spec))

    (reset! calls_ [])
    (reset! mode_ :learned-success)
    (with-redefs-fn {#'cluster/raw-topology-reply! raw-fn}
      #(cluster/refresh-topology! spec))
    (is (= @calls_
          [seed-addr learned-addr]))

    (reset! calls_ [])
    (reset! mode_ :learned-first)
    (with-redefs-fn {#'cluster/raw-topology-reply! raw-fn}
      #(cluster/refresh-topology! spec))
    [(is (= (first @calls_) learned-addr))
     (is (= (:last-success-addr @spec) learned-addr))]))

(deftest _cluster-refresh-failure
  (let [events_ (atom [])
        spec    (cluster/cluster-spec [["seed-a.redis" 7000] ["seed-b.redis" 7001]]
                  {:cbs {:on-refresh-error #(swap! events_ conj %)}})
        calls_ (atom 0)
        fail!  (fn [& _] (swap! calls_ inc) (throw (Exception. "Unavailable")))
        error
        (with-redefs-fn {#'cluster/raw-topology-reply! fail!}
          #(try
             (cluster/refresh-topology! spec)
             (catch Throwable t t)))]

    [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-error))
     (is (= (:n-attempts (ex-data error)) 2))
     (is (= (mapv :addr (:attempts (ex-data error)))
           [["seed-a.redis" 7000] ["seed-b.redis" 7001]]))
     (is (= @calls_ 2))
     (is (= (:cbid (first @events_)) :on-refresh-error))
     (is (false? (:stale-served? (first @events_))))
     (is (nil? (cluster/cached-topology spec)))])

  (let [events_ (atom [])
        spec    (cluster/cluster-spec [["seed.redis" 7000]]
                  {:cbs {:on-refresh-error #(swap! events_ conj %)}})
        topology
        (with-redefs-fn
          {#'cluster/raw-topology-reply! (fn [& _] (test-slots-reply))}
          #(cluster/refresh-topology! spec))
        stale
        (with-redefs-fn
          {#'cluster/raw-topology-reply! (fn [& _] (throw (Exception. "Unavailable")))}
          #(cluster/refresh-topology! spec))]
    [(is (identical? topology stale)
       "A failed refresh returns the last accepted topology")
     (is (identical? topology (cluster/cached-topology spec)))
     (is (= (count @events_) 1))
     (is (true? (:stale-served? (first @events_))))
     (is (= (get-in @spec [:stats :refresh-stats])
           {:n-attempts 4, :n-successes 1, :n-errors 1}))]))

(deftest _cluster-refresh-deadline
  (let [calls_ (atom 0)
        spec   (cluster/cluster-spec [["seed.redis" 7000]]
                 {:refresh-timeout-ms 1})
        error
        (with-redefs-fn
          {#'utils/remaining-timeout-ms (constantly nil)
           #'cluster/raw-topology-reply!
           (fn [& _] (swap! calls_ inc) (throw (Exception. "Unexpected I/O")))}
          #(try
             (cluster/refresh-topology! spec)
             (catch Throwable t t)))]
    [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-error))
     (is (true? (:timed-out? (ex-data error))))
     (is (zero? (:n-attempts (ex-data error))))
     (is (zero? @calls_) "An expired deadline performs no network I/O")])

  (let [calls_     (atom [])
        remaining_ (atom [15 15 15 nil])
        spec
        (cluster/cluster-spec
          [["seed-a.redis" 7000] ["seed-b.redis" 7001]]
          {:refresh-timeout-ms 1000})
        error
        (with-redefs-fn
          {#'utils/remaining-timeout-ms
           (fn [_]
             (let [remaining (first @remaining_)]
               (swap! remaining_ next)
               remaining))
           #'cluster/raw-topology-reply!
           (fn [conn-opts addr]
             (swap! calls_ conj [addr (:socket-opts conn-opts)])
             (throw (Exception. "Unavailable")))}
          #(try
             (cluster/refresh-topology! spec)
             (catch Throwable t t)))]
    [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-error))
     (is (true? (:timed-out? (ex-data error))))
     (is (= (mapv first @calls_) [["seed-a.redis" 7000]])
       "An exhausted sweep never advances to the next Cluster candidate")
     (is (truss/submap? (second (first @calls_))
           {:connect-timeout-ms 15
            :read-timeout-ms    15
            :ready-timeout-ms   15}))])

  (let [calls_  (atom [])
        silent_ (atom #{"a.redis" "b.redis"})
        spec
        (cluster/cluster-spec
          [["a.redis" 7000] ["b.redis" 7001]]
          {:refresh-timeout-ms 400})
        refresh!
        (fn []
          (with-redefs-fn
            {#'cluster/raw-topology-reply!
             (fn [_ addr]
               (swap! calls_ conj (first addr))
               (when (@silent_ (first addr)) (Thread/sleep 4000))
               (test-slots-reply))}
            #(try
               (cluster/refresh-topology! spec)
               (catch Throwable t t))))]
    [(is (true? (:timed-out? (ex-data (refresh!)))))
     (is (true? (:timed-out? (ex-data (refresh!)))))
     (do (reset! silent_ #{"a.redis"}) (is (true? (:timed-out? (ex-data (refresh!))))))
     (is (:complete? (refresh!)))
     (is (= @calls_ ["a.redis" "b.redis" "a.redis" "b.redis"])
       "Successive sweeps rotate past candidates that exhausted the deadline")])

  (let [events_ (atom [])
        spec
        (cluster/cluster-spec [["seed.redis" 7000]]
          {:cbs {:on-refresh-error #(swap! events_ conj %)}})
        topology
        (with-redefs-fn
          {#'cluster/raw-topology-reply! (fn [& _] (test-slots-reply))}
          #(cluster/refresh-topology! spec))
        stale
        (with-redefs-fn
          {#'utils/remaining-timeout-ms (constantly nil)}
          #(cluster/refresh-topology! spec))]
    [(is (identical? topology stale))
     (is (true? (:stale-served? (first @events_))))
     (is (true? (:timed-out? (first @events_))))])

  (let [spec (cluster/cluster-spec [["slow.redis" 7000]]
               {:refresh-timeout-ms 25})
        t0   (System/nanoTime)
        error
        (with-redefs-fn
          {#'cluster/raw-topology-reply!
           (fn [& _]
             (Thread/sleep 2000)
             (test-slots-reply))}
          #(try
             (cluster/refresh-topology! spec)
             (catch Throwable t t)))
        elapsed-ms (quot (- (System/nanoTime) t0) 1000000)]
    [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-error))
     (is (true? (:timed-out? (ex-data error))))
     (is (< elapsed-ms 1000)
       "The caller returns at the aggregate deadline even if discovery remains blocked")])

  (let [remaining_ (atom [1000 1000 1000])
        spec       (cluster/cluster-spec [["seed.redis" 7000]])
        topology
        (with-redefs-fn
          {#'utils/remaining-timeout-ms
           (fn [_]
             (let [remaining (first @remaining_)]
               (swap! remaining_ next)
               remaining))
           #'cluster/raw-topology-reply! (fn [& _] (test-slots-reply))}
          #(cluster/refresh-topology! spec))]
    [(is (:complete? topology)
       "The deadline bounds discovery I/O: a fully-received topology is kept even when the deadline expires after its fetch")
     (is (identical? topology (cluster/cached-topology spec)))]))

(defn- refresh-parked?
  "Returns true once every thread is waiting inside `refresh-topology!`."
  [threads]
  (let [waiting?
        (fn [^Thread thread]
          (and
            (contains?
              #{java.lang.Thread$State/BLOCKED
                java.lang.Thread$State/WAITING
                java.lang.Thread$State/TIMED_WAITING}
              (.getState thread))
            (some
              #(re-find #"taoensso\.carmine_v4\.cluster\$refresh_topology"
                 (.getClassName ^StackTraceElement %))
              (.getStackTrace thread))))]
    (try
      (support/await-pred! 5000 "parked refreshes" #(every? waiting? threads))
      (catch clojure.lang.ExceptionInfo _ false))))

(deftest _cluster-refresh-single-flight
  (let [n       8
        ready   (java.util.concurrent.CountDownLatch. n)
        start   (java.util.concurrent.CountDownLatch. 1)
        entered (promise)
        release (promise)
        calls_   (atom 0)
        threads_ (atom [])
        spec     (cluster/cluster-spec [["seed.redis" 7000]])
        raw-fn
        (fn [& _]
          (swap! calls_ inc)
          (deliver entered true)
          @release
          (test-slots-reply))
        tasks
        (mapv
          (fn [_]
            (future
              (swap! threads_ conj (Thread/currentThread))
              (.countDown ready)
              (.await start)
              (cluster/refresh-topology! spec)))
          (range n))]

    (is (.await ready 5 java.util.concurrent.TimeUnit/SECONDS))
    (let [[entered? parked? results]
          (with-redefs-fn {#'cluster/raw-topology-reply! raw-fn}
            (fn []
               (.countDown start)
               (let [entered? (deref entered 5000 false)
                     parked?  (refresh-parked? @threads_)]
                 (deliver release true)
                 [entered?
                  parked?
                  (mapv (fn [task] (deref task 5000 ::timeout)) tasks)])))]
      [(is entered?)
       (is parked? "Every task entered and parked in the refresh path")
       (is (not-any? #{::timeout} results))
       (is (every? #(identical? (first results) %) (next results)))
       (is (= @calls_ 1))])

    (with-redefs-fn {#'cluster/raw-topology-reply! raw-fn}
      #(cluster/refresh-topology! spec))
    (is (= @calls_ 2)
      "A completed single-flight slot permits a later refresh"))

  (let [calls_ (atom 0)
        spec   (cluster/cluster-spec [["seed.redis" 7000]])]
    (dotimes [_ 2]
      (with-redefs-fn
        {#'cluster/raw-topology-reply!
         (fn [& _] (swap! calls_ inc) (throw (Exception. "Unavailable")))}
        #(try (cluster/refresh-topology! spec) (catch Throwable _))))
    (is (= @calls_ 2)
      "A failed single-flight sweep does not wedge later refreshes")))

(deftest _cluster-refresh-failure-single-flight
  (let [n            8
        ready        (java.util.concurrent.CountDownLatch. n)
        start        (java.util.concurrent.CountDownLatch. 1)
        entered      (promise)
        release      (promise)
        calls_       (atom 0)
        threads_     (atom [])
        spec         (cluster/cluster-spec
                       [["seed-a.redis" 7000] ["seed-b.redis" 7001]])
        tasks
        (mapv
          (fn [_]
            (future
              (swap! threads_ conj (Thread/currentThread))
              (.countDown ready)
              (.await start)
              (try
                (cluster/refresh-topology! spec)
                (catch Throwable t t))))
          (range n))]
    (is (.await ready 5 java.util.concurrent.TimeUnit/SECONDS))
    (let [results
          (with-redefs-fn
            {#'cluster/raw-topology-reply!
             (fn [& _]
               (let [call (swap! calls_ inc)]
                 (when (= call 1)
                   (deliver entered true)
                   @release)
                 (throw (Exception. "Transient discovery failure"))))}
            (fn []
              (.countDown start)
              (is (= (deref entered 5000 ::timeout) true))
              (is (refresh-parked? @threads_)
                "Every task joined the shared failing refresh")
              (deliver release true)
              (mapv #(deref % 10000 ::timeout) tasks)))]
      [(is (not-any? #{::timeout} results))
       (is (every? #(= (:eid (ex-data %)) :carmine.cluster/refresh-error) results))
       (is (= @calls_ 2)
         "Concurrent joiners share one failed sweep across both seed addresses")])

    (with-redefs-fn
      {#'cluster/raw-topology-reply! (fn [& _] (test-slots-reply))}
      #(is (:complete? (cluster/refresh-topology! spec))))
    (is (= (get-in @spec [:stats :refresh-stats :n-successes]) 1)
      "A failed shared refresh leaves the next refresh usable")))

(deftest _cluster-refresh-context-and-reentrancy
  (let [observed-ctx_ (atom ::unset)
        spec          (cluster/cluster-spec [["seed.redis" 7000]])]
    (with-redefs-fn
      {#'cluster/raw-topology-reply!
       (fn [& _]
         (reset! observed-ctx_ resp/*ctx*)
         (test-slots-reply))}
      #(binding [resp/*ctx* ::ambient]
         (cluster/refresh-topology! spec)))
    (is (nil? @observed-ctx_)
      "Discovery never inherits an ambient request context"))

  (let [callback-errors_ (atom [])
        callback-ids_    (atom [])
        calls_           (atom 0)
        spec_            (atom nil)
        spec             (cluster/cluster-spec [["seed.redis" 7000]]
                           {:cbs
                            {:on-refresh-success
                             (fn [_]
                               (cluster/refresh-topology! @spec_))}})]
    (reset! spec_ spec)
    (with-redefs-fn
      {#'cluster/raw-topology-reply!
       (fn [& _] (swap! calls_ inc) (test-slots-reply))
       #'utils/report-callback-error!
       (fn [_ cbid t]
         (swap! callback-ids_ conj cbid)
         (swap! callback-errors_ conj t))}
      #(cluster/refresh-topology! spec))
    [(is (= @calls_ 1))
     (is (= @callback-ids_ [:on-refresh-success]))
     (is (= (count @callback-errors_) 1))
     (is (= (:eid (ex-data (first @callback-errors_)))
           :carmine.cluster/reentrant-refresh))]))

(deftest _cluster-public-topology
  (let [spec (cluster/cluster-spec [["seed.redis" 7000]])]
    (is (nil? (cluster/cluster-cached-topology spec))
      "Nil before discovery")
    (with-redefs-fn
      {#'cluster/raw-topology-reply! (fn [& _] (test-slots-reply))}
      (fn []
        (let [_         (cluster/refresh-topology! spec)
              refreshed (cluster/cluster-cached-topology spec)]
          [(is (= (dissoc refreshed :updated-at-ms)
                 {:shards
                  [{:slot-ranges [[0 16383]]
                    :master
                    {:node-id "master"
                     :addr     ["master.redis" 7000]
                     :tls-addr nil}
                    :replicas
                    [{:node-id "replica"
                      :addr     ["replica.redis" 7001]
                      :tls-addr nil}]}]})
             "The public projection exposes exactly the documented stable keys")
           (is (pos-int? (:updated-at-ms refreshed)))])))
    (is (->> (cluster/cluster-cached-topology {})
          (throws? :ex-info {:eid :carmine.cluster/invalid-spec}))))

  (let [spec (cluster/cluster-spec [["seed.redis" 7000]])]
    (with-redefs-fn
      {#'cluster/raw-topology-reply!
       (fn [& _]
         [[101 8000  ["a.redis" 7000 "a"] ["ra.redis" 7003 "ra"]]
          [0    99   ["a.redis" 7000 "a"] ["ra.redis" 7003 "ra"]]
          [100  100  ["b.redis" 7001 "b"]]
          [8001 16383 ["b.redis" 7001 "b"]]])}
      (fn []
        (cluster/refresh-topology! spec)
        (is (= (mapv (juxt #(get-in % [:master :node-id]) :slot-ranges
                       #(mapv :node-id (:replicas %)))
                 (:shards (cluster/cluster-cached-topology spec)))
              [["a" [[0 99] [101 8000]]    ["ra"]]
               ["b" [[100 100] [8001 16383]] []]])
          "One shard per master, even with non-contiguous slot ranges")))))

(deftest _cluster-refresh-topology-via-manager
  (let [spec (cluster/cluster-spec [["seed.redis" 7000]])]
    (with-open [mgr (conns/conn-manager-clustered
                      {:conn-opts {:server {:cluster-spec spec}}})]
      (with-redefs-fn
        {#'cluster/raw-topology-reply! (fn [& _] (test-slots-reply))}
        (fn []
          (let [refreshed (cluster/cluster-refresh-topology! mgr)]
            [(is (= (count (:shards refreshed)) 1))
             (is (= refreshed (cluster/cluster-cached-topology spec)))]))))
    (is (->> (cluster/cluster-refresh-topology! spec)
          (throws? :ex-info {:eid :carmine.cluster/invalid-manager}))
      "Needs a Cluster manager, for its auth and TLS options")))

(deftest _cluster-refresh-tls-candidates
  (let [spec (cluster/cluster-spec [["tls-seed.redis" 17000]]
               {:conn-opts {:socket-opts {:ssl true}}})]
    (with-redefs-fn
      {#'cluster/raw-topology-reply!
       (fn [& _]
         [[0 16383 ["master.redis" 17000 "master"] ["replica.redis" 17001 "replica"]]])}
      #(cluster/refresh-topology! spec))
    [(is (= (:known-addrs @spec)
           [["master.redis" 17000]
            ["replica.redis" 17001]
            ["tls-seed.redis" 17000]])
       "TLS discovery records its advertised ports as TLS addresses")
     (is (nil? (get-in (cluster/cached-topology spec) [:shards 0 :master :addr])))
     (is (= (#'cluster/target-addr (cluster/cached-topology spec) true {:kind :slot, :slot 1})
            ["master.redis" 17000])
       "TLS discovery routes by the advertised TLS address")]))

(deftest _cluster
  (let [ck-str    (cluster/cluster-key "ignore{foo}")
        source-ba (enc/str->utf8-ba "ignore{foo}")
        ck-ba     (cluster/cluster-key source-ba)]
    [(is (= (cluster/cluster-slot (cluster/cluster-key "foo")) 12182))
     (is (= (cluster/cluster-slot ck-str) 12182))
     (is (= (cluster/cluster-slot ck-ba)  12182)
       "Hash tags apply identically to string and binary keys")
     (is (= (cluster/cluster-slot (cluster/cluster-key ck-str)) 12182)
       "Wrapping is idempotent")
     (let [ba (enc/str->utf8-ba "foo")]
       (is (= (cluster/cluster-slot ba)
              (#'cluster/redis-arg-slot (write/prepare-arg ba)))
         "A byte-array key's slot is computed as it's routed"))
     (is (= (cluster/cluster-slot "ignore{foo}") 12182)
       "Ordinary string keys compute their hash-tag slot without wrapping")
     (is (= (cluster/cluster-slot (enc/str->utf8-ba "ignore{foo}")) 12182)
       "Byte-array keys hash their exact bytes, matching `cluster-key`")
     (is (->> (cluster/cluster-slot :kw)
           (throws? :ex-info {:eid :carmine.cluster/invalid-key})))

     (is (= (com/with-out->str
              (#'write/write-requests out [["GET" ck-str ck-ba]]))
           "*3\r\n$3\r\nGET\r\n$11\r\nignore{foo}\r\n$11\r\nignore{foo}\r\n")
       "Cluster keys write the exact unmarked bytes used for routing")

     (aset source-ba 0 (byte (int \X)))
     (is (= (com/with-out->str (#'write/write-requests out [[ck-ba]]))
           "*1\r\n$11\r\nignore{foo}\r\n")
       "Binary cluster keys snapshot mutable input")

     (is (not= (cluster/cluster-slot (cluster/cluster-key "foo{}{bar}"))
           (cluster/cluster-slot (cluster/cluster-key "bar")))
       "An empty first hash tag causes the full key to be hashed")

     (let [pending-reqs (java.util.LinkedList.)
           ctx
           (taoensso.carmine_v4.resp.Ctx.
             true false pending-reqs (java.util.LinkedList.) nil nil nil nil (object-array 1))]
       (binding [resp/*ctx* ctx]
         (resp/rcmd "GET" ck-str))
       (let [^taoensso.carmine_v4.resp.Req req (.getFirst pending-reqs)]
         (is (= (.-cluster-slot req) 12182)
           "Cluster requests resolve slots after the Cluster namespace loads")))]))

(deftest _cluster-topology-replies
  (let [source-addr ["seed.redis" 6379]
        slots-reply
        [[0 3
          ["master-1.redis" 7000 "master-1" ["hostname" "master-1.local"]]
          [""               7001 "replica-1"]]
         [5 5 ["?" 7002 "master-2"]]]
        slots-topology (cluster/parse-cluster-slots source-addr slots-reply)]

    [(is (truss/submap? slots-topology
           {:n-slots 5
            :missing-slot-ranges [[4 4] [6 16383]]
            :unroutable-slot-ranges [[5 5]]
            :slot-coverage-complete? false
            :routable? false
            :complete? false})
       "Gaps and unknown endpoints remain explicit instead of corrupting routing")

     (is (= (get-in slots-topology [:slot-ranges [0 3] :master :addr])
           ["master-1.redis" 7000]))
     (is (= (get-in slots-topology [:slot-ranges [0 3] :master :hostname])
           "master-1.local"))
     (is (= (get-in slots-topology [:slot-ranges [0 3] :replicas 0 :addr])
           ["seed.redis" 7001])
       "Empty preferred endpoints use the host that reported the topology")
     (is (nil? (get-in slots-topology [:slot-ranges [5 5] :master :addr]))
       "A question-mark endpoint remains unknown")])

  (let [node ["node.redis" 7000 "node-1"]]
    [(is (truss/submap?
           (cluster/parse-cluster-slots [[0 16383 node]])
           {:n-slots 16384
            :missing-slot-ranges []
            :routable? true
            :complete? true}))
     (is (->> (cluster/parse-cluster-slots
                [[0 10 node] [10 20 node]])
           (throws? :ex-info
             {:eid :carmine.cluster/invalid-topology
              :problem :overlapping-slot-ranges})))
     (is (->> (cluster/parse-cluster-slots [[0 10]])
           (throws? :ex-info
             {:eid :carmine.cluster/invalid-topology
              :problem :invalid-shard})))])

  (let [tls-topology
        (cluster/parse-cluster-slots ["seed.redis" 7443] true
          [[0 16383 ["master.redis" 7443 "master-tls"]]])]
    [(is (nil? (get-in tls-topology [:slot-ranges [0 16383] :master :addr])))
     (is (= (get-in tls-topology [:slot-ranges [0 16383] :master :tls-addr])
           ["master.redis" 7443]))
     (is (:complete? tls-topology)
       "SLOTS discovery records its endpoint for the transport used to discover it")])

  (is (->> (cluster/parse-cluster-slots nil)
        (throws? :ex-info
          {:eid :carmine.cluster/invalid-topology
           :problem :expected-array})))

  (is (truss/submap? (cluster/parse-cluster-slots [])
        {:n-slots 0
         :missing-slot-ranges [[0 16383]]
         :slot-coverage-complete? false
         :routable? false
         :complete? false})
    "An empty discovery reply is valid but explicitly incomplete"))

(deftest _cluster-routing
  (let [topology
        (cluster/parse-cluster-slots ["seed.redis" 7000]
          [[0 2 ["master.redis" 7000 "master"]]
           [4 5 ["?"            7001 "unknown"]]])]
    [(is (= (get-in (cluster/slot->shard topology 0) [:master :addr])
           ["master.redis" 7000]))
     (is (= (get-in (cluster/slot->shard topology 2) [:master :addr])
           ["master.redis" 7000]))
     (is (nil? (cluster/slot->shard topology 3))
       "Only uncovered slots return nil")
     (is (some? (cluster/slot->shard topology 4))
       "Covered but unroutable slots retain their shard state")
     (is (nil? (get-in (cluster/slot->shard topology 4) [:master :addr])))
     (is (->> (cluster/slot->shard topology -1)
           (throws? :ex-info {:eid :carmine.cluster/invalid-slot})))
     (is
       (->> (cluster/slot->shard topology 16384)
         (throws? :ex-info {:eid :carmine.cluster/invalid-slot})))
     (is (->> (#'cluster/target-addr topology true {:kind :slot, :slot 0})
           (throws? :ex-info
             {:eid :carmine.cluster/no-route
              :problem :transport-unavailable
              :transport :tls}))
       "TLS execution never falls back to a plaintext-only advertised route")])

  (let [plain-master {:id "plain", :role :master, :addr ["plain.redis" 7000]}
        tls-master   {:id "tls", :role :master, :tls-addr ["tls.redis" 7443]}
        topology {:shards [{:master plain-master, :nodes [plain-master]}
                           {:master tls-master,   :nodes [tls-master]}]}
        target-addr (fn [tls? target] (#'cluster/target-addr topology tls? target))]
    [(is (= (target-addr true  {:kind :any}) ["tls.redis" 7443])
       "`:any` chooses the first master compatible with the manager transport")
     (is (= (target-addr false {:kind :any}) ["plain.redis" 7000]))
     (is (= (target-addr false {:kind :node-id, :node-id "plain"}) ["plain.redis" 7000]))
     (is (= (target-addr false {:kind :addr, :addr ["plain.redis" 7000]}) ["plain.redis" 7000]))
     (is (->> (target-addr false {:kind :node-id, :node-id "tls"})
           (throws? :ex-info
             {:eid :carmine.cluster/no-route, :problem :transport-unavailable})))
     (is (->> (target-addr false {:kind :node-id, :node-id "nx"})
           (throws? :ex-info
             {:eid :carmine.cluster/no-route, :problem :unknown-node-id})))
     (is (->> (target-addr false {:kind :addr, :addr ["nx.redis" 7000]})
           (throws? :ex-info
             {:eid :carmine.cluster/no-route, :problem :unknown-address})))]))

(defn- settle-test-future! [f]
  (when (= (try
             (deref f 5000 ::timeout)
             (catch Throwable _ ::settled))
           ::timeout)
    (future-cancel f)))

(deftest _cluster-executor
  (let [spec   (cluster/cluster-spec [["seed.redis" 7000]])
        addr-a ["a.redis" 7000]
        addr-b ["b.redis" 7001]
        find-key
        (fn [pred prefix]
          (some
            (fn [idx]
              (let [ck (cluster/cluster-key (str prefix idx))]
                (when (pred (cluster/cluster-slot ck)) ck)))
            (range 100000)))
        key-a (find-key #(< % 8192) "a:")
        key-b (find-key #(>= % 8192) "b:")
        borrows_ (atom [])
        writes_  (atom {})]
    (with-open [mgr (conns/conn-manager-clustered
                      {:conn-opts {:server {:cluster-spec spec}}
                       :pool-opts {:test-while-idle? false}})]
      (with-redefs-fn
        {#'cluster/raw-topology-reply!
         (fn [& _]
           [[0 8191 ["a.redis" 7000 "id-a"]]
            [8192 16383 ["b.redis" 7001 "id-b"]]])}
        #(cluster/refresh-topology! spec))
      (with-redefs-fn
        {#'cluster/borrow-addr!
         (fn [_ addr f]
           (swap! borrows_ conj addr)
           (let [baos (java.io.ByteArrayOutputStream.)
                 out  (java.io.BufferedOutputStream. baos)
                 label (if (= addr addr-a) "A" "B")
                 result
                 (f nil
                   (com/str->in (apply str (repeat 8 (str "+" label "\r\n"))))
                   out)]
             (.flush out)
             (swap! writes_ assoc addr (enc/utf8-ba->str (.toByteArray baos)))
             result))}
        (fn []
          [(is (= (wcar mgr :as-vec
                    (resp/rcmd "SET" key-a "1")
                    (resp/local-echo :local)
                    (resp/rcmd "SET" key-b "2")
                    (car/skip-replies (resp/rcmd "GET" key-a))
                    (resp/rcmd "GET" key-b))
                  ["A" :local "B" "B"]))
           (is (= @borrows_ [addr-a addr-b]))
           (is (re-find #"SET" (get @writes_ addr-a)))
           (is (re-find #"GET" (get @writes_ addr-a)))
           (is (re-find #"SET" (get @writes_ addr-b)))
           (is (re-find #"GET" (get @writes_ addr-b)))])))))

(deftest _cluster-executor-parallel-round
  (let [addrs       [[:node-a 1] [:node-b 2] [:node-c 3] [:node-d 4]]
        tasks       (mapv (fn [idx addr] {:index idx, :addr addr}) (range) addrs)
        active_     (atom 0)
        max-active_ (atom 0)
        calls_      (atom [])
        bindings_   (atom [])
        started     (promise)
        release     (promise)
        marker      {:test-marker (Object.)}]
    (with-redefs-fn
      {#'cluster/execute-partition!
       (fn [_mgr {:keys [addr entries]}]
         (swap! calls_ conj addr)
         (swap! bindings_ conj car/*conn-cbs*)
         (let [n-active (swap! active_ inc)]
           (swap! max-active_ max n-active)
           (when (= n-active 2) (deliver started true)))
         (try
           @release
           (mapv #(assoc % :reply addr) entries)
           (finally (swap! active_ dec))))}
      (fn []
        (let [run
              (binding [car/*conn-cbs* marker]
                (future (#'cluster/execute-round! nil tasks 2)))]
          (try
            (is (true? (deref started 5000 false)))
            (is (= (count @calls_) 2)
              "Only the configured number of partitions starts before release")
            (deliver release true)
            (let [completed (deref run 5000 ::timeout)]
              [(is (not= completed ::timeout))
               (is (= (mapv :reply completed) addrs)
                 "Completion is restored to original partition order")
               (is (= @max-active_ 2))
               (is (= (count @calls_) 4))
               (is (every? #(identical? marker %) @bindings_)
                 "Worker futures convey the caller's dynamic bindings")])
            (finally
              (deliver release true)
              (settle-test-future! run))))))))

(deftest _cluster-executor-parallel-error-drain
  (let [expected-error (ex-info "partition failed" {:partition :node-a})
        n-started_   (atom 0)
        n-complete_  (atom 0)
        both-started (promise)
        failed       (promise)
        release-b    (promise)
        tasks        [{:index 0, :addr :node-a}
                      {:index 1, :addr :node-b}
                      {:index 2, :addr :node-c}]]
    (with-redefs-fn
      {#'cluster/execute-partition!
       (fn [_mgr {:keys [addr entries]}]
         (when (= (swap! n-started_ inc) 2) (deliver both-started true))
         @both-started
         (case addr
           :node-a (do (deliver failed true) (throw expected-error))
           :node-b (do @release-b (swap! n-complete_ inc) entries)
           :node-c entries))}
      (fn []
        (let [run
              (future
                (try
                  {:result (#'cluster/execute-round! nil tasks 2)}
                  (catch Throwable t
                    {:error t, :n-complete @n-complete_})))]
          (try
            (is (true? (deref both-started 5000 false)))
            (is (true? (deref failed 5000 false)))
            (deliver release-b true)
            (let [{:keys [error n-complete] :as outcome}
                  (deref run 5000 ::timeout)]
              [(is (not= outcome ::timeout))
               (is (identical? error expected-error))
               (is (= n-complete 1)
                 "The first error is rethrown only after active siblings settle")])
            (finally
              (deliver both-started true)
              (deliver release-b true)
              (settle-test-future! run))))))))

(deftest _cluster-executor-pre-interrupted
  (let [calls_ (atom 0)
        tasks [{:index 0, :addr :node-a}
               {:index 1, :addr :node-b}]]
    (with-redefs-fn
      {#'cluster/execute-partition!
       (fn [_mgr {:keys [entries]}]
         (swap! calls_ inc)
         entries)}
      (fn []
        (try
          (.interrupt (Thread/currentThread))
          (let [{:keys [error interrupted?]}
                (try
                  (#'cluster/execute-round! nil tasks 2)
                  (catch Throwable t
                    {:error t
                     :interrupted? (.isInterrupted (Thread/currentThread))}))]
            [(is (instance? InterruptedException error))
             (is interrupted?)
             (is (zero? @calls_)
               "A pre-interrupted caller starts no partition work")])
          (finally
            (Thread/interrupted)))))))

(deftest _cluster-executor-completed-worker-interruption
  (let [worker (future :done)
        stop?_ (atom false)]
    (is (= @worker :done))
    (try
      (.interrupt (Thread/currentThread))
      (let [{:keys [error interrupted?]}
            (try
              (#'cluster/await-workers! [worker] stop?_)
              (catch Throwable t
                {:error t
                 :interrupted? (.isInterrupted (Thread/currentThread))}))]
        [(is (instance? InterruptedException error))
         (is interrupted?
           "A pending interrupt is preserved when every worker is already complete")
         (is @stop?_ "A pending interrupt stops workers from claiming new work")])
      (finally
        (Thread/interrupted)))))

(deftest _cluster-executor-serial-interruption
  (let [calls_ (atom [])
        tasks [{:index 0, :addr :node-a}
               {:index 1, :addr :node-b}]]
    (with-redefs-fn
      {#'cluster/execute-partition!
       (fn [_mgr {:keys [addr entries]}]
         (swap! calls_ conj addr)
         (when (= addr :node-a)
           (.interrupt (Thread/currentThread)))
         entries)}
      (fn []
        (try
          (let [{:keys [error interrupted?]}
                (try
                  (#'cluster/execute-round! nil tasks 1)
                  (catch Throwable t
                    {:error t
                     :interrupted? (.isInterrupted (Thread/currentThread))}))]
            [(is (instance? InterruptedException error))
             (is interrupted?)
             (is (= @calls_ [:node-a])
               "Interruption between serial partitions prevents new work")])
          (finally
            (Thread/interrupted)))))))

(deftest _cluster-executor-parser-interruption
  (doseq [concurrency [1 2]]
    (let [failure (InterruptedException. "Parser interrupted")
          parser (com/fn-parser {} (fn [_] (throw failure)))
          tasks [{:index 0, :addr :node-a}
                 {:index 1, :addr :node-b}]]
      (with-redefs-fn
        {#'cluster/execute-partition!
         (fn [_mgr {:keys [addr entries]}]
           (if (= addr :node-a)
             ((.-f parser) :reply)
             entries))}
        (fn []
          (try
            (let [{:keys [error interrupted?]}
                  (try
                    (#'cluster/execute-round! nil tasks concurrency)
                    (catch Throwable t
                      {:error t
                       :interrupted? (.isInterrupted (Thread/currentThread))}))]
              (is (identical? failure error))
              (is interrupted?
                "Parser interruption restores the caller flag in both serial and parallel execution"))
            (finally (Thread/interrupted))))))))

(deftest _cluster-executor-parallel-interruption
  (let [tasks      [{:index 0, :addr :node-a}
                    {:index 1, :addr :node-b}
                    {:index 2, :addr :node-c}]
        calls_      (atom [])
        n-active_  (atom 0)
        n-complete_ (atom 0)
        started    (promise)
        release    (promise)
        outcome    (promise)
        observed-stop_ (promise)
        original-await-workers! @#'cluster/await-workers!
        caller
        (doto
          (Thread.
            ^Runnable
            (fn []
              (try
                (deliver outcome
                  {:result (#'cluster/execute-round! nil tasks 2)})
                (catch Throwable t
                  (deliver outcome
                    {:error t
                     :interrupted? (.isInterrupted (Thread/currentThread))
                     :n-complete @n-complete_})))))
          (.setDaemon true))]
    (with-redefs-fn
      {#'cluster/execute-partition!
       (fn [_mgr {:keys [addr entries]}]
         (swap! calls_ conj addr)
         (when (= (swap! n-active_ inc) 2) (deliver started true))
         (try
           @release
           (swap! n-complete_ inc)
           entries
           (finally (swap! n-active_ dec))))

       #'cluster/await-workers!
       (fn [workers stop?_]
         (deliver observed-stop_ stop?_)
         (original-await-workers! workers stop?_))}
      (fn []
        (try
          (.start caller)
          (is (true? (deref started 5000 false)))
          (let [stop?_ (deref observed-stop_ 5000 ::timeout)]
            (is (not= stop?_ ::timeout))
            (when (not= stop?_ ::timeout)
              (.interrupt caller)
              (is
                (true?
                  (support/await-pred! 5000 "the Cluster executor to observe interruption"
                    #(true? @stop?_))))
              (is (not (realized? outcome))
                "Caller interruption does not abandon active partition reads")
              (deliver release true)
              (let [{:keys [error interrupted? n-complete] :as result}
                    (deref outcome 5000 ::timeout)]
                [(is (not= result ::timeout))
                 (is (instance? InterruptedException error))
                 (is interrupted? "The caller's interrupt status is restored")
                 (is (= n-complete 2) "Every active partition settles before rethrow")
                 (is (and (= (count @calls_) 2)
                          (= (set @calls_) #{:node-a :node-b}))
                   "Interruption prevents workers from claiming new partitions")])))
          (finally
            (deliver release true)
            (.join caller 5000)
            (when (.isAlive caller)
              (.interrupt caller)
              (.join caller 1000))
            (is (not (.isAlive caller))
              "The test caller always terminates")))))))

(deftest _cluster-executor-retries
  (let [addr-a ["a.redis" 7000]
        addr-c ["c.redis" 7002]
        spec   (cluster/cluster-spec [["seed.redis" 7000]])
        key    (cluster/cluster-key "retry-key")
        slot   (cluster/cluster-slot key)]
    (with-redefs-fn
      {#'cluster/raw-topology-reply!
       (fn [& _] [[0 16383 ["a.redis" 7000 "id-a"]]])}
      #(cluster/refresh-topology! spec))
    (with-open [mgr
                (conns/conn-manager-clustered
                  {:conn-opts
                   {:server
                    {:cluster-spec spec
                     :cluster-opts
                     {:max-retry-rounds 2, :retry-backoff-ms 0}}}
                   :pool-opts {:test-while-idle? false}})]
      (let [run-case
            (fn [responses body-fn & [reply-opts]]
              (let [responses_ (atom responses)
                    calls_     (atom [])
                    refreshes_ (atom 0)]
                [(with-redefs-fn
                   {#'cluster/borrow-addr!
                    (fn [_ addr f]
                      (let [{expected-addr :addr, :keys [wire]} (first @responses_)
                            _ (swap! responses_ next)
                            baos (java.io.ByteArrayOutputStream.)
                            out  (java.io.BufferedOutputStream. baos)
                            result (f nil (com/str->in wire) out)]
                        (.flush out)
                        (swap! calls_ conj
                          {:addr addr
                           :expected-addr expected-addr
                           :write (enc/utf8-ba->str (.toByteArray baos))})
                        result))
                    #'cluster/refresh-for-execution!
                    (fn [& _]
                      (swap! refreshes_ inc)
                      (cluster/cached-topology spec))}
                   #(car/with-car mgr (merge {:as-vec? true, :error-mode :return} reply-opts)
                      (fn [] (body-fn))))
                 @calls_ @refreshes_]))
            error-wire (fn [kind target]
                         (str "-" kind
                           (when target (str " " slot " " target)) "\r\n"))]

        (let [[reply calls refreshes]
              (run-case
                [{:addr addr-a, :wire (error-wire "MOVED" "c.redis:7002")}
                 {:addr addr-c, :wire "+MOVED-OK\r\n"}]
                #(resp/rcmd "GET" key))]
          [(is (= reply ["MOVED-OK"]))
           (is (= (mapv :addr calls) [addr-a addr-c]))
           (is (every? #(= (:addr %) (:expected-addr %)) calls))
           (is (= refreshes 1))])

        (let [[reply calls refreshes]
              (run-case
                [{:addr addr-a, :wire (error-wire "MOVED" "c.redis:7002")}
                 {:addr addr-c, :wire "+MOVED-OK\r\n"}]
                #(com/parse
                   (fn [reply] [:parsed reply])
                   (resp/rcmd "GET" key)))]
          [(is (= reply [[:parsed "MOVED-OK"]])
             "Parsers see only the final reply after Cluster redirections")
           (is (= (mapv :addr calls) [addr-a addr-c]))
           (is (= refreshes 1))])

        (let [n-calls* (atom 0)
              [reply _ _]
              (run-case
                [{:addr addr-a, :wire "+OK\r\n"}]
                #(com/parse
                   (fn [_reply] (swap! n-calls* inc) (throw (Exception. "Parser boom")))
                   (resp/rcmd "GET" key)))]
          [(is (= @n-calls* 1)
             "A parser that throws on a successful reply runs exactly once")
           (is (com/reply-error? {:eid :carmine.read/parser-error} (first reply)))])

        (let [[reply calls refreshes]
              (run-case
                [{:addr addr-a, :wire (error-wire "ASK" "c.redis:7002")}
                 {:addr addr-c, :wire "+OK\r\n+ASK-OK\r\n"}]
                #(resp/rcmd "GET" key))]
          [(is (= reply ["ASK-OK"]))
           (is (= (mapv :addr calls) [addr-a addr-c]))
           (is (re-find #"ASKING" (:write (second calls))))
           (is (zero? refreshes))])

        (let [[reply calls refreshes]
              (run-case
                [{:addr addr-a, :wire (error-wire "ASK" "c.redis:7002")}
                 {:addr addr-c, :wire "+OK\r\n-TRYAGAIN later\r\n"}
                 {:addr addr-c, :wire "+OK\r\n+ASK-RETRY-OK\r\n"}]
                #(resp/rcmd "GET" key))]
          [(is (= reply ["ASK-RETRY-OK"]))
           (is (= (mapv :addr calls) [addr-a addr-c addr-c]))
           (is (every? #(= (:addr %) (:expected-addr %)) calls))
           (is (every? #(re-find #"ASKING" (:write %)) (next calls))
             "TRYAGAIN on an importing node retries there with a fresh ASKING")
           (is (zero? refreshes))])

        (let [[reply calls refreshes]
              (run-case
                [{:addr addr-a, :wire (error-wire "TRYAGAIN" nil)}
                 {:addr addr-a, :wire "+TRY-OK\r\n"}]
                #(resp/rcmd "GET" key))]
          [(is (= reply ["TRY-OK"]))
           (is (= (mapv :addr calls) [addr-a addr-a]))
           (is (zero? refreshes))])

        (let [[reply calls refreshes]
              (run-case
                [{:addr addr-a, :wire (error-wire "CLUSTERDOWN" nil)}
                 {:addr addr-a, :wire "+DOWN-OK\r\n"}]
                #(resp/rcmd "GET" key))]
          [(is (= reply ["DOWN-OK"]))
           (is (= (mapv :addr calls) [addr-a addr-a]))
           (is (= refreshes 1))])

        (let [[reply calls _]
              (run-case
                [{:addr addr-a, :wire (error-wire "MOVED" "c.redis:7002")}
                 {:addr addr-c, :wire "+OK\r\n"}]
                #(car/skip-replies (resp/rcmd "SET" key "value")))]
          [(is (nil? reply))
           (is (= (mapv :addr calls) [addr-a addr-c]))])

        (let [[reply calls _]
              (run-case
                [{:addr addr-a, :wire (error-wire "MOVED" "c.redis:7002")}
                 {:addr addr-c, :wire "-WRONGTYPE final\r\n"}]
                #(car/skip-replies (resp/rcmd "SET" key "value")))]
          [(is (= (count reply) 1))
           (is (com/reply-error? (first reply))
             "Public skip-replies retains final errors after Cluster redirection")
           (is (= (mapv :addr calls) [addr-a addr-c]))])

        (is (com/reply-error?
              (truss/throws
                (run-case
                  [{:addr addr-a, :wire "-WRONGTYPE final\r\n"}]
                  #(car/skip-replies (resp/rcmd "SET" key "value"))
                  {:error-mode :throw})))
          "Skipped Cluster errors respect the boundary's throw policy")

        (let [[reply calls _]
              (run-case
                (vec (repeat 3 {:addr addr-a, :wire (error-wire "TRYAGAIN" nil)}))
                #(resp/rcmd "GET" key))]
          [(is (= (count calls) 3))
           (is (com/reply-error? (first reply)))])

        (is (->> (run-case [{:addr addr-a, :wire "?bad\r\n"}]
                    #(resp/rcmd "GET" key))
              (throws? :ex-info {:eid :carmine.read/unexpected-read-error}))
          "Protocol errors escape while the node borrow can be invalidated")

        (let [stats (get-in @spec [:stats :execution-stats])]
          [(is (<= 2 (:n-moved stats)))
           (is (= (:n-ask stats) 2))
           (is (<= 3 (:n-try-again stats)))
           (is (= (:n-cluster-down stats) 1))
           (is (<= 7 (:n-retry-rounds stats)))])))))

(deftest _cluster-executor-retry-no-route-isolation
  (let [spec   (cluster/cluster-spec [["seed.redis" 7000]])
        key    (cluster/cluster-key "no-route-key")
        slot   (cluster/cluster-slot key)
        req    (fn [k]
                 (taoensso.carmine_v4.resp.Req.
                   com/read-opts-natural ["GET" k] slot
                   {:kind :slot, :slot slot} true nil 0))
        addr-a ["a.redis" 7000]
        tasks  [{:index 0, :request (req "k0"), :addr addr-a, :asking? false}
                {:index 1, :request (req "k1"), :addr addr-a, :asking? false}]
        retry-error
        (support/redis-error-reply "CLUSTERDOWN unavailable")
        route-attempts_ (atom 0)
        round_         (atom 0)
        retried-tasks_ (atom nil)
        final
        (with-redefs-fn
          {#'cluster/current-task-addr
           (fn [& _]
             (swap! route-attempts_ inc)
             (#'cluster/no-route! :unmapped-slot {:slot slot}))
           #'cluster/refresh-for-execution! (fn [& _])
           #'cluster/execute-round!
           (fn [_ tasks _]
             (if (= 1 (swap! round_ inc))
               (mapv
                 (fn [{:keys [index] :as task}]
                   (assoc task :reply (if (zero? ^long index) retry-error "ok-1")))
                 tasks)
               (do
                 (reset! retried-tasks_ tasks)
                 (mapv #(assoc % :reply "ok-0") tasks))))}
          #(#'cluster/execute-with-retries! nil spec
             {:max-retry-rounds 1, :retry-backoff-ms 0
              :max-concurrent-nodes 1}
             tasks))]
    [(is (= final {0 "ok-0", 1 "ok-1"})
       "A transiently unmapped slot never discards sibling replies")
     (is (= @route-attempts_ 1))
     (is (= (mapv :addr @retried-tasks_) [addr-a])
       "The no-route task retries at its previous address")])

  (let [spec (cluster/cluster-spec [["seed.redis" 7000]])
        key  (cluster/cluster-key "no-route-key")
        slot (cluster/cluster-slot key)
        req  (taoensso.carmine_v4.resp.Req.
               com/read-opts-natural ["GET" key] slot
               {:kind :slot, :slot slot} true nil 0)
        task {:index 0, :request req
              :addr ["a.redis" 7000], :asking? false}
        retry-error
        (support/redis-error-reply "CLUSTERDOWN unavailable")]
    (is (->> (with-redefs-fn
               {#'cluster/current-task-addr
                (fn [& _]
                  (truss/ex-info! "[Carmine] Failed to refresh Redis Cluster topology"
                    {:eid :carmine.cluster/refresh-error}))
                #'cluster/refresh-for-execution! (fn [& _])
                #'cluster/execute-round!
                (fn [_ tasks _] (mapv #(assoc % :reply retry-error) tasks))}
               #(#'cluster/execute-with-retries! nil spec
                  {:max-retry-rounds 1, :retry-backoff-ms 0
                   :max-concurrent-nodes 1}
                  [task]))
          (throws? :ex-info {:eid :carmine.cluster/refresh-error}))
      "Only routing gaps are isolated: re-resolution refresh failures still escape")))

(deftest _cluster-exhausted-topology-error-stales-cache
  (doseq [[code kind]
          [["MOVED" :moved]
           ["CLUSTERDOWN" :cluster-down]]]
    (let [spec (cluster/cluster-spec [["seed.redis" 7000]])
          key  (cluster/cluster-key (str "exhausted-" code))
          slot (cluster/cluster-slot key)
          req  (taoensso.carmine_v4.resp.Req.
                 com/read-opts-natural ["GET" key] nil
                 {:kind :slot, :slot slot} true nil 0)
          task {:index 0, :request req
                :addr ["a.redis" 7000], :asking? false}
          message (if (= kind :moved)
                    (str "MOVED " slot " c.redis:7002")
                    "CLUSTERDOWN unavailable")
          reply (support/redis-error-reply message)]
      (with-redefs-fn
        {#'cluster/raw-topology-reply!
         (fn [& _] [[0 16383 ["a.redis" 7000 "id-a"]]])}
        #(cluster/refresh-topology! spec))
      (is (some? (cluster/cached-topology spec)))
      (let [final
            (with-redefs-fn
              {#'cluster/execute-round!
               (fn [_ tasks _] (mapv #(assoc % :reply reply) tasks))}
              #(#'cluster/execute-with-retries! nil spec
                 {:max-retry-rounds 0, :retry-backoff-ms 0
                  :max-concurrent-nodes 1}
                 [task]))]
        [(is (identical? (get final 0) reply)
           "An exhausted retry budget preserves the final Redis reply")
         (is (nil? (cluster/cached-topology spec))
           (str code " marks the cache stale for the next request"))]))))

;; Retry counts from a flush that throws in a later round are still recorded
(deftest _cluster-retry-stats-survive-throw
  (let [spec (cluster/cluster-spec [["seed.redis" 7000]])
        key  (cluster/cluster-key "retry-stats")
        slot (cluster/cluster-slot key)
        req  (taoensso.carmine_v4.resp.Req.
               com/read-opts-natural ["GET" key] nil
               {:kind :slot, :slot slot} true nil 0)
        task {:index 0, :request req, :addr ["a.redis" 7000], :asking? false}
        moved (support/redis-error-reply (str "MOVED " slot " c.redis:7002"))
        round_ (atom 0)]
    [(is (->> (with-redefs-fn
                {#'cluster/refresh-for-execution! (fn [& _])
                 #'cluster/execute-round!
                 (fn [_ tasks _]
                   (if (= 1 (swap! round_ inc))
                     (mapv #(assoc % :reply moved) tasks)
                     (throw (ex-info "Expected" {:eid ::expected}))))}
                #(#'cluster/execute-with-retries! nil spec
                   {:max-retry-rounds 1, :retry-backoff-ms 0
                    :max-concurrent-nodes 1}
                   [task]))
           (throws? :ex-info {:eid ::expected})))
     (is (truss/submap? (get-in @spec [:stats :execution-stats])
           {:n-moved 1, :n-retry-rounds 1}))]))

(deftest _cluster-key-spec-indexes
  (let [args ["FAKE" "KEYS" "old-key" "KEYS" "new-a" "new-b"]
        backward-keyword-spec
        {:begin_search
         {:type "keyword", :spec {:keyword "KEYS", :startfrom -2}}
         :find_keys
         {:type "range", :spec {:lastkey -1, :keystep 1, :limit 0}}}]
    (is (= (vec (#'cluster/key-spec-indexes args backward-keyword-spec)) [4 5])
      "Negative startfrom searches backward for the last matching keyword")
    (is (nil? (#'cluster/key-spec-indexes
                ["KEYS" "not-a-keyword" "value"] backward-keyword-spec))
      "Backward keyword search stops before the command name at index zero"))

  (let [limit-spec ; E.g. XREAD-style `STREAMS key [key ...] id [id ...]`
        {:begin_search {:type "index", :spec {:index 1}}
         :find_keys
         {:type "range", :spec {:lastkey -1, :keystep 1, :limit 2}}}]
    [(is (= (vec (#'cluster/key-spec-indexes
                   ["FAKE" "k1" "k2" "a1" "a2"] limit-spec)) [1 2])
       "A positive limit stops the key search a factor into the remaining args")
     (is (= (vec (#'cluster/key-spec-indexes
                   ["FAKE" "k1" "k2" "a1"] limit-spec)) [1])
       "A truncated arity never yields an extra key index")]))
