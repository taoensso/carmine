(ns taoensso.carmine-v4.tests.cluster-unit
  "Deterministic Carmine v4 Cluster topology, routing, and executor tests."
  (:require
   [clojure.string      :as str]
   [clojure.test        :as test :refer [deftest testing is]]
   [taoensso.encore     :as enc]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine    :as v3-core]
   [taoensso.carmine-v4          :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.utils    :as utils]
   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.cluster  :as cluster]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tk  "Test key" support/test-key)
(def tc  "Unparsed test conn-opts" support/test-conn-opts)
(def tc+ "Parsed test conn-opts" support/parsed-test-conn-opts)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

(defn- redis-major-version [] (support/redis-major-version))

;;;; Cluster

(deftest _cluster-spec
  (let [spec (cluster/cluster-spec
               [["seed-a.redis" "7000"]
                ["seed-b.redis" 7001]
                ["seed-a.redis" 7000]]
               {:topology-source :cluster-slots})
        state @spec]
    [(is (cluster/cluster-spec? spec))
     (is (car/cluster-spec? spec))
     (is (= (:seed-addrs state)
           [["seed-a.redis" 7000] ["seed-b.redis" 7001]]))
     (is (= (:known-addrs state) (:seed-addrs state)))
     (is (= (get-in state [:cluster-spec-opts :topology-source]) :cluster-slots))
     (is (= (get-in state [:cluster-spec-opts :refresh-timeout-ms]) 2000))
     (is (nil? (:topology state)))
     (is (= (set (keys state))
           #{:cluster-spec-opts :seed-addrs :known-addrs :topology
             :last-success-addr :updated-at :stats}))])

  [(let [error (truss/throws (cluster/cluster-spec []))]
     [(is (= (:eid (ex-data error)) :carmine.cluster/invalid-spec))
      (is (= (:eid (ex-data (ex-cause error))) :carmine.cluster/no-seed-addrs))])
   (is (->> (cluster/cluster-spec [["seed.redis" "not-a-port"]])
         (throws? :ex-info {:eid :carmine.cluster/invalid-spec})))
   (is (->> (cluster/cluster-spec [["seed.redis" 7000]]
              {:topology-source :invalid})
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
          (throws? :ex-info {:eid :carmine.cluster/conn-not-supported})))))

(deftest _cluster-conn-manager
  (let [spec      (cluster/cluster-spec [["seed.redis" 7000]])
        conn-opts {:server {:cluster-spec spec}}
        expected  {:eid :carmine.cluster/manager-required}]
    [(is (->> (conns/conn-manager-unpooled {:conn-opts conn-opts})
           (throws? :ex-info expected)))
     (is (->> (conns/conn-manager-pooled {:conn-opts conn-opts})
           (throws? :ex-info expected)))
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
              [{:server-addr ["127.0.0.1" 6379], :active 0, :idle 1}]))
        (is (= (car/conn-manager-clear! mgr 1000)
              {:action :cleared, :graceful? true}))])
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
            clear-f (future (car/conn-manager-clear! mgr 2000))]
        (loop [n 0]
          (when (and (zero? (get-in @mgr [:stats :counts :cleared])) (< n 200))
            (Thread/sleep 5)
            (recur (inc n))))
        (deliver release :release)
        [(is (= (deref borrow-f 2000 ::timeout) :done))
         (is (= (deref clear-f 2000 ::timeout)
               {:action :cleared, :graceful? true}))
         (is (zero? (get-in @mgr [:stats :connections :idle])))
         (is (= (get-in (first @close-events_) [:data :via]) 'mgr-clear!))
         (is (= (conns/mgr-borrow-addr! mgr ["127.0.0.1" 6379]
                  (fn [_ in out]
                    (resp/with-replies in out false false resp/ping)))
                "PONG"))
         (is (> (get-in @mgr [:stats :counts :created]) (:created before)))])
      (finally
        (deliver release :release)
        (car/conn-manager-close! mgr 0 nil)))))

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
        (loop [n 0]
          (when (and (zero? (get-in @mgr [:stats :connections :waiting])) (< n 1000))
            (Thread/sleep 5)
            (recur (inc n))))
        (is (pos? (get-in @mgr [:stats :connections :waiting]))
          "The second borrower reaches pool exhaustion before release")
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
              (is (= (car/conn-manager-clear! mgr 5000)
                    {:action :cleared, :graceful? true}))
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
          ;; available to the default test suite.
          (real-new-conn (assoc conn-opts :server ["127.0.0.1" 6379])))
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
                        (let [result (car/conn-manager-clear! mgr 5000)]
                          (swap! completed-clears_ inc)
                          (recur (dec remaining) (conj results result)))))))
                clear-results (deref clear-f 30000 ::timeout)
                worker-results (mapv #(deref % 30000 ::timeout) workers)]
            [(is (not= clear-results ::timeout) "Bounded clears never deadlock")
             (is (= clear-results
                   (vec (repeat n-clears {:action :cleared, :graceful? true}))))
             (is (not-any? #{::timeout} worker-results) "Workers finish after clears cease")
             (is (= worker-results (vec (repeat n-workers n-iterations))))
             (is (= @completed-clears_ n-clears))
             (is (car/conn-manager-open? mgr))
             (is (zero? (get-in @mgr [:stats :connections :active])))
             (is (zero? (get-in @mgr [:stats :connections :waiting])))
             (is (<= n-clears (get-in @mgr [:stats :counts :cleared])))]))))))

(defn- test-shards-reply
  ([offset] (test-shards-reply offset "master.redis"))
  ([offset master-host]
   [{:slots [0 16383]
     :nodes
     [{:id "master", :endpoint master-host, :port 7000, :tls-port 17000
       :role "master", :health "online", :replication-offset offset}
      {:id "replica", :endpoint "replica.redis", :port 7001, :tls-port 17001
       :role "replica", :health "online", :replication-offset (dec offset)}]}]))

(defn- test-reply-error [message]
  (com/reply-error "[Carmine] Redis replied with an error"
    {:eid :carmine.read/redis-error-reply
     :code "ERR"
     :message message}))

(deftest _cluster-refresh-success
  (let [success-events_ (atom [])
        changed-events_ (atom [])
        offset_          (atom 10)
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
             (fn [conn-opts addr source]
               (swap! calls_ conj [addr source])
               (test-shards-reply @offset_ @master-host_))}
            #(cluster/refresh-topology! spec)))]

    (let [topology-1 (refresh)]
      [(is (:complete? topology-1))
       (is (identical? topology-1 (cluster/cached-topology spec)))
       (is (= (:known-addrs @spec)
             [["master.redis" 7000]
              ["replica.redis" 7001]
              ["seed.redis" 7000]]))
       (is (= @calls_ [[["seed.redis" 7000] :cluster-shards]]))])

    (swap! offset_ inc)
    (let [topology-2 (refresh)
          stats (:stats @spec)]
      [(is (= (get-in topology-2 [:shards 0 :master :replication-offset]) 11))
       (is (= (count @success-events_) 2))
       (is (= (count @changed-events_) 1)
         "Volatile replication offsets do not report routing changes")
       (is (= (mapv :cbid @success-events_)
             [:on-refresh-success :on-refresh-success]))
       (is (= (:cbid (first @changed-events_)) :on-changed-topology))
       (is (= (:refresh-stats stats)
             {:n-attempts 2, :n-fallbacks 0, :n-successes 2}))
       (is (= (get-in stats [:node-stats ["seed.redis" 7000]])
             {:n-attempts 2, :n-successes 2}))])

    (reset! master-host_ "moved.redis")
    (refresh)
    (let [{:keys [old new]} (:changed (second @changed-events_))]
      [(is (= (count @success-events_) 3))
       (is (= (count @changed-events_) 2))
       (is (= (get-in old [:shards 0 :master :addr]) ["master.redis" 7000]))
       (is (= (get-in new [:shards 0 :master :addr]) ["moved.redis" 7000]))])))

(deftest _cluster-refresh-candidates-and-fallback
  (let [calls_ (atom [])
        spec   (cluster/cluster-spec [["stale.redis" 7000] ["good.redis" 7001]])
        topology
        (with-redefs-fn
          {#'cluster/raw-topology-reply!
           (fn [_ addr source]
             (swap! calls_ conj [addr source])
             (cond
               (= addr ["stale.redis" 7000]) []
               (= source :cluster-shards)
               (test-reply-error "ERR unknown subcommand 'SHARDS'")
               :else
               [[0 16383 ["good.redis" 7001 "master"]]]))}
          #(cluster/refresh-topology! spec))
        stats (:stats @spec)]

    [(is (:complete? topology))
     (is (= (:source topology) :cluster-slots))
     (is (= @calls_
           [[["stale.redis" 7000] :cluster-shards]
            [["good.redis" 7001]  :cluster-shards]
            [["good.redis" 7001]  :cluster-slots]]))
     (is (= (:refresh-stats stats)
           {:n-attempts 2, :n-fallbacks 1, :n-successes 1}))
     (is (= (get-in stats [:node-stats ["stale.redis" 7000] :n-errors]) 1))])

  (let [calls_ (atom [])
        spec   (cluster/cluster-spec [["loading.redis" 7000] ["good.redis" 7001]])]
    (with-redefs-fn
      {#'cluster/raw-topology-reply!
       (fn [_ addr source]
         (swap! calls_ conj [addr source])
         (if (= addr ["loading.redis" 7000])
           (test-reply-error "ERR LOADING Redis is loading")
           (test-shards-reply 10)))}
      #(cluster/refresh-topology! spec))
    (is (= @calls_
          [[["loading.redis" 7000] :cluster-shards]
           [["good.redis" 7001]    :cluster-shards]])
      "Non-compatibility errors advance without same-node fallback"))

  (let [calls_ (atom [])
        spec   (cluster/cluster-spec [["old.redis" 7000] ["good.redis" 7001]]
                 {:topology-source :cluster-shards})]
    (with-redefs-fn
      {#'cluster/raw-topology-reply!
       (fn [_ addr source]
         (swap! calls_ conj [addr source])
         (if (= addr ["old.redis" 7000])
           (test-reply-error "ERR unknown subcommand 'SHARDS'")
           (test-shards-reply 10)))}
      #(cluster/refresh-topology! spec))
    [(is (= @calls_
           [[["old.redis" 7000]  :cluster-shards]
            [["good.redis" 7001] :cluster-shards]]))
     (is (= (get-in @spec [:stats :refresh-stats :n-fallbacks]) 0)
       "A pinned SHARDS source never falls back to SLOTS")]))

(deftest _cluster-refresh-learned-candidate
  (let [calls_       (atom [])
        mode_        (atom :initial)
        seed-addr    ["seed.redis" 7000]
        learned-addr ["master.redis" 7000]
        spec         (cluster/cluster-spec [seed-addr])
        raw-fn
        (fn [_ addr source]
          (swap! calls_ conj [addr source])
          (when (and (= @mode_ :learned-success) (= addr seed-addr))
            (throw (Exception. "Seed unavailable")))
          (test-shards-reply 10))]
    (with-redefs-fn {#'cluster/raw-topology-reply! raw-fn}
      #(cluster/refresh-topology! spec))

    (reset! calls_ [])
    (reset! mode_ :learned-success)
    (with-redefs-fn {#'cluster/raw-topology-reply! raw-fn}
      #(cluster/refresh-topology! spec))
    (is (= @calls_
          [[seed-addr :cluster-shards]
           [learned-addr :cluster-shards]]))

    (reset! calls_ [])
    (reset! mode_ :learned-first)
    (with-redefs-fn {#'cluster/raw-topology-reply! raw-fn}
      #(cluster/refresh-topology! spec))
    [(is (= (first @calls_) [learned-addr :cluster-shards]))
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

    [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-failed))
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
          {#'cluster/raw-topology-reply! (fn [& _] (test-shards-reply 10))}
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
           {:n-attempts 4, :n-fallbacks 0, :n-successes 1, :n-errors 1}))]))

(deftest _cluster-refresh-deadline
  (let [calls_ (atom 0)
        spec   (cluster/cluster-spec [["seed.redis" 7000]]
                 {:refresh-timeout-ms 0})
        error
        (with-redefs-fn
          {#'cluster/raw-topology-reply!
           (fn [& _] (swap! calls_ inc) (throw (Exception. "Unexpected I/O")))}
          #(try
             (cluster/refresh-topology! spec)
             (catch Throwable t t)))]
    [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-failed))
     (is (true? (:timed-out? (ex-data error))))
     (is (zero? (:n-attempts (ex-data error))))
     (is (zero? @calls_) "A zero aggregate timeout performs no network I/O")])

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
           (fn [conn-opts addr source]
             (swap! calls_ conj [addr source (:socket-opts conn-opts)])
             (throw (Exception. "Unavailable")))}
          #(try
             (cluster/refresh-topology! spec)
             (catch Throwable t t)))]
    [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-failed))
     (is (true? (:timed-out? (ex-data error))))
     (is (= (mapv first @calls_) [["seed-a.redis" 7000]])
       "An exhausted sweep never advances to the next Cluster candidate")
     (is (truss/submap? (nth (first @calls_) 2)
           {:connect-timeout-ms 15
            :read-timeout-ms    15
            :ready-timeout-ms   15}))])

  (let [events_ (atom [])
        spec
        (cluster/cluster-spec [["seed.redis" 7000]]
          {:cbs {:on-refresh-error #(swap! events_ conj %)}})
        topology
        (with-redefs-fn
          {#'cluster/raw-topology-reply! (fn [& _] (test-shards-reply 10))}
          #(cluster/refresh-topology! spec))
        stale
        (with-redefs-fn
          {#'utils/remaining-timeout-ms (constantly nil)}
          #(cluster/refresh-topology! spec))]
    [(is (identical? topology stale))
     (is (true? (:stale-served? (first @events_))))
     (is (true? (:timed-out? (first @events_))))])

  (let [calls_     (atom [])
        remaining_ (atom [30 25 20 10 8 5])
        spec       (cluster/cluster-spec [["seed.redis" 7000]])
        topology
        (with-redefs-fn
          {#'utils/remaining-timeout-ms
           (fn [_]
             (let [remaining (first @remaining_)]
               (swap! remaining_ next)
               remaining))
           #'cluster/raw-topology-reply!
           (fn [conn-opts addr source]
             (swap! calls_ conj [source (get-in conn-opts [:socket-opts :read-timeout-ms])])
             (if (= source :cluster-shards)
               (test-reply-error "ERR unknown subcommand 'SHARDS'")
               [[0 16383 [(first addr) (second addr) "master"]]]))}
          #(cluster/refresh-topology! spec))]
    [(is (:complete? topology))
     (is (= @calls_ [[:cluster-shards 25] [:cluster-slots 10]])
       "The same-node SLOTS fallback recomputes its cap from remaining budget")])

  (let [spec (cluster/cluster-spec [["slow.redis" 7000]]
               {:refresh-timeout-ms 25})
        t0   (System/nanoTime)
        error
        (with-redefs-fn
          {#'cluster/raw-topology-reply!
           (fn [& _]
             (Thread/sleep 2000)
             (test-shards-reply 10))}
          #(try
             (cluster/refresh-topology! spec)
             (catch Throwable t t)))
        elapsed-ms (quot (- (System/nanoTime) t0) 1000000)]
    [(is (= (:eid (ex-data error)) :carmine.cluster/refresh-failed))
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
           #'cluster/raw-topology-reply! (fn [& _] (test-shards-reply 10))}
          #(cluster/refresh-topology! spec))]
    [(is (:complete? topology)
       "The deadline bounds discovery I/O: a fully-received topology is kept even when the deadline expires after its fetch")
     (is (identical? topology (cluster/cached-topology spec)))]))

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
          (test-shards-reply 10))
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
                     parked?
                     (loop [deadline (+ (System/currentTimeMillis) 5000)]
                       (let [refresh-waiting?
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
                         (cond
                           (every? refresh-waiting? @threads_) true
                           (>= (System/currentTimeMillis) deadline) false
                           :else (do (Thread/sleep 1) (recur deadline)))))]
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
              (let [parked?
                    (loop [deadline (+ (System/currentTimeMillis) 5000)]
                      (let [refresh-waiting?
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
                        (cond
                          (every? refresh-waiting? @threads_) true
                          (>= (System/currentTimeMillis) deadline) false
                          :else (do (Thread/sleep 1) (recur deadline)))))]
                (is parked? "Every task joined the shared failing refresh"))
              (deliver release true)
              (mapv #(deref % 10000 ::timeout) tasks)))]
      [(is (not-any? #{::timeout} results))
       (is (every? #(= (:eid (ex-data %)) :carmine.cluster/refresh-failed) results))
       (is (= @calls_ 2)
         "Concurrent joiners share one failed sweep across both seed addresses")])

    (with-redefs-fn
      {#'cluster/raw-topology-reply! (fn [& _] (test-shards-reply 20))}
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
         (test-shards-reply 10))}
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
       (fn [& _] (swap! calls_ inc) (test-shards-reply 10))
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
      {#'cluster/raw-topology-reply! (fn [& _] (test-shards-reply 10))}
      (fn []
        (let [refreshed (cluster/cluster-refresh-topology! spec)
              cached    (cluster/cluster-cached-topology   spec)]
          [(is (= refreshed cached))
           (is (= (dissoc refreshed :updated-at-ms)
                 {:shards
                  [{:slot-ranges [[0 16383]]
                    :master
                    {:node-id "master"
                     :addr     ["master.redis" 7000]
                     :tls-addr ["master.redis" 17000]}
                    :replicas
                    [{:node-id "replica"
                      :addr     ["replica.redis" 7001]
                      :tls-addr ["replica.redis" 17001]}]}]
                  :complete? true})
             "The public projection exposes exactly the documented stable keys")
           (is (pos-int? (:updated-at-ms refreshed)))])))
    (is (->> (cluster/cluster-cached-topology {})
          (throws? :ex-info {:eid :carmine.cluster/invalid-spec})))
    (is (->> (cluster/cluster-refresh-topology! nil)
          (throws? :ex-info {:eid :carmine.cluster/invalid-spec})))))

(deftest _cluster-refresh-tls-candidates
  (let [spec (cluster/cluster-spec [["tls-seed.redis" 17000]]
               {:conn-opts {:socket-opts {:ssl true}}})]
    (with-redefs-fn
      {#'cluster/raw-topology-reply! (fn [& _] (test-shards-reply 10))}
      #(cluster/refresh-topology! spec))
    [(is (= (:known-addrs @spec)
           [["master.redis" 17000]
            ["replica.redis" 17001]
            ["tls-seed.redis" 17000]]))
     (is (not (some #{["master.redis" 7000]} (:known-addrs @spec))))]))

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
             true false pending-reqs (java.util.LinkedList.) nil nil nil nil)]
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
           {:source :cluster-slots
            :n-slots 5
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

  (let [shards-reply
        [{"slots" [0 2 4 4]
          "nodes"
          [["id" "master-1"
            "endpoint" "master.redis"
            "ip" "10.0.0.1"
            "port" 7000
            "role" "master"
            "health" "online"
            "replication-offset" 91
            "future-field" "preserved"]
           {:id "replica-1"
            :endpoint ""
            :ip "10.0.0.2"
            :tls-port 7443
            :role "replica"
            :health "loading"
            :replication-offset 89}]}]
        topology (cluster/parse-cluster-shards ["seed.redis" 6379] shards-reply)
        range-1  (get-in topology [:slot-ranges [0 2]])
        range-2  (get-in topology [:slot-ranges [4 4]])]

    [(is (= (:master range-1) (:master range-2))
       "Non-contiguous ranges normalize to the same shard description")
     (is (truss/submap? (:master range-1)
           {:id "master-1"
            :addr ["master.redis" 7000]
            :role :master
            :health :online
            :replication-offset 91}))
     (is (= (get-in range-1 [:master :attrs :future-field]) "preserved")
       "Unknown node attributes survive normalization")
     (is (truss/submap? (first (:replicas range-1))
           {:addr nil
            :tls-addr ["seed.redis" 7443]
            :role :replica
            :health :loading}))
     (is (= (:missing-slot-ranges topology) [[3 3] [5 16383]]))])

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
     (is (->> (cluster/parse-cluster-shards
                [{:slots [0 1 2], :nodes []}])
           (throws? :ex-info
             {:eid :carmine.cluster/invalid-topology
              :problem :invalid-shard-slots})))
     (let [topology
           (cluster/parse-cluster-shards
             [{:slots []
               :nodes
               [{:id "old-master", :endpoint "old", :port 7000
                 :role "master", :health "fail"}
                {:id "new-master", :endpoint "new", :port 7001
                 :role "master", :health "online"}]}])]
       [(is (= (get-in topology [:shards 0 :master :id]) "new-master")
          "A sole healthy master is selected during a failover transition")
        (is (= (mapv :id (get-in topology [:shards 0 :masters]))
              ["old-master" "new-master"]))
        (is (= (get-in (first (#'cluster/target-sources
                                topology false {:kind :any}))
                 [:source :node-id])
              "new-master")
          "Single-node routing uses the selected healthy master")
        (is (= (mapv #(get-in % [:source :node-id])
                 (#'cluster/target-sources topology false {:kind :masters}))
              ["new-master"])
          "Master broadcasts exclude a failed former master")
        (is (= (mapv #(get-in % [:source :node-id])
                 (#'cluster/target-sources topology false {:kind :nodes}))
              ["old-master" "new-master"])
          "All-node broadcasts retain routable raw failover membership")
        (is (= (:missing-slot-ranges topology) [[0 16383]]))])

     (let [topology
           (cluster/parse-cluster-shards
             [{:slots []
               :nodes
               [{:id "old-master", :endpoint "?", :port 7000
                 :role "master", :health "fail"}
                {:id "new-master", :endpoint "new", :port 7001
                 :role "master", :health "online"}]}])]
       (is (= (mapv #(get-in % [:source :node-id])
                (#'cluster/target-sources topology false {:kind :nodes}))
             ["new-master"])
         "An unroutable failed former master cannot block an all-node broadcast"))

     (let [topology
           (cluster/parse-cluster-shards
             [{:slots [0 1]
               :nodes
               [{:id "replica", :endpoint "replica", :port 7001
                 :role "replica", :health "online"}]}])]
       [(is (nil? (get-in topology [:slot-ranges [0 1] :master])))
        (is (= (:unroutable-slot-ranges topology) [[0 1]]))])])

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
     (is (->> (#'cluster/target-sources topology true {:kind :slot, :slot 0})
           (throws? :ex-info
             {:eid :carmine.cluster/no-route
              :problem :transport-unavailable
              :transport :tls}))
       "TLS execution never falls back to a plaintext-only advertised route")])

  (let [plain-master {:id "plain", :role :master, :addr ["plain.redis" 7000]}
        tls-master   {:id "tls", :role :master, :tls-addr ["tls.redis" 7443]}
        topology {:shards [{:master plain-master, :nodes [plain-master]}
                           {:master tls-master,   :nodes [tls-master]}]}
        tls-source (first (#'cluster/target-sources topology true {:kind :any}))]
    (is (= tls-source
          {:addr ["tls.redis" 7443]
           :source {:node-id "tls", :role :master, :addr ["tls.redis" 7443]}})
     "`:any` chooses the first master compatible with the manager transport"))

  (let [addr-a ["a.redis" 7000]
        addr-b ["b.redis" 7001]
        calls_ (atom [])
        entries
        [{:request :a1, :target {:kind :slot, :slot 1}}
         {:request :local, :local? true}
         {:request :broadcast, :target {:kind :nodes}}
         {:request :a2, :target {:kind :slot, :slot 1}}]
        plan
        (cluster/plan-requests entries
          {:target->sources
           (fn [target]
             (swap! calls_ conj target)
             (case (:kind target)
               :slot [{:addr addr-a, :source {:node-id "a", :addr addr-a}}]
               :nodes [{:addr addr-a, :source {:node-id "a", :addr addr-a}}
                       {:addr addr-b, :source {:node-id "b", :addr addr-b}}]))})]
    [(is (= (:n plan) 4))
     (is (= (:n-tasks plan) 4))
     (is (= (:broadcast-indexes plan) #{2}))
     (is (= (:local plan) [{:index 1, :request :local}]))
     (is (= (mapv :addr (:partitions plan)) [addr-a addr-b]))
     (is (= (mapv #(mapv :request (:entries %)) (:partitions plan))
           [[:a1 :broadcast :a2] [:broadcast]]))
     (is (= @calls_ [{:kind :slot, :slot 1} {:kind :nodes}])
       "Each distinct explicit target is resolved at most once")])

  [(is (= (cluster/plan-requests
            [{:request :a, :target {:kind :any}}]
            {:target->sources
             (constantly [{:addr ["target.redis" 7000], :source :target}])})
         {:n 1
          :n-tasks 1
          :partitions [{:addr ["target.redis" 7000]
                        :entries [{:index 0, :task-id 0, :request :a
                                   :source :target, :broadcast? false}]}]
          :local []
          :broadcast-indexes #{}})
     "Every network request carries an explicit target")

   (is (= (cluster/plan-requests
            [{:request nil, :local? true}]
            {})
         {:n 1, :n-tasks 0, :partitions []
          :local [{:index 0, :request nil}], :broadcast-indexes #{}})
     "All-local batches do not require a target resolver")

   (is (->> (cluster/plan-requests
              [{:request :a}]
              {:target->sources (constantly [])})
         (throws? :ex-info
           {:eid :carmine.cluster/invalid-plan-entry
            :problem :missing-target})))

   (is (->> (cluster/plan-requests
              [{:request :a, :supports-cluster? false}
               {:request :b, :target {:kind :slot, :slot 9}}
               {:request :c, :supports-cluster? false}]
              {:target->sources (constantly nil)})
         (throws? :ex-info
           {:eid :carmine.cluster/unsupported-command
            :indexes [0 2]}))
     "Permanent command errors take precedence over routing errors")

   (is (->> (cluster/plan-requests
              [{:request :a, :target {:kind :slot, :slot 9}}]
              {:target->sources (constantly nil)})
         (throws? :ex-info
           {:eid :carmine.cluster/no-route
            :problem :empty-target-set
            :index 0})))

   (is (->> (cluster/plan-requests
              [{:request :a, :local? true, :target {:kind :any}}]
              {})
         (throws? :ex-info
           {:eid :carmine.cluster/invalid-plan-entry
           :problem :local-with-target})))]

  (let [reply-error (com/reply-error "Expected" {:eid :test/reply})
        skipped     com/sentinel-skipped-reply
        indexed     [[4 :e] [2 nil] [0 :a] [3 reply-error] [1 skipped]]
        replies     (cluster/stitch-replies 5 indexed)]
    [(is (= (subvec replies 0 3) [:a skipped nil]))
     (is (identical? (get replies 3) reply-error))
     (is (= (get replies 4) :e))
     (is (= (cluster/stitch-replies 0 []) []))
     (is (->> (cluster/stitch-replies 2 [[0 :a] [0 :b]])
           (throws? :ex-info
             {:eid :carmine.cluster/reply-mismatch
              :problem :duplicate-index})))
     (is (->> (cluster/stitch-replies 2 [[0 :a]])
           (throws? :ex-info
             {:eid :carmine.cluster/reply-mismatch
              :problem :missing-indexes
              :indexes [1]})))
     (is (->> (cluster/stitch-replies 1 [[1 :a]])
           (throws? :ex-info
             {:eid :carmine.cluster/reply-mismatch
              :problem :index-out-of-range})))
     (is (->> (cluster/stitch-replies 1 [[0]])
           (throws? :ex-info
             {:eid :carmine.cluster/reply-mismatch
              :problem :invalid-indexed-reply})))

     (doseq [_ (range 50)]
       (let [expected (mapv #(if (zero? (mod % 7)) nil %) (range 32))
             indexed  (shuffle (mapv vector (range 32) expected))]
         (is (= (cluster/stitch-replies 32 indexed) expected)
           "Arbitrary cross-node reply arrival order is restored")))]))

(deftest _cluster-broadcast-replica-routability
  (let [master  {:id "m1", :role :master,  :addr ["m1.redis" 7000]}
        replica {:id "r1", :role :replica, :addr ["r1.redis" 7001]}
        ghost   {:id "r2", :role :replica} ; E.g. "?" endpoint, no address
        topology {:shards [{:master master, :nodes [replica master ghost]}]}]
    [(is (= (mapv :addr (#'cluster/target-sources topology false {:kind :nodes}))
           [["r1.redis" 7001] ["m1.redis" 7000]])
       "A `:nodes` broadcast skips unroutable non-masters, preserving topology order")
     (is (= (mapv :addr (#'cluster/target-sources topology false {:kind :masters}))
           [["m1.redis" 7000]]))
     (let [unroutable-master {:id "m2", :role :master}]
       (is (->> (#'cluster/target-sources
                  {:shards [{:master unroutable-master
                             :nodes [unroutable-master replica]}]}
                  false {:kind :nodes})
             (throws? :ex-info
               {:eid :carmine.cluster/no-route
                :problem :transport-unavailable}))
         "An unroutable selected master still fails the whole broadcast"))]))

(deftest _cluster-slots-discovery-retains-transport
  (let [spec
        (cluster/cluster-spec [["seed.redis" 7443]]
          {:topology-source :cluster-slots
           :conn-opts {:socket-opts {:ssl true}}})]
    (with-redefs-fn
      {#'cluster/raw-topology-reply!
       (fn [_conn-opts _addr _source]
         [[0 16383 ["master.redis" 7443 "master-tls"]]])}
      #(cluster/refresh-topology! spec))
    (let [topology (:topology @spec)
          source (first (#'cluster/target-sources topology true {:kind :slot, :slot 1}))]
      [(is (= (:source topology) :cluster-slots))
       (is (= (:addr source) ["master.redis" 7443]))
       (is (= (get-in source [:source :node-id]) "master-tls"))])))

(defn- settle-test-future! [f]
  (when (= (try
             (deref f 5000 ::timeout)
             (catch Throwable _ ::settled))
           ::timeout)
    (future-cancel f)))

(deftest _cluster-executor
  (let [spec   (cluster/cluster-spec [["seed.redis" 7000]]
                 {:topology-source :cluster-slots})
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
        spec   (cluster/cluster-spec [["seed.redis" 7000]]
                 {:topology-source :cluster-slots})
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
            (fn [responses body-fn]
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
                   #(car/with-car mgr {:as-vec? true, :error-mode :return}
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
                #(com/parse {:parse-error-replies? true}
                   (fn [reply] [:parsed reply])
                   (resp/rcmd "GET" key)))]
          [(is (= reply [[:parsed "MOVED-OK"]])
             "Error-parsing parsers never mask Cluster redirections")
           (is (= (mapv :addr calls) [addr-a addr-c]))
           (is (= refreshes 1))])

        (let [[reply _ _]
              (run-case
                [{:addr addr-a, :wire "-ERR final\r\n"}]
                #(com/parse {:parse-error-replies? true}
                   (fn [reply] [:parsed (:code (ex-data reply))])
                   (resp/rcmd "GET" key)))]
          (is (= reply [[:parsed "ERR"]])
            "Deferred error parsing still applies to final Redis errors"))

        (let [n-calls* (atom 0)
              [reply _ _]
              (run-case
                [{:addr addr-a, :wire "+OK\r\n"}]
                #(com/parse {:parse-error-replies? true}
                   (fn [_reply] (swap! n-calls* inc) (throw (Exception. "Parser boom")))
                   (resp/rcmd "GET" key)))]
          [(is (= @n-calls* 1)
             "A parser that throws on a successful reply runs exactly once:
              its parser-error is never re-parsed by the deferred transform")
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

(deftest _cluster-broadcast-retries-retain-source
  (let [spec (cluster/cluster-spec [["seed.redis" 7000]])
        req  (taoensso.carmine_v4.resp.Req.
               com/read-opts-natural ["PING"] nil {:kind :nodes} true nil 0)
        tasks [{:index 0, :task-id 0, :request req, :addr ["a.redis" 7000]
                :source {:node-id "a"}, :broadcast? true, :asking? false}
               {:index 0, :task-id 1, :request req, :addr ["b.redis" 7001]
                :source {:node-id "b"}, :broadcast? true, :asking? false}]
        round_ (atom 0)
        retried-addrs_ (atom nil)
        retry-error
        (com/reply-error "TRYAGAIN"
          {:eid :carmine.read/redis-error-reply, :code "TRYAGAIN", :message "later"})
        final
        (with-redefs-fn
          {#'cluster/execute-round!
           (fn [_ tasks _]
             (if (= 1 (swap! round_ inc))
               (mapv #(assoc % :reply retry-error) tasks)
               (do
                 (reset! retried-addrs_ (mapv :addr tasks))
                 (mapv #(assoc % :reply (get-in % [:source :node-id])) tasks))))}
          #(#'cluster/execute-with-retries! nil spec
             {:max-retry-rounds 1, :retry-backoff-ms 0
              :max-concurrent-partitions 1}
             tasks))]
    [(is (= @retried-addrs_ [["a.redis" 7000] ["b.redis" 7001]])
       "A transient broadcast retry stays on each task's original source")
     (is (= final {0 "a", 1 "b"}))]))

(deftest _cluster-executor-retry-no-route-isolation
  (let [spec   (cluster/cluster-spec [["seed.redis" 7000]])
        key    (cluster/cluster-key "no-route-key")
        slot   (cluster/cluster-slot key)
        req    (fn [k]
                 (taoensso.carmine_v4.resp.Req.
                   com/read-opts-natural ["GET" k] slot
                   {:kind :slot, :slot slot} true nil 0))
        addr-a ["a.redis" 7000]
        tasks  [{:index 0, :task-id 0, :request (req "k0"), :addr addr-a, :asking? false}
                {:index 1, :task-id 1, :request (req "k1"), :addr addr-a, :asking? false}]
        retry-error
        (com/reply-error "CLUSTERDOWN"
          {:eid :carmine.read/redis-error-reply
           :code "CLUSTERDOWN", :message "unavailable"})
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
                 (fn [{:keys [task-id] :as task}]
                   (assoc task :reply (if (zero? ^long task-id) retry-error "ok-1")))
                 tasks)
               (do
                 (reset! retried-tasks_ tasks)
                 (mapv #(assoc % :reply "ok-0") tasks))))}
          #(#'cluster/execute-with-retries! nil spec
             {:max-retry-rounds 1, :retry-backoff-ms 0
              :max-concurrent-partitions 1}
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
        task {:index 0, :task-id 0, :request req
              :addr ["a.redis" 7000], :asking? false}
        retry-error
        (com/reply-error "CLUSTERDOWN"
          {:eid :carmine.read/redis-error-reply
           :code "CLUSTERDOWN", :message "unavailable"})]
    (is (->> (with-redefs-fn
               {#'cluster/current-task-addr
                (fn [& _]
                  (truss/ex-info! "[Carmine] Failed to refresh Redis Cluster topology"
                    {:eid :carmine.cluster/refresh-failed}))
                #'cluster/refresh-for-execution! (fn [& _])
                #'cluster/execute-round!
                (fn [_ tasks _] (mapv #(assoc % :reply retry-error) tasks))}
               #(#'cluster/execute-with-retries! nil spec
                  {:max-retry-rounds 1, :retry-backoff-ms 0
                   :max-concurrent-partitions 1}
                  [task]))
          (throws? :ex-info {:eid :carmine.cluster/refresh-failed}))
      "Only routing gaps are isolated: re-resolution refresh failures still escape")))

(deftest _cluster-exhausted-topology-error-stales-cache
  (doseq [[code kind]
          [["MOVED" :moved]
           ["CLUSTERDOWN" :cluster-down]]]
    (let [spec (cluster/cluster-spec [["seed.redis" 7000]]
                 {:topology-source :cluster-slots})
          key  (cluster/cluster-key (str "exhausted-" code))
          slot (cluster/cluster-slot key)
          req  (taoensso.carmine_v4.resp.Req.
                 com/read-opts-natural ["GET" key] nil
                 {:kind :slot, :slot slot} true nil 0)
          task {:index 0, :task-id 0, :request req
                :addr ["a.redis" 7000], :asking? false}
          message (if (= kind :moved)
                    (str "MOVED " slot " c.redis:7002")
                    "CLUSTERDOWN unavailable")
          reply (com/reply-error message
                  {:eid :carmine.read/redis-error-reply
                   :code code, :message message})]
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
                  :max-concurrent-partitions 1}
                 [task]))]
        [(is (identical? (get final 0) reply)
           "An exhausted retry budget preserves the final Redis reply")
         (is (nil? (cluster/cached-topology spec))
           (str code " marks the cache stale for the next request"))]))))

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
