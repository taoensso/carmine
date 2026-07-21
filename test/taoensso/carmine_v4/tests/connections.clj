(ns taoensso.carmine-v4.tests.connections
  "Carmine v4 connection, manager lifecycle, and request-context tests."
  (:require
   [clojure.string      :as str]
   [clojure.test        :refer [deftest testing is]]
   [taoensso.encore     :as enc]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine-v4          :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.cluster  :as cluster]
   [taoensso.carmine-v4.tests.test-support :as support])
  (:import
   [org.apache.commons.pool2.impl GenericObjectPool]))

(def tk  "Test key" support/test-key)
(def tc+ "Parsed test conn-opts" support/parsed-test-conn-opts)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

;;;; Conns

(defn- test-manager [mgr_]
  (let [v   (volatile! [])
        v+ #(vswap! v conj %)]

    (with-open [mgr ^java.io.Closeable (force mgr_)]
      (v+
        (car/with-car mgr
          (fn []
            [(v+ (resp/ping))
             (v+ (car/with-replies (resp/rcmd "ECHO" "x")))])))

      [@v mgr])))

(deftest _conn-init-reqs
  (let [reqs (fn [init-opts] (#'conns/conn-init-reqs init-opts))]
    [(is (= (reqs
              {:resp3? false
               :auth {:password "pass"}
               :client-name nil})
           [["AUTH" "pass"]])
       "RESP2 uses legacy AUTH when no username was supplied")

     (is (= (reqs
              {:resp3? false
               :auth {:username "alice", :password "pass"}
               :client-name nil})
           [["AUTH" "alice" "pass"]])
       "RESP2 uses ACL AUTH when a username was supplied")

     (is (= (reqs
              {:resp3? false
               :auth {:username "alice"}
               :client-name nil})
           [["AUTH" "alice" ""]])
       "RESP2 authenticates password-free ACL users")

     (is (= (reqs
              {:resp3? false
               :auth {:username "alice", :password "pass"}
               :client-name "client"
               :select-db 3})
           [["AUTH" "alice" "pass"]
            ["CLIENT" "SETNAME" "client"]
            ["SELECT" 3]]))

     (is (= (reqs
              {:resp3? true
               :auth {:password "pass"}
               :client-name nil})
           [["HELLO" 3 "AUTH" "default" "pass"]])
       "RESP3 HELLO falls back to Redis's default ACL user")

     (is (= (reqs
              {:resp3? true
               :auth {:username "alice", :password "pass"}
               :client-name "client"
               :select-db 3})
           [["HELLO" 3 "AUTH" "alice" "pass" "SETNAME" "client"]
            ["SELECT" 3]]))

     (is (= (reqs
              {:resp3? true
               :auth {:username "alice"}
               :client-name nil})
           [["HELLO" 3 "AUTH" "alice" ""]])
       "RESP3 authenticates password-free ACL users")

     (is (= (reqs
              {:commands [["PING"]]
               :resp3? false
               :auth {:password "ignored"}
               :client-name "ignored"
               :select-db 3})
           [["PING"]])
       "Explicit commands remain a complete override")

     (is (nil? (reqs {:commands [[]]}))
       "Only empty commands mean no initialization work")
     (is (= (reqs {:commands [[] ["PING"] []]}) [["PING"]])
       "Empty commands are ignored during programmatic construction")]))

(deftest _valid-ping-reply
  [(is (#'conns/valid-ping-reply? "PONG")
     "Normal Redis PING replies are accepted")
   (is (#'conns/valid-ping-reply? ["pong" ""])
     "Subscribed-mode Redis PING replies are accepted")
   (is (not (#'conns/valid-ping-reply? ["ping" ""]))
     "The request name is not mistaken for the subscribed-mode reply")])

(deftest _basic-conns
  [(is (= (conns/with-new-conn tc+
            (fn [conn in out]
              [(#'conns/conn?       conn)
               (#'conns/conn-ready? conn)
               (resp/basic-ping!  in out)
               (resp/with-replies in out false false
                 (fn [] (resp/rcmd "ECHO" "x")))]))
         [true true "PONG" "x"])
     "Unmanaged conn")

   (is (= (conns/with-new-conn
            (opts/parse-conn-opts :redis
              {:init {:resp3? false, :client-name "carmine-test-resp2-init"}})
            (fn [conn in out]
              (resp/with-replies in out false false
                (fn [] (resp/rcmd "CLIENT" "GETNAME")))))
         "carmine-test-resp2-init")
     "RESP2 conn init sends CLIENT SETNAME even without auth/select-db")

   (let [[v mgr] (test-manager (delay (conns/conn-manager-unpooled {})))]
     [(is (= v [nil "x" "PONG"]))
      (support/assert-manager-stats-schema! :unpooled mgr)
      (is (truss/submap? @mgr
            {:open? false
             :stats {:schema-version 1, :kind :unpooled
                     :connections {:active 0, :idle nil, :waiting nil}
                     :counts {:created 1, :failed 0}}}))])

   (let [[v mgr] (test-manager (delay (conns/conn-manager-pooled {})))]
     [(is (= v [nil "x" "PONG"]))
      (support/assert-manager-stats-schema! :pooled mgr)
      (is (truss/submap? @mgr
            {:open? false,
             :stats {:schema-version 1, :kind :pooled
                     :connections {:idle 0, :waiting 0, :active 0}
                     :counts {:returned 1, :created 1, :cleared 0
                              :destroyed {:total 1}, :borrowed 1, :failed 0}}}))])])

(defn- trickle-incomplete-reply [^java.net.ServerSocket server]
  (let [writes_ (atom 0)
        result
        (future
          (with-open [sock (.accept server)]
            (let [in  (.getInputStream sock)
                  out (.getOutputStream sock)]
              (try
                (.read in) ; Wait for the client request
                (.write out (int \+))
                (.flush out)
                (swap! writes_ inc)
                (dotimes [_ 100]
                  (Thread/sleep 50)
                  (.write out (int \a))
                  (.flush out)
                  (swap! writes_ inc))
                :exhausted
                (catch Throwable _ :closed)))))]
    [writes_ result]))

(deftest _conn-operation-timeouts
  ;; A server that accepts TCP but never replies must not hang connection
  ;; creation: the init reply exchange is bounded by `:init-timeout-ms`.
  (with-open [silent-server (java.net.ServerSocket. 0)]
    (let [port (.getLocalPort silent-server)
          t0   (System/currentTimeMillis)
          err
          (truss/throws
            (conns/new-conn
              (opts/parse-conn-opts :redis
                {:server ["127.0.0.1" port]
                 :socket-opts {:init-timeout-ms 150}})))
          elapsed-ms (- (System/currentTimeMillis) t0)
          ;; Per-read so-timeout and the aggregate watchdog share one
          ;; duration: either may win (SocketTimeoutException vs the
          ;; watchdog's close => SocketException/deadline error)
          caused-by-init-bound?
          (loop [t err]
            (cond
              (nil? t) false
              (instance? java.net.SocketTimeoutException t) true
              (instance? java.net.SocketException t) true
              (= (:eid (ex-data t)) :carmine.conns/conn-init-deadline) true
              :else (recur (.getCause ^Throwable t))))]
      [(is (= (:eid (ex-data err)) :carmine.conns/new-conn-error))
       (is caused-by-init-bound?)
       (is (< elapsed-ms 5000)
         "Init fails within the configured bound, not indefinitely")]))

  (testing "Aggregate deadline vs byte-trickling server"
    ;; Per-read so-timeouts alone never fire against a server that keeps
    ;; trickling bytes; the aggregate watchdog must close the socket.
    (with-open [trickle-server (java.net.ServerSocket. 0)]
      (let [port (.getLocalPort trickle-server)
            [writes_ server-f] (trickle-incomplete-reply trickle-server)
            result
            (future
              (truss/throws
                (conns/new-conn
                  (opts/parse-conn-opts :redis
                    {:server ["127.0.0.1" port]
                     :socket-opts {:init-timeout-ms 300}}))))
            err (deref result 3000 ::timeout)]
        [(is (not= err ::timeout)
           "Initialization fails within the aggregate deadline despite trickled bytes")
         (is (= (:eid (ex-data err)) :carmine.conns/new-conn-error))
         (is (= (deref server-f 3000 ::timeout) :closed)
           "The deadline-closed socket ends the server's write loop")
         (is (>= (long @writes_) 3)
           "The server really did keep trickling bytes across per-read timeouts")])))

  (testing "Readiness aggregate deadline vs byte-trickling server"
    (doseq [[label socket-opts]
            [["explicit readiness timeout" {:ready-timeout-ms 300}]
             ["ambient read timeout"
              {:read-timeout-ms 300, :ready-timeout-ms nil}]]]
      (with-open [trickle-server (java.net.ServerSocket. 0)]
        (let [port (.getLocalPort trickle-server)
              [writes_ server-f] (trickle-incomplete-reply trickle-server)
              conn
              (conns/new-conn
                (opts/parse-conn-opts :redis
                  {:server ["127.0.0.1" port]
                   :init {:commands []}
                   :socket-opts socket-opts}))]
          (try
            (let [t0 (System/currentTimeMillis)
                  ready? (#'conns/conn-ready? conn)
                  elapsed-ms (- (System/currentTimeMillis) t0)]
              [(is (false? ready?) label)
               (is (< elapsed-ms 3000)
                 (str label " is an aggregate deadline"))
               (is (= (deref server-f 3000 ::timeout) :closed)
                 (str label " closes the trickling socket"))
               (is (>= (long @writes_) 3)
                 (str label " outlasted individual trickled reads"))])
            (finally
              (#'conns/conn-close! conn {:via 'test}))))))))

(deftest _conn-callbacks
  (testing "Successful close"
    (let [global-events_ (atom [])
          local-events_  (atom [])
          conn_          (volatile! nil)
          conn-opts
          (opts/parse-conn-opts :redis
            {:cbs {:on-conn-close #(swap! local-events_ conj %)}})]

      (binding [car/*conn-cbs* {:on-conn-close #(swap! global-events_ conj %)}]
        (is (= (conns/with-new-conn conn-opts
                  (fn [conn in out]
                    (vreset! conn_ conn)
                    (resp/basic-ping! in out)))
              "PONG")))

      (let [event (first @local-events_)]
        [(is (= @global-events_ @local-events_)
           "Dynamic and connection-local callbacks receive the same event")
         (is (truss/submap? event
               {:cbid :on-conn-close
                :via 'with-new-conn
                :host "127.0.0.1"
                :port 6379
                :data {:via 'with-new-conn}
                :closed? true}))
         (is (identical? (:conn event) @conn_))
         (is (false? (#'conns/conn-open? @conn_)))
         (is (nil?
               (#'conns/conn-close! @conn_ {:via 'repeated-close})))
         (is (= (count @local-events_) 1)
           "Repeated close is a no-op and emits no additional callback")])))

  (testing "Failed initialization"
    (let [events_ (atom [])
          conn-opts
          (opts/parse-conn-opts :redis
            {:cbs
             {:on-conn-close #(swap! events_ conj [:close %])
              :on-conn-error #(swap! events_ conj [:error %])}
             :init {:commands [["CARMINE-INVALID-COMMAND" "private-argument"]]}})]

      (is (->> (conns/with-new-conn conn-opts (fn [& _] nil))
            (throws? :ex-info {:eid :carmine.conns/new-conn-error})))

      (let [[[_ init-error] [_ close-event] [_ new-conn-error]] @events_]
        [(is (= (mapv (fn [[kind event]] [kind (:eid event) (:via event)]) @events_)
               [[:error :carmine.conns/conn-init-error 'conn-init!]
                [:close nil                            'new-conn]
                [:error :carmine.conns/new-conn-error  'new-conn]]))
         (is (= (get-in init-error [:conn-opts :init :commands])
               :carmine/redacted))
         (is (= (get-in init-error [:replies 0])
               {:request ["CARMINE-INVALID-COMMAND" :carmine/redacted]
                :reply :carmine/redacted}))
         (is (= (get-in close-event [:data :via]) 'new-conn))
         (is (instance? Throwable (get-in close-event [:data :cause])))
         (is (false? (#'conns/conn-open? (:conn close-event))))
         (is (= (:eid new-conn-error) :carmine.conns/new-conn-error))])))

  (testing "Failed socket wrapping"
    (with-open [server (java.net.ServerSocket. 0)]
      (let [socket_       (volatile! nil)
            close-events_ (atom [])
            error-events_ (atom [])
            conn-opts
            (opts/parse-conn-opts :redis
              {:server ["127.0.0.1" (.getLocalPort server)]
               :cbs
               {:on-conn-close #(swap! close-events_ conj %)
                :on-conn-error #(swap! error-events_ conj %)}
               :socket-opts
               {:connect-timeout-ms 500
                :ssl
                (fn [{:keys [socket]}]
                  (vreset! socket_ socket)
                  (throw (Exception. "Simulated SSL failure")))}})]

        [(is (->> (conns/with-new-conn conn-opts (fn [& _] nil))
               (throws? :ex-info {:eid :carmine.conns/new-conn-error})))
         (is (= (mapv (juxt :eid :via) @error-events_)
               [[:carmine.conns/new-conn-error 'new-conn]]))
         (is (empty? @close-events_)
           "A failure before Conn construction emits no close callback")
         (let [^java.net.Socket socket @socket_]
           (is (.isClosed socket)
             "The connected raw socket is closed when a custom SSL fn throws"))])))

  (testing "Invalid custom SSL return value"
    (with-open [server (java.net.ServerSocket. 0)]
      (let [ssl-context_ (volatile! nil)
            conn-opts
            (opts/parse-conn-opts :redis
              {:server ["127.0.0.1" (.getLocalPort server)]
               :socket-opts
               {:connect-timeout-ms 500
                :ssl
                (fn [{:keys [socket] :as ssl-context}]
                  (vreset! ssl-context_ ssl-context)
                  :not-a-socket)}})
            err (truss/throws (conns/new-conn conn-opts))
            cause (.getCause ^Throwable err)
            {:keys [socket host port]
             ssl-conn-opts :conn-opts} @ssl-context_]

        [(is (= (:eid (ex-data err)) :carmine.conns/new-conn-error))
         (is (= (:eid (ex-data cause))
               :carmine.conns/invalid-custom-ssl-socket))
         (is (= host "127.0.0.1"))
         (is (= port (.getLocalPort server)))
         (is (identical? conn-opts ssl-conn-opts))
         (is (.isClosed ^java.net.Socket socket)
           "The raw socket is closed when a custom SSL fn returns invalidly")]))))

(deftest _conn-manager-option-validation
  (let [cluster-spec (cluster/cluster-spec [["127.0.0.1" 7000]])
        constructors
        [[:unpooled conns/conn-manager-unpooled {}]
         [:pooled   conns/conn-manager-pooled {}]
         [:clustered conns/conn-manager-clustered
          {:conn-opts {:server {:cluster-spec cluster-spec}}}]]]
    (doseq [[kind constructor base-opts] constructors]
      [(is (->> (constructor (assoc base-opts :unknown true))
             (throws? :ex-info
               {:eid :carmine.conns/invalid-manager-opts
                :manager-kind kind})))
       (is (->> (constructor (assoc base-opts :push-fn :not-a-fn))
             (throws? :ex-info
               {:eid :carmine.conns/invalid-manager-opts
                :manager-kind kind})))
       (is (->> (constructor (assoc base-opts :push-queue-capacity 0))
             (throws? :ex-info
               {:eid :carmine.conns/invalid-manager-opts
                :manager-kind kind})))
       (is (->> (constructor
                  (assoc base-opts :push-queue-capacity
                    (inc (long Integer/MAX_VALUE))))
             (throws? :ex-info
               {:eid :carmine.conns/invalid-manager-opts
                :manager-kind kind})))])

    (is (->> (conns/conn-manager-unpooled nil)
          (throws? :ex-info {:eid :carmine.conns/invalid-manager-opts})))

    (is (->> (conns/conn-manager-pooled
               {:pool-opts
                {:ready-check-after-idle-ms (inc (bigint Long/MAX_VALUE))}})
          (throws? :ex-info
            {:eid :carmine.conns/invalid-ready-check-after-idle-ms})))

    (let [min-idle (java.time.Duration/ofSeconds 2)
          evict-every (java.time.Duration/ofSeconds 3)]
      (with-open [mgr
                  (conns/conn-manager-pooled
                    {:pool-opts
                     {:min-evictable-idle min-idle
                      :time-between-eviction-runs evict-every}})]
        (let [^GenericObjectPool pool
              (.-pool ^taoensso.carmine_v4.conns.ConnManagerPooled mgr)]
          [(is (= (.getMinEvictableIdleDuration pool) min-idle)
             "Caller Duration overrides the built-in millisecond alias")
           (is (= (.getDurationBetweenEvictionRuns pool) evict-every)
             "Caller eviction Duration overrides its millisecond alias")])))

    (with-open [mgr
                (conns/conn-manager-unpooled
                  {:example/extension true, :push-fn nil})]
      (is (true? (get-in @mgr [:mgr-opts :example/extension]))
        "Namespaced manager extension options are preserved"))))

(deftest _manager-construction-failures-close-owned-resources
  (let [expected-error (Exception. "Expected pool configuration failure")
        original-set-pool-opts! opts/set-pool-opts!
        original-close-dispatcher! (deref #'conns/close-push-dispatcher!)]
    (doseq [[kind constructor base-opts]
            [[:pooled conns/conn-manager-pooled {}]
             [:clustered conns/conn-manager-clustered
              {:conn-opts
               {:server {:cluster-spec
                         (cluster/cluster-spec [["127.0.0.1" 7000]])}}}]]]
      (let [pool_       (atom nil)
            dispatcher_ (atom nil)
            error
            (with-redefs-fn
              {#'opts/set-pool-opts!
               (fn [pool pool-opts]
                 (reset! pool_ pool)
                 (original-set-pool-opts! pool pool-opts)
                 (throw expected-error))

               #'conns/close-push-dispatcher!
               (fn [dispatcher]
                 (reset! dispatcher_ dispatcher)
                 (original-close-dispatcher! dispatcher))}

              #(try
                 (constructor
                   (assoc base-opts :push-fn identity))
                 (catch Throwable t t)))]
        [(is (identical? error expected-error) (name kind))
         (is (.isClosed
               ^org.apache.commons.pool2.impl.BaseGenericObjectPool @pool_)
           (str (name kind) " pool is closed"))
         (is (true?
               (:shutdown?
                 (#'conns/push-dispatcher-stats @dispatcher_)))
           (str (name kind) " push dispatcher is closed"))])))

  (testing "Cluster pool-option parsing is inside the resource guard"
    (let [dispatcher_ (atom nil)
          original-close-dispatcher! (deref #'conns/close-push-dispatcher!)
          error
          (with-redefs-fn
            {#'conns/close-push-dispatcher!
             (fn [dispatcher]
               (reset! dispatcher_ dispatcher)
               (original-close-dispatcher! dispatcher))}
            #(try
               (conns/conn-manager-clustered
                 {:conn-opts
                  {:server {:cluster-spec
                            (cluster/cluster-spec [["127.0.0.1" 7000]])}}
                  :pool-opts 1
                  :push-fn identity})
               (catch Throwable t t)))]
      [(is (instance? Throwable error))
       (is (true?
             (:shutdown?
               (#'conns/push-dispatcher-stats @dispatcher_))))]))

  (testing "Min-idle preparation failure also closes the completed pool"
    (let [pool_           (atom nil)
          cleanup-called_ (atom false)
          original-cleanup! (deref #'conns/close-construction-resources!)
          error
          (with-redefs-fn
            {#'conns/new-conn
             (fn [& _] (throw (Exception. "Expected connection failure")))

             #'conns/close-construction-resources!
             (fn [pool dispatcher cause]
               (reset! pool_ pool)
               (reset! cleanup-called_ true)
               (original-cleanup! pool dispatcher cause))}

            #(try
               (conns/conn-manager-pooled
                 {:pool-opts {:min-idle 1}})
               (catch Throwable t t)))]
      [(is (= (:eid (ex-data error)) :carmine.conns/borrow-conn-error))
       (is @cleanup-called_)
       (is (.isClosed
             ^org.apache.commons.pool2.impl.BaseGenericObjectPool @pool_))])))

(deftest _conn-manager-lifecycle
  (testing "Public lifecycle operations identify invalid managers"
    (doseq [[via f]
            [['conn-manager-open? #(car/conn-manager-open? :invalid)]
             ['conn-manager-stats #(car/conn-manager-stats :invalid)]
             ['conn-manager-clear! #(car/conn-manager-clear! :invalid)]
             ['conn-manager-close! #(car/conn-manager-close! :invalid)]]]
      (let [error (truss/throws (f))]
        (is (truss/submap? (ex-data error)
              {:eid :carmine.conns/invalid-manager, :via via})))))

  (testing "Clear result distinguishes an unpooled manager"
    (with-open [mgr (car/conn-manager-unpooled)]
      [(is (= (car/conn-manager-clear! mgr)
             {:action :not-applicable}))
       (is (car/conn-manager-open? mgr))]))

  (testing "Lifecycle wrappers accept delayed managers and concise arities"
    (let [mgr_ (delay (car/conn-manager))]
      (try
        [(is (car/conn-manager-open? mgr_))
         (is (= (car/conn-manager-clear! mgr_)
                {:action :cleared, :graceful? true}))
         (is (= (car/conn-manager-close! mgr_)
                {:action :closed, :graceful? true}))]
        (finally (car/conn-manager-close! mgr_ 0)))))

  (testing "Invalid public lifecycle input cannot change manager state"
    (with-open [mgr (conns/conn-manager-pooled {})]
      (let [error (truss/throws (car/conn-manager-close! mgr 1000 :bad-data))]
        [(is (truss/submap? (ex-data error)
                {:eid :carmine.conns/invalid-close-data
                 :via 'conn-manager-close!}))
         (is (car/conn-manager-open? mgr))])))

  (doseq [[label new-mgr]
          [["Unpooled" #(conns/conn-manager-unpooled {})]
           ["Pooled"   #(conns/conn-manager-pooled   {})]]]
    (testing label
      (let [mgr (new-mgr)]
        (try
          [(is (car/conn-manager-open? mgr))
           (is (= (wcar mgr (resp/ping)) "PONG"))
           (is (= (car/conn-manager-close! mgr 1000 {:reason :test})
                 {:action :closed, :graceful? true}))
           (is (false? (car/conn-manager-open? mgr)))
           (is (= (car/conn-manager-close! mgr 1000 {:reason :again})
                 {:action :already-closed}))
           (let [cleared-before (get-in @mgr [:stats :counts :cleared])]
             [(is (= (car/conn-manager-clear! mgr 1000)
                    {:action :already-closed}))
              (is (= (get-in @mgr [:stats :counts :cleared]) cleared-before)
                "Clear racing after close is a no-op and is not counted")])
           (is (->> (car/with-car mgr (fn [] nil))
                 (throws? :ex-info {:eid :carmine.conns/manager-closed})))]
          (finally (car/conn-manager-close! mgr 0 nil))))))

  (testing "Pooled clear and close callbacks retain their cause"
    (let [close-events_ (atom [])
          mgr
          (conns/conn-manager-pooled
            {:conn-opts
             {:cbs {:on-conn-close #(swap! close-events_ conj %)}}})]
      (try
        (is (= (wcar mgr (resp/ping)) "PONG"))
        (is (= (car/conn-manager-clear! mgr 1000)
              {:action :cleared, :graceful? true}))
        (is (car/conn-manager-open? mgr))
        (is (= (get-in @mgr [:stats :counts :cleared]) 1))
        (is (= (wcar mgr (resp/ping)) "PONG"))
        (is (= (car/conn-manager-close! mgr 1000
                 {:reason :test, :mgr :caller, :via :caller, :timeout-ms :caller})
              {:action :closed, :graceful? true}))

        (is (= (mapv
                 #(select-keys (:data %) [:via :timeout-ms :reason])
                 @close-events_)
              [{:via 'mgr-clear!, :timeout-ms 1000}
               {:via 'mgr-close!, :timeout-ms 1000, :reason :test}]))
        (is (every? :closed? @close-events_))
        (finally (car/conn-manager-close! mgr 0 nil))))))

(deftest _conn-manager-timeout-validation
  (let [cluster-spec (cluster/cluster-spec [["seed.redis" 7000]])
        constructors
        [[conns/conn-manager-unpooled {}]
         [conns/conn-manager-pooled {}]
         [conns/conn-manager-clustered
          {:conn-opts {:server {:cluster-spec cluster-spec}}}]]]
    (doseq [[constructor opts] constructors
            invalid-timeout [-1 1.5 "100" (inc (bigint Long/MAX_VALUE))]]
      (with-open [^java.io.Closeable mgr (constructor opts)]
        (let [close-error (truss/throws
                            (car/conn-manager-close! mgr invalid-timeout nil))
              clear-error (truss/throws
                            (car/conn-manager-clear! mgr invalid-timeout))]
          [(is (truss/submap? (ex-data close-error)
                 {:eid :carmine.conns/invalid-timeout-ms, :via 'mgr-close!}))
           (is (truss/submap? (ex-data clear-error)
                 {:eid :carmine.conns/invalid-timeout-ms, :via 'mgr-clear!}))
           (is (car/conn-manager-open? mgr)
             "Invalid lifecycle input cannot partially close the manager")])))))

(deftest _conn-manager-maximum-timeout
  (let [borrowed (promise)
        release  (promise)
        mgr      (conns/conn-manager-unpooled {})
        borrow-f (future
                   (conns/mgr-borrow! mgr
                     (fn [& _]
                       (deliver borrowed true)
                       @release)))]
    (try
      (is (= (deref borrowed 2000 ::timeout) true))
      (let [close-f (future (car/conn-manager-close! mgr Long/MAX_VALUE))]
        (loop [n 0]
          (when (and (car/conn-manager-open? mgr) (< n 400))
            (Thread/sleep 5)
            (recur (inc n))))
        (is (false? (car/conn-manager-open? mgr)))
        (deliver release true)
        [(is (= (deref borrow-f 2000 ::timeout) true))
         (is (= (deref close-f 2000 ::timeout)
                {:action :closed, :graceful? true}))])
      (finally
        (deliver release true)
        (car/conn-manager-close! mgr 0)))))

(deftest _pooled-borrow-interruption
  (let [borrowed (promise)
        release  (promise)
        outcome  (promise)
        mgr
        (conns/conn-manager-pooled
          {:pool-opts
           {:max-total 1, :max-idle 1, :test-on-borrow? false}})
        holder
        (future
          (conns/mgr-borrow! mgr
            (fn [& _]
              (deliver borrowed true)
              @release)))
        waiter
        (Thread.
          ^Runnable
          (reify Runnable
            (run [_]
              (deliver outcome
                (try
                  (conns/mgr-borrow! mgr (fn [& _] :unexpected))
                  {:result :unexpected}
                  (catch Throwable t
                    {:error t
                     :interrupted?
                     (.isInterrupted (Thread/currentThread))}))))))]
    (try
      (is (= (deref borrowed 2000 ::timeout) true))
      (.start waiter)
      (support/await-pred! 2000 "the second pool borrow to wait"
        #(pos? (long (get-in @mgr [:stats :connections :waiting]))))
      (.interrupt waiter)
      (let [{:keys [error interrupted?] :as result}
            (deref outcome 2000 {:result ::timeout})]
        [(is (not= (:result result) ::timeout))
         (is (= (:eid (ex-data error)) :carmine.conns/borrow-conn-error))
         (is (instance? InterruptedException (ex-cause error)))
         (is interrupted?
           "Wrapping an interrupted pool borrow preserves interrupt status")])
      (finally
        (deliver release true)
        (.interrupt waiter)
        (.join waiter 2000)
        (deref holder 2000 ::timeout)
        (car/conn-manager-close! mgr 0)))))

(deftest _unpooled-callback-interruption
  (let [started (promise)
        raised  (promise)
        outcome (promise)
        close-events_ (atom [])
        mgr
        (conns/conn-manager-unpooled
          {:conn-opts
           {:cbs {:on-conn-close #(swap! close-events_ conj %)}}})
        borrower
        (Thread.
          ^Runnable
          (reify Runnable
            (run [_]
              (deliver outcome
                (try
                  (conns/mgr-borrow! mgr
                    (fn [& _]
                      (deliver started true)
                      (try
                        (Thread/sleep 10000)
                        :unexpected
                        (catch InterruptedException t
                          (deliver raised t)
                          (throw t)))))
                  {:result :unexpected}
                  (catch Throwable t
                    {:error t
                     :interrupted? (.isInterrupted (Thread/currentThread))
                     :active (get-in @mgr [:stats :connections :active])
                     :close-events @close-events_}))))))]
    (try
      (.start borrower)
      (is (= (deref started 2000 ::timeout) true))
      (.interrupt borrower)
      (let [{:keys [error interrupted? active close-events] :as result}
            (deref outcome 2000 {:result ::timeout})]
        [(is (not= (:result result) ::timeout))
         (is (identical? error (deref raised 100 ::timeout))
           "The callback's InterruptedException is rethrown unchanged")
         (is interrupted?
           "Unpooled callback cleanup preserves interrupt status")
         (is (zero? active) "The interrupted connection was disjoined")
         (is (= (count close-events) 1)
           "The interrupted connection was closed before propagation")])
      (finally
        (.interrupt borrower)
        (.join borrower 2000)
        (car/conn-manager-close! mgr 0)))))

(deftest _unpooled-close-fences-in-flight-creation
  (let [creation-started (promise)
        resume-creation (promise)
        handler-called_ (atom false)
        original-new-conn @#'conns/new-conn
        mgr (conns/conn-manager-unpooled {})]
    (try
      (let [[close-result borrow-error]
            (with-redefs-fn
              {#'conns/new-conn
               (fn [& args]
                 (deliver creation-started true)
                 @resume-creation
                 (apply original-new-conn args))}
              (fn []
                (let [borrow-f
                      (future
                        (truss/throws
                          (conns/mgr-borrow! mgr
                            (fn [& _]
                              (reset! handler-called_ true)
                              :done))))]
                  (is (= (deref creation-started 2000 ::timeout) true))
                  (let [close-result (car/conn-manager-close! mgr 0 nil)]
                    (deliver resume-creation true)
                    [close-result (deref borrow-f 2000 ::timeout)]))))]
        [(is (= close-result {:action :closed, :graceful? true}))
         (is (= (:eid (ex-data borrow-error)) :carmine.conns/manager-closed))
         (is (false? @handler-called_)
           "A connection created after the close snapshot cannot enter user code")])
      (finally
        (deliver resume-creation true)
        (car/conn-manager-close! mgr 0 nil)))))

(deftest _pooled-conn-manager-active-clear
  (let [borrowed      (promise)
        release       (promise)
        close-events_ (atom [])
        mgr
        (conns/conn-manager-pooled
          {:conn-opts {:cbs {:on-conn-close #(swap! close-events_ conj %)}}
           :pool-opts {:test-while-idle? false}})]
    (try
      (let [borrow-f
            (future
              (conns/mgr-borrow! mgr
                (fn [_ _in _out]
                  (deliver borrowed :borrowed)
                  @release
                  :done)))
            _       (is (= (deref borrowed 2000 ::timeout) :borrowed))
            before  (get-in @mgr [:stats :counts])
            clear-f (future (car/conn-manager-clear! mgr 2000))]
        (loop [n 0]
          (when (and (zero? (get-in @mgr [:stats :counts :cleared])) (< n 200))
            (Thread/sleep 5)
            (recur (inc n))))
        (deliver release :release)
        [(is (= (deref borrow-f 2000 ::timeout) :done))
         (is (= (deref clear-f 2000 ::timeout)
               {:action :cleared, :graceful? true}))
         (is (zero? (get-in @mgr [:stats :connections :idle]))
           "An active connection from the cleared generation cannot return idle")
         (is (= (get-in (first @close-events_) [:data :via]) 'mgr-clear!))
         (is (= (wcar mgr (resp/ping)) "PONG"))
         (is (> (get-in @mgr [:stats :counts :created]) (:created before)))])
      (finally
        (deliver release :release)
        (car/conn-manager-close! mgr 0 nil)))))

(deftest _pooled-conn-manager-overlapping-clear-context
  (let [borrowed-a    (promise)
        borrowed-b    (promise)
        release-a     (promise)
        release-b     (promise)
        close-events_ (atom [])
        mgr
        (conns/conn-manager-pooled
          {:conn-opts {:cbs {:on-conn-close #(swap! close-events_ conj %)}}
           :pool-opts {:test-while-idle? false}})]
    (try
      (let [borrow-f-a
            (future
              (conns/mgr-borrow! mgr
                (fn [conn _in _out]
                  (deliver borrowed-a conn)
                  @release-a
                  :a)))
            _ (is (not= (deref borrowed-a 2000 ::timeout) ::timeout))
            clear-f-a (future (car/conn-manager-clear! mgr 5001))]

        (loop [n 0]
          (when (and (< (get-in @mgr [:stats :counts :cleared]) 1) (< n 400))
            (Thread/sleep 5)
            (recur (inc n))))
        (is (= (get-in @mgr [:stats :counts :cleared]) 1))

        (let [borrow-f-b
              (future
                (conns/mgr-borrow! mgr
                  (fn [conn _in _out]
                    (deliver borrowed-b conn)
                    @release-b
                    :b)))
              conn-b (deref borrowed-b 2000 ::timeout)
              _      (is (not= conn-b ::timeout))
              clear-f-b (future (car/conn-manager-clear! mgr 5002))]

          (loop [n 0]
            (when (and (< (get-in @mgr [:stats :counts :cleared]) 2) (< n 400))
              (Thread/sleep 5)
              (recur (inc n))))
          (is (= (get-in @mgr [:stats :counts :cleared]) 2))

          ;; Synchronize after clear B's generation change and snapshot.
          (is (= (wcar mgr (resp/ping)) "PONG"))

          ;; Clear A finishes while clear B is still draining. It must not erase
          ;; B's callback context before B's active connection is returned.
          (deliver release-a :release)
          [(is (= (deref borrow-f-a 2000 ::timeout) :a))
           (is (= (deref clear-f-a 2000 ::timeout)
                 {:action :cleared, :graceful? true}))]

          (deliver release-b :release)
          [(is (= (deref borrow-f-b 2000 ::timeout) :b))
           (is (= (deref clear-f-b 2000 ::timeout)
                 {:action :cleared, :graceful? true}))]

          (let [event-b (some #(when (identical? (:conn %) conn-b) %) @close-events_)]
            [(is (some? event-b))
             (is (truss/submap? (:data event-b)
                   {:via 'mgr-clear!, :timeout-ms 5002}))])))
      (finally
        (deliver release-a :release)
        (deliver release-b :release)
        (car/conn-manager-close! mgr 0 nil)))))

(deftest _pooled-ready-check-idle-threshold
  [(is (#'conns/pooled-conn-ping? 0 0 1000)
     "A newly created pooled object is always checked")
   (is (not (#'conns/pooled-conn-ping? 1 999 1000))
     "A recently returned connection skips the extra PING")
   (is (#'conns/pooled-conn-ping? 1 1000 1000)
     "The idle threshold restores the PING check")
   (is (#'conns/pooled-conn-ping? 1 0 nil)
     "Nil restores a PING on every validation")
   (is (#'conns/pooled-conn-ping? 1 0 0)
     "Zero restores a PING on every validation")])

(defn- kill-client! [client-id]
  (wcar mgr_ (resp/rcmd "CLIENT" "KILL" "ID" client-id)))

(deftest _conn-manager-dropped-connection-recovery
  (testing "Idle connection is rejected on its next borrow"
    (let [close-events_ (atom [])
          error-events_ (atom [])]
      (with-open [mgr
                  (conns/conn-manager-pooled
                    {:conn-opts
                     {:socket-opts {:read-timeout-ms 2000}
                      :cbs
                      {:on-conn-close #(swap! close-events_ conj %)
                       :on-conn-error #(swap! error-events_ conj %)}}
                     :pool-opts {:test-while-idle? false
                                 :ready-check-after-idle-ms 0}})]
        (let [client-id (wcar mgr (resp/rcmd "CLIENT" "ID"))
              before    (get-in @mgr [:stats :counts])]
          (is (= (kill-client! client-id) 1))

          (let [[ping new-client-id]
                (wcar mgr
                  (resp/rcmds ["PING"] ["CLIENT" "ID"]))
                after (get-in @mgr [:stats :counts])]
            [(is (= ping "PONG"))
             (is (not= new-client-id client-id))
             (is (> (:created after) (:created before)))
             (is (> (get-in after [:destroyed :by-borrow-validation])
                   (get-in before [:destroyed :by-borrow-validation])))
             (is (= (count @error-events_) 1))
             (is (= (count @close-events_) 1))
             (is (truss/submap? (first @error-events_)
                   {:eid :carmine.conns/conn-not-ready
                    :via 'conn-ready?}))
             (is (instance? Throwable (:cause (first @error-events_))))
             (is (truss/submap? (:data (first @close-events_))
                   {:via 'mgr-borrow!, :validation? true}))
             (is (true? (:closed? (first @close-events_))))])))))

  (testing "Active connection failure is invalidated before recovery"
    (let [close-events_ (atom [])
          killed-id_    (volatile! nil)
          kill-reply_   (volatile! nil)]
      (with-open [mgr
                  (conns/conn-manager-pooled
                    {:conn-opts
                     {:socket-opts {:read-timeout-ms 2000}
                      :cbs {:on-conn-close #(swap! close-events_ conj %)}}})]
        (let [before (get-in @mgr [:stats :counts])
              failure
              (truss/throws
                (car/with-car mgr
                  (fn []
                    (let [client-id (with-replies (resp/rcmd "CLIENT" "ID"))]
                      (vreset! killed-id_ client-id)
                      (vreset! kill-reply_ (kill-client! client-id))
                      (with-replies (resp/ping))))))
              [ping new-client-id]
              (wcar mgr
                (resp/rcmds ["PING"] ["CLIENT" "ID"]))
              after (get-in @mgr [:stats :counts])]
          [(is (= @kill-reply_ 1))
           (is (instance? Throwable failure))
           (is (= ping "PONG"))
           (is (not= new-client-id @killed-id_))
           (is (> (:created after) (:created before)))
           (is (> (get-in after [:destroyed :total])
                 (get-in before [:destroyed :total])))
           (is (= (get-in after [:destroyed :by-borrow-validation])
                 (get-in before [:destroyed :by-borrow-validation])))
           (is (= (count @close-events_) 1))
           (is (identical? (get-in (first @close-events_) [:data :cause]) failure))
           (is (identical? (:cause (first @close-events_)) failure))
           (is (= (:via (first @close-events_)) 'mgr-borrow!))
           (is (true? (:closed? (first @close-events_))))])))))

(deftest _conn-manager-resp-limit-invalidation
  (let [close-events_ (atom [])]
    (with-open [mgr
                (conns/conn-manager-pooled
                  {:conn-opts
                   {:init {:resp3? false, :client-name nil}
                    :resp-opts {:limits {:max-blob-bytes 3}}
                    :cbs {:on-conn-close #(swap! close-events_ conj %)}}})]
      (let [before (get-in @mgr [:stats :counts])
            failure
            (truss/throws
              (wcar mgr {:as-vec? true}
                (resp/ping)
                (resp/echo "four")
                (resp/ping)))
            ping  (wcar mgr (resp/ping))
            after (get-in @mgr [:stats :counts])]
        [(is (truss/submap? (ex-data failure)
               {:eid :carmine.read/resource-limit
                :limit :max-blob-bytes, :max 3, :actual 4})
           "A pipeline limit breach escapes instead of returning a partial vector")
         (is (= ping "PONG"))
         (is (> (:created after) (:created before)))
         (is (> (get-in after [:destroyed :total])
                (get-in before [:destroyed :total])))
         (is (= (count @close-events_) 1))
         (is (identical? (get-in (first @close-events_) [:data :cause]) failure))
         (is (= (:via (first @close-events_)) 'mgr-borrow!))]))))

(deftest _conn-manager-graceful-close
  (doseq [[label constructor]
          [["Unpooled" conns/conn-manager-unpooled]
           ["Pooled"   conns/conn-manager-pooled]]]
    (testing label
      (let [borrowed      (promise)
            release       (promise)
            close-events_ (atom [])
            mgr
            (constructor
              {:conn-opts
               {:cbs {:on-conn-close #(swap! close-events_ conj %)}}})]
        (try
          (let [borrow-f
                (future
                  (car/with-car mgr
                    (fn []
                      (deliver borrowed :borrowed)
                      @release
                      :done))
                  :done)]

            (is (= (deref borrowed 2000 ::timeout) :borrowed))
            (let [close-f
                  (future
                    (car/conn-manager-close! mgr 5000 {:reason :test}))]

              (loop [n 0]
                (when (and (car/conn-manager-open? mgr) (< n 200))
                  (Thread/sleep 5)
                  (recur (inc n))))

              (is (false? (car/conn-manager-open? mgr)))
              (deliver release :release)
              (is (= (deref borrow-f 2000 ::timeout) :done))
              (is (= (deref close-f 2000 ::timeout)
                    {:action :closed, :graceful? true})
                "Close finishes when an active connection is returned normally")
              (is (= (mapv
                       #(select-keys (:data %) [:via :timeout-ms :reason])
                       @close-events_)
                    [{:via 'mgr-close!, :timeout-ms 5000, :reason :test}])
                "A connection destroyed on the borrower thread retains close context")))

          (finally
            (deliver release :release)
            (car/conn-manager-close! mgr 0 nil)))))))

(deftest _conn-manager-close-interruption
  (let [borrowed (promise)
        release  (promise)
        result   (promise)
        close-events_ (atom [])
        mgr
        (conns/conn-manager-unpooled
          {:conn-opts
           {:cbs {:on-conn-close #(swap! close-events_ conj %)}}})
        borrow-f
        (future
          (conns/mgr-borrow! mgr
            (fn [conn _in _out]
              (deliver borrowed conn)
              @release
              :done)))
        close-thread
        (Thread.
          ^Runnable
          (reify Runnable
            (run [_]
              (try
                (deliver result (car/conn-manager-close! mgr))
                (catch Throwable t
                  (deliver result [t (.isInterrupted (Thread/currentThread))]))))))]
    (try
      (let [conn (deref borrowed 2000 ::timeout)]
        (is (not= conn ::timeout))
        (.start close-thread)
        (loop [n 0]
          (when (and (car/conn-manager-open? mgr) (< n 200))
            (Thread/sleep 5)
            (recur (inc n))))
        (.interrupt close-thread)
        (let [[error interrupted?] (deref result 2000 [::timeout false])]
          [(is (instance? InterruptedException error))
           (is interrupted? "Shutdown restores the interrupted status")
           (is (= (count @close-events_) 1))
           (is (= (select-keys (:data (first @close-events_))
                    [:via :timeout-ms])
                  {:via 'mgr-close!, :timeout-ms nil})
             "Interrupted shutdown force-closes with manager-close context")
           (is (= (car/conn-manager-close! mgr) {:action :already-closed}))]))
      (finally
        (deliver release :release)
        (.interrupt close-thread)
        (.join close-thread 2000)
        (deref borrow-f 2000 ::timeout)
        (car/conn-manager-close! mgr 0 nil)))))

(deftest _conn-manager-interrupt
  (let [mgr (conns/conn-manager-unpooled {})
        k1  (tk "tlist")
        client-name (str "carmine-v4-interrupt-" (java.util.UUID/randomUUID))
        f
        (future
          (wcar mgr
            (resp/rcmds
              ["CLIENT" "SETNAME" client-name]
              ["DEL"   k1]
              ["LPUSH" k1 "x"]
              ["LPOP"  k1]
              ["BLPOP" k1 5] ; Block for 5 secs
              )))]

    (support/await-pred! 5000 "the test client to block in BLPOP"
      #(some
         (fn [line]
           (and (str/includes? line (str "name=" client-name))
                (str/includes? line "cmd=blpop")))
         (str/split-lines (wcar mgr_ (resp/rcmd "CLIENT" "LIST")))))
    [(is (= (car/conn-manager-close! mgr 0 {})
           {:action :closed, :graceful? false})) ; Interrupt pool conns
     (is (throws? java.net.SocketException @f)
       "Close with zero timeout interrupts blocking blpop")]))

(deftest _wcar-basics
  [(doseq [legacy-opts [#{:as-vec} #{:natural-replies} :as-vce]]
     (is (->> (resp/parse-body-reply-opts [legacy-opts])
           (throws? :ex-info {:eid :carmine/invalid-reply-opts}))))
   (is (= (resp/parse-body-reply-opts [:as-vec '(ping)])
         [{:as-vec? true, :natural-replies? false, :error-mode :throw}
          '((ping))]))
   (is (= (resp/parse-body-reply-opts [:as-pipeline '(ping)])
          (resp/parse-body-reply-opts [:as-vec      '(ping)]))
     "`:as-pipeline` is an undocumented alias for `:as-vec` (v3 back compatibility)")
   (doseq [opts [{:unknown? true}
                 {:as-vec? :yes}
                 {:natural-replies? nil}
                 {:error-mode :invalid}]]
     (is (->> (resp/parse-reply-opts opts)
           (throws? :ex-info {:eid :carmine/invalid-reply-opts}))))
   (is (throws? :ex-info (wcar nil (resp/ping)))
     "v4 never creates a hidden default manager")
   (is (throws? :ex-info
         (car/with-car :not-a-manager (fn [] (resp/ping)))))
   (is (->> (car/with-car mgr_ {:unknown? true} (fn [] (resp/ping)))
         (throws? :ex-info {:eid :carmine/invalid-reply-opts})))
   (is (= (wcar mgr_                 (resp/ping))  "PONG"))
   (is (= (wcar mgr_ {:as-vec? true} (resp/ping)) ["PONG"]))
   (is (= (wcar mgr_ :as-vec (resp/ping)) ["PONG"]) "`:as-vec` shorthand")
   (let [as-vec? true]
     (is (= (wcar mgr_ {:as-vec? as-vec?} (resp/ping)) ["PONG"])
       "Reply option values are evaluated and validated at runtime"))
   (let [as-vec? true]
     (is (= (wcar mgr_
              (resp/local-echo
                (with-replies {:as-vec? as-vec?} (resp/ping))))
           ["PONG"])
       "Nested reply option expressions are also runtime values"))
   (is (->> (car/set (Object.) "outside-context")
         (throws? :ex-info {:eid :carmine/no-context}))
     "Generated commands check for a request context before preparing args")
   (is (= (wcar mgr_ (resp/local-echo "hello")) "hello") "Local echo")
   (is (= (wcar mgr_ :as-vec
            (car/rcmds ["PING"] ["ECHO" "a"])
            (car/rcmds* [["ECHO" "b"]])
            (car/local-echos :c :d)
            (car/local-echos* [:e :f]))
          ["PONG" "a" "b" :c :d :e :f])
     "Stable no-doc plural helpers remain available at the top level")
   (is (= (wcar mgr_ :as-vec (resp/local-echos* (range 12)))
          (vec (range 12)))
     "High-cardinality reply collection preserves order")

   (let [k1 (tk "k1")
         v1 (str (rand-int 1e6))]
     (is
       (= (wcar mgr_
            (resp/ping)
            (resp/rset k1 v1)
            (resp/echo (wcar mgr_ (resp/rget k1)))
            (resp/rset k1 "0"))

         ["PONG" "OK" v1 "OK"])

       "Flush triggered by `wcar` in `wcar`"))

   (let [k1 (tk "k1")
         v1 (str (rand-int 1e6))]
     (is
      (= (wcar mgr_
           (resp/ping)
           (resp/rset k1 v1)
           (resp/echo         (with-replies (resp/rget k1)))
           (resp/echo (str (= (with-replies (resp/rget k1)) v1)))
           (resp/rset k1 "0"))

        ["PONG" "OK" v1 "true" "OK"])

      "Flush triggered by `with-replies` in `wcar`"))

   (is (= (wcar mgr_ (resp/ping) (wcar mgr_))    "PONG") "Parent replies not swallowed by `wcar`")
   (is (= (wcar mgr_ (resp/ping) (with-replies)) "PONG") "Parent replies not swallowed by `with-replies`")

   (is (= (let [k1 (tk "k1")]
            (wcar mgr_
              (resp/rset k1 "v1")
              (resp/echo
                (with-replies
                  (car/skip-replies (resp/rset k1 "v2"))
                  (resp/echo
                    (with-replies (resp/rget k1)))))))
         ["OK" "v2"]))

   (is (=
         (wcar mgr_
           (resp/ping)
           (resp/echo       (first (with-replies {:as-vec? true} (resp/ping))))
           (resp/local-echo (first (with-replies {:as-vec? true} (resp/ping)))))

         ["PONG" "PONG" "PONG"])

     "Nested :as-vec")])

(deftest _wcar-reply-error-policy
  (testing "Default throw mode drains the complete pipeline"
    (let [failure
          (truss/throws
            (wcar mgr_
              (resp/ping)
              (resp/rcmd "CARMINE-INVALID-COMMAND")
              (resp/echo "middle")
              (resp/rcmd "CARMINE-INVALID-COMMAND")
              (resp/echo "tail")))
          {:keys [eid replies error-indexes]} (ex-data failure)]
      [(is (car/reply-error? failure))
       (is (= eid :carmine.read/drained-reply-errors))
       (is (= error-indexes [1 3]))
       (is (= [(first replies) (last replies)] ["PONG" "tail"]))
       (is (car/reply-error? (second replies)))
       (is (car/reply-error? (nth replies 3)))
       (is (identical? (ex-cause failure) (second replies)))
       (is (= (wcar mgr_ (resp/ping)) "PONG")
         "The reply after the Redis error was consumed")]))

  (testing "Return mode preserves Redis errors as ordinary reply values"
    (let [replies
          (wcar mgr_ {:error-mode :return}
            (resp/ping)
            (resp/rcmd "CARMINE-INVALID-COMMAND")
            (resp/echo "tail"))
          single-error
          (wcar mgr_ {:error-mode :return}
            (resp/rcmd "CARMINE-INVALID-COMMAND"))]
      [(is (= [(first replies) (last replies)] ["PONG" "tail"]))
       (is (car/reply-error? (second replies)))
       (is (car/reply-error? single-error))
       (is (car/reply-error?
             (truss/throws
               (wcar mgr_ {:as-vec? true}
                 (resp/rcmd "CARMINE-INVALID-COMMAND"))))
         "Vector shaping does not disable the default throw policy")]))

  (testing "Nested reply boundaries use the same policy and remain usable"
    (let [nested-failure_ (volatile! nil)
          outer-replies
          (wcar mgr_
            (try
              (with-replies
                (resp/rcmd "CARMINE-INVALID-COMMAND")
                (resp/ping))
              (catch Throwable t
                (vreset! nested-failure_ t)))
            (resp/echo "outer-tail"))
          returned
          (wcar mgr_
            (resp/local-echo
              (with-replies {:error-mode :return}
                (resp/rcmd "CARMINE-INVALID-COMMAND")
                (resp/ping))))]
      [(is (= (:error-indexes (ex-data @nested-failure_)) [0]))
       (is (= outer-replies "outer-tail"))
       (is (= (second returned) "PONG"))
       (is (car/reply-error? (first returned)))]))

  (testing "A fully-drained reply error does not discard a healthy pooled connection"
    (with-open [mgr
                (conns/conn-manager-pooled
                  {:pool-opts {:max-total 1, :max-idle 1
                               :test-on-borrow? false}})]
      (let [client-id-before (wcar mgr (resp/rcmd "CLIENT" "ID"))
            counts-before    (get-in @mgr [:stats :counts])
            failure
            (truss/throws
              (wcar mgr
                (resp/rcmd "CARMINE-INVALID-COMMAND")
                (resp/ping)))
            client-id-after (wcar mgr (resp/rcmd "CLIENT" "ID"))
            counts-after    (get-in @mgr [:stats :counts])]
        [(is (car/reply-error? failure))
         (is (= client-id-before client-id-after))
         (is (= (get-in counts-before [:destroyed :total])
                (get-in counts-after  [:destroyed :total])))
         (is (< (:returned counts-before) (:returned counts-after)))]))))

(deftest _pooled-stateful-commands-are-not-reused
  (with-open [mgr
              (conns/conn-manager-pooled
                {:pool-opts {:max-total 1, :max-idle 1
                             :test-on-borrow? false}})]
    (let [client-id-before (wcar mgr (resp/rcmd "CLIENT" "ID"))
          failure
          (truss/throws
            (wcar mgr
              (resp/rcmd "MULTI")
              (resp/rcmd "CARMINE-INVALID-COMMAND")))
          ping (wcar mgr (resp/ping))
          client-id-after (wcar mgr (resp/rcmd "CLIENT" "ID"))]
      [(is (car/reply-error? failure))
       (is (= ping "PONG")
         "The next borrower never inherits MULTI and receives QUEUED")
       (is (not= client-id-before client-id-after)
         "A drained error only reuses connections whose commands were state-safe")])))

(deftest _wcar-read-opts-interactions
  (let [key (tk (str "read-opts:" (java.util.UUID/randomUUID)))
        value {:nested [1 2 3]}]
    (wcar mgr_ (car/set key value))
    (let [natural (wcar mgr_ {:natural-replies? true} (car/get key))]
      [(is (and (string? natural) (not= natural value))
         "Natural top-level replies bypass automatic thawing")
       (is (= (wcar mgr_ (car/thaw nil (car/get key))) value))
       (is (enc/bytes? (wcar mgr_ (car/as-bytes (car/get key))))
         "Explicit bytes mode overrides automatic thawing")])

    (is (= (wcar mgr_
             (car/parse nil str/lower-case (car/ping))
             (resp/echo
               (with-replies {:as-vec? true}
                 (car/skip-replies (car/set key "updated"))
                 (car/get key))))
           ["pong" ["updated"]])
      "Parsers, nested vector boundaries, and skipped replies compose")

    (is (nil?
          (car/skip-replies
            (wcar mgr_ {:as-vec? true}
              (car/set key "skipped")
              (car/get key))))
      "A fully skipped vector boundary follows the zero-reply nil contract
       (deliberate: zero replies => nil, even with `:as-vec?`)")
    (is (nil?
          (car/skip-replies
            (wcar mgr_
              (car/set key "skipped")
              (car/get key))))
      "Same nil contract without `:as-vec?`")))
