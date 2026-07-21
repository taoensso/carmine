(ns taoensso.carmine-v4.tests.connections
  "Carmine v4 connection, manager lifecycle, and request-context tests."
  (:require
   [clojure.string      :as str]
   [clojure.test        :refer [deftest testing is]]
   [taoensso.encore     :as enc]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine-v4          :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.resp.read :as read]
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
             :stats {:kind :unpooled
                     :connections {:active 0, :idle nil, :waiting nil}
                     :counts {:created 1, :failed 0}}}))])

   (let [[v mgr] (test-manager (delay (conns/conn-manager-pooled {})))]
     [(is (= v [nil "x" "PONG"]))
      (support/assert-manager-stats-schema! :pooled mgr)
      (is (truss/submap? @mgr
            {:open? false,
             :stats {:kind :pooled
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

(deftest _pool-opts-for-wrong-manager-kind
  (is (->> (car/conn-manager {:pool-opts {:max-total-per-key 4}})
        (throws? :ex-info {:eid :carmine.conns/pool-option-not-supported}))
    "A v3-style `-per-key` option gets a clear error from a non-Cluster manager"))

(deftest _socket-deadline-error
  ;; A socket closed by the deadline watchdog reports the deadline, with the
  ;; socket's own error as cause
  (with-open [silent-server (java.net.ServerSocket. 0)
              socket (java.net.Socket. "127.0.0.1" (.getLocalPort silent-server))]
    (let [err
          (truss/throws
            (#'conns/with-socket-deadline! socket 50 :init
              (fn [] (.read (.getInputStream socket)))))]
      [(is (= (:eid (ex-data err)) :carmine.conns/conn-init-deadline))
       (is (instance? java.io.IOException (ex-cause err)))])))

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
                :addr (:server conn-opts)
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
         (is (= (:eid new-conn-error) :carmine.conns/new-conn-error))
         (is (nil? (:cause init-error))
           "A redacted reply error isn't exposed as cause")]))

    (let [err
          (truss/throws
            (conns/with-new-conn
              (opts/parse-conn-opts :redis {:init {:select-db 99999}})
              (fn [& _] nil)))
          init-error (ex-cause err)]
      [(is (= (:eid (ex-data init-error)) :carmine.conns/conn-init-error))
       (is (= (:code (ex-data (ex-cause init-error))) "ERR")
         "The first init reply error is kept as cause")]))

  (testing "Init error replies that include the password keep only their code"
    (let [err    (fn [msg] (#'read/redis-reply-error msg))
          redact #'conns/redact-init-reply
          echo   (err "ERR unknown command `HELLO`, with args beginning with: `3`, `AUTH`, `default`, `secret`")
          wrong  (err "WRONGPASS invalid username-password pair or user is disabled.")]
      [(let [e (redact false "secret" echo)]
         [(is (= (:code (ex-data e)) "ERR"))
          (is (not (str/includes? (str (ex-message e) (ex-data e)) "secret")))])
       (is (= (redact false "secret" (err "secret")) :carmine/redacted))
       (is (= (:code (ex-data (redact false "long-secret-token" (err "ERR `long-secr")))) "ERR")
         "Truncated echo")
       (is (= (redact false "secret" wrong) wrong) "Otherwise kept in full")
       (is (= (redact false nil      echo)  echo))
       (is (= (redact true  nil      wrong) :carmine/redacted))]))

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
                :mgr-kind kind})))
       (is (= (:opts (ex-data (truss/throws (constructor "redis://user:secret@host"))))
              {:type String})
         "No value, may be a URI with password")
       (is (->> (constructor (assoc base-opts :on-push :not-a-fn))
             (throws? :ex-info
               {:eid :carmine.conns/invalid-manager-opts
                :mgr-kind kind})))
       (is (->> (constructor (assoc base-opts :push-queue-capacity 0))
             (throws? :ex-info
               {:eid :carmine.conns/invalid-manager-opts
                :mgr-kind kind})))
       (is (->> (constructor
                  (assoc base-opts :push-queue-capacity
                    (inc (long Integer/MAX_VALUE))))
             (throws? :ex-info
               {:eid :carmine.conns/invalid-manager-opts
                :mgr-kind kind})))])

    (is (->> (conns/conn-manager-unpooled nil)
          (throws? :ex-info {:eid :carmine.conns/invalid-manager-opts})))

    (is (->> (conns/conn-manager-pooled
               {:pool-opts
                {:ready-check-after-idle-ms (inc (bigint Long/MAX_VALUE))}})
          (throws? :ex-info
            {:eid :carmine.conns/invalid-pool-option})))

    (with-open [mgr
                (conns/conn-manager-unpooled
                  {:example/extension true, :on-push nil})]
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
                   (assoc base-opts :on-push identity))
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
                  :on-push identity})
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
             false))
       (is (car/conn-manager-open? mgr))]))

  (testing "Lifecycle wrappers accept delayed managers and concise arities"
    (let [mgr_ (delay (car/conn-manager))]
      (try
        [(is (car/conn-manager-open? mgr_))
         (is (= (car/conn-manager-clear! mgr_)
                true))
         (is (= (car/conn-manager-close! mgr_)
                true))]
        (finally (car/conn-manager-close! mgr_ {:timeout-ms 0})))))

  (testing "Invalid public lifecycle input cannot change manager state"
    (with-open [mgr (conns/conn-manager-pooled {})]
      (let [error (truss/throws (car/conn-manager-close! mgr {:timeout-ms 1000, :data :bad-data}))]
        [(is (truss/submap? (ex-data error)
                {:eid :carmine.conns/invalid-close-data
                 :via 'conn-manager-close!}))
         (is (->> (car/conn-manager-close! mgr {:data {:via :caller}})
               (throws? :ex-info {:eid :carmine.conns/invalid-close-data, :keys [:via]})))
         (is (->> (car/conn-manager-close! mgr 1000)
               (throws? :ex-info {:eid :carmine.conns/invalid-opts})))
         (is (->> (car/conn-manager-clear! mgr {:timeout-ms 1000, :data {}})
               (throws? :ex-info {:eid :carmine.conns/invalid-opts})))
         (is (car/conn-manager-open? mgr))])))

  (doseq [[label new-mgr]
          [["Unpooled" #(conns/conn-manager-unpooled {})]
           ["Pooled"   #(conns/conn-manager-pooled   {})]]]
    (testing label
      (let [mgr (new-mgr)]
        (try
          [(is (car/conn-manager-open? mgr))
           (is (= (wcar mgr (resp/ping)) "PONG"))
           (is (= (car/conn-manager-close! mgr {:timeout-ms 1000, :data {:reason :test}})
                 true))
           (is (false? (car/conn-manager-open? mgr)))
           (is (= (car/conn-manager-close! mgr {:timeout-ms 1000, :data {:reason :again}})
                 false))
           (let [cleared-before (get-in @mgr [:stats :counts :cleared])]
             [(is (= (car/conn-manager-clear! mgr {:timeout-ms 1000})
                    false))
              (is (= (get-in @mgr [:stats :counts :cleared]) cleared-before)
                "Clear racing after close is a no-op and is not counted")])
           (is (->> (car/with-car mgr (fn [] nil))
                 (throws? :ex-info {:eid :carmine.conns/manager-closed})))]
          (finally (car/conn-manager-close! mgr {:timeout-ms 0}))))))

  (testing "Pooled clear and close callbacks retain their cause"
    (let [close-events_ (atom [])
          mgr
          (conns/conn-manager-pooled
            {:conn-opts
             {:cbs {:on-conn-close #(swap! close-events_ conj %)}}})]
      (try
        (is (= (wcar mgr (resp/ping)) "PONG"))
        (is (= (car/conn-manager-clear! mgr {:timeout-ms 1000})
              true))
        (is (car/conn-manager-open? mgr))
        (is (= (get-in @mgr [:stats :counts :cleared]) 1))
        (is (= (wcar mgr (resp/ping)) "PONG"))
        (is (= (car/conn-manager-close! mgr
                 {:timeout-ms 1000
                  :data {:reason :test}})
              true))

        (is (= (mapv
                 #(select-keys (:data %) [:via :timeout-ms :reason])
                 @close-events_)
              [{:via 'mgr-clear!, :timeout-ms 1000}
               {:via 'mgr-close!, :timeout-ms 1000, :reason :test}]))
        (is (every? :closed? @close-events_))
        (finally (car/conn-manager-close! mgr {:timeout-ms 0}))))))

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
                            (car/conn-manager-close! mgr {:timeout-ms invalid-timeout}))
              clear-error (truss/throws
                            (car/conn-manager-clear! mgr {:timeout-ms invalid-timeout}))]
          [(is (truss/submap? (ex-data close-error)
                 {:eid :carmine.conns/invalid-timeout, :via 'mgr-close!}))
           (is (truss/submap? (ex-data clear-error)
                 {:eid :carmine.conns/invalid-timeout, :via 'mgr-clear!}))
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
      (let [close-f (future (car/conn-manager-close! mgr {:timeout-ms Long/MAX_VALUE}))]
        (support/await-pred! 2000 "the manager to close"
          #(not (car/conn-manager-open? mgr)))
        (deliver release true)
        [(is (= (deref borrow-f 2000 ::timeout) true))
         (is (= (deref close-f 2000 ::timeout)
                true))])
      (finally
        (deliver release true)
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

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
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

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
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

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
                  (let [close-result (car/conn-manager-close! mgr {:timeout-ms 0})]
                    (deliver resume-creation true)
                    [close-result (deref borrow-f 2000 ::timeout)]))))]
        [(is (= close-result true))
         (is (= (:eid (ex-data borrow-error)) :carmine.conns/manager-closed))
         (is (false? @handler-called_)
           "A connection created after the close snapshot cannot enter user code")])
      (finally
        (deliver resume-creation true)
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

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
            clear-f (future (car/conn-manager-clear! mgr {:timeout-ms 2000}))]
        (support/await-pred! 2000 "the clear to start"
          #(pos? (get-in @mgr [:stats :counts :cleared])))
        (deliver release :release)
        [(is (= (deref borrow-f 2000 ::timeout) :done))
         (is (= (deref clear-f 2000 ::timeout)
               true))
         (is (zero? (get-in @mgr [:stats :connections :idle]))
           "An active connection from the cleared generation cannot return idle")
         (is (= (get-in (first @close-events_) [:data :via]) 'mgr-clear!))
         (is (= (wcar mgr (resp/ping)) "PONG"))
         (is (> (get-in @mgr [:stats :counts :created]) (:created before)))])
      (finally
        (deliver release :release)
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

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
            clear-f-a (future (car/conn-manager-clear! mgr {:timeout-ms 5001}))]

        (support/await-pred! 2000 "clear A to start"
          #(= (get-in @mgr [:stats :counts :cleared]) 1))

        (let [borrow-f-b
              (future
                (conns/mgr-borrow! mgr
                  (fn [conn _in _out]
                    (deliver borrowed-b conn)
                    @release-b
                    :b)))
              conn-b (deref borrowed-b 2000 ::timeout)
              _      (is (not= conn-b ::timeout))
              clear-f-b (future (car/conn-manager-clear! mgr {:timeout-ms 5002}))]

          (support/await-pred! 2000 "clear B to start"
            #(= (get-in @mgr [:stats :counts :cleared]) 2))

          ;; Synchronize after clear B's generation change and snapshot.
          (is (= (wcar mgr (resp/ping)) "PONG"))

          ;; Clear A finishes while clear B is still draining. It must not erase
          ;; B's callback context before B's active connection is returned.
          (deliver release-a :release)
          [(is (= (deref borrow-f-a 2000 ::timeout) :a))
           (is (= (deref clear-f-a 2000 ::timeout)
                 true))]

          (deliver release-b :release)
          [(is (= (deref borrow-f-b 2000 ::timeout) :b))
           (is (= (deref clear-f-b 2000 ::timeout)
                 true))]

          (let [event-b (some #(when (identical? (:conn %) conn-b) %) @close-events_)]
            [(is (some? event-b))
             (is (truss/submap? (:data event-b)
                   {:via 'mgr-clear!, :timeout-ms 5002}))])))
      (finally
        (deliver release-a :release)
        (deliver release-b :release)
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

(deftest _pooled-ready-check-idle-threshold
  [(is (#'conns/pooled-conn-ping? 0 0 1000)
     "A check at creation (`:test-on-create?`) always sends PING")
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
                    (car/conn-manager-close! mgr {:timeout-ms 5000, :data {:reason :test}}))]

              (support/await-pred! 2000 "the manager to close"
                #(not (car/conn-manager-open? mgr)))
              (deliver release :release)
              (is (= (deref borrow-f 2000 ::timeout) :done))
              (is (= (deref close-f 2000 ::timeout)
                    true)
                "Close finishes when an active connection is returned normally")
              (is (= (mapv
                       #(select-keys (:data %) [:via :timeout-ms :reason])
                       @close-events_)
                    [{:via 'mgr-close!, :timeout-ms 5000, :reason :test}])
                "A connection destroyed on the borrower thread retains close context")))

          (finally
            (deliver release :release)
            (car/conn-manager-close! mgr {:timeout-ms 0})))))))

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
        (support/await-pred! 2000 "the manager to close"
          #(not (car/conn-manager-open? mgr)))
        (.interrupt close-thread)
        (let [[error interrupted?] (deref result 2000 [::timeout false])]
          [(is (instance? InterruptedException error))
           (is interrupted? "Shutdown restores the interrupted status")
           (is (= (count @close-events_) 1))
           (is (= (select-keys (:data (first @close-events_))
                    [:via :timeout-ms])
                  {:via 'mgr-close!, :timeout-ms nil})
             "Interrupted shutdown force-closes with manager-close context")
           (is (= (car/conn-manager-close! mgr) false))]))
      (finally
        (deliver release :release)
        (.interrupt close-thread)
        (.join close-thread 2000)
        (deref borrow-f 2000 ::timeout)
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

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
    [(is (= (car/conn-manager-close! mgr {:timeout-ms 0, :data {}})
           true)) ; Interrupt pool conns
     (is (throws? java.net.SocketException @f)
       "Close with zero timeout interrupts blocking blpop")]))

(deftest _request-io-errors
  (with-open [mgr (conns/conn-manager-pooled
                    {:conn-opts
                     (enc/nested-merge support/test-conn-opts
                       {:socket-opts {:read-timeout-ms 100}})})]
    (let [err  (truss/throws (wcar mgr (resp/rcmd "BLPOP" (tk "no-list") 1)))
          data (ex-data err)]
      [(is (= (:eid data) :carmine.conns/conn-io-error))
       (is (= (:addr data) (:server tc+)))
       (is (= (:read-timeout-ms data) 100))
       (is (instance? java.net.SocketTimeoutException (ex-cause err)))
       (is (instance? java.net.SocketTimeoutException
             (ex-cause
               (truss/throws
                 (wcar mgr (wcar mgr (resp/rcmd "BLPOP" (tk "no-list") 1))))))
         "An error from a nested borrow is wrapped once")
       (is (= (wcar mgr (resp/ping)) "PONG"))])))

(deftest _caught-flush-errors
  (with-open [mgr (conns/conn-manager-pooled
                    {:conn-opts
                     (enc/nested-merge support/test-conn-opts
                       {:socket-opts {:read-timeout-ms 200}})})]
    (let [timeout! ; Read times out before reply arrives, error is caught
          (fn []
            (truss/catching
              (with-replies (resp/rcmd "BLPOP" (tk "no-list") 1))))]

      [(is (->> (wcar mgr (timeout!) (resp/echo "same-conn"))
             (throws? :ex-info {:eid :carmine/conn-broken}))
         "No more requests on a conn after a failed flush")
       (is (= (wcar mgr (resp/echo "fresh")) "fresh"))
       (is (= (wcar mgr (timeout!)) nil))
       (is (= (wcar mgr (resp/echo "fresh")) "fresh")
         "The unread reply doesn't leak to the next borrower")
       (is (= (:total (:destroyed (:counts (car/conn-manager-stats mgr)))) 2))])))

(deftest _requests-from-reply-parsers
  (with-open [mgr (conns/conn-manager-pooled {:conn-opts support/test-conn-opts})]
    (let [replies
          (wcar mgr {:as-vec? true, :error-mode :return}
            (car/parse (fn [_] (with-replies (resp/echo "inner")))
              (resp/echo "first"))
            (resp/echo "tail")
            (with-replies) ; Flushes the above, with their context still current
            (resp/echo "next"))]

      [(is (car/reply-error? {:eid :carmine.read/parser-error} (first replies)))
       (is (= (:eid (ex-data (ex-cause (first replies)))) :carmine/conn-busy)
         "A parser can't send requests on the conn that's reading its reply")
       (is (= (subvec replies 1) ["tail" "next"]))
       (is (= (:total (:destroyed (:counts (car/conn-manager-stats mgr)))) 0)
         "And the conn remains reusable")])))

(deftest _borrow-error-message
  (let [port (with-open [ss (java.net.ServerSocket. 0)] (.getLocalPort ss)) ; Now closed
        err
        (with-open [mgr (conns/conn-manager-pooled {:conn-opts {:server ["127.0.0.1" port]}})]
          (truss/throws (wcar mgr (resp/ping))))]
    [(is (= (:eid  (ex-data err)) :carmine.conns/borrow-conn-error))
     (is (= (:addr (ex-data err)) ["127.0.0.1" port]))
     (is (enc/str-starts-with? (ex-message err)
           (str "[Carmine] Error borrowing connection from `ConnManager` (127.0.0.1:" port "): Connection refused"))
       "Message includes the address and root cause")]))

(deftest _nested-borrows
  (with-open [mgr (conns/conn-manager-pooled {:conn-opts support/test-conn-opts})]
    (let [counts (fn [] (:counts (car/conn-manager-stats mgr)))
          ids    (fn [& outer-reqs] ; => [<outer-conn-id> <nested-conn-id>]
                   (vec
                     (take-last 2
                       (wcar mgr :as-vec
                         (run! resp/rcmd* outer-reqs)
                         (resp/rcmd "CLIENT" "ID")
                         (resp/local-echo (wcar mgr (resp/rcmd "CLIENT" "ID")))))))
          _ (dotimes [_ 3] (ids))
          counts1 (counts)
          [outer-id nested-id] (ids ["SELECT" 1])]

      [(is (= (:created counts1) 2)
         "Initializing a nested borrow's conn leaves the outer conn reusable")
       (is (= (:total (:destroyed counts1)) 0))
       (is (= (:total (:destroyed (counts))) 1))
       (is (= (some #{outer-id}  (ids)) nil)
         "A stateful request queued before a nested borrow closes the outer conn")
       (is (= (some #{nested-id} (ids)) nested-id)
         "And not the nested conn")])))

(deftest _wcar-basics
  [(doseq [legacy-opts [#{:as-vec} #{:natural-replies} :as-vce]]
     (is (->> (resp/parse-body-reply-opts [legacy-opts])
           (throws? :ex-info {:eid :carmine/invalid-reply-opts}))))
   (is (= (resp/parse-body-reply-opts [:as-vec '(ping)])
         [{:as-vec? true, :error-mode :throw}
          '((ping))]))
   (is (= (resp/parse-body-reply-opts [:as-pipeline '(ping)])
          (resp/parse-body-reply-opts [:as-vec      '(ping)]))
     "`:as-pipeline` is an undocumented alias for `:as-vec` (v3 back compatibility)")
   (doseq [opts [{:unknown? true}
                 {:as-vec? :yes}
                 {:natural-replies? true}
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
       (is (= eid :carmine/reply-errors))
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

;;;; Returns

(deftest _pooled-invalidation-outside-clear-lock
  (let [borrowed-a (promise), release-a (promise)
        borrowed-b (promise), release-b (promise)
        gate-armed_ (atom false)
        creating    (promise)
        resume      (promise)
        original-new-conn @#'conns/new-conn
        mgr (conns/conn-manager-pooled
              {:pool-opts {:max-total 2, :test-while-idle? false}})]
    (try
      (with-redefs-fn
        {#'conns/new-conn
         (fn [& args]
           (when @gate-armed_ (deliver creating true) @resume)
           (apply original-new-conn args))}
        (fn []
          (let [borrow-a (future (wcar mgr (deliver borrowed-a true) @release-a (resp/rcmd "SELECT" "0")))
                borrow-b (future (wcar mgr (deliver borrowed-b true) @release-b (resp/ping)))
                _ [(deref borrowed-a 2000 ::timeout) (deref borrowed-b 2000 ::timeout)]
                _ (reset! gate-armed_ true)
                waiter (future (wcar mgr (resp/ping)))]
            (support/await-pred! 2000 "a borrower to wait"
              #(pos? (get-in @mgr [:stats :connections :waiting])))
            (deliver release-a true) ; Stateful => invalidated, replacement blocks
            [(is (= (deref creating 2000 ::timeout) true))
             (do
               (deliver release-b true)
               (is (= (deref borrow-b 2000 ::timeout) "PONG")
                 "Another return doesn't wait on the replacement's creation"))
             (do
               (deliver resume true)
               [(is (= (deref borrow-a 2000 ::timeout) "OK"))
                (is (= (deref waiter   2000 ::timeout) "PONG"))])])))
      (finally
        (deliver release-a true)
        (deliver release-b true)
        (deliver resume true)
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

(deftest _pooled-stale-replacement-not-reused
  (let [borrowed (promise), release (promise)
        gate-armed_ (atom false)
        creating    (promise)
        resume      (promise)
        original-new-conn @#'conns/new-conn
        mgr (conns/conn-manager-pooled
              {:pool-opts {:max-total 1, :max-wait-ms 1000, :test-while-idle? false}})]
    (try
      (with-redefs-fn
        {#'conns/new-conn
         (fn [& args]
           (when @gate-armed_ (deliver creating true) @resume)
           (apply original-new-conn args))}
        (fn []
          (let [borrow (future (wcar mgr (deliver borrowed true) @release (resp/rcmd "SELECT" "0")))
                _      (deref borrowed 2000 ::timeout)
                _      (reset! gate-armed_ true)
                waiter (future (truss/throws (wcar mgr (resp/ping))))]
            (support/await-pred! 2000 "a borrower to wait"
              #(pos? (get-in @mgr [:stats :connections :waiting])))
            (deliver release true) ; Invalidated, replacement creation blocks
            (is (= (deref creating 2000 ::timeout) true))
            (is (= (:eid (ex-data (deref waiter 2000 nil))) :carmine.conns/borrow-conn-error)
              "The waiter gives up")
            (is (true? (car/conn-manager-clear! mgr {:timeout-ms 0})))
            (reset! gate-armed_ false)
            (deliver resume true)
            (is (= (deref borrow 2000 ::timeout) "OK"))
            (support/await-pred! 2000 "the replacement to land idle"
              #(= (get-in @mgr [:stats :connections :idle]) 1))
            (let [destroyed #(get-in @mgr [:stats :counts :destroyed :total])
                  before (destroyed)]
              [(is (= (wcar mgr (resp/ping)) "PONG"))
               (is (= (destroyed) (inc before))
                 "A conn created before a clear is never reused after it")]))))
      (finally
        (deliver release true)
        (deliver resume true)
        (car/conn-manager-close! mgr {:timeout-ms 0})))))

(deftest _clear-within-borrow
  (with-open [mgr (conns/conn-manager-pooled {:pool-opts {:max-total 1}})]
    (let [t0 (System/currentTimeMillis)
          [cleared? pong]
          (wcar mgr :as-vec
            (resp/local-echo (car/conn-manager-clear! mgr {:timeout-ms 5000}))
            (resp/ping))]
      [(is (true? cleared?))
       (is (= pong "PONG"))
       (is (< (- (System/currentTimeMillis) t0) 2500)
         "A clear doesn't wait on the calling thread's own borrow")
       (is (zero? (get-in @mgr [:stats :connections :idle]))
         "Which isn't reused")])))

(deftest _return-validation
  [(let [errors_ (atom [])]
     (binding [taoensso.carmine-v4/*conn-cbs* {:on-conn-error #(swap! errors_ conj %)}]
       (with-open [conn (conns/new-conn tc+)]
         [(is (false? (#'conns/return-valid? (fn [_] (throw (ex-info "Lookup failed" {}))) conn))
            "A return check that throws fails")
          (is (= (ex-message (:cause (first @errors_))) "Lookup failed")
            "And is reported")])))

   (with-open [mgr (conns/conn-manager-pooled
                     {:pool-opts {:max-total 1, :test-on-return? true
                                  :test-while-idle? false}})]
     (let [stats  (fn [] (:stats @mgr))
           result (wcar mgr (resp/rcmd "INCR" (tk "return-validation")))
           before (stats)
           killed
           (wcar mgr
             (kill-client! (car/with-replies (resp/rcmd "CLIENT" "ID")))
             (resp/local-echo :killed))]
       [(is (= result 1))
        (is (= (get-in before [:connections :idle]) 1) "A healthy conn is returned")
        (is (= killed :killed) "A failed return check keeps the request's result")
        (is (= (get-in (stats) [:connections :idle]) 0))
        (is (> (get-in (stats) [:counts :destroyed :total])
               (get-in before  [:counts :destroyed :total])))
        (is (= (wcar mgr (resp/rcmd "GET" (tk "return-validation"))) "1"))]))])

(deftest _nested-unpooled-borrow-keeps-outer-conn
  (with-open [outer (conns/conn-manager-pooled {:pool-opts {:max-total 1}})
              inner (conns/conn-manager-unpooled {})]
    (let [id      #(wcar outer (resp/rcmd "CLIENT" "ID"))
          id-before (id)
          nested  (wcar outer :as-vec
                    (resp/rcmd "CLIENT" "ID")
                    (resp/local-echo (wcar inner (resp/rcmd "SELECT" "0"))))]
      [(is (= nested [id-before "OK"]))
       (is (= (id) id-before) "A stateful inner unpooled request doesn't discard the outer conn")
       (is (zero? (get-in @outer [:stats :counts :destroyed :total])))])))

(deftest _skip-replies-preserves-errors
  (with-open [mgr
              (conns/conn-manager-pooled
                {:pool-opts {:max-total 1, :max-idle 1
                             :test-on-borrow? false}})]
    (let [key (tk "skip-replies:counter")
          client-id-before (wcar mgr (resp/rcmd "CLIENT" "ID"))
          failure
          (truss/throws
            (wcar mgr
              (car/skip-replies
                (car/set key "not-an-integer")
                (car/incr key)
                (car/ping))
              (car/get key)))
          {:keys [replies error-indexes]} (ex-data failure)]
      [(is (car/reply-error? failure))
       (is (= error-indexes [0])
         "Error indexes describe retained replies, not original commands")
       (is (car/reply-error? (first replies)))
       (is (= (second replies) "not-an-integer")
         "The whole pipeline is drained before throwing")
       (is (= (wcar mgr (resp/rcmd "CLIENT" "ID")) client-id-before)
         "A skipped Redis error leaves the drained connection reusable")])

    (let [parser-called? (atom false)
          replies
          (wcar mgr {:error-mode :return}
            (car/skip-replies
              (car/parse
                (fn [_] (reset! parser-called? true))
                (car/ping)
                (resp/rcmd "CARMINE-INVALID-COMMAND")))
            (car/ping))]
      [(is (= (count replies) 2))
       (is (car/reply-error? (first replies)))
       (is (= (second replies) "PONG"))
       (is (false? @parser-called?)
         "Skipped replies bypass parsers")])

    (is (car/reply-error?
          (wcar mgr {:error-mode :return}
            (car/skip-replies (resp/rcmd "CARMINE-INVALID-COMMAND"))))
      "One retained error follows the usual scalar reply shape")))

(deftest _wcar-read-opts-interactions
  (let [key (tk (str "read-opts:" (java.util.UUID/randomUUID)))
        value {:nested [1 2 3]}]
    (wcar mgr_ (car/set key value))
    (let [natural (wcar mgr_ (car/natural-replies (car/get key)))]
      [(is (and (string? natural) (not= natural value))
         "Natural top-level replies bypass automatic thawing")
       (is (= (wcar mgr_ (car/thaw nil (car/get key))) value))
       (is (enc/bytes? (wcar mgr_ (car/as-bytes (car/get key))))
         "Explicit bytes mode overrides automatic thawing")])

    (is (= (wcar mgr_
             (car/parse str/lower-case (car/ping))
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

(deftest _conn-state-after-wait-and-himport
  (with-open [mgr (conns/conn-manager-pooled {:pool-opts {:max-total 1, :max-idle 1}})]
    (let [id #(wcar mgr (resp/rcmd "CLIENT" "ID"))
          id1 (id)]
      [(is (integer? (wcar mgr (resp/rcmd "WAIT" 0 0))))
       (is (= (id) id1) "WAIT leaves its connection reusable")
       (truss/catching (wcar mgr (resp/rcmd "HIMPORT" "DISCARDALL"))) ; Needs Redis 8.10+
       (is (not= (id) id1) "HIMPORT fieldsets aren't returned to the pool")])))
