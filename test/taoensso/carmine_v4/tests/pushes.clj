(ns taoensso.carmine-v4.tests.pushes
  "Carmine v4 RESP3 push-dispatch tests."
  (:require
   [clojure.test        :refer [deftest is]]
   [taoensso.truss      :as truss]
   [taoensso.carmine-v4          :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.cluster  :as cluster]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tk  "Test key" support/test-key)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

(defn- redis-major-version [] (support/redis-major-version))

;;;; Pushes

(deftest _manager-push-dispatch
  (let [handled_ (atom [])
        global-errors_ (atom [])
        conn-errors_   (atom [])
        mgr
        (conns/conn-manager-unpooled
          {:mgr-name "push-test"
           :conn-opts {:cbs {:on-push-error #(swap! conn-errors_ conj %)}}
           :push-fn
           (fn [event]
             (swap! handled_ conj
               {:event event
                :thread-name (.getName (Thread/currentThread))
                :daemon? (.isDaemon (Thread/currentThread))})
             (when (= (:type event) :throws)
               (throw (ex-info "Expected manager push failure" {:event event}))))})
        dispatch (conns/mgr-push-fn mgr ["node.redis" 6380])
        await-count
        (fn [a n]
          (let [deadline (+ (System/currentTimeMillis) 5000)]
            (loop []
              (if (<= n (count @a))
                true
                (if (< (System/currentTimeMillis) deadline)
                  (do (Thread/sleep 2) (recur))
                  false)))))]
    (try
      (binding [car/*conn-cbs* {:on-push-error #(swap! global-errors_ conj %)}]
        (dispatch [:notice "one"])
        (dispatch [:throws "two"])
        (is (await-count handled_ 2))
        (is (await-count global-errors_ 1))
        (is (await-count conn-errors_ 1))

        (let [[first-call second-call] @handled_
              first-event (:event first-call)
              second-event (:event second-call)
              error (first @conn-errors_)]
          [(is (= (mapv (comp :type :event) @handled_) [:notice :throws])
             "One manager dispatches pushes serially in submission order")
           (is (truss/submap? first-event
                 {:data [:notice "one"]
                  :type :notice
                  :source
                  {:server-addr ["node.redis" 6380]
                   :cluster? false}}))
           (is (identical? (get-in first-event [:source :manager]) mgr))
           (is (= (:thread-name first-call) (:thread-name second-call)))
           (is (true? (:daemon? first-call)))
           (is (= @global-errors_ @conn-errors_))
           (is (truss/submap? error
                 {:cbid :on-push-error
                  :eid :carmine.push/handler-error
                  :via 'manager-push-fn}))
           (is (= (:push-event error) second-event))
           (is (= (ex-message (:cause error)) "Expected manager push failure"))
           (is (truss/submap? (car/conn-manager-stats mgr)
                 {:schema-version 1, :kind :unpooled}))
           (is (= (get-in (car/conn-manager-stats mgr)
                    [:push :counts])
                 {:received 2, :completed 2, :rejected 0, :handler-errors 1
                  :discarded-on-close 0}))])

        (is (= (car/conn-manager-close! mgr nil nil)
              {:action :closed, :graceful? true}))
        (dispatch [:after-close])
        (is (await-count global-errors_ 2))
        (is (= (get-in (car/conn-manager-stats mgr)
                 [:push :counts :rejected]) 1))
        (is (instance? java.util.concurrent.RejectedExecutionException
              (:cause (second @global-errors_)))
          "Closing a manager shuts down and rejects work on its push executor"))
      (finally (car/conn-manager-close! mgr 0 nil)))))

(deftest _manager-push-dispatch-is-bounded
  (let [started (promise)
        release (promise)
        errors_ (atom [])]
    (with-open [mgr
                (conns/conn-manager-unpooled
                  {:push-queue-capacity 1
                   :conn-opts {:cbs {:on-push-error #(swap! errors_ conj %)}}
                   :push-fn
                   (fn [_]
                     (deliver started true)
                     @release)})]
      (let [dispatch (conns/mgr-push-fn mgr ["node.redis" 6380])]
        (try
          (dispatch [:first])
          (is (= (deref started 5000 ::timeout) true))
          (dispatch [:queued])
          (dispatch [:overflow])
          (let [deadline (+ (System/currentTimeMillis) 5000)]
            (loop []
              (when (and (empty? @errors_) (< (System/currentTimeMillis) deadline))
                (Thread/sleep 2)
                (recur))))
          [(is (= (:eid (first @errors_)) :carmine.push/dispatch-rejected))
           (is (instance? java.util.concurrent.RejectedExecutionException
                 (:cause (first @errors_)))
             "A full bounded push queue drops new work observably")
           (is (= (car/conn-manager-close! mgr 0 nil)
                 {:action :closed, :graceful? true}))
           (is (truss/submap? (get-in (car/conn-manager-stats mgr)
                                [:push :counts])
                 {:received 3, :rejected 1, :discarded-on-close 1})
             "Close accounts for accepted push tasks that never started")]
          (finally (deliver release true)))))))

(deftest _pooled-validation-dispatches-pushes
  (when (>= (redis-major-version) 6)
    (let [key (tk (str "validation-push:" (java.util.UUID/randomUUID)))
          event_ (promise)]
      (with-open [mgr
                  (conns/conn-manager-pooled
                    {:push-fn #(deliver event_ %)
                     :conn-opts
                     {:init
                      {:commands
                       [["HELLO" 3]
                        ["CLIENT" "TRACKING" "ON" "BCAST" "PREFIX" key]]}}
                     :pool-opts
                     {:max-total 1, :max-idle 1
                      :ready-check-after-idle-ms 0}})]
        (is (nil? (wcar mgr (resp/rcmd "GET" key))))
        (is (= (wcar mgr_ (resp/rcmd "SET" key "changed")) "OK"))
        (is (= (wcar mgr (resp/ping)) "PONG"))
        (let [event (deref event_ 5000 ::timeout)]
          [(is (not= event ::timeout)
             "Borrow validation dispatches a queued push before consuming PONG")
           (is (= (:type event) "invalidate"))
           (is (identical? (get-in event [:source :manager]) mgr))])))))

(deftest _cluster-push-dispatch-source
  (let [event_ (promise)
        spec (cluster/cluster-spec [["seed.redis" 7000]])]
    (with-open [mgr
                (conns/conn-manager-clustered
                  {:conn-opts {:server {:cluster-spec spec}}
                   :push-fn #(deliver event_ %)})]
      (let [addr ["node.redis" 7001]
            reply
            (#'cluster/read-cluster-reply!
              com/read-opts-natural
              (conns/mgr-push-fn mgr addr)
              (com/str->in ">2\r\n+notice\r\n+payload\r\n+OK\r\n"))
            event (deref event_ 5000 ::timeout)]
        [(is (= reply "OK"))
         (is (truss/submap? event
               {:data ["notice" "payload"]
                :type "notice"
                :source {:server-addr addr, :cluster? true}}))
         (is (identical? (get-in event [:source :manager]) mgr))]))))

(deftest _live-push-interleaving
  (when (some true? (support/supported-resp3-options))
    (let [key    (tk (str "push:" (java.util.UUID/randomUUID)))
          event_ (promise)
          results_ (volatile! nil)]
      (with-open [mgr
                  (conns/conn-manager-unpooled
                    {:conn-opts {:init {:resp3? true}
                                 :socket-opts {:read-timeout-ms 2000}}
                     :push-fn #(deliver event_ %)})]
        (car/with-car mgr
          (fn []
            (let [tracking
                  (with-replies
                    (resp/rcmd "CLIENT" "TRACKING" "ON"
                      "BCAST" "PREFIX" key))]

              ;; A different connection generates a push that Redis places
              ;; ahead of the tracking connection's next ordinary reply.
              (wcar mgr_ (resp/rcmd "SET" key "value"))

              (vreset! results_
                {:tracking tracking
                 :replies
                 (with-replies {:as-vec? true}
                   (resp/rcmd "PING")
                   (resp/rcmd "GET" key)
                   (resp/rcmd "ECHO" "tail"))
                 :tracking-off
                 (with-replies (resp/rcmd "CLIENT" "TRACKING" "OFF"))
                 :final-ping
                 (with-replies (resp/rcmd "PING"))}))))

        (let [{:keys [tracking replies tracking-off final-ping]} @results_
              push-event (deref event_ 2000 ::timeout)]

          [(is (= tracking "OK"))
           (is (truss/submap? push-event
                 {:data ["invalidate" [key]], :type "invalidate"})
             "A real RESP3 tracking invalidation is consumed as a push")
           (is (identical? (get-in push-event [:source :manager]) mgr))
           (is (false? (get-in push-event [:source :cluster?])))
           (is (= replies ["PONG" "value" "tail"])
             "The interleaved push does not consume or reorder pipeline replies")
           (is (= tracking-off "OK"))
           (is (= final-ping "PONG")
             "The connection remains correctly framed and usable")])))))
