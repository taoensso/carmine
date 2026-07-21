(ns taoensso.carmine-v4.tests.pubsub
  "High-level v4 Pub/Sub tests against a running standalone Redis server."
  (:require
   [clojure.test :refer [deftest testing is]]
   [taoensso.trove :as trove]
   [taoensso.truss :as truss :refer [throws?]]
   [taoensso.carmine-v4 :as car :refer [wcar]]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.pubsub :as pubsub]
   [taoensso.carmine-v4.resp.write :as write]
   [taoensso.carmine-v4.tests.test-support :as support]))

(defn- await! [p]
  (let [result (deref p 5000 ::timeout)]
    (is (not= result ::timeout))
    result))

(defn- await-pred! [timeout-ms description f]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (if-let [result (f)]
        result
        (if (< (System/currentTimeMillis) deadline)
          (do (Thread/sleep 10) (recur))
          (throw (ex-info (str "Timed out waiting for " description)
                   {:timeout-ms timeout-ms})))))))

(defn- assert-stats-schema! [expected-kind listener]
  (let [{:keys [schema-version kind since-client-time-ms
                snapshot-client-time-ms counts] :as stats}
        (car/pubsub-stats listener)]
    [(is (= (set (keys stats))
           #{:schema-version :kind :since-client-time-ms
             :snapshot-client-time-ms :counts}))
     (is (= [schema-version kind] [2 expected-kind]))
     (is (pos-int? since-client-time-ms))
     (is (<= since-client-time-ms snapshot-client-time-ms))
     (is (= (set (keys counts))
           #{:events :messages :handler-errors
             :recoveries :recovery-errors}))
     (is (= (dissoc stats :snapshot-client-time-ms)
            (dissoc (:stats @listener) :snapshot-client-time-ms)))]))

(deftest _frame-normalization
  [(is (= (dissoc (pubsub/frame->event ["message" "c" "v"]) :raw)
          {:kind :message, :channel "c", :payload "v"}))
   (is (= (dissoc (pubsub/frame->event ["pmessage" "p*" "pub" "v"]) :raw)
          {:kind :pmessage, :pattern "p*", :channel "pub", :payload "v"}))
   (is (= (dissoc (pubsub/frame->event ["unsubscribe" nil 0]) :raw)
          {:kind :unsubscribe, :channel nil, :count 0}))
   (is (= (:kind (pubsub/frame->event ["smessage" "c" "v"])) :other))
   (is (truss/submap? (pubsub/frame->event ["pong" ""])
         {:kind :pong, :payload nil}))
   (is (truss/submap? (pubsub/frame->event "PONG")
         {:kind :pong, :payload nil}))])

(deftest _cluster-rejected
  (let [spec (car/cluster-spec [["127.0.0.1" 7000]])]
    (is (throws? :ex-info {:eid :carmine.pubsub/cluster-not-supported}
          (car/pubsub-listener
            {:conn-opts {:server {:cluster-spec spec}}
             :handler-fn identity})))))

(deftest _stats-reject-invalid-listener
  (is (throws? :ex-info {:eid :carmine.pubsub/invalid-listener}
        (car/pubsub-stats :invalid))))

(deftest _listener-option-contract
  [(is (throws? :ex-info {}
         (car/pubsub-listener {:listener-fn identity})))
   (is (throws? :ex-info {}
         (car/pubsub-listener {:handler-fn identity, :listener-fn :invalid})))
   (is (throws? :ex-info {}
         (car/pubsub-listener
           {:handler-fn identity, :handler-dispatch-fn identity})))
   (is
     (throws? :ex-info {}
       (car/pubsub-listener {:handler-fn identity, :cbs {}})))])

(deftest _initial-subscriptions-validated-before-connect
  (let [connects_ (atom 0)]
    (with-redefs [conns/new-conn
                  (fn [& _]
                    (swap! connects_ inc)
                    (throw (AssertionError. "Connection should not be opened")))]
      [(is (throws? :ex-info {:eid :carmine.pubsub/invalid-name}
             (car/pubsub-listener
               {:handler-fn identity, :init-subs {:channels [42]}})))
       (is (throws? :ex-info {}
             (car/pubsub-listener
               {:handler-fn identity
                :init-subs {:channels ["channel"], :timeout-ms :invalid}})))
       (is (zero? @connects_))])))

(deftest _operation-options-validated-before-effects
  (doseq [recovery [nil {:check-ms 60000}]]
    (let [listener
          (car/pubsub-listener
            (cond-> {:handler-fn identity}
              recovery (assoc :recovery recovery)))
          writes_ (atom 0)
          original-write write/write-requests]
      (try
        (with-redefs [write/write-requests
                      (fn [& args]
                        (swap! writes_ inc)
                        (apply original-write args))]
          (doseq [operation
                  [#(car/pubsub-subscribe! listener
                      {:channels ["invalid"], :timeout-ms :invalid})
                   #(car/pubsub-unsubscribe! listener
                      {:timeout-ms :invalid})
                   #(car/pubsub-ping! listener
                      {:message "invalid", :timeout-ms :invalid})]]
            (is (throws? :ex-info {} (operation)))))
        [(is (zero? @writes_))
         (is (true? (:open? @listener)))
         (is (= (:subs @listener) {:channels #{}, :patterns #{}}))
         (when recovery
           (is (= (:desired-subs @listener)
                 {:channels #{}, :patterns #{}})))]
        (finally (car/pubsub-close! listener))))))

(deftest _recovery-rejects-sentinel-replica-preference
  (let [spec (car/sentinel-spec {:primary [["127.0.0.1" 26379]]})]
    (is (throws? :ex-info {:eid :carmine.pubsub/replica-recovery-not-supported}
          (car/pubsub-listener
            {:conn-opts
             {:server
              {:master-name :primary
               :sentinel-spec spec
               :sentinel-opts {:prefer-read-replica? true}}}
             :handler-fn identity
             :recovery {}})))))

(deftest _live-protocol-parity
  (doseq [resp3? (support/supported-resp3-options)]
    (testing (str "RESP" (if resp3? 3 2))
      (let [suffix       (str (java.util.UUID/randomUUID))
            channel      (str "carmine:v4:pubsub:" suffix)
            pattern      (str "carmine:v4:pubsub-pattern:" suffix ":*")
            pattern-chan (str "carmine:v4:pubsub-pattern:" suffix ":1")
            duplicate-channel (str "carmine:v4:pubsub-duplicate:" suffix)
            message_     (promise)
            pmessage_    (promise)
            handler-cb_  (promise)
            close-cbs_   (atom [])
            conn-errors_ (atom [])
            events_      (atom [])
            listener
            (car/pubsub-listener
              {:conn-opts {:init {:resp3? resp3?}}
               :handler-fn
               (fn [event]
                 (swap! events_ conj event)
                 (case (:kind event)
                   :message  (deliver message_ event)
                   :pmessage (deliver pmessage_ event)
                   nil)
                 (when (= (:payload event) {:throw? true})
                   (throw (ex-info "Expected handler failure" {}))))
               :init-subs {:channels [channel], :patterns [pattern]}
               :listener-fn
               (fn [event]
                 (case (:kind event)
                   :handler-error (deliver handler-cb_ event)
                   :conn-error    (swap! conn-errors_ conj event)
                   :closed        (swap! close-cbs_ conj event)
                   nil))})]
        (try
          (is (car/pubsub-listener? listener))
          (assert-stats-schema! :connection-bound listener)
          (is (= (:subs @listener)
                {:channels #{channel}, :patterns #{pattern}}))

          (with-open [mgr (car/conn-manager-unpooled
                            {:conn-opts {:init {:resp3? resp3?}}})]
            (is (= (wcar mgr (car/publish channel {:protocol (if resp3? 3 2)})) 1))
            (is (= (wcar mgr (car/publish pattern-chan "pattern-value")) 1))
            (wcar mgr (car/publish channel {:throw? true})))

          (is (= (:payload (await! message_)) {:protocol (if resp3? 3 2)}))
          (is (= (:payload (await! pmessage_)) "pattern-value"))
          (let [event (await! handler-cb_)]
            [(is (= (:kind event) :handler-error))
             (is (identical? (:listener event) listener))
             (is (= (:epoch event) 0))
             (is (= (get-in event [:redis-event :payload]) {:throw? true}))
             (is (instance? Throwable (:cause event)))
             (is (empty? (select-keys event [:eid :cbid :via :phase])))])
          (is (truss/submap? (car/pubsub-ping! listener)
                {:kind :pong, :payload nil, :listener listener, :epoch 0}))
          (is (= (:payload (car/pubsub-ping! listener {:message "probe"})) "probe"))

          (is (= :acknowledged (car/pubsub-subscribe! listener {:channels [channel]}))
            "Redis re-acknowledges duplicate subscriptions")
          (is (= :acknowledged (car/pubsub-subscribe! listener {:channels [duplicate-channel]})))
          (is (= :acknowledged (car/pubsub-unsubscribe! listener {:channels [duplicate-channel]})))
          (is (= :acknowledged (car/pubsub-unsubscribe! listener {:channels [duplicate-channel]}))
            "Redis re-acknowledges duplicate unsubscriptions")

          (is (= :acknowledged (car/pubsub-unsubscribe! listener
                       {:channels [channel], :patterns [pattern]})))
          (is (= (:subs @listener) {:channels #{}, :patterns #{}}))
          (is (= :acknowledged (car/pubsub-unsubscribe! listener {}))
            "Unsubscribe-all handles Redis's nil-name acknowledgements")
          (is (= :acknowledged (car/pubsub-subscribe! listener
                       {:channels [channel], :patterns [pattern]})))
          (is (= :acknowledged (car/pubsub-unsubscribe! listener))
            "Unsubscribe-all drains active channel and pattern subscriptions")
          (is (= (:subs @listener) {:channels #{}, :patterns #{}}))

          (finally
            (is (true? (car/pubsub-close! listener)))
            (is (false? (car/pubsub-close! listener)))
            (is (false? (:open? @listener)))
            (is (= 1 (count @close-cbs_)))
            (is (= (:reason (first @close-cbs_)) :requested))
            (is (empty? @conn-errors_)
              "Requested close is not reported as a connection error")))))))

(deftest _handler-driven-subscribe-does-not-deadlock
  (let [suffix  (str (java.util.UUID/randomUUID))
        first-c (str "carmine:v4:pubsub:first:" suffix)
        next-c  (str "carmine:v4:pubsub:next:" suffix)
        armed?_ (atom true)
        operation-results_ (promise)
        next-ack_ (promise)
        message_  (promise)]
    (with-open [^java.io.Closeable listener
                (car/pubsub-listener
                  {:handler-fn
                   (fn [event]
                     (when (and (= (:kind event) :subscribe)
                                (= (:channel event) first-c)
                                (compare-and-set! armed?_ true false))
                       (deliver operation-results_
                         [(car/pubsub-subscribe! (:listener event)
                            {:channels [next-c]})
                          (car/pubsub-unsubscribe! (:listener event)
                            {:channels [first-c]})
                          (car/pubsub-ping! (:listener event))
                          (car/pubsub-await-synced! (:listener event))]))
                     (when (and (= (:kind event) :subscribe)
                                (= (:channel event) next-c))
                       (deliver next-ack_ event))
                     (when (= (:kind event) :message)
                       (deliver message_ event)))})]
      (is (= :acknowledged (car/pubsub-subscribe! listener {:channels [first-c]})))
      (is (= (await! operation-results_) [:pending :pending :pending :pending]))
      (is (= (:channel (await! next-ack_)) next-c))
      (with-open [mgr (car/conn-manager-unpooled {})]
        (is (= (wcar mgr (car/publish next-c "ok")) 1)))
      (is (= (:payload (await! message_)) "ok")))))

(deftest _stale-epoch-handler-never-awaits-replacement
  (let [suffix  (str (java.util.UUID/randomUUID))
        first-c (str "carmine:v4:pubsub:stale:first:" suffix)
        next-c  (str "carmine:v4:pubsub:stale:next:" suffix)
        entered_ (promise), release_ (promise), results_ (promise)
        listener
        (car/pubsub-listener
          {:handler-fn
           (fn [event]
             (when (and (= (:kind event) :message)
                        (= (:channel event) first-c))
               (deliver entered_ true)
               @release_
               ;; Recovery has published a new physical reader by this point,
               ;; but calls from the old direct handler still must not wait.
               (deliver results_
                 [(car/pubsub-subscribe! (:listener event) {:channels [next-c]})
                  (car/pubsub-unsubscribe! (:listener event) {:channels [next-c]})
                  (car/pubsub-ping! (:listener event))])))
           :init-subs {:channels [first-c]}
           :ping-ms 10
           :default-timeout-ms 40
           :recovery {:check-ms 10, :backoff-ms 0}})]
    (try
      (with-open [mgr (car/conn-manager-unpooled {})]
        (is (= (wcar mgr (car/publish first-c "block-old-reader")) 1)))
      (is (true? (await! entered_)))
      (await-pred! 5000 "a replacement epoch while the old handler is blocked"
        #(when (pos? (get-in @listener [:recovery :epoch])) true))
      (deliver release_ true)
      (is (= (await! results_) [:pending :pending :pending]))
      (is (= (car/pubsub-await-synced! listener {:timeout-ms 5000}) :synced))
      (is (= (:desired-subs @listener) (:subs @listener)))
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener)))))

(deftest _handler-and-handler-error-are-direct
  (let [handler-call_ (promise), handler-error_ (promise)
        expected (ex-info "Expected direct handler failure" {:secret "hidden"})
        listener
        (car/pubsub-listener
          {:handler-fn
           (fn [event]
             (when (= (:kind event) :pong)
               (deliver handler-call_
                 {:thread (Thread/currentThread), :event event})
               (throw expected)))
           :listener-fn
           #(when (= (:kind %) :handler-error)
              (deliver handler-error_
                {:thread (Thread/currentThread), :event %}))})]
    (try
      (let [pong (car/pubsub-ping! listener)
            handler-call (await! handler-call_)
            handler-error (await! handler-error_)]
        [(is (identical? (:thread handler-call) (:thread handler-error)))
         (is (= (.getName ^Thread (:thread handler-call))
                "carmine-v4-pubsub-reader"))
         (is (identical? (:listener pong) listener))
         (is (identical? (get-in handler-error [:event :cause]) expected))
         (is (= (get-in @listener [:stats :counts :handler-errors]) 1))
         (is (true? (:open? @listener)))])
      (finally (car/pubsub-close! listener)))))

(deftest _synchronous-submission-failure-is-a-handler-error
  (let [handler-error_ (promise)
        expected (java.util.concurrent.RejectedExecutionException. "Expected rejection")
        listener
        (car/pubsub-listener
          {:handler-fn
           (fn [event]
             (when (= (:kind event) :pong) (throw expected)))
           :listener-fn
           #(when (= (:kind %) :handler-error) (deliver handler-error_ %))})]
    (try
      (car/pubsub-ping! listener)
      (let [event (await! handler-error_)]
        [(is (identical? (:cause event) expected))
         (is (not (contains? event :phase)))
         (is (true? (:open? @listener)))])
      (finally (car/pubsub-close! listener)))))

(deftest _default-trove-logging-is-allowlisted
  (let [logs_ (atom [])
        secret "pubsub-secret-payload"
        expected
        (doto (Exception. secret)
          (.setStackTrace
            (into-array StackTraceElement
              [(StackTraceElement. "pkg.Secret" "method" secret 7)])))
        log-fn
        (fn [ns coords level id data_]
          (swap! logs_ conj
            {:ns ns, :coords coords, :level level, :id id, :data (force data_)}))]
    (binding [trove/*log-fn* log-fn]
      (let [listener
            (car/pubsub-listener
              {:handler-fn
               (fn [event]
                 (when (= (:kind event) :pong)
                   (throw expected)))})]
        (try
          (car/pubsub-ping! listener {:message secret})
          (let [{:keys [id data]}
                (await-pred! 2000 "the default handler-error log" #(first @logs_))
                logged-error (:error data)]
            [(is (= id :carmine.pubsub/handler-error))
             (is (= (set (keys (:data data)))
                   #{:kind :epoch :error-class}))
             (is (= (get-in data [:data :kind]) :handler-error))
             (is (= (get-in data [:data :error-class])
                   (.getName ^Class (class expected))))
             (is (identical? logged-error expected)
               "Trove receives the original Throwable unchanged")])
          (is (true? (car/pubsub-close! listener)))
          (is (= (count @logs_) 1)
            "Requested close is silent by default")
          (finally (car/pubsub-close! listener)))))))

(deftest _trove-backend-failures-propagate
  (let [expected (Exception. "Expected logging backend failure")]
    (binding [trove/*log-fn* (fn [& _] (throw expected))]
      (is (identical? expected
            (truss/throws
              (#'pubsub/log-listener-event!
                :carmine.pubsub/handler-error :error
                {:kind :handler-error, :epoch 7} (Exception. "cause"))))))))

(deftest _default-trove-logging-coalesces-connection-close
  (let [logs_ (atom [])
        cause (Exception. "Expected connection failure")]
    (binding [trove/*log-fn*
              (fn [_ns _coords level id data_]
                (swap! logs_ conj
                  {:level level, :id id, :data (force data_)}))]
      (#'pubsub/default-log-listener-event!
        {:kind :conn-error, :epoch 1, :recovering? false, :cause cause})
      (#'pubsub/default-log-listener-event!
        {:kind :closed, :epoch 1, :reason :conn-error, :cause cause})
      (#'pubsub/default-log-listener-event!
        {:kind :closed, :epoch 2, :reason :supervisor-error, :cause cause}))
    [(is (= (mapv :id @logs_)
           [:carmine.pubsub/conn-error :carmine.pubsub/closed]))
     (is (= (mapv :level @logs_) [:error :error]))
     (is (every? #(identical? (get-in % [:data :error]) cause) @logs_))]))

(deftest _supervised-construction-reconciles-handler-intent
  (let [suffix (str (java.util.UUID/randomUUID))
        initial (str "carmine:v4:pubsub:init:" suffix)
        added   (str "carmine:v4:pubsub:added:" suffix)
        armed?_ (atom true), result_ (promise)
        listener
        (car/pubsub-listener
          {:handler-fn
           (fn [event]
             (when (and (= (:kind event) :subscribe)
                        (= (:channel event) initial)
                        (compare-and-set! armed?_ true false))
               (deliver result_
                 {:listener (:listener event)
                  :result
                  (car/pubsub-subscribe! (:listener event) {:channels [added]})})))
           :init-subs {:channels [initial]}
           :recovery {:check-ms 60000, :backoff-ms 0}})]
    (try
      (let [{callback-listener :listener, :keys [result]} (await! result_)]
        [(is (identical? callback-listener listener))
         (is (= result :pending))
         (is (= (car/pubsub-await-synced! listener) :synced))
         (is (= (:subs @listener) {:channels #{initial added}, :patterns #{}}))])
      (finally (car/pubsub-close! listener)))))

(deftest _close-during-supervised-construction-cannot-resurrect-inner
  (let [initial (str "carmine:v4:pubsub:init-close:" (java.util.UUID/randomUUID))
        armed?_ (atom true), close-result_ (promise), events_ (atom [])
        listener
        (car/pubsub-listener
          {:handler-fn
           (fn [event]
             (when (and (= (:kind event) :subscribe)
                        (compare-and-set! armed?_ true false))
               (deliver close-result_
                 (car/pubsub-close! (:listener event)))))
           :listener-fn #(swap! events_ conj %)
           :init-subs {:channels [initial]}
           :recovery {:check-ms 60000, :backoff-ms 0}})]
    [(is (true? (await! close-result_)))
     (is (false? (:open? @listener)))
     (is (nil? (:conn @listener)))
     (is (false? (car/pubsub-close! listener)))
     (is (= (mapv :kind @events_) [:closed]))]))

(deftest _ack-timeout-closes-listener
  (let [close-event_ (promise)]
    (with-redefs [write/write-requests (fn [& _] nil)]
      (let [listener
            (car/pubsub-listener
              {:handler-fn identity
               :default-timeout-ms 20
               :listener-fn
               #(when (= (:kind %) :closed) (deliver close-event_ %))})]
        (let [failure
              (try
                (car/pubsub-subscribe! listener {:channels ["never-acked"]})
                nil
                (catch Throwable t t))]
          [(is (= (:eid (ex-data failure)) :carmine.pubsub/ack-timeout))
           (is (false? (:open? @listener)))
           (is (= (:reason (:close-data @listener)) :conn-error))
           (is (= (:kind (await! close-event_)) :closed)
             "Timeout closure notifies lifecycle callbacks once")])))))

(deftest _close-drains-an-in-flight-operation
  (let [write-called_ (promise)]
    (with-redefs [write/write-requests
                  (fn [& _] (deliver write-called_ true))]
      (let [listener
            (car/pubsub-listener
              {:handler-fn identity, :default-timeout-ms nil})]
        (try
          (let [operation_
                (future
                  (try
                    (car/pubsub-subscribe! listener {:channels ["pending"]})
                    (catch Throwable t t)))]
            (is (true? (await! write-called_)))
            (is (true? (car/pubsub-close! listener)))
            (is (= (:eid (ex-data (deref operation_ 2000 nil)))
                  :carmine.pubsub/listener-closed)))
          (finally (car/pubsub-close! listener)))))))

(deftest _close-does-not-wait-for-a-running-handler
  (let [channel (str "carmine:v4:pubsub:blocking:" (java.util.UUID/randomUUID))
        entered_ (promise), release_ (promise), finished_ (promise)
        listener
        (car/pubsub-listener
          {:handler-fn
           (fn [event]
             (when (= (:kind event) :message)
               (deliver entered_ true)
               @release_
               (deliver finished_ true)))
           :init-subs {:channels [channel]}})]
    (try
      (with-open [mgr (car/conn-manager-unpooled {})]
        (is (= (wcar mgr (car/publish channel "block")) 1)))
      (is (true? (await! entered_)))
      (is (true? (car/pubsub-close! listener)))
      (is (not (realized? finished_)))
      (deliver release_ true)
      (is (true? (await! finished_)))
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener)))))

(deftest _subscription-readiness-api
  (doseq [recovery [nil {:check-ms 60000}]]
    (let [listener
          (car/pubsub-listener
            (cond-> {:handler-fn identity}
              recovery (assoc :recovery recovery)))]
      (try
        [(is (true? (:synced? @listener)))
         (is (= (car/pubsub-await-synced! listener) :synced))
         (is (= (car/pubsub-subscribe! listener {:channels ["readiness"]})
                :acknowledged))
         (is (= (car/pubsub-await-synced! listener {:timeout-ms 0}) :synced))]
        (finally
          (car/pubsub-close! listener)
          (is (throws? :ex-info {:eid :carmine.pubsub/listener-closed}
                (car/pubsub-await-synced! listener))))))))

(deftest _listener-fn-is-synchronous-on-close
  (let [entered_ (promise), release_ (promise), finished_ (promise)
        closing-thread_ (promise)
        listener
        (car/pubsub-listener
          {:handler-fn identity
           :listener-fn
           (fn [event]
             (when (= (:kind event) :closed)
              (deliver entered_ true)
              @release_
              (deliver finished_ (Thread/currentThread))))})]
    (try
      (let [closing_
            (future
              (deliver closing-thread_ (Thread/currentThread))
              (car/pubsub-close! listener))]
        (is (true? (await! entered_)))
        (is (not (realized? closing_))
          "The winning close waits for its synchronous listener function")
        (is (false? (car/pubsub-close! listener))
          "A concurrent second close returns promptly")
        (deliver release_ true)
        (is (true? (deref closing_ 2000 ::timeout)))
        (is (identical? (await! finished_) (await! closing-thread_))))
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener)))))

(deftest _listener-fn-may-close-reentrantly
  (let [calls_ (atom []), listener
        (car/pubsub-listener
          {:handler-fn
           (fn [event]
             (when (= (:kind event) :pong)
               (throw (Exception. "Expected handler failure"))))
           :listener-fn
           (fn [event]
             (swap! calls_ conj (:kind event))
             (when (= (:kind event) :handler-error)
               (is (true? (car/pubsub-close! (:listener event))))))})]
    (try
      (is (= (:kind (car/pubsub-ping! listener)) :pong))
      (await-pred! 2000 "the reentrant close notification"
        #(when (= @calls_ [:handler-error :closed]) true))
      (is (= @calls_ [:handler-error :closed]))
      (is (false? (:open? @listener)))
      (finally (car/pubsub-close! listener)))))

(deftest _blocking-recovery-listener-holds-no-lifecycle-locks
  (let [checks_ (atom 0), entered_ (promise), release_ (promise)
        close-event_ (promise)]
    (with-redefs [pubsub/sentinel-inner-current?
                  (fn [_ _] (> (swap! checks_ inc) 1))]
      (let [listener
            (car/pubsub-listener
              {:handler-fn identity
               :recovery {:check-ms 25, :backoff-ms 0}
               :listener-fn
               (fn [event]
                 (case (:kind event)
                   :recovered
                   (do
                     (deliver entered_ true)
                     (try @release_
                       (catch InterruptedException _ @release_)))
                   :closed    (deliver close-event_ event)
                   nil))})]
        (try
          (is (true? (await! entered_)))
          (is (true? (deref (future (car/pubsub-close! listener)) 1000 ::timeout))
            "A blocking supervisor callback holds no lifecycle/operation lock")
          (is (= (:kind (await! close-event_)) :closed))
          (finally
            (deliver release_ true)
            (car/pubsub-close! listener)))))))

(deftest _write-failure-notifies-outside-operation-lock
  (let [channel (str "carmine:v4:pubsub:write-failure:" (java.util.UUID/randomUUID))
        first-subscribe?_ (atom true), callback-sync_ (promise)
        original-write write/write-requests
        listener
        (car/pubsub-listener
          {:handler-fn identity
           :recovery {:check-ms 25, :backoff-ms 0}
           :listener-fn
           (fn [event]
             (when (= (:kind event) :conn-error)
               ;; This would time out (and an unbounded wait would deadlock) if
               ;; Carmine invoked the function while retaining the ops-lock.
               (deliver callback-sync_
                 (car/pubsub-await-synced! (:listener event)
                   {:timeout-ms 2000}))))})]
    (try
      (with-redefs [write/write-requests
                    (fn [out requests]
                      (if (and (= (ffirst requests) "SUBSCRIBE")
                               (compare-and-set! first-subscribe?_ true false))
                        (throw (java.io.IOException. "Expected write failure"))
                        (original-write out requests)))]
        [(is (= (car/pubsub-subscribe! listener {:channels [channel]}) :pending))
         (is (= (await! callback-sync_) :synced))])
      [(is (true? (:open? @listener)))
       (is (= (:desired-subs @listener) (:subs @listener)))]
      (finally (car/pubsub-close! listener)))))

(deftest _reader-thread-rejection-closes-basic-listener
  (let [result_ (promise), closed_ (promise), armed?_ (atom true)
        original-write write/write-requests
        listener
        (car/pubsub-listener
          {:handler-fn
           (fn [event]
             (when (and (= (:kind event) :pong)
                        (compare-and-set! armed?_ true false))
               (deliver result_
                 (car/pubsub-subscribe! (:listener event)
                   {:channels ["reader-rejected"]}))))
           :listener-fn
           #(when (= (:kind %) :closed) (deliver closed_ %))})]
    (try
      (with-redefs [write/write-requests
                    (fn [out requests]
                      (original-write out
                        (if (= (ffirst requests) "SUBSCRIBE")
                          [["CARMINE_UNKNOWN_COMMAND"]]
                          requests)))]
        (car/pubsub-ping! listener)
        [(is (= (await! result_) :pending))
         (is (= (:reason (await! closed_)) :conn-error))])
      [(is (false? (:open? @listener)))
       (is (false? (:synced? @listener)))]
      (finally (car/pubsub-close! listener)))))

(deftest _reader-thread-definitive-rejection-is-terminal-after-recovery
  (let [result_ (promise), closed_ (promise), armed?_ (atom true)
        original-write write/write-requests
        listener
        (car/pubsub-listener
          {:handler-fn
           (fn [event]
             (when (and (= (:kind event) :pong)
                        (compare-and-set! armed?_ true false))
               (deliver result_
                 (car/pubsub-subscribe! (:listener event)
                   {:channels ["supervised-reader-rejected"]}))))
           :recovery {:check-ms 25, :backoff-ms 0}
           :listener-fn
           #(when (= (:kind %) :closed) (deliver closed_ %))})]
    (try
      (with-redefs [write/write-requests
                    (fn [out requests]
                      (original-write out
                        (if (= (ffirst requests) "SUBSCRIBE")
                          [["CARMINE_UNKNOWN_COMMAND"]]
                          requests)))]
        (car/pubsub-ping! listener)
        [(is (= (await! result_) :pending))
         (is (= (:reason (await! closed_)) :sync-error))])
      [(is (false? (:open? @listener)))
       (is (= (get-in @listener [:close-data :reason]) :sync-error))
       (is (= (get-in @listener [:recovery :last-error :kind]) :sync-error))]
      (finally (car/pubsub-close! listener)))))

(deftest _supervised-timeout-becomes-pending-and-recovers
  (let [channel (str "carmine:v4:pubsub:timeout:" (java.util.UUID/randomUUID))
        message_ (promise)
        listener
        (car/pubsub-listener
          {:handler-fn
           #(when (= (:kind %) :message) (deliver message_ %))
           :default-timeout-ms 25
           :recovery {:check-ms 25, :backoff-ms 0}})]
    (try
      (with-redefs [write/write-requests (fn [& _] nil)]
        [(is (= (car/pubsub-subscribe! listener {:channels [channel]}) :pending))
         (is (false? (:synced? @listener)))
         (is (= (car/pubsub-await-synced! listener {:timeout-ms 0}) :pending))])
      (is (= (car/pubsub-await-synced! listener {:timeout-ms 5000}) :synced))
      (with-open [mgr (car/conn-manager-unpooled {})]
        (is (= (wcar mgr (car/publish channel "after-timeout")) 1)))
      (is (= (:payload (await! message_)) "after-timeout"))
      (finally (car/pubsub-close! listener)))))

(deftest _definitive-subscription-rejection-is-terminal
  (let [listener
        (car/pubsub-listener
          {:handler-fn identity
           :recovery {:check-ms 60000}})
        original-write write/write-requests
        failure
        (try
          (with-redefs [pubsub/recovery-window? (fn [& _] true)
                        write/write-requests
                        (fn [out _]
                          (original-write out [["CARMINE_UNKNOWN_COMMAND"]]))]
            (car/pubsub-subscribe! listener {:channels ["rejected"]}))
          nil
          (catch Throwable t t))]
    [(is (= (:eid (ex-data failure)) :carmine.read/redis-error-reply))
     (is (false? (:open? @listener)))
     (is (= (get-in @listener [:close-data :reason]) :sync-error))
     (is (= (get-in @listener [:recovery :last-error :kind]) :sync-error))
     (is (throws? :ex-info {:eid :carmine.pubsub/listener-closed}
           (car/pubsub-await-synced! listener)))
     (is (false? (car/pubsub-close! listener)))]))

(deftest _transient-subscription-rejection-recovers
  (let [channel (str "carmine:v4:pubsub:transient:" (java.util.UUID/randomUUID))
        listener
        (car/pubsub-listener
          {:handler-fn identity
           :recovery {:check-ms 25, :backoff-ms 0}})
        original-write write/write-requests]
    (try
      (with-redefs [write/write-requests
                    (fn [out _]
                      (original-write out
                        [["EVAL" "return redis.error_reply('LOADING simulated')" 0]]))]
        (is (= (car/pubsub-subscribe! listener {:channels [channel]}) :pending)))
      [(is (true? (:open? @listener)))
       (is (= (car/pubsub-await-synced! listener {:timeout-ms 5000}) :synced))
       (is (= (:desired-subs @listener) (:subs @listener)))]
      (finally (car/pubsub-close! listener)))))

(deftest _ping-is-transport-only-during-reconciliation
  (let [base-channel (str "carmine:v4:pubsub:base:" (java.util.UUID/randomUUID))
        next-channel (str base-channel ":next")
        build-entered_ (promise), release-build_ (promise)
        reconcile-entered_ (promise), release-reconcile_ (promise)
        block-build?_ (atom false)
        original-new-conn conns/new-conn
        original-reconcile @#'pubsub/enqueue-reconcile-subs!]
    (with-redefs-fn
      {#'conns/new-conn
       (fn [& args]
         (when (and @block-build?_
                    (= (.getName (Thread/currentThread))
                       "carmine-v4-pubsub-supervisor"))
           (deliver build-entered_ true)
           @release-build_)
         (apply original-new-conn args))

       #'pubsub/enqueue-reconcile-subs!
       (fn [& args]
         (when @block-build?_
           (deliver reconcile-entered_ true)
           @release-reconcile_)
         (apply original-reconcile args))}
      (fn []
        (let [listener
              (car/pubsub-listener
                {:handler-fn identity
                 :init-subs {:channels [base-channel]}
                 :recovery {:check-ms 25, :backoff-ms 0}})]
          (try
            (with-open [mgr (car/conn-manager {})]
              (reset! block-build?_ true)
              (is (pos? (long (wcar mgr (car/client-kill "TYPE" "PUBSUB")))))
              (is (true? (await! build-entered_)))
              (is (= (car/pubsub-subscribe! listener {:channels [next-channel]})
                     :pending))
              (deliver release-build_ true)
              (is (true? (await! reconcile-entered_)))
              (is (= (:kind (car/pubsub-ping! listener)) :pong)
                "A pong can precede subscription reconciliation")
              (let [synced_ (future
                              (car/pubsub-await-synced! listener
                                {:timeout-ms 2000}))]
                (Thread/sleep 25)
                (is (not (realized? synced_))
                  "Transport liveness must not satisfy the readiness barrier")
                (deliver release-reconcile_ true)
                (is (= (deref synced_ 3000 ::timeout) :synced))))
            (finally
              (reset! block-build?_ false)
              (deliver release-build_ true)
              (deliver release-reconcile_ true)
              (car/pubsub-close! listener))))))))

(deftest _ping-loop-failure-closes-the-epoch
  (let [close-event_ (promise)]
    (with-redefs [pubsub/pubsub-ping!
                  (fn [& _] (throw (Exception. "Expected ping failure")))]
      (let [listener
            (car/pubsub-listener
              {:handler-fn identity
               :ping-ms 10
               :listener-fn
               #(when (= (:kind %) :closed) (deliver close-event_ %))})]
        (try
          (let [state (await-pred! 2000 "ping-loop closure"
                        #(when-not (:open? @listener) @listener))]
            (is (= (:reason (:close-data state)) :conn-error)))
          (is (= (:kind (await! close-event_)) :closed))
          (finally (car/pubsub-close! listener)))))))

(deftest _poll-detected-recovery-restores-subscriptions
  (let [channel (str "carmine:v4:pubsub:poll:" (java.util.UUID/randomUUID))
        checks_ (atom 0), recovered_ (promise), message_ (promise)]
    (with-redefs [pubsub/sentinel-inner-current?
                  (fn [_ _] (> (swap! checks_ inc) 1))]
      (with-open [^java.io.Closeable listener
                  (car/pubsub-listener
                    {:handler-fn
                     #(when (= (:kind %) :message) (deliver message_ %))
                     :init-subs {:channels [channel]}
                     :recovery {:check-ms 25, :backoff-ms 0}
                     :listener-fn
                     #(when (= (:kind %) :recovered) (deliver recovered_ %))})]
        (let [event (await! recovered_)]
          [(is (= (:epoch event) 1))
           (is (= (:desired-subs @listener) (:subs @listener)))])
        (with-open [mgr (car/conn-manager-unpooled {})]
          (is (= (wcar mgr (car/publish channel "after-poll")) 1)))
        (is (= (:payload (await! message_)) "after-poll"))))))

(deftest _closed-recovery-candidate-is-not-reported-recovered
  (let [channel (str "carmine:v4:pubsub:closed-candidate:" (java.util.UUID/randomUUID))
        close-next?_ (atom false), recovered_ (promise), recovery-events_ (atom [])
        original-await @#'pubsub/await-operations!]
    (with-redefs [pubsub/await-operations!
                  (fn [inner expectations timeout-ms]
                    (let [result (original-await inner expectations timeout-ms)]
                      (when (and (= (.getName (Thread/currentThread))
                                      "carmine-v4-pubsub-supervisor")
                                 (compare-and-set! close-next?_ true false))
                        (#'pubsub/close-listener! inner
                          {:reason :conn-error
                           :via 'closed-recovery-candidate-test
                           :cause (Exception. "Expected candidate failure")}))
                      result))]
      (let [listener
            (car/pubsub-listener
              {:handler-fn identity
               :init-subs {:channels [channel]}
               :recovery {:check-ms 25, :backoff-ms 0}
               :listener-fn
               (fn [event]
                 (when (= (:kind event) :recovered)
                   (swap! recovery-events_ conj event)
                   (deliver recovered_ event)))})]
        (try
          (with-open [mgr (car/conn-manager {})]
            (reset! close-next?_ true)
            (is (pos? (long (wcar mgr (car/client-kill "TYPE" "PUBSUB")))))
            (let [event (await! recovered_)]
              [(is (> (:epoch event) 1)
                 "The deliberately closed epoch was not reported as recovered")
               (is (= (count @recovery-events_) 1))
               (is (true? (:open? @listener)))
               (is (= (:desired-subs @listener) (:subs @listener)))]))
          (finally (car/pubsub-close! listener)))))))

(deftest _close-during-rebuild-does-not-publish-an-inner
  (let [channel (str "carmine:v4:pubsub:close-rebuild:" (java.util.UUID/randomUUID))
        block?_ (atom false), build-entered_ (promise), release-build_ (promise)
        recovery-events_ (atom []), close-events_ (atom [])
        original-new-conn conns/new-conn]
    (with-redefs [conns/new-conn
                  (fn [& args]
                    (when @block?_
                      (deliver build-entered_ true)
                      @release-build_)
                    (apply original-new-conn args))]
      (let [listener
            (car/pubsub-listener
              {:handler-fn identity
               :init-subs {:channels [channel]}
               :recovery {:check-ms 25, :backoff-ms 0}
               :listener-fn
               (fn [event]
                 (case (:kind event)
                   :recovered (swap! recovery-events_ conj event)
                   :closed    (swap! close-events_ conj event)
                   nil))})]
        (try
          (with-open [mgr (car/conn-manager {})]
            (is (= (wcar mgr (car/ping)) "PONG"))
            (reset! block?_ true)
            (is (pos? (long (wcar mgr (car/client-kill "TYPE" "PUBSUB")))))
            (is (true? (await! build-entered_)))
            (is (true? (car/pubsub-close! listener)))
            (deliver release-build_ true)
            (await-pred! 2000 "the abandoned inner to close"
              #(when (= (wcar mgr (car/pubsub-numsub channel)) [channel 0]) true)))
          [(is (false? (:open? @listener)))
           (is (nil? (:conn @listener)))
           (is (empty? @recovery-events_))
           (is (= (count @close-events_) 1))]
          (finally
            (reset! block?_ false)
            (deliver release-build_ true)
            (car/pubsub-close! listener)))))))

(deftest _standalone-recovery-restores-intent
  (let [suffix (str (java.util.UUID/randomUUID))
        first-channel (str "carmine:v4:recover:first:" suffix)
        pending-channel (str "carmine:v4:recover:pending:" suffix)
        messages_ (atom [])
        recovered_ (promise)
        recovery-error_ (promise)
        close-events_ (atom [])
        fail-connect?_ (atom false)
        original-new-conn conns/new-conn]
    (with-redefs [conns/new-conn
                  (fn [& args]
                    (if @fail-connect?_
                      (throw (ex-info "Expected recovery build failure" {}))
                      (apply original-new-conn args)))]
      (let [listener
            (car/pubsub-listener
              {:handler-fn
               #(when (= (:kind %) :message)
                  (swap! messages_ conj [(:channel %) (:payload %)]))
               :init-subs {:channels [first-channel]}
               :recovery {:check-ms 25, :backoff-ms 10}
               :listener-fn
               (fn [event]
                 (case (:kind event)
                   :recovered      (deliver recovered_ event)
                   :recovery-error (deliver recovery-error_ event)
                   :closed         (swap! close-events_ conj event)
                   nil))})]
        (try
          (assert-stats-schema! :supervised listener)
          (with-open [mgr (car/conn-manager {})]
            (is (= (wcar mgr (car/publish first-channel "before")) 1))
            (await-pred! 2000 "the initial message"
              #(some #{[first-channel "before"]} @messages_))

            (reset! fail-connect?_ true)
            (is (pos? (long (wcar mgr (car/client-kill "TYPE" "PUBSUB")))))
            (await-pred! 2000 "the recovery gap" #(get-in @listener [:recovery :recovering?]))
            (is (= (car/pubsub-ping! listener) :recovering)
              "No ping is queued during recovery, and no inner error escapes")
            (is (= (car/pubsub-unsubscribe! listener {:channels [first-channel]})
                  :pending))
            (is (= (car/pubsub-unsubscribe! listener) :pending)
              "Unsubscribe-all intent is durable during recovery")
            (is (= (car/pubsub-subscribe! listener {:channels [pending-channel]})
                  :pending))
            (let [event (await! recovery-error_)]
              [(is (= (:kind event) :recovery-error))
               (is (pos-int? (:epoch event)))
               (is (instance? Throwable (:cause event)))])

            (reset! fail-connect?_ false)
            (let [event (await! recovered_)]
              [(is (= (:kind event) :recovered))
               (is (< 0 (:epoch event)))
               (is (vector? (:old-addr event)))
               (is (vector? (:new-addr event)))])
            (await-pred! 2000 "restored desired subscriptions"
              #(= (:subs @listener) (:desired-subs @listener)))

            (is (= (wcar mgr (car/publish first-channel "removed")) 0))
            (is (= (wcar mgr (car/publish pending-channel "after")) 1))
            (await-pred! 2000 "the post-recovery message"
              #(some #{[pending-channel "after"]} @messages_)))

          [(is (= (:desired-subs @listener)
                 {:channels #{pending-channel}, :patterns #{}}))
           (is (pos-int? (get-in @listener [:recovery :epoch])))
           (is (truss/submap? (get-in (car/pubsub-stats listener) [:counts])
                 {:messages 2, :recoveries 1})
             "Supervised counters span all connection epochs exactly once")
           (is (empty? @close-events_)
             "Inner failures do not close the supervised listener")]
          (finally
            (reset! fail-connect?_ false)
            (is (true? (car/pubsub-close! listener)))
            (is (false? (car/pubsub-close! listener)))
            (is (= (count @close-events_) 1))))))))

(deftest _projected-subs
  ;; Pure projection semantics: subs + FIFO deltas of pending expectations
  (let [listener
        (fn [subs expectations]
          (pubsub/->PubSubListener
            nil nil nil nil nil nil 0 nil nil nil nil nil
            (atom subs) (atom expectations) nil nil nil nil nil nil))
        base {:channels #{"c1"}, :patterns #{"p1"}}]
    [(is (= (#'pubsub/projected-subs (listener base [])) base))
     (is (= (#'pubsub/projected-subs
              (listener base
                [{:kind :subscribe,   :pending {"c2" 1}}
                 {:kind :unsubscribe, :pending {"c1" 1}}
                 {:kind :psubscribe,  :pending {"p2" 1}}
                 {:kind :pong,        :pending {}}]))
           {:channels #{"c2"}, :patterns #{"p1" "p2"}})
       "Pending expectations project onto the current subs in FIFO order")
     (is (= (#'pubsub/projected-subs
              (listener base [{:kind :unsubscribe, :pending {nil 1}}]))
           base)
       "Nil names (empty unsubscribe-all acknowledgements) are ignored")
     (is (= (#'pubsub/projected-subs
              (listener {:channels #{"c1" "c2"}, :patterns #{}}
                [{:kind :subscribe, :pending {"c1" 1}}]))
           {:channels #{"c1" "c2"}, :patterns #{}})
       "Replaying an already-applied delta is idempotent")]))

(deftest _unsubscribe-all-under-concurrency
  ;; Regression: unsubscribe-all used to build its expected-ack set from a
  ;; snapshot taken outside the write-lock; concurrent ops could make the
  ;; expectation impossible => ack timeout => healthy listener closed.
  (let [suffix (str (java.util.UUID/randomUUID))
        chan   #(str "carmine:v4:pubsub:racing:" suffix ":" %)
        listener
        (car/pubsub-listener
          {:handler-fn (fn [_]), :default-timeout-ms 4000})]
    (try
      (dotimes [_ 10]
        (let [ops
              (into
                (mapv
                  (fn [idx]
                    (future
                      (car/pubsub-subscribe! listener {:channels [(chan idx)]})
                      (when (odd? idx)
                        (car/pubsub-unsubscribe! listener {:channels [(chan idx)]}))
                      :acknowledged))
                  (range 4))
                [(future (car/pubsub-unsubscribe! listener {}))])]
          (doseq [f ops]
            (let [result (try (deref f 5000 ::timeout) (catch Throwable t t))]
              (is (= result :acknowledged))))))
      (is (:open? @listener) "Listener survives racing unsubscribe-all calls")
      (is (= :acknowledged (car/pubsub-unsubscribe! listener {})))
      (is (= (:kind (car/pubsub-ping! listener)) :pong))
      (finally (car/pubsub-close! listener)))))

(deftest _supervised-ops-are-serialized
  ;; Regression: concurrent supervised sub/unsub could apply to desired state
  ;; in one order and hit the wire in the other, diverging until recovery.
  (let [suffix  (str (java.util.UUID/randomUUID))
        channel (str "carmine:v4:pubsub:supervised-race:" suffix)
        listener
        (car/pubsub-listener
          {:handler-fn (fn [_])
           :recovery {:check-ms 60000}})] ; No poll-driven recovery in-test
    (try
      (dotimes [_ 25]
        (let [add (future (car/pubsub-subscribe! listener {:channels [channel]}))
              del (future (car/pubsub-unsubscribe! listener {:channels [channel]}))]
          @add @del))
      ;; A pong is acknowledged after all prior ops' acknowledgements
      (car/pubsub-ping! listener)
      (await-pred! 4000 "subs to converge to desired state"
        #(= (:subs @listener) (:desired-subs @listener)))
      (is (= (:subs @listener) (:desired-subs @listener))
        "Wire state converges to desired state without any recovery")
      (is (zero? (get-in (car/pubsub-stats listener) [:counts :recoveries])))
      (finally (car/pubsub-close! listener)))))

(deftest _recovery-vs-ops-convergence
  ;; Stress: epoch recoveries racing concurrent supervised sub/unsub churn
  ;; must always converge (inner publication + reconciliation enqueue are one
  ;; atomic step w.r.t. supervised ops via the ops-lock).
  (let [suffix  (str (java.util.UUID/randomUUID))
        channel (str "carmine:v4:recover-race:" suffix)
        listener
        (car/pubsub-listener
          {:handler-fn (fn [_])
           :init-subs {:channels [(str channel ":base")]}
           :recovery {:check-ms 25, :backoff-ms 10}})]
    (try
      (with-open [mgr (car/conn-manager {})]
        (dotimes [_round 6]
          (let [churn
                (mapv
                  (fn [idx]
                    (future
                      (dotimes [_ 5]
                        (car/pubsub-subscribe! listener
                          {:channels [(str channel ":" idx)]})
                        (car/pubsub-unsubscribe! listener
                          {:channels [(str channel ":" idx)]}))
                      true))
                  (range 3))]
            ;; Force an epoch recovery mid-churn
            (wcar mgr (car/client-kill "TYPE" "PUBSUB"))
            (doseq [f churn]
              (let [result (try (deref f 8000 ::timeout) (catch Throwable t t))]
                (is (true? result))))))
        (await-pred! 8000 "recovery to settle and subs to converge"
          #(and (not (get-in @listener [:recovery :recovering?]))
                (= (:subs @listener) (:desired-subs @listener))))
        (is (= (:subs @listener) (:desired-subs @listener))
          "Wire state converges to desired state across recoveries"))
      (finally (car/pubsub-close! listener)))))
