(ns taoensso.carmine-v4.tests.pubsub
  "High-level v4 Pub/Sub tests against a running standalone Redis server."
  (:require
   [clojure.test :refer [deftest testing is]]
   [taoensso.trove :as trove]
   [taoensso.truss :as truss :refer [throws?]]
   [taoensso.carmine-v4 :as car :refer [wcar]]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.pubsub :as pubsub]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.resp.write :as write]
   [taoensso.carmine-v4.tests.test-support :as support]))

(defn- await! [p]
  (let [result (deref p 5000 ::timeout)]
    (is (not= result ::timeout))
    result))

(defn- test-channel [label]
  (str "carmine:v4:pubsub:" label ":" (java.util.UUID/randomUUID)))

(defn- publish! [channel payload]
  (with-open [mgr (car/conn-manager-unpooled {})]
    (wcar mgr (car/publish channel payload))))

(defn- kill-pubsub-conns! []
  (with-open [mgr (car/conn-manager-unpooled {})]
    (wcar mgr (car/client-kill "TYPE" "PUBSUB"))))

(defn- write-instead
  "Returns a `write/write-requests` replacement that writes `reqs` instead of
  the given requests (or of only those matching `pred`), e.g. to have Redis
  reject a listener's command."
  ([reqs] (write-instead (constantly true) reqs))
  ([pred reqs]
   (let [write write/write-requests]
     (fn [out requests] (write out (if (pred requests) reqs requests))))))

(defn- assert-stats-schema! [listener]
  (let [{:keys [since-client-time-ms snapshot-client-time-ms counts] :as stats}
        (car/pubsub-stats listener)]
    [(is (= (set (keys stats))
           #{:since-client-time-ms :snapshot-client-time-ms :counts}))
     (is (pos-int? since-client-time-ms))
     (is (<= since-client-time-ms snapshot-client-time-ms))
     (is (= (set (keys counts))
           #{:events :messages :handler-errors
             :recoveries :recovery-errors}))
     (is (= (dissoc stats :snapshot-client-time-ms)
            (dissoc (:stats @listener) :snapshot-client-time-ms)))]))

(deftest _frame-normalization
  [(is (= (pubsub/frame->event ["message" "c" "v"])
          {:kind :message, :channel "c", :payload "v"}))
   (is (= (pubsub/frame->event ["pmessage" "p*" "pub" "v"])
          {:kind :pmessage, :pattern "p*", :channel "pub", :payload "v"}))
   (is (= (pubsub/frame->event ["unsubscribe" nil 0])
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
             :on-message identity})))))

(deftest _stats-reject-invalid-listener
  (is (throws? :ex-info {:eid :carmine.pubsub/invalid-listener}
        (car/pubsub-stats :invalid))))

(deftest _listener-option-contract
  [(is (= (ex-message (truss/throws (car/pubsub-listener {:on-message identity, :on-mesage identity})))
          "[Carmine] Invalid Pub/Sub listener options: unknown keys [:on-mesage]"))
   (is (= (ex-message (truss/throws (car/pubsub-listener {:on-message :invalid})))
          "[Carmine] Invalid Pub/Sub listener options: `:on-message` should be a fn"))
   (is (throws? :ex-info {:eid :carmine.pubsub/invalid-opts}
         (car/pubsub-listener {:on-event identity})))
   (is (throws? :ex-info {:eid :carmine.pubsub/invalid-opts}
         (car/pubsub-listener {:on-message identity, :on-event :invalid})))
   (is (throws? :ex-info {:eid :carmine.pubsub/invalid-opts}
         (car/pubsub-listener
           {:on-message identity, :handler-dispatch-fn identity})))
   (is
     (throws? :ex-info {:eid :carmine.pubsub/invalid-opts}
       (car/pubsub-listener {:on-message identity, :cbs {}})))])

(deftest _initial-subscriptions-validated-before-connect
  (let [connects_ (atom 0)]
    (with-redefs [conns/new-conn
                  (fn [& _]
                    (swap! connects_ inc)
                    (throw (AssertionError. "Connection should not be opened")))]
      [(is (throws? :ex-info {:eid :carmine.pubsub/invalid-name}
             (car/pubsub-listener
               {:on-message identity, :init-subs {:channels [42]}})))
       (is (throws? :ex-info {:eid :carmine.pubsub/invalid-name}
             (car/pubsub-listener
               {:on-message identity, :init-subs {:channels ["lone-surrogate-\ud800"]}})))
       (is (throws? :ex-info {}
             (car/pubsub-listener
               {:on-message identity
                :init-subs {:channels ["channel"], :timeout-ms 100}}))
         "Initial subscriptions take no timeout of their own")
       (is (zero? @connects_))])))

(deftest _socket-setup-failure-closes-acquired-connection
  (let [failure (java.net.SocketException. "Cannot configure socket")
        conn (atom {:socket (proxy [java.net.Socket] []
                              (setSoTimeout [_] (throw failure)))})
        closed_ (atom [])]
    (with-redefs [conns/new-conn (constantly conn)
                  conns/conn-close! (fn [conn _data] (swap! closed_ conj conn))]
      (is (identical? failure
            (try
              (car/pubsub-listener {:on-message identity})
              (catch Throwable t t))))
      (is (= @closed_ [conn])))))

(deftest _overlapping-acknowledgements-preserve-operation-order
  (let [enqueued_ (promise), completed_ (promise), events_ (atom [])
        listener
        (car/pubsub-listener
          {:on-message (fn [_])
           :on-event
           (fn [{:keys [kind channel listener] :as event}]
             (if (and (= kind :subscribe) (= channel "queue-start"))
               (deliver enqueued_
                 [(car/pubsub-subscribe! listener {:channels ["queue-a" "queue-b"]})
                  (car/pubsub-subscribe! listener {:channels ["queue-c"]})
                  (car/pubsub-unsubscribe! listener {:channels ["queue-a" "queue-b" "queue-c"]})
                  (car/pubsub-ping! listener {:message "queue-done"})])
               (do
                 (swap! events_ conj [kind channel])
                 (when (= kind :pong) (deliver completed_ event)))))})]
    (with-open [_ listener]
      (is (= (car/pubsub-subscribe! listener {:channels ["queue-start"]}) true))
      (is (= (await! enqueued_) [false false false false]))
      (is (= (:payload (await! completed_)) "queue-done"))
      (is (= (subvec @events_ 0 3)
            [[:subscribe "queue-a"] [:subscribe "queue-b"] [:subscribe "queue-c"]]))
      (is (= (set (subvec @events_ 3 6))
            #{[:unsubscribe "queue-a"] [:unsubscribe "queue-b"] [:unsubscribe "queue-c"]}))
      (is (= (peek @events_) [:pong nil]))
      (is (true? (car/pubsub-await-synced! listener {:timeout-ms 0}))))))

(deftest _callback-operations-during-startup-and-recovery-keep-order
  ;; Callbacks for a connection's initial subscriptions can run before
  ;; `pubsub-listener` returns, or before recovery completes. Their operations
  ;; must still be written in order, incl. ones that cancel out, and pings.
  (let [suffix (str (java.util.UUID/randomUUID))
        start  (str "carmine:v4:pubsub:order-start:" suffix)
        [a b c] (mapv #(str "carmine:v4:pubsub:order-" % ":" suffix) ["a" "b" "c"])
        results_ (atom {}), events_ (atom {}), pongs_ (atom {}), recovered_ (promise)
        listener
        (car/pubsub-listener
          ;; The pattern's acknowledgement is still awaited while the callback
          ;; for the channel's acknowledgement runs
          {:init-subs {:channels [start], :patterns [(str start ":*")]}
           :recovery-opts {:check-ms 25, :backoff-ms 0}
           :on-message (fn [_])
           :on-event
           (fn [{:keys [kind channel listener epoch] :as event}]
             (cond
               (and (= kind :subscribe) (= channel start))
               (swap! results_ assoc epoch
                 [(car/pubsub-subscribe!   listener {:channels [a b]})
                  (car/pubsub-subscribe!   listener {:channels [c]})
                  (car/pubsub-unsubscribe! listener {:channels [a b c]})
                  (car/pubsub-ping!        listener {:message "done"})
                  (get-in @listener [:recovery :epoch])
                  (some? (:conn @listener))])

               (#{:subscribe :unsubscribe :pong} kind)
               (do
                 (swap! events_ update epoch (fnil conj []) [kind channel])
                 (when (= kind :pong) (swap! pongs_ assoc epoch event)))

               (= kind :recovered) (deliver recovered_ event)))})
        check-epoch!
        (fn [epoch]
          (support/await-pred! 2000 "the callback's ping to be answered"
            #(get @pongs_ epoch))
          (let [events (get @events_ epoch)]
            [(is (= (get @results_ epoch)
                   [false false false false epoch true])
               "The callback's listener is current, with its connection")
             (is (= (get-in @pongs_ [epoch :payload]) "done"))
             (is (= (subvec events 0 3) [[:subscribe a] [:subscribe b] [:subscribe c]]))
             (is (= (set (subvec events 3 6))
                   #{[:unsubscribe a] [:unsubscribe b] [:unsubscribe c]}))
             (is (= (count events) 7))
             (is (= (peek events) [:pong nil]))]))]
    (with-open [_ listener]
      (check-epoch! 0)
      (is (true? (car/pubsub-await-synced! listener)))
      (is (= (:subs @listener) {:channels #{start}, :patterns #{(str start ":*")}}))
      (is (pos? (long (kill-pubsub-conns!))))
      (check-epoch! (:epoch (await! recovered_)))
      (is (true? (car/pubsub-await-synced! listener)))
      (is (= (:subs @listener) {:channels #{start}, :patterns #{(str start ":*")}})))))

(deftest _dropped-connection-is-reported-as-lost
  (let [error_ (promise)
        listener
        (car/pubsub-listener
          {:on-message identity
           :init-subs  {:channels [(test-channel "conn-lost")]}
           :on-event   #(when (= (:kind %) :conn-error) (deliver error_ %))})]
    (with-open [_ listener]
      (is (true? (car/pubsub-await-synced! listener)))
      (is (pos? (long (kill-pubsub-conns!))))
      (let [cause (:cause (await! error_))]
        [(is (= (:eid (ex-data cause)) :carmine.pubsub/conn-lost))
         (is (vector? (:addr (ex-data cause))))
         (is (instance? java.io.IOException (ex-cause cause)))]))))

(deftest _operation-options-validated-before-effects
  (let [listener
        (car/pubsub-listener {:on-message identity})
        writes_ (atom 0)
        original-write write/write-requests]
    (with-open [_ listener]
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
          (is (throws? :ex-info {:eid :carmine.pubsub/invalid-opts} (operation))))
        (is (throws? :ex-info {:eid :carmine.pubsub/empty-subscribe}
              (car/pubsub-subscribe! listener {}))))
      [(is (zero? @writes_))
       (is (true? (:open? @listener)))
       (is (= (:subs @listener) {:channels #{}, :patterns #{}}))
       (is (= (:desired-subs @listener) {:channels #{}, :patterns #{}}))])))

(deftest _rejects-sentinel-replica-preference
  (let [spec (car/sentinel-spec {:primary [["127.0.0.1" 26379]]})]
    (is (throws? :ex-info {:eid :carmine.pubsub/replica-manager-not-supported}
          (car/pubsub-listener
            {:conn-opts
             {:server
              {:master-name :primary
               :sentinel-spec spec
               :sentinel-opts {:prefer-read-replica? true}}}
             :on-message identity})))))

(deftest _sentinel-master-check-uses-listener-addr
  (let [inner    (atom {:conn {:addr ["127.0.0.1" 6379]}})
        current? (fn [resolved-addr]
                   (#'pubsub/sentinel-inner-current?
                     {:server
                      {:master-name :primary
                       :sentinel-spec
                       (reify sentinel/ISentinelSpec
                         (resolved-addr? [_ _ _ _ addr] (= addr resolved-addr)))}}
                     inner))]
    [(is (true?  (current? ["127.0.0.1" 6379])))
     (is (false? (current? ["127.0.0.1" 6380])))
     (is (true?  (#'pubsub/sentinel-inner-current? {:server ["127.0.0.1" 6379]} inner))
       "Always current without Sentinel")]))

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
            close-cbs_   (atom [])
            conn-errors_ (atom [])
            listener
            (car/pubsub-listener
              {:conn-opts {:init {:resp3? resp3?}}
               :on-message
               (fn [event]
                 (case (:kind event)
                   :message  (deliver message_ event)
                   :pmessage (deliver pmessage_ event)
                   nil))
               :init-subs {:channels [channel], :patterns [pattern]}
               :on-event
               (fn [event]
                 (case (:kind event)
                   :conn-error (swap! conn-errors_ conj event)
                   :closed     (swap! close-cbs_ conj event)
                   nil))})]
        (try
          (is (car/pubsub-listener? listener))
          (assert-stats-schema! listener)
          (is (= (:subs @listener)
                {:channels #{channel}, :patterns #{pattern}}))

          (with-open [mgr (car/conn-manager-unpooled
                            {:conn-opts {:init {:resp3? resp3?}}})]
            (is (= (wcar mgr (car/publish channel {:protocol (if resp3? 3 2)})) 1))
            (is (= (wcar mgr (car/publish pattern-chan "pattern-value")) 1)))

          (is (= (:payload (await! message_)) {:protocol (if resp3? 3 2)}))
          (is (= (:payload (await! pmessage_)) "pattern-value"))
          (is (= (car/pubsub-ping! listener) true))
          (is (= (car/pubsub-ping! listener {:message "probe"}) true))

          (is (= true (car/pubsub-subscribe! listener {:channels [channel]}))
            "Redis re-acknowledges duplicate subscriptions")
          (is (= true (car/pubsub-subscribe! listener {:channels [duplicate-channel]})))
          (is (= true (car/pubsub-unsubscribe! listener {:channels [duplicate-channel]})))
          (is (= true (car/pubsub-unsubscribe! listener {:channels [duplicate-channel]}))
            "Redis re-acknowledges duplicate unsubscriptions")

          (is (= true (car/pubsub-unsubscribe! listener
                       {:channels [channel], :patterns [pattern]})))
          (is (= (:subs @listener) {:channels #{}, :patterns #{}}))
          (is (= true (car/pubsub-unsubscribe! listener {}))
            "Unsubscribe-all handles Redis's nil-name acknowledgements")
          (is (= true (car/pubsub-subscribe! listener
                       {:channels [channel], :patterns [pattern]})))
          (is (= true (car/pubsub-unsubscribe! listener))
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

(deftest _listener-captures-nippy-thaw-opts
  (let [pwd      [:cached "carmine-v4-test-password"]
        data     {:secret [1 2 3]}
        channel  (str "carmine:v4:pubsub-nippy:" (java.util.UUID/randomUUID))
        payloads (java.util.concurrent.LinkedBlockingQueue.)
        ^java.io.Closeable listener
        (car/with-thaw-opts {:password pwd}
          (car/pubsub-listener
            {:on-message
             (fn [event]
               (when (= (:kind event) :message)
                 (.put payloads [(:payload event)])))
             :init-subs {:channels [channel]}
             :recovery-opts {:check-ms 25, :backoff-ms 0}}))
        next!    #(first (.poll payloads 5000 java.util.concurrent.TimeUnit/MILLISECONDS))
        publish-frozen!
        (fn [mgr]
          (car/with-freeze-opts {:password pwd}
            (wcar mgr (car/publish channel data) (car/publish channel "plain"))))]
    (with-open [_ listener]
      (with-open [mgr (car/conn-manager-unpooled {})]
        [(is (= (publish-frozen! mgr) [1 1]))
         (is (= (next!) data) "Captured at construction, after the binding exited")
         (is (= (next!) "plain"))

         (testing "Recovery epochs keep the construction-time opts"
           (is (pos? (long (wcar mgr (car/client-kill "TYPE" "PUBSUB")))))
           (support/await-pred! 5000 "a recovered epoch"
             #(when (pos? (get-in @listener [:recovery :epoch])) true))
           (is (true? (car/pubsub-await-synced! listener {:timeout-ms 5000})))
           [(is (= (publish-frozen! mgr) [1 1]))
            (is (= (next!) data))
            (is (= (next!) "plain"))])]))))

(deftest _listener-is-independent-of-creating-request-context
  (let [channel (test-channel "ctx")
        key     (str channel ":key")
        seen_   (promise)]
    (with-open [mgr (conns/conn-manager-pooled {:pool-opts {:max-total 1, :max-idle 1}})
                handler-mgr (car/conn-manager-unpooled {})]
      (let [client-id (wcar mgr (car/rcmd "CLIENT" "ID"))]
        (try
          (is (= (wcar mgr
                   (car/set key "queued")
                   (with-open [listener
                               (car/pubsub-listener
                                 {:on-message
                                  (fn [event]
                                    (when (= (:kind event) :message)
                                      (deliver seen_
                                        (try
                                          (wcar handler-mgr (car/ping) (car/exists key))
                                          (catch Throwable t t)))))})]
                     (car/pubsub-subscribe! listener {:channels [channel]})
                     ;; Another thread, so as not to flush this `wcar`'s queued SET
                     (doto (Thread. ^Runnable #(publish! channel "x")) (.start))
                     (is (= (await! seen_) ["PONG" 0])
                       "A handler's requests don't send the creating thread's queued requests")))
                "OK"))
          (is (= (wcar mgr (car/rcmd "CLIENT" "ID")) client-id)
            "Subscribing doesn't mark the calling thread's borrowed connection as stateful")
          (finally (wcar mgr (car/del key))))))))

(deftest _handler-driven-subscribe-does-not-deadlock
  (let [suffix  (str (java.util.UUID/randomUUID))
        first-c (str "carmine:v4:pubsub:first:" suffix)
        next-c  (str "carmine:v4:pubsub:next:" suffix)
        armed?_ (atom true)
        operation-results_ (promise)
        next-ack_ (promise)
        message_  (promise)]
    (with-open [listener
                (car/pubsub-listener
                  {:on-message #(deliver message_ %)
                   :on-event
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
                       (deliver next-ack_ event)))})]
      (is (= true (car/pubsub-subscribe! listener {:channels [first-c]})))
      (is (= (await! operation-results_) [false false false false]))
      (is (= (:channel (await! next-ack_)) next-c))
      (is (= (publish! next-c "ok") 1))
      (is (= (:payload (await! message_)) "ok")))))

(deftest _stale-epoch-handler-never-awaits-replacement
  (let [suffix  (str (java.util.UUID/randomUUID))
        first-c (str "carmine:v4:pubsub:stale:first:" suffix)
        next-c  (str "carmine:v4:pubsub:stale:next:" suffix)
        entered_ (promise), release_ (promise), results_ (promise)
        listener
        (car/pubsub-listener
          {:on-message
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
           :default-timeout-ms 40
           :recovery-opts {:check-ms 10, :backoff-ms 0}})]
    (try
      (is (= (publish! first-c "block-old-reader") 1))
      (is (true? (await! entered_)))
      (#'pubsub/close-listener!
        @(.-inner_ ^taoensso.carmine_v4.pubsub.PubSubListener listener)
        {:reason :conn-error, :cause (Exception. "Expected connection failure")})
      (support/await-pred! 5000 "a replacement epoch while the old handler is blocked"
        #(when (pos? (get-in @listener [:recovery :epoch])) true))
      (deliver release_ true)
      (is (= (await! results_) [false false false]))
      (is (true? (car/pubsub-await-synced! listener {:timeout-ms 5000})))
      (is (= (:desired-subs @listener) (:subs @listener)))
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener)))))

(deftest _handler-and-handler-error-are-direct
  (let [channel (test-channel "direct")
        handler-call_ (promise), handler-error_ (promise)
        expected (ex-info "Expected direct handler failure" {:secret "hidden"})
        listener
        (car/pubsub-listener
          {:init-subs {:channels [channel]}
           :on-message
           (fn [event]
             (deliver handler-call_
               {:thread (Thread/currentThread), :event event})
             (throw expected))
           :on-event
           #(when (= (:kind %) :handler-error)
              (deliver handler-error_
                {:thread (Thread/currentThread), :event %}))})]
    (with-open [_ listener]
      (publish! channel "x")
      (let [handler-call (await! handler-call_)
            handler-error (await! handler-error_)]
        [(is (identical? (:thread handler-call) (:thread handler-error)))
         (is (= (.getName ^Thread (:thread handler-call))
                "carmine-v4-pubsub-reader"))
         (is (identical? (get-in handler-call [:event :listener]) listener))
         (is (= (set (keys (:event handler-call)))
               #{:kind :channel :payload :listener :epoch}))
         (is (identical? (get-in handler-error [:event :listener]) listener))
         (is (= (get-in handler-error [:event :epoch]) 0))
         (is (= (get-in handler-error [:event :redis-event :payload]) "x"))
         (is (identical? (get-in handler-error [:event :cause]) expected))
         (is (= (get-in @listener [:stats :counts :handler-errors]) 1))
         (is (true? (:open? @listener)))]))))

(deftest _on-event-error-is-logged-and-ignored
  (let [channel (test-channel "on-event-error")
        expected (Exception. "Expected on-event failure")
        logs_ (atom [])]
    (binding [trove/*log-fn*
              (fn [_ns _coords _level id data_]
                (swap! logs_ conj {:id id, :data (force data_)}))]
      (with-open [listener
                  (car/pubsub-listener
                    {:on-message identity
                     :on-event #(when (= (:kind %) :subscribe) (throw expected))})]
        [(is (= (car/pubsub-subscribe! listener {:channels [channel]}) true))
         (let [{:keys [id data]}
               (support/await-pred! 2000 "the on-event error log" #(first @logs_))]
           [(is (= id :carmine.pubsub/on-event-error))
            (is (= (get-in data [:data :kind]) :subscribe))
            (is (identical? (:error data) expected))])
         (is (true? (:synced? @listener)))
         (is (= (:subs @listener) {:channels #{channel}, :patterns #{}}))]))))

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
      (with-open [listener
                  (car/pubsub-listener
                    {:init-subs {:channels [secret]}
                     :on-message (fn [_] (throw expected))})]
        (publish! secret secret)
        (let [{:keys [id data]}
              (support/await-pred! 2000 "the default handler-error log" #(first @logs_))
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
          "Requested close is silent by default")))))

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
        {:kind :conn-error, :epoch 1, :cause cause})
      (#'pubsub/default-log-listener-event!
        {:kind :closed, :epoch 1, :reason :conn-error, :cause cause})
      (#'pubsub/default-log-listener-event! ; Thrown, so not also logged
        {:kind :closed, :epoch 0, :reason :startup-error, :cause cause})
      (#'pubsub/default-log-listener-event!
        {:kind :closed, :epoch 2, :reason :supervisor-error, :cause cause}))
    [(is (= (mapv :id @logs_)
           [:carmine.pubsub/conn-error :carmine.pubsub/closed]))
     (is (= (mapv :level @logs_) [:warn :error]))
     (is (every? #(identical? (get-in % [:data :error]) cause) @logs_))]))

(deftest _supervised-construction-applies-handler-intent
  (let [suffix (str (java.util.UUID/randomUUID))
        initial (str "carmine:v4:pubsub:init:" suffix)
        added   (str "carmine:v4:pubsub:added:" suffix)
        armed?_ (atom true), result_ (promise)
        listener
        (car/pubsub-listener
          {:on-message (fn [_])
           :on-event
           (fn [event]
             (when (and (= (:kind event) :subscribe)
                        (= (:channel event) initial)
                        (compare-and-set! armed?_ true false))
               (deliver result_
                 {:listener (:listener event)
                  :result
                  (car/pubsub-subscribe! (:listener event) {:channels [added]})})))
           :init-subs {:channels [initial]}
           :recovery-opts {:check-ms 60000, :backoff-ms 0}})]
    (with-open [_ listener]
      (let [{callback-listener :listener, :keys [result]} (await! result_)]
        [(is (identical? callback-listener listener))
         (is (= result false))
         (is (true? (car/pubsub-await-synced! listener)))
         (is (= (:subs @listener) {:channels #{initial added}, :patterns #{}}))]))))

(deftest _close-during-supervised-construction-cannot-resurrect-inner
  ;; Several initial acknowledgements are still awaited when the first one's
  ;; callback closes the listener. Construction returns the closed listener.
  (let [initial (test-channel "init-close")
        armed?_ (atom true), close-result_ (promise), events_ (atom [])
        listener
        (car/pubsub-listener
          {:on-message (fn [_])
           :on-event
           (fn [event]
             (if (= (:kind event) :subscribe)
               (when (compare-and-set! armed?_ true false)
                 (deliver close-result_
                   (car/pubsub-close! (:listener event))))
               (swap! events_ conj event)))
           :init-subs {:channels [initial (str initial ":2")]
                       :patterns [(str initial ":*")]}
           :recovery-opts {:check-ms 60000, :backoff-ms 0}})]
    [(is (true? (await! close-result_)))
     (is (false? (:open? @listener)))
     (is (nil? (:conn @listener)))
     (is (false? (car/pubsub-close! listener)))
     (is (= (mapv :kind @events_) [:closed]))]))

(deftest _startup-failure-throws-even-if-callback-closes
  ;; Closing from a `:conn-error` callback doesn't hide the startup failure
  (let [events_ (atom [])
        result
        (with-redefs [write/write-requests (fn [& _] nil)]
          (try
            (car/pubsub-listener
              {:on-message (fn [_])
               :init-subs {:channels ["carmine:v4:pubsub:never-acked"]}
               :default-timeout-ms 25
               :on-event
               (fn [event]
                 (swap! events_ conj (:kind event))
                 (when (= (:kind event) :conn-error)
                   (car/pubsub-close! (:listener event))))})
            (catch Throwable t t)))]
    [(is (truss/submap? (ex-data result)
           {:eid :carmine.pubsub/ack-timeout, :timeout-ms 25})
       "Initial subscriptions use the listener's default timeout")
     (is (= @events_ [:conn-error :closed]))]))

(deftest _startup-failure-before-first-write-throws
  ;; As above, for a connection that fails before anything is written to it
  (let [expected (Exception. "Expected connection failure")
        original @#'pubsub/new-supervised-inner
        result
        (with-redefs-fn
          {#'pubsub/new-supervised-inner
           (fn [& args]
             (let [inner (apply original args)]
               (#'pubsub/close-listener! inner {:reason :conn-error, :cause expected})
               inner))}
          (fn []
            (try
              (car/pubsub-listener
                {:on-message (fn [_])
                 :on-event
                 (fn [event]
                   (when (= (:kind event) :conn-error)
                     (car/pubsub-close! (:listener event))))})
              (catch Throwable t t))))]
    [(is (= (:eid (ex-data result)) :carmine.pubsub/listener-closed))
     (is (identical? (ex-cause result) expected))]))

(deftest _close-drains-an-in-flight-operation
  (let [write-called_ (promise)]
    (with-redefs [write/write-requests
                  (fn [& _] (deliver write-called_ true))]
      (with-open [listener
                  (car/pubsub-listener
                    {:on-message identity, :default-timeout-ms nil})]
        (let [operation_
              (future
                (try
                  (car/pubsub-subscribe! listener {:channels ["pending"]})
                  (catch Throwable t t)))]
          (is (true? (await! write-called_)))
          (is (true? (car/pubsub-close! listener)))
          (is (= (:eid (ex-data (deref operation_ 2000 nil)))
                :carmine.pubsub/listener-closed)))))))

(deftest _close-does-not-wait-for-a-running-handler
  (let [channel (test-channel "blocking")
        entered_ (promise), release_ (promise), finished_ (promise)
        listener
        (car/pubsub-listener
          {:on-message
           (fn [event]
             (when (= (:kind event) :message)
               (deliver entered_ true)
               @release_
               (deliver finished_ true)))
           :init-subs {:channels [channel]}})]
    (try
      (is (= (publish! channel "block") 1))
      (is (true? (await! entered_)))
      (is (true? (car/pubsub-close! listener)))
      (is (not (realized? finished_)))
      (deliver release_ true)
      (is (true? (await! finished_)))
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener)))))

(deftest _subscription-readiness-api
  (let [listener (car/pubsub-listener {:on-message identity})]
    (try
      [(is (true? (:synced? @listener)))
       (is (true? (car/pubsub-await-synced! listener)))
       (is (= (car/pubsub-subscribe! listener {:channels ["readiness"]})
              true))
       (is (true? (car/pubsub-await-synced! listener {:timeout-ms 0})))]
      (finally
        (car/pubsub-close! listener)
        (is (throws? :ex-info {:eid :carmine.pubsub/listener-closed}
              (car/pubsub-await-synced! listener)))))))

(deftest _on-event-is-synchronous-on-close
  (let [entered_ (promise), release_ (promise), finished_ (promise)
        closing-thread_ (promise)
        listener
        (car/pubsub-listener
          {:on-message identity
           :on-event
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

(deftest _on-event-may-close-reentrantly
  (let [channel (test-channel "reentrant")
        calls_ (atom []), listener
        (car/pubsub-listener
          {:init-subs {:channels [channel]}
           :on-message
           (fn [_] (throw (Exception. "Expected handler failure")))
           :on-event
           (fn [event]
             (when (#{:handler-error :closed} (:kind event))
               (swap! calls_ conj (:kind event)))
             (when (= (:kind event) :handler-error)
               (is (true? (car/pubsub-close! (:listener event))))))})]
    (with-open [_ listener]
      (publish! channel "x")
      (support/await-pred! 2000 "the reentrant close notification"
        #(when (= @calls_ [:handler-error :closed]) true))
      (is (= @calls_ [:handler-error :closed]))
      (is (false? (:open? @listener))))))

(deftest _blocking-recovery-listener-holds-no-lifecycle-locks
  (let [checks_ (atom 0), entered_ (promise), release_ (promise)
        close-event_ (promise)]
    (with-redefs [pubsub/sentinel-inner-current?
                  (fn [_ _] (> (swap! checks_ inc) 1))]
      (let [listener
            (car/pubsub-listener
              {:on-message identity
               :recovery-opts {:check-ms 25, :backoff-ms 0}
               :on-event
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
  (let [channel (test-channel "write-failure")
        first-subscribe?_ (atom true), callback-sync_ (promise)
        original-write write/write-requests
        listener
        (car/pubsub-listener
          {:on-message identity
           :recovery-opts {:check-ms 25, :backoff-ms 0}
           :on-event
           (fn [event]
             (when (= (:kind event) :conn-error)
               ;; This would time out (and an unbounded wait would deadlock) if
               ;; Carmine invoked the function while retaining the ops-lock.
               (deliver callback-sync_
                 (car/pubsub-await-synced! (:listener event)
                   {:timeout-ms 2000}))))})]
    (with-open [_ listener]
      (with-redefs [write/write-requests
                    (fn [out requests]
                      (if (and (= (ffirst requests) "SUBSCRIBE")
                               (compare-and-set! first-subscribe?_ true false))
                        (throw (java.io.IOException. "Expected write failure"))
                        (original-write out requests)))]
        [(is (= (car/pubsub-subscribe! listener {:channels [channel]}) false))
         (is (true? (await! callback-sync_)))])
      [(is (true? (:open? @listener)))
       (is (= (:desired-subs @listener) (:subs @listener)))])))

(deftest _reader-thread-definitive-rejection-is-terminal
  (let [result_ (promise), closed_ (promise), armed?_ (atom true)
        listener
        (car/pubsub-listener
          {:on-message (fn [_])
           :recovery-opts {:check-ms 25, :backoff-ms 0}
           :on-event
           (fn [event]
             (when (= (:kind event) :closed) (deliver closed_ event))
             (when (and (= (:kind event) :pong)
                        (compare-and-set! armed?_ true false))
               (deliver result_
                 (car/pubsub-subscribe! (:listener event)
                   {:channels ["supervised-reader-rejected"]}))))})]
    (with-open [_ listener]
      (with-redefs [write/write-requests
                    (write-instead #(= (ffirst %) "SUBSCRIBE") [["CARMINE_UNKNOWN_COMMAND"]])]
        (car/pubsub-ping! listener)
        [(is (= (await! result_) false))
         (is (= (:reason (await! closed_)) :sync-error))])
      [(is (false? (:open? @listener)))
       (is (= (get-in @listener [:close-data :reason]) :sync-error))
       (is (= (get-in @listener [:recovery :last-error :kind]) :sync-error))])))

(deftest _unconfirmed-change-is-applied-after-stalled-connection-is-replaced
  ;; A timeout doesn't replace the connection, but its silence then does
  (let [channel (test-channel "timeout")
        message_ (promise)
        listener
        (car/pubsub-listener
          {:on-message
           #(when (= (:kind %) :message) (deliver message_ %))
           :ping-ms 25
           :default-timeout-ms 200
           :recovery-opts {:check-ms 25, :backoff-ms 0}})]
    (with-open [_ listener]
      (with-redefs [write/write-requests (fn [& _] nil)]
        [(is (false? (car/pubsub-subscribe! listener {:channels [channel], :timeout-ms 25})))
         (is (false? (:synced? @listener)))
         (is (false? (car/pubsub-await-synced! listener {:timeout-ms 0})))
         (support/await-pred! 5000 "the silent connection to be replaced"
           #(pos? (get-in @listener [:recovery :epoch])))])
      (is (true? (car/pubsub-await-synced! listener {:timeout-ms 5000})))
      (is (= (publish! channel "after-timeout") 1))
      (is (= (:payload (await! message_)) "after-timeout")))))

(deftest _definitive-subscription-rejection-is-terminal
  (let [listener
        (car/pubsub-listener
          {:on-message identity
           :recovery-opts {:check-ms 60000}})
        failure
        (try
          (with-redefs [write/write-requests (write-instead [["CARMINE_UNKNOWN_COMMAND"]])]
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
  (doseq [code ["BUSY" "OOM"]]
  (let [channel (test-channel "transient")
        listener
        (car/pubsub-listener
          {:on-message identity
           :recovery-opts {:check-ms 25, :backoff-ms 0}})]
    (with-open [_ listener]
      (with-redefs [write/write-requests
                    (write-instead [["EVAL" (str "return redis.error_reply('" code " simulated')") 0]])]
        (is (= (car/pubsub-subscribe! listener {:channels [channel]}) false)))
      [(is (true? (:open? @listener)))
       (is (true? (car/pubsub-await-synced! listener {:timeout-ms 5000})))
       (is (= (:desired-subs @listener) (:subs @listener)))]))))

(deftest _slow-handler-keeps-the-connection
  ;; A slow handler delays acknowledgements and keepalive replies on a
  ;; healthy connection, here for longer than the 220ms of silence allowed
  (let [channel (test-channel "slow")
        next-c  (str channel ":next")
        entered_ (promise), release_ (promise)
        listener
        (car/pubsub-listener
          {:on-message (fn [_] (deliver entered_ true) @release_)
           :init-subs  {:channels [channel]}
           :ping-ms 20
           :default-timeout-ms 200})]
    (try
      (is (= (publish! channel "block-handler") 1))
      (is (true? (await! entered_)))
      [(is (false? (car/pubsub-subscribe! listener {:channels [next-c], :timeout-ms 25})))
       (is (false? (car/pubsub-subscribe! listener {:channels [next-c], :timeout-ms 0}))
         "Doesn't wait")
       (is (false? (car/pubsub-ping! listener {:timeout-ms 25})))]
      (Thread/sleep 500)
      (deliver release_ true)
      [(is (true? (car/pubsub-await-synced! listener {:timeout-ms 5000})))
       (is (= (:subs @listener) {:channels #{channel next-c}, :patterns #{}})
         "Unconfirmed changes were still applied")
       (is (true? (car/pubsub-ping! listener)))
       (is (zero? (get-in @listener [:recovery :epoch])) "No reconnect")]
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener)))))

(deftest _late-rejection-closes-the-listener
  ;; Redis rejects a change after its caller has stopped waiting for it
  (let [channel (test-channel "late")
        next-c  (str channel ":next")
        entered_ (promise), release_ (promise), closed_ (promise)
        listener
        (car/pubsub-listener
          {:on-message (fn [_] (deliver entered_ true) @release_)
           :on-event   #(when (= (:kind %) :closed) (deliver closed_ %))
           :init-subs  {:channels [channel]}})]
    (try
      (is (= (publish! channel "block-handler") 1))
      (is (true? (await! entered_)))
      (with-redefs [write/write-requests (write-instead [["CARMINE_UNKNOWN_COMMAND"]])]
        (is (false? (car/pubsub-subscribe! listener {:channels [next-c], :timeout-ms 25}))))
      (deliver release_ true)
      (let [closed (await! closed_)]
        [(is (= (:reason closed) :sync-error))
         (is (= (:eid (ex-data (:cause closed))) :carmine.read/redis-error-reply)
           "The rejection isn't lost")
         (is (false? (:open? @listener)))
         (is (throws? :ex-info {:eid :carmine.pubsub/listener-closed}
               (car/pubsub-subscribe! listener {:channels [next-c]})))])
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener)))))

(deftest _unexpected-acknowledgement-replaces-the-connection
  ;; A reply that the oldest pending operation isn't waiting for means that
  ;; the listener's view of its connection is wrong
  (let [channel (test-channel "unexpected")
        conn-error_ (promise)
        listener
        (car/pubsub-listener
          {:on-message identity
           :on-event   #(when (= (:kind %) :conn-error) (deliver conn-error_ %))
           :recovery-opts {:backoff-ms 0}})]
    (with-open [_ listener]
      (with-redefs [write/write-requests (fn [& _] nil)] ; Never sent
        (is (false? (car/pubsub-subscribe! listener {:channels [channel], :timeout-ms 0}))))
      (is (false? (car/pubsub-ping! listener {:timeout-ms 0})))
      [(is (= (:eid (ex-data (:cause (await! conn-error_))))
              :carmine.pubsub/unexpected-ack))
       (is (true? (car/pubsub-await-synced! listener {:timeout-ms 5000})))
       (is (= (:subs @listener) {:channels #{channel}, :patterns #{}}))])))

(deftest _unrequested-reply-replaces-the-connection
  (let [conn-error_ (promise)
        listener
        (car/pubsub-listener
          {:on-message identity
           :on-event   #(when (= (:kind %) :conn-error) (deliver conn-error_ %))
           :recovery-opts {:backoff-ms 0}})
        ^taoensso.carmine_v4.pubsub.PubSubConn inner
        @(.-inner_ ^taoensso.carmine_v4.pubsub.PubSubListener listener)]
    (with-open [_ listener]
      ;; A reply that no operation is waiting for
      (locking (.-write-lock inner)
        (write/write-requests (.-out inner) [["PING"]]))
      [(is (= (:eid (ex-data (:cause (await! conn-error_))))
              :carmine.pubsub/unexpected-ack))
       (is (true? (car/pubsub-await-synced! listener {:timeout-ms 5000})))
       (is (true? (car/pubsub-ping! listener)))])))

(deftest _names-that-redis-would-change-are-rejected
  (let [channel  (test-channel "names")
        listener (car/pubsub-listener
                   {:on-message identity, :init-subs {:channels [channel]}})]
    (with-open [_ listener]
      [(is (throws? :ex-info {:eid :carmine.pubsub/invalid-name}
             (car/pubsub-subscribe! listener {:channels ["lone-surrogate-\ud800"]})))
       (is (throws? :ex-info {:eid :carmine.pubsub/invalid-name}
             (car/pubsub-subscribe! listener {:patterns [(str (char 0) "null-prefix")]})))
       (is (throws? :ex-info {:eid :carmine.pubsub/invalid-name}
             (car/pubsub-ping! listener {:message (str (char 0) "null-prefix")})))
       (is (true? (car/pubsub-subscribe! listener {:channels ["näme-✓-😀"]}))
         "Other Unicode is fine")
       (is (true? (car/pubsub-unsubscribe! listener {:channels ["näme-✓-😀"]})))
       (is (true? (car/pubsub-subscribe!   listener {:channels [:my.ns/kw-name]})))
       (is (true? (car/pubsub-unsubscribe! listener {:channels ["my.ns/kw-name"]})))
       (is (= (:desired-subs @listener) {:channels #{channel}, :patterns #{}})
         "Rejected before anything changed")
       (is (zero? (get-in @listener [:recovery :epoch])) "No reconnect")])))

(deftest _slow-handler-does-not-fail-recovery
  ;; While resubscribing, the reader is held between the two commands'
  ;; acknowledgements for longer than `:default-timeout-ms`
  (let [channel (test-channel "slow-recovery")
        recovered_ (promise)
        listener
        (car/pubsub-listener
          {:on-message identity
           :init-subs  {:channels [channel], :patterns [(str channel ":*")]}
           :default-timeout-ms 100
           :recovery-opts {:backoff-ms 0}
           :on-event
           (fn [{:keys [kind epoch] :as event}]
             (case kind
               :subscribe (when (pos? (long epoch)) (Thread/sleep 400))
               :recovered (deliver recovered_ event)
               nil))})]
    (with-open [_ listener]
      (is (pos? (long (kill-pubsub-conns!))))
      [(is (= (:epoch (await! recovered_)) 1) "The first new connection is kept")
       (is (zero? (get-in (car/pubsub-stats listener) [:counts :recovery-errors])))
       (is (true? (car/pubsub-await-synced! listener)))])))

(deftest _master-change-is-noticed-while-resubscribing
  ;; A callback holds the first new connection's second acknowledgement,
  ;; and the Sentinel master changes in the meantime
  (let [channel (test-channel "master-change")
        entered_ (promise), release_ (promise), recovered_ (promise)
        errors_ (atom [])
        listener
        (car/pubsub-listener
          {:on-message identity
           :init-subs  {:channels [channel], :patterns [(str channel ":*")]}
           :recovery-opts {:check-ms 25, :backoff-ms 0}
           :on-event
           (fn [{:keys [kind epoch] :as event}]
             (case kind
               :subscribe (when (= epoch 1) (deliver entered_ true) @release_)
               :recovered (deliver recovered_ event)
               :recovery-error (swap! errors_ conj [epoch (:eid (ex-data (:cause event)))])
               nil))})]
    (try
      (with-redefs [pubsub/sentinel-inner-current?
                    (fn [_ inner]
                      (not (and (realized? entered_)
                                (= (.-epoch ^taoensso.carmine_v4.pubsub.PubSubConn inner) 1))))]
        (is (pos? (long (kill-pubsub-conns!))))
        [(is (= (:epoch (await! recovered_)) 2) "Replaced while still held")
         (is (= @errors_ [[1 :carmine.pubsub/master-changed]]))
         (is (true? (car/pubsub-await-synced! listener)))])
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener)))))

(deftest _rejected-resubscribe-closes-the-listener
  (let [channel (test-channel "rejected-resub")
        closed_ (promise), recovered?_ (atom false)
        listener
        (car/pubsub-listener
          {:on-message identity
           :init-subs  {:channels [channel]}
           :recovery-opts {:check-ms 25, :backoff-ms 0}
           :on-event
           (fn [{:keys [kind] :as event}]
             (case kind
               :closed    (deliver closed_ event)
               :recovered (reset! recovered?_ true)
               nil))})
        ^Thread supervisor
        @(.-supervisor-thread_ ^taoensso.carmine_v4.pubsub.PubSubListener listener)]
    (with-open [_ listener]
      (with-redefs [write/write-requests
                    (write-instead #(= (ffirst %) "SUBSCRIBE") [["CARMINE_UNKNOWN_COMMAND"]])]
        (is (pos? (long (kill-pubsub-conns!))))
        (let [closed (await! closed_)]
          [(is (= (:reason closed) :sync-error))
           (is (= (:eid (ex-data (:cause closed))) :carmine.read/redis-error-reply))
           (is (false? @recovered?_))
           (is (do (.join supervisor 2000) (not (.isAlive supervisor))))])))))

(deftest _late-transient-rejection-replaces-the-connection
  ;; Redis rejects a change, for now, after its caller has stopped waiting.
  ;; Needs RESP3: a subscribed RESP2 connection can't run the simulating EVAL.
  (when (some true? (support/supported-resp3-options))
  (let [channel (test-channel "late-busy")
        next-c  (str channel ":next")
        entered_ (promise), release_ (promise)
        listener
        (car/pubsub-listener
          {:conn-opts  {:init {:resp3? true}}
           :on-message (fn [_] (deliver entered_ true) @release_)
           :init-subs  {:channels [channel]}
           :recovery-opts {:check-ms 25, :backoff-ms 0}})]
    (try
      (is (= (publish! channel "block-handler") 1))
      (is (true? (await! entered_)))
      (with-redefs [write/write-requests
                    (write-instead [["EVAL" "return redis.error_reply('BUSY simulated')" 0]])]
        (is (false? (car/pubsub-subscribe! listener {:channels [next-c], :timeout-ms 25}))))
      (deliver release_ true)
      [(support/await-pred! 5000 "the connection to be replaced"
         #(pos? (get-in @listener [:recovery :epoch])))
       (is (true? (car/pubsub-await-synced! listener {:timeout-ms 5000})))
       (is (= (:subs @listener) {:channels #{channel next-c}, :patterns #{}})
         "The change is applied on the next connection")]
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener))))))

(deftest _ping-is-transport-only-during-resubscription
  (let [base-channel (test-channel "base")
        next-channel (str base-channel ":next")
        build-entered_ (promise), release-build_ (promise)
        resubscribe-entered_ (promise), release-resubscribe_ (promise)
        block-build?_ (atom false)
        original-new-conn conns/new-conn
        original-enqueue @#'pubsub/enqueue-desired-subs!]
    (with-redefs-fn
      {#'conns/new-conn
       (fn [& args]
         (when (and @block-build?_
                    (= (.getName (Thread/currentThread))
                       "carmine-v4-pubsub-supervisor"))
           (deliver build-entered_ true)
           @release-build_)
         (apply original-new-conn args))

       #'pubsub/enqueue-desired-subs!
       (fn [& args]
         (when @block-build?_
           (deliver resubscribe-entered_ true)
           @release-resubscribe_)
         (apply original-enqueue args))}
      (fn []
        (let [listener
              (car/pubsub-listener
                {:on-message identity
                 :init-subs {:channels [base-channel]}
                 :recovery-opts {:check-ms 25, :backoff-ms 0}})]
          (try
            (with-open [mgr (car/conn-manager {})]
              (reset! block-build?_ true)
              (is (pos? (long (wcar mgr (car/client-kill "TYPE" "PUBSUB")))))
              (is (true? (await! build-entered_)))
              (is (= (car/pubsub-subscribe! listener {:channels [next-channel]})
                     false))
              (deliver release-build_ true)
              (is (true? (await! resubscribe-entered_)))
              (is (= (car/pubsub-ping! listener) true)
                "A pong can precede resubscription")
              (is (false? (car/pubsub-await-synced! listener {:timeout-ms 25}))
                "Transport liveness must not satisfy the readiness barrier")
              (is (true? (get-in @listener [:recovery :recovering?]))
                "Still recovering until resubscribed")
              (deliver release-resubscribe_ true)
              (is (true? (car/pubsub-await-synced! listener {:timeout-ms 3000})))
              (support/await-pred! 2000 "recovery to complete"
                #(not (get-in @listener [:recovery :recovering?]))))
            (finally
              (reset! block-build?_ false)
              (deliver release-build_ true)
              (deliver release-resubscribe_ true)
              (car/pubsub-close! listener))))))))

(deftest _keepalive-pings
  (testing "Keepalive replies are hidden"
    (let [events_ (atom []), n-keepalives_ (atom 0)
          original-ping @#'pubsub/send-keepalive!]
      (with-redefs [pubsub/send-keepalive!
                    (fn [inner] (swap! n-keepalives_ inc) (original-ping inner))]
        (with-open [listener
                    (car/pubsub-listener
                      {:on-message identity
                       :ping-ms 10
                       :on-event #(swap! events_ conj (:kind %))})]
          (support/await-pred! 5000 "several keepalive pings"
            #(>= @n-keepalives_ 5))
          (is (true? (car/pubsub-ping! listener)))
          (support/await-pred! 5000 "the requested ping's reply event"
            #(seq @events_))
          [(is (= @events_ [:pong]) "Only the requested ping's reply is an event")
           (is (= (get-in @listener [:stats :counts :events]) 1))
           (is (zero? (get-in @listener [:recovery :epoch])) "No reconnect")]))))

  (testing "A rejected keepalive still shows that the connection is alive"
    (let [original-write write/write-requests
          n-rejected_ (atom 0)]
      (with-redefs [write/write-requests
                    (fn [out reqs]
                      (if (= (.getName (Thread/currentThread)) "carmine-v4-pubsub-ping")
                        (do
                          (original-write out [["CARMINE_UNKNOWN_COMMAND"]])
                          (swap! n-rejected_ inc))
                        (original-write out reqs)))]
        (with-open [listener
                    (car/pubsub-listener {:on-message identity, :ping-ms 10})]
          (support/await-pred! 5000 "several rejected keepalive pings"
            #(>= @n-rejected_ 5))
          [(is (true? (car/pubsub-ping! listener)))
           (is (zero? (get-in @listener [:recovery :epoch])) "No reconnect")]))))

  (testing "A connection that stays silent is replaced"
    (let [original-ping @#'pubsub/send-keepalive!
          conn-error_ (promise), recovered_ (promise)]
      ;; Pings on the first connection are never sent, as if it had stalled
      (with-redefs [pubsub/send-keepalive!
                    (fn [^taoensso.carmine_v4.pubsub.PubSubConn inner]
                      (when (pos? (.-epoch inner)) (original-ping inner)))]
        (with-open [listener
                    (car/pubsub-listener
                      {:on-message identity
                       :ping-ms 20
                       :default-timeout-ms 100
                       :recovery-opts {:backoff-ms 0}
                       :on-event
                       (fn [event]
                         (case (:kind event)
                           :conn-error (deliver conn-error_ event)
                           :recovered  (deliver recovered_  event)
                           nil))})]
          [(let [cause (:cause (await! conn-error_))]
             [(is (= (select-keys (ex-data cause) [:eid :read-timeout-ms])
                     {:eid :carmine.pubsub/stalled, :read-timeout-ms 120}))
              (is (instance? java.net.SocketTimeoutException (ex-cause cause)))])
           (is (= (:epoch (await! recovered_)) 1))])))))

(deftest _ping-loop-failure-closes-the-epoch
  (let [expected (Exception. "Expected ping failure")
        conn-error_ (promise), recovered_ (promise), recovered-ping_ (promise)
        armed?_ (atom true), original-ping @#'pubsub/send-keepalive!]
    (with-redefs [pubsub/send-keepalive!
                  (fn [^taoensso.carmine_v4.pubsub.PubSubConn inner]
                    (if (compare-and-set! armed?_ true false)
                      (throw expected)
                      (do
                        (when (= (.-epoch inner) 1) (deliver recovered-ping_ true))
                        (original-ping inner))))]
      (with-open [listener
                  (car/pubsub-listener
                    {:on-message identity
                     :ping-ms 10
                     :recovery-opts {:backoff-ms 0}
                     :on-event
                     (fn [event]
                       (case (:kind event)
                         :conn-error (deliver conn-error_ event)
                         :recovered  (deliver recovered_ event)
                         nil))})]
        [(is (identical? (:cause (await! conn-error_)) expected))
         (is (= (:epoch (await! recovered_)) 1))
         (is (true? (await! recovered-ping_))
           "The recovered connection is pinged too")]))))

(deftest _poll-detected-recovery-restores-subscriptions
  (let [channel (test-channel "poll")
        checks_ (atom 0), recovered_ (promise), message_ (promise)]
    (with-redefs [pubsub/sentinel-inner-current?
                  (fn [_ _] (> (swap! checks_ inc) 1))]
      (with-open [listener
                  (car/pubsub-listener
                    {:on-message
                     #(when (= (:kind %) :message) (deliver message_ %))
                     :init-subs {:channels [channel]}
                     :recovery-opts {:check-ms 25, :backoff-ms 0}
                     :on-event
                     #(when (= (:kind %) :recovered) (deliver recovered_ %))})]
        (let [event (await! recovered_)]
          [(is (= (:epoch event) 1))
           (is (= (:desired-subs @listener) (:subs @listener)))])
        (is (= (publish! channel "after-poll") 1))
        (is (= (:payload (await! message_)) "after-poll"))))))

(deftest _closed-recovery-candidate-is-not-reported-recovered
  (let [channel (test-channel "closed-candidate")
        close-next?_ (atom false), recovered_ (promise), recovery-events_ (atom [])
        original-await @#'pubsub/await-operations!]
    (with-redefs [pubsub/await-operations!
                  (fn [inner expectations timeout-ms close-on-timeout?]
                    (let [result (original-await inner expectations timeout-ms
                                   close-on-timeout?)]
                      (when (and (= (.getName (Thread/currentThread))
                                      "carmine-v4-pubsub-supervisor")
                                 (compare-and-set! close-next?_ true false))
                        (#'pubsub/close-listener! inner
                          {:reason :conn-error
                           :via 'closed-recovery-candidate-test
                           :cause (Exception. "Expected candidate failure")}))
                      result))]
      (with-open [listener
                  (car/pubsub-listener
                    {:on-message identity
                     :init-subs {:channels [channel]}
                     :recovery-opts {:check-ms 25, :backoff-ms 0}
                     :on-event
                     (fn [event]
                       (when (= (:kind event) :recovered)
                         (swap! recovery-events_ conj event)
                         (deliver recovered_ event)))})]
        (with-open [mgr (car/conn-manager {})]
          (reset! close-next?_ true)
          (is (pos? (long (wcar mgr (car/client-kill "TYPE" "PUBSUB")))))
          (let [event (await! recovered_)]
            [(is (> (:epoch event) 1)
               "The deliberately closed epoch was not reported as recovered")
             (is (= (count @recovery-events_) 1))
             (is (true? (:open? @listener)))
             (is (= (:desired-subs @listener) (:subs @listener)))]))))))

(deftest _close-during-rebuild-does-not-publish-an-inner
  (let [channel (test-channel "close-rebuild")
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
              {:on-message identity
               :init-subs {:channels [channel]}
               :recovery-opts {:check-ms 25, :backoff-ms 0}
               :on-event
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
            (support/await-pred! 2000 "the abandoned inner to close"
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
              {:on-message
               #(when (= (:kind %) :message)
                  (swap! messages_ conj [(:channel %) (:payload %)]))
               :init-subs {:channels [first-channel]}
               :recovery-opts {:check-ms 25, :backoff-ms 10}
               :on-event
               (fn [event]
                 (case (:kind event)
                   :recovered      (deliver recovered_ event)
                   :recovery-error (deliver recovery-error_ event)
                   :closed         (swap! close-events_ conj event)
                   nil))})]
        (try
          (assert-stats-schema! listener)
          (with-open [mgr (car/conn-manager {})]
            (is (= (wcar mgr (car/publish first-channel "before")) 1))
            (support/await-pred! 2000 "the initial message"
              #(some #{[first-channel "before"]} @messages_))

            (reset! fail-connect?_ true)
            (is (pos? (long (wcar mgr (car/client-kill "TYPE" "PUBSUB")))))
            (support/await-pred! 2000 "the recovery gap" #(get-in @listener [:recovery :recovering?]))
            (is (= (car/pubsub-ping! listener) false)
              "No ping is queued during recovery, and no inner error escapes")
            (is (= (car/pubsub-unsubscribe! listener {:channels [first-channel]})
                  false))
            (is (= (car/pubsub-unsubscribe! listener) false)
              "Unsubscribe-all intent is durable during recovery")
            (is (= (car/pubsub-subscribe! listener {:channels [pending-channel]})
                  false))
            (let [event (await! recovery-error_)]
              [(is (= (:kind event) :recovery-error))
               (is (pos-int? (:epoch event)))
               (is (instance? Throwable (:cause event)))
               (is (not= (:eid (ex-data (:cause event))) :carmine.pubsub/listener-closed)
                 "Reports the underlying cause")])

            (reset! fail-connect?_ false)
            (let [event (await! recovered_)]
              [(is (= (:kind event) :recovered))
               (is (< 0 (:epoch event)))
               (is (vector? (:old-addr event)))
               (is (vector? (:new-addr event)))])
            (support/await-pred! 2000 "restored desired subscriptions"
              #(= (:subs @listener) (:desired-subs @listener)))

            (is (= (wcar mgr (car/publish first-channel "removed")) 0))
            (is (= (wcar mgr (car/publish pending-channel "after")) 1))
            (support/await-pred! 2000 "the post-recovery message"
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

(deftest _repeated-names-are-each-acknowledged
  (let [channel  (test-channel "repeated")
        listener (car/pubsub-listener {:on-message identity})]
    (with-open [_ listener]
      [(is (true? (car/pubsub-subscribe!   listener {:channels [channel channel]})))
       (is (true? (car/pubsub-unsubscribe! listener {:channels [channel "not-subscribed"]})))
       (is (true? (car/pubsub-subscribe!   listener {:channels [channel channel], :patterns [channel]})))
       (is (true? (car/pubsub-unsubscribe! listener)))
       (is (true? (car/pubsub-unsubscribe! listener)) "With nothing left")
       (is (true? (car/pubsub-ping! listener)))
       (is (true? (:synced? @listener)))
       (is (zero? (get-in @listener [:recovery :epoch])) "No reconnect")])))

(deftest _unsubscribe-all-under-concurrency
  ;; Regression: unsubscribe-all used to build its expected-ack set from a
  ;; snapshot taken outside the write-lock; concurrent ops could make the
  ;; expectation impossible => ack timeout => healthy listener closed.
  (let [suffix (str (java.util.UUID/randomUUID))
        chan   #(str "carmine:v4:pubsub:racing:" suffix ":" %)
        listener
        (car/pubsub-listener
          {:on-message (fn [_]), :default-timeout-ms 4000})]
    (with-open [_ listener]
      (dotimes [_ 10]
        (let [ops
              (into
                (mapv
                  (fn [idx]
                    (future
                      (car/pubsub-subscribe! listener {:channels [(chan idx)]})
                      (when (odd? idx)
                        (car/pubsub-unsubscribe! listener {:channels [(chan idx)]}))
                      true))
                  (range 4))
                [(future (car/pubsub-unsubscribe! listener {}))])]
          (doseq [f ops]
            (let [result (try (deref f 5000 ::timeout) (catch Throwable t t))]
              (is (= result true))))))
      (is (:open? @listener) "Listener survives racing unsubscribe-all calls")
      (is (= true (car/pubsub-unsubscribe! listener {})))
      (is (= (car/pubsub-ping! listener) true)))))

(deftest _supervised-close-interrupts-blocked-writes
  (let [listener (car/pubsub-listener
                   {:on-message identity, :on-event identity
                    :recovery-opts {:check-ms 60000}})
        inner @(.-inner_ ^taoensso.carmine_v4.pubsub.PubSubListener listener)
        socket (:socket @(.-conn ^taoensso.carmine_v4.pubsub.PubSubConn inner))
        entered_ (promise), release_ (promise)
        write-requests write/write-requests]
    (try
      (with-redefs [write/write-requests
                    (fn [out requests]
                      (deliver entered_ true)
                      @release_
                      (write-requests out requests))]
        (let [subscribe_ (future
                           (truss/throws
                             (car/pubsub-subscribe! listener
                               {:channels ["carmine:v4:pubsub:blocked-write"]})))
              _ (await! entered_)
              close_ (future (car/pubsub-close! listener))]
          (try
            [(is (= true (deref close_ 2000 ::timeout))
               "Close completes even while a subscription write holds ops-lock")
             (is (.isClosed ^java.net.Socket socket)
               "Closing the socket can interrupt a blocked network write")]
            (finally (deliver release_ true)))
          (is (instance? Throwable (deref subscribe_ 2000 ::timeout))
            "The in-flight operation fails after its socket closes")
          (await! close_)))
      (finally
        (deliver release_ true)
        (car/pubsub-close! listener)))))

(deftest _supervised-ops-are-serialized
  ;; Regression: concurrent supervised sub/unsub could apply to desired state
  ;; in one order and hit the wire in the other, diverging until recovery.
  (let [suffix  (str (java.util.UUID/randomUUID))
        channel (str "carmine:v4:pubsub:supervised-race:" suffix)
        listener
        (car/pubsub-listener
          {:on-message (fn [_])
           :recovery-opts {:check-ms 60000}})] ; No poll-driven recovery in-test
    (with-open [_ listener]
      (dotimes [_ 25]
        (let [add (future (car/pubsub-subscribe! listener {:channels [channel]}))
              del (future (car/pubsub-unsubscribe! listener {:channels [channel]}))]
          @add @del))
      ;; A pong is acknowledged after all prior ops' acknowledgements
      (car/pubsub-ping! listener)
      (support/await-pred! 4000 "subs to converge to desired state"
        #(= (:subs @listener) (:desired-subs @listener)))
      (is (= (:subs @listener) (:desired-subs @listener))
        "Wire state converges to desired state without any recovery")
      (is (zero? (get-in (car/pubsub-stats listener) [:counts :recoveries]))))))

(deftest _recovery-vs-ops-convergence
  ;; Stress: epoch recoveries racing concurrent supervised sub/unsub churn
  ;; must always converge (inner publication + resubscription enqueue are one
  ;; atomic step w.r.t. supervised ops via the ops-lock).
  (let [suffix  (str (java.util.UUID/randomUUID))
        channel (str "carmine:v4:recover-race:" suffix)
        listener
        (car/pubsub-listener
          {:on-message (fn [_])
           :init-subs {:channels [(str channel ":base")]}
           :recovery-opts {:check-ms 25, :backoff-ms 10}})]
    (with-open [_ listener]
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
        (support/await-pred! 8000 "recovery to settle and subs to converge"
          #(and (not (get-in @listener [:recovery :recovering?]))
                (= (:subs @listener) (:desired-subs @listener))))
        (is (= (:subs @listener) (:desired-subs @listener))
          "Wire state converges to desired state across recoveries")))))
