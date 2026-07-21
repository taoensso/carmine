(ns taoensso.carmine-v4.tests.transactions
  "Carmine v4 standalone transaction tests."
  (:require
   [clojure.test        :refer [deftest testing is]]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine-v4          :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tk  "Test key" support/test-key)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

;;;; Transactions

(deftest _script-fallback-in-effects
  [(is (throws? :ex-info {:eid :carmine.script/tx-effects-not-supported}
         (car/transact! mgr_ #(car/eval* "return 1" 0)))
     "`eval*`'s NOSCRIPT fallback cannot work inside MULTI (every reply is QUEUED)")
   (is (= (wcar mgr_ (car/eval* "return 'QUEUED'" 0)) "QUEUED")
     "A script legitimately returning \"QUEUED\" is unaffected outside transactions")])

(deftest _transaction-nippy-options
  (let [pwd  [:cached "carmine-v4-test-password"]
        data {:secret [1 2 3]}
        key  (tk (str "transaction:nippy:" (java.util.UUID/randomUUID)))
        skey (str key ":plain")]
    [(is (= (:replies
              (car/with-freeze-opts {:password pwd}
                (car/transact! mgr_ #(do (car/set key data) (car/set skey "plain")))))
           ["OK" "OK"]))
     (is (= (car/with-thaw-opts {:password pwd}
              (car/transact! mgr_ {:watch-keys [key]}
                #(with-replies (car/get key))
                (fn [plan] (car/get key) (car/get skey) (car/set skey "next"))))
           {:replies [data "plain" "OK"], :plan data, :attempts 1})
       "Plan and EXEC replies thaw with opts around `transact!`")]))

(deftest _transactions
  (let [key (tk (str "transaction:" (java.util.UUID/randomUUID)))]
    (wcar mgr_ (car/del key) (car/set key "0"))

    (testing "Blind and planned transactions"
      (is (= (car/transact! mgr_ #(car/set key "1"))
            {:replies ["OK"], :plan nil, :attempts 1}))
      (is (throws? :ex-info (car/transact! nil car/ping)))
      (is (throws? :ex-info {:eid :carmine.tx/invalid-opts}
            (car/transact! mgr_ {:unknown true} car/ping)))
      (is (throws? :ex-info {:eid :carmine.tx/invalid-opts}
            (car/transact! mgr_ {:watch-keys false} car/ping)))
      (is (= (wcar mgr_
               (car/ping)
               (resp/local-echo (:replies (car/transact! mgr_ car/ping))))
            ["PONG" ["PONG"]])
        "Nested transactions use a separate borrow after flushing the parent")
      (is (= (car/transact! mgr_ {:watch-keys [key]}
               #(with-replies (car/as-long (car/get key)))
               (fn [current] (car/set key (inc current))))
            {:replies ["OK"], :plan 1, :attempts 1}))
      (is (= (wcar mgr_ (car/get key)) "2")))

    (testing "Planning may issue additional completed WATCH commands"
      (is (= (:replies
               (car/transact! mgr_ {}
                 (fn []
                   (with-replies (car/watch key))
                   (with-replies (car/get key)))
                 (fn [current] (car/set key current))))
            ["OK"])))

    (testing "Optimistic conflicts retry only after nil EXEC"
      (let [attempts_ (atom 0)
            result
            (car/transact! mgr_
              {:watch-keys [key], :max-attempts 3
               :retry-backoff-ms (fn [attempt] (is (= attempt 1)) 0)}
              (fn []
                (let [current (with-replies (car/as-long (car/get key)))]
                  (when (= (swap! attempts_ inc) 1)
                    (wcar mgr_ (car/set key "10")))
                  current))
              (fn [current] (car/set key (inc current))))]
        [(is (= result {:replies ["OK"], :plan 10, :attempts 2}))
         (is (= @attempts_ 2))
         (is (= (wcar mgr_ (car/get key)) "11"))]))

    (testing "Conflicts are bounded and leave the protocol clean"
      (let [attempts_ (atom 0)
            failure
            (truss/throws
              (car/transact! mgr_ {:watch-keys [key], :max-attempts 2}
                (fn []
                  (let [current (with-replies (car/get key))]
                    (swap! attempts_ inc)
                    (wcar mgr_ (car/incr key))
                    current))
                (fn [current] (car/set key current))))]
        [(is (truss/submap? (ex-data failure)
               {:eid :carmine.tx/conflict, :attempts 2}))
         (is (= @attempts_ 2))
         (is (= (wcar mgr_ (car/ping)) "PONG"))]))

    (testing "Queue errors and EXEC errors remain distinguishable"
      (let [queue-failure
            (truss/throws
              (car/transact! mgr_
                #(resp/rcmd "CARMINE-INVALID-TRANSACTION-COMMAND")))
            unexpected
            (truss/throws
              (car/transact! mgr_ #(resp/local-echo :not-a-redis-command)))
            _ (wcar mgr_ (car/set key "string"))
            exec-failure
            (truss/throws
              (car/transact! mgr_ {}
                (fn [] :plan)
                (fn [_] (car/incr key) (car/ping) (car/incr key))))
            result
            (car/transact! mgr_ {:error-mode :return}
              #(do (car/incr key) (car/ping)))
            codes #(mapv (fn [x] (if (com/reply-error? x) (:code (ex-data x)) x)) %)]
        [(is (truss/submap? (ex-data queue-failure)
               {:eid :carmine.tx/queued-command-rejected}))
         (is (truss/submap? (ex-data unexpected)
               {:eid :carmine.tx/unexpected-reply, :phase :queue}))
         (is (car/reply-error? {:code "ERR"} exec-failure))
         (is (truss/submap? (ex-data exec-failure)
               {:eid :carmine/reply-errors, :error-indexes [0 2], :plan :plan, :attempts 1}))
         (is (= (codes (:replies (ex-data exec-failure))) ["ERR" "PONG" "ERR"]))
         (is (= (codes (:replies result)) ["ERR" "PONG"]))
         (is (truss/submap? (ex-data (truss/throws (car/transact! mgr_ {:error-mode :bad} #(car/ping))))
               {:eid :carmine.tx/invalid-opts}))]))

    (testing "EXEC errors and final conflicts leave the pooled connection reusable"
      (with-open [mgr
                  (conns/conn-manager-pooled
                    {:pool-opts {:max-total 1, :max-idle 1}})]
        (wcar mgr (car/set key "string"))
        (let [client-id (wcar mgr (resp/rcmd "CLIENT" "ID"))]
          [(is (car/reply-error? (truss/throws (car/transact! mgr #(car/incr key)))))
           (is (= (wcar mgr (resp/rcmd "CLIENT" "ID")) client-id))
           (is (throws? :ex-info {:eid :carmine.tx/conflict, :attempts 1, :watch-keys [key]}
                 (car/transact! mgr {:watch-keys [key], :max-attempts 1}
                   (fn []  (wcar mgr_ (car/set key "changed")))
                   (fn [_] (car/set key "lost")))))
           (is (= (wcar mgr (resp/rcmd "CLIENT" "ID")) client-id))
           (is (= (wcar mgr (car/get key)) "changed"))])))

    (testing "An unexpected EXEC reply is never retried"
      (let [counter (str key ":counter")
            rcmd    resp/rcmd
            failure
            (with-redefs [resp/rcmd ; Swap EXEC's reply for a scalar, after Redis commits
                          (fn [& args]
                            (if (= args ["EXEC"])
                              (car/parse (fn [_] :scalar) (apply rcmd args))
                              (apply rcmd args)))]
              (truss/throws (car/transact! mgr_ #(car/incr counter))))]
        [(is (truss/submap? (ex-data failure)
               {:eid :carmine.tx/unexpected-reply, :phase :exec, :reply :scalar}))
         (is (= (wcar mgr_ (car/get counter)) "1"))]))

    (testing "Throwing paths invalidate dirty pooled connections"
      (with-open [mgr
                  (conns/conn-manager-pooled
                    {:pool-opts {:max-total 1, :max-idle 1}})]
        (is (= (wcar mgr (car/ping)) "PONG"))
        (let [before (get-in @mgr [:stats :counts :destroyed :total])]
          [(is (throws? Exception "Expected plan failure"
                 (car/transact! mgr {:watch-keys [key]}
                   (fn [] (throw (Exception. "Expected plan failure")))
                   (fn [_] (car/ping)))))
           (is (= (wcar mgr (car/ping)) "PONG"))
           (is (> (get-in @mgr [:stats :counts :destroyed :total]) before))])
        (let [before (get-in @mgr [:stats :counts :destroyed :total])]
          [(is (throws? Exception "Expected effects failure"
                 (car/transact! mgr
                   #(throw (Exception. "Expected effects failure")))))
           (is (= (wcar mgr (car/ping)) "PONG"))
           (is (> (get-in @mgr [:stats :counts :destroyed :total]) before))])
        (let [before (get-in @mgr [:stats :counts :destroyed :total])
              failure
              (truss/throws
                (car/transact! mgr {:watch-keys [key]}
                  #(with-replies
                     (resp/rcmd "CARMINE-INVALID-COMMAND"))
                  (fn [_] (car/ping))))]
          [(is (car/reply-error? failure))
           (is (= (:eid (ex-data failure)) :carmine/reply-errors))
           (is (= (wcar mgr (car/ping)) "PONG"))
           (is (> (get-in @mgr [:stats :counts :destroyed :total]) before))])))

    (testing "Plan requests must be explicitly completed"
      [(is (throws? :ex-info {:eid :carmine.tx/stray-plan-requests}
             (car/transact! mgr_ {}
               (fn [] (car/get key) :plan)
               (fn [_] (car/ping)))))
       (let [failure
             (truss/throws
               (car/transact! mgr_ {}
                 (fn []
                   (car/ping)
                   (with-replies (car/get key)))
                 (fn [_] (car/ping))))]
         (is (truss/submap? (ex-data failure)
               {:eid :carmine.tx/stray-plan-requests
                :pending-requests 0
                :pending-replies 1})))])

    (testing "Ambient read modes cannot suppress transaction orchestration"
      (is (= (car/skip-replies
               (car/transact! mgr_ #(car/ping)))
            {:replies ["PONG"], :plan nil, :attempts 1})))

    (testing "Cluster and replica-preferring managers are rejected before borrow"
      (let [cluster-spec (car/cluster-spec [["127.0.0.1" 7000]])
            sentinel-spec (car/sentinel-spec {:primary [["127.0.0.1" 26379]]})]
        (with-open [cluster-mgr
                    (car/conn-manager-clustered
                      {:conn-opts {:server {:cluster-spec cluster-spec}}})
                    replica-mgr
                    (car/conn-manager-unpooled
                      {:conn-opts
                       {:server
                        {:master-name :primary
                         :sentinel-spec sentinel-spec
                         :sentinel-opts {:prefer-read-replica? true}}}})]
          [(is (throws? :ex-info {:eid :carmine.tx/cluster-not-supported}
                 (car/transact! cluster-mgr car/ping)))
           (is (throws? :ex-info {:eid :carmine.script/cluster-not-supported}
                 (wcar cluster-mgr (car/lua-local "return 1" {} {}))))
           (is (throws? :ex-info {:eid :carmine.tx/replica-manager-not-supported}
                 (car/transact! replica-mgr car/ping)))])))))
