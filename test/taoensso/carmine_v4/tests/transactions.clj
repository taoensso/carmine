(ns taoensso.carmine-v4.tests.transactions
  "Carmine v4 standalone transaction tests."
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

;;;; Transactions

(deftest _script-fallback-in-effects
  [(is (throws? :ex-info {:eid :carmine.script/tx-effects-not-supported}
         (car/transact! mgr_ #(car/eval* "return 1" 0)))
     "`eval*`'s NOSCRIPT fallback cannot work inside MULTI (every reply is QUEUED)")
   (is (= (wcar mgr_ (car/eval* "return 'QUEUED'" 0)) "QUEUED")
     "A script legitimately returning \"QUEUED\" is unaffected outside transactions")])

(deftest _transactions
  (let [key (tk (str "transaction:" (java.util.UUID/randomUUID)))]
    (wcar mgr_ (car/del key) (car/set key "0"))

    (testing "Blind and planned transactions"
      (is (= (car/transact! mgr_ #(car/set key "1"))
            {:replies ["OK"], :plan nil, :attempts 1}))
      (is (throws? :ex-info (car/transact! nil car/ping)))
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
            result
            (car/transact! mgr_
              #(do (car/incr key) (car/ping)))]
        [(is (truss/submap? (ex-data queue-failure)
               {:eid :carmine.tx/queued-command-rejected}))
         (is (truss/submap? (ex-data unexpected)
               {:eid :carmine.tx/unexpected-reply, :phase :queue}))
         (is (= (mapv #(if (com/reply-error? %) (:code (ex-data %)) %) (:replies result))
               ["ERR" "PONG"]))]))

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
           (is (= (:eid (ex-data failure)) :carmine.read/drained-reply-errors))
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
           (is (throws? :ex-info {:eid :carmine.tx/replica-manager-not-supported}
                 (car/transact! replica-mgr car/ping)))])))))
