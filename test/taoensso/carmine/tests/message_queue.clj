(ns taoensso.carmine.tests.message-queue
  (:require [clojure.test :refer :all]
            [clojure.string   :as str]
            [taoensso.carmine :as car]
            [taoensso.carmine.message-queue :as mq]))

(defmacro wcar* [& body] `(car/wcar {:pool {} :spec {}} ~@body))
(defn testq [& [more]] (if-not more "testq" (str "testq-" more)))
(defn clean-up []
  (when-let [test-keys (seq (wcar* (car/keys (mq/qkey (str (testq) "*")))))]
    (wcar* (apply car/del test-keys))))

(use-fixtures :each (fn [f] (clean-up) (f)))

(defn generate-keys []
  (into [] (map #(wcar* (mq/enqueue (testq) (str %))) (range 10))))

(deftest baseline
  (let [ids (generate-keys)]
    (is (= (wcar* (mq/dequeue-1 (testq))) "backoff"))
    (is (= (wcar* (mq/dequeue-1 (testq))) [(-> ids first first ) "0" "new"]))))

(defn slurp-keys []
  (doseq [i (range 10)]
    (let [[id _ s] (wcar* (mq/dequeue-1 (testq) :worker-context? true))]
      (wcar* (car/sadd (mq/qkey (testq) "recently-done") id)))))

(deftest worker-mimicking
  (let [[[id _]] (generate-keys)]
    (is (= (wcar* (mq/status (testq) id)) "pending"))
    (is (= (wcar* (mq/dequeue-1 (testq))) "backoff"))
    (is (= (wcar* (mq/dequeue-1 (testq) :worker-context? true)) [id "0" "new"]))
    (is (= (wcar* (mq/status (testq) id)) "processing"))
    (wcar* (car/sadd (mq/qkey (testq) "recently-done") id))
    (is (= (wcar* (mq/status (testq) id)) "done"))
    (slurp-keys)
    (Thread/sleep 2000) ; Wait for backoff to expire
    (slurp-keys)
    (is (= (wcar* (mq/status (testq) id)) nil))))

(defn queue-metadata "Returns given queue's current metadata."
  [qname]
  {:messsages     (wcar* (car/hgetall* (mq/qkey qname "messages")))
   :locks         (wcar* (car/hgetall* (mq/qkey qname "locks")))
   :id-circle     (wcar* (car/lrange   (mq/qkey qname "id-circle") 0 -1))
   :recently-done (wcar* (car/smembers (mq/qkey qname "recently-done")))
   :backoff?      (wcar* (car/get      (mq/qkey qname "backoff?")))})

(deftest cleanup
  (let [[id c]     (wcar* (mq/enqueue (testq) 1))
        [u-id u-c] (wcar* (mq/enqueue (testq "untouched") 1))]
    (is (= c) 2) ; One message + end of circle
    (is (= (wcar* (mq/dequeue-1 (testq) :worker-context? true)) "backoff"))
    (Thread/sleep 2000) ; Wait for backoff to expire
    (is (= (wcar* (mq/dequeue-1 (testq) :worker-context? true)) [id 1 "new"]))
    (wcar* (mq/clear (testq)))
    (is (= (queue-metadata (testq))
        {:backoff? nil :id-circle [] :messsages {} :recently-done [] :locks {}}))
    (is (= (queue-metadata (testq "untouched")) ; cleanup is isolated
        {:backoff? nil, :id-circle [u-id "end-of-circle"]
         :messsages {u-id 1}, :recently-done [], :locks {}}))))

(defn create-worker [q resp]
  (let [prm (promise)]
    (mq/make-dequeue-worker {:pool {} :spec {}} q
      :handler-fn (fn [x] (deliver prm x) resp))
    prm))

(defn assert-unlocked [q _]  (empty? (:locks (queue-metadata q))))
(defn assert-done     [q id] ((set (:recently-done (queue-metadata q))) id))

(deftest statuses
  (are [q resp i assertion]
       (let [prm    (create-worker q resp)
             [id _] (wcar* (mq/enqueue q i))
             result @prm]
         (Thread/sleep 500) ; Allow time for post-handler msg cleanup
         (and (= result i) (assertion q id)))
       (testq "default") nil      1 assert-done
       (testq "retry")   :retry   2 assert-unlocked
       (testq "success") :success 3 assert-done
       (testq "error")   :error   4 assert-done))

(clean-up)