(ns taoensso.carmine.tests.message-queue
  (:require [clojure.test :refer :all]
            [clojure.string   :as str]
            [taoensso.carmine :as car :refer (wcar)]
            [taoensso.carmine.message-queue :as mq]))

(defn testq [& [more]] (if-not more "testq" (str "testq-" more)))
(defn clean-up []
  (when-let [test-keys (seq (wcar {} (car/keys (mq/qkey (str (testq) "*")))))]
    (wcar {} (apply car/del test-keys))))

(use-fixtures :each (fn [f] (clean-up) (f)))

(defn generate-keys []
  (into [] (map #(wcar {} (mq/enqueue (testq) (str %))) (range 10))))

(defn eoq-backoff [] (Thread/sleep 2000))

(deftest baseline
  (let [ids (generate-keys)]
    (is (= (wcar {} (mq/dequeue (testq))) "eoq-backoff"))
    (eoq-backoff)
    (is (= (wcar {} (mq/dequeue (testq))) [(first ids) "0" 1]))))

(defn slurp-keys []
  (doseq [i (range 10)]
    (let [[id _ _] (wcar {} (mq/dequeue (testq)))]
      (wcar {} (car/sadd (mq/qkey (testq) "recently-done") id)))))

(deftest worker-mimicking
  (let [[id] (generate-keys)]
    (is (= (mq/message-status {} (testq) id) :queued))
    (is (= (wcar {} (mq/dequeue (testq))) "eoq-backoff"))
    (eoq-backoff)
    (is (= (wcar {} (mq/dequeue (testq))) [id "0" 1]))
    (is (= (mq/message-status {} (testq) id) :locked))
    (wcar {} (car/sadd (mq/qkey (testq) "recently-done") id))
    (is (= (mq/message-status {} (testq) id) :recently-done))
    (slurp-keys)
    (eoq-backoff)
    (slurp-keys)
    (is (= (mq/message-status {} (testq) id) nil))))

(deftest cleanup
  (let [id   (wcar {} (mq/enqueue (testq) 1))
        u-id (wcar {} (mq/enqueue (testq "untouched") 1))]
    (is (= (wcar {} (mq/dequeue (testq))) "eoq-backoff"))
    (eoq-backoff)
    (is (= (wcar {} (mq/dequeue (testq))) [id 1 1]))
    (mq/clear-queues {} (testq))
    (is (= (mq/queue-status {} (testq))
           {:messages {} :locks {} :backoffs {} :retry-counts {}
            :recently-done #{} :eoq-backoff? nil :dry-runs nil
            :mid-circle []}))
    (is (= (mq/queue-status {} (testq "untouched")) ; cleanup is isolated
           {:messages {u-id 1} :locks {} :backoffs {} :retry-counts {}
            :recently-done #{} :eoq-backoff? nil :dry-runs nil
            :mid-circle [u-id "end-of-circle"]}))))

(defn create-worker [q resp]
  (let [prm (promise)]
    (mq/worker {} q {:handler (fn [{:keys [message]}] (deliver prm message)
                                resp)})
    prm))

(defn assert-unlocked [q _]  (empty? (:locks (mq/queue-status {} q))))
(defn assert-done     [q id] ((:recently-done (mq/queue-status {} q)) id))

(deftest statuses
  (are [q resp i assertion]
       (let [prm (create-worker q resp)
             id  (wcar {} (mq/enqueue q i))
             result @prm]
         (Thread/sleep 500) ; Allow time for post-handler msg cleanup
         (and (= result i) (assertion q id)))
       (testq "success") {:status :success} 1 assert-done
       (testq "retry")   {:status :retry}   2 assert-unlocked
       (testq "error")   {:status :error}   3 assert-done))

(clean-up)