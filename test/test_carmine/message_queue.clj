(ns test_carmine.message_queue
  (:use     [clojure.test])
  (:require [clojure.string   :as str]
            [taoensso.carmine :as car]
            [taoensso.carmine.message-queue :as mq]))

(def p (car/make-conn-pool))
(def s (car/make-conn-spec))
(defmacro wcar [& body] `(car/with-conn p s ~@body))

(def testq "testq")
(def tkey (partial mq/qkey testq))

(use-fixtures :each
  (fn [f] ; Delete all queue keys
    (when-let [queues (seq (wcar (car/keys (tkey "*"))))]
      (wcar (apply car/del queues)))
    (f)))

(defn generate-keys []
  (into [] (map #(wcar (mq/enqueue testq (str %))) (range 10))))

(deftest baseline
  (let [ids (generate-keys)]
    (is (= (wcar (mq/dequeue-1 testq)) "backoff"))
    (is (= (wcar (mq/dequeue-1 testq)) [(-> ids first first ) "0" "new"]))))

(defn slurp-keys []
  (doseq [i (range 10)]
    (let [[id _ s] (wcar (mq/dequeue-1 testq :worker-context? true))]
      (wcar (car/sadd (tkey "recently-done") id)))))

(deftest worker-mimicking
  (let [[[id _]] (generate-keys)]
    (is (= (wcar (mq/status testq id)) "pending"))
    (is (= (wcar (mq/dequeue-1 testq)) "backoff"))
    (is (= (wcar (mq/dequeue-1 testq :worker-context? true)) [id "0" "new"]))
    (is (= (wcar (mq/status testq id)) "processing"))
    (wcar (car/sadd (tkey "recently-done") id))
    (is (= (wcar (mq/status testq id)) "done"))
    (slurp-keys)
    (Thread/sleep 2000) ; Wait for backoff to expire
    (slurp-keys)
    (is (= (wcar (mq/status testq id)) nil))))

(defn queue-metadata "Returns given queue's current metadata."
   [qname]
   {:backoff?  (wcar (car/get      (mq/qkey qname "backoff?")))
    :id-circle (wcar (car/lrange   (mq/qkey qname "id-circle") 0 -1))
    :messsages (wcar (car/hgetall* (mq/qkey qname "messages")))
    :locks     (wcar (car/hgetall* (mq/qkey qname "locks")))})

(deftest cleanup
  (wcar (mq/clear testq))
  (let [[id c] (wcar (mq/enqueue testq 1))]
    (is (= c) 2) ; One message + end of circle
    (is (= (wcar (mq/dequeue-1 testq :worker-context? true)) "backoff"))
    (Thread/sleep 2000) ; Wait for backoff to expire
    (is (= (wcar (mq/dequeue-1 testq :worker-context? true)) [id 1 "new"]))
    (wcar (mq/clear testq))
    (is (= (queue-metadata testq))
        {:backoff? nil :id-circle [] :messsages {} :locks {}})))


