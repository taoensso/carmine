(ns test_carmine.message_queue
  (:use clojure.test
        clojure.pprint
        taoensso.carmine.message-queue)
  (:require [clojure.string :as str]
            [taoensso.carmine :as car]))


(def p (car/make-conn-pool))
(def s (car/make-conn-spec))

(defmacro wcar [& body] `(car/with-conn p s ~@body))

;; Delete all queue keys


(use-fixtures :each 
              (fn [f] 
                (let [queues (wcar (car/keys (qkey "*")))]
                  (when (seq queues) (wcar (apply car/del queues))))
                  (f) 
                ))
(defn generate-keys []
  (doall (into [] (map #(wcar (enqueue "myq" (str %))) (range 10)))))

(deftest baseline 
  (let [ids (generate-keys)]
    (is (= (wcar (dequeue-1 "myq")) "backoff")) 
    (is (= (wcar (dequeue-1 "myq")) [(-> ids first first ) "0" "new"])) 
    )) 

(defn slurp-keys []
  (doseq [i (range 10)]
    (let [[_id _ s] (wcar (dequeue-1 "myq" :worker-context? true))] 
      (wcar (car/sadd (qkey "myq" "recently-done") _id)))))

(deftest worker-mimicking
  (let [[[id _]] (generate-keys)]
    (is (= (wcar (status "myq" id)) "pending")) 
    (is (= (wcar (dequeue-1 "myq")) "backoff")) 
    (is (= (wcar (dequeue-1 "myq" :worker-context? true)) [id "0" "new"])) 
    (is (= (wcar (status "myq" id)) "processing")) 
    (wcar (car/sadd (qkey "myq" "recently-done") id))
    (is (= (wcar (status "myq" id)) "done")) 
    (slurp-keys)
    (Thread/sleep 2000); waiting for backoff to expire
    (slurp-keys)
    (is (= (wcar (status "myq" id)) nil)) 
    )
  )


