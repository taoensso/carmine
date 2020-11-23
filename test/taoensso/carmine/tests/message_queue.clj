(ns taoensso.carmine.tests.message-queue
  (:require
   [clojure.test     :as test :refer [is deftest testing]]
   [taoensso.carmine :as car  :refer [wcar]]
   [taoensso.carmine.message-queue :as mq]))

(comment
  (remove-ns      'taoensso.carmine.tests.message-queue)
  (test/run-tests 'taoensso.carmine.tests.message-queue))

(def ^:private tq :carmine-test-queue)
(def ^:private conn-opts {})

(defn- clear-tq  [] (mq/clear-queues conn-opts tq))
(defn- tq-status [] (mq/queue-status conn-opts tq))

(defmacro wcar* [& body] `(car/wcar conn-opts ~@body))
(defn- dequeue* [qname & [opts]]
  (let [r (mq/dequeue qname (merge {:eoq-backoff-ms 175} opts))]
    (Thread/sleep 205) r))

(defn- test-fixture [f] (clear-tq) (f) (clear-tq))
(test/use-fixtures :once test-fixture)

(deftest tests-1 ; Basic enqueuing & dequeuing
  (is (do (println (str "Running message queue tests")) true))
  (is (= "eoq-backoff" (do (clear-tq) (wcar* (dequeue* tq)))))
  (is (= "mid1"        (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))))
  (is
    (let [status (tq-status)
          {:keys [messages mid-circle]} status]
      (and
        (= messages {"mid1" :msg1})
        (= mid-circle ["mid1" "end-of-circle"]))))
  (is (= :queued                     (wcar* (mq/message-status tq :mid1))))

  ;; Dupe
  (is (= {:carmine.mq/error :queued} (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))))

  (is (= "eoq-backoff"    (wcar* (dequeue* tq))))
  (is (= ["mid1" :msg1 1] (wcar* (dequeue* tq)))) ; New msg
  (is (= :locked          (wcar* (mq/message-status tq :mid1))))
  (is (= "eoq-backoff"    (wcar* (dequeue* tq))))
  (is (= nil              (wcar* (dequeue* tq)))) ; Locked msg
  )

(deftest tests-2 ; Handling: success
  (is (= "mid1" (do (clear-tq) (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})))))
  ;; (is (= "eoq-backoff" (wcar* (dequeue* tq))))

  ;; Handler will *not* run against eoq-backoff/nil reply:
  (is (= nil (mq/handle1 conn-opts tq nil (wcar* (dequeue* tq)))))
  (is (= {:qname :carmine-test-queue :mid "mid1" :message :msg1, :attempt 1}
        (let [p (promise)]
          (mq/handle1 conn-opts tq #(do (deliver p %) {:status :success})
            (wcar* (dequeue* tq)))
          @p)))

  (is (= :done-awaiting-gc (wcar* (mq/message-status tq :mid1))))
  (is (= "eoq-backoff"     (wcar* (dequeue* tq))))
  (is (= nil               (wcar* (dequeue* tq)))) ; Will gc
  (is (= nil               (wcar* (mq/message-status tq :mid1)))))

(deftest tests-3 ; Handling: handler crash
  (is (= "mid1"        (do (clear-tq) (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})))))
  (is (= "eoq-backoff" (wcar* (dequeue* tq))))

  ;; Simulates bad handler
  (is (= ["mid1" :msg1 1] (wcar* (dequeue* tq {:lock-ms 3000}))))

  (is (= :locked          (wcar* (mq/message-status tq :mid1))))
  (is (= "eoq-backoff"    (wcar* (dequeue* tq))))
  (is (= ["mid1" :msg1 2] (do (Thread/sleep 3000) ; Wait for lock to expire
                              (wcar* (dequeue* tq))))))

(deftest tests-4 ; Handling: retry with backoff
  (is (= "mid1"        (do (clear-tq) (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})))))
  (is (= "eoq-backoff" (wcar* (dequeue* tq))))
  (is (= {:qname :carmine-test-queue :mid "mid1" :message :msg1, :attempt 1}
        (let [p (promise)]
          (mq/handle1 conn-opts tq
            #(do (deliver p %) {:status :retry :backoff-ms 3000})
            (wcar* (dequeue* tq)))
          @p)))
  (is (= :queued-with-backoff (wcar* (mq/message-status tq :mid1))))
  (is (= "eoq-backoff"        (wcar* (dequeue* tq))))
  (is (= nil                  (wcar* (dequeue* tq)))) ; Backoff (< 3s)
  (is (= "eoq-backoff"        (wcar* (dequeue* tq))))
  (is (= ["mid1" :msg1 2]     (do (Thread/sleep 3000) ; Wait for backoff to expire
                                  (wcar* (dequeue* tq))))))

(deftest tests-5 ; Handling: success with backoff (dedupe)
  (is (= "mid1"        (do (clear-tq) (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})))))
  (is (= "eoq-backoff" (wcar* (dequeue* tq))))
  (is (= {:qname :carmine-test-queue :mid "mid1" :message :msg1, :attempt 1}
        (let [p (promise)]
          (mq/handle1 conn-opts tq
            #(do (deliver p %) {:status :success :backoff-ms 3000})
            (wcar* (dequeue* tq)))
          @p)))
  (is (= :done-with-backoff (wcar* (mq/message-status tq :mid1))))
  (is (= "eoq-backoff"      (wcar* (dequeue* tq))))
  (is (= nil                (wcar* (dequeue* tq)))) ; Will gc
  (is (= :done-with-backoff (wcar* (mq/message-status tq :mid1)))) ; Backoff (< 3s)
  (is (= {:carmine.mq/error :done-with-backoff}
         (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})))) ; Dupe
  (is (= "mid1" (do (Thread/sleep 3000) ; Wait for backoff to expire
                    (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))))))

(deftest test-6 ; Handling: enqueue while :locked
  (is (=  "mid1"        (do (clear-tq) (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})))))
  (is (= "eoq-backoff" (wcar* (dequeue* tq))))
  (is (= :locked
        (do (future
              (mq/handle1 conn-opts tq
                (fn [_] (Thread/sleep 3000) ; Hold lock
                  {:status :success})
                (wcar* (dequeue* tq))))
            (Thread/sleep 50)
            (wcar* (mq/message-status tq :mid1)))))
  (is (= {:carmine.mq/error :locked} (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))))
  (is (= "mid1" (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :allow-requeue? true}))))
  (is (= {:carmine.mq/error :locked-with-requeue}
        (wcar* (mq/enqueue tq :msg1-requeued {:unique-message-id :mid1 :allow-requeue? true}))))
  (is (= :queued ; cmp :done-awaiting-gc
        (do (Thread/sleep 3500) ; Wait for handler to complete (extra time for future!)
            (wcar* (mq/message-status tq :mid1)))))
  (is (= "eoq-backoff"    (wcar* (dequeue* tq))))
  (is (= ["mid1" :msg1 1] (wcar* (dequeue* tq)))))

(deftest test-7 ; Handling: enqueue while :done-with-backoff
  (is (= "mid1" (do (clear-tq) (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})))))
  (is (= "eoq-backoff" (wcar* (dequeue* tq))))
  (is (= :done-with-backoff
        (do (mq/handle1 conn-opts tq
              (fn [_] {:status :success :backoff-ms 3000})
              (wcar* (dequeue* tq)))
            (Thread/sleep 20)
            (wcar* (mq/message-status tq :mid1)))))
  (is (= {:carmine.mq/error :done-with-backoff} (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))))
  (is (= "mid1" (wcar* (mq/enqueue tq :msg1-requeued {:unique-message-id :mid1 :allow-requeue? true}))))
  (is (= :queued ; cmp :done-awaiting-gc
    (do (Thread/sleep 3000) ; Wait for backoff to expire
        (wcar* (mq/message-status tq :mid1)))))
  (is (= "eoq-backoff"    (wcar* (dequeue* tq))))
  (is (= ["mid1" :msg1 1] (wcar* (dequeue* tq)))))

(deftest test-8 ; Enqueue/dequeue with initial backoff
  (is (= "eoq-backoff" (do (clear-tq) (wcar* (dequeue* tq)))))
  (is (= "mid1"        (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :initial-backoff-ms 500}))))
  (is (= "mid2"        (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2 :initial-backoff-ms 100}))))
  (is
    (let [status (tq-status)
          {:keys [messages mid-circle]} status]
      (and
        (= messages {"mid1" :msg1 "mid2" :msg2})
        (= mid-circle ["mid2" "mid1" "end-of-circle"]))))
  ;; Dupes before the backoff expired
  (is (= {:carmine.mq/error :queued-with-backoff} (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))))
  (is (= {:carmine.mq/error :queued-with-backoff} (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2}))))
  ;; Both should be queued with backoff before the backoff expires
  (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid1))))
  (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid2))))
  ;; Move time past second message
  (is (do (Thread/sleep 150) true))
  (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid1))))
  (is (= :queued                     (wcar* (mq/message-status tq :mid2))))
  ;; Move time past first message
  (is (do (Thread/sleep 750) true))
  (is (= :queued                     (wcar* (mq/message-status tq :mid1))))
  (is (= :queued                     (wcar* (mq/message-status tq :mid2))))
  ;; Dupes after the backoff expired
  (is (= {:carmine.mq/error :queued} (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))))
  (is (= {:carmine.mq/error :queued} (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2}))))
  ;; TODO Is the order of retrieval actually predictable?
  (is (= "eoq-backoff"    (wcar* (dequeue* tq))))
  (is (= ["mid1" :msg1 1] (wcar* (dequeue* tq))))
  (is (= :locked          (wcar* (mq/message-status tq :mid1))))
  (is (= ["mid2" :msg2 1] (wcar* (dequeue* tq))))
  (is (= :locked          (wcar* (mq/message-status tq :mid2))))
  (is (= "eoq-backoff"    (wcar* (dequeue* tq))))
  (is (= nil              (wcar* (dequeue* tq))))
  )

(deftest test-message-queue-with-initial-backoff
  (testing "Message status changes over time"
    ;; Setup
    (is (= "eoq-backoff" (do (clear-tq) (wcar* (dequeue* tq)))))
    (is (= "mid1"                      (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :initial-backoff-ms 500}))))
    (is (= "mid2"                      (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2 :initial-backoff-ms 100}))))
    (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid1))))
    (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid2))))
    (is (do (Thread/sleep 150) true)) ;; Move time past second message
    (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid1))))
    (is (= :queued                     (wcar* (mq/message-status tq :mid2))))
    (is (do (Thread/sleep 750) true)) ;; Move time past first message
    (is (= :queued                     (wcar* (mq/message-status tq :mid1))))
    (is (= :queued                     (wcar* (mq/message-status tq :mid2)))))
  (testing "Errors when we enqueue with same ids"
    (is (= "eoq-backoff" (do (clear-tq) (wcar* (dequeue* tq)))))
    (is (= "mid1"        (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :initial-backoff-ms 500}))))
    (is (= "mid2"        (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2 :initial-backoff-ms 100}))))
    (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid1))))
    (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid2))))
    (is (= {:carmine.mq/error :queued-with-backoff} (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))))
    (is (= {:carmine.mq/error :queued-with-backoff} (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2})))))
  (testing "Errors change over time"
    (is (= "eoq-backoff" (do (clear-tq) (wcar* (dequeue* tq)))))
    (is (= "mid1"                      (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :initial-backoff-ms 500}))))
    (is (= "mid2"                      (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2 :initial-backoff-ms 100}))))
    (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid1))))
    (is (= :queued-with-backoff        (wcar* (mq/message-status tq :mid2))))
    (is (do (Thread/sleep 150) true)) ;; Move time past second message
    (is (= :queued                     (wcar* (mq/message-status tq :mid2))))
    (is (do (Thread/sleep 750) true)) ;; Move time past first message
    (is (= :queued                     (wcar* (mq/message-status tq :mid1))))))
