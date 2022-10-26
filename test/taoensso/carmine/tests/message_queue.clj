(ns taoensso.carmine.tests.message-queue
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.carmine :as car  :refer [wcar]]
   [taoensso.carmine.message-queue :as mq]))

(comment
  (remove-ns      'taoensso.carmine.tests.message-queue)
  (test/run-tests 'taoensso.carmine.tests.message-queue))

;;;; Config, etc.

(def conn-opts {})
(defmacro wcar* [& body] `(car/wcar conn-opts ~@body))

(def tq :carmine-test-queue)
(defn clear-tq! [] (mq/clear-queues conn-opts tq))
(defn tq-status [] (mq/queue-status conn-opts tq))

(defn test-fixture [f] (f) (clear-tq!))
(test/use-fixtures :once test-fixture)

(let [default-opts {:eoq-backoff-ms 100}]
  (defn- dequeue* [qname & [opts]]
    (mq/dequeue qname (conj default-opts opts))))

(defn sleep [n]
  (let [n (case n :eoq 500 n)]
    (Thread/sleep n)
    (str "slept " n "msecs")))

;;;;

(deftest tests-01
  (testing "Basic enqueue & dequeue"
    (clear-tq!)
    [(is (= (wcar* (dequeue* tq)) "eoq-backoff"))
     (is (:eoq-backoff? (tq-status)))

     (sleep :eoq)
     (is (not (:eoq-backoff? (tq-status))))

     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) "mid1"))
     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))
           {:carmine.mq/error :queued})) ; Dupe

     (is (= :queued (wcar* (mq/message-status tq :mid1))))
     (let [{:keys [messages mid-circle]} (tq-status)]
       [(is (= messages {"mid1" :msg1}))
        (is (= mid-circle ["mid1" "end-of-circle"]))])

     (is (= (wcar* (dequeue* tq)) "eoq-backoff")) ; End of circle
     (sleep :eoq)
     (is (= (peek (:mid-circle (tq-status))) "mid1"))
     (is (= (wcar* (dequeue* tq)) ["mid1" :msg1 1]))

     (is (= (wcar* (mq/message-status tq :mid1)) :locked))
     (is (= (wcar* (dequeue*          tq))       "eoq-backoff")) ; End of circle
     (sleep :eoq)
     (is (= (wcar* (dequeue*          tq)) nil)) ; Locked mid1
     ]))

(deftest tests-02
  (testing "Handling: success"
    (clear-tq!)
    [(is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) "mid1"))
     ;; (is (= (wcar* (dequeue* tq)) "eoq-backoff))

     ;; Handler will *not* run against eoq-backoff/nil reply:
     (let [reply (wcar* (dequeue* tq))]
       [(is (= reply "eoq-backoff"))
        (is (= (mq/handle1 conn-opts tq (fn handler [reply]) reply) nil))])

     (sleep :eoq)

     (let [reply (wcar* (dequeue* tq))
           p_ (promise)]

       [(is (= reply ["mid1" :msg1 1]))
        (is (= (mq/handle1 conn-opts tq
                 (fn handler [reply] (deliver p_ reply) {:status :success})
                 reply)
              :success))
        (is (= (deref p_ 0 ::timeout)
              {:qname :carmine-test-queue :mid "mid1"
               :message :msg1, :attempt 1}))])

     (is (= (wcar* (mq/message-status tq :mid1)) :done-awaiting-gc ))
     (is (= (wcar* (dequeue* tq)) "eoq-backoff"))
     (sleep :eoq)

     (is (= (wcar* (dequeue* tq)) nil)) ; Will gc
     (is (= (wcar* (mq/message-status tq :mid1)) nil))]))

(deftest tests-03
  (testing "Handling: handler crash"
    (clear-tq!)
    [(is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) "mid1"))
     (is (= (wcar* (dequeue*   tq)) "eoq-backoff"))
     (sleep :eoq)

     ;; Simulate bad handler
     (is (= (wcar* (dequeue* tq {:lock-ms 1000})) ["mid1" :msg1 1]))

     (is (= (wcar* (mq/message-status tq :mid1)) :locked))
     (is (= (wcar* (dequeue* tq)) "eoq-backoff"))

     (sleep 1500) ; Wait for lock to expire
     (is (= (wcar* (dequeue* tq)) ["mid1" :msg1 2]))]))

(deftest tests-04
  (testing "Handling: retry with backoff"
    (clear-tq!)
    [(is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) "mid1"))
     (is (= (wcar* (dequeue*   tq)) "eoq-backoff"))
     (sleep :eoq)

     (let [reply (wcar* (dequeue* tq))
           p_ (promise)]

       [(is (= reply ["mid1" :msg1 1]))
        (is (= (mq/handle1 conn-opts tq
                 (fn handler [reply] (deliver p_ reply) {:status :retry :backoff-ms 2000})
                 reply)
              :retry))

        (is (= (deref p_ 0 ::timeout)
              {:qname :carmine-test-queue :mid "mid1"
               :message :msg1, :attempt 1}))])

     (is (= (wcar* (mq/message-status tq :mid1)) :queued-with-backoff))
     (is (= (wcar* (dequeue* tq)) "eoq-backoff"))
     (sleep :eoq)
     (is (= (wcar* (dequeue* tq)) nil)) ; < handler backoff
     (is (= (wcar* (dequeue* tq)) "eoq-backoff"))

     (sleep 2500) ; > handler backoff
     (is (= (wcar* (dequeue* tq)) ["mid1" :msg1 2]))]))

(deftest tests-05
  (testing "Handling: success with backoff (dedupe)"
    (clear-tq!)
    [(is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) "mid1"))
     (is (= (wcar* (dequeue*   tq)) "eoq-backoff"))
     (sleep :eoq)

     (let [reply (wcar* (dequeue* tq))
           p_ (promise)]

       [(is (= reply ["mid1" :msg1 1]))
        (is (= (mq/handle1 conn-opts tq
                 (fn handler [reply] (deliver p_ reply) {:status :success :backoff-ms 2000})
                 reply)
              :success))

        (= (deref p_ 0 ::timeout)
          {:qname :carmine-test-queue :mid "mid1"
           :message :msg1, :attempt 1})])

     (is (= (wcar* (mq/message-status tq :mid1)) :done-with-backoff))
     (is (= (wcar* (dequeue* tq)) "eoq-backoff"))
     (sleep :eoq)

     (is (= (wcar* (dequeue* tq)) nil)) ; Will gc
     (is (= (wcar* (mq/message-status tq :mid1)) :done-with-backoff )) ; < handler backoff

     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1}))
           {:carmine.mq/error :done-with-backoff})) ; Dupe

     (sleep 2500) ; > handler backoff

     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) "mid1"))]))

(deftest tests-06
  (testing "Handling: enqueue while :locked"
    (clear-tq!)
    [(is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) "mid1"))
     (is (= (wcar* (dequeue*   tq)) "eoq-backoff"))
     (sleep :eoq)

     (let [reply (wcar* (dequeue* tq))
           p_ (promise)]

       [(is (= reply ["mid1" :msg1 1]))
        (future
          (mq/handle1 conn-opts tq
            (fn handler [_reply] (deliver p_ :handler-running)
              (sleep 2000) {:status :success}) ; Hold lock
            reply))

        (is (= (deref p_ 1000 ::timeout) :handler-running))
        (is (= (wcar* (mq/message-status tq :mid1)) :locked))])

     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) {:carmine.mq/error :locked}))
     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :allow-requeue? true})) "mid1"))
     (is (= (wcar* (mq/enqueue tq :msg1-requeued {:unique-message-id :mid1 :allow-requeue? true}))
           {:carmine.mq/error :locked-with-requeue}))

     (sleep 2500) ; > handler lock

     (is (= (wcar* (mq/message-status tq :mid1)) :queued)) ; Not :done-awaiting-gc
     (is (= (wcar* (dequeue* tq)) "eoq-backoff"))
     (sleep :eoq)
     (is (= (wcar* (dequeue* tq)) ["mid1" :msg1 1]))]))

(deftest tests-07
  (testing "Handling: enqueue while :done-with-backoff"
    (clear-tq!)
    [(is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) "mid1"))
     (is (= (wcar* (dequeue*   tq)) "eoq-backoff"))
     (sleep :eoq)

     (let [reply (wcar* (dequeue* tq))]

       [(is (= reply ["mid1" :msg1 1]))
        (is (= (mq/handle1 conn-opts tq
                 (fn handler [_reply] {:status :success :backoff-ms 2000})
                 reply)
              :success))
        (is (= (wcar* (mq/message-status tq :mid1)) :done-with-backoff))])

     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) {:carmine.mq/error :done-with-backoff}))
     (is (= (wcar* (mq/enqueue tq :msg1-requeued {:unique-message-id :mid1 :allow-requeue? true})) "mid1"))

     (sleep 2500) ; > handler backoff
     (is (= (wcar* (mq/message-status tq :mid1)) :queued)) ; Not :done-awaiting-gc

     (is (= (wcar* (dequeue* tq)) "eoq-backoff"))
     (sleep :eoq)
     (is (= (wcar* (dequeue* tq)) ["mid1" :msg1 1]))]))

(deftest tests-08
  (testing "Enqueue/dequeue with initial backoff"
    (clear-tq!)
    [(is (= (wcar* (dequeue* tq)) "eoq-backoff"))
     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :initial-backoff-ms 500})) "mid1"))
     (is (= (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2 :initial-backoff-ms 100})) "mid2"))

     (let [{:keys [messages mid-circle]} (tq-status)]
       [(is (= messages {"mid1" :msg1 "mid2" :msg2}))
        (is (= mid-circle ["mid2" "mid1" "end-of-circle"]))])

     ;; Dupes before the backoff expired
     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) {:carmine.mq/error :queued-with-backoff}))
     (is (= (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2})) {:carmine.mq/error :queued-with-backoff}))

     ;; Both should be queued with backoff before the backoff expires
     (is (= (wcar* (mq/message-status tq :mid1)) :queued-with-backoff))
     (is (= (wcar* (mq/message-status tq :mid2)) :queued-with-backoff))

     (sleep 150) ; > 2nd msg
     (is (= (wcar* (mq/message-status tq :mid1)) :queued-with-backoff))
     (is (= (wcar* (mq/message-status tq :mid2)) :queued))

     (sleep 750) ; > 1st msg
     (is (= (wcar* (mq/message-status tq :mid1)) :queued))
     (is (= (wcar* (mq/message-status tq :mid2)) :queued))

     ;; Dupes after backoff expired
     (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) {:carmine.mq/error :queued}))
     (is (= (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2})) {:carmine.mq/error :queued}))

     (is (= (wcar* (dequeue* tq)) "eoq-backoff"))
     (sleep :eoq)

     (is (= (wcar* (dequeue* tq)) ["mid1" :msg1 1]))
     (is (= (wcar* (mq/message-status tq :mid1)) :locked))

     (is (= (wcar* (dequeue* tq)) ["mid2" :msg2 1]))

     (is (= (wcar* (mq/message-status tq :mid2)) :locked))
     (is (= (wcar* (dequeue* tq)) "eoq-backoff"))
     (sleep :eoq)
     (is (= (wcar* (dequeue* tq)) nil))]))

(deftest test-message-queue-with-initial-backoff
  [(testing "Message status changes over time"
     (clear-tq!)
     [(is (= (wcar* (dequeue* tq)) "eoq-backoff"))
      (sleep :eoq)

      (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :initial-backoff-ms 500})) "mid1"))
      (is (= (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2 :initial-backoff-ms 100})) "mid2"))

      (is (= (wcar* (mq/message-status tq :mid1)) :queued-with-backoff))
      (is (= (wcar* (mq/message-status tq :mid2)) :queued-with-backoff))

      (sleep 150) ; > 2nd msg
      (is (= (wcar* (mq/message-status tq :mid1)) :queued-with-backoff))
      (is (= (wcar* (mq/message-status tq :mid2)) :queued))

      (sleep 750) ; > 1st msg
      (is (= (wcar* (mq/message-status tq :mid1)) :queued))
      (is (= (wcar* (mq/message-status tq :mid2)) :queued))])

   (testing "Errors when we enqueue with same ids"
     (clear-tq!)
     [(is (= (wcar* (dequeue* tq)) "eoq-backoff"))
      (sleep :eoq)

      (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :initial-backoff-ms 500})) "mid1"))
      (is (= (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2 :initial-backoff-ms 100})) "mid2"))
      (is (= (wcar* (mq/message-status tq :mid1)) :queued-with-backoff))
      (is (= (wcar* (mq/message-status tq :mid2)) :queued-with-backoff))

      (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1})) {:carmine.mq/error :queued-with-backoff}))
      (is (= (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2})) {:carmine.mq/error :queued-with-backoff} ))])

   (testing "Errors change over time"
     (clear-tq!)
     [(is (= (wcar* (dequeue* tq)) "eoq-backoff"))
      (sleep :eoq)

      (is (= (wcar* (mq/enqueue tq :msg1 {:unique-message-id :mid1 :initial-backoff-ms 500})) "mid1"))
      (is (= (wcar* (mq/enqueue tq :msg2 {:unique-message-id :mid2 :initial-backoff-ms 100})) "mid2"))
      (is (= (wcar* (mq/message-status tq :mid1)) :queued-with-backoff))
      (is (= (wcar* (mq/message-status tq :mid2)) :queued-with-backoff))

      (sleep 150) ; > 2nd msg
      (is (= (wcar* (mq/message-status tq :mid2)) :queued))

      (sleep 750) ; > 1st msg
      (is (= (wcar* (mq/message-status tq :mid1)) :queued))])])
