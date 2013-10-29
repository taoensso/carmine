(ns taoensso.carmine.tests.message-queue
  (:require [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.message-queue :as mq]))

(comment (test/run-tests '[taoensso.carmine.tests.message-queue]))

(defmacro wcar* [& body] `(car/wcar {:pool {} :spec {}} ~@body))
(def tq :carmine-test-queue)

(defn- before-run {:expectations-options :before-run} [] (mq/clear-queues {} tq))
(defn- after-run  {:expectations-options :after-run}  [] (mq/clear-queues {} tq))

(defn- dequeue*
  "Like `mq/dequeue` but has a constant (175ms) eoq-backoff-ms and always sleeps
  the same amount before returning."
  [qname & [opts]]
  (let [r (mq/dequeue qname (merge {:eoq-backoff-ms 175} opts))]
    (Thread/sleep 205) r))

;;;; Basic enqueuing & dequeuing
(expect (constantly true) (mq/clear-queues {} tq))
(expect "eoq-backoff" (wcar {} (dequeue* tq)))
(expect "mid1" (wcar {} (mq/enqueue tq :msg1 :mid1)))
(expect {:messages {"mid1" :msg1}, :mid-circle ["mid1" "end-of-circle"]}
        (-> (mq/queue-status {} tq)
            (select-keys [:mid-circle :messages])))
(expect :queued                     (wcar {} (mq/message-status tq :mid1)))
(expect {:carmine.mq/error :queued} (wcar {} (mq/enqueue tq :msg1 :mid1))) ; Dupe
(expect "eoq-backoff"    (wcar {} (dequeue* tq)))
(expect ["mid1" :msg1 1] (wcar {} (dequeue* tq))) ; New msg
(expect :locked          (wcar {} (mq/message-status tq :mid1)))
(expect "eoq-backoff"    (wcar {} (dequeue* tq)))
(expect nil              (wcar {} (dequeue* tq))) ; Locked msg

;;;; Handling: success
(expect (constantly true) (mq/clear-queues {} tq))
(expect "mid1" (wcar {} (mq/enqueue tq :msg1 :mid1)))
;; (expect "eoq-backoff" (wcar {} (dequeue* tq)))
;; Handler will *not* run against eoq-backoff/nil reply:
(expect nil (mq/handle1 {} tq nil (wcar {} (dequeue* tq))))
(expect {:mid "mid1", :message :msg1, :attempt 1}
        (let [p (promise)]
          (mq/handle1 {} tq #(do (deliver p %) {:status :success})
                      (wcar {} (dequeue* tq)))
          @p))
(expect :done-awaiting-gc (wcar {} (mq/message-status tq :mid1)))
(expect "eoq-backoff" (wcar {} (dequeue* tq)))
(expect nil           (wcar {} (dequeue* tq))) ; Will gc
(expect nil           (wcar {} (mq/message-status tq :mid1)))

;;;; Handling: handler crash
(expect (constantly true) (mq/clear-queues {} tq))
(expect "mid1" (wcar {} (mq/enqueue tq :msg1 :mid1)))
(expect "eoq-backoff" (wcar {} (dequeue* tq)))
(expect ["mid1" :msg1 1]
        (wcar {} (dequeue* tq {:lock-ms 3000}))) ; Simulates bad handler
(expect :locked          (wcar {} (mq/message-status tq :mid1)))
(expect "eoq-backoff"    (wcar {} (dequeue* tq)))
(expect ["mid1" :msg1 2] (do (Thread/sleep 3000) ; Wait for lock to expire
                             (wcar {} (dequeue* tq))))

;;;; Handling: retry with backoff
(expect (constantly true) (mq/clear-queues {} tq))
(expect "mid1" (wcar {} (mq/enqueue tq :msg1 :mid1)))
(expect "eoq-backoff" (wcar {} (dequeue* tq)))
(expect {:mid "mid1", :message :msg1, :attempt 1}
        (let [p (promise)]
          (mq/handle1 {} tq #(do (deliver p %) {:status :retry :backoff-ms 3000})
                      (wcar {} (dequeue* tq)))
          @p))
(expect :retry-backoff   (wcar {} (mq/message-status tq :mid1)))
(expect "eoq-backoff"    (wcar {} (dequeue* tq)))
(expect nil              (wcar {} (dequeue* tq))) ; Backoff (< 3s)
(expect "eoq-backoff"    (wcar {} (dequeue* tq)))
(expect ["mid1" :msg1 2] (do (Thread/sleep 3000) ; Wait for backoff to expire
                             (wcar {} (dequeue* tq))))

;;;; Handling: success with backoff (dedupe)
(expect (constantly true) (mq/clear-queues {} tq))
(expect "mid1" (wcar {} (mq/enqueue tq :msg1 :mid1)))
(expect "eoq-backoff" (wcar {} (dequeue* tq)))
(expect {:mid "mid1", :message :msg1, :attempt 1}
        (let [p (promise)]
          (mq/handle1 {} tq #(do (deliver p %) {:status :success :backoff-ms 3000})
                      (wcar {} (dequeue* tq)))
          @p))
(expect :done-with-backoff (wcar {} (mq/message-status tq :mid1)))
(expect "eoq-backoff"      (wcar {} (dequeue* tq)))
(expect nil                (wcar {} (dequeue* tq))) ; Will gc
(expect :done-with-backoff (wcar {} (mq/message-status tq :mid1))) ; Backoff (< 3s)
(expect {:carmine.mq/error :done-with-backoff}
        (wcar {} (mq/enqueue tq :msg1 :mid1))) ; Dupe
(expect "mid1" (do (Thread/sleep 3000) ; Wait for backoff to expire
                   (wcar {} (mq/enqueue tq :msg1 :mid1))))

;;;; Handling: finalized
(expect (constantly true) (mq/clear-queues {} tq))
(expect "mid1" (wcar {} (mq/enqueue tq :msg1 :mid1)))
(expect "eoq-backoff" (wcar {} (dequeue* tq)))
(expect {:mid "mid1", :message :msg1, :attempt 1}
        (let [p (promise)]
          (mq/handle1 {} tq #(do (deliver p %)
                                 (car/atomic {} 100
                                   (car/multi)
                                   (mq/finalize tq :success (:mid %)))
                                 {:status :finalized})
                      (wcar {} (dequeue* tq)))
          @p))
(expect :done-awaiting-gc (wcar {} (mq/message-status tq :mid1)))
