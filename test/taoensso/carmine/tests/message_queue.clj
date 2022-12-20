(ns taoensso.carmine.tests.message-queue
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc]
   [taoensso.carmine :as car  :refer [wcar]]
   [taoensso.carmine.message-queue :as mq]))

(comment
  (remove-ns      'taoensso.carmine.tests.message-queue)
  (test/run-tests 'taoensso.carmine.tests.message-queue))

;;;; Utils, etc.

(defn subvec? [v sub]
  (enc/reduce-indexed
    (fn [acc idx in]
      (if (= in (get v idx ::nx))
        acc
        (reduced false)))
    true
    sub))

(comment
  [(subvec? [:a :b :c] [:a :b])
   (subvec? [:a :b]    [:a :b :c])])

;;;; Config, etc.

(def conn-opts {})
(defmacro wcar* [& body] `(car/wcar conn-opts ~@body))

(def tq :carmine-test-queue)
(defn clear-tq! [] (mq/clear-queues conn-opts [tq]))
(defn tq-status [] (mq/queue-status conn-opts  tq))

(defn test-fixture [f] (f) (clear-tq!))
(test/use-fixtures :once test-fixture) ; Just for final teardown

(def ^:const default-lock-ms (enc/ms :mins 60))
(def ^:const eoq-backoff-ms 100)

(do
  (def handle1  #'mq/handle1)
  (def enqueue    mq/enqueue)
  (def msg-status mq/message-status)

  (let [default-opts {:eoq-backoff-ms eoq-backoff-ms}]
    (defn- dequeue [qname & [opts]]
      (#'mq/dequeue qname (conj default-opts opts)))))

(defn sleep
  ([          n] (sleep nil n))
  ([isleep-on n]
   (let [n (int (case n :eoq (* 2.5 eoq-backoff-ms) n))]
     (if-let [on isleep-on]
       (#'mq/interruptible-sleep conn-opts tq on n)
       (Thread/sleep                             n))

     (if-let [on isleep-on]
       (str "islept " n "msecs on " isleep-on)
       (str  "slept " n "msecs")))))

;;;;

(defn throw! [] (throw (Exception.)))
(defn handle-end-of-circle [isleep-on]
  (let [reply (wcar* (dequeue tq))]
    (every? identity
      [(is (= reply ["sleep" "end-of-circle" isleep-on eoq-backoff-ms]))
       (is (subvec? (handle1 nil conn-opts tq (fn hf [_] (throw!)) reply -1 nil)
             [:slept "end-of-circle" isleep-on #_eoq-backoff-ms]))
       (sleep isleep-on :eoq)])))

;;;;

(deftest basics
  (testing "Basic enqueue & dequeue"
    (clear-tq!)
    [(is (= (wcar* (dequeue tq)) ["sleep" "end-of-circle" "a" eoq-backoff-ms]))
     (sleep "a" :eoq)

     (is (= (wcar* (enqueue tq :msg1a {:mid :mid1}))                   {:action :added, :mid :mid1}))
     (is (= (wcar* (enqueue tq :msg1b {:mid :mid1}))                   {:error :already-queued}) "Dupe mid")
     (is (= (wcar* (enqueue tq :msg1b {:mid :mid1 :can-update? true})) {:action :updated, :mid :mid1}))

     (is (= (wcar* (msg-status tq :mid1)) :queued))
     (is (enc/submap? (tq-status)
           {:messages   {"mid1" :msg1b}
            :mids-ready ["mid1"]
            :mid-circle ["end-of-circle"]}))

     (is (subvec? (wcar* (dequeue    tq)) ["handle" "mid1" :msg1b 1 default-lock-ms #_udt]))
     (is (=       (wcar* (msg-status tq :mid1)) :locked))
     (is (=       (wcar* (dequeue    tq)) ["sleep" "end-of-circle" "a" eoq-backoff-ms]))]))

(deftest init-backoff
  (testing "Enqueue with initial backoff"
    (clear-tq!)
    [(is (= (wcar* (dequeue tq)) ["sleep" "end-of-circle" "a" eoq-backoff-ms]))
     (is (= (wcar* (enqueue tq :msg1 {:mid :mid1 :init-backoff-ms 500})) {:action :added, :mid :mid1}))
     (is (= (wcar* (enqueue tq :msg2 {:mid :mid2 :init-backoff-ms 100})) {:action :added, :mid :mid2}))

     (is (enc/submap? (tq-status)
           {:messages   {"mid1" :msg1, "mid2" :msg2}
            :mid-circle ["mid2" "mid1" "end-of-circle"]}))

     ;; Dupes before the backoff expired
     (is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:error :already-queued}))
     (is (= (wcar* (enqueue tq :msg2 {:mid :mid2})) {:error :already-queued}))

     ;; Both should be queued with backoff before the backoff expires
     (is (= (wcar* (msg-status tq :mid1)) :queued-with-backoff))
     (is (= (wcar* (msg-status tq :mid2)) :queued-with-backoff))

     (sleep 150) ; > 2nd msg
     (is (= (wcar* (msg-status tq :mid1)) :queued-with-backoff))
     (is (= (wcar* (msg-status tq :mid2)) :queued))

     (sleep 750) ; > 1st msg
     (is (= (wcar* (msg-status tq :mid1)) :queued))
     (is (= (wcar* (msg-status tq :mid2)) :queued))

     ;; Dupes after backoff expired
     (is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:error :already-queued}))
     (is (= (wcar* (enqueue tq :msg2 {:mid :mid2})) {:error :already-queued}))

     (handle-end-of-circle "b")

     (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
     (is (= (wcar* (msg-status tq :mid1)) :locked))

     (is (subvec? (wcar* (dequeue tq)) ["handle" "mid2" :msg2 1 default-lock-ms #_udt]))
     (is (= (wcar* (msg-status tq :mid2)) :locked))]))

(defn test-handler
  "Returns [<poll-reply> <handler-arg> <handle1-result>]"
  ([       hf] (test-handler false hf))
  ([async? hf]
   (let [poll-reply   (wcar* (dequeue tq))
         handler-arg_ (promise)
         handle1
         (fn []
           (handle1 nil conn-opts tq
             (fn [m] (deliver handler-arg_ m) (hf m)) poll-reply -1 nil))

         handle1-result
         (if async?
           (future-call handle1)
           (do         (handle1)))]

     [poll-reply (deref handler-arg_ 5000 :timeout) handle1-result])))

(deftest handlers
  [(testing "Handler => success"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:action :added, :mid :mid1}))

      (let [[pr ha hr] (test-handler (fn [_m] {:status :success}))]
        [(is (subvec?     pr ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
         (is (enc/submap? ha
               {:qname :carmine-test-queue, :mid "mid1", :message :msg1,
                :attempt 1, :lock-ms default-lock-ms}))
         (is (= hr [:handled :success]))])

      (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
      (handle-end-of-circle "a")
      (is (= (wcar* (dequeue    tq)) ["skip" "did-gc"]))
      (is (= (wcar* (msg-status tq :mid1)) nil))])

   (testing "Handler => throws"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:action :added, :mid :mid1}))

      (let [[pr ha hr] (test-handler (fn [_m] (throw!)))]
        [(is (subvec? pr ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
         (is (=       hr [:handled :error]))])

      (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc ))
      (handle-end-of-circle "a")
      (is (= (wcar* (dequeue    tq)) ["skip" "did-gc"]))
      (is (= (wcar* (msg-status tq :mid1)) nil))])

   (testing "Handler => success with backoff (dedupe)"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:action :added, :mid :mid1}))

      (let [[pr ha hr] (test-handler (fn [_m] {:status :success :backoff-ms 2000}))]
        [(is (subvec? pr ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
         (is (=       hr [:handled :success]))])

      (is (= (wcar* (msg-status tq :mid1)) :done-with-backoff))
      (handle-end-of-circle "a")
      (is (= (wcar* (dequeue tq)) ["skip" "done-with-backoff"]))

      (sleep 2500) ; > handler backoff
      (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
      (handle-end-of-circle "b")

      (is (= (wcar* (dequeue tq)) ["skip" "did-gc"]))])

   (testing "Handler => retry with backoff"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:action :added, :mid :mid1}))

      (let [[pr ha hr] (test-handler (fn [_m] {:status :retry :backoff-ms 2000}))]
        [(is (subvec? pr ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
         (is (=       hr [:handled :retry]))])

      (is (= (wcar* (msg-status tq :mid1)) :queued-with-backoff))
      (handle-end-of-circle "a")
      (is (= (wcar* (dequeue tq)) ["skip" "queued-with-backoff"]))

      (sleep 2500) ; > handler backoff
      (is (= (wcar* (msg-status tq :mid1)) :queued))
      (handle-end-of-circle "b")

      (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1 2 default-lock-ms #_udt]))])

   (testing "Handler => lock timeout"

     (testing "Default lock time"
       (clear-tq!)
       [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:action :added, :mid :mid1}))

        ;; Simulate bad handler
        (is (subvec? (wcar* (dequeue tq {:default-lock-ms 1000})) ["handle" "mid1" :msg1 1 1000 #_udt]))

        (is (= (wcar* (msg-status tq :mid1)) :locked))
        (handle-end-of-circle "a")

        (sleep 1500) ; Wait for lock to expire
        (is (subvec? (wcar* (dequeue tq {:default-lock-ms 1000})) ["handle" "mid1" :msg1 2 1000 #_udt]))])

     (testing "Custom lock time"
       (clear-tq!)
       [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1 :lock-ms 2000})) {:action :added, :mid :mid1}))

        ;; Simulate bad handler
        (is (subvec? (wcar* (dequeue tq {:default-lock-ms 500})) ["handle" "mid1" :msg1 1 2000 #_udt]))

        (is (= (wcar* (msg-status tq :mid1)) :locked))
        (handle-end-of-circle "a")

        (sleep 2500) ; Wait for lock to expire
        (is (subvec? (wcar* (dequeue tq {:default-lock-ms 500})) ["handle" "mid1" :msg1 2 2000 #_udt]))]))])

(deftest requeue
  [(testing "Enqueue while :locked"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1a {:mid :mid1})) {:action :added, :mid :mid1}))

      (do (test-handler :async (fn [_m] (Thread/sleep 2000) {:status :success})) :async-handler-running)

      (is (= (wcar* (msg-status tq :mid1)) :locked))
      (is (= (wcar* (enqueue    tq :msg1b {:mid :mid1})) {:error :locked}))

      (is (= (wcar* (enqueue    tq :msg1c {:mid :mid1, :can-requeue? true}))  {:action :added, :mid :mid1}))
      (is (= (wcar* (enqueue    tq :msg1d {:mid :mid1, :can-requeue? true}))  {:error :already-queued}))
      (is (= (wcar* (enqueue    tq :msg1e {:mid :mid1, :can-requeue? true,
                                           :can-update? true, :lock-ms 500})) {:action :updated, :mid :mid1}))

      (is (= (wcar* (msg-status tq :mid1)) :locked-with-requeue))
      (sleep 2500) ; > handler lock
      (is (= (wcar* (msg-status tq :mid1)) :done-with-requeue) "Not :done-awaiting-gc")
      (handle-end-of-circle "a")

      (is (=       (wcar* (dequeue tq)) ["skip" "did-requeue"]))
      (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1e 1 500 #_udt]))])

   (testing "Enqueue while :done-with-backoff"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1a {:mid :mid1})) {:action :added, :mid :mid1}))

      (do (test-handler (fn [_m] {:status :success :backoff-ms 2000})) :ran-handler)

      (is (= (wcar* (msg-status tq :mid1)) :done-with-backoff))
      (is (= (wcar* (enqueue    tq :msg1b {:mid :mid1}))                     {:error :backoff}))
      (is (= (wcar* (enqueue    tq :msg1c {:mid :mid1, :can-requeue? true,
                                           :lock-ms 500}))                   {:action :added, :mid :mid1}))
      (is (= (wcar* (msg-status tq :mid1)) :done-with-requeue))

      (handle-end-of-circle "a")
      (sleep 2500) ; > handler backoff

      (is (=       (wcar* (dequeue tq)) ["skip" "did-requeue"]))
      (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1c 1 500 #_udt]))])])

(deftest workers
  (testing "Basic worker functionality"
    (clear-tq!)
    (let [msgs_ (atom [])
          handler-fn
          (fn [{:keys [mid message] :as in}]
            (swap! msgs_ conj message)
            {:status :success})

          queue-status (fn [] (mq/queue-status conn-opts tq {:incl-legacy-data? false}))]

      (with-open [^java.io.Closeable worker
                  (mq/worker conn-opts tq
                    {:auto-start false,
                     :handler handler-fn
                     :throttle-ms    10
                     :eoq-backoff-ms 10})]

        [(is (enc/submap? (wcar* (enqueue tq :msg1 {:mid :mid1})) {:action :added}))
         (is (enc/submap? (wcar* (enqueue tq :msg2 {:mid :mid2})) {:action :added}))

         (is (enc/submap? (queue-status)
               {:mids-ready ["mid2" "mid1"]
                :mid-circle ["end-of-circle"]}))

         (is (mq/start   worker))
         (is (:running? @worker))

         (sleep 1000)
         (is (= @msgs_ [:msg1 :msg2]))
         (is (enc/submap? (queue-status)
               {:mids-ready []
                :mid-circle ["end-of-circle"]}))

         (is (mq/stop worker))]))))
