(ns taoensso.carmine.tests.message-queue
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc]
   [taoensso.carmine :as car  :refer [wcar]]
   [taoensso.carmine.message-queue :as mq]
   [taoensso.carmine.impl.mq :as impl]
   [taoensso.nippy.tools :as nippy-tools]
   [taoensso.carmine.tests.config :as config]))

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

(def conn-opts config/conn-opts)
(defmacro wcar* [& body] `(car/wcar conn-opts ~@body))

(def tq "carmine-test-queue")
(def client (#'mq/client conn-opts))
(defn clear-tq! [] (mq/queues-clear!! conn-opts [tq]))

(defn test-fixture [f] (f) (clear-tq!))
(test/use-fixtures :once test-fixture) ; Just for final teardown

(def ^:const default-lock-ms (enc/ms :mins 60))
(def ^:const eoq-backoff-ms 100)

(do
  (def enqueue    mq/enqueue)
  (def msg-status mq/message-status)

  (let [default-opts {:eoq-backoff-ms eoq-backoff-ms, :max-poll-steps 1}] ; Single-step polls
    (defn- dequeue
      ([qname]                  (dequeue qname nil (enc/uuid-str)))
      ([qname opts]             (dequeue qname opts (enc/uuid-str)))
      ([qname opts lease-token] (#'mq/send-req (impl/req-dequeue qname (conj default-opts opts) lease-token))))))

(defn sleep
  ([          n] (sleep nil n))
  ([isleep-on n]
   (let [n (int (case n :eoq (* 2.5 eoq-backoff-ms) n))]
     (if-let [on isleep-on]
       (impl/interruptible-sleep client tq n)
       (Thread/sleep                             n))

     (if-let [on isleep-on]
       (str "islept " n "msecs on " isleep-on)
       (str  "slept " n "msecs")))))

(defn wait-for
  ([pred] (wait-for 3000 pred))
  ([timeout-ms pred]
   (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
     (loop []
       (cond
         (pred) true
         (< (System/nanoTime) deadline)
         (do (Thread/sleep 10) (recur))
         :else false)))))

;;;;

(defn throw! [] (throw (Exception.)))
(defn handle-end-of-circle
  ([] (handle-end-of-circle eoq-backoff-ms))
  ([sleep-ms]
  (let [reply (wcar* (dequeue tq))]
    (every? identity
      [(is (= (subvec reply 0 2) ["sleep" "end-of-circle"]))
       (is (contains? #{"a" "b"} (get reply 2)))
       (is (= (get reply 3) sleep-ms))
       (is (subvec? (impl/handle1 client tq (fn hf [_] (throw!)) reply {})
             [:slept "end-of-circle" (get reply 2) #_eoq-backoff-ms]))]))))

;;;;

(deftest basics
  (testing "Basic enqueue & dequeue"
    (clear-tq!)
    [(let [reply (wcar* (dequeue tq))]
       (is (= (subvec reply 0 2) ["sleep" "end-of-circle"])))

     (is (= (wcar* (enqueue tq :msg1a {:mid :mid1}))                   {:success? true,  :action :added, :mid :mid1}))
     (is (= (wcar* (enqueue tq :msg1b {:mid :mid1}))                   {:success? false, :error :already-queued}) "Dupe mid")
     (is (= (wcar* (enqueue tq :msg1b {:mid :mid1 :can-update? true})) {:success? true,  :action :updated, :mid :mid1}))

     (is (= (wcar* (msg-status tq :mid1)) :queued))
     (is (enc/submap? (impl/queue-mids client tq)
           {:ready  ["mid1"]
            :circle ["end-of-circle"]}))

     (let [poll-reply (wcar* (dequeue tq))]
       [(is (= (count poll-reply) 6) "Preserves the public monitor reply shape")
        (is (subvec? poll-reply ["handle" "mid1" :msg1b 1 default-lock-ms #_udt]))])
     (is (=       (wcar* (msg-status tq :mid1)) :locked))
     (is (= (subvec (wcar* (dequeue tq)) 0 2) ["sleep" "end-of-circle"]))
     (is (contains? (mq/queue-names conn-opts) tq))]))

(deftest init-backoff
  (testing "Enqueue with initial backoff"
    (clear-tq!)
    [(is (= (subvec (wcar* (dequeue tq)) 0 2) ["sleep" "end-of-circle"]))
     (is (= (wcar* (enqueue tq :msg1 {:mid :mid1 :init-backoff-ms 500})) {:success? true, :action :added, :mid :mid1}))
     (is (= (wcar* (enqueue tq :msg2 {:mid :mid2 :init-backoff-ms 100})) {:success? true, :action :added, :mid :mid2}))

     (is (enc/submap? (impl/queue-mids client tq)
           {:ready  []
            :circle ["mid2" "mid1" "end-of-circle"]}))

     (is (enc/submap? (mq/queue-content conn-opts tq)
           {"mid1" {:message :msg1}
            "mid2" {:message :msg2}}))

     ;; Dupes before the backoff expired
     (is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? false, :error :already-queued}))
     (is (= (wcar* (enqueue tq :msg2 {:mid :mid2})) {:success? false, :error :already-queued}))

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
     (is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? false, :error :already-queued}))
     (is (= (wcar* (enqueue tq :msg2 {:mid :mid2})) {:success? false, :error :already-queued}))

     (is (= (wcar* (enqueue tq :msg2 {:mid :mid2 :init-backoff-ms 500 :reset-init-backoff? true}))
           {:success? true, :action :updated, :mid :mid2}) "Reset init backoff")

     (handle-end-of-circle 1) ; Capped by due hint from enqueue

     (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
     (is (= (wcar* (msg-status tq :mid1)) :locked))

     (is (subvec? (wcar* (dequeue tq)) ["skip" "queued-with-backoff"]))
     (is (= (wcar* (msg-status tq :mid2))      :queued-with-backoff))])

  (testing "Reschedule with `:reset-init-backoff?`"
    (clear-tq!)
    (let [del-signals! (fn [] (wcar* (car/del (impl/qkey tq :isleep-a) (impl/qkey tq :isleep-b))))
          nsignals
          (fn [] (reduce + (wcar* (car/llen (impl/qkey tq :isleep-a))
                                  (car/llen (impl/qkey tq :isleep-b)))))

          next-due   (fn [] (enc/as-?int (wcar* (car/get (impl/qkey tq :next-due)))))
          reschedule (fn [ms] (wcar* (enqueue tq :msg1 {:mid :mid1 :init-backoff-ms ms :reset-init-backoff? true})))
          updated    {:success? true, :action :updated, :mid :mid1}]

      [(is (= (subvec (wcar* (dequeue tq)) 0 2) ["sleep" "end-of-circle"]))
       (is (= (wcar* (enqueue tq :msg1 {:mid :mid1 :init-backoff-ms 60000})) {:success? true, :action :added, :mid :mid1}))
       (is (= (subvec (wcar* (dequeue tq)) 0 2) ["sleep" "end-of-circle"]))
       (is (subvec? (wcar* (dequeue tq)) ["skip" "queued-with-backoff"]) "Worker lap passes mid")
       (del-signals!)

       (let [due1 (next-due)]
         [(is (pos? due1))
          (is (= (reschedule 120000) updated) "Later")
          (is (= (nsignals) 0)    "No need to wake a sleeper")
          (is (= (next-due) due1) "Due hint never raised")

          (is (= (reschedule 30000) updated) "Sooner")
          (is (= (nsignals) 1)   "Woke a sleeper")
          (is (< (next-due) due1) "Due hint lowered")])

       (del-signals!)
       (is (= (reschedule 0) updated) "Now")
       (is (= (nsignals) 1))
       (is (= (wcar* (msg-status tq :mid1)) :queued))

       (is (= (wcar* (dequeue tq)) ["sleep" "end-of-circle" "a" 1]) "Lap ends, due hint prevents real sleep")
       (let [lease-token (enc/uuid-str)
             poll-reply  (wcar* (dequeue tq nil lease-token))]
         [(is (subvec? poll-reply ["handle" "mid1" :msg1 1]))
          (is (= (reschedule 60000) {:success? false, :error :locked}) "Only while awaiting handling")
          (is (subvec? (impl/handle1 client tq (fn [_] {:status :retry :backoff-ms 60000})
                         poll-reply {:lease-token lease-token})
                [:handled :retry]))])

       (is (= (wcar* (msg-status tq :mid1)) :queued-with-backoff))
       (del-signals!)
       (is (= (wcar* (enqueue tq :msg1b {:mid :mid1 :can-update? true :reset-init-backoff? true})) updated)
         "Reschedule a retry to now, with updated content")
       (is (= (nsignals) 1))
       (is (= (subvec (wcar* (dequeue tq)) 0 2) ["sleep" "end-of-circle"]))
       (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1b 2]) "Attempts kept")])))

(defn test-handler
  "Returns [<poll-reply> <handler-arg> <handle1-result>]"
  ([       hf] (test-handler false hf))
  ([async? hf]
   (let [lease-token  (enc/uuid-str)
         poll-reply   (wcar* (dequeue tq nil lease-token))
         handler-arg_ (promise)
         handle1
         (fn []
           (impl/handle1 client tq
             (fn [m] (deliver handler-arg_ m) (hf m))
             poll-reply {:lease-token lease-token}))

         handle1-result
         (if async?
           (future-call handle1)
           (do         (handle1)))]

     [poll-reply (deref handler-arg_ 5000 :timeout) handle1-result])))

(deftest handlers
  [(testing "Handler => success"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))

      (let [[pr ha hr] (test-handler (fn [_m] {:status :success}))]
        [(is (subvec?     pr ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
         (is (enc/submap? ha
               {:qname "carmine-test-queue", :mid "mid1", :message :msg1,
                :attempt 1, :lock-ms default-lock-ms}))
         (is (= hr [:handled :success]))])

      (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
      (handle-end-of-circle)
      (is (= (wcar* (dequeue    tq)) ["skip" "did-gc"]))
      (is (= (wcar* (msg-status tq :mid1)) nil))])

   (testing "Handler => throws"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))

      (let [[pr ha hr] (test-handler (fn [_m] (throw!)))]
        [(is (subvec? pr ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
         (is (=       hr [:handled :error]))])

      (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc ))
      (handle-end-of-circle)
      (is (= (wcar* (dequeue    tq)) ["skip" "did-gc"]))
      (is (= (wcar* (msg-status tq :mid1)) nil))])

   (testing "Handler => success with backoff (dedupe)"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))

      (let [[pr ha hr] (test-handler (fn [_m] {:status :success :backoff-ms 2000}))]
        [(is (subvec? pr ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
         (is (=       hr [:handled :success]))])

      (is (= (wcar* (msg-status tq :mid1)) :done-with-backoff))
      (handle-end-of-circle)
      (is (= (wcar* (dequeue tq)) ["skip" "done-with-backoff"]))

      (sleep 2500) ; > handler backoff
      (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
      (handle-end-of-circle 1) ; Skipped mid is now due

      (is (= (wcar* (dequeue tq)) ["skip" "did-gc"]))])

   (testing "Handler => retry with backoff"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))

      (let [[pr ha hr] (test-handler (fn [_m] {:status :retry :backoff-ms 2000}))]
        [(is (subvec? pr ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
         (is (=       hr [:handled :retry]))])

      (is (= (wcar* (msg-status tq :mid1)) :queued-with-backoff))
      (handle-end-of-circle)
      (is (= (wcar* (dequeue tq)) ["skip" "queued-with-backoff"]))

      (sleep 2500) ; > handler backoff
      (is (= (wcar* (msg-status tq :mid1)) :queued))
      (handle-end-of-circle 1) ; Skipped mid is now due

      (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1 2 default-lock-ms #_udt]))])

   (testing "Max attempts, error retry, give-up hook"
     (let [gave-up_ (atom [])
           ncalls_  (atom 0)
           on-give-up
           (fn [{:keys [mid attempt reason throwable]}]
             (swap! gave-up_ conj [mid attempt reason (some? throwable)]))

           handle ; Handles next message, returns [<attempt> <handle1-result>]
           (fn [hf opts]
             (let [lease-token (enc/uuid-str)
                   poll-reply
                   (loop [n 0]
                     (let [reply (wcar* (dequeue tq nil lease-token))]
                       (if (or (= (first reply) "handle") (>= n 8))
                         reply
                         (recur (inc n)))))]
               [(get poll-reply 3)
                (impl/handle1 client tq (fn [m] (swap! ncalls_ inc) (hf m)) poll-reply
                  (assoc opts :lease-token lease-token))]))

           reset!! (fn [] (clear-tq!) (reset! gave-up_ []) (reset! ncalls_ 0)
                     (wcar* (enqueue tq :msg1 {:mid :mid1})))]

       [(testing "Retries are limited by max attempts"
          (reset!!)
          (let [opts {:max-attempts 2, :on-give-up on-give-up}
                hf   (fn [_] {:status :retry})]
            [(is (= (handle hf opts) [1 [:handled :retry]]))
             (is (= (wcar* (msg-status tq :mid1)) :queued))
             (is (= @gave-up_ []))
             (is (= (handle hf opts) [2 [:handled :retry]]))
             (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
             (is (= @gave-up_ [["mid1" 2 :max-attempts false]]))]))

        (testing "Errors are final by default"
          (reset!!)
          [(is (= (handle (fn [_] (throw!)) {:max-attempts 2, :on-give-up on-give-up}) [1 [:handled :error]]))
           (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
           (is (= @gave-up_ [["mid1" 1 :error true]]))])

        (testing "Errors can be retried, with backoff"
          (reset!!)
          (let [opts {:max-attempts 2, :on-give-up on-give-up, :error-retry-backoff-ms 0}
                hf   (fn [_] (throw!))]
            [(is (= (handle hf opts) [1 [:handled :error]]))
             (is (= (wcar* (msg-status tq :mid1)) :queued))
             (is (= @gave-up_ []))
             (is (= (handle hf opts) [2 [:handled :error]]))
             (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
             (is (= @gave-up_ [["mid1" 2 :max-attempts true]]))])

          (reset!!)
          [(is (= (handle (fn [_] (throw!)) {:error-retry-backoff-ms (fn [attempt] (* 2000 (long attempt)))})
                 [1 [:handled :error]]))
           (is (= (wcar* (msg-status tq :mid1)) :queued-with-backoff))])

        (testing "Message is kept if give-up hook throws"
          (reset!!)
          (let [nstats_ (atom {})
                opts    {:max-attempts 1, :nstats_ nstats_}]
            [(is (= (handle (fn [_] {:status :retry}) (assoc opts :on-give-up (fn [_] (throw!))))
                   [1 [:handled :retry]]))
             (is (= (wcar* (msg-status tq :mid1)) :queued-with-backoff))
             (is (= (get @nstats_ :handler/gave-up) nil))
             (do (sleep 2100) true) ; > max first backoff
             (is (= (handle (fn [_] {:status :retry}) (assoc opts :on-give-up on-give-up))
                   [2 [:handled :gave-up]]) "Hook retried, without handler")
             (is (= @ncalls_ 1))
             (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
             (is (= @gave-up_ [["mid1" 2 :max-attempts false]]))
             (is (= (get @nstats_ :handler/gave-up) 1))]))

        (testing "Attempts ended by lock expiry count toward max attempts"
          (reset!!)
          [(is (subvec? (wcar* (dequeue tq {:default-lock-ms 1})) ["handle" "mid1" :msg1 1 1]))
           (do (sleep 20) true)
           (is (= (handle (fn [_] {:status :success}) {:max-attempts 1, :on-give-up on-give-up})
                 [2 [:handled :gave-up]]))
           (is (= @ncalls_ 0) "Handler not called")
           (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
           (is (= @gave-up_ [["mid1" 2 :max-attempts false]]))])]))

   (testing "Handler => lock timeout"

     (testing "Default lock time"
       (clear-tq!)
       [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))

        ;; Simulate bad handler
        (is (subvec? (wcar* (dequeue tq {:default-lock-ms 1000})) ["handle" "mid1" :msg1 1 1000 #_udt]))

        (is (= (wcar* (msg-status tq :mid1)) :locked))
        (handle-end-of-circle)

        (sleep 1500) ; Wait for lock to expire
        (is (subvec? (wcar* (dequeue tq {:default-lock-ms 1000})) ["handle" "mid1" :msg1 2 1000 #_udt]))])

     (testing "Custom lock time"
       (clear-tq!)
       [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1 :lock-ms 2000})) {:success? true, :action :added, :mid :mid1}))

        ;; Simulate bad handler
        (is (subvec? (wcar* (dequeue tq {:default-lock-ms 500})) ["handle" "mid1" :msg1 1 2000 #_udt]))

        (is (= (wcar* (msg-status tq :mid1)) :locked))
        (handle-end-of-circle)

        (sleep 2500) ; Wait for lock to expire
        (is (subvec? (wcar* (dequeue tq {:default-lock-ms 500})) ["handle" "mid1" :msg1 2 2000 #_udt]))]))])

(deftest requeue
  [(testing "Enqueue while :locked"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1a {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))

      (do (test-handler :async (fn [_m] (Thread/sleep 2000) {:status :success})) :async-handler-running)

      (is (= (wcar* (msg-status tq :mid1)) :locked))
      (is (= (wcar* (enqueue    tq :msg1b {:mid :mid1})) {:success? false, :error :locked}))

      (is (= (wcar* (enqueue    tq :msg1c {:mid :mid1, :can-requeue? true}))  {:success? true,  :action :added, :mid :mid1}))
      (is (= (wcar* (enqueue    tq :msg1d {:mid :mid1, :can-requeue? true}))  {:success? false, :error :already-queued}))
      (is (= (wcar* (enqueue    tq :msg1e {:mid :mid1, :can-requeue? true,
                                           :can-update? true, :lock-ms 500})) {:success? true,  :action :updated, :mid :mid1}))

      (is (= (wcar* (msg-status tq :mid1)) :locked-with-requeue))
      (sleep 2500) ; > handler lock
      (is (= (wcar* (msg-status tq :mid1)) :done-with-requeue) "Not :done-awaiting-gc")
      (handle-end-of-circle)

      (is (=       (wcar* (dequeue tq)) ["skip" "did-requeue"]))
      (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1e 1 500 #_udt]))])

   (testing "Enqueue while :done-with-backoff"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1a {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))

      (do (test-handler (fn [_m] {:status :success :backoff-ms 2000})) :ran-handler)

      (is (= (wcar* (msg-status tq :mid1)) :done-with-backoff))
      (is (= (wcar* (enqueue    tq :msg1b {:mid :mid1}))                     {:success? false, :error :backoff}))
      (is (= (wcar* (enqueue    tq :msg1c {:mid :mid1, :can-requeue? true,
                                           :lock-ms 500}))                   {:success? true,  :action :added, :mid :mid1}))
      (is (= (wcar* (msg-status tq :mid1)) :done-with-requeue))

      (handle-end-of-circle)
      (sleep 2500) ; > handler backoff

      (is (=       (wcar* (dequeue tq)) ["skip" "did-requeue"]))
      (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1c 1 500 #_udt]))])])

(deftest cancel
  [(testing "Nothing to cancel"
     (clear-tq!)
     [(is (= (wcar* (mq/cancel tq :mid1)) nil) "Not in queue")
      (is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))
      (do (test-handler (fn [_m] {:status :success :backoff-ms 2000})) :ran-handler)
      (is (= (wcar* (mq/cancel  tq :mid1)) nil) "Already done")
      (is (= (wcar* (msg-status tq :mid1)) :done-with-backoff) "Dedupe backoff kept")])

   (testing "Cancel while :queued"
     (clear-tq!)
     [(is (= (wcar* (enqueue    tq :msg1 {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))
      (is (= (wcar* (enqueue    tq :msg2 {:mid :mid2, :init-backoff-ms 5000})) {:success? true, :action :added, :mid :mid2}))
      (is (= (wcar* (mq/cancel  tq :mid1)) :queued))
      (is (= (wcar* (mq/cancel  tq :mid2)) :queued-with-backoff))
      (is (= (wcar* (mq/cancel  tq :mid1)) nil))
      (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
      (is (= (wcar* (msg-status tq :mid2)) :done-awaiting-gc))
      (is (= (wcar* (dequeue    tq)) ["skip" "did-gc"]))
      (is (= (wcar* (msg-status tq :mid1)) nil))
      (is (= (wcar* (dequeue    tq {:max-poll-steps 16})) ["sleep" "end-of-circle" "a" eoq-backoff-ms])
        "Neither message handled")
      (do (wcar* (dequeue tq {:max-poll-steps 16})) :next-circle-lap)
      (is (= (wcar* (msg-status tq :mid2)) nil))

      (is (= (wcar* (enqueue    tq :msg1b {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))
      (is (= (wcar* (mq/cancel  tq :mid1)) :queued))
      (is (= (wcar* (enqueue    tq :msg1c {:mid :mid1})) {:success? true, :action :added, :mid :mid1})
        "Can enqueue again right away")
      (is (= (wcar* (dequeue tq)) ["skip" "did-requeue"]))
      (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1c 1 default-lock-ms #_udt]))])

   (testing "Cancel while :locked"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))
      (let [release  (promise)
            extended (promise)
            [_ _ hr]
            (test-handler :async
              (fn [{:keys [lock-extend!]}]
                (deref release 5000 nil)
                (deliver extended (lock-extend!))
                {:status :retry}))]

        [(is (= (wcar* (msg-status tq :mid1)) :locked))
         (is (= (wcar* (enqueue    tq :msg1b {:mid :mid1, :can-requeue? true})) {:success? true, :action :added, :mid :mid1}))
         (is (= (wcar* (mq/cancel  tq :mid1)) :locked-with-requeue))
         (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
         (do (deliver release true) true)
         (is (= (deref extended 2000 :timeout) nil) "Handler can no longer extend")
         (is (= (deref hr       2000 :timeout) [:handled :retry]))
         (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc) "Handler result ignored")
         (is (= (wcar* (dequeue tq {:max-poll-steps 16})) ["sleep" "end-of-circle" "a" eoq-backoff-ms]))
         (do (wcar* (dequeue tq {:max-poll-steps 16})) :next-circle-lap)
         (is (= (wcar* (msg-status tq :mid1)) nil))])])

   (testing "Cancelled message isn't given up on"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))
      (let [gave-up?_   (atom false)
            lease-token (enc/uuid-str)
            poll-reply  (wcar* (dequeue tq nil lease-token))]
        [(is (= (impl/handle1 client tq
                  (fn [_] (wcar* (mq/cancel tq :mid1)) (throw!)) poll-reply
                  {:lease-token lease-token, :on-give-up (fn [_] (reset! gave-up?_ true))})
               [:handled :error]))
         (is (= @gave-up?_ false))
         (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))])])

   (testing "Cancel requeue while :done-with-backoff"
     (clear-tq!)
     [(is (= (wcar* (enqueue tq :msg1a {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))
      (do (test-handler (fn [_m] {:status :success :backoff-ms 2000})) :ran-handler)
      (is (= (wcar* (enqueue    tq :msg1b {:mid :mid1, :can-requeue? true})) {:success? true, :action :added, :mid :mid1}))
      (is (= (wcar* (msg-status tq :mid1)) :done-with-requeue))
      (is (= (wcar* (mq/cancel  tq :mid1)) :done-with-requeue))
      (is (= (wcar* (msg-status tq :mid1)) :done-with-backoff))
      (is (= (wcar* (mq/cancel  tq :mid1)) nil))])])

(deftest multi-step-polls
  (testing "Poll can do several maintenance steps"
    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    (wcar* (dequeue tq nil "owner"))
    [(is (= (impl/finalize client tq "mid1" "owner" true nil) ["finalized"]))
     (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
     (is (= (subvec (wcar* (dequeue tq {:max-poll-steps 16})) 0 2) ["sleep" "end-of-circle"]))
     (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
     (is (= (subvec (wcar* (dequeue tq {:max-poll-steps 16})) 0 2) ["sleep" "end-of-circle"])
       "GC, then end of circle")
     (is (= (wcar* (msg-status tq :mid1)) nil))])

  (testing "Ready mids can't starve circle maintenance"
    (doseq [max-steps [1 16]]
      (clear-tq!)
      (wcar* (enqueue tq :msg1 {:mid :mid1}))
      (wcar* (dequeue tq nil "owner"))
      (impl/finalize client tq "mid1" "owner" true nil)
      (wcar* (enqueue tq :msg2 {:mid :mid2}))
      (is (subvec? (wcar* (dequeue tq {:max-poll-steps max-steps})) ["handle" "mid2"]))
      (is (= (wcar* (msg-status tq :mid1))
            (if (= max-steps 1) :done-awaiting-gc nil))
        "Single-step poll takes ready mid without touching circle")))

  (testing "Retries in circle and ready mids can't starve each other"
    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    (wcar* (dequeue tq nil "owner"))
    (impl/finalize client tq "mid1" "owner" false nil) ; Retry without backoff
    (wcar* (enqueue tq :msg2 {:mid :mid2}))
    [(is (subvec? (wcar* (dequeue tq {:max-poll-steps 16})) ["handle" "mid2" :msg2 1]))
     (is (= (:ready (impl/queue-mids client tq)) ["mid1"]) "Retry waits in turn behind ready mids")
     (is (subvec? (wcar* (dequeue tq {:max-poll-steps 16})) ["handle" "mid1" :msg1 2]))
     (is (= (subvec (wcar* (dequeue tq {:max-poll-steps 16})) 0 2) ["sleep" "end-of-circle"]))])

  (testing "Handling a mid wakes another sleeper iff more mids are ready"
    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    (wcar* (enqueue tq :msg2 {:mid :mid2}))
    (wcar* (enqueue tq :msg3 {:mid :mid3}))
    (let [nsignals
          (fn [] (reduce + (wcar* (car/llen (impl/qkey tq :isleep-a))
                                  (car/llen (impl/qkey tq :isleep-b)))))]
      (wcar* (car/del (impl/qkey tq :isleep-a) (impl/qkey tq :isleep-b))) ; Discard enqueue signals
      [(is (subvec? (wcar* (dequeue tq)) ["handle" "mid1"]))
       (is (= (nsignals) 1) "More mids ready")
       (is (subvec? (wcar* (dequeue tq)) ["handle" "mid2"]))
       (is (= (nsignals) 1) "More mids ready, but at most one persistent signal")
       (is (subvec? (wcar* (dequeue tq)) ["handle" "mid3"]))
       (is (= (nsignals) 1) "No more mids ready, existing signal untouched")
       (is (= (subvec (wcar* (dequeue tq)) 0 2) ["sleep" "end-of-circle"]))
       (is (= (nsignals) 0))

       (do (wcar* (enqueue tq :msg4 {:mid :mid4}))
           (wcar* (car/del (impl/qkey tq :isleep-a) (impl/qkey tq :isleep-b)))
           (is (subvec? (wcar* (dequeue tq)) ["handle" "mid4"])))
       (is (= (nsignals) 0) "No more mids ready")])))

(deftest end-of-circle-sleep-cap
  (testing "End-of-circle sleep is capped by the earliest skipped backoff or lock expiry"
    (let [eoq-ms 60000
          poll!  (fn [] (wcar* (dequeue tq {:eoq-backoff-ms eoq-ms, :max-poll-steps 16})))]

      (clear-tq!)
      (wcar* (enqueue tq :msg1 {:mid :mid1 :init-backoff-ms 4000}))
      (wcar* (enqueue tq :msg2 {:mid :mid2 :init-backoff-ms 2000}))
      [(let [[kind reason _ ms] (poll!)]
         [(is (= [kind reason] ["sleep" "end-of-circle"]))
          (is (<= 1 ms 2000) "Nothing skipped yet, capped by enqueue")])
       (let [[kind reason _ ms] (poll!)]
         [(is (= [kind reason] ["sleep" "end-of-circle"]))
          (is (<= 1 ms 2000) "Capped by earliest backoff")])]

      (clear-tq!)
      (wcar* (enqueue tq :msg1 {:mid :mid1}))
      [(is (subvec? (wcar* (dequeue tq {:default-lock-ms 2000})) ["handle" "mid1"]))
       (is (= (poll!) ["sleep" "end-of-circle" "a" eoq-ms]))
       (let [[_ _ _ ms] (poll!)] (is (<= 1 ms 2000) "Capped by lock expiry"))])))

(deftest workers
  (testing "Basic worker functionality"
    (clear-tq!)
    (let [msgs_ (atom [])
          handler-fn
          (fn [{:keys [mid message] :as in}]
            (swap! msgs_ conj message)
            {:status :success})]

      (with-open [^java.io.Closeable worker
                  (mq/worker conn-opts tq
                    {:auto-start false,
                     :handler handler-fn
                     :throttle-ms    10
                     :eoq-backoff-ms 10})]

        [(is (enc/submap? (wcar* (enqueue tq :msg1 {:mid :mid1})) {:success? true, :action :added}))
         (is (enc/submap? (wcar* (enqueue tq :msg2 {:mid :mid2})) {:success? true, :action :added}))

         (is (enc/submap? (worker :queue-mids)
               {:ready  ["mid2" "mid1"]
                :circle ["end-of-circle"]}))

         (is (mq/start   worker))
         (is (:running? @worker))

         (sleep 1000)
         (is (= @msgs_ [:msg1 :msg2]))
         (is (enc/submap? (worker :queue-mids)
               {:ready  []
                :circle ["end-of-circle"]}))

         (is (mq/stop worker))]))))

(deftest server-time
  (testing "`server-udt` converts Redis `TIME` replies to epoch msecs"
    [(is (= (impl/server-udt ["1784523705" "818999"]) 1784523705818) "Micros floored to msecs")
     (is (= (impl/server-udt ["1784523705"      "0"]) 1784523705000))
     (is (= (impl/server-udt ["1784523705"    "999"]) 1784523705000) "Sub-msec micros discarded")])

  (testing "Handler `:age-ms` is relative to the Redis server clock"
    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    (let [lease-token (enc/uuid-str)
          poll-reply  (wcar* (dequeue tq nil lease-token))
          udt         (long (get poll-reply 5))
          age-ms_     (promise)]
      (impl/handle1 client tq (fn [m] (deliver age-ms_ (:age-ms m)) {:status :success}) poll-reply
        {:lease-token lease-token, :server-udt (+ udt 5000)})
      (is (= (deref age-ms_ 0 nil) 5000)))

    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    (let [lease-token (enc/uuid-str)
          poll-reply  (wcar* (dequeue tq nil lease-token))
          udt         (long (get poll-reply 5))
          ages_       (atom [])
          hf          (fn [m] (swap! ages_ conj (:age-ms m)) {:status :retry})]
      (impl/handle1 client tq hf poll-reply {:lease-token lease-token, :server-udt (- udt 5000)})
      (is (= @ages_ [0]) "Never negative"))

    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    (let [lease-token (enc/uuid-str)
          poll-reply  (wcar* (dequeue tq nil lease-token))
          age-ms_     (promise)]
      (impl/handle1 client tq (fn [m] (deliver age-ms_ (:age-ms m)) {:status :success}) poll-reply
        {:lease-token lease-token})
      (is (enc/nat-int? (deref age-ms_ 0 nil)) "Falls back to local clock")))

  (testing "Queue scripts take their time from the Redis server"
    (mapv
      (fn [sname]
        (let [src (enc/slurp-resource (str "taoensso/carmine/lua/mq/" sname ".lua"))]
          [(is (not (enc/str-contains? src "_:now"))
             (str sname ".lua shouldn't accept a client-supplied `now`"))
           (is (enc/str-contains? src "redis.call('TIME')")
             (str sname ".lua should read the server clock"))]))
      ["msg-status" "enqueue" "cancel" "dequeue" "finalize" "extend-lock"]))

  (testing "Write scripts request effects replication before their first write"
    (mapv
      (fn [sname]
        (let [^String src  (enc/slurp-resource (str "taoensso/carmine/lua/mq/" sname ".lua"))
              i-replicate  (.indexOf src "redis.replicate_commands()")
              i-first-call (.indexOf src "redis.call(")]
          (is (and (not= i-replicate -1) (not= i-first-call -1)
                (< i-replicate i-first-call))
            (str sname ".lua should call `redis.replicate_commands()` before its first `redis.call`"))))
      ["enqueue" "cancel" "dequeue" "finalize" "extend-lock"]))

  (testing "Handler backoffs are stored as integer epoch msecs"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg1 {:mid :mid1}))))
    (let [[_pr _ha hr] (test-handler (fn [_m] {:status :retry :backoff-ms 2000.9}))
          stored       (second (wcar* (car/hgetall (impl/qkey tq :backoffs))))]
      [(is (= hr [:handled :retry]))
       (is (re-matches #"\d+" (str stored))
         "Fractional `:backoff-ms` shouldn't produce a fractional expiry")])))

(deftest hardening
  (testing "End-of-circle sleep blocks and enqueue wakes it"
    (clear-tq!)
    (let [reply (wcar* (#'mq/send-req (impl/req-dequeue tq {:eoq-backoff-ms 10000} (enc/uuid-str))))
          started (promise)
          sleeper (future
                    (deliver started true)
                    (impl/handle1 client tq identity reply {}))]
      [(is (= (deref started 5000 :timeout) true))
       (Thread/sleep 75)
       (is (not (realized? sleeper)))
       (is (:success? (wcar* (enqueue tq :wake {:mid :wake}))))
       (is (subvec? (deref sleeper 5000 :timeout) [:slept "end-of-circle"]))]))

  (testing "Repeated pre-block enqueues cannot cancel the wake signal"
    (clear-tq!)
    (let [reply (wcar* (#'mq/send-req (impl/req-dequeue tq {:eoq-backoff-ms 1000} (enc/uuid-str))))
          enqueue-replies
          (wcar*
            (enqueue tq :wake-a {:mid :wake-a})
            (enqueue tq :wake-b {:mid :wake-b}))
          sleep-reply
          (impl/interruptible-sleep client tq 1000)]
      [(is (every? :success? enqueue-replies))
       (is (= (peek sleep-reply) "_")
         "BRPOP consumes a notification sent before the blocking call")
       (is (enc/submap? (impl/queue-mids client tq)
             {:ready ["wake-b" "wake-a"]
              :circle ["end-of-circle"]}))]))

  (testing "A stale handler cannot finalize a successor's lease"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 200}))))
    (let [token-a "lease-a"
          lease-a (wcar* (dequeue tq nil token-a))]
      (Thread/sleep 250)
      (wcar* (dequeue tq)) ; Rotate the end-of-circle marker.
      (let [token-b "lease-b"
            lease-b (wcar* (dequeue tq nil token-b))
            started (promise)
            release (promise)
            handler-b
            (future
              (impl/handle1 client tq
                (fn [{:keys [lock-extend!]}]
                  (deliver started (lock-extend!))
                  (deref release 3000 nil)
                  {:status :success})
                lease-b {:lease-token token-b}))]
        [(is (= (subvec lease-a 0 5) ["handle" "mid1" :msg 1 200]))
         (is (= (subvec lease-b 0 5) ["handle" "mid1" :msg 2 200]))
         (is (= (deref started 5000 :timeout) true))
         (is (= (impl/handle1 client tq (constantly {:status :success}) lease-a
                  {:lease-token token-a})
               [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :locked))
         (deliver release true)
         (is (= (deref handler-b 5000 :timeout) [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
         (is (nil? (wcar* (car/hget (car/key :carmine :mq tq :lock-tokens) :mid1))))])))

  (testing "Fenced results are counted separately from completed handlings"
    (mapv
      (fn [[result expected-k]]
        (clear-tq!)
        (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 200}))))
        (let [lease-a  (wcar* (dequeue tq nil "token-a"))
              fenced_  (atom {})
              normal_  (atom {})]
          (Thread/sleep 250)                 ; Expire lease-a
          (wcar* (dequeue tq))               ; Rotate the end-of-circle marker
          (wcar* (dequeue tq nil "token-b")) ; Supersede lease-a's token

          ;; Stale token => fenced, no status/backoff counts
          (impl/handle1 client tq (constantly result) lease-a
            {:lease-token "token-a", :nstats_ fenced_})

          ;; Current token => normal accounting
          (impl/handle1 client tq (constantly result) lease-a
            {:lease-token "token-b", :nstats_ normal_})

          [(is (= (:handler/fenced @fenced_) 1)
             (str "Fenced: " (pr-str result)))
           (is (= (dissoc @fenced_ :handler/fenced) {})
             (str "A fenced result shouldn't be counted any other way: "
               (pr-str result)))
           (is (= (get @normal_ expected-k) 1)
             (str "Non-fenced should count " expected-k ": " (pr-str result)))
           (is (nil? (:handler/fenced @normal_)))
           (is (= (boolean (:handler/backoff @normal_))
                 (boolean (:backoff-ms result)))
             (str ":handler/backoff should track :backoff-ms: " (pr-str result)))]))

      [[{:status :success}                    :handler/success]
       [{:status :retry, :backoff-ms 1000}    :handler/retry]
       [{:status :error}                      :handler/error]
       [{:status :bogus}                      :handler/unexpected]
       [{:status nil}                         :handler/unexpected]
       [{:status 1}                           :handler/unexpected]]))

  (testing "Queue clear and same-ID re-enqueue reject old finalization"
    (clear-tq!)
    (wcar* (enqueue tq :old {:mid :mid1 :lock-ms 1000}))
    (let [token-a "before-clear"
          lease-a (wcar* (dequeue tq nil token-a))]
      (clear-tq!)
      (wcar* (enqueue tq :new {:mid :mid1 :lock-ms 1000}))
      (let [token-b "after-clear"
            lease-b (wcar* (dequeue tq nil token-b))]
        [(is (= (impl/handle1 client tq (constantly {:status :success}) lease-a
                  {:lease-token token-a})
               [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :locked))
         (is (= (impl/handle1 client tq (constantly {:status :success}) lease-b
                  {:lease-token token-b})
               [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))])))

  (testing "Renewal advances expiry and never shortens it"
    (clear-tq!)
    (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 5000}))
    (let [lease-token "extendable"
          _ (wcar* (dequeue tq nil lease-token))
          expiry #(Long/parseLong
                    (wcar* (car/hget (car/key :carmine :mq tq :locks) :mid1)))
          original (expiry)
          extended (impl/extend-lock client tq "mid1" lease-token 10000)]
      (is (> extended original))
      (is (= extended (expiry)))
      (is (= extended (impl/extend-lock client tq "mid1" lease-token 1)))
      (is (= extended (expiry)))))

  (testing "A stale token cannot renew a successor's lease"
    (clear-tq!)
    (wcar* (enqueue tq :msg {:mid :mid1}))
    (wcar* (dequeue tq nil "old"))
    ;; Expire deterministically using the stored server-clock timestamp.
    (wcar* (car/hset (car/key :carmine :mq tq :locks) :mid1 0))
    (wcar* (dequeue tq)) ; End-of-circle
    (wcar* (dequeue tq nil "new"))
    (let [expiry (wcar* (car/hget (car/key :carmine :mq tq :locks) :mid1))]
      (is (nil? (impl/extend-lock client tq "mid1" "old" 10000000)))
      (is (= expiry (wcar* (car/hget (car/key :carmine :mq tq :locks) :mid1))))))

  (testing "Expired but unsuperseded ownership can renew or finalize"
    (doseq [renew? [false true]]
      (clear-tq!)
      (wcar* (enqueue tq :msg {:mid :mid1}))
      (wcar* (dequeue tq nil "owner"))
      (wcar* (car/hset (car/key :carmine :mq tq :locks) :mid1 0))
      (when renew?
        (is (number? (impl/extend-lock client tq "mid1" "owner" 5000)))
        (is (= (wcar* (msg-status tq :mid1)) :locked)))
      (is (= (impl/finalize client tq "mid1" "owner" true nil) ["finalized"]))
      (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))))

  (testing "Finalization is fenced by token only: own extensions can't fence the owner"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 250}))))
    (let [lease-token "extended"
          lease (wcar* (dequeue tq nil lease-token))]
      ;; Extend server-side as if the handler's extend reply were lost: the
      ;; stored expiry no longer matches the expiry known at lease time.
      [(is (= (subvec lease 0 3) ["handle" "mid1" :msg]))
       (is (impl/extend-lock client tq "mid1" lease-token 5000))
       (is (= (impl/handle1 client tq (constantly {:status :success}) lease
                {:lease-token lease-token})
             [:handled :success]))
       (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc)
         "Owner finalized successfully despite the extended (mismatched) expiry")]))

  (testing "Producer update of an expired lease fences the stalled handler"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :old {:mid :mid1 :lock-ms 100}))))
    (let [lease-token "before-update"
          lease-a (wcar* (dequeue tq nil lease-token))]
      (Thread/sleep 150) ; Expire lease-a
      [(is (= (subvec lease-a 0 3) ["handle" "mid1" :old]))
       (is (= (wcar* (enqueue tq :new {:mid :mid1 :can-update? true}))
             {:success? true, :action :updated, :mid :mid1}))
       ;; The stalled handler completes, but must be fenced:
       (is (= (impl/handle1 client tq (constantly {:status :success}) lease-a
                {:lease-token lease-token})
             [:handled :success]))
       (is (= (wcar* (msg-status tq :mid1)) :queued)
         "Replacement payload survives the stale finalization")
       ;; And the replacement payload is actually delivered:
       (is (= (subvec
                (loop [n 0]
                  (let [reply (wcar* (dequeue tq))]
                    (if (and (vector? reply) (= (get reply 0) "handle"))
                      reply
                      (if (< n 5) (recur (inc n)) reply))))
                0 3)
             ["handle" "mid1" :new]))]))

  (testing "Producer backoff reset of an expired lease fences the stalled handler"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 100}))))
    (let [lease-token "before-backoff-reset"
          lease-a (wcar* (dequeue tq nil lease-token))]
      (Thread/sleep 150) ; Expire lease-a
      [(is (= (subvec lease-a 0 3) ["handle" "mid1" :msg]))
       (is (= (wcar* (enqueue tq :msg {:mid :mid1 :reset-init-backoff? true}))
             {:success? true, :action :updated, :mid :mid1}))
       (is (= (impl/handle1 client tq (constantly {:status :success}) lease-a
                {:lease-token lease-token})
             [:handled :success]))
       (is (= (wcar* (msg-status tq :mid1)) :queued)
         "Re-asserted message survives the stale finalization")])))

(deftest worker-hardening
  (testing "Invalid `:max-attempts` is rejected"
    (is (thrown? Exception (mq/worker conn-opts tq {:max-attempts 0, :auto-start false}))))

  (testing "Monitor retains the six-element handle reply"
    (clear-tq!)
    (let [handled (promise)
          monitored (promise)]
      (with-open [^java.io.Closeable worker
                  (mq/worker conn-opts tq
                    {:auto-start false
                     :handler (fn [_] (deliver handled true) {:status :success})
                     :monitor
                     (fn [{:keys [poll-reply]}]
                       (when (= (get poll-reply 0) "handle")
                         (deliver monitored poll-reply)))
                     :throttle-ms 1, :eoq-backoff-ms 20})]
        (wcar* (enqueue tq :msg {:mid :mid1}))
        (mq/start worker)
        (let [poll-reply (deref monitored 2000 :timeout)]
          [(is (= (count poll-reply) 6))
           (is (subvec? poll-reply ["handle" "mid1" :msg 1 default-lock-ms]))
           (is (= (deref handled 2000 :timeout) true))]))))

  (testing "Handler capacity is reserved before a Redis lease"
    (clear-tq!)
    (let [started (promise)
          release (promise)
          handled_ (atom [])
          handler
          (fn [{:keys [message]}]
            (deliver started true)
            (deref release 3000 nil)
            (swap! handled_ conj message)
            {:status :success})]
      (with-open [^java.io.Closeable worker
                  (mq/worker conn-opts tq
                    {:auto-start false, :handler handler
                     :nthreads-worker 2, :nthreads-handler 1
                     :throttle-ms 1, :eoq-backoff-ms 20})]
        (wcar* (enqueue tq :a {:mid :a}) (enqueue tq :b {:mid :b}))
        (mq/start worker)
        [(is (= (deref started 2000 :timeout) true))
         (Thread/sleep 100)
         (is (= (set [(wcar* (msg-status tq :a)) (wcar* (msg-status tq :b))])
               #{:locked :queued}))
         (deliver release true)
         (is (wait-for #(= (count @handled_) 2)))
         (is (= (set @handled_) #{:a :b}))])))

  (testing "Handler threads poll for themselves, independently of `:nthreads-worker`"
    (clear-tq!)
    (let [nhandling* (enc/counter 0)
          release (promise)]
      (with-open [^java.io.Closeable worker
                  (mq/worker conn-opts tq
                    {:auto-start false
                     :handler (fn [_] (nhandling*) (deref release 5000 nil) {:status :success})
                     :nthreads-worker 1, :nthreads-handler 3
                     :throttle-ms 10000, :eoq-backoff-ms 20})]
        (wcar* (enqueue tq :a) (enqueue tq :b) (enqueue tq :c))
        (mq/start worker)
        [(is (wait-for #(= @nhandling* 3)) "Not limited to 1 poll per throttle period")
         (do (deliver release true) (mq/stop worker) (is (mq/drain worker 5000)))])))

  (testing "Automatic lock extension"
    (doseq [[max-ms expected-attempts]
            [[5000 [1]]     ; Lease kept throughout slow handling
             [250  [1 2]]]] ; Extended, then left to expire once max time exceeded
      (clear-tq!)
      (let [attempts_ (atom [])
            started   (promise)
            release   (promise)]
        (with-open [^java.io.Closeable worker
                    (mq/worker conn-opts tq
                      {:handler
                       (fn [{:keys [attempt]}]
                         (swap! attempts_ conj attempt)
                         (when (= attempt 1)
                           (deliver started true)
                           (deref release 5000 nil))
                         {:status :success})
                       :lock-ms 300, :lock-auto-extend-max-ms max-ms
                       :nthreads-handler 2, :throttle-ms 10, :eoq-backoff-ms 20})]
          (wcar* (enqueue tq :msg {:mid :mid1}))
          (is (= (deref started 2000 :timeout) true))
          (if (= expected-attempts [1])
            (do (Thread/sleep 1200) ; 4x lock-ms
                (is (= @attempts_ [1])))
            (is (wait-for #(= @attempts_ expected-attempts))))
          (deliver release true)))))

  (testing "Monitor failures do not abandon leased messages"
    (clear-tq!)
    (let [handled (promise)]
      (with-open [^java.io.Closeable worker
                  (mq/worker conn-opts tq
                    {:handler (fn [_] (deliver handled true) {:status :success})
                     :monitor (fn [_] (throw (Exception. "monitor")))
                     :throttle-ms 1, :eoq-backoff-ms 20})]
        [(is (:success? (wcar* (enqueue tq :msg {:mid :mid1}))))
         (is (= (deref handled 2000 :timeout) true))
         (is (wait-for #(= (wcar* (msg-status tq :mid1)) :done-awaiting-gc)))])))

  (testing "A handler can stop its own worker without deadlocking"
    (clear-tq!)
    (let [worker_ (atom nil)
          stopped (promise)
          worker
          (mq/worker conn-opts tq
            {:auto-start false
             :handler
             (fn [_]
               (deliver stopped (mq/stop @worker_))
               {:status :success})
             :throttle-ms 1, :eoq-backoff-ms 20})]
      (reset! worker_ worker)
      (try
        (wcar* (enqueue tq :msg {:mid :mid1}))
        (mq/start worker)
        (is (= (deref stopped 2000 :timeout) true))
        (is (mq/drain worker))
        (finally (.close ^java.io.Closeable worker)))))

  (testing "Drain wakes every sleeping handler thread"
    (clear-tq!)
    (let [opts   {:auto-start false, :handler (fn [_] {:status :success})
                  :nthreads-worker 2, :nthreads-handler 2
                  :throttle-ms 1, :eoq-backoff-ms 10000}
          slept? (fn [w n]
                   (>= (long (or (get-in @w [:stats :counts :sleep/end-of-circle]) 0))
                     (long n)))
          worker     (mq/worker conn-opts tq opts)
          ;; Competes for the queue-global wake sentinels throughout the drain
          competitor (mq/worker conn-opts tq opts)]
      (try
        (mq/start worker)
        (mq/start competitor)
        (is (wait-for 5000 #(and (slept? worker 2) (slept? competitor 2)))
          "Both handler threads of both workers should reach their eoq sleep")
        (is (mq/stop worker))
        (let [t0      (System/currentTimeMillis)
              drained (worker :drain)
              elapsed (- (System/currentTimeMillis) t0)]
          [(is (true? drained))
           ;; eoq backoff is 10s (desynced >= 8s), so anything well under that
           ;; proves the sleepers were woken rather than timing out
           (is (< elapsed 5000)
             (str "Drain should wake its own sleepers despite a competing "
               "worker on the same queue (took " elapsed "ms)"))])
        (finally
          (.close ^java.io.Closeable worker)
          (.close ^java.io.Closeable competitor)))))

  (testing "Close prevents a delayed auto-start"
    (let [worker (mq/worker conn-opts tq {:auto-start 100})]
      (.close ^java.io.Closeable worker)
      (Thread/sleep 150)
      [(is (:closed? @worker))
       (is (not (:running? @worker)))
       (is (nil? (mq/start worker)))])))

(deftest worker-lifecycle-races
  (testing "Restart cannot interleave with stop's generation transition"
    (clear-tq!)
    (let [handled (promise)
          worker (mq/worker conn-opts tq
                   {:auto-start false, :nthreads 1, :throttle-ms 1, :eoq-backoff-ms 20
                    :handler (fn [_] (deliver handled true) {:status :success})})
          running (:running?_ (.-state ^taoensso.carmine.message_queue.CarmineMessageQueueWorker worker))
          paused (promise), release (promise), restarting (promise)]
      (try
        (mq/start worker)
        (add-watch running ::pause-stop
          (fn [_ _ old new]
            (when (and old (not new))
              (deliver paused true)
              (deref release 5000 nil))))
        (let [stopping (future (mq/stop worker))]
          (is (= true (deref paused 2000 :timeout)))
          (let [starting (future (deliver restarting true) (mq/start worker))]
            (is (= true (deref restarting 2000 :timeout)))
            (is (= :blocked (deref starting 100 :blocked)))
            (deliver release true)
            (is (= true (deref stopping 2000 :timeout)))
            (is (= true (deref starting 2000 :timeout)))
            (wcar* (enqueue tq :msg {:mid :mid1}))
            (is (= true (deref handled 2000 :timeout)))))
        (finally
          (deliver release true)
          (remove-watch running ::pause-stop)
          (.close ^java.io.Closeable worker)
          (worker :drain)))))

  (testing "Close waits for an in-flight start to publish its futures"
    (clear-tq!)
    (let [worker (mq/worker conn-opts tq {:auto-start false, :nthreads 1
                                        :throttle-ms 1, :eoq-backoff-ms 20})
          running (:running?_ (.-state ^taoensso.carmine.message_queue.CarmineMessageQueueWorker worker))
          paused (promise), release (promise), closing (promise)]
      (try
        (add-watch running ::pause-start
          (fn [_ _ old new]
            (when (and (not old) new)
              (deliver paused true)
              (deref release 5000 nil))))
        (let [starting (future (mq/start worker))]
          (is (= true (deref paused 2000 :timeout)))
          (let [closer (future (deliver closing true) (.close ^java.io.Closeable worker) :closed)]
            (is (= true (deref closing 2000 :timeout)))
            (is (= :blocked (deref closer 100 :blocked)))
            (deliver release true)
            (is (= true (deref starting 2000 :timeout)))
            (is (= :closed (deref closer 2000 :timeout)))
            (is (:closed? @worker))
            (is (false? (:running? @worker)))
            (is (nil? (mq/start worker)))))
        (finally
          (deliver release true)
          (remove-watch running ::pause-start)
          (.close ^java.io.Closeable worker)
          (worker :drain))))))

(defn- close-and-drain! [w]
  (.close ^java.io.Closeable w)
  (is (true? (w :drain 5000)) "Worker drained during cleanup"))

(deftest bounded-drain
  [(testing "Bounded drain"
     (clear-tq!)
     (let [started (promise), release (promise), done_ (atom false)
           w (mq/worker conn-opts tq
               {:handler (fn [_] (deliver started true) (deref release 5000 nil)
                           (Thread/sleep 200) (reset! done_ true) {:status :success})
                :throttle-ms 1, :eoq-backoff-ms 20})]
       (try
         [(is (:success? (wcar* (mq/enqueue tq :msg))))
          (is (= (deref started 3000 :timeout) true))
          (is (false? (w :drain 50)) "Running")
          (is (true?  (mq/stop w)))
          (is (false? (w :drain 0)) "Zero timeout")
          (let [t0 (System/currentTimeMillis)]
            [(is (false? (w :drain 300)) "Timed out")
             (is (< (- (System/currentTimeMillis) t0) 1000) "Returned on time")])
          (do (deliver release true) :released)
          (is (true? (w :drain 5000)))
          (is (true? @done_))]
         (finally (deliver release true) (close-and-drain! w)))))

   (testing "Drain returns false if worker restarts meanwhile"
     (clear-tq!)
     (let [started (promise), release (promise)
           w (mq/worker conn-opts tq
               {:handler (fn [_] (deliver started true) (deref release 5000 nil) {:status :success})
                :throttle-ms 1, :eoq-backoff-ms 20})]
       (try
         [(is (:success? (wcar* (mq/enqueue tq :msg))))
          (is (= (deref started 3000 :timeout) true))
          (is (true? (mq/stop w)))
          (let [drain (future (w :drain))]
            [(is (= (deref drain 200 :waiting) :waiting) "Drain is waiting on the active handler")
             (is (true? (mq/start w)))
             (is (false? (deref drain 3000 :timeout)) "Returns while handler still active")])]
         (finally (deliver release true) (close-and-drain! w)))))])

(deftest deprecated-make-dequeue-worker
  (clear-tq!)
  (let [handled (promise)
        w (#'mq/make-dequeue-worker (:pool conn-opts) (:spec conn-opts) tq
            :handler-fn (fn [msg] (deliver handled msg) :success)
            :throttle-msecs 10, :backoff-msecs 20)]
    (try
      [(is (:running? @w) "Auto-starts by default")
       (is (:success? (wcar* (mq/enqueue tq :msg))))
       (is (= (deref handled 5000 :timeout) :msg))]
      (finally (close-and-drain! w)))))

(deftest stop-responsiveness
  (testing "Stop doesn't wait for long worker sleeps (throttle, error backoff)"
    (clear-tq!)
    (let [worker (mq/worker conn-opts tq
                   {:throttle-ms 5000 ; Throttle + error backoff factor
                    :eoq-backoff-ms (fn [_] (throw (Exception. "Test worker error")))})]
      (try
        (Thread/sleep 500) ; First error => 5s+ sleep
        (let [t0 (System/currentTimeMillis)]
          [(is (true? (mq/stop worker)))
           (is (< (- (System/currentTimeMillis) t0) 1000))])
        (finally (close-and-drain! worker))))))

(deftest unthrottled-workers
  (doseq [throttle-ms [0 nil]]
    (testing (str "Worker with `:throttle-ms " (pr-str throttle-ms) "`")
      (clear-tq!)
      (let [handled_ (promise)
            worker (mq/worker conn-opts tq
                     {:throttle-ms throttle-ms, :eoq-backoff-ms 20
                      :handler (fn [{:keys [message]}] (deliver handled_ message) {:status :success})})]
        (try
          (wcar* (mq/enqueue tq :msg1))
          [(is (= (deref handled_ 5000 :timeout) :msg1))
           (is (:running? @worker))
           (is (true? (mq/stop worker)))]
          (finally (close-and-drain! worker)))))))

(deftest custom-nippy-opts
  (let [nopts {:password [:salted "test-password"]}
        creation-opts {:password [:salted "unused-creation-password"]}
        messages #(set (map :message (vals %)))
        w (car/with-thaw-opts creation-opts
            (mq/worker conn-opts tq {:auto-start false}))]
    (clear-tq!)
    (try
      (wcar* (enqueue tq (car/freeze :explicit nopts)))
      (nippy-tools/with-freeze-opts nopts
        (wcar* (enqueue tq :dynamic)))
      (testing "Enqueue preserves explicit and ambient freeze options"
        (is (= (car/with-thaw-opts nopts (messages (mq/queue-content conn-opts tq)))
               #{:explicit :dynamic})))
      (testing "Worker inspection uses caller bindings, not creation bindings"
        (is (= (car/with-thaw-opts nopts (messages (w :queue-content)))
               #{:explicit :dynamic})))
      (finally (close-and-drain! w)))))

(deftest worker-nippy-bindings
  (clear-tq!)
  (let [creation-opts {:password [:salted "unused-creation-password"]}
        phase-opts {:first  {:password [:salted "first-start-password"]}
                    :second {:password [:salted "restart-password"]}}
        override-opts {:password [:salted "handler-override-password"]}
        plain-key     (str tq ":serialization:plain")
        inherited-key (str tq ":serialization:inherited")
        override-key  (str tq ":serialization:override")
        handled_ (atom {})
        w (car/with-thaw-opts creation-opts
            (mq/worker conn-opts tq
              {:auto-start false, :throttle-ms 10, :eoq-backoff-ms 10
               :handler
               (fn [{:keys [message]}]
                 (swap! handled_ assoc message
                   {:inherited  (wcar* (car/get inherited-key))
                    :plain      (wcar* (car/get plain-key))
                    :overridden (car/with-thaw-opts override-opts
                                  (wcar* (car/get override-key)))})
                 {:status :success})}))]
    (try
      (wcar*
        (car/set plain-key "plain-string")
        (car/set override-key (car/freeze :other-secret override-opts)))
      (doseq [phase [:first :second]]
        (testing (str "Bindings inherited at " (name phase) " start, including by handlers")
          (let [opts (get phase-opts phase)]
            (wcar*
              (car/set inherited-key (car/freeze [phase :cached] opts))
              (enqueue tq (car/freeze phase opts)))
            (is (true? (car/with-thaw-opts opts (mq/start w))))
            (is (wait-for 5000 #(contains? @handled_ phase)))
            (is (= (get @handled_ phase)
                   {:inherited [phase :cached], :plain "plain-string", :overridden :other-secret}))
            (is (true? (mq/stop w)))
            (is (true? (w :drain 5000)) "Drain the old generation before restart"))))
      (finally
        (close-and-drain! w)
        (wcar* (car/del plain-key inherited-key override-key))))))

(deftest queue-status-head-age
  (testing "`queue-status` reports age of next ready message"
    (clear-tq!)
    [(is (nil? (:head-age-ms (mq/queue-status conn-opts tq))) "Nothing ready")
     (do (wcar* (enqueue tq :msg1 {:mid :mid1 :init-backoff-ms 60000})) :enqueued-with-backoff)
     (is (nil? (:head-age-ms (mq/queue-status conn-opts tq))) "Backing off, so not ready")
     (do (wcar* (enqueue tq :msg2 {:mid :mid2}))
         (Thread/sleep 50)
         (wcar* (enqueue tq :msg3 {:mid :mid3})))
     (is (<= 50 (:head-age-ms (mq/queue-status conn-opts tq)) 5000) "Age of oldest ready message")
     (is (enc/submap? (mq/queue-status conn-opts tq) {:nqueued 2, :nbackoff 1}))
     (do (clear-tq!) :cleared)]))
