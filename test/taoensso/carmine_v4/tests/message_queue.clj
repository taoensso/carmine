(ns taoensso.carmine-v4.tests.message-queue
  "Tests for the Carmine v4 message queue, run under both RESP2 and RESP3.
  Mostly a port of the v3 MQ tests, plus v4-specific and v3<->v4 interop tests."
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc]
   [taoensso.trove   :as trove]
   [taoensso.carmine-v4 :as car]
   [taoensso.carmine-v4.message-queue :as mq]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.tests.test-support :as support]
   ;;
   [taoensso.carmine :as car3]
   [taoensso.carmine.message-queue :as mq3]
   [taoensso.carmine.impl.mq :as impl]))

(comment
  (remove-ns      'taoensso.carmine-v4.tests.message-queue)
  (test/run-tests 'taoensso.carmine-v4.tests.message-queue))

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

(def ^:dynamic *mgr* "Manager for current RESP protocol" nil)
(def ^:dynamic *resp3?* nil)

(defonce ^:private managers_
  (delay
    (into {}
      (map (fn [resp3?]
             [resp3? (car/conn-manager
                       {:conn-opts (assoc-in support/test-conn-opts [:init :resp3?] resp3?)})]))
      (support/supported-resp3-options))))

(defmacro wcar* [& body] `(car/wcar *mgr* ~@body))

(def tq "carmine-v4-test-queue")
(defn- client [] (#'mq/client *mgr*))
(defn clear-tq! [] (mq/queues-clear!! *mgr* [tq]))

(defn- each-protocol
  "Runs each test once per supported RESP protocol."
  [f]
  (doseq [[resp3? mgr] (sort-by key @managers_)]
    (binding [*mgr* mgr, *resp3?* resp3?]
      (testing (str "[resp3? " resp3? "]")
        (clear-tq!)
        (try (f) (finally (clear-tq!)))))))

(test/use-fixtures :each each-protocol)

(def ^:const default-lock-ms (enc/ms :mins 60))
(def ^:const eoq-backoff-ms 100)

(do
  (def enqueue    mq/enqueue)
  (def msg-status mq/message-status)

  (let [default-opts {:eoq-backoff-ms eoq-backoff-ms, :max-poll-steps 1}] ; Single-step polls
    (defn- dequeue
      ([qname]                  (dequeue qname nil (enc/uuid-str)))
      ([qname opts]             (dequeue qname opts (enc/uuid-str)))
      ([qname opts lock-token] (#'mq/send-req (impl/req-dequeue qname (conj default-opts opts) lock-token))))))

(defn sleep
  ([          n] (sleep nil n))
  ([isleep-on n]
   (let [n (int (case n :eoq (* 2.5 eoq-backoff-ms) n))]
     (if-let [on isleep-on]
       (impl/interruptible-sleep (client) tq n)
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

(defn- dequeue-until-handle [lock-token]
  (loop [n 0]
    (let [reply (wcar* (dequeue tq nil lock-token))]
      (if (or (= (get reply 0) "handle") (>= n 10))
        reply
        (recur (inc n))))))

(defn- close-and-drain! [w]
  (.close ^java.io.Closeable w)
  (is (true? (w :drain 5000)) "Worker drained during cleanup"))

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
       (is (subvec? (impl/handle1 (client) tq (fn hf [_] (throw!)) reply {})
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
     (is (enc/submap? (impl/queue-mids (client) tq)
           {:ready  ["mid1"]
            :circle ["end-of-circle"]}))

     (let [poll-reply (wcar* (dequeue tq))]
       [(is (= (count poll-reply) 6) "Preserves the public monitor reply shape")
        (is (subvec? poll-reply ["handle" "mid1" :msg1b 1 default-lock-ms #_udt]))])
     (is (=       (wcar* (msg-status tq :mid1)) :locked))
     (is (= (subvec (wcar* (dequeue tq)) 0 2) ["sleep" "end-of-circle"]))
     (is (contains? (mq/queue-names *mgr*) tq))])

  (testing "Manager-first arities"
    (clear-tq!)
    [(is (= (enqueue *mgr* tq :msg1a {:mid :mid1}) {:success? true,  :action :added, :mid :mid1}))
     (is (= (enqueue *mgr* tq :msg1b {:mid :mid1}) {:success? false, :error :already-queued}))
     (is (= (enqueue (delay *mgr*) tq :msg1b {:mid :mid1 :can-update? true})
           {:success? true, :action :updated, :mid :mid1}) "Delays are accepted")

     (is (enc/submap? (enqueue *mgr* tq :msg2)     {:success? true, :action :added}) "No opts")
     (is (enc/submap? (enqueue *mgr* tq :msg3 nil) {:success? true, :action :added}) "Nil opts")
     (is (enc/submap? (enqueue *mgr* tq nil)       {:success? true, :action :added}) "Nil message")

     (is (= (msg-status *mgr* tq :mid1) :queued))
     (is (= (msg-status *mgr* tq :nx)   nil))
     (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1b 1]))

     (is (= (mq/cancel *mgr* tq :mid1) :locked))
     (is (= (mq/cancel *mgr* tq :mid1) nil))
     (is (= (msg-status *mgr* tq :mid1) :done-awaiting-gc))]))

(deftest init-backoff
  (testing "Enqueue with initial backoff"
    (clear-tq!)
    [(is (= (subvec (wcar* (dequeue tq)) 0 2) ["sleep" "end-of-circle"]))
     (is (= (wcar* (enqueue tq :msg1 {:mid :mid1 :init-backoff-ms 500})) {:success? true, :action :added, :mid :mid1}))
     (is (= (wcar* (enqueue tq :msg2 {:mid :mid2 :init-backoff-ms 100})) {:success? true, :action :added, :mid :mid2}))

     (is (enc/submap? (impl/queue-mids (client) tq)
           {:ready  []
            :circle ["mid2" "mid1" "end-of-circle"]}))

     (is (enc/submap? (mq/queue-content *mgr* tq)
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
       (let [lock-token (enc/uuid-str)
             poll-reply  (wcar* (dequeue tq nil lock-token))]
         [(is (subvec? poll-reply ["handle" "mid1" :msg1 1]))
          (is (= (reschedule 60000) {:success? false, :error :locked}) "Only while awaiting handling")
          (is (subvec? (impl/handle1 (client) tq (fn [_] {:status :retry :backoff-ms 60000})
                         poll-reply {:lock-token lock-token})
                [:handled :retry]))])

       (is (= (wcar* (msg-status tq :mid1)) :queued-with-backoff))
       (del-signals!)
       (is (= (wcar* (enqueue tq :msg1b {:mid :mid1 :can-update? true :reset-init-backoff? true})) updated)
         "Reschedule a retry to now, with updated content")
       (is (= (nsignals) 1))
       (is (= (subvec (wcar* (dequeue tq)) 0 2) ["sleep" "end-of-circle"]))
       (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1b 2]) "Attempts kept")])))

(deftest undecodable-messages
  (let [gave-up_ (atom [])
        ncalls_  (atom 0)
        handle
        (fn [opts]
          (let [lock-token (enc/uuid-str)
                poll-reply
                (loop [n 0]
                  (let [reply (wcar* (dequeue tq nil lock-token))]
                    (if (or (= (first reply) "handle") (>= n 8))
                      reply
                      (recur (inc n)))))
                poll-reply (assoc poll-reply 2 (com/reply-error "Thaw failed" {:eid :carmine.read/nippy-thaw-error}))]
            (impl/handle1 (client) tq (fn [_] (swap! ncalls_ inc)) poll-reply
              (assoc opts :lock-token lock-token
                :on-give-up (fn [m] (swap! gave-up_ conj (:reason m)))))))]

    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    [(is (= (handle {:error-retry-backoff-ms 0}) [:handled :error]))
     (is (= (wcar* (msg-status tq :mid1)) :queued) "Retried like a handler error")
     (is (= (handle {}) [:handled :error]))
     (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
     (is (= [@ncalls_ @gave-up_] [0 [:error]]) "Handler never called")]))

(defn test-handler
  "Returns [<poll-reply> <handler-arg> <handle1-result>]"
  ([       hf] (test-handler false hf))
  ([async? hf]
   (let [lock-token  (enc/uuid-str)
         poll-reply   (wcar* (dequeue tq nil lock-token))
         handler-arg_ (promise)
         handle1
         (fn []
           (impl/handle1 (client) tq
             (fn [m] (deliver handler-arg_ m) (hf m))
             poll-reply {:lock-token lock-token}))

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
               {:qname "carmine-v4-test-queue", :mid "mid1", :message :msg1,
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
             (let [lock-token (enc/uuid-str)
                   poll-reply
                   (loop [n 0]
                     (let [reply (wcar* (dequeue tq nil lock-token))]
                       (if (or (= (first reply) "handle") (>= n 8))
                         reply
                         (recur (inc n)))))]
               [(get poll-reply 3)
                (impl/handle1 (client) tq (fn [m] (swap! ncalls_ inc) (hf m)) poll-reply
                  (assoc opts :lock-token lock-token))]))

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
      (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1c 1 500 #_udt]))])

   (testing "Repeat enqueues while :done-with-requeue don't grow the ready list"
     (clear-tq!)
     (let [nready #(count (:ready (impl/queue-mids (client) tq)))]
       [(is (:success? (wcar* (enqueue tq :msg1a {:mid :mid1}))))
        (do (test-handler (fn [_m] {:status :success})) :ran-handler)
        (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
        (is (= (wcar* (enqueue    tq :msg1b {:mid :mid1})) {:success? true, :action :added, :mid :mid1}))
        (is (= (wcar* (msg-status tq :mid1)) :done-with-requeue))
        (is (= (nready) 1))
        (is (= (wcar* (enqueue tq :msg1c {:mid :mid1}))                    {:success? false, :error :already-queued}))
        (is (= (wcar* (enqueue tq :msg1d {:mid :mid1, :can-update? true})) {:success? true, :action :updated, :mid :mid1}))
        (is (= (nready) 1))]))])

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
            lock-token (enc/uuid-str)
            poll-reply  (wcar* (dequeue tq nil lock-token))]
        [(is (= (impl/handle1 (client) tq
                  (fn [_] (wcar* (mq/cancel tq :mid1)) (throw!)) poll-reply
                  {:lock-token lock-token, :on-give-up (fn [_] (reset! gave-up?_ true))})
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
    [(is (= (impl/finalize (client) tq "mid1" "owner" true nil) ["finalized"]))
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
      (impl/finalize (client) tq "mid1" "owner" true nil)
      (wcar* (enqueue tq :msg2 {:mid :mid2}))
      (is (subvec? (wcar* (dequeue tq {:max-poll-steps max-steps})) ["handle" "mid2"]))
      (is (= (wcar* (msg-status tq :mid1))
            (if (= max-steps 1) :done-awaiting-gc nil))
        "Single-step poll takes ready mid without touching circle")))

  (testing "Retries in circle and ready mids can't starve each other"
    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    (wcar* (dequeue tq nil "owner"))
    (impl/finalize (client) tq "mid1" "owner" false nil) ; Retry without backoff
    (wcar* (enqueue tq :msg2 {:mid :mid2}))
    [(is (subvec? (wcar* (dequeue tq {:max-poll-steps 16})) ["handle" "mid2" :msg2 1]))
     (is (= (:ready (impl/queue-mids (client) tq)) ["mid1"]) "Retry waits in turn behind ready mids")
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
                  (mq/worker *mgr* tq
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
    (let [lock-token (enc/uuid-str)
          poll-reply  (wcar* (dequeue tq nil lock-token))
          udt         (long (get poll-reply 5))
          age-ms_     (promise)]
      (impl/handle1 (client) tq (fn [m] (deliver age-ms_ (:age-ms m)) {:status :success}) poll-reply
        {:lock-token lock-token, :server-udt (+ udt 5000)})
      (is (= (deref age-ms_ 0 nil) 5000)))

    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    (let [lock-token (enc/uuid-str)
          poll-reply  (wcar* (dequeue tq nil lock-token))
          udt         (long (get poll-reply 5))
          ages_       (atom [])
          hf          (fn [m] (swap! ages_ conj (:age-ms m)) {:status :retry})]
      (impl/handle1 (client) tq hf poll-reply {:lock-token lock-token, :server-udt (- udt 5000)})
      (is (= @ages_ [0]) "Never negative"))

    (clear-tq!)
    (wcar* (enqueue tq :msg1 {:mid :mid1}))
    (let [lock-token (enc/uuid-str)
          poll-reply  (wcar* (dequeue tq nil lock-token))
          age-ms_     (promise)]
      (impl/handle1 (client) tq (fn [m] (deliver age-ms_ (:age-ms m)) {:status :success}) poll-reply
        {:lock-token lock-token})
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
          stored       (wcar* (car/hget (impl/qkey tq :backoffs) "mid1"))]
      [(is (= hr [:handled :retry]))
       (is (re-matches #"\d+" (str stored))
         "Fractional `:backoff-ms` shouldn't produce a fractional expiry")])))

(defmacro ^:private testing-interruptible-sleep
  "Like `testing`, but skipped for Redis < 6, which lacks the fractional
  blocking timeouts that interruptible worker sleeps rely on."
  [string & body]
  `(when (>= (long (support/redis-major-version)) 6)
     (testing ~string ~@body)))

(deftest hardening
  (testing-interruptible-sleep "End-of-circle sleep blocks and enqueue wakes it"
    (clear-tq!)
    (let [reply (wcar* (#'mq/send-req (impl/req-dequeue tq {:eoq-backoff-ms 10000} (enc/uuid-str))))
          started (promise)
          sleeper (future
                    (deliver started true)
                    (impl/handle1 (client) tq identity reply {}))]
      [(is (= (deref started 5000 :timeout) true))
       (Thread/sleep 75)
       (is (not (realized? sleeper)))
       (is (:success? (wcar* (enqueue tq :wake {:mid :wake}))))
       (is (subvec? (deref sleeper 5000 :timeout) [:slept "end-of-circle"]))]))

  (testing-interruptible-sleep "Repeated pre-block enqueues cannot cancel the wake signal"
    (clear-tq!)
    (let [reply (wcar* (#'mq/send-req (impl/req-dequeue tq {:eoq-backoff-ms 1000} (enc/uuid-str))))
          enqueue-replies
          (wcar*
            (enqueue tq :wake-a {:mid :wake-a})
            (enqueue tq :wake-b {:mid :wake-b}))
          sleep-reply
          (impl/interruptible-sleep (client) tq 1000)]
      [(is (every? :success? enqueue-replies))
       (is (= (peek sleep-reply) "_")
         "BRPOP consumes a notification sent before the blocking call")
       (is (enc/submap? (impl/queue-mids (client) tq)
             {:ready ["wake-b" "wake-a"]
              :circle ["end-of-circle"]}))]))

  (testing "A failed blocking sleep is slept out locally"
    (let [failing-client
          (reify impl/IClient
            (exec         [_ _] (throw (ex-info "Simulated failure" {})))
            (open?        [_] true)
            (log-ns       [_] "taoensso.carmine.tests")
            (sleep-cap-ms [_] nil))
          slept_ (atom [])
          t0     (System/currentTimeMillis)]
      [(is (nil? (impl/interruptible-sleep failing-client tq 150)))
       (is (>= (- (System/currentTimeMillis) t0) 140))
       (is (nil? (impl/interruptible-sleep failing-client tq 150 #(swap! slept_ conj %))))
       (is (<= 140 (long (first @slept_)) 150) "Uses given local sleep fn")]))

  (testing "A stale handler cannot finalize a successor's lock"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 200}))))
    (let [token-a "lock-a"
          lock-a (wcar* (dequeue tq nil token-a))]
      (Thread/sleep 250)
      (wcar* (dequeue tq)) ; Rotate the end-of-circle marker.
      (let [token-b "lock-b"
            lock-b (wcar* (dequeue tq nil token-b))
            started (promise)
            release (promise)
            handler-b
            (future
              (impl/handle1 (client) tq
                (fn [{:keys [lock-extend!]}]
                  (deliver started (lock-extend!))
                  (deref release 3000 nil)
                  {:status :success})
                lock-b {:lock-token token-b}))]
        [(is (= (subvec lock-a 0 5) ["handle" "mid1" :msg 1 200]))
         (is (= (subvec lock-b 0 5) ["handle" "mid1" :msg 2 200]))
         (is (= (deref started 5000 :timeout) true))
         (is (= (impl/handle1 (client) tq (constantly {:status :success}) lock-a
                  {:lock-token token-a})
               [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :locked))
         (deliver release true)
         (is (= (deref handler-b 5000 :timeout) [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))
         (is (nil? (wcar* (car/hget (impl/qkey tq :lock-tokens) :mid1))))])))

  (testing "Fenced results are counted separately from completed handlings"
    (mapv
      (fn [[result expected-k]]
        (clear-tq!)
        (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 200}))))
        (let [lock-a  (wcar* (dequeue tq nil "token-a"))
              fenced_  (atom {})
              normal_  (atom {})]
          (Thread/sleep 250)                 ; Expire lock-a
          (wcar* (dequeue tq))               ; Rotate the end-of-circle marker
          (wcar* (dequeue tq nil "token-b")) ; Supersede lock-a's token

          ;; Stale token => fenced, no status/backoff counts
          (impl/handle1 (client) tq (constantly result) lock-a
            {:lock-token "token-a", :nstats_ fenced_})

          ;; Current token => normal accounting
          (impl/handle1 (client) tq (constantly result) lock-a
            {:lock-token "token-b", :nstats_ normal_})

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
          lock-a (wcar* (dequeue tq nil token-a))]
      (clear-tq!)
      (wcar* (enqueue tq :new {:mid :mid1 :lock-ms 1000}))
      (let [token-b "after-clear"
            lock-b (wcar* (dequeue tq nil token-b))]
        [(is (= (impl/handle1 (client) tq (constantly {:status :success}) lock-a
                  {:lock-token token-a})
               [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :locked))
         (is (= (impl/handle1 (client) tq (constantly {:status :success}) lock-b
                  {:lock-token token-b})
               [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))])))

  (testing "Renewal advances expiry and never shortens it"
    (clear-tq!)
    (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 5000}))
    (let [lock-token "extendable"
          _ (wcar* (dequeue tq nil lock-token))
          expiry #(Long/parseLong
                    (wcar* (car/hget (impl/qkey tq :locks) :mid1)))
          original (expiry)
          extended (impl/extend-lock (client) tq "mid1" lock-token 10000)]
      (is (> extended original))
      (is (= extended (expiry)))
      (is (= extended (impl/extend-lock (client) tq "mid1" lock-token 1)))
      (is (= extended (expiry)))))

  (testing "A stale token cannot renew a successor's lock"
    (clear-tq!)
    (wcar* (enqueue tq :msg {:mid :mid1}))
    (wcar* (dequeue tq nil "old"))
    ;; Expire deterministically using the stored server-clock timestamp.
    (wcar* (car/hset (impl/qkey tq :locks) :mid1 0))
    (wcar* (dequeue tq)) ; End-of-circle
    (wcar* (dequeue tq nil "new"))
    (let [expiry (wcar* (car/hget (impl/qkey tq :locks) :mid1))]
      (is (nil? (impl/extend-lock (client) tq "mid1" "old" 10000000)))
      (is (= expiry (wcar* (car/hget (impl/qkey tq :locks) :mid1))))))

  (testing "Expired but unsuperseded ownership can renew or finalize"
    (doseq [renew? [false true]]
      (clear-tq!)
      (wcar* (enqueue tq :msg {:mid :mid1}))
      (wcar* (dequeue tq nil "owner"))
      (wcar* (car/hset (impl/qkey tq :locks) :mid1 0))
      (when renew?
        (is (number? (impl/extend-lock (client) tq "mid1" "owner" 5000)))
        (is (= (wcar* (msg-status tq :mid1)) :locked)))
      (is (= (impl/finalize (client) tq "mid1" "owner" true nil) ["finalized"]))
      (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))))

  (testing "Finalization is fenced by token only: own extensions can't fence the owner"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 250}))))
    (let [lock-token "extended"
          lock (wcar* (dequeue tq nil lock-token))]
      ;; Extend server-side as if the handler's extend reply were lost: the
      ;; stored expiry no longer matches the expiry known at lock time.
      [(is (= (subvec lock 0 3) ["handle" "mid1" :msg]))
       (is (impl/extend-lock (client) tq "mid1" lock-token 5000))
       (is (= (impl/handle1 (client) tq (constantly {:status :success}) lock
                {:lock-token lock-token})
             [:handled :success]))
       (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc)
         "Owner finalized successfully despite the extended (mismatched) expiry")]))

  (testing "Producer update of an expired lock fences the stalled handler"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :old {:mid :mid1 :lock-ms 100}))))
    (let [lock-token "before-update"
          lock-a (wcar* (dequeue tq nil lock-token))]
      (Thread/sleep 150) ; Expire lock-a
      [(is (= (subvec lock-a 0 3) ["handle" "mid1" :old]))
       (is (= (wcar* (enqueue tq :new {:mid :mid1 :can-update? true}))
             {:success? true, :action :updated, :mid :mid1}))
       ;; The stalled handler completes, but must be fenced:
       (is (= (impl/handle1 (client) tq (constantly {:status :success}) lock-a
                {:lock-token lock-token})
             [:handled :success]))
       (is (= (wcar* (msg-status tq :mid1)) :queued)
         "Replacement payload survives the stale finalization")
       ;; And the replacement payload is actually delivered:
       (is (subvec? (dequeue-until-handle (enc/uuid-str)) ["handle" "mid1" :new]))]))

  (testing "Producer backoff reset of an expired lock fences the stalled handler"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 100}))))
    (let [lock-token "before-backoff-reset"
          lock-a (wcar* (dequeue tq nil lock-token))]
      (Thread/sleep 150) ; Expire lock-a
      [(is (= (subvec lock-a 0 3) ["handle" "mid1" :msg]))
       (is (= (wcar* (enqueue tq :msg {:mid :mid1 :reset-init-backoff? true}))
             {:success? true, :action :updated, :mid :mid1}))
       (is (= (impl/handle1 (client) tq (constantly {:status :success}) lock-a
                {:lock-token lock-token})
             [:handled :success]))
       (is (= (wcar* (msg-status tq :mid1)) :queued)
         "Re-asserted message survives the stale finalization")])))

(deftest worker-hardening
  (testing "Monitor retains the six-element handle reply"
    (clear-tq!)
    (let [handled (promise)
          monitored (promise)]
      (with-open [^java.io.Closeable worker
                  (mq/worker *mgr* tq
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

  (testing "Handler capacity is reserved before a Redis lock"
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
                  (mq/worker *mgr* tq
                    {:auto-start false, :handler handler
                     :nthreads-handler 1
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

  (testing "Handler threads poll for themselves"
    (clear-tq!)
    (let [nhandling* (enc/counter 0)
          release (promise)]
      (with-open [^java.io.Closeable worker
                  (mq/worker *mgr* tq
                    {:auto-start false
                     :handler (fn [_] (nhandling*) (deref release 5000 nil) {:status :success})
                     :nthreads-handler 3
                     :throttle-ms 10000, :eoq-backoff-ms 20})]
        (wcar* (enqueue tq :a) (enqueue tq :b) (enqueue tq :c))
        (mq/start worker)
        [(is (wait-for #(= @nhandling* 3)) "Not limited to 1 poll per throttle period")
         (do (deliver release true) (mq/stop worker) (is (mq/drain worker 5000)))])))

  (testing "Automatic lock extension"
    (doseq [[max-ms expected-attempts]
            [[5000 [1]]     ; Lock kept throughout slow handling
             [250  [1 2]]]] ; Extended, then left to expire once max time exceeded
      (clear-tq!)
      (let [attempts_ (atom [])
            started   (promise)
            release   (promise)]
        (with-open [^java.io.Closeable worker
                    (mq/worker *mgr* tq
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

  (testing "Monitor failures do not abandon locked messages"
    (clear-tq!)
    (let [handled (promise)]
      (with-open [^java.io.Closeable worker
                  (mq/worker *mgr* tq
                    {:handler (fn [_] (deliver handled true) {:status :success})
                     :monitor (fn [_] (throw (Exception. "monitor")))
                     :throttle-ms 1, :eoq-backoff-ms 20})]
        [(is (:success? (wcar* (enqueue tq :msg {:mid :mid1}))))
         (is (= (deref handled 2000 :timeout) true))
         (is (wait-for #(= (wcar* (msg-status tq :mid1)) :done-awaiting-gc)))])))

  (testing "A throwing `:throttle-ms` fn doesn't end its handler thread"
    (clear-tq!)
    (let [ncalls_ (atom 0)
          handled (promise)]
      (with-open [^java.io.Closeable worker
                  (mq/worker *mgr* tq
                    {:handler (fn [_] (deliver handled true) {:status :success})
                     :throttle-ms
                     (fn [_] (if (= (swap! ncalls_ inc) 1) (throw (Exception. "throttle")) 1))
                     :eoq-backoff-ms 20})]
        [(is (wait-for #(>= (long @ncalls_) 2)) "Polling continued after throw")
         (is (:success? (wcar* (enqueue tq :msg {:mid :mid1}))))
         (is (= (deref handled 2000 :timeout) true))])))

  (testing "A handler can stop its own worker without deadlocking"
    (clear-tq!)
    (let [worker_ (atom nil)
          stopped (promise)
          worker
          (mq/worker *mgr* tq
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
                  :nthreads-handler 2
                  :throttle-ms 1, :eoq-backoff-ms 10000}
          slept? (fn [w n]
                   (>= (long (or (get-in @w [:stats :counts :sleep/end-of-circle]) 0))
                     (long n)))
          worker     (mq/worker *mgr* tq opts)
          ;; Competes for the queue-global wake sentinels throughout the drain
          competitor (mq/worker *mgr* tq opts)]
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
          (.close ^java.io.Closeable competitor))))))

(deftest worker-lifecycle-races
  (testing "Restart cannot interleave with stop's generation transition"
    (clear-tq!)
    (let [handled (promise)
          worker (mq/worker *mgr* tq
                   {:auto-start false, :throttle-ms 1, :eoq-backoff-ms 20
                    :handler (fn [_] (deliver handled true) {:status :success})})
          running (:running?_ (.-state ^taoensso.carmine_v4.message_queue.CarmineMessageQueueWorker worker))
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
    (let [worker (mq/worker *mgr* tq {:auto-start false
                                        :throttle-ms 1, :eoq-backoff-ms 20})
          running (:running?_ (.-state ^taoensso.carmine_v4.message_queue.CarmineMessageQueueWorker worker))
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

;;;; v4-specific

(defmacro ^:private throws-eid
  "Returns the `:eid` of the ex-data thrown by body, or ::no-throw."
  [& body]
  `(try ~@body ::no-throw
     (catch Throwable t# (or (:eid (ex-data t#)) (type t#)))))

(deftest removed-v3-forms
  [(testing "Removed enqueue options and call shapes"
     [(is (= (throws-eid (wcar* (enqueue tq :msg :mid1)))                    :carmine.mq/invalid-opts))
      (is (= (throws-eid (wcar* (enqueue tq :msg :mid1 true)))               :carmine.mq/invalid-opts))
      (is (= (throws-eid (wcar* (enqueue tq :msg {:mid :mid1} :extra)))      :carmine.mq/invalid-opts))
      (is (= (throws-eid (wcar* (enqueue tq :msg {:unique-message-id :m})))  :carmine.mq/removed-option))
      (is (= (throws-eid (wcar* (enqueue tq :msg {:initial-backoff-ms 10}))) :carmine.mq/removed-option))
      (is (= (throws-eid (wcar* (enqueue tq :msg {:allow-requeue? true})))   :carmine.mq/removed-option))
      (is (= (throws-eid (enqueue *mgr* tq))                              :carmine.mq/invalid-args))
      (is (= (throws-eid (enqueue *mgr* tq :msg :mid1))                   :carmine.mq/invalid-opts))
      (is (= (throws-eid (enqueue *mgr* tq :msg {:mid :mid1} :extra))     :carmine.mq/invalid-opts))
      (is (= (throws-eid (enqueue *mgr* tq :msg {:allow-requeue? true}))  :carmine.mq/removed-option))
      (is (nil? (wcar* (msg-status tq :mid1))) "Nothing was enqueued")])

   (testing "Removed worker options"
     [(is (= (throws-eid (mq/worker *mgr* tq {:auto-start false, :nthreads 2}))       :carmine.mq/removed-option))
      (is (= (throws-eid (mq/worker *mgr* tq {:auto-start false, :auto-start? false})) :carmine.mq/removed-option))
      (is (= (throws-eid (mq/worker *mgr* tq {:auto-start false, :throttle-ms :auto})) :carmine.mq/removed-option))
      (is (= (throws-eid (mq/worker *mgr* tq {:auto-start 100}))                       :carmine.mq/removed-option))
      (is (= (throws-eid (mq/worker *mgr* tq {:auto-start false, :throttle-ms "100"})) :carmine.mq/invalid-opts))
      (is (= (try (mq/worker *mgr* tq {:auto-start false, :nthreads 2}) (catch Throwable t (:replacement (ex-data t))))
            "`:nthreads-handler`"))
      (is (= (throws-eid (mq/worker *mgr* tq {:auto-start false, :nthreads-worker 2})) :carmine.mq/removed-option))])

   (testing "Removed queues-clear!! varargs"
     [(is (= (throws-eid (mq/queues-clear!! *mgr* tq))        :carmine.mq/invalid-qnames))
      (is (= (throws-eid (mq/queues-clear!! *mgr* tq "other")) clojure.lang.ArityException))
      (is (= (mq/queues-clear!! *mgr* [tq]) [tq]))
      (is (nil? (mq/queues-clear!! *mgr* [])))])

   (testing "Removed deprecated vars"
     [(is (nil? (resolve 'taoensso.carmine-v4.message-queue/clear-queues)))
      (is (nil? (resolve 'taoensso.carmine-v4.message-queue/make-dequeue-worker)))])])

(deftest manager-validation
  [(testing "Invalid managers"
     [(is (= (throws-eid (mq/queue-status {} tq))         :carmine.mq/invalid-manager))
      (is (= (throws-eid (enqueue (delay :x) tq :msg))    :carmine.mq/invalid-manager))
      (is (= (throws-eid (mq/worker support/test-conn-opts tq)) :carmine.mq/invalid-manager))
      (is (= (mq/queue-status (delay *mgr*) tq) (mq/queue-status *mgr* tq)) "Delays are accepted")])

   (testing "Cluster managers are rejected everywhere"
     (let [mgr (car/conn-manager-clustered
                 {:conn-opts {:server {:cluster-spec (car/cluster-spec [["127.0.0.1" 1]])}}})]
       (try
         [(is (= (throws-eid (mq/worker         mgr tq))         :carmine.mq/cluster-not-supported))
          (is (= (throws-eid (mq/queue-status   mgr tq))         :carmine.mq/cluster-not-supported))
          (is (= (throws-eid (mq/queue-content  mgr tq))         :carmine.mq/cluster-not-supported))
          (is (= (throws-eid (mq/queue-names    mgr))            :carmine.mq/cluster-not-supported))
          (is (= (throws-eid (mq/queues-clear!! mgr [tq]))       :carmine.mq/cluster-not-supported))
          (is (= (throws-eid (enqueue    mgr tq :msg))            :carmine.mq/cluster-not-supported))
          (is (= (throws-eid (msg-status mgr tq :mid))            :carmine.mq/cluster-not-supported))
          (is (= (throws-eid (mq/cancel  mgr tq :mid))            :carmine.mq/cluster-not-supported))
          (is (= (throws-eid (car/wcar mgr (enqueue    tq :msg))) :carmine.mq/cluster-not-supported))
          (is (= (throws-eid (car/wcar mgr (msg-status tq :mid))) :carmine.mq/cluster-not-supported))]
         (finally (car/conn-manager-close! mgr)))))

   (testing "Replica-preferring Sentinel managers are rejected for writes"
     (let [mgr (car/conn-manager-unpooled
                 {:conn-opts
                  {:server {:master-name "mq-test"
                            :sentinel-spec (car/sentinel-spec {"mq-test" [["127.0.0.1" 1]]})
                            :sentinel-opts {:prefer-read-replica? true}}}})]
       (try
         [(is (= (throws-eid (mq/worker              mgr tq {:auto-start false})) :carmine.mq/replica-manager-not-supported))
          (is (= (throws-eid (mq/queues-clear!!      mgr [tq])) :carmine.mq/replica-manager-not-supported))
          (is (= (throws-eid (mq/queues-clear-all!!! mgr))      :carmine.mq/replica-manager-not-supported))
          (is (= (throws-eid (enqueue                mgr tq :msg)) :carmine.mq/replica-manager-not-supported))
          (is (= (throws-eid (mq/cancel              mgr tq :mid)) :carmine.mq/replica-manager-not-supported))
          (is (not= (throws-eid (msg-status          mgr tq :mid)) :carmine.mq/replica-manager-not-supported))
          (is (not= (throws-eid (mq/queue-status     mgr tq))   :carmine.mq/replica-manager-not-supported)
            "Reads are allowed (here failing only to connect)")]
         (finally (car/conn-manager-close! mgr)))))

   (testing "Script fallback is unavailable inside `transact!` effects"
     [(is (= (throws-eid (car/transact! *mgr* #(enqueue    tq :msg))) :carmine.script/tx-effects-not-supported))
      (is (= (throws-eid (car/transact! *mgr* #(msg-status tq :mid))) :carmine.script/tx-effects-not-supported))
      (is (= (throws-eid (car/transact! *mgr* #(mq/cancel  tq :mid))) :carmine.script/tx-effects-not-supported))
      (is (= (throws-eid (car/transact! *mgr* #(enqueue    *mgr* tq :msg))) :carmine.script/tx-effects-not-supported))
      (is (= (throws-eid (car/transact! *mgr* #(msg-status *mgr* tq :mid))) :carmine.script/tx-effects-not-supported))
      (is (= (throws-eid (car/transact! *mgr* #(mq/cancel  *mgr* tq :mid))) :carmine.script/tx-effects-not-supported))])])

(deftest reply-handling
  (testing "Cold (NOSCRIPT) and cached script paths parse identically, in pipelines"
    [(wcar* (car/script-flush))
     (is (= (wcar* (car/ping) (enqueue tq :msg1 {:mid :mid1}) (car/ping))
           ["PONG" {:success? true, :action :added, :mid :mid1} "PONG"]) "Cold")
     (is (= (wcar* (car/ping) (enqueue tq :msg2 {:mid :mid2}) (car/ping))
           ["PONG" {:success? true, :action :added, :mid :mid2} "PONG"]) "Cached")
     (wcar* (car/script-flush))
     (is (= (wcar* (msg-status tq :mid1) (msg-status tq :mid2)) [:queued :queued]) "Cold")
     (is (= (wcar* (msg-status tq :mid1) (msg-status tq :mid2)) [:queued :queued]) "Cached")
     (is (= (wcar* (car/parse str (enqueue tq :msg1 {:mid :mid1})))
           {:success? false, :error :already-queued}) "Own parser replaces an outer parser")])

  (testing "Internal requests ignore ambient reply bindings"
    (clear-tq!)
    (wcar* (enqueue tq {:a 1} {:mid :mid1}))
    (let [content          #(enc/map-vals (fn [m] (dissoc m :age-ms)) (mq/queue-content *mgr* tq))
          status           #(update (mq/queue-status *mgr* tq) :head-age-ms nat-int?) ; Age varies
          baseline-status  (status)
          baseline-content (content)
          baseline-names   (mq/queue-names   *mgr* tq)]
      [(is (= (get-in baseline-content ["mid1" :message]) {:a 1}))
       (is (= baseline-names #{tq}))
       (is (= (car/natural-replies (status)) baseline-status))
       (is (= (car/natural-replies (content)) baseline-content))
       (is (= (car/natural-replies (mq/queue-names   *mgr* tq)) baseline-names))
       (is (= (car/as-bytes        (content)) baseline-content))
       (is (= (car/as-bytes        (mq/queue-names   *mgr* tq)) baseline-names))
       (is (= (binding [car/*keywordize-maps?* true] (content)) baseline-content))
       (is (= (binding [car/*auto-thaw?*       false] (content)) baseline-content))
       (is (= (car/parse str (status)) baseline-status))
       (is (= (car/natural-replies (msg-status *mgr* tq :mid1)) :queued))
       (is (= (car/natural-replies (enqueue    *mgr* tq {:a 1} {:mid :mid1}))
             {:success? false, :error :already-queued}))
       (is (= (wcar* (car/ping) (msg-status *mgr* tq :mid1) (car/ping)) ["PONG" "PONG"])
         "Manager-first calls add no reply to an enclosing request context")
       (is (= (car/natural-replies (mq/cancel *mgr* tq :nx)) nil))
       (let [handled (promise)]
         (with-open [^java.io.Closeable _w
                     (binding [car/*auto-thaw?* false] ; Conveyed to worker threads
                       (mq/worker *mgr* tq
                         {:handler (fn [m] (deliver handled (:message m)) {:status :success})
                          :throttle-ms 1, :eoq-backoff-ms 20}))]
           (is (= (deref handled 3000 :timeout) {:a 1}) "Worker delivers thawed messages")))]))

  (testing "Un-thawable message content arrives as a reply error value"
    (clear-tq!)
    (wcar* (enqueue tq :msg {:mid :mid1}))
    (wcar* (car/hset (impl/qkey tq :messages) "mid1"
             (car/bytes (byte-array [0 (int \>) 1 2 3]))))
    (let [reply (wcar* (dequeue tq))]
      [(is (= (subvec reply 0 2) ["handle" "mid1"]))
       (is (car/reply-error? (get reply 2)))])))

(deftest custom-nippy-opts
  (let [nopts {:password [:salted "test-password"]}
        creation-opts {:password [:salted "unused-creation-password"]}
        messages #(set (map :message (vals %)))
        w (car/with-thaw-opts creation-opts
            (mq/worker *mgr* tq {:auto-start false}))]
    (clear-tq!)
    (try
      (wcar* (enqueue tq (car/freeze :explicit nopts)))
      (car/with-freeze-opts nopts
        (wcar* (enqueue tq :dynamic)))
      (testing "Enqueue preserves explicit and ambient freeze options"
        (is (= (car/with-thaw-opts nopts (messages (mq/queue-content *mgr* tq)))
               #{:explicit :dynamic})))
      (testing "Worker inspection uses caller bindings, not creation bindings"
        (is (= (car/with-thaw-opts nopts (messages (w :queue-content)))
               #{:explicit :dynamic})))
      (testing "Message content is always frozen, even without auto-freeze"
        (clear-tq!)
        (binding [car/*auto-freeze?* false]
          (wcar*  (enqueue tq {:v 1}))
          (enqueue *mgr*   tq {:v 2}))
        (is (= (messages (mq/queue-content *mgr* tq)) #{{:v 1} {:v 2}})))
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
            (mq/worker *mgr* tq
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

(deftest worker-request-isolation
  ;; Worker threads mustn't send the pending requests of a `wcar` body that
  ;; starts or drains the worker
  (let [key      (str tq ":isolation")
        started  (promise)
        release  (promise)
        aborted? (fn [f]
                   (try
                     (wcar* (car/set key "leaked") (f) (sleep 300)
                       (throw (ex-info "Abort" {::abort true})))
                     (catch Throwable t (boolean (::abort (ex-data t))))))
        w (mq/worker *mgr* tq
            {:auto-start false, :throttle-ms 1, :eoq-backoff-ms 20
             :handler (fn [_] (deliver started true) (deref release 5000 nil) {:status :success})})]
    (try
      (wcar* (car/del key) (enqueue tq :msg))
      [(is (aborted? #(mq/start w)))
       (is (= (wcar* (car/exists key)) 0) "Start")
       (is (= (deref started 3000 :timeout) true))
       (is (true? (mq/stop w)))
       (is (= (wcar* (car/del key)) 0))
       (is (aborted? #(w :drain 100)))
       (is (= (wcar* (car/exists key)) 0) "Drain")]
      (finally
        (deliver release true)
        (close-and-drain! w)
        (wcar* (car/del key))))))

(deftest sleep-timeouts
  [(testing "Sleep cap leaves margin below read timeout"
     [(is (= (mapv #'mq/sleep-cap-ms [100 300 1000 10000]) [-150 50 750 7500]))])

   (doall (for [read-timeout-ms [150 600]] ; Local sleep, capped BRPOP
    (testing (str "Idle sleep doesn't trip a short read timeout: " read-timeout-ms)
     (let [mgr (car/conn-manager
                 {:conn-opts (assoc-in support/test-conn-opts [:socket-opts :read-timeout-ms] read-timeout-ms)})
           handled (promise)]
       (try
         (with-open [^java.io.Closeable w
                     (mq/worker mgr tq
                       {:handler (fn [m] (deliver handled (:message m)) {:status :success})
                        :throttle-ms 1, :eoq-backoff-ms 1000})]
           [(is (wait-for 5000 #(>= (long (get-in @w [:stats :counts :sleep/end-of-circle] 0)) 3)))
            (is (= (get-in (car/conn-manager-stats mgr) [:counts :destroyed :total]) 0)
              "No connection was invalidated by a read timeout")
            (is (:success? (car/wcar mgr (enqueue tq :msg))))
            (is (= (deref handled 3000 :timeout) :msg))])
         (finally (car/conn-manager-close! mgr)))))))])

(deftest worker-lifecycle
  [(testing "Deref includes manager"
     (with-open [^java.io.Closeable w (mq/worker *mgr* tq {:auto-start false})]
       (is (identical? (:conn-mgr @w) *mgr*))))

   (testing "Bounded drain, and close doesn't wait for active handlers"
     (let [started (promise), release (promise), done_ (atom false)
           w (mq/worker *mgr* tq
               {:handler (fn [_] (deliver started true) (deref release 5000 nil)
                           (Thread/sleep 200) (reset! done_ true) {:status :success})
                :throttle-ms 1, :eoq-backoff-ms 20})]
       (try
         [(is (:success? (wcar* (enqueue tq :msg))))
          (is (= (deref started 3000 :timeout) true))
          (is (false? (w :drain 50)) "Running")
          (is (true?  (mq/stop w)))
          (is (false? (w :drain 0)) "Zero timeout")
          (let [t0 (System/currentTimeMillis)]
            [(is (false? (w :drain 300)) "Timed out")
             (is (< (- (System/currentTimeMillis) t0) 1000) "Returned on time")])
          (let [t0 (System/currentTimeMillis)]
            (.close ^java.io.Closeable w)
            [(is (< (- (System/currentTimeMillis) t0) 1000) "Close returned promptly")
             (is (false? @done_) "Handler still active after close")])
          (do (deliver release true) :released)
          (is (true? (w :drain 5000)) "Drain works after close")
          (is (true? @done_))
          (is (:closed? @w))]
         (finally (deliver release true) (.close ^java.io.Closeable w)))))

   (testing-interruptible-sleep "Bounded drain returns on time even when sleepers hold every pooled connection"
     (let [mgr (car/conn-manager {:conn-opts support/test-conn-opts, :pool-opts {:max-total 1}})
           w   (mq/worker mgr tq {:throttle-ms 1, :eoq-backoff-ms 3000})]
       (try
         [(is (wait-for 5000 #(pos? (long (get-in @w [:stats :counts :sleep/end-of-circle] 0)))))
          (is (true? (mq/stop w)))
          (let [t0 (System/currentTimeMillis)
                drained (w :drain 300)
                elapsed (- (System/currentTimeMillis) t0)]
            [(is (false? drained))
             (is (< elapsed 1500) (str "Took " elapsed "ms"))])
          (is (true? (w :drain 10000)) "Eventually drains")]
         (finally (.close ^java.io.Closeable w) (car/conn-manager-close! mgr)))))

   (doall (for [throttle-ms [0 nil]]
     (testing (str "Worker with `:throttle-ms " (pr-str throttle-ms) "`")
       (let [handled (promise)
             w (mq/worker *mgr* tq
                 {:throttle-ms throttle-ms, :eoq-backoff-ms 20
                  :handler (fn [m] (deliver handled (:message m)) {:status :success})})]
         (try
           [(is (:success? (wcar* (enqueue tq :msg))))
            (is (= (deref handled 5000 :timeout) :msg))
            (is (:running? @w))
            (is (true? (mq/stop w)))]
           (finally (.close ^java.io.Closeable w)))))))

   (testing "Drain returns false if worker restarts meanwhile"
     (let [started (promise), release (promise)
           w (mq/worker *mgr* tq
               {:handler (fn [_] (deliver started true) (deref release 5000 nil) {:status :success})
                :throttle-ms 1, :eoq-backoff-ms 20})]
       (try
         [(is (:success? (wcar* (enqueue tq :msg))))
          (is (= (deref started 3000 :timeout) true))
          (is (true? (mq/stop w)))
          (let [drain (future (w :drain))]
            [(is (= (deref drain 200 :waiting) :waiting) "Drain is waiting on the active handler")
             (is (true? (mq/start w)))
             (is (false? (deref drain 3000 :timeout)) "Returns while handler still active")])]
         (finally (deliver release true) (.close ^java.io.Closeable w)))))

   (testing "Close doesn't wait for local (short read timeout) sleeps"
     (let [mgr (car/conn-manager
                 {:conn-opts (assoc-in support/test-conn-opts [:socket-opts :read-timeout-ms] 150)})
           w   (mq/worker mgr tq {:throttle-ms 1, :eoq-backoff-ms 5000})]
       (try
         [(is (wait-for 5000 #(pos? (long (get-in @w [:stats :counts :sleep/end-of-circle] 0)))))
          (let [t0 (System/currentTimeMillis)]
            (.close ^java.io.Closeable w)
            (is (< (- (System/currentTimeMillis) t0) 1500)))
          (is (true? (w :drain 2000)))]
         (finally (.close ^java.io.Closeable w) (car/conn-manager-close! mgr)))))

   (testing "Stop doesn't wait for long worker sleeps (throttle, error backoff)"
     (let [w (mq/worker *mgr* tq
               {:throttle-ms 5000 ; Throttle + error backoff factor
                :eoq-backoff-ms (fn [_] (throw (Exception. "Test worker error")))})]
       (try
         (Thread/sleep 500) ; First error => 5s+ sleep
         (let [t0 (System/currentTimeMillis)]
           [(is (true? (mq/stop w)))
            (is (< (- (System/currentTimeMillis) t0) 1000))])
         (finally (.close ^java.io.Closeable w)))))

   (testing "Manager closure stops worker"
     (let [mgr (car/conn-manager {:conn-opts support/test-conn-opts})
           w   (mq/worker mgr tq {:throttle-ms 1, :eoq-backoff-ms 20})]
       (try
         [(is (:running? @w))
          (do (car/conn-manager-close! mgr) :closed-mgr)
          (is (wait-for 5000 #(not (:running? @w))))
          (is (= (throws-eid (mq/start w)) :carmine.mq/manager-closed))
          (is (= (throws-eid (mq/worker mgr tq)) :carmine.mq/manager-closed))]
         (finally (.close ^java.io.Closeable w)))))])

;;;; v3 <-> v4 interop

(def ^:private v3-conn-opts support/v3-test-conn-opts)
(defmacro ^:private wcar3 [& body] `(car3/wcar v3-conn-opts ~@body))

(defn- comparable [x] (if (enc/bytes? x) [:bytes (vec x)] x))
(def ^:private interop-msgs
  {"nil"  nil
   "bin"  (byte-array [1 2 3])
   "str"  "str"
   "kw"   :kw
   "long" 42
   "map"  {:a [1 #{2}] :b "str" :c nil}})

(defn- handle-all!
  "Runs `worker-fn` until it handles `n` messages, returns {<mid> <message>}."
  [worker-fn n]
  (let [handled_ (atom {})
        w (worker-fn
            {:handler (fn [{:keys [mid message]}] (swap! handled_ assoc mid message) {:status :success})
             :throttle-ms 1, :eoq-backoff-ms 20})]
    (try
      (is (wait-for 5000 #(= (count @handled_) n)))
      @handled_
      (finally (close-and-drain! w)))))

(deftest v3-interop
  [(testing "v3 enqueue -> v4 worker"
     (doseq [[mid msg] interop-msgs] (wcar3 (mq3/enqueue tq msg {:mid mid})))
     [(is (= (enc/map-vals (comp comparable :message) (mq/queue-content *mgr* tq))
             (enc/map-vals comparable interop-msgs)))
      (is (= (enc/map-vals comparable (handle-all! #(mq/worker *mgr* tq %) (count interop-msgs)))
             (enc/map-vals comparable interop-msgs)))])

   (testing "v4 enqueue -> v3 worker"
    (clear-tq!)
     (doseq [[mid msg] interop-msgs] (wcar* (enqueue tq msg {:mid mid})))
     [(is (= (enc/map-vals comparable (handle-all! #(mq3/worker v3-conn-opts tq %) (count interop-msgs)))
             (enc/map-vals comparable interop-msgs)))])

   (testing "Status, backoff, requeue and update across versions"
    (clear-tq!)
     [(is (= (wcar3 (mq3/enqueue tq :msg1 {:mid :mid1 :init-backoff-ms 5000})) {:success? true, :action :added, :mid :mid1}))
      (is (= (wcar* (msg-status tq :mid1)) :queued-with-backoff))
      (is (= (wcar* (enqueue tq :msg1b {:mid :mid1 :can-update? true})) {:success? true, :action :updated, :mid :mid1}))
      (is (= (wcar3 (mq3/message-status tq :mid1)) :queued-with-backoff))
      (is (= (get-in (car3/wcar v3-conn-opts (mq3/enqueue tq :msg2 {:mid :mid2})) [:success?]) true))
      (is (subvec? (dequeue-until-handle "v4-token") ["handle" "mid2" :msg2 1]))
      (is (= (wcar3 (mq3/enqueue tq :msg2b {:mid :mid2 :can-requeue? true})) {:success? true, :action :added, :mid :mid2}))
      (is (= (wcar* (msg-status tq :mid2)) :locked-with-requeue))])

   (testing "Lock renewal and finalize fencing across versions"
    (clear-tq!)
     (wcar3 (mq3/enqueue tq :msg {:mid :mid1}))
     (let [lock-v3 (car3/wcar v3-conn-opts (#'mq3/send-req (impl/req-dequeue tq {} "v3-token")))]
       [(is (subvec? lock-v3 ["handle" "mid1" :msg 1]))
        (is (nil?    (impl/extend-lock (client) tq "mid1" "wrong-token" 5000)) "v4 can't renew a v3 lock with wrong token")
        (is (number? (impl/extend-lock (client) tq "mid1" "v3-token"    5000)) "v4 can renew a v3 lock")
        (is (= (impl/finalize (client) tq "mid1" "wrong-token" true nil) ["stale"]))
        (is (= (impl/finalize (#'mq3/client v3-conn-opts) tq "mid1" "wrong-token" true nil) ["stale"]))
        (is (= (impl/finalize (client) tq "mid1" "v3-token" true nil) ["finalized"]))
        (is (= (wcar3 (mq3/message-status tq :mid1)) :done-awaiting-gc))]))])

(deftest encrypted-v3-interop
  (let [nopts {:password [:salted "shared-v3-v4-password"]}
        expected {"explicit" {:source :explicit}, "dynamic" {:source :dynamic}}]
    (testing "Encrypted v3 enqueue -> v4 inspection and worker"
      (wcar3 (mq3/enqueue tq (car3/freeze {:source :explicit} nopts) {:mid "explicit"}))
      (car/with-freeze-opts nopts
        (wcar3 (mq3/enqueue tq {:source :dynamic} {:mid "dynamic"})))
      (car3/with-thaw-opts nopts
        (is (= (enc/map-vals :message (mq/queue-content *mgr* tq)) expected))
        (is (= (handle-all! #(mq/worker *mgr* tq %) 2) expected))))
    (clear-tq!)
    (testing "Encrypted v4 enqueue -> v3 inspection and worker"
      (wcar* (enqueue tq (car/freeze {:source :explicit} nopts) {:mid "explicit"}))
      (car/with-freeze-opts nopts
        (wcar* (enqueue tq {:source :dynamic} {:mid "dynamic"})))
      (car/with-thaw-opts nopts
        (is (= (enc/map-vals :message (mq3/queue-content v3-conn-opts tq)) expected))
        (is (= (handle-all! #(mq3/worker v3-conn-opts tq %) 2) expected))))))

(deftest queue-status-head-age
  (testing "`queue-status` reports age of next ready message"
    (clear-tq!)
    [(is (nil? (:head-age-ms (mq/queue-status *mgr* tq))) "Nothing ready")
     (do (wcar* (enqueue tq :msg1 {:mid :mid1 :init-backoff-ms 60000})) :enqueued-with-backoff)
     (is (nil? (:head-age-ms (mq/queue-status *mgr* tq))) "Backing off, so not ready")
     (do (wcar* (enqueue tq :msg2 {:mid :mid2}))
         (Thread/sleep 50)
         (wcar* (enqueue tq :msg3 {:mid :mid3})))
     (is (<= 50 (:head-age-ms (mq/queue-status *mgr* tq)) 5000) "Age of oldest ready message")
     (is (enc/submap? (mq/queue-status *mgr* tq) {:nqueued 2, :nbackoff 1}))
     (do (clear-tq!) :cleared)]))

(deftest unknown-options
  (testing "Unknown (e.g. misspelled) options are rejected"
    (clear-tq!)
    [(is (= (throws-eid (wcar* (enqueue tq :msg {:mid :mid1, :can-update true}))) :carmine.mq/unknown-option))
     (is (= (throws-eid (enqueue *mgr* tq :msg {:init-backoff 10}))             :carmine.mq/unknown-option))
     (is (= (throws-eid (mq/worker *mgr* tq {:auto-start false, :max-attempt 3})) :carmine.mq/unknown-option))
     (is (= (try (mq/worker *mgr* tq {:auto-start false, :max-attempt 3}) (catch Throwable t (:opt-key (ex-data t)))) :max-attempt))
     (is (nil? (wcar* (msg-status tq :mid1))) "Nothing was enqueued")
     (with-open [^java.io.Closeable w (mq/worker *mgr* tq {:auto-start false, :my.app/tag 1})]
       (is (mq/worker? w) "Qualified keys are allowed"))]))

(deftest helpers
  (clear-tq!)
  [(do (wcar* (enqueue tq :msg1) (enqueue tq :msg2)) :enqueued)
   (is (= (mq/queue-size *mgr* tq) (:nqueued (mq/queue-status *mgr* tq)) 2))
   (is (= (mapv mq/default-throttle-ms-fn [0 129 257]) [200 100 50]))
   (is (< 0 (mq/exp-backoff 1 {}) (mq/exp-backoff 10 {})))
   (let [ids_    (atom [])
         monitor (mq/monitor-fn tq 1 60000)]
     (binding [trove/*log-fn* (fn [_ns _coords _level id _data] (swap! ids_ conj id))]
       (run! #(monitor {:queue-size %}) [1 2 2]))
     (is (= @ids_ [:carmine.mq/monitor-size-warning]) "Warns over max size, with backoff"))
   (do (clear-tq!) :cleared)])
