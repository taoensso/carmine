(ns taoensso.carmine.tests.message-queue
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc]
   [taoensso.carmine :as car  :refer [wcar]]
   [taoensso.carmine.message-queue :as mq]
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
(defn clear-tq! [] (mq/queues-clear!! conn-opts [tq]))

(defn test-fixture [f] (f) (clear-tq!))
(test/use-fixtures :once test-fixture) ; Just for final teardown

(def ^:const default-lock-ms (enc/ms :mins 60))
(def ^:const eoq-backoff-ms 100)

(do
  (def enqueue    mq/enqueue)
  (def msg-status mq/message-status)

  (let [default-opts {:eoq-backoff-ms eoq-backoff-ms}]
    (defn- dequeue
      ([qname]                  (dequeue qname nil (enc/uuid-str)))
      ([qname opts]             (dequeue qname opts (enc/uuid-str)))
      ([qname opts lease-token] (#'mq/dequeue qname (conj default-opts opts) lease-token)))))

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
(defn handle-end-of-circle []
  (let [reply (wcar* (dequeue tq))]
    (every? identity
      [(is (= (subvec reply 0 2) ["sleep" "end-of-circle"]))
       (is (contains? #{"a" "b"} (get reply 2)))
       (is (= (get reply 3) eoq-backoff-ms))
       (is (subvec? (#'mq/handle1 conn-opts tq (fn hf [_] (throw!)) reply {})
             [:slept "end-of-circle" (get reply 2) #_eoq-backoff-ms]))])))

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
     (is (enc/submap? (#'mq/queue-mids conn-opts tq)
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

     (is (enc/submap? (#'mq/queue-mids conn-opts tq)
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

     (handle-end-of-circle)

     (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1 1 default-lock-ms #_udt]))
     (is (= (wcar* (msg-status tq :mid1)) :locked))

     (is (subvec? (wcar* (dequeue tq)) ["skip" "queued-with-backoff"]))
     (is (= (wcar* (msg-status tq :mid2))      :queued-with-backoff))]))

(defn test-handler
  "Returns [<poll-reply> <handler-arg> <handle1-result>]"
  ([       hf] (test-handler false hf))
  ([async? hf]
   (let [lease-token  (enc/uuid-str)
         poll-reply   (wcar* (dequeue tq nil lease-token))
         handler-arg_ (promise)
         handle1
         (fn []
           (#'mq/handle1 conn-opts tq
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
      (handle-end-of-circle)

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
      (handle-end-of-circle)

      (is (subvec? (wcar* (dequeue tq)) ["handle" "mid1" :msg1 2 default-lock-ms #_udt]))])

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
    [(is (= (#'mq/server-udt ["1784523705" "818999"]) 1784523705818) "Micros floored to msecs")
     (is (= (#'mq/server-udt ["1784523705"      "0"]) 1784523705000))
     (is (= (#'mq/server-udt ["1784523705"    "999"]) 1784523705000) "Sub-msec micros discarded")])

  (testing "Queue scripts take their time from the Redis server"
    (mapv
      (fn [sname]
        (let [src (enc/slurp-resource (str "taoensso/carmine/lua/mq/" sname ".lua"))]
          [(is (not (enc/str-contains? src "_:now"))
             (str sname ".lua shouldn't accept a client-supplied `now`"))
           (is (enc/str-contains? src "redis.call('TIME')")
             (str sname ".lua should read the server clock"))]))
      ["msg-status" "enqueue" "dequeue" "finalize" "extend-lock"]))

  (testing "Write scripts request effects replication before their first write"
    (mapv
      (fn [sname]
        (let [^String src  (enc/slurp-resource (str "taoensso/carmine/lua/mq/" sname ".lua"))
              i-replicate  (.indexOf src "redis.replicate_commands()")
              i-first-call (.indexOf src "redis.call(")]
          (is (and (not= i-replicate -1) (not= i-first-call -1)
                (< i-replicate i-first-call))
            (str sname ".lua should call `redis.replicate_commands()` before its first `redis.call`"))))
      ["enqueue" "dequeue" "finalize" "extend-lock"]))

  (testing "Handler backoffs are stored as integer epoch msecs"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg1 {:mid :mid1}))))
    (let [[_pr _ha hr] (test-handler (fn [_m] {:status :retry :backoff-ms 2000.9}))
          stored       (second (wcar* (car/hgetall (#'mq/qkey tq :backoffs))))]
      [(is (= hr [:handled :retry]))
       (is (re-matches #"\d+" (str stored))
         "Fractional `:backoff-ms` shouldn't produce a fractional expiry")])))

(deftest hardening
  (testing "End-of-circle sleep blocks and enqueue wakes it"
    (clear-tq!)
    (let [reply (wcar* (#'mq/dequeue tq {:eoq-backoff-ms 10000} (enc/uuid-str)))
          started (promise)
          sleeper (future
                    (deliver started true)
                    (#'mq/handle1 conn-opts tq identity reply {}))]
      [(is (= (deref started 5000 :timeout) true))
       (Thread/sleep 75)
       (is (not (realized? sleeper)))
       (is (:success? (wcar* (enqueue tq :wake {:mid :wake}))))
       (is (subvec? (deref sleeper 5000 :timeout) [:slept "end-of-circle"]))]))

  (testing "Repeated pre-block enqueues cannot cancel the wake signal"
    (clear-tq!)
    (let [reply (wcar* (#'mq/dequeue tq {:eoq-backoff-ms 1000} (enc/uuid-str)))
          enqueue-replies
          (wcar*
            (enqueue tq :wake-a {:mid :wake-a})
            (enqueue tq :wake-b {:mid :wake-b}))
          sleep-reply
          (#'mq/interruptible-sleep conn-opts tq (get reply 2) 1000)]
      [(is (every? :success? enqueue-replies))
       (is (= (peek sleep-reply) "_")
         "BRPOP consumes a notification sent before the blocking call")
       (is (enc/submap? (#'mq/queue-mids conn-opts tq)
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
              (#'mq/handle1 conn-opts tq
                (fn [{:keys [lock-extend!]}]
                  (deliver started (lock-extend!))
                  (deref release 3000 nil)
                  {:status :success})
                lease-b {:lease-token token-b}))]
        [(is (= (subvec lease-a 0 5) ["handle" "mid1" :msg 1 200]))
         (is (= (subvec lease-b 0 5) ["handle" "mid1" :msg 2 200]))
         (is (= (deref started 5000 :timeout) true))
         (is (= (#'mq/handle1 conn-opts tq (constantly {:status :success}) lease-a
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
          (#'mq/handle1 conn-opts tq (constantly result) lease-a
            {:lease-token "token-a", :nstats_ fenced_})

          ;; Current token => normal accounting
          (#'mq/handle1 conn-opts tq (constantly result) lease-a
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
        [(is (= (#'mq/handle1 conn-opts tq (constantly {:status :success}) lease-a
                  {:lease-token token-a})
               [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :locked))
         (is (= (#'mq/handle1 conn-opts tq (constantly {:status :success}) lease-b
                  {:lease-token token-b})
               [:handled :success]))
         (is (= (wcar* (msg-status tq :mid1)) :done-awaiting-gc))])))

  (testing "The current handler can extend its lease"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 5000}))))
    (let [lease-token "extendable"
          lease (wcar* (dequeue tq nil lease-token))]
      [(Thread/sleep 75)
       (is (#'mq/extend-lock conn-opts tq "mid1" lease-token 1000))
       (Thread/sleep 250)
       (is (= (wcar* (msg-status tq :mid1)) :locked))
       (do (wcar* (dequeue tq)) nil) ; Rotate the end-of-circle marker.
       (is (= (wcar* (dequeue tq)) ["skip" "locked"]))]))

  (testing "Finalization is fenced by token only: own extensions can't fence the owner"
    (clear-tq!)
    (is (:success? (wcar* (enqueue tq :msg {:mid :mid1 :lock-ms 250}))))
    (let [lease-token "extended"
          lease (wcar* (dequeue tq nil lease-token))]
      ;; Extend server-side as if the handler's extend reply were lost: the
      ;; stored expiry no longer matches the expiry known at lease time.
      [(is (= (subvec lease 0 3) ["handle" "mid1" :msg]))
       (is (#'mq/extend-lock conn-opts tq "mid1" lease-token 5000))
       (is (= (#'mq/handle1 conn-opts tq (constantly {:status :success}) lease
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
       (is (= (#'mq/handle1 conn-opts tq (constantly {:status :success}) lease-a
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
       (is (= (#'mq/handle1 conn-opts tq (constantly {:status :success}) lease-a
                {:lease-token lease-token})
             [:handled :success]))
       (is (= (wcar* (msg-status tq :mid1)) :queued)
         "Re-asserted message survives the stale finalization")])))

(deftest worker-hardening
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
        (is (worker :drain))
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
