(ns taoensso.carmine.message-queue
  "Carmine-backed Clojure message queue. All heavy lifting by Redis.
  Message circle architecture used here is simple, reliable, and has
  reasonable throughput but at best mediocre latency.

  Redis keys:
    * carmine:mq:<qname>:messages     - hash, {mid mcontent}.
    * carmine:mq:<qname>:locks        - hash, {mid lock-expiry-time}.
    * carmine:mq:<qname>:backoffs     - hash, {mid backoff-expiry-time}.
    * carmine:mq:<qname>:nattempts    - hash, {mid attempt-count}.
    * carmine:mq:<qname>:mid-circle   - list, rotating list of mids.
    * carmine:mq:<qname>:done         - set, awaiting gc, requeue, etc.
    * carmine:mq:<qname>:requeue      - set, for `allow-requeue?` option.
    * carmine:mq:<qname>:eoq-backoff? - ttl flag, used for queue-wide
                                        (every-worker) polling backoff.
    * carmine:mq:<qname>:ndry-runs    - int, number of times worker(s) have
                                        burnt through queue w/o work to do.

  Ref. http://antirez.com/post/250 for basic implementation details."
  {:author "Peter Taoussanis"}
  (:require [clojure.string   :as str]
            [taoensso.timbre  :as timbre]
            [taoensso.carmine :as car :refer (wcar)]
            [taoensso.encore  :as encore]))

;; TODO Ability to enqueue something with an init backoff

;;;; Ideas to avoid polling
;; 1. Lua brpoplpush - doesn't currently exist, unlikely to in future.
;; 2. Redis 2.8+ Pub/Sub keyspace notifications? Maybe with reliable Pub/Sub?
;; 3. Possible 2-circle (or alternative) designs?
;; 4. Leave it as is since we'd want a polling worker anyway for gc, lock
;;    checks, etc.

;;;; Public utils

(defn get-expected-throughput
  "Returns ~n msgs handled per second, excluding gc + queue maintenance."
  [nthreads ?throttle-ms]
  (* 0.5 nthreads (/ 1000 (or ?throttle-ms 0.5))))

(comment
  (get-expected-throughput 1 100)
  (get-expected-throughput 10 nil))

(defn exp-backoff "Returns binary exponential backoff value."
  [attempt & [{:keys [factor min max] :or {factor 2200}}]]
  (let [binary-exp (Math/pow 2 (dec attempt))
        time       (* (+ binary-exp (rand binary-exp)) 0.5 factor)]
    (long (let [time (if min (clojure.core/max min time) time)
                time (if max (clojure.core/min max time) time)]
            time))))

(comment (mapv #(exp-backoff % {}) (range 5)))

;;;; Admin

(def qkey "Prefixed queue key" (memoize (partial car/key :carmine :mq)))

(defn clear-queues [conn-opts & qnames]
  (when (seq qnames)
    (wcar conn-opts
      (encore/backport-run!
        (fn [qname]
          (let [qk (partial qkey qname)]
            (car/del
              (qk :messages)
              (qk :locks)
              (qk :backoffs)
              (qk :nattempts)
              (qk :mid-circle)
              (qk :done)
              (qk :requeue)
              (qk :eoq-backoff?)
              (qk :ndry-runs))))
        qnames))))

(defn queue-status [conn-opts qname]
  (let [qk (partial qkey qname)]
    (zipmap [:last-mid :next-mid :messages :locks :backoffs :nattempts
             :mid-circle :done :requeue :eoq-backoff? :ndry-runs]
     (wcar conn-opts
       (car/lindex        (qk :mid-circle)  0)
       (car/lindex        (qk :mid-circle) -1)
       (car/hgetall*      (qk :messages))
       (car/hgetall*      (qk :locks))
       (car/hgetall*      (qk :backoffs))
       (car/hgetall*      (qk :nattempts))
       (car/lrange        (qk :mid-circle) 0 -1)
       (->> (car/smembers (qk :done))         (car/parse set))
       (->> (car/smembers (qk :requeue))      (car/parse set))
       (->> (car/get      (qk :eoq-backoff?)) (car/parse-bool)) ; Give TTL?
       (->> (car/get      (qk :ndry-runs))    (car/parse-int))))))

;;;; Implementation

(def message-status
  "Returns current message status, e/o:
    :queued               - Awaiting handler.
    :queued-with-backoff  - Awaiting rehandling.
    :locked               - Currently with handler.
    :locked-with-requeue  - Currently with handler, will requeue on success.
    :done-awaiting-gc     - Finished handling, awaiting GC.
    :done-with-backoff    - Finished handling, awaiting dedupe timeout.
  nil                   - Already GC'd or invalid message id."
  (let [script (encore/slurp-resource "lua/mq/msg-status.lua")]
    (fn [qname mid]
      (car/parse-keyword
        (car/lua script
          {:qk-messages (qkey qname :messages)
           :qk-locks    (qkey qname :locks)
           :qk-backoffs (qkey qname :backoffs)
           :qk-done     (qkey qname :done)
           :qk-requeue  (qkey qname :requeue)}
          {:now         (System/currentTimeMillis)
           :mid         mid})))))

(def enqueue
  "Pushes given message (any Clojure datatype) to named queue and returns unique
  message id or {:carmine.mq/error <message-status>}. Options:
    * unique-message-id  - Specify an explicit message id (e.g. message hash) to
                           perform a de-duplication check. If unspecified, a
                           unique id will be auto-generated.
    * allow-requeue?     - When true, allow buffered escrow-requeue for a
                           message in the :locked or :done-with-backoff state."
  (let [script (encore/slurp-resource "lua/mq/enqueue.lua")]
    (fn [qname message & [unique-message-id allow-requeue?]]
      (car/parse
        #(if (vector? %) (first %) {:carmine.mq/error (keyword %)})
        (car/lua script
          {:qk-messages    (qkey qname :messages)
           :qk-locks       (qkey qname :locks)
           :qk-backoffs    (qkey qname :backoffs)
           :qk-nattempts   (qkey qname :nattempts)
           :qk-mid-circle  (qkey qname :mid-circle)
           :qk-done        (qkey qname :done)
           :qk-requeue     (qkey qname :requeue)}
          {:now            (System/currentTimeMillis)
           :mid            (or unique-message-id (str (java.util.UUID/randomUUID)))
           :mcontent       (car/freeze message)
           :allow-requeue? (if allow-requeue? "true" "false")})))))

(def dequeue
  "IMPLEMENTATION DETAIL: Use `worker` instead.
  Rotates queue's mid-circle and processes next mid. Returns:
    nil             - If msg GC'd, locked, or set to backoff.
    \"eoq-backoff\" - If circle uninitialized or end-of-circle marker reached.
    [<mid> <mcontent> <attempt>] - If message should be (re)handled now."
  (let [script (encore/slurp-resource "lua/mq/dequeue.lua")]
    (fn [qname & [{:keys [lock-ms eoq-backoff-ms]
                  :or   {lock-ms (* 1000 60 60) eoq-backoff-ms exp-backoff}}]]
      (let [;; Precomp 5 backoffs so that `dequeue` can init the backoff
            ;; atomically. This is hackish, but a decent tradeoff.
            eoq-backoff-ms-vec
            (cond
              (fn?      eoq-backoff-ms) (mapv eoq-backoff-ms              (range 5))
              (integer? eoq-backoff-ms) (mapv (constantly eoq-backoff-ms) (range 5))
              :else (throw (ex-info (str "Bad eoq-backoff-ms: " eoq-backoff-ms)
                             {:eoq-backoff-ms eoq-backoff-ms})))]
        (car/lua script
          {:qk-messages    (qkey qname :messages)
           :qk-locks       (qkey qname :locks)
           :qk-backoffs    (qkey qname :backoffs)
           :qk-nattempts   (qkey qname :nattempts)
           :qk-mid-circle  (qkey qname :mid-circle)
           :qk-done        (qkey qname :done)
           :qk-requeue     (qkey qname :requeue)
           :qk-eoq-backoff (qkey qname :eoq-backoff?)
           :qk-ndry-runs   (qkey qname :ndry-runs)}
          {:now            (System/currentTimeMillis)
           :lock-ms        lock-ms
           :eoq-ms0        (nth eoq-backoff-ms-vec 0)
           :eoq-ms1        (nth eoq-backoff-ms-vec 1)
           :eoq-ms2        (nth eoq-backoff-ms-vec 2)
           :eoq-ms3        (nth eoq-backoff-ms-vec 3)
           :eoq-ms4        (nth eoq-backoff-ms-vec 4)})))))

(comment
  (clear-queues {} :q1)
  (queue-status {} :q1)
  (wcar {} (enqueue :q1 :msg1 :mid1))
  (wcar {} (message-status :q1 :mid1))
  (wcar {} (dequeue :q1))
  ;;(mapv exp-backoff (range 5))
  (wcar {} (car/pttl (qkey :q1 :eoq-backoff?))))

(defn handle1 "Implementation detail!"
  [conn-opts qname handler [mid mcontent attempt :as poll-reply]]
  (when (and poll-reply (not= poll-reply "eoq-backoff"))
    (let [qk   (partial qkey qname)
          done (fn [status mid & [backoff-ms]]
                 ;; TODO Switch to Lua script
                 (car/atomic conn-opts 100
                   (car/watch (qk :requeue))
                   (let [requeue?
                         (car/with-replies (->> (car/sismember (qk :requeue) mid)
                                                (car/parse-bool)))
                         status (if (and (= status :success) requeue?)
                                  :requeue status)]
                     (car/multi)
                     (when backoff-ms ; Retry or dedupe backoff, depending on type
                       (car/hset (qk :backoffs) mid (+ (System/currentTimeMillis)
                                                       backoff-ms)))

                     (car/hdel (qk :locks) mid)
                     (case status
                       (:success :error) (car/sadd (qk :done) mid)
                       :requeue          (do (car/srem (qk :requeue)   mid)
                                             (car/hdel (qk :nattempts) mid))
                       nil))))

          error (fn [mid poll-reply & [throwable]]
                  (done :error mid)
                  (timbre/errorf
                    (if throwable throwable
                      (ex-info ":error handler response" {}))
                    "Error handling %s queue message:\n%s" qname poll-reply))

          {:keys [status throwable backoff-ms]}
          (let [result (try (handler {:mid mid :message mcontent :attempt attempt})
                            (catch Throwable t {:status :error :throwable t}))]
            (when (map? result) result))]

      (case status
        :success (done status mid backoff-ms)
        :retry   (done status mid backoff-ms)
        :error   (error mid poll-reply throwable)
        (do (done :success mid) ; For backwards-comp with old API
            (timbre/warnf "Invalid handler status: %s" status))))))

;;;; Workers

(defprotocol IWorker (start [this]) (stop [this]))
(defrecord    Worker [conn-opts qname running? opts]
  java.io.Closeable (close [this] (stop this))
  IWorker
  (stop [_]
    (let [stopped? @running?]
      (reset! running? false)
      (when stopped? (timbre/infof "Message queue worker stopped: %s" qname))
      stopped?))

  (start [_]
    (when-not @running?
      (timbre/infof "Message queue worker starting: %s" qname)
      (reset! running? true)
      (let [{:keys [handler monitor nthreads throttle-ms]} opts
            qk (partial qkey qname)
            start-polling-loop!
            (fn []
              (loop [nerrors 0]
                (when @running?
                  (let [?error
                        (try
                          (let [[poll-reply ndruns mid-circle-size]
                                (wcar conn-opts
                                  (dequeue qname opts)
                                  (car/get  (qk :ndry-runs))
                                  (car/llen (qk :mid-circle)))]

                            (when monitor
                              (monitor {:mid-circle-size mid-circle-size
                                        :ndry-runs       (or ndruns 0)
                                        :poll-reply      poll-reply}))

                            (if (= poll-reply "eoq-backoff")
                              (Thread/sleep
                               (max (wcar conn-opts (car/pttl (qk :eoq-backoff?)))
                                    10))
                              (handle1 conn-opts qname handler poll-reply))

                            (when (and throttle-ms (> throttle-ms 0))
                              (Thread/sleep throttle-ms)))
                          nil ; Successful worker loop
                          (catch Throwable t t))]

                    (if-not ?error (recur 0)
                      (let [t ?error]
                        (timbre/errorf t "Worker error! Will backoff & retry.")
                        (Thread/sleep (exp-backoff (inc nerrors)))
                        (recur (inc nerrors))))))))]

        (dorun (repeatedly nthreads (fn [] (future (start-polling-loop!))))))
      true)))

(defn monitor-fn
  "Returns a worker monitor fn that warns when queue's mid-circle exceeds
  the prescribed size. A backoff timeout can be provided to rate-limit this
  warning."
  [qname max-circle-size warn-backoff-ms]
  (let [udt-last-warning_ (atom nil)]
    (fn [{:keys [mid-circle-size]}]
      (let [instant (System/currentTimeMillis)]
        (when (and (> mid-circle-size max-circle-size)
                   (> (- instant (or @udt-last-warning_ 0))
                      (or warn-backoff-ms 0)))
          (reset! udt-last-warning_ instant)
          (timbre/warnf "Message queue size warning: %s (mid-circle-size: %s)"
                        qname max-circle-size))))))

(defn worker
  "Returns a threaded worker to poll for and handle messages `enqueue`'d to
  named queue. Options:
   :handler        - (fn [{:keys [mid message attempt]}]) that throws an ex
                     or returns {:status     <#{:success :error :retry}>
                                 :throwable  <Throwable>
                                 :backoff-ms <retry-or-dedupe-backoff-ms}.
   :monitor        - (fn [{:keys [mid-circle-size ndry-runs poll-reply]}])
                     called on each worker loop iteration. Useful for queue
                     monitoring/logging. See also `monitor-fn`.
   :lock-ms        - Max time handler may keep a message before handler
                     considered fatally stalled and message re-queued. Must be
                     sufficiently high to prevent double handling.
   :eoq-backoff-ms - Thread sleep period each time end of queue is reached.
                     Can be a (fn [ndry-runs]) -> ms (n<=5) will be used.
                     Sleep synchronized for all queue workers.
   :nthreads       - Number of synchronized worker threads to use.
   :throttle-ms    - Thread sleep period between each poll."
  [conn-opts qname &
   [{:keys [handler monitor lock-ms eoq-backoff-ms nthreads
            throttle-ms auto-start] :as opts
     :or   {handler (fn [args] (timbre/infof "%s" args) {:status :success})
            monitor (monitor-fn qname 1000 (* 1000 60 60 6))
            lock-ms        (encore/ms :hours 1)
            nthreads       1
            throttle-ms    200
            eoq-backoff-ms exp-backoff
            auto-start     true}}]]
  (let [w (Worker. conn-opts qname (atom false)
            {:handler        handler
             :monitor        monitor
             :lock-ms        lock-ms
             :eoq-backoff-ms eoq-backoff-ms
             :nthreads       nthreads
             :throttle-ms    throttle-ms})]

    (let [;; For backwards-compatibility with old API:
          auto-start (if-not (contains? opts :auto-start?) auto-start
                       (:auto-start? opts))]
      (when auto-start
        (if (integer? auto-start)
          (future (Thread/sleep auto-start) (start w))
          (start w))))
    w))

;;;; Renamed/deprecated

(defn make-dequeue-worker "DEPRECATED: Use `worker` instead."
  [pool spec & {:keys [handler-fn handler-ttl-msecs backoff-msecs throttle-msecs
                       auto-start?]}]
  (worker {:pool pool :spec spec}
    (merge (when-let [ms handler-ttl-msecs] {:lock-ms        ms})
           (when-let [ms backoff-msecs]     {:eoq-backoff-ms ms})
           (when-let [ms throttle-msecs]    {:throttle-ms    ms})
           (when-let [hf handler-fn]
             {:handler (fn [{:keys [message]}]
                         {:status (or (#{:success :error :retry} (hf message))
                                      :success)})})
           {:auto-start? auto-start?})))
