(ns taoensso.carmine.message-queue
  "Carmine-backed Clojure message queue. All heavy lifting by Redis (2.6+).
  Simple implementation. Very simple API. Reliable. Fast.

  Redis keys:
    * carmine:mq:<qname>:messages     -> hash, {mid mcontent}.
    * carmine:mq:<qname>:locks        -> hash, {mid lock-expiry-time}.
    * carmine:mq:<qname>:backoffs     -> hash, {mid backoff-expiry-time}.
    * carmine:mq:<qname>:nattempts    -> hash, {mid attempt-count}.
    * carmine:mq:<qname>:mid-circle   -> list, rotating list of mids.
    * carmine:mq:<qname>:gc           -> set, for efficient mid removal from circle.
    * carmine:mq:<qname>:eoq-backoff? -> ttl flag, used for queue-wide (every-worker)
                                         polling backoff.
    * carmine:mq:<qname>:ndry-runs    -> int, number of times worker(s) have burnt
                                         through queue w/o work to do.

  Ref. http://antirez.com/post/250 for basic implementation details."
  {:author "Peter Taoussanis"}
  (:require [clojure.string   :as str]
            [taoensso.carmine :as car :refer (wcar)]
            [taoensso.timbre  :as timbre]))

;; TODO Redis 2.8+ Pub/Sub notifications could be used to more efficiently
;; coordinate worker backoffs. This'd allow us to essentially go from a polling
;; design to an evented design even w/o the need for a Lua brpoplpush.
;;
;; Note that we'd want the Pub/Sub notifier _in addition to_ the current
;; rpoplpush design: both for atomicity and reliability (Pub/Sub doesn't
;; currently have reliability guarantees).

;;;; Public utils

(defn exp-backoff "Returns binary exponential backoff value."
  [attempt & [{:keys [factor min max] :or {factor 2200}}]]
  (let [binary-exp (Math/pow 2 (dec attempt))
        time (* (+ binary-exp (rand binary-exp)) 0.5 factor)]
    (long (let [time (if min (clojure.core/max min time) time)
                time (if max (clojure.core/min max time) time)]
            time))))

(comment (mapv #(exp-backoff % {}) (range 5)))

;;;; Admin

(def qkey "Prefixed queue key" (memoize (partial car/key :carmine :mq)))

(defn clear-queues [conn & qnames]
  (wcar conn
    (doseq [qname qnames]
      (when-let [qks (seq (wcar conn (car/keys (qkey qname :*))))]
        (apply car/del qks)))))

(defn queue-status [conn qname]
  (let [qk (partial qkey qname)]
    (zipmap [:messages :locks :backoffs :nattempts :mid-circle :gc
             :eoq-backoff? :ndry-runs]
     (wcar conn
       (car/hgetall*      (qk :messages))
       (car/hgetall*      (qk :locks))
       (car/hgetall*      (qk :backoffs))
       (car/hgetall*      (qk :nattempts))
       (car/lrange        (qk :mid-circle) 0 -1)
       (->> (car/smembers (qk :gc))            (car/parse set))
       (->> (car/get      (qk :eoq-backoff?))  (car/parse-bool)) ; Give TTL?
       (->> (car/get      (qk :ndry-runs))     (car/parse-long))))))

;;;; Implementation

(defn message-status
  "Returns current message status, e/o:
    :queued            - Awaiting (re)handling.
    :locked            - Currently with handler.
    :retry-backoff     - Awaiting backoff for handler retry (rehandling).
    :done-with-backoff - Finished handling, awaiting dedupe timeout.
    :done-awaiting-gc  - Finished handling, no dedupe timeout, awaiting GC.
    nil                - Already GC'd or invalid message id."
  [qname mid]
  (car/parse-keyword
   (car/lua ; Careful, logic here is necessarily quite subtle!
    "--
    local now         = tonumber(_:now)
    local lock_exp    = tonumber(redis.call('hget', _:qk-locks,    _:mid) or 0)
    local backoff_exp = tonumber(redis.call('hget', _:qk-backoffs, _:mid) or 0)

    if redis.call('hexists', _:qk-messages, _:mid) ~= 1 then
      if (now < backoff_exp) then return 'done-with-backoff' end
      return nil
    else -- Necessary for `enqueue`
      if redis.call('sismember', _:qk-gc, _:mid) == 1 then
        if (now < backoff_exp) then return 'done-with-backoff' end
        return 'done-awaiting-gc'
      else -- Necessary for `enqueue`
        if (now < lock_exp)    then return 'locked'        end
        if (now < backoff_exp) then return 'retry-backoff' end
        return 'queued'
      end
    end"
    {:qk-messages (qkey qname :messages)
     :qk-locks    (qkey qname :locks)
     :qk-backoffs (qkey qname :backoffs)
     :qk-gc       (qkey qname :gc)}
    {:now (System/currentTimeMillis)
     :mid mid})))

(defn enqueue
  "Pushes given message (any Clojure datatype) to named queue and returns unique
  message id or {:carmine.mq/error #{<:queued :locked :retry-backoff
                                      :done-with-backoff>}}.

  For deduplication provide an explicit message id (e.g. message hash), otherwise
  a unique message id will automatically be generated."
  [qname message & [unique-message-id]]
  (->>
   (car/lua
    "--- Check for dupes (inline `message-status` logic)
    local now         = tonumber(_:now)
    local lock_exp    = tonumber(redis.call('hget', _:qk-locks,    _:mid) or 0)
    local backoff_exp = tonumber(redis.call('hget', _:qk-backoffs, _:mid) or 0)

    if redis.call('hexists', _:qk-messages, _:mid) ~= 1 then
      if (now < backoff_exp) then return 'done-with-backoff' end
      -- return nil
    else
      if redis.call('sismember', _:qk-gc, _:mid) == 1 then
        if (now < backoff_exp) then return 'done-with-backoff' end
        -- return 'done-awaiting-gc'
      else
        if (now < lock_exp)    then return 'locked'        end
        if (now < backoff_exp) then return 'retry-backoff' end
        return 'queued'
      end
    end
    ---

    redis.call('hset', _:qk-messages, _:mid, _:mcontent)

    -- lpushnx end-of-circle marker to ensure an initialized mid-circle
    if redis.call('exists', _:qk-mid-circle) ~= 1 then
      redis.call('lpush', _:qk-mid-circle, 'end-of-circle')
    end

    redis.call('lpush', _:qk-mid-circle, _:mid)
    return {_:mid} -- Wrap to distinguish from error replies
" ; Necessary for Lua script-end weirdness
    {:qk-messages   (qkey qname :messages)
     :qk-locks      (qkey qname :locks)
     :qk-backoffs   (qkey qname :backoffs)
     :qk-mid-circle (qkey qname :mid-circle)
     :qk-gc         (qkey qname :gc)}
    {:now           (System/currentTimeMillis)
     :mid           (or unique-message-id (str (java.util.UUID/randomUUID)))
     :mcontent      (car/freeze message)})
   (car/parse #(if (vector? %) (first %) {:carmine.mq/error (keyword %)}))))

(defn dequeue
  "IMPLEMENTATION DETAIL: Use `worker` instead.
  Rotates queue's mid-circle and processes next mid. Returns:
    nil             - If msg already GC'd, locked, or set to backoff.
    \"eoq-backoff\" - If circle uninitialized or end-of-circle marker reached.
    [<mid> <mcontent> <attempt-count>] - If message should be (re)handled now."
  [qname & [{:keys [lock-ms eoq-backoff-ms]
             :or   {lock-ms (* 1000 60 60) eoq-backoff-ms exp-backoff}}]]
  (let [;; Precomp 5 backoffs so that `dequeue` can init the backoff
        ;; atomically. This is hackish, but a decent tradeoff.
        eoq-backoff-ms-vec
        (cond (fn?      eoq-backoff-ms) (mapv eoq-backoff-ms              (range 5))
              (integer? eoq-backoff-ms) (mapv (constantly eoq-backoff-ms) (range 5))
              :else (throw (Exception. (str "Bad eoq-backoff-ms: " eoq-backoff-ms))))]
    (car/lua
     "if redis.call('exists', _:qk-eoq-backoff) == 1 then return 'eoq-backoff' end

     -- TODO Waiting for Lua brpoplpush support to get us long polling
     local mid = redis.call('rpoplpush', _:qk-mid-circle, _:qk-mid-circle)
     local now = tonumber(_:now)

     if (not mid) or (mid == 'end-of-circle') then -- Uninit'd or eoq

       -- Calculate eoq_backoff_ms
       local ndry_runs = tonumber(redis.call('get', _:qk-ndry-runs) or 0)
       local eoq_ms_tab = {_:eoq-ms0, _:eoq-ms1, _:eoq-ms2, _:eoq-ms3, _:eoq-ms4}
       local eoq_backoff_ms = tonumber(eoq_ms_tab[math.min(5, (ndry_runs + 1))])

       -- Set queue-wide polling backoff flag
       redis.call('psetex', _:qk-eoq-backoff, eoq_backoff_ms, 'true')
       redis.call('incr',   _:qk-ndry-runs)

       -- Prune expired backoffs. We have to do this manually if we want to keep
       -- backoffs in a hash as we do for efficiency & Cluster support. This hash
       -- is usu. quite small since it consists only of active retry and dedupe
       -- backoffs.
       for i,k in pairs(redis.call('hkeys', _:qk-backoffs)) do
         if (now >= tonumber(redis.call('hget', _:qk-backoffs, k))) then
           redis.call('hdel', _:qk-backoffs, k)
         end
       end

       return 'eoq-backoff'
     end

     if redis.call('sismember', _:qk-gc, mid) == 1 then -- GC
       redis.call('lrem', _:qk-mid-circle, 1, mid) -- Efficient here
       redis.call('srem', _:qk-gc,            mid)
       redis.call('hdel', _:qk-messages,      mid)
       redis.call('hdel', _:qk-locks,         mid)
       --redis.call('hdel', _:qk-backoffs,    mid) -- No! Will use as dedupe backoff
       redis.call('hdel', _:qk-nattempts,     mid)
       redis.call('set',  _:qk-ndry-runs, 0) -- Did work
       return nil
     end

     local lock_exp    = tonumber(redis.call('hget', _:qk-locks,    mid) or 0)
     local backoff_exp = tonumber(redis.call('hget', _:qk-backoffs, mid) or 0)

     -- Active lock/backoff
     if (now < lock_exp) or (now < backoff_exp) then return nil end

     redis.call('hset', _:qk-locks,    mid, now + tonumber(_:lock-ms)) -- Acquire
     redis.call('hdel', _:qk-backoffs, mid) -- Expired, so prune (keep gc-prune fast)
     redis.call('set',  _:qk-ndry-runs, 0) -- Did work

     local mcontent  = redis.call('hget',    _:qk-messages,  mid)
     local nattempts = redis.call('hincrby', _:qk-nattempts, mid, 1)
     return {mid, mcontent, nattempts}"
     {:qk-messages    (qkey qname :messages)
      :qk-locks       (qkey qname :locks)
      :qk-backoffs    (qkey qname :backoffs)
      :qk-nattempts   (qkey qname :nattempts)
      :qk-mid-circle  (qkey qname :mid-circle)
      :qk-gc          (qkey qname :gc)
      :qk-eoq-backoff (qkey qname :eoq-backoff?)
      :qk-ndry-runs   (qkey qname :ndry-runs)}
     {:now            (System/currentTimeMillis)
      :lock-ms        lock-ms
      :eoq-ms0        (nth eoq-backoff-ms-vec 0)
      :eoq-ms1        (nth eoq-backoff-ms-vec 1)
      :eoq-ms2        (nth eoq-backoff-ms-vec 2)
      :eoq-ms3        (nth eoq-backoff-ms-vec 3)
      :eoq-ms4        (nth eoq-backoff-ms-vec 4)})))

(comment
  (clear-queues {} :q1)
  (queue-status {} :q1)
  (wcar {} (enqueue :q1 :msg1 :mid1))
  (wcar {} (message-status :q1 :mid1))
  (wcar {} (dequeue :q1))
  ;;(mapv exp-backoff (range 5))
  (wcar {} (car/pttl (qkey :q1 :eoq-backoff?))))

(defn handle1 "Implementation detail!"
  [conn qname handler [mid mcontent attempt :as poll-reply]]
  (when (and poll-reply (not= poll-reply "eoq-backoff"))
    (let [qk   (partial qkey qname)
          done (fn [status mid & [backoff-ms]]
                 (wcar conn
                   (when backoff-ms ; Retry or dedupe backoff, depending on type
                     (car/hset (qk :backoffs) mid (+ (System/currentTimeMillis)
                                                     backoff-ms)))
                   (case status
                     :success (car/sadd (qk :gc)    mid) ; Queue for GC
                     :retry   (car/hdel (qk :locks) mid) ; Unlock
                     )))

          error (fn [mid poll-reply & [throwable]]
                  (done mid)
                  (timbre/errorf
                   (if throwable throwable (Exception. ":error handler response"))
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
(defrecord    Worker [conn qname running? opts]
  java.io.Closeable (close [this] (stop this))
  IWorker
  (stop  [_] (let [stopped? @running?] (reset! running? false) stopped?))
  (start [_]
    (when-not @running?
      (reset! running? true)
      (let [{:keys [handler monitor nthreads throttle-ms _]} opts
            qk (partial qkey qname)
            start-polling-loop!
            (fn []
              (try
                (while @running?
                  (let [[poll-reply ndruns mid-circle-size]
                        (wcar conn (dequeue qname opts)
                                   (car/get  (qk :ndry-runs))
                                   (car/llen (qk :mid-circle)))]

                    (when monitor (monitor {:mid-circle-size mid-circle-size
                                            :ndry-runs       (or ndruns 0)
                                            :poll-reply      poll-reply}))

                    (if (= poll-reply "eoq-backoff")
                      (Thread/sleep (max (wcar conn (car/pttl (qk :eoq-backoff?)))
                                         10))
                      (handle1 conn qname handler poll-reply)))
                  (when throttle-ms (Thread/sleep throttle-ms)))
                (catch Throwable t (timbre/fatalf t "Worker error!") (throw t))))]
        (dorun (repeatedly nthreads (fn [] (future (start-polling-loop!))))))
      true)))

(defn monitor-fn
  "Returns a worker monitor fn that warns when queue's mid-circle exceeds
  the prescribed size. A backoff timeout can be provided to rate-limit this
  warning."
  [qname max-circle-size warn-backoff-ms]
  (let [udt-last-warning (atom nil)]
    (fn [{:keys [mid-circle-size]}]
      (let [instant (System/currentTimeMillis)]
        (when (and (> mid-circle-size max-circle-size)
                   (> (- instant (or @udt-last-warning 0))
                      (or warn-backoff-ms 0)))
          (reset! udt-last-warning instant)
          (timbre/warnf "Message queue size warning: %s (mid-circle-size: %s)"
                        qname max-circle-size))))))

(defn worker
  "Returns a threaded worker to poll for and handle messages `enqueue`'d to
  named queue. Options:
   :handler        - (fn [{:keys [mid message attempt]}]) that throws an exception
                     or returns {:status     <#{:success :error :retry}>
                                 :throwable  <Throwable>
                                 :backoff-ms <retry-or-dedupe-backoff-ms}.
   :monitor        - (fn [{:keys [mid-circle-size ndry-runs poll-reply]}]) called
                     on each worker loop iteration. Useful for queue
                     monitoring/logging. See also `monitor-fn`.
   :lock-ms        - Max time handler may keep a message before handler
                     considered fatally stalled and message re-queued. Must be
                     sufficiently high to prevent double handling.
   :eoq-backoff-ms - Thread sleep period each time end of queue is reached.
                     Can be a (fn [ndry-runs]) -> ms (n<=5) will be used.
                     Sleep synchronized for all queue workers.
   :nthreads       - Number of synchronized worker threads to use.
   :throttle-ms    - Thread sleep period between each poll."
  [conn qname & [{:keys [handler monitor lock-ms eoq-backoff-ms nthreads
                         throttle-ms auto-start?]
                  :or   {handler (fn [args] (timbre/infof "%s" args)
                                           {:status :success})
                         monitor (monitor-fn qname 1000 (* 1000 60 60 6))
                         lock-ms        (* 1000 60 60)
                         nthreads       1
                         throttle-ms    200
                         eoq-backoff-ms exp-backoff
                         auto-start?    true}}]]
  (let [w (->Worker conn qname (atom false)
                    {:handler        handler
                     :monitor        monitor
                     :lock-ms        lock-ms
                     :eoq-backoff-ms eoq-backoff-ms
                     :nthreads       nthreads
                     :throttle-ms    throttle-ms})]
    (when auto-start? (start w)) w))

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
