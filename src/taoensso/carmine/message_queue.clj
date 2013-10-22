(ns taoensso.carmine.message-queue
  "Carmine-backed Clojure message queue. All heavy lifting by Redis (2.6+).
  Simple implementation. Very simple API. Reliable. Fast.

  Redis keys:
    * carmine:mq:<qname>:messages      -> hash, {mid mcontent}.
    * carmine:mq:<qname>:locks         -> hash, {mid lock-expiry-time}.
    * carmine:mq:<qname>:backoffs      -> hash, {mid backoff-expiry-time}.
    * carmine:mq:<qname>:nretries      -> hash, {mid retry-count}.
    * carmine:mq:<qname>:mid-circle    -> list, rotating list of mids.
    * carmine:mq:<qname>:recently-done -> set, for efficient mid removal from circle.
    * carmine:mq:<qname>:eoq-backoff?  -> ttl flag, used for queue-wide (every-worker)
                                          polling backoff.
    * carmine:mq:<qname>:ndry-runs     -> int, number of times worker(s) have burnt
                                          through queue w/o work to do.

  Ref. http://antirez.com/post/250 for basic implementation details."
  {:author "Peter Taoussanis"}
  (:require [clojure.string   :as str]
            [taoensso.carmine :as car :refer (wcar)]
            [taoensso.timbre  :as timbre]))

;;;; Public utils

(defn exp-backoff "Returns binary exponential backoff value."
  [attempt & [{:keys [factor min max]
               :or   {factor 2000}}]]
  (let [binary-exp (Math/pow 2 (dec attempt))
        time (* (+ binary-exp (rand binary-exp)) 0.5 factor)]
    (long (let [time (if min (clojure.core/max min time) time)
                time (if max (clojure.core/min max time) time)]
            time))))

(comment (map #(exp-backoff % {}) (range 10)))

;;;; Implementation

(def qkey "Prefixed queue key" (memoize (partial car/key :carmine :mq)))

(defn clear-queues [conn & qnames]
  (wcar conn
    (doseq [qname qnames]
      (when-let [qks (seq (wcar conn (car/keys (qkey qname :*))))]
        (apply car/del qks)))))

(defn queue-status [conn qname]
  (let [qk (partial qkey qname)]
    (zipmap [:messages :locks :backoffs :nretries :mid-circle :recently-done
             :eoq-backoff? :ndry-runs]
     (wcar conn
       (car/hgetall*      (qk :messages))
       (car/hgetall*      (qk :locks))
       (car/hgetall*      (qk :backoffs))
       (car/hgetall*      (qk :nretries))
       (car/lrange        (qk :mid-circle) 0 -1)
       (->> (car/smembers (qk :recently-done)) (car/parse set))
       (->> (car/get      (qk :eoq-backoff?))  (car/parse-bool))
       (->> (car/get      (qk :ndry-runs))     (car/parse-long))))))

(defn message-status
  "Returns current message status, e/o:
    :queued        - Waiting for processing.
    :locked        - Currently processing.
    :backoff       - Waiting for retry after handler-imposed backoff.
    :recently-done - Finished processing, waiting for GC.
    nil            - Unknown status, or invalid message id."
  [conn qname mid]
  (wcar conn
    (car/parse-keyword
     (car/lua
      "if redis.call('sismember', _:qk-recently-done, _:mid) == 1 then
         return 'recently-done'
       else
         local now         = tonumber(_:now)
         local lock_exp    = tonumber(redis.call('hget', _:qk-locks,    _:mid) or 0)
         local backoff_exp = tonumber(redis.call('hget', _:qk-backoffs, _:mid) or 0)

         if     (now < lock_exp)    then return 'locked'
         elseif (now < backoff_exp) then return 'backoff'
         elseif redis.call('hexists',_:qk-messages, _:mid) == 1 then
           return 'queued'
         end
         return nil
       end"
      {:qk-messages      (qkey qname :messages)
       :qk-locks         (qkey qname :locks)
       :qk-backoffs      (qkey qname :backoffs)
       :qk-recently-done (qkey qname :recently-done)}
      {:now (System/currentTimeMillis)
       :mid mid}))))

(defn enqueue
  "Pushes given message (any Clojure datatype) to named queue and returns a
  unique message id."
  [qname message]
  (car/lua
   "redis.call('hset', _:qk-messages, _:mid, _:mcontent)

    -- lpushnx end-of-circle marker to ensure an initialized mid-circle
    if redis.call('exists', _:qk-mid-circle) == 0 then
      redis.call('lpush', _:qk-mid-circle, 'end-of-circle')
    end

    redis.call('lpush', _:qk-mid-circle, _:mid)
    return _:mid"
   {:qk-messages   (qkey qname :messages)
    :qk-mid-circle (qkey qname :mid-circle)}
   {:mid           (str (java.util.UUID/randomUUID))
    :mcontent      (car/freeze message)}))

(defn dequeue
  "IMPLEMENTATION DETAIL: Use `worker` instead.
  Rotates queue's mid-circle and processes next mid. Returns:
    nil             - If msg locked, recently GC'd, or set to backoff.
    \"eoq-backoff\" - If circle uninitialized or end-of-circle marker reached.
    [<mid> <mcontent> <attempt-count>] - If message should be (re)handled now."
  [qname & [{:keys [lock-ms eoq-backoff-ms]
             :or   {lock-ms (* 1000 60 60)
                    eoq-backoff-ms 2000}}]]
  (car/lua
   "if redis.call('exists', _:qk-eoq-backoff) == 1 then
      return 'eoq-backoff'
    else
      -- TODO Waiting for Lua brpoplpush support to get us long polling
      local mid = redis.call('rpoplpush', _:qk-mid-circle, _:qk-mid-circle)

      if (not mid) or (mid == 'end-of-circle') then
        -- Set queue-wide polling backoff flag
        redis.call('psetex', _:qk-eoq-backoff, _:eoq-backoff-ms, 'true')
        redis.call('incr', _:qk-ndry-runs)
        return 'eoq-backoff'
      elseif redis.call('sismember', _:qk-recently-done, mid) == 1 then -- GC
        redis.call('lrem', _:qk-mid-circle, 1, mid) -- Efficient here
        redis.call('srem', _:qk-recently-done, mid)
        redis.call('hdel', _:qk-messages,      mid)
        redis.call('hdel', _:qk-locks,         mid)
        redis.call('hdel', _:qk-nretries,      mid)
        redis.call('hdel', _:qk-backoffs,      mid)
        return nil
      end

      redis.call('set', _:qk-ndry-runs, 0)

      local now         = tonumber(_:now)
      local lock_exp    = tonumber(redis.call('hget', _:qk-locks,    mid) or 0)
      local backoff_exp = tonumber(redis.call('hget', _:qk-backoffs, mid) or 0)

      if (now < lock_exp) or (now < backoff_exp) then
        return nil -- Has active lock or backoff
      else
        -- (Re)acquire lock
        redis.call('hset', _:qk-locks, mid, now + tonumber(_:lock-ms))
      end

      local retries = 0
      if (lock_exp ~= 0) then
        retries = tonumber(redis.call('hincrby', _:qk-nretries, mid))
      else
        retries = tonumber(redis.call('hget', _:qk-nretries, mid) or 0)
      end

      local mcontent = redis.call('hget', _:qk-messages, mid)
      local attempts = retries + 1
      return {mid, mcontent, attempts}
    end"
   {:qk-messages      (qkey qname :messages)
    :qk-locks         (qkey qname :locks)
    :qk-backoffs      (qkey qname :backoffs)
    :qk-nretries      (qkey qname :nretries)
    :qk-mid-circle    (qkey qname :mid-circle)
    :qk-recently-done (qkey qname :recently-done)
    :qk-eoq-backoff   (qkey qname :eoq-backoff?)
    :qk-ndry-runs     (qkey qname :ndry-runs)}
   {:now              (System/currentTimeMillis)
    :lock-ms          lock-ms
    :eoq-backoff-ms   eoq-backoff-ms}))

(comment
  (clear-queues {} "myq")
  (queue-status {} "myq")
  (let [mid (wcar {} (enqueue "myq" "msg"))]
    (message-status {} "myq" mid))

  (wcar {} (dequeue "myq")))

;;;; Workers

(defprotocol IWorker (start [this]) (stop [this]))

(defrecord Worker [conn qname running? opts]
  IWorker
  (stop  [_] (let [stopped? @running?] (reset! running? false) stopped?))
  (start [_]
    (when-not @running?
      (reset! running? true)
      (future
        (let [{:keys [handler monitor throttle-ms eoq-backoff-ms]} opts
              qk    (partial qkey qname)
              done  (fn [mid] (wcar conn (car/sadd (qk :recently-done) mid)))
              retry (fn [mid & [backoff-ms]]
                      (wcar conn
                       (when backoff-ms
                         (car/hset (qk :backoffs) mid (+ (System/currentTimeMillis)
                                                         backoff-ms)))
                        (car/hdel (qk :locks) mid)))
              error (fn [mid poll-reply & [throwable]]
                      (done mid)
                      (timbre/error
                       (if throwable throwable (Exception. ":error handler response"))
                       (str "Error handling queue message: " qname "\n" poll-reply)))]

          (while @running?
            (try
              (let [[ndruns mid-circle-size] (wcar conn (car/get  (qk :ndry-runs))
                                                        (car/llen (qk :mid-circle)))
                    ndruns (or (car/as-long ndruns) 0)
                    eoq-backoff-ms* (if (fn? eoq-backoff-ms)
                                      (eoq-backoff-ms (inc ndruns))
                                      eoq-backoff-ms)
                    opts* (assoc opts :eoq-backoff-ms eoq-backoff-ms*)]

                (when monitor (monitor {:mid-circle-size mid-circle-size
                                        :ndry-runs       ndruns}))

                (when-let [[mid mcontent attempt :as poll-reply]
                           (wcar conn (dequeue qname opts*))]
                  (if (= poll-reply "eoq-backoff")
                    (when eoq-backoff-ms* (Thread/sleep eoq-backoff-ms*))
                    (let [{:keys [status throwable backoff-ms]}
                          (let [result (try (handler {:message mcontent
                                                      :attempt attempt})
                                            (catch Throwable t {:status :error
                                                                :throwable t}))]
                            (when (map? result) result))]
                      (case status
                        :success (done mid)
                        :retry   (retry mid backoff-ms)
                        :error   (error mid poll-reply throwable)
                        (do (done mid)
                            (timbre/warn (str "Invalid handler status:" status))))))))
              (catch Throwable t
                (timbre/fatal t "Worker error!")
                (throw t)))
            (when throttle-ms (Thread/sleep throttle-ms)))))
      true))

  java.io.Closeable
  (close [this] (stop this)))

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
   :handler        - (fn [{:keys [message attempt]}]) that throws an exception
                     or returns {:status     <#{:success :error :retry}>
                                 :throwable  <Throwable>
                                 :backoff-ms <retry-backoff-ms}.
   :monitor        - (fn [{:keys [mid-circle-size ndry-runs]}]) called on each
                     worker loop iteration. Useful for queue monitoring/logging.
                     See also `monitor-fn`.
   :lock-ms        - Max time handler may keep a message before handler
                     considered fatally stalled and message re-queued. Must be
                     sufficiently high to prevent double handling.
   :eoq-backoff-ms - Thread sleep period each time end of queue is reached.
                     Can be a (fn [ndry-runs]) => ms. Sleep synchronized for all
                     queue workers.
   :throttle-ms    - Thread sleep period between each poll."
  [conn qname & [{:keys [handler monitor lock-ms eoq-backoff-ms throttle-ms
                         auto-start?]
                  :or   {handler (fn [{:keys [message attempt]}]
                                   (timbre/info qname message attempt)
                                   {:status :success})
                         monitor (monitor-fn qname 1000 (* 1000 60 60))
                         lock-ms        (* 1000 60 60)
                         throttle-ms    200
                         eoq-backoff-ms (fn [ndruns] (exp-backoff ndruns {:max 10000}))
                         auto-start?    true}}]]
  (let [w (->Worker conn qname (atom false)
                    {:handler        handler
                     :monitor        monitor
                     :lock-ms        lock-ms
                     :eoq-backoff-ms eoq-backoff-ms
                     :throttle-ms    throttle-ms})]
    (when auto-start? (start w)) w))

(comment
  (def w1 (worker {} "myq"))
  (wcar {} (enqueue "myq" "msg"))
  (stop w1)
  (queue-status {} "myq"))

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
