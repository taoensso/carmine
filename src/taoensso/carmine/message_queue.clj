(ns taoensso.carmine.message-queue
  "Alpha - subject to change.
  Carmine-backed Clojure message queue. All heavy lifting by Redis (2.6+).
  Simple implementation. Very simple API. Reliable. Fast.

  Redis keys:
    * carmine:mq:<qname>:messages      -> hash, {id content}
    * carmine:mq:<qname>:locks         -> hash, {id lock-acquired-time}
    * carmine:mq:<qname>:id-circle     -> list, rotating list of ids
    * carmine:mq:<qname>:recently-done -> set, used for efficient id removal from
                                          circle
    * carmine:mq:<qname>:backoff?      -> ttl flag, used for queue-wide (every-worker)
                                          polling backoff

  Ref. http://antirez.com/post/250 for implementation details."
  {:author "Peter Taoussanis"}
  (:require [clojure.string   :as str]
            [taoensso.carmine :as car :refer (wcar)]
            [taoensso.timbre  :as timbre])
  (:import  [taoensso.carmine.connections ConnectionPool]
            [java.util UUID]))

;; TODO This ns is badly in need of a (breaking) refactor!
;; TODO Add message-backoff feature (exponential-backoff retries, etc.).

(def qkey "Prefixed queue key" (memoize (partial car/kname "carmine" "mq")))

(defn status
  "Returns current job status, or nil if unknown."
  [qname msg-id & {:keys [handler-ttl-msecs]
                   :or   {handler-ttl-msecs (* 60 60 1000)}}]
  (car/lua-script
   "if redis.call('sismember', _:qk-recently-done, _:msg-id) == 1 then
      return 'done' -- known temporarily until a worker cleans it up
    else
      local current_time  = tonumber(_:current-time)
      local lock_ttl      = tonumber(_:handler-ttl-msecs)
      local lock_acquired = tonumber(redis.call('hget', _:qk-locks, _:msg-id) or 0)
      if (lock_acquired ~= 0) and (current_time - lock_acquired) < lock_ttl then
        return 'processing'
      elseif redis.call('hexists',_:qk-messages, _:msg-id) == 1 then
        return 'pending'
      end
      return nil
    end"
    {:qk-messages       (qkey qname "messages")
     :qk-locks          (qkey qname "locks")
     :qk-recently-done  (qkey qname "recently-done")}
    {:msg-id            msg-id
     :current-time      (System/currentTimeMillis)
     :handler-ttl-msecs handler-ttl-msecs}))

(defn enqueue
  "Pushes given message (any Clojure datatype) to named queue. Returns message
  id and the number of messages currently queued or recently dequeued."
  [qname message]
  (car/lua-script
   "redis.call('hset', _:qk-messages, _:msg-id, _:msg-content)

    -- lpushnx EOC sentinel to ensure an initialized id-circle
    if redis.call('exists', _:qk-id-circle) == 0 then
      redis.call('lpush', _:qk-id-circle, 'end-of-circle')
    end

    return {_:msg-id, redis.call('lpush', _:qk-id-circle, _:msg-id)}"
   {:qk-messages  (qkey qname "messages")
    :qk-id-circle (qkey qname "id-circle")}
   {:msg-id       (str (UUID/randomUUID))
    :msg-content  (car/serialize message)}))

(defn dequeue-1
  "Rotates queue's id-circle and processes next id. Returns:
    * nil if id is locked or was just garbage collected (i.e. previously marked
      as done).
    * \"backoff\" if circle is uninitialized or sentinel (end of circle) has
      been reached.
    * [message-id message \"new\"] or [message-id message \"retry\"] if message
      should be (re)handled now.

  Exposes implementation details: prefer `make-dequeue-worker` when possible."
  [qname & {:keys [handler-ttl-msecs backoff-msecs worker-context?]
            :or   {handler-ttl-msecs (* 60 60 1000)
                   backoff-msecs     2000}}]
  (car/lua-script
   "if redis.call('exists', _:qk-backoff) == 1 then
      return 'backoff'
    else

      -- TODO Waiting for Lua brpoplpush support to get us long polling
      local msg_id = redis.call('rpoplpush', _:qk-id-circle, _:qk-id-circle)

      if (not msg_id) or (msg_id == 'end-of-circle') then
        if _:worker-context? == '1' then
          -- Initiate queue-wide polling backoff
          redis.call('psetex', _:qk-backoff, _:backoff-msecs, 'true')
        end
        return 'backoff'
      elseif redis.call('sismember', _:qk-recently-done, msg_id) == 1 then
        redis.call('lrem', _:qk-id-circle, 1, msg_id) -- Efficient here
        redis.call('srem', _:qk-recently-done, msg_id)
        redis.call('hdel', _:qk-messages, msg_id)
        redis.call('hdel', _:qk-locks, msg_id)
        return nil
      else
        local current_time  = tonumber(_:current-time)
        local lock_ttl      = tonumber(_:handler-ttl-msecs)
        local lock_acquired = tonumber(redis.call('hget', _:qk-locks, msg_id) or 0)

        if (lock_acquired ~= 0) and (current_time - lock_acquired) < lock_ttl then
          return nil -- Has active lock
        else

          if _:worker-context? == '1' then
            -- (Re)acquire lock
            redis.call('hset', _:qk-locks, msg_id, current_time)
          end
          local msg_content = redis.call('hget', _:qk-messages, msg_id)

          if (lock_acquired == 0) then
            return {msg_id, msg_content, 'new'}
          else
            return {msg_id, msg_content, 'retry'}
          end
        end
      end
    end"
   {:qk-messages       (qkey qname "messages")
    :qk-locks          (qkey qname "locks")
    :qk-id-circle      (qkey qname "id-circle")
    :qk-recently-done  (qkey qname "recently-done")
    :qk-backoff        (qkey qname "backoff?")}
   {:current-time      (System/currentTimeMillis)
    :handler-ttl-msecs handler-ttl-msecs
    :backoff-msecs     backoff-msecs
    :worker-context?   (if worker-context? "1" "0")}))

(defprotocol IDequeueWorker
  (stop  [this])
  (start [this]))

(defn- mark-as-done [conn qname message-id]
  (wcar conn (car/sadd (qkey qname "recently-done") message-id)))

(defn- unlock [conn qname message-id]
  (wcar conn (car/hdel (qkey qname "locks") message-id)))

(defn- handle-error
  [conn qname message-id poll-reply & [throwable]]
  (mark-as-done conn qname message-id)
  (let [error-msg (str "Error while handling message from queue: "
                       qname "\n" poll-reply)]
    (if throwable
      (timbre/error throwable error-msg)
      (timbre/error error-msg))))

(defrecord DequeueWorker [conn qname opts active?]
  IDequeueWorker
  (stop  [_] (reset! active? false) nil)
  (start [_]
    (when-not @active?
      (reset! active? true)
      (let [{:keys [handler-fn throttle-msecs backoff-msecs]} opts
            flat-opts (apply concat opts)]
        (future
          (while @active?
            (when-let [poll-reply (wcar conn
                                    (apply dequeue-1 qname :worker-context? true
                                           flat-opts))]
              (if (= poll-reply "backoff")
                (Thread/sleep backoff-msecs)
                (let [[message-id message-content type] poll-reply]
                  (when (= type "retry")
                    (timbre/warn (str "Retrying message from queue: "
                                      qname "\n") poll-reply))
                  (try
                    (case (handler-fn message-content)
                      :success (mark-as-done conn qname message-id)
                      :retry   (unlock       conn qname message-id)
                      :error   (handle-error conn qname message-id poll-reply)
                      (mark-as-done conn qname message-id))
                    (catch Throwable t
                      (handle-error conn qname message-id poll-reply t))))))
            (when throttle-msecs (Thread/sleep throttle-msecs)))))
      true)))

(defn- make-dequeue-worker*
  "Creates a threaded worker to poll for and handle messages pushed to named
  queue.
    * `handler-fn` should be a unary fn of dequeued messages (presumably with
      side-effects) that throws an exception or returns e/o #{:success :error
      :retry}.
    * `handler-ttl-msecs` specifies how long a handler may keep a message
      before that handler is considered fatally stalled and the message
      re-activated in queue. BEWARE the risk of duplicate processing if ttl is
      too low.
    * `throttle-msecs` specifies thread sleep period between each poll.
    * `backoff-msecs` specifies thread sleep period each time end of queue is
      reached. Backoff is synchronized between all dequeue workers."
  [conn qname
   & {:keys [handler-fn handler-ttl-msecs throttle-msecs backoff-msecs auto-start?]
      :or   {handler-fn (fn [msg] (timbre/info (str "Message received from queue: "
                                                   qname "\n") msg))
             handler-ttl-msecs (* 60 60 1000)
             throttle-msecs    200
             backoff-msecs     2000
             auto-start?       true}}]

  (let [worker (DequeueWorker. conn qname
                 {:handler-fn        handler-fn
                  :handler-ttl-msecs handler-ttl-msecs
                  :throttle-msecs    throttle-msecs
                  :backoff-msecs     backoff-msecs}
                 (atom false))]
    (when auto-start? (start worker))
    worker))

;; 1.x backwards compatiblity
(def ^{:doc (-> make-dequeue-worker* var meta :doc)
       :arglists (-> make-dequeue-worker* var meta :arglists
                     (conj '[& deprecated-args]))}
  make-dequeue-worker
  (car/conn-shim make-dequeue-worker*))

(defn clear
  "Removes all queue's currently queued messages and all queue metadata."
  [qname]
  (car/lua-script
   "local queues = redis.call('keys', _:keys)
    for i,q in pairs(queues) do
      redis.call('del',queues[i])
    end"
   {:keys (qkey qname "*")}
   {}))

;;;; Examples, tests, etc.

(comment
  ;; Delete all queue keys
  (when-let [queues (seq (wcar {} (car/keys (qkey "*"))))]
    (wcar {} (apply car/del queues)))

  (def my-worker (make-dequeue-worker {} "myq"))
  (wcar {} (doall (map #(enqueue "myq" (str %)) (range 10))))

  (wcar {} (dequeue-1 "myq"))
  (wcar {} (car/lrange (qkey "myq" "id-circle") 0 -1))

  (def my-buggy-worker
    (make-dequeue-worker {}
     :handler-fn (fn [msg] (throw (Exception. "Oh noes")))
     :handler-ttl-msecs 10000))

  (stop my-worker))