(ns taoensso.carmine.message-queue
  "Carmine-backed Clojure message queue. All heavy lifting by Redis (2.6+).
  Simple implementation. Very simple API. Reliable. Fast.

  Currently ALPHA QUALITY!!!

  Prefixed keys:
    * qname:hash:messages     ; Id -> content
    * qname:hash:locks        ; Id -> lock-acquired time
    * qname:list:id-circle    ; Rotating list of ids
    * qname:set:recently-done ; Used for efficient id removal from circle
    * qname:flag:backoff?     ; Used for queue-wide (every-worker) polling
                              ; backoff

  See http://antirez.com/post/250 for implementation details."
  {:author "Peter Taoussanis"}
  (:require [clojure.string   :as str]
            [taoensso.carmine :as car]
            [taoensso.carmine (protocol :as protocol)]
            [taoensso.timbre  :as timbre])
  (:import  [java.util UUID]))

(def ^:private qkey "Prefixed queue key"
  (memoize (car/make-keyfn "carmine" "queue")))

(defn enqueue
  "Pushes given message (any Clojure datatype) to named queue. Returns message
  id and the number of messages currently queued or recently dequeued."
  [qname message]
  (car/lua-script
   "redis.call('hset', _:qk-messages, _:msg-id, _:msg-content)

    -- lpushnx EOC sentinel to ensure an initialized id-circle
    if redis.call ('exists', _:qk-id-circle) == 0 then
      redis.call ('lpush', _:qk-id-circle, 'end-of-circle')
    end

    return {_:msg-id, redis.call ('lpush', _:qk-id-circle, _:msg-id)}"
   {:qk-messages  (qkey qname "hash" "messages")
    :qk-id-circle (qkey qname "list" "id-circle")}
   {:msg-id       (str (UUID/randomUUID))
    :msg-content  (car/preserve message)}))

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
   {:qk-messages       (qkey qname "hash" "messages")
    :qk-locks          (qkey qname "hash" "locks")
    :qk-id-circle      (qkey qname "list" "id-circle")
    :qk-recently-done  (qkey qname "set"  "recently-done")
    :qk-backoff        (qkey qname "flag" "backoff?")}
   {:current-time      (str (System/currentTimeMillis))
    :handler-ttl-msecs (str handler-ttl-msecs)
    :backoff-msecs     (str backoff-msecs)
    :worker-context?   (if worker-context? "1" "0")}))

(defprotocol IDequeueWorker
  (stop  [this])
  (start [this]))

(defrecord DequeueWorker [pool spec qname opts active?]
  IDequeueWorker
  (stop  [_] (reset! active? false) nil)
  (start [_]
    (when-not @active?
      (reset! active? true)
      (let [{:keys [handler-fn throttle-msecs backoff-msecs]} opts
            flat-opts (apply concat opts)]
        (future
          (while @active?
            (when-let [poll-reply (car/with-conn pool spec
                                    (apply dequeue-1 qname :worker-context? true
                                           flat-opts))]
              (if (= poll-reply "backoff")
                (Thread/sleep backoff-msecs)
                (let [[message-id message-content type] poll-reply]
                  (when (= type "retry")
                    (timbre/warn (str "Retrying message from queue: "
                                      qname "\n") poll-reply))

                  (try (handler-fn message-content)
                       (car/with-conn pool spec
                         (car/sadd (qkey qname "set" "recently-done")
                                       message-id))
                       (catch Exception e
                         (timbre/error
                          e (str "Exception while handling message from queue: "
                                 qname "\n") poll-reply))))))
            (when throttle-msecs (Thread/sleep throttle-msecs))))))
    nil))

(defn make-dequeue-worker
  "Creates a threaded worker to poll for and handle messages pushed to named
  queue.
    * `handler-fn` should be a unary fn of dequeued messages (presumably with
      side-effects).
    * `handler-ttl-msecs` specifies how long a handler may keep a message
      before that handler is considered fatally stalled and the message
      re-activated in queue. BEWARE the risk of duplicate processing if ttl is
      too low.
    * `throttle-msecs` specifies thread sleep period between each poll.
    * `backoff-msecs` specifies thread sleep period each time end of queue is
      reached. Backoff is synchronized between all dequeue workers."
  [connection-pool connection-spec qname
   & {:keys [handler-fn handler-ttl-msecs throttle-msecs backoff-msecs auto-start?]
      :or   {handler-fn (fn [msg] (timbre/info (str "Message received from queue: "
                                                   qname "\n") msg))
             handler-ttl-msecs (* 60 60 1000)
             throttle-msecs    200
             backoff-msecs     2000
             auto-start?       true}}]

  (let [worker (DequeueWorker. connection-pool connection-spec qname
                               {:handler-fn        handler-fn
                                :handler-ttl-msecs handler-ttl-msecs
                                :throttle-msecs    throttle-msecs
                                :backoff-msecs     backoff-msecs}
                               (atom false))]
    (when auto-start? (start worker))
    worker))

;;;; Examples, tests, etc.

(comment

  (do (def p (car/make-conn-pool))
      (def s (car/make-conn-spec))
      (defmacro wcar [& body] `(car/with-conn p s ~@body)))

  ;; Delete all queue keys
  (let [queues (wcar (car/keys (qkey "*")))]
    (when (seq queues) (wcar (apply car/del queues))))

  (def my-worker (make-dequeue-worker nil nil "myq"))
  (wcar (doall (map #(enqueue "myq" (str %)) (range 10))))

  (wcar (dequeue-1 "myq"))
  (wcar (car/lrange (qkey "myq" "list" "id-circle") 0 -1))

  (def my-buggy-worker
    (make-dequeue-worker
     nil nil "myq"
     :handler-fn (fn [msg] (throw (Exception. "Oh noes")))
     :handler-ttl-msecs 10000))

  (stop my-worker))