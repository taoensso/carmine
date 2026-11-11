(ns taoensso.carmine.message-queue
  "Carmine-backed Clojure message queue, v2.
  All heavy lifting by Redis.

  Uses an optimized message circle architecture that is simple, reliable,
  and has pretty good throughput and latency.

  Delivery is lease-based and at-least-once. A message may be handled more than
  once when a lease expires, though lease-ownership checks prevent a stale
  handler from finalizing or deleting a successor's lease. Handlers should be
  idempotent. Lease timing uses the Redis server clock, so it doesn't depend
  on agreement between client clocks.

  See `mq-architecture.svg` in repo for diagram of architecture,
  Ref. <http://antirez.com/post/250> for initial inspiration.

  Message status e/o:
    nil                  - Not in queue or already GC'd
    :queued              - Awaiting handler
    :queued-with-backoff - Awaiting handler, but skip until backoff expired
    :locked              - Currently with handler
    :locked-with-requeue - Currently with handler, will requeue when done
    :done-awaiting-gc    - Finished handling, awaiting GC
    :done-with-backoff   - Finished handling, awaiting GC,
                           but skip until dedupe backoff expired
    :done-with-requeue   - Will requeue, but skip until dedupe backoff expired

  Redis keys (all prefixed with `carmine:mq:<qname>:`):
    * messages      - hash: {mid mcontent} ; Message content
    * messages-rq   - hash: {mid mcontent} ; '' for requeues
    * lock-times    - hash: {mid lock-ms}  ; Optional mid-specific lock duration
    * lock-times-rq - hash: {mid lock-ms}  ; '' for requeues
    * udts          - hash: {mid  udt-first-enqueued}
    * locks         - hash: {mid    lock-expiry-time} ; Active locks
    * lock-tokens   - hash: {mid unique-lease-token}  ; Fences stale handlers
    * backoffs      - hash: {mid backoff-expiry-time} ; Active backoffs
    * nattempts     - hash: {mid attempt-count}
    * done          - mid set: awaiting gc, etc.
    * requeue       - mid set: awaiting requeue ; Deprecated

    * mids-ready    - list: mids for immediate handling     (push to left, pop from right)
    * mid-circle    - list: mids for maintenance processing (push to left, pop from right)
    * ndry-runs     - int: num times worker(s) have lapped queue w/o work to do

    * isleep-a      - list: 0/1 sentinel element for `interruptible-sleep`
    * isleep-b      - list: 0/1 sentinel element for `interruptible-sleep`

  The `udts`, `locks` and `backoffs` times above are absolute epoch msecs,
  written using the Redis server clock. Values written by older clients
  instead use the clock of whichever client wrote them."

  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string        :as str]
   [taoensso.encore       :as enc]
   [taoensso.truss        :as truss]
   [taoensso.carmine      :as car :refer [wcar]]
   [taoensso.carmine.impl.mq :as impl]
   [taoensso.nippy.tools  :as nippy-tools]
   [taoensso.trove        :as trove]))

;;;; TODO/later
;; - Use Redis v7 functions instead of lua?

;;;; Utils

(enc/defalias exp-backoff enc/exp-backoff)

(comment (mapv #(exp-backoff % {}) (range 5)))

(defn- send-req
  "Sends given `impl/IClient` request within current `wcar` context."
  [req]
  (if (identical? (nth req 0) :lua)
    (let [[_ script keys args] req] (car/lua script keys args))
    (car/redis-call req)))

(defn- client
  "Returns `impl/IClient` for given `conn-opts`."
  [conn-opts]
  (reify impl/IClient
    (exec [_ reqs]
      (let [replies (wcar conn-opts :as-pipeline (enc/run! send-req reqs))]
        (if-let [t (enc/rfirst #(instance? Throwable %) replies)]
          (throw t)
          replies)))

    (open?        [_] true)
    (log-ns       [_] "taoensso.carmine.message-queue")
    (sleep-cap-ms [_] nil)))

;;;; Admin

(defn queue-names
  "Returns a non-empty set of existing queue names, or nil.
  Executes a Redis scan command, so O(n) of the number of keys in db."
  ([conn-opts        ] (queue-names conn-opts "*"))
  ([conn-opts pattern]
   (impl/qkeys->qnames (car/scan-keys conn-opts (impl/qkey pattern :messages)))))

(comment (queue-names {}))

(defn queues-clear!!
  "Permanently deletes ALL content for the Carmine message queues with
  given names.

  Returns nil, or a non-empty vector of the queue names that were cleared."
  {:arglists '([conn-opts qnames])}
  [conn-opts & more]
  (let [qnames ; Back compatibility
        (when-let [[x1] more]
          (if (coll? x1) ; Common case (new API)
            x1
            more))]

    (impl/queues-clear! (client conn-opts) qnames)))

(defn queues-clear-all!!!
  "**DANGER**!
  Permanently deletes ALL content for *ALL* Carmine message queues.
  Returns nil, or a non-empty set of the queue names that were cleared."
  [conn-opts]
  (when-let [qnames (queue-names conn-opts "*")]
    (queues-clear!! conn-opts qnames)))

(defn queue-status
  "Returns in O(1) the approx {:keys [nqueued nlocked nbackoff ntotal]}
  counts for given named queue.

  `nlocked` and `nbackoff` may include expired entries!"
  [conn-opts qname] (impl/queue-status (client conn-opts) qname))

(comment (queue-status {} "qname"))

(defn queue-size
  "Returns in O(1) the approx number of messages awaiting handler for
  given named queue. Same as (:nqueued (queue-status conn-opts qname))."
  ^long [conn-opts qname] (get (queue-status conn-opts qname) :nqueued))

(comment (queue-size {} "qname"))

(defn queue-content
  "Returns detailed {<mid> {:keys [message status ...]}} map for every
  message currently in queue.

  O(n_mids) and expensive, avoid use in production."
  [conn-opts qname] (impl/queue-content (client conn-opts) qname))

(comment (queue-content {} "qname"))

;;;; Implementation

(defn message-status
  "Returns current message status, e/o:
      `nil`                  - Not in queue or already GC'd
      `:queued`              - Awaiting handler
      `:queued-with-backoff` - Awaiting handler, but skip until backoff expired
      `:locked`              - Currently with handler
      `:locked-with-requeue` - Currently with handler, will requeue when done
      `:done-awaiting-gc`    - Finished handling, awaiting GC
      `:done-with-backoff`   - Finished handling, awaiting GC,
                               but skip until dedupe backoff expired
      `:done-with-requeue`   - Will requeue, but skip until dedupe backoff expired"
  [qname mid]
  (car/parse impl/->message-status
    (send-req (impl/req-message-status qname mid))))

(comment (wcar {} (message-status "qname" "mid1")))

(defn enqueue
  "Pushes given message (any Clojure data type) to named queue and returns
  a map with keys: [success? mid action error].

  When `success?` is true:  `mid`, `action` will be present, with
                            `action` e/o #{:added :updated}.

  When `success?` is false: `error` will be present, with
                            `error` e/o #{:already-queued :locked :backoff}.

  Message is frozen with Nippy. For custom Nippy options:
    - Enqueue (car/freeze message freeze-opts) value, and
    - Use matching `car/with-thaw-opts` around worker creation/start
      and queue inspection.

  Options:
      `:init-backoff-ms` - Optional initial backoff in msecs.
      `:lock-ms`         - Optional lock time in msecs. When unspecified, the
                           worker's default lock time will be used.

      `:mid`             - Optional unique message id (e.g. message hash) to
                           identify a specific message for dedupe/update/requeue.
                           When unspecified, a random uuid will be used.

      `:can-update?`     - When true, will update message content and/or lock-ms for
                           an mid still awaiting handling.

      `:can-requeue?`    - When true, will mark message with `:locked` or
                           `:done-with-backoff` status so that it will be
                           automatically requeued after garbage collection."

  {:arglists
   '([qname message]
     [qname message
      {:keys
       [mid init-backoff-ms lock-ms
        can-update? can-requeue? reset-init-backoff?]}])}

  [qname message & more]
  (let [opts ; Back compatibility: [a b & [c d]] -> [a b ?{:keys [c d]}]
        (when-let [[x1 x2] more]
          (if (map? x1) ; Common case (new API)
            x1
            {:mid          x1
             :can-requeue? x2}))

        {:keys [mid init-backoff-ms can-requeue?]} opts

        ;;; Back compatibility
        mid             (or mid             (get opts :unique-message-id))
        init-backoff-ms (or init-backoff-ms (get opts :initial-backoff-ms))
        can-requeue?    (or can-requeue?    (get opts :allow-requeue?))

        mid (or mid (enc/uuid-str))]

    (car/parse
      (partial impl/enqueue-result mid)
      (send-req
        (impl/req-enqueue qname mid
          (if (nippy-tools/wrapped-for-freezing? message)
            (do         message)
            (car/freeze message))

          (assoc opts
            :init-backoff-ms init-backoff-ms
            :can-requeue?    can-requeue?))))))

(comment
  (queues-clear!! {} :q1)
  (wcar {} (enqueue :q1 :msg1 :mid1))
  (wcar {} (message-status :q1 :mid1)))

;;;; Workers

(defprotocol IWorker
  "Private, please don't use this."
  (^:no-doc start [this])
  (^:no-doc stop  [this]))

(deftype CarmineMessageQueueWorker [conn-opts state]
  java.io.Closeable
  (close [this] (impl/worker-close! state))

  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine.CarmineMessageQueueWorker"
      (impl/worker-summary state)))

  clojure.lang.IDeref
  (deref [this] (assoc (impl/worker-status state) :conn-opts conn-opts))

  clojure.lang.IFn
  (invoke [this cmd]
    (case cmd
      :start (start this)
      :stop  (stop  this)
      (impl/worker-command state cmd)))

  (invoke [this cmd arg] (impl/worker-command state cmd arg))

  IWorker
  (stop  [this] (impl/worker-stop!  state))
  (start [this] (impl/worker-start! state this)))

(enc/def-print-impl [x CarmineMessageQueueWorker] (str "#" x))
(defn ^:no-doc worker? [x] (instance? CarmineMessageQueueWorker x))

(defn monitor-fn
  "Returns a worker monitor fn that warns when queue exceeds the prescribed
  size. A backoff timeout can be provided to rate-limit this warning."
  [qname max-queue-size warn-backoff-ms]
  (impl/monitor-fn "taoensso.carmine.message-queue"
    qname max-queue-size warn-backoff-ms))

(enc/defalias default-throttle-ms-fn impl/default-throttle-ms-fn)

(comment (default-throttle-ms-fn 0))


(defn worker
  "Returns a stateful threaded `CarmineMessageQueueWorker` to handle messages
  added to named queue with `enqueue`.

  API:
    - (deref <worker>)          => Detailed worker status map (see \"Debugging\" below).
    - (<worker> :start)         => Same as calling (start <worker>).
    - (<worker> :stop)          => Same as calling (stop  <worker>).
    - (<worker> :queue-size)    => Same as calling `queue-size`    for given qname.
    - (<worker> :queue-status)  => Same as calling `queue-status`  for given qname.
    - (<worker> :queue-content) => Same as calling `queue-content` for given qname.
    - (<worker> :drain)         => After stopping, waits for active handler tasks.
                                   Returns false if running or called by a handler.
    - (<worker> :drain <msecs>) => Like `:drain`, but returns false on timeout.
    - (.close <worker>)         => Permanently stops worker. Like `stop`, doesn't
                                   wait for active handler tasks, see `:drain`.

  Debugging:

    To debug a worker, deref it to see detailed status map with keys:
      `:qname`     - Worker's queue name (string)
      `:opts`      - Worker's            options map
      `:conn-opts` - Worker's connection options map
      `:running?`  - Is the worker currently running? (true/false)
      `:closed?`   - Has the worker been permanently closed? (true/false)
      `:nthreads`  - {:keys [worker handler]}
      `:stats`
        `:queue-size`        - {:keys [last min max mean mad var p50 p90 ...]}
        `:queueing-time-ms`  - {:keys [last min max mean mad var p50 p90 ...]}
        `:handling-time-ns`  - {:keys [last min max mean mad var p50 p90 ...]}
        `:counts`
          `:handler/success`    - Number of handler results with `:success` status
          `:handler/error`      - Number of handler results with `:error`   status
          `:handler/retry`      - Number of handler results with `:retry`   status
          `:handler/unexpected` - Number of handler results with any other status
          `:handler/backoff`    - Number of the above that supplied a `:backoff-ms`
          `:handler/fenced`     - Number of stale handler results safely ignored

            The status counts are disjoint from `:handler/fenced`: a fenced
            result had no effect on the queue, so it isn't also counted as a
            completed handling (nor in `:handler/backoff`).
          ...

    See also the `:monitor` option below, and utils:
      `queue-names`, `queue-size`, `queue-content`, `message-status`, etc.

  Options:

    `:handler`
      (fn [{:keys [qname mid message attempt lock-extend!]}]) called for each
      worker message. `lock-extend!` is a nullary fn that safely extends the
      current lease by its original duration, returning true on success or nil
      if the lease has already been superseded. Expiry alone permits another
      delivery but does not invalidate ownership: an expired, unsuperseded
      owner can still renew or finalize. Renewal never shortens the lease.
      Note that `lock-extend!` may also THROW on Redis connection errors.
      Should return a map with possible keys:
        `:status`     - ∈ {:success :error :retry}
        `:throwable`  - Optional Throwable when relevant
        `:backoff-ms` - Optional time (in milliseconds) to backoff
                        for dedupe or before retrying

      If handler throws ANY `Throwable`, will assume `:error` status and NOT retry.
      For custom error handling, make sure to use an appropriate try/catch
      within your handler fn!

    `:monitor` - (fn [{:keys [queue-size ndry-runs poll-reply]}])
      Called on each worker loop iteration. Useful for queue monitoring/logging.
      `:poll-reply` retains its historical shape; \"handle\" replies contain
      exactly [\"handle\" <mid> <message> <attempt> <lock-ms> <udt>].
      See also `monitor-fn`.

    `:lock-ms` (default 60 minutes)
      Default time (in milliseconds) that handler may keep a message before handler
      is considered fatally stalled and message is re-queued. Must be large enough
      to prevent double handling!

      Can be overridden on a per-message basis via `enqueue`.

      Lease expiry permits another worker to receive the message. This provides
      at-least-once delivery and can cause concurrent handling; use idempotent
      handlers and call `lock-extend!` during legitimately long work.

    `:throttle-ms` (default `default-throttle-ms-fn`)
      Thread sleep period (in milliseconds) between each poll.
      Can be a (fn [queue-size]) -> ?sleep-msecs.

    `:eoq-backoff-ms` (default `exp-backoff`)
      Max time (in milliseconds) to sleep thread each time end of queue is reached.
      Can be a (fn [ndry-runs]) -> msecs for n<=5.
      Sleep may be interrupted when new messages are enqueued.
      If present, connection read timeout should be comfortably > max msecs
      (e.g. by >= 250ms)!

    `:nthreads-worker`  - Number of threads to monitor and maintain queue.
    `:nthreads-handler` - Number of threads to handle queue messages with handler fn.

      Worker threads submit polls, paced by `:throttle-ms`. Each poll runs on
      a handler thread and holds that slot through handling or idle sleep.
      Concurrent polls/handlers are limited by `:nthreads-handler`; even one
      worker thread can fill multiple handler slots. No free slot means no
      poll, preventing leases from being acquired ahead of handler capacity."

  ([conn-opts qname] (worker conn-opts qname nil))
  ([conn-opts qname
    {:keys [handler monitor lock-ms eoq-backoff-ms throttle-ms auto-start
            nthreads-worker nthreads-handler] :as worker-opts

     :or
     {handler (fn [m] (trove/log! {:level :info, :id :carmine.mq/logging-handler, :data m}) {:status :success})
      monitor (monitor-fn qname 1000 (enc/ms :hours 6))
      lock-ms (enc/ms :mins 60)
      nthreads-worker  1
      nthreads-handler 1
      throttle-ms    default-throttle-ms-fn
      eoq-backoff-ms exp-backoff
      auto-start     true}}]

   (let [nthreads             (get       worker-opts :nthreads 1) ; Back compatibility
         nthreads-worker  (if (contains? worker-opts :nthreads-worker)  nthreads-worker  nthreads)
         nthreads-handler (if (contains? worker-opts :nthreads-handler) nthreads-handler nthreads)

         _
         (when (and (pos-int? nthreads-worker) (not (pos-int? nthreads-handler)))
           (truss/ex-info!
             "[Carmine/mq] Positive worker threads require positive handler threads"
             {:nthreads-worker nthreads-worker, :nthreads-handler nthreads-handler}))

         qname (enc/as-qname qname)
         worker-opts
         (conj (or worker-opts {})
           {:handler          handler
            :monitor          monitor
            :default-lock-ms  lock-ms
            :eoq-backoff-ms   eoq-backoff-ms
            :nthreads-worker  nthreads-worker
            :nthreads-handler nthreads-handler
            :throttle-ms
            (if (identical? throttle-ms :auto) ; Back compatibility
              default-throttle-ms-fn
              throttle-ms)})

         state
         (impl/worker-state (client conn-opts) qname worker-opts
           (when monitor
             (fn [m] (monitor (assoc m :mid-circle-size (get m :queue-size)))))) ; Back compatibility

         w (CarmineMessageQueueWorker. conn-opts state)

         ;; Back compatibility
         auto-start (get worker-opts :auto-start? auto-start)]

     (when auto-start
       (if (integer? auto-start) ; Undocumented
         (future (Thread/sleep (int auto-start)) (start w))
         (do                                     (start w))))

     w)))

;;;; Deprecated

(enc/deprecated
  (enc/defalias ^:no-doc clear-queues queues-clear!! {:deprecated "v3.3.0-RC1 (2023-07-18)"})

  (defn ^:no-doc ^:deprecated make-dequeue-worker
    "Prefer `worker`."
    [pool spec qname & {:keys [handler-fn handler-ttl-msecs backoff-msecs throttle-msecs
                               auto-start?] :or {auto-start? true}}]
    (worker {:pool pool :spec spec} qname
      (merge (when-let [ms handler-ttl-msecs] {:lock-ms        ms})
        (when-let [ms backoff-msecs]          {:eoq-backoff-ms ms})
        (when-let [ms throttle-msecs]         {:throttle-ms    ms})
        (when-let [hf handler-fn]
          {:handler (fn [{:keys [message]}]
                      {:status (or (#{:success :error :retry} (hf message))
                                 :success)})})
        {:auto-start? auto-start?}))))
