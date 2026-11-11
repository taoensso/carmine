(ns taoensso.carmine-v4.message-queue
  "Carmine-backed Clojure message queue, v2.
  All heavy lifting by Redis.

  Port of `taoensso.carmine.message-queue` (Carmine v3) to Carmine v4
  connection managers. Shares Lua scripts, Redis key layout and delivery
  semantics with v3, so v3 and v4 clients can share queues. See the
  Carmine v4 wiki for changes from v3 and migration notes:
  https://github.com/taoensso/carmine/wiki/5-Carmine-v4#message-queue

  Preview: this API may still change before the final v4 release.

  Connection managers:
    Functions that take a `conn-mgr` accept a Carmine v4 connection manager,
    or a delay of one. `enqueue`, `message-status` and `cancel` can be called
    with a `conn-mgr`, or without one inside a `wcar` request context.

    Cluster managers are unsupported.

  Uses an optimized message circle architecture that is simple, reliable,
  and has pretty good throughput and latency.

  Delivery is lock-based and at-least-once. A message may be handled more than
  once when a lock expires, though lock-ownership checks prevent a stale
  handler from finalizing or deleting a successor's lock. Handlers should be
  idempotent. Lock timing uses the Redis server clock, so it doesn't depend
  on agreement between client clocks.

  See `mq-architecture.svg` in repo for diagram of message statuses,
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
    * lock-tokens   - hash: {mid unique-lock-token}  ; Fences stale handlers
    * backoffs      - hash: {mid backoff-expiry-time} ; Active backoffs
    * nattempts     - hash: {mid attempt-count}
    * done          - mid set: awaiting gc, etc.
    * requeue       - mid set: awaiting requeue ; Deprecated

    * mids-ready    - list: mids for immediate handling     (push to left, pop from right)
    * mid-circle    - list: mids for maintenance processing (push to left, pop from right)
    * ndry-runs     - int: num times worker(s) have lapped queue w/o work to do
    * next-due      - int: earliest known time that a mid skipped during the
                      current lap will need attention, caps end-of-circle sleep

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
   [taoensso.trove        :as trove]
   [taoensso.carmine-v4   :as car]
   [taoensso.carmine.impl.mq :as impl]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp        :as resp]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.conns       :as conns]
   [taoensso.carmine-v4.cluster     :as cluster]))

;;;; TODO/later
;; - Use Redis v7 functions instead of lua?

;;;; Utils

(enc/defalias exp-backoff enc/exp-backoff)

(comment (mapv #(exp-backoff % {}) (range 5)))

(defn- have-mgr
  "Returns the connection manager for given `conn-mgr` (a manager or delay
  of one), or throws. `write?` rejects managers that prefer read replicas."
  [via conn-mgr write?]
  (let [mgr (force conn-mgr)]
    (when-not (car/conn-manager? mgr)
      (truss/ex-info! "[Carmine/mq] Expected a Carmine v4 connection manager"
        {:eid :carmine.mq/invalid-manager, :via via, :mgr (enc/typed-val mgr)}))

    (when (conns/mgr-cluster-server mgr)
      (truss/ex-info! "[Carmine/mq] Message queue doesn't support Redis Cluster"
        {:eid :carmine.mq/cluster-not-supported, :via via}))

    (when (and write? (get-in (conns/mgr-conn-opts mgr) [:server :sentinel-opts :prefer-read-replica?]))
      (truss/ex-info! "[Carmine/mq] Message queue writes require a Redis master connection"
        {:eid :carmine.mq/replica-manager-not-supported, :via via}))
    mgr))

(defn- reject-cluster-context! [via]
  (when (resp/cluster-context?)
    (truss/ex-info! "[Carmine/mq] Message queue doesn't support Redis Cluster"
      {:eid :carmine.mq/cluster-not-supported, :via via})))

(defmacro ^:private isolated
  "Evaluates body with default reply decoding, ignoring any ambient reply
  bindings (natural replies, read modes, parsers, map keywordization).
  Always thaws, since `enqueue` always freezes message content. Ambient
  Nippy thaw options (e.g. for encryption) are preserved."
  [& body]
  `(enc/binding*
     [car/*auto-thaw?*            true
      com/*natural-replies?*      false
      com/*read-mode*             nil
      com/*parser*                nil
      car/*keywordize-maps?*      false
      car/*raw-verbatim-strings?* false]
     ~@body))

(defn- reject-removed-opts!
  "Throws if `opts` contains any v3 option removed in v4."
  [via opts removed]
  (when opts
    (enc/run-kv!
      (fn [k replacement]
        (when (contains? opts k)
          (truss/ex-info!
            (str "[Carmine/mq] `" k "` option was removed in v4, use " replacement " instead")
            {:eid :carmine.mq/removed-option, :via via, :opt-key k, :replacement replacement})))
      removed)))

(comment (reject-removed-opts! 'f {:a 1} {:a "`:b`"}))

(defn- have-worker-opt! [opt-key expected pred opt-val]
  (when-not (pred opt-val)
    (truss/ex-info! (str "[Carmine/mq] Invalid `worker` option: `" opt-key "` should be " expected)
      {:eid :carmine.mq/invalid-opts, :via 'worker
       :opt-key opt-key, :opt-val (enc/typed-val opt-val)})))

(defn- reject-unknown-opts!
  "Throws if `opts` contains any unqualified key not in `known` set,
  e.g. a misspelled option that'd otherwise be silently ignored."
  [via opts known]
  (when opts
    (enc/run-kv!
      (fn [k _]
        (when-not (or (contains? known k) (and (keyword? k) (namespace k)))
          (truss/ex-info! (str "[Carmine/mq] Unknown `" via "` option: " k)
            {:eid :carmine.mq/unknown-option, :via via, :opt-key k})))
      opts)))

(defn- read-timeout-ms
  "Returns `mgr`'s positive socket read timeout in msecs, or nil (unlimited)."
  [mgr]
  (let [socket-opts (get (conns/mgr-conn-opts mgr) :socket-opts)
        ms (get socket-opts :read-timeout-ms)]
    (when (and ms (pos? (long ms))) (long ms))))

(defn- sleep-cap-ms
  "Returns max `BRPOP` sleep msecs for given positive read timeout, leaving
  margin for latency and Redis's blocking timeout resolution (~100ms with
  the default `hz` 10). May be <= 0."
  ^long [^long read-timeout-ms]
  (- read-timeout-ms (max 250 (quot read-timeout-ms 4))))

(comment (mapv sleep-cap-ms [100 300 1000 10000]))

(defn- send-req
  "Sends given `impl/IClient` request within current `wcar` context."
  [req]
  (if (identical? (nth req 0) :lua)
    (let [[_ script keys args] req] (car/lua script keys args))
    (car/rcmd* req)))

(defn- client
  "Returns `impl/IClient` for given (already validated) connection manager.
  Requests are `isolated` from ambient reply bindings, and reply errors
  throw (`:error-mode :throw`).

  Idle sleeps are capped below the manager's read timeout (if any), since
  tripping it would invalidate the connection."
  [mgr]
  (reify impl/IClient
    (exec [_ reqs]
      (car/with-car mgr {:as-vec? true}
        (fn [] (isolated (enc/run! send-req reqs)))))

    (open?        [_] (car/conn-manager-open? mgr))
    (log-ns       [_] "taoensso.carmine-v4.message-queue")
    (sleep-cap-ms [_] (when-let [rt (read-timeout-ms mgr)] (sleep-cap-ms rt)))))

;;;; Admin

(defn queue-names
  "Returns a non-empty set of existing queue names, or nil.
  Executes a Redis scan command, so O(n) of the number of keys in db."
  ([conn-mgr        ] (queue-names conn-mgr "*"))
  ([conn-mgr pattern]
   (impl/qkeys->qnames
     (isolated
       (car/scan-keys (have-mgr 'queue-names conn-mgr false)
         (impl/qkey pattern :messages))))))

(comment (queue-names mgr))

(defn queues-clear!!
  "Permanently deletes ALL content for the Carmine message queues with
  given names.

  Returns nil, or the given non-empty collection of queue names."
  [conn-mgr qnames]
  (let [mgr (have-mgr 'queues-clear!! conn-mgr true)]
    (when-not (or (nil? qnames) (coll? qnames))
      (truss/ex-info! "[Carmine/mq] Expected a collection of queue names"
        {:eid :carmine.mq/invalid-qnames, :qnames (enc/typed-val qnames)
         :hint "(queues-clear!! conn-mgr [qname1 qname2 ...])"}))

    (impl/queues-clear! (client mgr) qnames)))

(defn queues-clear-all!!!
  "**DANGER**!
  Permanently deletes ALL content for *ALL* Carmine message queues.
  Returns nil, or a non-empty set of the queue names that were cleared."
  [conn-mgr]
  (let [mgr (have-mgr 'queues-clear-all!!! conn-mgr true)]
    (when-let [qnames (queue-names mgr "*")]
      (queues-clear!! mgr qnames))))

(defn queue-status
  "Returns in O(1) the approx {:keys [nqueued nlocked nbackoff ntotal]}
  counts for given named queue, and `:head-age-ms`: the approx msecs since
  the oldest message still waiting in line was enqueued (nil if none). Useful
  to monitor whether workers are keeping up. Doesn't cover messages waiting
  on a backoff or retry.

  `nlocked` and `nbackoff` may include expired entries!"
  [conn-mgr qname]
  (impl/queue-status (client (have-mgr 'queue-status conn-mgr false)) qname))

(comment (queue-status mgr "qname"))

(defn queue-size
  "Returns in O(1) the approx number of messages awaiting handler for
  given named queue. Same as (:nqueued (queue-status conn-mgr qname))."
  ^long [conn-mgr qname] (impl/queue-size (client (have-mgr 'queue-size conn-mgr false)) qname))

(comment (queue-size mgr "qname"))

(defn queue-content
  "Returns detailed {<mid> {:keys [message status ...]}} map for every
  message currently in queue.

  O(n_mids) and expensive, avoid use in production."
  [conn-mgr qname]
  (impl/queue-content (client (have-mgr 'queue-content conn-mgr false)) qname))

(comment (queue-content mgr "qname"))

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
      `:done-with-requeue`   - Will requeue, but skip until dedupe backoff expired

  Call with a `conn-mgr`, or without one inside a `wcar` request context
  (where context's reply options apply as for other Redis commands).
  Can't be used inside `transact!` effects."
  ([qname mid]
   (reject-cluster-context! 'message-status)
   (car/parse impl/->message-status
     (send-req (impl/req-message-status qname mid))))

  ([conn-mgr qname mid]
   (impl/->message-status
     (impl/exec1 (client (have-mgr 'message-status conn-mgr false))
       (impl/req-message-status qname mid)))))

(comment
  (message-status mgr "qname" "mid1")
  (car/wcar mgr (message-status "qname" "mid1")))

(defn- mgr-arg?
  "Returns true iff given a connection manager or delay of one."
  [x] (or (delay? x) (car/conn-manager? x)))

(defn- enqueue-req
  "Returns [<mid> <request>] for given `enqueue` args, or throws."
  [qname message more]
  (let [[opts] more]
    (when (or (next more) (not (or (nil? opts) (map? opts))))
      (truss/ex-info! "[Carmine/mq] Expected `enqueue` opts map or nil"
        {:eid :carmine.mq/invalid-opts, :args (enc/typed-val (vec more))
         :hint "Positional [mid can-requeue?] args were removed in v4, use {:mid _, :can-requeue? _}"}))

    (reject-removed-opts! 'enqueue opts
      {:unique-message-id  "`:mid`"
       :initial-backoff-ms "`:init-backoff-ms`"
       :allow-requeue?     "`:can-requeue?`"})

    (reject-unknown-opts! 'enqueue opts
      #{:init-backoff-ms :lock-ms :mid :can-update? :can-requeue? :reset-init-backoff?})

    (let [mid (or (get opts :mid) (enc/uuid-str))
          mcontent ; Always marked as frozen, since workers always thaw
          (binding [car/*auto-freeze?* true]
            (write/prepare-arg (car/freeze message)))]
      [mid (impl/req-enqueue qname mid mcontent opts)])))

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
                           automatically requeued after garbage collection.

      `:reset-init-backoff?` - When true, will reschedule an mid still awaiting
                               handling to become due after `:init-backoff-ms`
                               (default 0, i.e. now). Useful to postpone or
                               hasten a delayed message, or with `:can-update?`
                               to debounce. NB always provide the real message,
                               since it may be enqueued as usual.

  Call with a `conn-mgr`, or without one inside a `wcar` request context
  (where context's reply options apply as for other Redis commands).
  Can't be used inside `transact!` effects."

  {:arglists
   '([qname message]
     [qname message
      {:keys
       [mid init-backoff-ms lock-ms
        can-update? can-requeue? reset-init-backoff?]}]
     [conn-mgr qname message]
     [conn-mgr qname message opts])}

  [x y & more]
  (if (mgr-arg? x)
    (let [mgr (have-mgr 'enqueue x true)
          _
          (when (empty? more)
            (truss/ex-info! "[Carmine/mq] Expected `enqueue` message"
              {:eid :carmine.mq/invalid-args
               :hint "(enqueue conn-mgr qname message) or (enqueue conn-mgr qname message opts)"}))

          [mid req] (enqueue-req y (first more) (next more))]
      (impl/enqueue-result mid (impl/exec1 (client mgr) req)))

    (do
      (reject-cluster-context! 'enqueue)
      (let [[mid req] (enqueue-req x y more)]
        (car/parse
          (partial impl/enqueue-result mid)
          (send-req req))))))

(comment
  (queues-clear!! mgr [:q1])
  (enqueue        mgr :q1 :msg1 {:mid :mid1})
  (message-status mgr :q1 :mid1)
  (car/wcar mgr (enqueue :q1 :msg1 {:mid :mid1})))

(defn cancel
  "Cancels any pending handling and/or requeue of message with given id.
  Returns nil if there was nothing to cancel (message not in queue, or
  already done), otherwise returns the message's prior status as given by
  `message-status`, e.g.:
      `:queued`              - Message will not be handled
      `:queued-with-backoff` - ''
      `:locked`              - Message was already with handler, see below
      `:locked-with-requeue` - '', and will not be requeued
      `:done-with-requeue`   - Message will not be requeued

  A handler that's already running can't be interrupted, but its result will
  be ignored and its `lock-extend!` will return nil.

  A message cancelled while queued or locked can be enqueued again right away.

  Call with a `conn-mgr`, or without one inside a `wcar` request context
  (where context's reply options apply as for other Redis commands).
  Can't be used inside `transact!` effects."
  ([qname mid]
   (reject-cluster-context! 'cancel)
   (car/parse impl/cancel-result
     (send-req (impl/req-cancel qname mid))))

  ([conn-mgr qname mid]
   (impl/cancel-result
     (impl/exec1 (client (have-mgr 'cancel conn-mgr true))
       (impl/req-cancel qname mid)))))

(comment
  (cancel mgr "qname" "mid1")
  (car/wcar mgr (cancel "qname" "mid1")))

;;;; Workers

(defprotocol IWorker
  "Worker lifecycle, see `worker`. Not for extension."
  (start [worker] "Starts given worker's handler threads. Returns true iff worker was started.")
  (stop  [worker]
    "Stops given worker's handler threads without waiting for active handlers
    to finish (see `drain`). Returns true iff worker was running."))

(deftype CarmineMessageQueueWorker [conn-mgr state]
  java.io.Closeable
  (close [this] (impl/worker-close! state))

  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine-v4.CarmineMessageQueueWorker"
      (impl/worker-summary state)))

  clojure.lang.IDeref
  (deref [this] (assoc (impl/worker-status state) :conn-mgr conn-mgr))

  clojure.lang.IFn
  (invoke [this cmd]
    (case cmd
      :start (start this)
      :stop  (stop  this)
      :drain (.invoke this :drain nil)
      (impl/worker-command state cmd)))

  ;; Worker threads are independent of any outer request context
  (invoke [this cmd arg]
    (cluster/without-request-context*
      #(impl/worker-command state cmd arg)))

  IWorker
  (stop  [this] (impl/worker-stop! state))
  (start [this]
    (cluster/without-request-context*
      #(impl/worker-start! state this))))

(enc/def-print-impl [x CarmineMessageQueueWorker] (str "#" x))
(defn ^:no-doc worker? [x] (instance? CarmineMessageQueueWorker x))

(defn drain
  "Waits for given stopped worker's active handlers to finish, e.g. for
  graceful shutdown. Returns true once finished, or false if `timeout-msecs`
  elapsed first, worker is running, or called by one of worker's handlers.
  Same as calling (<worker> :drain <?timeout-msecs>)."
  ([worker              ] (worker :drain))
  ([worker timeout-msecs] (worker :drain timeout-msecs)))

(defn monitor-fn
  "Returns a worker monitor fn that warns when queue exceeds the prescribed
  size. A backoff timeout can be provided to rate-limit this warning."
  [qname max-queue-size warn-backoff-ms]
  (impl/monitor-fn "taoensso.carmine-v4.message-queue"
    qname max-queue-size warn-backoff-ms))

(enc/defalias default-throttle-ms-fn impl/default-throttle-ms-fn)

(comment (default-throttle-ms-fn 0))

(defn worker
  "Returns a stateful threaded `CarmineMessageQueueWorker` to handle messages
  added to named queue with `enqueue`.

  `conn-mgr` is a Carmine v4 connection manager, or a delay of one.
  Close the worker before closing the manager.

  Each handler thread may hold a connection while idle, so size the
  manager's pool for the sum of `:nthreads-handler` over all workers that
  share it, plus other usage (a dedicated manager is recommended). Keep
  a finite socket `:read-timeout-ms` (the default), so that a dead
  connection can't block a handler thread indefinitely.

  API:
    - (deref <worker>)          => Detailed worker status map (see \"Debugging\" below).
    - (<worker> :start)         => Same as calling `start`.
    - (<worker> :stop)          => Same as calling `stop`.
    - (<worker> :queue-size)    => Same as calling `queue-size`    for given qname.
    - (<worker> :queue-status)  => Same as calling `queue-status`  for given qname.
    - (<worker> :queue-content) => Same as calling `queue-content` for given qname.
    - (<worker> :drain)         => After stopping, waits for active handler tasks.
                                   Returns false if running or called by a handler.
    - (<worker> :drain <msecs>) => Like `:drain`, but returns false on timeout.
    - (.close <worker>)         => Permanently stops worker. Like `stop`, doesn't
                                   wait for active handler tasks, see `:drain`.

    For graceful shutdown: close (or stop) worker, then `drain` (usu. with a timeout)
    before exiting.

    Messages whose handlers hadn't completed when the process exited will be
    retried after their lock expires. For faster recovery, prefer a short
    `:lock-ms` with `:lock-auto-extend-max-ms`.

  Debugging:

    To debug a worker, deref it to see detailed status map with keys:
      `:qname`     - Worker's queue name (string)
      `:opts`      - Worker's            options map
      `:conn-mgr`  - Worker's connection manager
      `:running?`  - Is the worker currently running? (true/false)
      `:closed?`   - Has the worker been permanently closed? (true/false)
      `:nthreads`  - {:keys [handler]}
      `:stats`
        `:queue-size`        - {:keys [last min max mean mad var p50 p90 ...]}
        `:queueing-time-ms`  - {:keys [last min max mean mad var p50 p90 ...]}
        `:handling-time-ns`  - {:keys [last min max mean mad var p50 p90 ...]}
        `:counts`
          `:handler/success`    - Number of handler results with `:success` status
          `:handler/error`      - Number of handler results with `:error`   status
          `:handler/retry`      - Number of handler results with `:retry`   status
          `:handler/unexpected` - Number of handler results with any other status
          `:handler/backoff`    - Number of messages returned to queue (or marked
                                  done) with a backoff
          `:handler/gave-up`    - Number of messages dropped without success, see
                                  `:on-give-up`
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
      current lock by its original duration, returning true on success or nil
      if the lock has already been superseded. Expiry alone permits another
      delivery but does not invalidate ownership: an expired, unsuperseded
      owner can still renew or finalize. Renewal never shortens the lock.
      Note that `lock-extend!` may also THROW on Redis connection errors.
      Should return a map with possible keys:
        `:status`     - ∈ {:success :error :retry}
        `:throwable`  - Optional Throwable when relevant
        `:backoff-ms` - Optional time (in milliseconds) to backoff
                        for dedupe or before retrying

      If handler throws ANY `Throwable`, will assume `:error` status and NOT retry
      (unless `:error-retry-backoff-ms` is set). For custom error handling, make
      sure to use an appropriate try/catch within your handler fn!

      A message that can't be decoded is treated the same way, without
      calling handler.

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

      Lock expiry permits another worker to receive the message. This provides
      at-least-once delivery and can cause concurrent handling; use idempotent
      handlers and call `lock-extend!` during legitimately long work, or see
      `:lock-auto-extend-max-ms`.

    `:lock-auto-extend-max-ms` (default nil => disabled)
      If set, worker will automatically keep extending a message's lock while
      its handler is running, for up to this long (in milliseconds) after
      handler started. After that the lock is left to expire as usual, as a
      guard against stalled handlers.

      Allows a short `:lock-ms` (so that messages held by a dead worker are
      soon retried) with less risk of double handling for slow handlers.

    `:max-attempts` (default nil => unlimited)
      Max number of times that a message will be handled, counted across all
      workers (incl. attempts that ended by lock expiry). A message that still
      needs a retry after this many attempts is instead dropped, see `:on-give-up`.

    `:error-retry-backoff-ms` (default nil => don't retry)
      If set, a message with `:error` status (incl. handler throwing) will be
      retried after this long (in milliseconds), rather than dropped.
      Can be a (fn [attempt]) -> msecs, e.g.:
        (fn [attempt] (exp-backoff attempt {:max 60000}))

      A `:backoff-ms` in handler's result takes precedence.
      Best used with `:max-attempts`!

    `:on-give-up` - (fn [{:keys [qname mid message attempt reason throwable]}])
      Called just before worker drops a message for one of the reasons below,
      e.g. to log or to store message elsewhere (\"dead letters\").
      `:reason` is e/o:
        `:error`        - `:error` status that won't be retried
        `:max-attempts` - Retry needed, but `:max-attempts` reached

      If this fn throws, message is kept and retried later.

    `:throttle-ms` (default `default-throttle-ms-fn`)
      Min time (in milliseconds) between polls by each handler thread, incl.
      any time spent handling or sleeping.
      Can be a (fn [queue-size]) -> ?msecs.

    `:eoq-backoff-ms` (default `exp-backoff`)
      Max time (in milliseconds) to sleep thread each time end of queue is reached.
      Can be a (fn [ndry-runs]) -> msecs for n<=5.
      Sleep may be interrupted when new messages are enqueued.
      Capped below the connection manager's read timeout, if any.

    `:nthreads-handler` - Number of threads to poll queue and handle messages,
      i.e. max number of concurrent handlers.

    `:auto-start` (default true)
      Start worker immediately? If false, start it later with `start`."

  (^java.io.Closeable [conn-mgr qname] (worker conn-mgr qname nil))
  (^java.io.Closeable
   [conn-mgr qname
    {:keys [handler monitor lock-ms eoq-backoff-ms throttle-ms auto-start
            nthreads-handler] :as worker-opts

     :or
     {handler (fn [m] (trove/log! {:level :info, :id :carmine.mq/logging-handler, :data m}) {:status :success})
      monitor (monitor-fn qname 1000 (enc/ms :hours 6))
      lock-ms (enc/ms :mins 60)
      nthreads-handler 1
      throttle-ms    default-throttle-ms-fn
      eoq-backoff-ms exp-backoff
      auto-start     true}}]

   (let [conn-mgr (have-mgr 'worker conn-mgr true)
         _
         (do
           (have-worker-opt! :opts "nil or a map" (some-fn nil? map?) worker-opts)
           (reject-removed-opts! 'worker worker-opts
             {:nthreads        "`:nthreads-handler`"
              :nthreads-worker "`:nthreads-handler`"
              :auto-start?     "`:auto-start`"})

           (reject-unknown-opts! 'worker worker-opts
             #{:handler :monitor :lock-ms :eoq-backoff-ms :throttle-ms :auto-start
               :nthreads-handler :lock-auto-extend-max-ms :max-attempts
               :error-retry-backoff-ms :on-give-up})

           (when (identical? throttle-ms :auto)
             (truss/ex-info! "[Carmine/mq] `:throttle-ms :auto` was removed in v4, use `default-throttle-ms-fn` instead"
               {:eid :carmine.mq/removed-option, :via 'worker, :opt-key :throttle-ms,
                :replacement "`default-throttle-ms-fn`"}))

           (when-not (contains? #{nil true false} (get worker-opts :auto-start))
             (truss/ex-info! "[Carmine/mq] `:auto-start` must be a boolean in v4, use `start` to start a worker later"
               {:eid :carmine.mq/removed-option, :via 'worker, :opt-key :auto-start,
                :replacement "`start`"}))

           (have-worker-opt! :nthreads-handler "a positive integer" pos-int? nthreads-handler)
           (have-worker-opt! :lock-auto-extend-max-ms "nil or a positive integer"
             (some-fn nil? pos-int?) (get worker-opts :lock-auto-extend-max-ms))
           (have-worker-opt! :max-attempts "nil or a positive integer"
             (some-fn nil? pos-int?) (get worker-opts :max-attempts))
           (have-worker-opt! :on-give-up "nil or a fn"
             (some-fn nil? fn?) (get worker-opts :on-give-up))
           (have-worker-opt! :error-retry-backoff-ms "nil, a non-negative integer, or a fn"
             (some-fn nil? nat-int? fn?) (get worker-opts :error-retry-backoff-ms))
           (have-worker-opt! :throttle-ms    "nil, a number, or a fn" (some-fn nil? number? fn?) throttle-ms)
           (have-worker-opt! :eoq-backoff-ms "a number or a fn"       (some-fn      number? fn?) eoq-backoff-ms))

         qname (enc/as-qname qname)
         worker-opts
         (conj (or worker-opts {})
           {:handler          handler
            :monitor          monitor
            :default-lock-ms  lock-ms
            :eoq-backoff-ms   eoq-backoff-ms
            :nthreads-handler nthreads-handler
            :throttle-ms      throttle-ms})

         state (impl/worker-state (client conn-mgr) qname worker-opts monitor)
         w     (CarmineMessageQueueWorker. conn-mgr state)]

     (when auto-start (start w))

     w)))
