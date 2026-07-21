(ns ^:no-doc taoensso.carmine-v4.pubsub
  "Private implementation of the public v4 Pub/Sub API."
  (:require
   [taoensso.encore :as enc]
   [taoensso.trove  :as trove]
   [taoensso.truss  :as truss]
   [taoensso.nippy.tools :as tools]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.cluster :as cluster]
   [taoensso.carmine-v4.opts  :as opts]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.utils :as utils]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write])
  (:import
   [java.net Socket SocketTimeoutException]
   [java.util.concurrent LinkedBlockingQueue TimeUnit ThreadLocalRandom]
   [java.util.concurrent.atomic AtomicLong]
   [taoensso.carmine_v4.resp.common ReadOpts]
   [taoensso.carmine_v4.resp.read Push]))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-thaw?*)

(alias 'core 'taoensso.carmine-v4)

(declare
  pubsub-close!
  ^:private listener-synced?
  ^:private close-supervised!
  ^:private supervised-subscribe!
  ^:private supervised-unsubscribe!
  ^:private supervised-ping!
  ^:private close-listener!
  ^:private close-definitive-sync-error!
  ^:private sentinel-inner-current?
  ^:private reader-loop!)

(def ^:private empty-pubsub-counts
  {:events 0, :messages 0, :handler-errors 0
   :recoveries 0, :recovery-errors 0})

(defn- pubsub-stats-snapshot [since-client-time-ms counts]
  {:since-client-time-ms since-client-time-ms
   :snapshot-client-time-ms (System/currentTimeMillis)
   :counts (merge empty-pubsub-counts counts)})

(deftype PubSubConn
  [conn in out on-message on-event public-listener ^long epoch terminal-signal-fn
   default-timeout-ms
   since-client-time-ms
   open?_ close-data_ subs_ expectations_ write-lock reader-thread_
   ping-thread_
   ^AtomicLong n-events ^AtomicLong n-messages
   ^AtomicLong n-handler-errors]

  java.io.Closeable
  (close [this] (close-listener! this {:reason :requested}))

  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine.PubSubConn"
      {:open? (open?_), :subs @subs_}))

  clojure.lang.IDeref
  (deref [this]
    (let [{:keys [addr conn-opts]} @conn]
      {:open? (open?_)
       :subs @subs_
       :conn {:addr addr, :resp3? (boolean (get-in conn-opts [:init :resp3?]))}
       :close-data @close-data_
       :stats
       (pubsub-stats-snapshot since-client-time-ms
         {:events (.get n-events)
          :messages (.get n-messages)
          :handler-errors (.get n-handler-errors)})})))

(do (enc/def-print-impl [x PubSubConn] (str "#" x)))

(deftype PubSubListener
  [conn-opts on-message on-event default-timeout-ms ping-ms recovery-opts
   since-client-time-ms
   desired_ inner_ open?_ close-data_ lifecycle-lock ops-lock wake-queue
   supervisor-thread_
   last-recovery_ last-error_
   ^AtomicLong epoch ^AtomicLong epoch-seq
   ^AtomicLong n-events ^AtomicLong n-messages
   ^AtomicLong n-recoveries ^AtomicLong n-recovery-errors
   ^AtomicLong n-handler-errors]

  java.io.Closeable
  (close [this] (close-supervised! this))

  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine.PubSubListener"
      {:open? (open?_), :epoch (.get epoch), :desired-subs @desired_}))

  clojure.lang.IDeref
  (deref [this]
    (let [inner-state (when-let [inner @inner_] @inner)
          counts
          {:events (.get n-events)
           :messages (.get n-messages)
           :recoveries (.get n-recoveries)
           :recovery-errors (.get n-recovery-errors)
           :handler-errors (.get n-handler-errors)}]
      {:open? (open?_)
       :synced? (listener-synced? this)
       :subs (or (:subs inner-state) {:channels #{}, :patterns #{}})
       :desired-subs @desired_
       :conn (:conn inner-state)
       :close-data @close-data_
       :recovery
       {:epoch (.get epoch)
        :recovering?
        (and (open?_)
          (or (not (:open? inner-state))
              ;; Reconnected, but still awaiting `:recovered`
              (let [e (.get epoch)]
                (and (pos? e) (not= e (:epoch @last-recovery_))))))
        :last-recovery @last-recovery_
        :last-error @last-error_}
       :stats (pubsub-stats-snapshot since-client-time-ms counts)})))

(do (enc/def-print-impl [x PubSubListener] (str "#" x)))

(defn- qname [x]
  (let [s (cond
            (string? x)  x
            (keyword? x) (if-let [ns (namespace x)] (str ns "/" (name x)) (name x))
            :else
            (truss/ex-info! "[Carmine] Pub/Sub names must be strings or keywords"
              {:eid :carmine.pubsub/invalid-name
               :name (enc/typed-val x)}))

        ;; Redis must get and so acknowledge the name as given: no null prefix
        ;; (reserved by Carmine), and no chars that UTF-8 can't encode
        [sent cause]
        (try
          [(enc/utf8-ba->str (write/arg-payload-bytes s))]
          (catch clojure.lang.ExceptionInfo e [nil e]))]

    (if (= s sent)
      s
      (truss/ex-info! "[Carmine] Pub/Sub name can't be sent to Redis unchanged"
        {:eid :carmine.pubsub/invalid-name
         :name x}
        cause))))

(defn- names [xs]
  (mapv qname (or xs [])))

(defn- normalize-timeout-ms [opts default-timeout-ms]
  (let [timeout-ms (get opts :timeout-ms default-timeout-ms)]
    (utils/have-opt! :carmine.pubsub/invalid-opts "Pub/Sub options" :timeout-ms "nil or a non-negative integer"
      (utils/nil-or nat-int?) timeout-ms)))

(defn- normalize-sub-opts [operation opts default-timeout-ms]
  (let [opts (or opts {})]
    (utils/have-opts! :carmine.pubsub/invalid-opts (if (= operation :init) "Pub/Sub `:init-subs`" "Pub/Sub options")
      (if (= operation :init)
        #{:channels :patterns}
        #{:channels :patterns :timeout-ms})
      opts)
    (let [channels (names (:channels opts))
          patterns (names (:patterns opts))]
      (when (and (= operation :subscribe)
                 (not (or (seq channels) (seq patterns))))
        (truss/ex-info! "[Carmine] Pub/Sub subscribe requires a channel or pattern"
          {:eid :carmine.pubsub/empty-subscribe}))
      {:channels channels
       :patterns patterns
       :timeout-ms (normalize-timeout-ms opts default-timeout-ms)})))

(defn- normalize-ping-opts [opts default-timeout-ms]
  (let [opts (or opts {})]
    (utils/have-opts! :carmine.pubsub/invalid-opts "Pub/Sub ping options" #{:message :timeout-ms} opts)
    (let [message (get opts :message)]
      {:message (when (some? message) (qname message))
       :timeout-ms (normalize-timeout-ms opts default-timeout-ms)})))

(defn- listener-default-timeout-ms [^PubSubListener listener]
  (.-default-timeout-ms listener))

(defn- listener-closed-error
  "`listener` may be a `PubSubListener`, or one of its `PubSubConn`s."
  [listener cause]
  (truss/ex-info "[Carmine] Pub/Sub listener is closed"
    (enc/assoc-some
      {:eid :carmine.pubsub/listener-closed
       :listener
       (if (instance? PubSubConn listener)
         (.-public-listener ^PubSubConn listener)
         listener)}
      :cause cause)
    cause))

(defn- log-listener-event!
  "Logs allowlisted Pub/Sub event data and the original throwable through
  Trove. The configured Trove backend owns failure handling."
  [id level event error]
  (let [data
        (cond-> (select-keys event
                  [:kind :epoch :reason :attempt :old-addr :new-addr])
          error (assoc :error-class (.getName ^Class (class error))))]
    (trove/log! {:level level, :id id, :data data, :error error}))
  nil)

(defn- default-log-listener-event! [{:keys [kind reason cause] :as event}]
  (case kind
    :handler-error  (log-listener-event! :carmine.pubsub/handler-error  :error event cause)
    :conn-error     (log-listener-event! :carmine.pubsub/conn-error     :warn  event cause)
    :recovery-error (log-listener-event! :carmine.pubsub/recovery-error :warn  event cause)
    :recovered      (log-listener-event! :carmine.pubsub/recovered      :info  event nil)
    :closed         (when-not (contains? #{:requested :conn-error :startup-error} reason) ; Startup errors are thrown
                      (log-listener-event! :carmine.pubsub/closed :error event cause))
    nil))

(defn- notify-listener! [on-event event]
  (if on-event
    (try
      (on-event event)
      (catch Throwable t
        (log-listener-event! :carmine.pubsub/on-event-error :error event t)
        (when (instance? InterruptedException t)
          (.interrupt (Thread/currentThread)))))
    (default-log-listener-event! event))
  nil)

(def ^:private handler-source
  ;; A plain ThreadLocal is intentionally not inherited or conveyed into async
  ;; work started by a handler. It identifies only Carmine's synchronous call.
  (ThreadLocal.))

(defn- inner-public-listener [^PubSubConn inner]
  (.-public-listener inner))

(defn- handler-reader-for?
  "Returns true when the current direct handler invocation belongs to the same
  public listener as `target`, even if recovery has replaced its physical
  connection epoch."
  [^PubSubConn target]
  (when-let [^PubSubConn source (.get ^ThreadLocal handler-source)]
    (and (identical? (Thread/currentThread) @(.-reader-thread_ source))
         (identical? (inner-public-listener source)
           (inner-public-listener target)))))

(defn- inner-event [^PubSubConn inner kind]
  {:kind kind
   :listener (inner-public-listener inner)
   :epoch (.-epoch inner)})

(defn- supervised-event [^PubSubListener listener kind epoch]
  {:kind kind, :listener listener, :epoch epoch})

(defn- drain-expectations! [listener cause]
  (let [expectations_ (.-expectations_ ^taoensso.carmine_v4.pubsub.PubSubConn listener)
        pending       (first (swap-vals! expectations_ (constantly clojure.lang.PersistentQueue/EMPTY)))
        error         (listener-closed-error listener cause)]
    (run! #(deliver (:result_ %) error) pending)))

(defn- pop-expectation!
  "Removes and returns the oldest expectation, if any."
  [listener]
  (let [expectations_ (.-expectations_ ^taoensso.carmine_v4.pubsub.PubSubConn listener)
        [pending]     (swap-vals! expectations_ #(if (seq %) (pop %) %))]
    (first pending)))

(defn- complete-expectation!
  "Applies an acknowledgement or pong to the oldest expectation, which is for
  one command and the `:n` replies that Redis sends for it. Returns that
  expectation if this completed it, or `::unexpected` if it's for another kind
  of reply or there is none. Replies arrive in command order, so the latter
  means that the queue no longer matches the connection."
  [listener event]
  (let [expectations_ (.-expectations_ ^taoensso.carmine_v4.pubsub.PubSubConn listener)
        result_       (volatile! nil)]
    (swap! expectations_
      (fn [pending]
        (let [{:keys [kind n] :as expectation} (first pending)]
          (cond
            (not= kind (:kind event))
            (do (vreset! result_ ::unexpected) pending)

            (> (long n) 1)
            (do
              (vreset! result_ nil)
              (into (empty pending)
                (cons (assoc expectation :n (dec (long n))) (next pending))))

            :else
            (do (vreset! result_ expectation) (pop pending))))))
    (let [result @result_]
      (when (map? result) (deliver (:result_ result) event))
      result)))

(defn frame->event
  "Normalizes one RESP2 Pub/Sub array, RESP3 push, or PING reply."
  [frame]
  (let [raw  (if (instance? Push frame) (:data frame) frame)
        data (when (vector? raw) raw)
        kind (when-let [x (first data)]
               (when (string? x) (keyword (.toLowerCase ^String x))))]
    (case kind
      :message
      {:kind :message, :channel (nth data 1 nil), :payload (nth data 2 nil)}

      :pmessage
      {:kind :pmessage, :pattern (nth data 1 nil), :channel (nth data 2 nil)
       :payload (nth data 3 nil)}

      :subscribe
      {:kind :subscribe, :channel (nth data 1 nil), :count (nth data 2 nil)}

      :unsubscribe
      {:kind :unsubscribe, :channel (nth data 1 nil), :count (nth data 2 nil)}

      :psubscribe
      {:kind :psubscribe, :pattern (nth data 1 nil), :count (nth data 2 nil)}

      :punsubscribe
      {:kind :punsubscribe, :pattern (nth data 1 nil), :count (nth data 2 nil)}

      :pong
      (let [payload (nth data 1 nil)]
        {:kind :pong, :payload (when-not (= payload "") payload)})

      (if (string? raw)
        {:kind :pong, :payload (when-not (#{"" "PONG"} raw) raw)}
        {:kind :other, :raw raw}))))

(defn- update-subs! [subs_ {:keys [kind channel pattern]}]
  (case kind
    :subscribe   (swap! subs_ update :channels conj channel)
    :unsubscribe (swap! subs_ update :channels disj channel)
    :psubscribe  (swap! subs_ update :patterns conj pattern)
    :punsubscribe (swap! subs_ update :patterns disj pattern)
    nil))

(def ^:private subscription-expectation-kinds
  #{:subscribe :unsubscribe :psubscribe :punsubscribe})

(defn- pending-subscription-ack? [^PubSubConn listener]
  (boolean
    (some #(contains? subscription-expectation-kinds (:kind %))
      @(.-expectations_ listener))))

(defn- listener-synced? [^PubSubListener listener]
  ;; Never waits for ops-lock: an operation may hold it while blocked on a
  ;; write. An operation that's still being applied on another thread may
  ;; so be seen as not yet applied.
  (let [inner @(.-inner_ listener)]
    (boolean
      (and @(.-open?_ listener)
           inner
           @(.-open?_ ^PubSubConn inner)
           (= @(.-desired_ listener) @(.-subs_ ^PubSubConn inner))
           (not (pending-subscription-ack? inner))))))

(defn ^:public pubsub-listener?
  "Returns true iff `x` is a Pub/Sub listener."
  [x]
  (instance? PubSubListener x))

(defn ^:public pubsub-listener-stats
  "Returns a snapshot of stats for `listener`, same as `(:stats @listener)`."
  [listener]
  (when-not (pubsub-listener? listener)
    (truss/ex-info! "[Carmine] Expected a Pub/Sub listener"
      {:eid :carmine.pubsub/invalid-listener
       :listener (enc/typed-val listener)}))
  (:stats @listener))

(defn- conn-error-cause
  "Returns the cause to report for a connection closed by error `t`. An IO
  error (incl. one wrapped by the reader) is reported as a stalled or lost
  connection, anything else as is."
  [^PubSubConn listener t]
  (let [io-error
        (enc/cond
          (instance? java.io.IOException t) t
          (= (get (ex-data t) :eid) :carmine.read/unexpected-read-error)
          (let [cause (ex-cause t)]
            (when (instance? java.io.IOException cause) cause)))]
    (enc/cond
      (nil? io-error) t

      (instance? SocketTimeoutException io-error)
      (truss/ex-info
        "[Carmine] Pub/Sub connection stalled: no reply to keepalive pings (see `:ping-ms`)"
        (enc/assoc-some
          {:eid :carmine.pubsub/stalled
           :addr (conns/conn-addr (.-conn listener))}
          :read-timeout-ms
          (try
            (.getSoTimeout ^Socket (:socket @(.-conn listener)))
            (catch Throwable _ nil)))
        io-error)

      :else
      (truss/ex-info "[Carmine] Pub/Sub connection lost"
        {:eid :carmine.pubsub/conn-lost
         :addr (conns/conn-addr (.-conn listener))}
        io-error))))

(defn- close-listener! [^PubSubConn listener data]
  (when (compare-and-set! (.-open?_ listener) true false)
    (let [data
          (if (= (:reason data) :conn-error)
            (update data :cause #(conn-error-cause listener %))
            data)]
      (reset! (.-close-data_ listener) (select-keys data [:reason :cause]))
      (when-let [^Thread ping-thread @(.-ping-thread_ listener)]
        (when-not (identical? ping-thread (Thread/currentThread))
          (.interrupt ping-thread)))
      (conns/conn-close! (.-conn listener)
        {:via 'pubsub-close!, :cause (:cause data)
         :listener (.-public-listener listener)})
      (drain-expectations! listener (:cause data))
      ;; Private supervision must never depend on the public listener function.
      (when-let [terminal-signal-fn (.-terminal-signal-fn listener)]
        (try
          (terminal-signal-fn
            {:inner listener, :reason (:reason data), :cause (:cause data)})
          (catch Throwable t
            (log-listener-event! :carmine.pubsub/terminal-signal-error :error
              (inner-event listener :terminal-signal-error) t))))
      (when (= (:reason data) :conn-error)
        (notify-listener! (.-on-event listener)
          (assoc (inner-event listener :conn-error) :cause (:cause data))))
      true)))

(defn ^:public pubsub-close!
  "Closes `listener` and its connection. Returns true iff this call closed
  it. `:on-event` gets a `:closed` event before this returns.

  Doesn't wait for callbacks: one that is running may finish after this
  returns."
  [listener]
  (truss/have pubsub-listener? listener)
  (boolean (close-supervised! listener)))

(defn- handler-error! [^PubSubConn listener redis-event cause]
  (.incrementAndGet ^AtomicLong (.-n-handler-errors listener))
  (when @(.-open?_ listener)
    (notify-listener! (.-on-event listener)
      (assoc (inner-event listener :handler-error)
        :redis-event redis-event
        :cause cause))))

(defn- invoke-handler! [^PubSubConn listener event]
  (let [event (assoc event
                :listener (inner-public-listener listener)
                :epoch (.-epoch listener))
        prior (.get ^ThreadLocal handler-source)
        _     (update-subs! (.-subs_ listener) event)
        completed
        (when (or (= (:kind event) :pong)
                  (contains? subscription-expectation-kinds (:kind event)))
          (complete-expectation! listener event))]
    (when (identical? completed ::unexpected)
      ;; As after an acknowledgement timeout: reconnect to resubscribe
      (close-listener! listener
        {:reason :conn-error
         :cause
         (truss/ex-info "[Carmine] Unexpected Pub/Sub acknowledgement"
           {:eid :carmine.pubsub/unexpected-ack
            :event event})}))
    ;; The reply to a keepalive ping isn't an event for users
    (when-not (or (identical? completed ::unexpected) (:keepalive? completed))
      (.incrementAndGet ^AtomicLong (.-n-events listener))
      (when (#{:message :pmessage} (:kind event))
        (.incrementAndGet ^AtomicLong (.-n-messages listener)))
      (.set ^ThreadLocal handler-source listener)
      (try
        (if (#{:message :pmessage} (:kind event))
          (try
            ((.-on-message listener) event)
            (catch Throwable t
              (handler-error! listener event t)
              (when (instance? InterruptedException t)
                (.interrupt (Thread/currentThread)))))
          ;; Acknowledgements, pongs, and unrecognised frames
          (notify-listener! (.-on-event listener) event))
        (finally
          (if prior
            (.set ^ThreadLocal handler-source prior)
            (.remove ^ThreadLocal handler-source)))))))

(def ^:private transient-pubsub-reply-codes
  "Codes of reply errors that Redis may return to (P)SUBSCRIBE while in a
  temporary state: running a slow script, or over its memory limit."
  #{"BUSY" "OOM"})

(defn- transient-pubsub-reply-error? [t]
  (and (com/reply-error? {:eid :carmine.read/redis-error-reply} t)
       (contains? transient-pubsub-reply-codes (:code (ex-data t)))))

(defn- reject-expectation!
  "Handles Redis's rejection of the command that `expectation` is for, then
  gives the rejection to any waiting caller. A rejected ping changes nothing,
  and still shows that the connection is alive. A rejected subscription change
  leaves Redis without subscriptions that the listener wants: a rejection that
  could pass (see `transient-pubsub-reply-codes`) replaces the connection to
  try again, and any other closes the listener. A connection that has been
  replaced leaves that to its replacement, which sends the same commands."
  [^PubSubConn listener expectation error]
  (let [deliver! #(deliver (:result_ expectation) error)]
    (try
      (when (contains? subscription-expectation-kinds (:kind expectation))
        (let [^PubSubListener public (.-public-listener listener)]
          (cond
            (transient-pubsub-reply-error? error)
            (close-listener! listener {:reason :conn-error, :cause error})

            ;; A waiting caller is released once the listener is closed, but
            ;; before its `:closed` callback runs
            (identical? listener @(.-inner_ public))
            (close-definitive-sync-error! public error deliver!))))
      (finally (deliver!)))))

(defn- reader-loop! [^PubSubConn listener ^ReadOpts read-opts]
  (try
    (while @(.-open?_ listener)
      (let [frame (read/read-reply-or-push read-opts (.-in listener))]
        (if (com/reply-error? frame)
          (if (com/reply-error? {:eid :carmine.read/redis-error-reply} frame)
            (if-let [expectation (pop-expectation! listener)]
              (reject-expectation! listener expectation frame)
              (close-listener! listener
                {:reason :conn-error, :cause frame}))
            (close-listener! listener
              {:reason :conn-error, :cause frame}))
          (invoke-handler! listener (frame->event frame)))))
    (catch Throwable t
      (when @(.-open?_ listener)
        (close-listener! listener
          {:reason :conn-error, :cause t})))
    (finally
      (drain-expectations! listener
        (:cause @(.-close-data_ listener))))))

(defn- timeout-error [operation timeout-ms]
  (truss/ex-info "[Carmine] Timed out awaiting Pub/Sub acknowledgement"
    {:eid :carmine.pubsub/ack-timeout
     :operation operation
     :timeout-ms timeout-ms}))

(defn- await-expectation!
  "Waits for an expectation's acknowledgement, and returns its event. Returns
  `:pending` without waiting on the listener's reader thread.

  After `timeout-ms`: closes the connection and throws when
  `close-on-timeout?`, otherwise returns `:pending` and leaves the expectation
  queued for its late acknowledgement."
  [^PubSubConn listener expectation timeout-ms close-on-timeout?]
  (if (or (identical? (Thread/currentThread) @(.-reader-thread_ listener))
          (handler-reader-for? listener))
    :pending
    (let [result_ (:result_ expectation)
          result  (if (nil? timeout-ms)
                    @result_
                    (deref result_ timeout-ms ::timeout))]
      (cond
        (identical? result ::timeout)
        (if close-on-timeout?
          (let [error (timeout-error (:kind expectation) timeout-ms)]
            (close-listener! listener
              {:reason :conn-error, :cause error})
            (throw error))
          :pending)

        (instance? Throwable result) (throw result)
        :else result))))

(defn- enqueue-operation!
  "Queues one Pub/Sub command and the expectation of its `n` replies under the
  listener write lock, so that expectations are in wire order. Returns the
  expectation for a separate wait. If the write throws, the caller must close
  the connection. A `keepalive?` operation has no waiting caller, and its
  reply is hidden from users."
  [^PubSubConn listener kind command-names n & [keepalive?]]
  (let [command
        (case kind
          :subscribe    "SUBSCRIBE"
          :psubscribe   "PSUBSCRIBE"
          :unsubscribe  "UNSUBSCRIBE"
          :punsubscribe "PUNSUBSCRIBE"
          :pong         "PING")
        expectation
        {:kind kind, :n n, :keepalive? (boolean keepalive?), :result_ (promise)}]
    (locking (.-write-lock listener)
      (when-not @(.-open?_ listener)
        ;; With its cause, e.g. the rejection of this call's previous command
        (throw (listener-closed-error listener (:cause @(.-close-data_ listener)))))
      (swap! (.-expectations_ listener) conj expectation)
      ;; The listener's own conn, so mustn't mark a caller's borrowed conn
      ;; as non-reusable
      (binding [write/*conn-reusable?_ nil]
        (write/write-requests (.-out listener) [(into [command] command-names)]))
      expectation)))

(defn- enqueue-or-close!
  "Calls `f` to enqueue work, closing `listener` after any internal caller lock
  has been released if the write fails."
  [^PubSubConn listener f]
  (try
    (f)
    (catch Throwable t
      (close-listener! listener
        {:reason :conn-error, :cause t})
      (throw t))))

(defn- await-operations!
  "Waits for the expectations in order, and returns `:acknowledged`. Returns
  `:pending` on the listener reader thread.

  When `close-on-timeout?`, each expectation may take `timeout-ms`, after which
  the connection is closed and this throws. Otherwise `timeout-ms` covers all
  of them, after which this returns `:pending`."
  [^PubSubConn listener expectations timeout-ms close-on-timeout?]
  (let [deadline
        (when (and timeout-ms (not close-on-timeout?))
          (utils/timeout-deadline-nanos timeout-ms))]
    (reduce
      (fn [_ expectation]
        (let [timeout-ms
              (if deadline
                (or (utils/remaining-timeout-ms deadline) 0)
                timeout-ms)]
          (if (identical? :pending
                (await-expectation! listener expectation timeout-ms
                  close-on-timeout?))
            (reduced :pending)
            :acknowledged)))
      :acknowledged expectations)))

(defn- ping-inner!
  "Sends a `PING` and waits for its reply. Returns the `:pong` event, or
  `:pending` if it hasn't arrived after `timeout-ms`."
  [^PubSubConn listener message timeout-ms]
  (await-expectation! listener
    (enqueue-or-close! listener
      #(enqueue-operation! listener :pong
         (if (some? message) [message] []) 1))
    timeout-ms false))

(defn- enqueue-subscribe!
  "Queues normalized subscribe commands and returns their expectations."
  [^PubSubConn listener channels patterns]
  (cond-> []
    (seq channels)
    (conj (enqueue-operation! listener :subscribe channels (count channels)))
    (seq patterns)
    (conj (enqueue-operation! listener :psubscribe patterns (count patterns)))))

(defn- enqueue-unsubscribe!
  "Queues normalized unsubscribe commands and returns their expectations.
  With no names this unsubscribes from everything, which Redis acknowledges
  once per subscription, or once if there are none. `prior` is the desired
  subscriptions before this call, and so those of the connection at this
  point on the wire: both change together under the ops-lock."
  [^PubSubConn listener channels patterns prior]
  (if (or (seq channels) (seq patterns))
    (cond-> []
      (seq channels)
      (conj (enqueue-operation! listener :unsubscribe channels (count channels)))
      (seq patterns)
      (conj (enqueue-operation! listener :punsubscribe patterns (count patterns))))
    [(enqueue-operation! listener :unsubscribe  [] (max 1 (count (:channels prior))))
     (enqueue-operation! listener :punsubscribe [] (max 1 (count (:patterns prior))))]))

(defn ^:public pubsub-subscribe!
  "Subscribes `listener` to the given channels and/or patterns:
    (pubsub-subscribe! my-listener {:channels [\"alerts\"], :patterns [\"news.*\"]})

  Waits up to `:timeout-ms` (defaults to the listener's `:default-timeout-ms`)
  for Redis to confirm the change, and returns true once it has. Messages
  published after that will be delivered.

  Returns false if the change isn't confirmed yet: after the timeout, while
  reconnecting, or when called from `:on-message` or another of the
  listener's callbacks that can't wait. The change is still applied, see
  `pubsub-await-synced!`.

  Use `{:timeout-ms 0}` to not wait for confirmation, or `{:timeout-ms nil}`
  to wait without limit. Throws if the listener is closed, e.g. because Redis rejected a
  subscription.

  See also `pubsub-unsubscribe!`."
  [listener opts]
  (truss/have pubsub-listener? listener)
  (let [opts (normalize-sub-opts :subscribe opts
               (listener-default-timeout-ms listener))]
    (identical? :acknowledged (supervised-subscribe! listener opts))))

(defn ^:public pubsub-unsubscribe!
  "Unsubscribes `listener` from the given channels and/or patterns, or from
  everything when none are given:
    (pubsub-unsubscribe! my-listener {:channels [\"alerts\"]})
    (pubsub-unsubscribe! my-listener) ; Everything

  Returns true or false, as `pubsub-subscribe!`."
  ([listener] (pubsub-unsubscribe! listener nil))
  ([listener opts]
   (truss/have pubsub-listener? listener)
   (let [opts (normalize-sub-opts :unsubscribe opts
                (listener-default-timeout-ms listener))]
     (identical? :acknowledged (supervised-unsubscribe! listener opts)))))

(defn ^:public pubsub-ping!
  "Sends a `PING` on `listener`'s connection, and returns true once Redis
  has replied. Messages that Redis sent the listener before then have been
  handled by `:on-message`.

  Returns false if there's no reply yet: after `:timeout-ms` (defaults to the
  listener's `:default-timeout-ms`), while reconnecting, or when called from
  `:on-message` or another of the listener's callbacks that can't wait.
  Throws if the listener is closed.

  `opts` may have `:timeout-ms`, and a `:message` for Redis to reply with.
  To check subscriptions, use `pubsub-await-synced!`."
  ([listener] (pubsub-ping! listener nil))
  ([listener opts]
   (truss/have pubsub-listener? listener)
   (supervised-ping! listener
     (normalize-ping-opts opts (listener-default-timeout-ms listener)))))

(defn- listener-open? [^PubSubListener listener]
  @(.-open?_ listener))

(defn- listener-close-cause [^PubSubListener listener]
  (:cause @(.-close-data_ listener)))

(defn- listener-reader-thread? [^PubSubListener listener]
  (let [source (.get ^ThreadLocal handler-source)
        inner  @(.-inner_ listener)]
    (boolean
      (or
        (and source
             (identical? listener (inner-public-listener source))
             (identical? (Thread/currentThread)
               @(.-reader-thread_ ^PubSubConn source)))
        (and inner
             (identical? (Thread/currentThread)
               @(.-reader-thread_ ^PubSubConn inner)))))))

(defn- listener-supervisor-thread? [^PubSubListener listener]
  (identical? (Thread/currentThread) @(.-supervisor-thread_ listener)))

(defn ^:public pubsub-await-synced!
  "Waits until Redis has acknowledged all of `listener`'s current
  subscriptions, and returns true. Returns false after `:timeout-ms` (defaults
  to the listener's `:default-timeout-ms`, nil waits without limit), or when
  called from one of the listener's callbacks. Throws if the listener is
  closed.

  Useful after creating a listener or changing its subscriptions, before
  publishing messages that it should receive."
  ([listener] (pubsub-await-synced! listener nil))
  ([listener opts]
   (truss/have pubsub-listener? listener)
   (let [opts       (or opts {})
         _          (utils/have-opts! :carmine.pubsub/invalid-opts "Pub/Sub options" #{:timeout-ms} opts)
         timeout-ms (normalize-timeout-ms opts (listener-default-timeout-ms listener))
         deadline   (when (some? timeout-ms)
                      (utils/timeout-deadline-nanos timeout-ms))]
     (if (or (listener-reader-thread? listener)
             (listener-supervisor-thread? listener))
       (cond
         (listener-synced? listener) true
         (not (listener-open? listener))
         (throw (listener-closed-error listener (listener-close-cause listener)))
         :else false)
       (loop []
         (let [remaining-ms (when deadline (utils/remaining-timeout-ms deadline))]
           (cond
             (listener-synced? listener) true
             (not (listener-open? listener))
             (throw (listener-closed-error listener (listener-close-cause listener)))
             (and deadline (nil? remaining-ms)) false
             :else
             (let [sleep-ms (if deadline (min 10 (long remaining-ms)) 10)]
               (try
                 (Thread/sleep sleep-ms)
                 (catch InterruptedException t
                   (.interrupt (Thread/currentThread))
                   (throw t)))
               (recur)))))))))

(defn- new-daemon-thread
  "Conveys the creating thread's bindings (e.g. Nippy opts), except
  request-scoped ones (e.g. from a `wcar` body), so callbacks can't affect
  another thread's requests."
  [name f]
  (let [bound-f (bound-fn [] (cluster/without-request-context* f))]
    (doto (Thread. ^Runnable bound-f ^String name)
      (.setDaemon true))))

(defn- new-basic-listener
  "Creates one connection-bound Pub/Sub listener epoch and starts its reader
  thread. Writes nothing to the connection, so normally the only callback
  that can run before the caller's first write is one for a connection error.
  Internal: options are validated by `pubsub-listener`."
  [{:keys [conn-opts on-message on-event default-timeout-ms ping-ms
           public-listener epoch terminal-signal-fn]}
   [n-events n-messages n-handler-errors]]
  (let [conn-opts (opts/parse-conn-opts :redis conn-opts)]
    (let [conn (conns/new-conn conn-opts)]
      (try
        (let [{:keys [socket in out]} @conn
              ;; With keepalive pings a healthy connection is never silent
              ;; for long, so a read that times out means it has stalled.
              ;; A slow handler can't cause this: no read is waiting then.
              _ (.setSoTimeout ^Socket socket
                  (if ping-ms
                    (int (min Integer/MAX_VALUE
                           (+' (long ping-ms) (long (or default-timeout-ms ping-ms)))))
                    0))
              listener
              (PubSubConn. conn in out on-message on-event public-listener
                epoch terminal-signal-fn default-timeout-ms
                (System/currentTimeMillis)
                (enc/latom true) (atom nil)
                (atom {:channels #{}, :patterns #{}})
                ;; Queue pops release completed entries even under continuous overlap.
                (atom clojure.lang.PersistentQueue/EMPTY) (Object.) (atom nil) (atom nil)
                n-events n-messages n-handler-errors)
              ;; Decode options, incl. ambient Nippy thaw opts, are captured
              ;; at construction. Supervisor threads convey construction-time
              ;; thread bindings to recovery epochs; unbound root vars remain live.
              read-opts (ReadOpts. nil nil core/*auto-thaw?*
                          core/*issue-83-workaround?* false
                          core/*raw-verbatim-strings?* tools/*thaw-opts* nil)
              reader-thread
              (new-daemon-thread "carmine-v4-pubsub-reader"
                #(reader-loop! listener read-opts))]
          (reset! (.-reader-thread_ listener) reader-thread)
          (try
            (.start ^Thread reader-thread)
            listener
            (catch Throwable t
              (close-listener! listener
                {:reason :startup-error, :cause t})
              (throw t))))
        (catch Throwable t
          (conns/conn-close! conn {:via 'pubsub-listener, :cause t})
          (throw t))))))

(defn- send-keepalive!
  "Sends a `PING` without waiting for its reply. A separate fn for tests to
  redefine."
  [^PubSubConn listener]
  (enqueue-operation! listener :pong [] 1 true))

(defn- start-ping-thread!
  "Starts a thread to ping `listener` every `ping-ms` msecs while it's open.
  Replies aren't awaited: the socket's read timeout detects their absence."
  [^PubSubConn listener ping-ms]
  (when ping-ms
    (let [^Thread ping-thread
          (new-daemon-thread "carmine-v4-pubsub-ping"
            (fn []
              (try
                (while @(.-open?_ listener)
                  (Thread/sleep (long ping-ms))
                  (when @(.-open?_ listener)
                    (send-keepalive! listener)))
                (catch InterruptedException _
                  (.interrupt (Thread/currentThread)))
                (catch Throwable t
                  (when @(.-open?_ listener)
                    (close-listener! listener
                      {:reason :conn-error, :cause t}))))))]
      ;; Registered before starting, so that closing can always interrupt it.
      ;; If already closed, the thread exits at once.
      (reset! (.-ping-thread_ listener) ping-thread)
      (.start ping-thread))))

(defn- remove-inner! [^PubSubListener listener]
  (locking (.-lifecycle-lock listener)
    (first (swap-vals! (.-inner_ listener) (constantly nil)))))

(defn- inner-terminal-signal-fn [^PubSubListener listener]
  (fn [_data]
    ;; Recovery coordination is private and happens before any synchronous
    ;; public listener notification can block the failing reader.
    ;; One pending wake-up is enough: the supervisor checks the current
    ;; connection, whichever one woke it.
    (let [^LinkedBlockingQueue wake-queue (.-wake-queue listener)]
      (when (.isEmpty wake-queue)
        (.offer wake-queue :wake)))))

(defn- new-supervised-inner
  [^PubSubListener listener inner-epoch]
  (new-basic-listener
    {:conn-opts (.-conn-opts listener)
     :on-message (.-on-message listener)
     :on-event (.-on-event listener)
     :public-listener listener
     :epoch inner-epoch
     :terminal-signal-fn (inner-terminal-signal-fn listener)
     :default-timeout-ms (.-default-timeout-ms listener)
     :ping-ms (.-ping-ms listener)}
    [(.-n-events listener) (.-n-messages listener)
     (.-n-handler-errors listener)]))

(defn- recovery-error! [^PubSubListener listener epoch attempt cause]
  (.incrementAndGet ^AtomicLong (.-n-recovery-errors listener))
  (let [event
        (enc/assoc-some
          (supervised-event listener :recovery-error epoch)
          :attempt attempt
          :cause cause)]
    (reset! (.-last-error_ listener) event)
    (when @(.-open?_ listener)
      (notify-listener! (.-on-event listener) event))))

(defn- recovery-backoff! [^PubSubListener listener attempt]
  (utils/backoff! (get (.-recovery-opts listener) :backoff-ms) attempt))

(defn- close-definitive-sync-error!
  [^PubSubListener listener cause before-notify]
  (let [epoch (.get ^AtomicLong (.-epoch listener))
        event (assoc (supervised-event listener :sync-error epoch) :cause cause)]
    ;; A definitive Redis rejection cannot be rolled back safely when later
    ;; concurrent operations touched the same names. Fail closed instead of
    ;; exposing a healthy-looking desired/actual split.
    (reset! (.-last-error_ listener) event)
    (close-supervised! listener
      {:reason :sync-error
       :cause cause
       :before-notify before-notify})))

(defn- enqueue-desired-subs!
  "Queues subscribe commands for the given desired subscriptions on `inner`
  and returns their expectations. A separate fn for tests to redefine."
  [inner {:keys [channels patterns]}]
  (enqueue-subscribe! inner (vec channels) (vec patterns)))

(defn- publish-inner!
  "Makes `inner` the supervised listener's current connection and queues
  subscribe commands for the desired subscriptions on it. Returns their
  expectations to await, or `::not-published` if the listener is closed.

  The two steps are atomic w.r.t. supervised subscription operations
  (ops-lock), and nothing is written to `inner` before them. So callbacks for
  `inner`'s replies cannot run before it is current, and subscription
  operations hit the wire in the order they were applied to `desired_`.
  Subscription operations wait for this write, as they do for each other's.
  Lock order: ops-lock -> lifecycle-lock."
  [^PubSubListener listener inner inner-epoch]
  (let [result
        (locking (.-ops-lock listener)
          (if (locking (.-lifecycle-lock listener)
                (when @(.-open?_ listener)
                  (reset! (.-inner_ listener) inner)
                  (.set ^AtomicLong (.-epoch listener) (long inner-epoch))
                  true))
            ;; Outside lifecycle-lock, so that closing can unblock a stuck write
            (try
              (enqueue-desired-subs! inner @(.-desired_ listener))
              (catch Throwable t t))
            ::not-published))]
    (if (instance? Throwable result)
      (do
        ;; Outside ops-lock, since closing notifies the public listener
        (close-listener! inner {:reason :conn-error, :cause result})
        (throw result))
      result)))

(defn- recover-inner!
  [^PubSubListener listener]
  (let [old-inner (remove-inner! listener)
        old-addr  (when old-inner (conns/conn-addr (.-conn ^PubSubConn old-inner)))]
    (when old-inner
      (close-listener! old-inner {:reason :recovered}))
    (loop [attempt 1]
      (when @(.-open?_ listener)
        (let [next-epoch (.incrementAndGet ^AtomicLong (.-epoch-seq listener))
              success?
              (try
                (let [inner (new-supervised-inner listener next-epoch)
                      expectations (publish-inner! listener inner next-epoch)]
                  (if (identical? expectations ::not-published)
                    (close-listener! inner
                      {:reason :requested})
                    (do
                      (start-ping-thread! inner (.-ping-ms listener))
                      ;; Acknowledgements are awaited outside the locks. With
                      ;; keepalive pings the read timeout detects a stalled
                      ;; connection, so that a slow handler alone can't fail
                      ;; the recovery. The Sentinel master is still checked
                      ;; while waiting.
                      (if (.-ping-ms listener)
                        (while (identical? :pending
                                 (await-operations! inner expectations
                                   (get (.-recovery-opts listener) :check-ms)
                                   false))
                          (when-not (try
                                      (sentinel-inner-current? (.-conn-opts listener) inner)
                                      (catch InterruptedException t (throw t))
                                      (catch Throwable _ true)) ; As `supervisor-loop!`
                            (truss/ex-info! "[Carmine] Pub/Sub master changed while resubscribing"
                              {:eid :carmine.pubsub/master-changed})))
                        (await-operations! inner expectations
                          (.-default-timeout-ms ^PubSubConn inner)
                          true))
                      (if-let [event
                               (locking (.-lifecycle-lock listener)
                                 (when (and @(.-open?_ listener)
                                            @(.-open?_ ^PubSubConn inner)
                                            (identical? inner @(.-inner_ listener)))
                                   (.incrementAndGet ^AtomicLong (.-n-recoveries listener))
                                   (let [event
                                         (assoc
                                           (supervised-event listener :recovered next-epoch)
                                           :old-addr old-addr
                                           :new-addr (conns/conn-addr
                                                       (.-conn ^PubSubConn inner)))]
                                     (reset! (.-last-recovery_ listener) event)
                                     (reset! (.-last-error_ listener) nil)
                                     event)))]
                        (do
                          (notify-listener! (.-on-event listener) event)
                          true)
                        (throw
                          (listener-closed-error inner
                            (:cause @(.-close-data_ ^PubSubConn inner))))))))
                (catch InterruptedException t
                  (.interrupt (Thread/currentThread))
                  (when @(.-open?_ listener) (throw t))
                  true)
                (catch Throwable t
                  (let [t ; Report why the new connection closed, if known
                        (or
                          (when (= (:eid (ex-data t)) :carmine.pubsub/listener-closed)
                            (ex-cause t))
                          t)]
                    (when-let [failed (remove-inner! listener)]
                      (close-listener! failed
                        {:reason :recovery-error, :cause t}))
                    (when @(.-open?_ listener)
                      (recovery-error! listener next-epoch attempt t))
                    false)))]
          (when (and (not success?) @(.-open?_ listener))
            (let [backoff-error
                  (try
                    (recovery-backoff! listener attempt)
                    nil
                    (catch InterruptedException t (throw t))
                    (catch Throwable t t))]
              (if backoff-error
                (do
                  (recovery-error! listener next-epoch attempt backoff-error)
                  (close-supervised! listener
                    {:reason :recovery-error
                     :cause backoff-error}))
                (recur (inc attempt))))))))))

(defn- sentinel-inner-current?
  [conn-opts inner]
  (if-let [{:keys [master-name sentinel-spec sentinel-opts]}
           (opts/get-sentinel-server conn-opts)]
    (boolean
      (enc/binding* [sentinel/*mgr-conn-opts* conn-opts]
        (sentinel/resolved-addr?
          sentinel-spec master-name sentinel-opts false
          (get-in @inner [:conn :addr]))))
    true))

(defn- supervisor-loop! [^PubSubListener listener]
  (let [check-ms (long (get (.-recovery-opts listener) :check-ms))]
    (try
      (while @(.-open?_ listener)
        (let [_ (.poll ^LinkedBlockingQueue (.-wake-queue listener)
                  check-ms TimeUnit/MILLISECONDS)
              inner @(.-inner_ listener)]
          (when @(.-open?_ listener)
            (try
              (when (or (nil? inner)
                        (not (:open? @inner))
                        (not (sentinel-inner-current? (.-conn-opts listener) inner)))
                (recover-inner! listener))
              (catch InterruptedException t (throw t))
              (catch Throwable t
                ;; A failed verification does not tear down a healthy socket.
                (recovery-error! listener
                  (.get ^AtomicLong (.-epoch listener)) nil t))))))
      (catch InterruptedException t
        (.interrupt (Thread/currentThread))
        (when @(.-open?_ listener)
          (recovery-error! listener
            (.get ^AtomicLong (.-epoch listener)) nil t)
          (close-supervised! listener
            {:reason :supervisor-error, :cause t})))
      (catch Throwable t
        (when @(.-open?_ listener)
          (recovery-error! listener
            (.get ^AtomicLong (.-epoch listener)) nil t)
          (close-supervised! listener
            {:reason :supervisor-error, :cause t}))))))

(defn- close-supervised!
  ([^PubSubListener listener]
   (close-supervised! listener {:reason :requested}))
  ([^PubSubListener listener data]
   (let [{:keys [closed? inner]}
         ;; Never wait for ops-lock: an operation may hold it while blocked
         ;; writing to Redis. Closing its socket must be able to unblock it.
         ;; lifecycle-lock alone serializes closing with inner publication.
         (locking (.-lifecycle-lock listener)
           (when (compare-and-set! (.-open?_ listener) true false)
             (reset! (.-close-data_ listener) (select-keys data [:reason :cause]))
             (.offer ^LinkedBlockingQueue (.-wake-queue listener) :wake)
             (when-let [^Thread thread @(.-supervisor-thread_ listener)]
               (when-not (identical? thread (Thread/currentThread))
                 (.interrupt thread)))
             {:closed? true
              :inner (first (swap-vals! (.-inner_ listener) (constantly nil)))}))]
     (when inner
       (close-listener! inner data))
     (when-let [f (:before-notify data)] (f))
     (when closed?
       ;; The public function runs synchronously but outside all lifecycle and
       ;; operation locks. It may re-enter `pubsub-close!` safely.
       (notify-listener! (.-on-event listener)
         (cond->
           (assoc
             (supervised-event listener :closed
               (.get ^AtomicLong (.-epoch listener)))
             :reason (:reason data))
           (:cause data) (assoc :cause (:cause data))))
       true))))

(defn- recovery-window?
  "Returns true iff `inner` is stale (replaced or closed) while the
  supervised listener remains open. This occurs during connection
  replacement. Recorded subscription intent is applied on the next connection."
  [^PubSubListener listener inner]
  (and @(.-open?_ listener)
       (or (not (identical? inner @(.-inner_ listener)))
           (not (:open? @inner)))))

(defn- pending-or-throw
  "For a failed subscription change: returns `:pending` if the connection is
  being replaced, since the next one will apply the change. Otherwise throws.
  The reader has by now closed the listener for a definitive rejection, or
  the connection for a transient one, see `reject-expectation!`."
  [^PubSubListener listener inner t]
  (cond
    (instance? InterruptedException t)
    (do (.interrupt (Thread/currentThread)) (throw t))

    (recovery-window? listener inner) :pending
    :else (throw t)))

(defn- supervised-operation-result
  "Awaits enqueued expectations outside the ops-lock."
  [^PubSubListener listener inner expectations timeout-ms]
  (cond
    (nil? inner) :pending
    (instance? Throwable expectations)
    (do
      ;; Enqueueing happened under the outer operation lock, but direct public
      ;; notification must not. Close and classify only after that lock unwinds.
      (close-listener! inner
        {:reason :conn-error
         :cause expectations})
      (pending-or-throw listener inner expectations))
    :else
    (try
      (await-operations! inner expectations timeout-ms false)
      (catch Throwable t (pending-or-throw listener inner t)))))

(defn- supervised-subscribe!
  [^PubSubListener listener
   {:keys [channels patterns timeout-ms]}]
  (when-not @(.-open?_ listener)
    (throw (listener-closed-error listener nil)))
  ;; Desired-state mutation and wire enqueue are serialized under ops-lock
  ;; so concurrent ops apply to `desired_` and hit the wire in the SAME
  ;; order. Acknowledgement awaiting happens outside the lock (timeouts may
  ;; be nil/unbounded).
  (let [[inner expectations]
        (locking (.-ops-lock listener)
          (when-not @(.-open?_ listener)
            (throw (listener-closed-error listener nil)))
          (swap! (.-desired_ listener)
            (fn [subs]
              (-> subs
                (update :channels into channels)
                (update :patterns into patterns))))
          (if-let [inner @(.-inner_ listener)]
            [inner (try (enqueue-subscribe! inner channels patterns)
                        (catch Throwable t t))]
            [nil nil]))]
    (supervised-operation-result listener inner expectations timeout-ms)))

(defn- supervised-unsubscribe!
  [^PubSubListener listener
   {:keys [channels patterns timeout-ms]}]
  (let [specific? (or (seq channels) (seq patterns))
        channels (vec (set channels))
        patterns (vec (set patterns))]
    (when-not @(.-open?_ listener)
      (throw (listener-closed-error listener nil)))
    (let [[inner expectations]
          (locking (.-ops-lock listener)
            (when-not @(.-open?_ listener)
              (throw (listener-closed-error listener nil)))
            (let [[prior]
                  (swap-vals! (.-desired_ listener)
                    (fn [subs]
                      (if specific?
                        (-> subs
                          (update :channels #(reduce disj % channels))
                          (update :patterns #(reduce disj % patterns)))
                        {:channels #{}, :patterns #{}})))]
              (if-let [inner @(.-inner_ listener)]
                [inner (try (enqueue-unsubscribe! inner channels patterns prior)
                            (catch Throwable t t))]
                [nil nil])))]
      (supervised-operation-result listener inner expectations timeout-ms))))

(defn- supervised-ping! [^PubSubListener listener opts]
  (when-not @(.-open?_ listener)
    (throw (listener-closed-error listener nil)))
  (if-let [inner @(.-inner_ listener)]
    (try
      (map? (ping-inner! inner (:message opts) (:timeout-ms opts)))
      (catch Throwable t
        ;; A failure from a stale (replaced or closed) inner means that no
        ;; pong arrived. Errors from a still-current transport rethrow.
        (if (recovery-window? listener inner)
          false
          (throw t))))
    false))

(defn- default-recovery-backoff-ms [attempt]
  (let [shift (min 5 (dec (long attempt)))
        base  (min 8000 (* 250 (bit-shift-left 1 shift)))
        jitter (.nextLong (ThreadLocalRandom/current) (inc (quot base 4)))]
    (+ base jitter)))

(defn ^:public pubsub-listener
  "Returns a new Pub/Sub listener with its own connection to Redis:

    (def my-listener
      (pubsub-listener
        {:conn-opts  {:server [\"127.0.0.1\" 6379]}
         :init-subs  {:channels [\"updates\"], :patterns [\"news.*\"]}
         :on-message (fn [{:keys [channel payload]}] (println channel payload))}))

    (wcar mgr (publish \"updates\" \"hello\")) ; Prints \"updates hello\"
    (pubsub-close! my-listener)

  Options:
    - `:conn-opts`  - See `default-conn-opts`. Standalone or Sentinel.
    - `:on-message` - Required `(fn [event])` for messages.
    - `:on-event`   - Optional `(fn [event])` for everything else.
    - `:init-subs`  - Optional `{:channels [...], :patterns [...]}`.
    - `:ping-ms`    - How often to check the connection with `PING` (default
        10000). A connection that then stays silent for `:default-timeout-ms`
        is replaced. Nil disables the check.
    - `:default-timeout-ms` - How long to wait for Redis to acknowledge
        subscription changes and pings (default 5000). Nil waits without limit.
    - `:recovery-opts` - Optional `{:check-ms _, :backoff-ms _}` for reconnecting:
        how often to check the Sentinel master (default 5000), and how long to
        wait before each attempt: nil, msecs, or `(fn [attempt])` => msecs
        (default exponential backoff).

  If its connection fails, the listener reconnects and resubscribes
  automatically. Messages published in the meantime are lost.

  Events are maps with `:kind`, `:listener`, and:
    `:message`      - `:channel`, `:payload`.
    `:pmessage`     - `:pattern`, `:channel`, `:payload`.
    `:subscribe`,  `:unsubscribe`  - `:channel`, `:count`.
    `:psubscribe`, `:punsubscribe` - `:pattern`, `:count`.
    `:pong`           - `:payload`.
    `:conn-error`     - `:cause`.
    `:recovered`      - `:old-addr`, `:new-addr`.
    `:recovery-error` - `:cause`, `:attempt`.
    `:handler-error`  - `:cause`, `:redis-event`. `:on-message` threw.
    `:closed`         - `:reason` (`:requested`, or the kind of error),
                        `:cause`.

  `:on-message` and `:on-event` block the listener while they run, and may
  be called before `pubsub-listener` returns. They should return quickly:
  hand slow work to another thread. Without `:on-event`, errors are logged.

  `:on-message` gets one message at a time, except that after reconnecting a
  call for the old connection may still be running.

  Deref a listener for its current state, incl. `:open?`, `:synced?`,
  `:subs`, and `:stats`.

  Throws if the first connection fails. Cluster and sharded Pub/Sub aren't
  supported.

  See also `pubsub-subscribe!`, `pubsub-await-synced!`, `pubsub-close!`."
  ^java.io.Closeable
  [{:keys [conn-opts on-message on-event init-subs ping-ms
           default-timeout-ms recovery-opts]
    :or {default-timeout-ms 5000, ping-ms 10000}
    :as listener-opts}]
  (let [what "Pub/Sub listener options"]
    (utils/have-opts! :carmine.pubsub/invalid-opts what
      #{:conn-opts :on-message :on-event :init-subs :ping-ms
        :default-timeout-ms :recovery-opts}
      listener-opts)
    (utils/have-opt! :carmine.pubsub/invalid-opts what :on-message "a fn" fn? on-message)
    (utils/have-opt! :carmine.pubsub/invalid-opts what :on-event   "nil or a fn" (utils/nil-or fn?) on-event)
    (utils/have-opt! :carmine.pubsub/invalid-opts what :ping-ms    "nil or a positive integer"
      (utils/nil-or pos-int?) ping-ms)
    (utils/have-opt! :carmine.pubsub/invalid-opts what :default-timeout-ms "nil or a positive integer"
      (utils/nil-or pos-int?) default-timeout-ms))
  (let [init-subs
        (when init-subs
          (normalize-sub-opts :init init-subs default-timeout-ms))
        listener-opts (assoc listener-opts :init-subs init-subs)
        conn-opts (opts/parse-conn-opts :redis conn-opts)]
    (when (opts/get-cluster-server conn-opts)
      (truss/ex-info! "[Carmine] High-level Pub/Sub does not support Redis Cluster"
        {:eid :carmine.pubsub/cluster-not-supported}))
    (let [recovery-opts (or recovery-opts {})
          _ (utils/have-opts! :carmine.pubsub/invalid-opts "Pub/Sub `:recovery-opts`" #{:check-ms :backoff-ms} recovery-opts)
          recovery-opts
          (merge {:check-ms 5000, :backoff-ms default-recovery-backoff-ms}
            recovery-opts)
          _ (utils/have-opt! :carmine.pubsub/invalid-opts "Pub/Sub `:recovery-opts`" :check-ms "a positive integer"
              pos-int? (:check-ms recovery-opts))
          _ (utils/have-opt! :carmine.pubsub/invalid-opts "Pub/Sub `:recovery-opts`" :backoff-ms
              "nil, a non-negative integer, or a fn"
              (utils/nil-or (some-fn nat-int? fn?)) (:backoff-ms recovery-opts))
          sentinel-server (opts/get-sentinel-server conn-opts)
          _
          (when (get-in sentinel-server [:sentinel-opts :prefer-read-replica?])
            (truss/ex-info! "[Carmine] Pub/Sub requires a Sentinel master"
              {:eid :carmine.pubsub/replica-manager-not-supported}))
          desired
          {:channels (set (names (:channels init-subs)))
           :patterns (set (names (:patterns init-subs)))}
          listener
          (PubSubListener.
            conn-opts on-message on-event default-timeout-ms ping-ms
            recovery-opts (System/currentTimeMillis)
            (atom desired) (atom nil) (enc/latom true) (atom nil)
            (Object.) (Object.) (LinkedBlockingQueue.) (atom nil)
            (atom nil) (atom nil)
            (AtomicLong. 0) (AtomicLong. 0) (AtomicLong. 0)
            (AtomicLong. 0) (AtomicLong. 0)
            (AtomicLong. 0) (AtomicLong. 0))]
      (try
        (let [inner        (new-supervised-inner listener 0)
              expectations (publish-inner! listener inner 0)]
          (if (identical? expectations ::not-published)
            (do
              (close-listener! inner {:reason :requested})
              ;; A callback closed the listener. That hides no startup error
              ;; unless the connection had already failed.
              (let [{:keys [reason cause]} @(.-close-data_ ^PubSubConn inner)]
                (when-not (= reason :requested)
                  (throw (listener-closed-error inner cause)))))
            (do
              (start-ping-thread! inner ping-ms)
              (when (try
                      (await-operations! inner expectations
                        default-timeout-ms true)
                      true
                      (catch Throwable t
                        ;; A callback may close the listener during startup,
                        ;; which isn't a startup error
                        (if (= (:reason @(.-close-data_ ^PubSubConn inner)) :requested)
                          false
                          (throw t))))
                ;; Started last, so that a failure above is only a startup error
                (let [supervisor-thread
                      (new-daemon-thread "carmine-v4-pubsub-supervisor"
                        #(supervisor-loop! listener))]
                  (reset! (.-supervisor-thread_ listener) supervisor-thread)
                  (.start ^Thread supervisor-thread))))))
        listener
        (catch Throwable t
          (close-supervised! listener
            {:reason :startup-error, :cause t})
          (throw t))))))
