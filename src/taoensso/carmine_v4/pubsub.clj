(ns ^:no-doc taoensso.carmine-v4.pubsub
  "Private implementation of the public v4 Pub/Sub API."
  (:require
   [clojure.set :as set]
   [taoensso.encore :as enc]
   [taoensso.trove  :as trove]
   [taoensso.truss  :as truss]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.opts  :as opts]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.utils :as utils]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write])
  (:import
   [java.net Socket]
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
  ^:private reader-loop!)

(def ^:private pubsub-stats-schema-version 2)
(def ^:private empty-pubsub-counts
  {:events 0, :messages 0, :handler-errors 0
   :recoveries 0, :recovery-errors 0})

(defn- pubsub-stats-snapshot [kind since-client-time-ms counts]
  {:schema-version pubsub-stats-schema-version
   :kind kind
   :since-client-time-ms since-client-time-ms
   :snapshot-client-time-ms (System/currentTimeMillis)
   :counts (merge empty-pubsub-counts counts)})

(deftype PubSubListener
  [conn in out handler-fn listener-fn public-listener ^long epoch terminal-signal-fn
   default-timeout-ms
   since-client-time-ms
   open?_ close-data_ subs_ expectations_ write-lock reader-thread_
   ping-thread_
   ^AtomicLong n-events ^AtomicLong n-messages
   ^AtomicLong n-handler-errors]

  java.io.Closeable
  (close [this] (pubsub-close! this))

  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine.PubSubListener"
      {:open? (open?_), :subs @subs_}))

  clojure.lang.IDeref
  (deref [this]
    (let [{:keys [host port conn-opts]} @conn]
      {:open? (open?_)
       :synced? (listener-synced? this)
       :subs @subs_
       :conn {:host host, :port port, :resp3? (boolean (get-in conn-opts [:init :resp3?]))}
       :close-data @close-data_
       :stats
       (pubsub-stats-snapshot :connection-bound since-client-time-ms
         {:events (.get n-events)
          :messages (.get n-messages)
          :handler-errors (.get n-handler-errors)})})))

(do (enc/def-print-impl [x PubSubListener] (str "#" x)))

(deftype SupervisedPubSubListener
  [conn-opts handler-fn listener-fn default-timeout-ms ping-ms recovery-opts
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
    (enc/str-impl this "taoensso.carmine.SupervisedPubSubListener"
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
        :recovering? (and (open?_) (not (:open? inner-state)))
        :last-recovery @last-recovery_
        :last-error @last-error_}
       :stats (pubsub-stats-snapshot :supervised since-client-time-ms counts)})))

(do (enc/def-print-impl [x SupervisedPubSubListener] (str "#" x)))

(defn- qname [x]
  (cond
    (string? x)  x
    (keyword? x) (if-let [ns (namespace x)] (str ns "/" (name x)) (name x))
    :else
    (truss/ex-info! "[Carmine] Pub/Sub names must be strings or keywords"
      {:eid :carmine.pubsub/invalid-name
       :name (enc/typed-val x)})))

(defn- names [xs]
  (mapv qname (or xs [])))

(defn- normalize-timeout-ms [opts default-timeout-ms]
  (let [timeout-ms (get opts :timeout-ms default-timeout-ms)]
    (truss/have [:or nil? nat-int?] timeout-ms)
    timeout-ms))

(defn- normalize-sub-opts [operation opts default-timeout-ms]
  (let [opts (or opts {})]
    (truss/have map? opts)
    (truss/have [:ks<= #{:channels :patterns :timeout-ms}] opts)
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
    (truss/have map? opts)
    (truss/have [:ks<= #{:message :timeout-ms}] opts)
    (let [message (get opts :message)]
      {:message (when (some? message) (qname message))
       :timeout-ms (normalize-timeout-ms opts default-timeout-ms)})))

(defn- listener-default-timeout-ms [listener]
  (if (instance? SupervisedPubSubListener listener)
    (.-default-timeout-ms ^SupervisedPubSubListener listener)
    (.-default-timeout-ms ^PubSubListener listener)))

(defn- listener-closed-error [listener cause]
  (truss/ex-info "[Carmine] Pub/Sub listener is closed"
    (enc/assoc-some
      {:eid :carmine.pubsub/listener-closed
       :listener listener}
      :cause cause)
    cause))

(defn- log-listener-event!
  "Logs allowlisted Pub/Sub event data and the original throwable through
  Trove. The configured Trove backend owns failure handling."
  [id level event error]
  (let [data
        (cond-> (select-keys event
                  [:kind :epoch :reason :recovering? :attempt
                   :old-addr :new-addr])
          error (assoc :error-class (.getName ^Class (class error))))]
    (trove/log! {:level level, :id id, :data data, :error error}))
  nil)

(defn- default-log-listener-event! [{:keys [kind reason recovering? cause] :as event}]
  (case kind
    :handler-error  (log-listener-event! :carmine.pubsub/handler-error  :error event cause)
    :conn-error     (log-listener-event! :carmine.pubsub/conn-error
                      (if recovering? :warn :error) event cause)
    :recovery-error (log-listener-event! :carmine.pubsub/recovery-error :warn  event cause)
    :recovered      (log-listener-event! :carmine.pubsub/recovered      :info  event nil)
    :closed         (when-not (contains? #{:requested :conn-error} reason)
                      (log-listener-event! :carmine.pubsub/closed :error event cause))
    nil))

(defn- notify-listener! [listener-fn event]
  (if listener-fn
    (try
      (listener-fn event)
      (catch Throwable t
        (log-listener-event! :carmine.pubsub/listener-fn-error :error event t)
        (when (instance? InterruptedException t)
          (.interrupt (Thread/currentThread)))))
    (default-log-listener-event! event))
  nil)

(def ^:private handler-source
  ;; A plain ThreadLocal is intentionally not inherited or conveyed into async
  ;; work started by a handler. It identifies only Carmine's synchronous call.
  (ThreadLocal.))

(defn- inner-public-listener [^PubSubListener inner]
  (or (.-public-listener inner) inner))

(defn- handler-reader-for?
  "Returns true when the current direct handler invocation belongs to the same
  public listener as `target`, even if recovery has replaced its physical
  connection epoch."
  [^PubSubListener target]
  (when-let [^PubSubListener source (.get ^ThreadLocal handler-source)]
    (and (identical? (Thread/currentThread) @(.-reader-thread_ source))
         (identical? (inner-public-listener source)
           (inner-public-listener target)))))

(defn- inner-event [^PubSubListener inner kind]
  {:kind kind
   :listener (inner-public-listener inner)
   :epoch (.-epoch inner)})

(defn- supervised-event [^SupervisedPubSubListener listener kind epoch]
  {:kind kind, :listener listener, :epoch epoch})

(defn- drain-expectations! [listener cause]
  (let [expectations_ (.-expectations_ ^taoensso.carmine_v4.pubsub.PubSubListener listener)
        pending       (first (swap-vals! expectations_ (constantly [])))
        error         (listener-closed-error listener cause)]
    (run! #(deliver (:result_ %) error) pending)))

(defn- fail-oldest-expectation! [listener error]
  (let [expectations_ (.-expectations_ ^taoensso.carmine_v4.pubsub.PubSubListener listener)
        failed_       (volatile! nil)]
    (swap! expectations_
      (fn [pending]
        (if-let [expectation (first pending)]
          (do (vreset! failed_ expectation) (subvec pending 1))
          pending)))
    (when-let [expectation @failed_]
      (deliver (:result_ expectation) error)
      expectation)))

(defn- fail-expectation! [listener expected error]
  (let [expectations_ (.-expectations_ ^taoensso.carmine_v4.pubsub.PubSubListener listener)
        [before after]
        (swap-vals! expectations_
          (fn [pending]
            (into [] (remove #(identical? % expected)) pending)))]
    (when (< (count after) (count before))
      (deliver (:result_ expected) error)
      true)))

(defn- remove-one [pending x]
  (let [n (get pending x 0)]
    (cond
      (> n 1) (assoc pending x (dec n))
      (= n 1) (dissoc pending x)
      :else   pending)))

(defn- expectation-name [{:keys [kind channel pattern]}]
  (case kind
    (:subscribe :unsubscribe)   channel
    (:psubscribe :punsubscribe) pattern
    nil))

(defn- complete-expectation! [listener event]
  (let [expectations_ (.-expectations_ ^taoensso.carmine_v4.pubsub.PubSubListener listener)
        completed_    (volatile! nil)]
    (swap! expectations_
      (fn [pending]
        (if-let [{expected-kind :kind expected-names :pending :as expectation} (first pending)]
          (if (= expected-kind (:kind event))
            (let [event-name (expectation-name event)
                  remaining  (remove-one expected-names event-name)
                  unsubscribe-all?
                  (and (nil? event-name)
                       (#{:unsubscribe :punsubscribe} expected-kind))]
              (if (or unsubscribe-all? (empty? remaining))
                (do (vreset! completed_ expectation) (subvec pending 1))
                (assoc pending 0 (assoc expectation :pending remaining))))
            pending)
          pending)))
    (when-let [expectation @completed_]
      (deliver (:result_ expectation) event)
      true)))

(defn frame->event
  "Normalizes one RESP2 Pub/Sub array, RESP3 push, or PING reply."
  [frame]
  (let [raw  (if (instance? Push frame) (:data frame) frame)
        data (when (vector? raw) raw)
        kind (when-let [x (first data)]
               (when (string? x) (keyword (.toLowerCase ^String x))))]
    (case kind
      :message
      {:kind :message, :channel (nth data 1 nil), :payload (nth data 2 nil), :raw raw}

      :pmessage
      {:kind :pmessage, :pattern (nth data 1 nil), :channel (nth data 2 nil)
       :payload (nth data 3 nil), :raw raw}

      :subscribe
      {:kind :subscribe, :channel (nth data 1 nil), :count (nth data 2 nil), :raw raw}

      :unsubscribe
      {:kind :unsubscribe, :channel (nth data 1 nil), :count (nth data 2 nil), :raw raw}

      :psubscribe
      {:kind :psubscribe, :pattern (nth data 1 nil), :count (nth data 2 nil), :raw raw}

      :punsubscribe
      {:kind :punsubscribe, :pattern (nth data 1 nil), :count (nth data 2 nil), :raw raw}

      :pong
      (let [payload (nth data 1 nil)]
        {:kind :pong, :payload (when-not (= payload "") payload), :raw raw})

      (if (string? raw)
        {:kind :pong, :payload (when-not (#{"" "PONG"} raw) raw), :raw raw}
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

(defn- pending-subscription-ack? [^PubSubListener listener]
  (boolean
    (some #(contains? subscription-expectation-kinds (:kind %))
      @(.-expectations_ listener))))

(defn- listener-synced? [listener]
  (if (instance? SupervisedPubSubListener listener)
    (let [^SupervisedPubSubListener listener listener]
      (locking (.-ops-lock listener)
        (let [inner @(.-inner_ listener)]
          (boolean
            (and @(.-open?_ listener)
                 inner
                 @(.-open?_ ^PubSubListener inner)
                 (= @(.-desired_ listener) @(.-subs_ ^PubSubListener inner))
                 (not (pending-subscription-ack? inner)))))))
    (let [^PubSubListener listener listener]
      (boolean
        (and @(.-open?_ listener)
             (not (pending-subscription-ack? listener)))))))

(defn ^:public pubsub-listener?
  "Returns true iff the given `x` is a Carmine v4 Pub/Sub listener."
  [x]
  (or (instance? PubSubListener x)
      (instance? SupervisedPubSubListener x)))

(defn ^:public pubsub-listener-stats
  "Returns a stable, versioned snapshot of `listener`'s cumulative local
  counters. A connection-bound listener reports zero recovery counts. The same
  map is available at `(:stats @listener)`. The outer deref is diagnostic and
  may gain keys independently."
  [listener]
  (when-not (pubsub-listener? listener)
    (truss/ex-info! "[Carmine] Expected a Pub/Sub listener"
      {:eid :carmine.pubsub/invalid-listener
       :listener (enc/typed-val listener)}))
  (:stats @listener))

(defn- close-listener! [^PubSubListener listener data]
  (when (compare-and-set! (.-open?_ listener) true false)
    (reset! (.-close-data_ listener) (select-keys data [:reason :cause]))
    (when-let [^Thread ping-thread @(.-ping-thread_ listener)]
      (when-not (identical? ping-thread (Thread/currentThread))
        (.interrupt ping-thread)))
    (conns/conn-close! (.-conn listener)
      {:via 'pubsub-close!, :cause (:cause data), :listener listener})
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
      (let [outer (.-public-listener listener)]
        (notify-listener! (.-listener-fn listener)
          (assoc (inner-event listener :conn-error)
            :recovering? (boolean (and outer @(.-open?_ ^SupervisedPubSubListener outer)))
            :cause (:cause data)))))
    ;; A supervised physical inner is private. Only its outer listener owns the
    ;; public terminal transition and `:closed` event.
    (when-not (.-public-listener listener)
      (notify-listener! (.-listener-fn listener)
        (cond-> (assoc (inner-event listener :closed) :reason (:reason data))
          (:cause data) (assoc :cause (:cause data)))))
    true))

(defn ^:public pubsub-close!
  "Idempotently closes `listener` and its dedicated connection. Returns true
  if this call initiates closing; other calls return false. Before the winning
  call returns, a configured `:listener-fn` is invoked synchronously with
  `:closed`."
  [listener]
  (truss/have pubsub-listener? listener)
  (boolean
    (if (instance? SupervisedPubSubListener listener)
      (close-supervised! listener)
      (close-listener! listener {:reason :requested, :via 'pubsub-close!}))))

(defn- handler-error! [^PubSubListener listener redis-event cause]
  (.incrementAndGet ^AtomicLong (.-n-handler-errors listener))
  (when @(.-open?_ listener)
    (notify-listener! (.-listener-fn listener)
      (assoc (inner-event listener :handler-error)
        :redis-event redis-event
        :cause cause))))

(defn- invoke-handler! [^PubSubListener listener event]
  (let [event (assoc event
                :listener (inner-public-listener listener)
                :epoch (.-epoch listener))
        prior (.get ^ThreadLocal handler-source)]
    (.incrementAndGet ^AtomicLong (.-n-events listener))
    (when (#{:message :pmessage} (:kind event))
      (.incrementAndGet ^AtomicLong (.-n-messages listener)))
    (update-subs! (.-subs_ listener) event)
    (when (or (= (:kind event) :pong)
              (contains? subscription-expectation-kinds (:kind event)))
      (complete-expectation! listener event))
    (.set ^ThreadLocal handler-source listener)
    (try
      ((.-handler-fn listener) event)
      (catch Throwable t
        (handler-error! listener event t)
        (when (instance? InterruptedException t)
          (.interrupt (Thread/currentThread))))
      (finally
        (if prior
          (.set ^ThreadLocal handler-source prior)
          (.remove ^ThreadLocal handler-source))))))

(defn- reader-loop! [^PubSubListener listener ^ReadOpts read-opts]
  (try
    (while @(.-open?_ listener)
      (let [frame (read/read-reply-or-push read-opts (.-in listener))]
        (if (com/reply-error? frame)
          (if (com/reply-error? {:eid :carmine.read/redis-error-reply} frame)
            (if-let [expectation (fail-oldest-expectation! listener frame)]
              ;; A reader-thread operation has no waiting caller to classify a
              ;; Redis rejection. Close the epoch so readiness cannot become a
              ;; false positive and a supervisor can reconcile recorded intent.
              (when @(get expectation :detached?_)
                (close-listener! listener
                  {:reason :conn-error, :via 'reader-loop!, :cause frame}))
              (close-listener! listener
                {:reason :conn-error, :via 'reader-loop!, :cause frame}))
            (close-listener! listener
              {:reason :conn-error, :via 'reader-loop!, :cause frame}))
          (invoke-handler! listener (frame->event frame)))))
    (catch Throwable t
      (when @(.-open?_ listener)
        (close-listener! listener
          {:reason :conn-error, :via 'reader-loop!, :cause t})))
    (finally
      (drain-expectations! listener
        (:cause @(.-close-data_ listener))))))

(defn- timeout-error [operation timeout-ms]
  (truss/ex-info "[Carmine] Timed out awaiting Pub/Sub acknowledgement"
    {:eid :carmine.pubsub/ack-timeout
     :operation operation
     :timeout-ms timeout-ms}))

(defn- projected-subs
  "Returns the projected server subscription state at the current wire
  position. The result is `subs_` plus the changes from pending expectations.
  The caller must hold the listener write lock.

  The function snapshots expectations before subscriptions. The reader applies
  an acknowledgement to `subs_` before it changes the expectation, so an
  operation absent from the earlier expectation snapshot is already in the
  later subscription snapshot. An operation still present in the earlier
  snapshot may have its delta applied twice, which is a harmless idempotent set
  operation."
  [^PubSubListener listener]
  (let [expectations @(.-expectations_ listener)
        subs         @(.-subs_         listener)]
    (reduce
      (fn [acc {:keys [kind pending]}]
        (let [names (remove nil? (keys pending))]
          (case kind
            :subscribe    (update acc :channels into names)
            :unsubscribe  (update acc :channels #(reduce disj % names))
            :psubscribe   (update acc :patterns into names)
            :punsubscribe (update acc :patterns #(reduce disj % names))
            acc)))
      subs expectations)))

(defn- await-expectation! [^PubSubListener listener expectation timeout-ms]
  (if (or (identical? (Thread/currentThread) @(.-reader-thread_ listener))
          (handler-reader-for? listener))
    (do (reset! (:detached?_ expectation) true) :pending)
    (let [result_ (:result_ expectation)
          result  (if (nil? timeout-ms)
                    @result_
                    (deref result_ timeout-ms ::timeout))]
      (when (identical? result ::timeout)
        (let [error (timeout-error (:kind expectation) timeout-ms)]
          ;; The timed-out acknowledgement may still be in flight. Close rather
          ;; than allow it to satisfy a later operation with the same shape.
          (close-listener! listener
            {:reason :conn-error, :via 'await-expectation!, :cause error})
          (throw error)))
      (if (instance? Throwable result)
        (throw result)
        result))))

(defn- enqueue-operation!
  "Queues one Pub/Sub command and its acknowledgement expectation under the
  listener write lock. Returns the expectation for a separate wait.
  `expected-names` may be a zero-argument function. If so, this function calls
  it under the lock at the command's exact wire position."
  [^PubSubListener listener kind command-names expected-names]
  (when-not @(.-open?_ listener)
    (throw (listener-closed-error listener nil)))
  (let [command
        (case kind
          :subscribe    "SUBSCRIBE"
          :psubscribe   "PSUBSCRIBE"
          :unsubscribe  "UNSUBSCRIBE"
          :punsubscribe "PUNSUBSCRIBE"
          :pong         "PING")
        {:keys [expectation error]}
        (locking (.-write-lock listener)
          (when-not @(.-open?_ listener)
            (throw (listener-closed-error listener nil)))
          (let [expected-names
                (if (fn? expected-names) (expected-names) expected-names)
                expectation
                {:kind kind
                 :pending (frequencies expected-names)
                 :detached?_ (atom false)
                 :result_ (promise)}]
            (swap! (.-expectations_ listener) conj expectation)
            (try
              (write/write-requests (.-out listener) [(into [command] command-names)])
              {:expectation expectation}
              (catch Throwable t
                {:expectation expectation, :error t}))))]
    (if error
      (do
        (fail-expectation! listener expectation error)
        (throw error))
      expectation)))

(defn- enqueue-or-close!
  "Calls `f` to enqueue work, closing `listener` after any internal caller lock
  has been released if the write fails."
  [^PubSubListener listener f]
  (try
    (f)
    (catch Throwable t
      (close-listener! listener
        {:reason :conn-error, :via 'enqueue-operation!, :cause t})
      (throw t))))

(defn- await-operations!
  "Waits for the expectations in order. Returns `:pending` on the listener
  reader thread."
  [^PubSubListener listener expectations timeout-ms]
  (if (or (identical? (Thread/currentThread) @(.-reader-thread_ listener))
          (handler-reader-for? listener))
    (do (run! #(reset! (:detached?_ %) true) expectations) :pending)
    (reduce
      (fn [_ expectation]
        (let [result (await-expectation! listener expectation timeout-ms)]
          (if (identical? result :pending) (reduced :pending) :acknowledged)))
      :acknowledged expectations)))

(defn- write-operation!
  ([listener kind names timeout-ms]
   (write-operation! listener kind names names timeout-ms))
  ([^PubSubListener listener kind command-names expected-names timeout-ms]
   (await-expectation! listener
     (enqueue-or-close! listener
       #(enqueue-operation! listener kind command-names expected-names))
     timeout-ms)))

(defn- enqueue-subscribe!
  "Queues normalized subscribe commands and returns their expectations."
  [^PubSubListener listener channels patterns]
  (cond-> []
    (seq channels)
    (conj (enqueue-operation! listener :subscribe channels channels))
    (seq patterns)
    (conj (enqueue-operation! listener :psubscribe patterns patterns))))

(defn- enqueue-unsubscribe!
  "Queues normalized unsubscribe commands and returns their expectations.
  For unsubscribe-all, projects acknowledgements at each command's exact wire
  position, so concurrent operations cannot block the expectation queue. See
  [[projected-subs]]."
  [^PubSubListener listener specific? channels patterns]
  (if specific?
    (cond-> []
      (seq channels)
      (conj (enqueue-operation! listener :unsubscribe channels channels))
      (seq patterns)
      (conj (enqueue-operation! listener :punsubscribe patterns patterns)))
    [(enqueue-operation! listener :unsubscribe []
       #(if-let [xs (seq (:channels (projected-subs listener)))] (vec xs) [nil]))
     (enqueue-operation! listener :punsubscribe []
       #(if-let [xs (seq (:patterns (projected-subs listener)))] (vec xs) [nil]))]))

(defn ^:public pubsub-subscribe!
  "Subscribes an open listener to channels and/or patterns.

  `opts` accepts `:channels`, `:patterns`, and `:timeout-ms`. Names must be
  strings or keywords. The function waits for Redis acknowledgements and
  returns `:acknowledged`, except that a call on the listener's reader thread,
  including from `:handler-fn`, returns `:pending` without
  waiting. A supervised (`:recovery`) listener also returns `:pending` if it
  records the subscription during connection replacement; recovery then
  applies the subscription."
  [listener opts]
  (truss/have pubsub-listener? listener)
  (let [{:keys [channels patterns timeout-ms] :as opts}
        (normalize-sub-opts :subscribe opts
          (listener-default-timeout-ms listener))]
    (if (instance? SupervisedPubSubListener listener)
      (supervised-subscribe! listener opts)
      (await-operations! listener
        (enqueue-or-close! listener
          #(enqueue-subscribe! listener channels patterns))
        timeout-ms))))

(defn ^:public pubsub-unsubscribe!
  "Unsubscribes an open listener from channels and/or patterns.

  `opts` accepts `:channels`, `:patterns`, and `:timeout-ms`. With omitted or
  nil `opts`, or with no names, unsubscribes from all channels and patterns.
  The function waits for acknowledgements and returns `:acknowledged`, except
  that a call on the listener's reader thread, including from `:handler-fn`,
  returns `:pending` without waiting. A supervised (`:recovery`) listener also
  returns `:pending` if it records the unsubscription during connection
  replacement; recovery then applies the unsubscription."
  ([listener] (pubsub-unsubscribe! listener nil))
  ([listener opts]
   (truss/have pubsub-listener? listener)
   (let [{:keys [channels patterns timeout-ms] :as opts}
         (normalize-sub-opts :unsubscribe opts
           (listener-default-timeout-ms listener))]
     (if (instance? SupervisedPubSubListener listener)
       (supervised-unsubscribe! listener opts)
       (let [specific? (or (seq channels) (seq patterns))]
         (await-operations! listener
           (enqueue-or-close! listener
             #(enqueue-unsubscribe! listener specific? channels patterns))
           timeout-ms))))))

(defn ^:public pubsub-ping!
  "Pings an open Pub/Sub listener and returns its pong event.
  `opts` accepts `:message` and `:timeout-ms`. A call on the listener's reader
  thread, including from `:handler-fn`, returns `:pending`
  without waiting for the pong. A supervised (`:recovery`) listener returns
  `:recovering` during connection replacement, which means it received no pong;
  the ping may or may not have reached Redis before the connection closed. The
  listener remains open. A pong proves transport liveness only; use
  [[pubsub-await-synced!]] as the subscription-readiness barrier."
  ([listener] (pubsub-ping! listener nil))
  ([listener opts]
   (truss/have pubsub-listener? listener)
   (let [{:keys [message timeout-ms] :as opts}
         (normalize-ping-opts opts (listener-default-timeout-ms listener))]
     (if (instance? SupervisedPubSubListener listener)
       (supervised-ping! listener opts)
       (write-operation! listener :pong
         (if (some? message) [message] []) [] timeout-ms)))))

(defn- listener-open? [listener]
  (if (instance? SupervisedPubSubListener listener)
    @(.-open?_ ^SupervisedPubSubListener listener)
    @(.-open?_ ^PubSubListener listener)))

(defn- listener-close-cause [listener]
  (:cause
    @(if (instance? SupervisedPubSubListener listener)
       (.-close-data_ ^SupervisedPubSubListener listener)
       (.-close-data_ ^PubSubListener listener))))

(defn- listener-reader-thread? [listener]
  (let [source (.get ^ThreadLocal handler-source)
        inner
        (if (instance? SupervisedPubSubListener listener)
          @(.-inner_ ^SupervisedPubSubListener listener)
          listener)]
    (boolean
      (or
        (and source
             (identical? listener (inner-public-listener source))
             (identical? (Thread/currentThread)
               @(.-reader-thread_ ^PubSubListener source)))
        (and inner
             (identical? (Thread/currentThread)
               @(.-reader-thread_ ^PubSubListener inner)))))))

(defn- listener-supervisor-thread? [listener]
  (boolean
    (and (instance? SupervisedPubSubListener listener)
         (identical? (Thread/currentThread)
           @(.-supervisor-thread_ ^SupervisedPubSubListener listener)))))

(defn ^:public pubsub-await-synced!
  "Waits until the listener's current desired subscriptions are acknowledged
  by Redis on a live connection. Returns `:synced`, or `:pending` if
  `:timeout-ms` expires first. This is a point-in-time barrier: a concurrent
  change may establish a later readiness point. On the listener reader thread,
  returns `:pending` rather than blocking. Throws if the listener is or becomes
  closed.

  `opts` accepts only `:timeout-ms`, which defaults to the listener's
  `:default-timeout-ms`; nil waits indefinitely. This is the subscription
  readiness barrier. [[pubsub-ping!]] checks transport liveness only."
  ([listener] (pubsub-await-synced! listener nil))
  ([listener opts]
   (truss/have pubsub-listener? listener)
   (let [opts       (or opts {})
         _          (truss/have map? opts)
         _          (truss/have [:ks<= #{:timeout-ms}] opts)
         timeout-ms (get opts :timeout-ms (listener-default-timeout-ms listener))
         _          (truss/have [:or nil? nat-int?] timeout-ms)
         deadline   (when (some? timeout-ms)
                      (utils/timeout-deadline-nanos timeout-ms))]
     (if (or (listener-reader-thread? listener)
             (listener-supervisor-thread? listener))
       (cond
         (listener-synced? listener) :synced
         (not (listener-open? listener))
         (throw (listener-closed-error listener (listener-close-cause listener)))
         :else :pending)
       (loop []
         (let [remaining-ms (when deadline (utils/remaining-timeout-ms deadline))]
           (cond
             (listener-synced? listener) :synced
             (not (listener-open? listener))
             (throw (listener-closed-error listener (listener-close-cause listener)))
             (and deadline (nil? remaining-ms)) :pending
             :else
             (let [sleep-ms (if deadline (min 10 (long remaining-ms)) 10)]
               (try
                 (Thread/sleep sleep-ms)
                 (catch InterruptedException t
                   (.interrupt (Thread/currentThread))
                   (throw t)))
               (recur)))))))))

(defn- new-daemon-thread [name f]
  (let [bound-f (bound-fn [] (f))]
    (doto (Thread. ^Runnable bound-f ^String name)
      (.setDaemon true))))

(defn- start-daemon! [name f]
  (let [^Thread thread (new-daemon-thread name f)]
    (.start thread)
    thread))

(defn- new-basic-listener
  "Creates one connection-bound Pub/Sub listener epoch.

  Options:
    - `:conn-opts`: Standard v4 connection options (standalone or Sentinel).
    - `:handler-fn`: Required unary function that receives normalized event maps.
    - `:listener-fn`: Optional unary function that receives listener events.
    - `:init-subs`: Optional `{:channels [...], :patterns [...]}`.
    - `:ping-ms`: Optional keepalive interval. A failed ping closes the
      listener.
    - `:default-timeout-ms`: Acknowledgement timeout (default 5000).

  Redis Cluster and sharded Pub/Sub are not supported."
  ([listener-opts]
   (new-basic-listener listener-opts nil))
  ([{:keys [conn-opts handler-fn listener-fn init-subs ping-ms
            default-timeout-ms public-listener epoch terminal-signal-fn]
     :or {default-timeout-ms 5000, epoch 0}
     :as listener-opts}
    shared-event-counters]
   (truss/have map? listener-opts)
   (truss/have fn? handler-fn)
   (truss/have [:or nil? fn?] listener-fn)
   (truss/have [:or nil? fn?] terminal-signal-fn)
   (truss/have nat-int? epoch)
   (truss/have [:ks<= #{:conn-opts :handler-fn :listener-fn :init-subs
                        :ping-ms :default-timeout-ms
                        :public-listener :epoch :terminal-signal-fn}]
     listener-opts)
   (truss/have [:or nil? nat-int?] default-timeout-ms)
   (truss/have [:or nil? pos-int?] ping-ms)
   (when init-subs
     (truss/have map? init-subs)
     (truss/have [:ks<= #{:channels :patterns :timeout-ms}] init-subs))
   (let [conn-opts (opts/parse-conn-opts :redis conn-opts)]
     (when (opts/get-cluster-server conn-opts)
       (truss/ex-info! "[Carmine] High-level Pub/Sub does not support Redis Cluster"
         {:eid :carmine.pubsub/cluster-not-supported}))
     (let [conn (conns/new-conn conn-opts)
           {:keys [socket in out]} @conn
           _ (.setSoTimeout ^Socket socket 0)
           [n-events n-messages n-handler-errors]
           (or shared-event-counters
             [(AtomicLong. 0) (AtomicLong. 0) (AtomicLong. 0)])
           listener
           (PubSubListener. conn in out handler-fn listener-fn public-listener
             epoch terminal-signal-fn default-timeout-ms
             (System/currentTimeMillis)
             (enc/latom true) (atom nil)
             (atom {:channels #{}, :patterns #{}})
             (atom []) (Object.) (atom nil) (atom nil)
             n-events n-messages n-handler-errors)
           read-opts (ReadOpts. nil nil core/*auto-thaw?*
                       core/*issue-83-workaround?* false
                       core/*raw-verbatim-strings?* nil)
           reader-thread
           (new-daemon-thread "carmine-v4-pubsub-reader"
             #(reader-loop! listener read-opts))]
       (reset! (.-reader-thread_ listener) reader-thread)
       (try
         (.start ^Thread reader-thread)
         (when (or (seq (:channels init-subs)) (seq (:patterns init-subs)))
           (pubsub-subscribe! listener init-subs))
         (when ping-ms
           (reset! (.-ping-thread_ listener)
             (start-daemon! "carmine-v4-pubsub-ping"
               (fn []
                 (try
                   (while @(.-open?_ listener)
                     (Thread/sleep (long ping-ms))
                     (when @(.-open?_ listener)
                       (pubsub-ping! listener)))
                   (catch InterruptedException _
                     (.interrupt (Thread/currentThread)))
                   (catch Throwable t
                     (when @(.-open?_ listener)
                       (close-listener! listener
                         {:reason :conn-error, :via 'ping-loop!, :cause t}))))))))
         listener
         (catch Throwable t
           (close-listener! listener
             {:reason :startup-error, :via 'pubsub-listener, :cause t})
           (throw t)))))))

(defn- remove-inner! [^SupervisedPubSubListener listener]
  (locking (.-lifecycle-lock listener)
    (first (swap-vals! (.-inner_ listener) (constantly nil)))))

(defn- inner-terminal-signal-fn [^SupervisedPubSubListener listener]
  (fn [{:keys [inner] :as data}]
    ;; Recovery coordination is private and happens before any synchronous
    ;; public listener notification can block the failing reader.
    (.offer ^LinkedBlockingQueue (.-wake-queue listener)
      {:inner inner, :data data})))

(defn- new-supervised-inner
  [^SupervisedPubSubListener listener init-subs inner-epoch]
  (new-basic-listener
    {:conn-opts (.-conn-opts listener)
     :handler-fn (.-handler-fn listener)
     :listener-fn (.-listener-fn listener)
     :public-listener listener
     :epoch inner-epoch
     :terminal-signal-fn (inner-terminal-signal-fn listener)
     :init-subs init-subs
     :ping-ms (.-ping-ms listener)
     :default-timeout-ms (.-default-timeout-ms listener)}
    [(.-n-events listener) (.-n-messages listener)
     (.-n-handler-errors listener)]))

(defn- recovery-error! [^SupervisedPubSubListener listener epoch attempt cause]
  (.incrementAndGet ^AtomicLong (.-n-recovery-errors listener))
  (let [event
        (enc/assoc-some
          (supervised-event listener :recovery-error epoch)
          :attempt attempt
          :cause cause)]
    (reset! (.-last-error_ listener) event)
    (when @(.-open?_ listener)
      (notify-listener! (.-listener-fn listener) event))))

(defn- recovery-backoff! [^SupervisedPubSubListener listener attempt]
  (utils/backoff! (get (.-recovery-opts listener) :backoff-ms) attempt))

(def ^:private transient-pubsub-reply-codes
  #{"LOADING" "MASTERDOWN" "TRYAGAIN" "CLUSTERDOWN"})

(defn- transient-pubsub-reply-error? [t]
  (and (com/reply-error? {:eid :carmine.read/redis-error-reply} t)
       (contains? transient-pubsub-reply-codes (:code (ex-data t)))))

(defn- definitive-pubsub-reply-error? [t]
  (and (com/reply-error? {:eid :carmine.read/redis-error-reply} t)
       (not (transient-pubsub-reply-error? t))))

(defn- close-definitive-sync-error!
  [^SupervisedPubSubListener listener cause]
  (let [epoch (.get ^AtomicLong (.-epoch listener))
        event (assoc (supervised-event listener :sync-error epoch) :cause cause)]
    ;; A definitive Redis rejection cannot be rolled back safely when later
    ;; concurrent operations touched the same names. Fail closed instead of
    ;; exposing a healthy-looking desired/actual split.
    (reset! (.-last-error_ listener) event)
    (close-supervised! listener
      {:reason :sync-error
       :via 'pubsub-subscription-sync
       :cause cause})))

(defn- enqueue-reconcile-subs!
  "Queues the `[snapshot -> current-desired]` difference on `inner` and returns
  its expectations. The caller must hold the supervised listener's `ops-lock`.
  Supervised operations apply to `desired_` and enqueue to the published inner
  listener in one serialized order, so this difference's commands cannot land
  after a newer operation's commands. Duplicate SUBSCRIBE commands are safe
  because Redis acknowledges them again."
  [inner before after]
  (let [add-channels    (set/difference (:channels after) (:channels before))
        remove-channels (set/difference (:channels before) (:channels after))
        add-patterns    (set/difference (:patterns after) (:patterns before))
        remove-patterns (set/difference (:patterns before) (:patterns after))]
    (into
      (enqueue-subscribe! inner (vec add-channels) (vec add-patterns))
      (when (or (seq remove-channels) (seq remove-patterns))
        (enqueue-unsubscribe! inner true
          (vec remove-channels) (vec remove-patterns))))))

(defn- recover-inner!
  [^SupervisedPubSubListener listener trigger]
  (let [old-inner (remove-inner! listener)
        old-addr  (when old-inner (conns/conn-addr (.-conn ^PubSubListener old-inner)))
        cause     (or (get-in trigger [:data :cause]) (:cause trigger))]
    (when old-inner
      (close-listener! old-inner
        {:reason :recovered, :via 'recover-inner!, :cause cause}))
    (loop [attempt 1]
      (when @(.-open?_ listener)
        (let [snapshot @(.-desired_ listener)
              next-epoch (.incrementAndGet ^AtomicLong (.-epoch-seq listener))
              success?
              (try
                (let [inner (new-supervised-inner listener snapshot next-epoch)
                      ;; Inner publication and reconciliation enqueue are one
                      ;; atomic step w.r.t. supervised ops (ops-lock), with
                      ;; acknowledgements awaited only after release. Lock
                      ;; order: ops-lock -> lifecycle-lock.
                      reconcile-expectations
                      (locking (.-ops-lock listener)
                        (if-not
                          (locking (.-lifecycle-lock listener)
                            (when @(.-open?_ listener)
                              (reset! (.-inner_ listener) inner)
                              (.set ^AtomicLong (.-epoch listener) next-epoch)
                              true))
                          ::not-published
                          (enqueue-reconcile-subs! inner snapshot
                            @(.-desired_ listener))))]
                  (if (identical? reconcile-expectations ::not-published)
                    (close-listener! inner
                      {:reason :requested, :via 'recover-inner!})
                    (do
                      (await-operations! inner reconcile-expectations
                        (.-default-timeout-ms ^PubSubListener inner))
                      (if-let [event
                               (locking (.-lifecycle-lock listener)
                                 (when (and @(.-open?_ listener)
                                            @(.-open?_ ^PubSubListener inner)
                                            (identical? inner @(.-inner_ listener)))
                                   (.incrementAndGet ^AtomicLong (.-n-recoveries listener))
                                   (let [event
                                         (assoc
                                           (supervised-event listener :recovered next-epoch)
                                           :old-addr old-addr
                                           :new-addr (conns/conn-addr
                                                       (.-conn ^PubSubListener inner)))]
                                     (reset! (.-last-recovery_ listener) event)
                                     (reset! (.-last-error_ listener) nil)
                                     event)))]
                        (do
                          (notify-listener! (.-listener-fn listener) event)
                          true)
                        (throw
                          (listener-closed-error inner
                            (:cause @(.-close-data_ ^PubSubListener inner))))))))
                (catch InterruptedException t
                  (.interrupt (Thread/currentThread))
                  (when @(.-open?_ listener) (throw t))
                  true)
                (catch Throwable t
                  (when-let [failed (remove-inner! listener)]
                    (close-listener! failed
                      {:reason :recovery-error, :via 'recover-inner!, :cause t}))
                  (if (and @(.-open?_ listener)
                           (definitive-pubsub-reply-error? t))
                    (do (close-definitive-sync-error! listener t) true)
                    (do
                      (when @(.-open?_ listener)
                        (recovery-error! listener next-epoch attempt t))
                      false))))]
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
                     :via 'recovery-backoff!
                     :cause backoff-error}))
                (recur (inc attempt))))))))))

(defn- sentinel-inner-current?
  [^SupervisedPubSubListener listener inner]
  (if-let [{:keys [master-name sentinel-spec sentinel-opts]}
           (opts/get-sentinel-server (.-conn-opts listener))]
    (let [{:keys [host port]} (:conn @inner)]
      (boolean
        (sentinel/resolved-addr?
          sentinel-spec master-name sentinel-opts false [host port])))
    true))

(defn- supervisor-loop! [^SupervisedPubSubListener listener]
  (let [check-ms (long (get (.-recovery-opts listener) :check-ms))]
    (try
      (while @(.-open?_ listener)
        (let [trigger
              (.poll ^LinkedBlockingQueue (.-wake-queue listener)
                check-ms TimeUnit/MILLISECONDS)
              inner @(.-inner_ listener)]
          (when @(.-open?_ listener)
            (try
              (when (or (nil? inner)
                        (not (:open? @inner))
                        (not (sentinel-inner-current? listener inner)))
                (recover-inner! listener trigger))
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
            {:reason :supervisor-error, :via 'supervisor-loop!, :cause t})))
      (catch Throwable t
        (when @(.-open?_ listener)
          (recovery-error! listener
            (.get ^AtomicLong (.-epoch listener)) nil t)
          (close-supervised! listener
            {:reason :supervisor-error, :via 'supervisor-loop!, :cause t}))))))

(defn- close-supervised!
  ([^SupervisedPubSubListener listener]
   (close-supervised! listener {:reason :requested, :via 'pubsub-close!}))
  ([^SupervisedPubSubListener listener data]
   (let [{:keys [closed? inner]}
         (locking (.-ops-lock listener)
           (locking (.-lifecycle-lock listener)
             (when (compare-and-set! (.-open?_ listener) true false)
               (reset! (.-close-data_ listener) (select-keys data [:reason :cause]))
               (.offer ^LinkedBlockingQueue (.-wake-queue listener) {:close? true})
               (when-let [^Thread thread @(.-supervisor-thread_ listener)]
                 (when-not (identical? thread (Thread/currentThread))
                   (.interrupt thread)))
               {:closed? true
                :inner (first (swap-vals! (.-inner_ listener) (constantly nil)))})))]
     (when inner
       (close-listener! inner data))
     (when closed?
       ;; The public function runs synchronously but outside all lifecycle and
       ;; operation locks. It may re-enter `pubsub-close!` safely.
       (notify-listener! (.-listener-fn listener)
         (cond->
           (assoc
             (supervised-event listener :closed
               (.get ^AtomicLong (.-epoch listener)))
             :reason (:reason data))
           (:cause data) (assoc :cause (:cause data))))
       true))))

(defn- recovery-window?
  "Returns true iff `t` came from a stale (replaced or closed) inner listener
  while the supervised listener remains open. This occurs during connection
  replacement. Acknowledgement timeouts close the suspect inner and therefore
  also enter this state; recorded subscription intent is reconciled later."
  [^SupervisedPubSubListener listener inner _t]
  (and @(.-open?_ listener)
       (or (not (identical? inner @(.-inner_ listener)))
           (not (:open? @inner)))))

(defn- pending-or-throw [^SupervisedPubSubListener listener inner t]
  (cond
    (definitive-pubsub-reply-error? t)
    (do
      (close-definitive-sync-error! listener t)
      (throw t))

    (transient-pubsub-reply-error? t)
    (do
      (close-listener! inner
        {:reason :conn-error
         :via 'supervised-operation-result
         :cause t})
      :pending)

    (recovery-window? listener inner t) :pending

    :else (throw t)))

(defn- supervised-operation-result
  "Awaits enqueued expectations outside the ops-lock."
  [^SupervisedPubSubListener listener inner expectations timeout-ms]
  (cond
    (nil? inner) :pending
    (instance? Throwable expectations)
    (do
      ;; Enqueueing happened under the outer operation lock, but direct public
      ;; notification must not. Close and classify only after that lock unwinds.
      (close-listener! inner
        {:reason :conn-error
         :via 'supervised-operation-result
         :cause expectations})
      (pending-or-throw listener inner expectations))
    :else
    (try
      (await-operations! inner expectations timeout-ms)
      (catch Throwable t (pending-or-throw listener inner t)))))

(defn- supervised-subscribe!
  [^SupervisedPubSubListener listener
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
  [^SupervisedPubSubListener listener
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
            (swap! (.-desired_ listener)
              (fn [subs]
                (if specific?
                  (-> subs
                    (update :channels #(reduce disj % channels))
                    (update :patterns #(reduce disj % patterns)))
                  {:channels #{}, :patterns #{}})))
            (if-let [inner @(.-inner_ listener)]
              [inner (try (enqueue-unsubscribe! inner specific? channels patterns)
                          (catch Throwable t t))]
              [nil nil]))]
      (supervised-operation-result listener inner expectations timeout-ms))))

(defn- supervised-ping! [^SupervisedPubSubListener listener opts]
  (when-not @(.-open?_ listener)
    (throw (listener-closed-error listener nil)))
  (if-let [inner @(.-inner_ listener)]
    (try
      (pubsub-ping! inner opts)
      (catch Throwable t
        ;; Unlike subscribe/unsubscribe there is no recorded intent to apply
        ;; later, so any stale-inner failure is `:recovering`, not `:pending`.
        ;; Errors from a still-current transport rethrow unchanged.
        (if (recovery-window? listener inner t)
          :recovering
          (throw t))))
    :recovering))

(defn- default-recovery-backoff-ms [attempt]
  (let [shift (min 5 (dec (long attempt)))
        base  (min 8000 (* 250 (bit-shift-left 1 shift)))
        jitter (.nextLong (ThreadLocalRandom/current) (inc (quot base 4)))]
    (+ base jitter)))

(defn ^:public pubsub-listener
  "Creates and starts a v4 Pub/Sub listener with a dedicated connection.

  Options:
    - `:conn-opts`: Standard v4 connection options (standalone or Sentinel).
    - `:handler-fn`: Required unary function that receives normalized event maps.
    - `:listener-fn`: Optional unary function that receives listener event maps.
    - `:init-subs`: Optional `{:channels [...], :patterns [...]}`.
    - `:ping-ms`: Optional keepalive interval.
    - `:default-timeout-ms`: Acknowledgement timeout (default 5000).
    - `:recovery`: Optional `{:check-ms n, :backoff-ms value}`.
      `:check-ms` defaults to 5000. `value` may be nil, a non-negative integer,
      or a function. Nil or zero, including as a function result, adds no delay
      before the attempt. `:backoff-ms` defaults to exponential backoff with
      jitter.

  Creation throws immediately if the first connection fails, including with
  `:recovery`; supervision starts only after a successful connection, so
  configuration errors do not become background retries.

  Recovery replaces failed standalone connections and verifies Sentinel
  addresses. Subscribe and unsubscribe return `:acknowledged` after Redis
  confirms the change, or `:pending` if they record intent during replacement
  or the current connection times out. Pending intent is applied during
  recovery; [[pubsub-await-synced!]] is the readiness barrier. Ping checks only
  transport liveness. Transient Redis states such as `LOADING` trigger another
  recovery; a definitive Redis command rejection throws and closes the
  supervised listener rather than leaving desired and actual state split.
  The listener serializes subscription operations so desired state and wire
  order agree, but waits for acknowledgements outside this serialization.

  Pub/Sub delivery remains at-most-once, so messages during connection
  replacement are lost. Carmine calls both functions directly and
  synchronously, outside internal locks. Calls to `:handler-fn` are serialized
  within one connection epoch; `:listener-fn` calls may be concurrent or
  reentrant. Both functions may run before this constructor returns. Every
  event includes `:listener` and `:epoch`.

  Redis event maps have these fields (kinds and maps are open to future values):
    - `:message`: `:channel`, `:payload`.
    - `:pmessage`: `:pattern`, `:channel`, `:payload`.
    - `:subscribe` and `:unsubscribe`: `:channel`, `:count`.
    - `:psubscribe` and `:punsubscribe`: `:pattern`, `:count`.
    - `:pong`: `:payload` (possibly nil).

  Listener events have these fields (all event maps are open to future keys and
  kinds):
    - `:handler-error`: `:redis-event`, `:cause`.
    - `:conn-error`: `:cause`, `:recovering?`.
    - `:recovery-error`: `:cause`, optional `:attempt`. With `:attempt`, `:epoch`
      identifies the reserved replacement epoch; without it, the current epoch.
    - `:recovered`: `:old-addr` (possibly nil), `:new-addr`; addresses are
      `[host port]` vectors.
    - `:closed`: `:reason`, optional `:cause`. Reasons are machine-readable and
      open; current values include `:requested`, `:conn-error`, `:startup-error`,
      `:sync-error`, `:recovery-error`, and `:supervisor-error`.

  The function supplier owns all downstream execution, ordering, buffering,
  and backpressure. Blocking either function delays its originating reader,
  supervisor, or caller thread. Direct `:handler-fn` failures produce a
  `:handler-error` listener event and reading continues. Failures from async
  work started by either function belong to that async facility. Listener
  function failures are logged through Trove and never recursively reported.
  With no `:listener-fn`, Carmine logs allowlisted abnormal-event metadata
  without adding Redis message payloads or connection options. The original
  Throwable is passed unchanged and may contain application data. Carmine
  assumes the configured Trove backend handles its own failures. Requested
  close is silent.

  Redis Cluster and sharded Pub/Sub are not supported."
  [{:keys [conn-opts handler-fn listener-fn init-subs ping-ms
           default-timeout-ms recovery]
    :or {default-timeout-ms 5000}
    :as listener-opts}]
  (truss/have map? listener-opts)
  (truss/have fn? handler-fn)
  (truss/have [:or nil? fn?] listener-fn)
  (truss/have
    [:ks<= #{:conn-opts :handler-fn :listener-fn :init-subs :ping-ms
             :default-timeout-ms :recovery}]
    listener-opts)
  (truss/have [:or nil? nat-int?] default-timeout-ms)
  (truss/have [:or nil? pos-int?] ping-ms)
  (let [init-subs
        (when init-subs
          (normalize-sub-opts :init init-subs default-timeout-ms))
        listener-opts (assoc listener-opts :init-subs init-subs)
        conn-opts (opts/parse-conn-opts :redis conn-opts)]
    (when (opts/get-cluster-server conn-opts)
      (truss/ex-info! "[Carmine] High-level Pub/Sub does not support Redis Cluster"
        {:eid :carmine.pubsub/cluster-not-supported}))
    (if-not recovery
      (new-basic-listener
        (assoc (dissoc listener-opts :recovery) :conn-opts conn-opts))
      (let [_ (truss/have map? recovery)
            _ (truss/have [:ks<= #{:check-ms :backoff-ms}] recovery)
            recovery
            (merge {:check-ms 5000, :backoff-ms default-recovery-backoff-ms}
              recovery)
            _ (truss/have pos-int? (:check-ms recovery))
            _ (truss/have [:or nil? nat-int? fn?] (:backoff-ms recovery))
            sentinel-server (opts/get-sentinel-server conn-opts)
            _
            (when (get-in sentinel-server [:sentinel-opts :prefer-read-replica?])
              (truss/ex-info! "[Carmine] Pub/Sub recovery requires a Sentinel master"
                {:eid :carmine.pubsub/replica-recovery-not-supported}))
            ;; Recovery targets the verified master deterministically.
            conn-opts
            (if sentinel-server
              (assoc-in conn-opts
                [:server :sentinel-opts :prefer-read-replica?] false)
              conn-opts)
            desired
            {:channels (set (names (:channels init-subs)))
             :patterns (set (names (:patterns init-subs)))}
            listener
            (SupervisedPubSubListener.
              conn-opts handler-fn listener-fn default-timeout-ms ping-ms
              recovery (System/currentTimeMillis)
              (atom desired) (atom nil) (enc/latom true) (atom nil)
              (Object.) (Object.) (LinkedBlockingQueue.) (atom nil)
              (atom nil) (atom nil)
              (AtomicLong. 0) (AtomicLong. 0) (AtomicLong. 0)
              (AtomicLong. 0) (AtomicLong. 0)
              (AtomicLong. 0) (AtomicLong. 0))]
        (try
          (let [snapshot desired
                inner
                (new-supervised-inner listener
                  (cond-> snapshot
                    (contains? init-subs :timeout-ms)
                    (assoc :timeout-ms (:timeout-ms init-subs)))
                  0)
                reconcile-expectations
                (locking (.-ops-lock listener)
                  (if-not
                    (locking (.-lifecycle-lock listener)
                      (when @(.-open?_ listener)
                        (reset! (.-inner_ listener) inner)
                        true))
                    ::not-published
                    (enqueue-reconcile-subs! inner snapshot @(.-desired_ listener))))]
            (if (identical? reconcile-expectations ::not-published)
              (close-listener! inner {:reason :requested, :via 'pubsub-listener})
              (let [supervisor-thread
                    (new-daemon-thread "carmine-v4-pubsub-supervisor"
                      #(supervisor-loop! listener))]
                (reset! (.-supervisor-thread_ listener) supervisor-thread)
                (.start ^Thread supervisor-thread)
                (await-operations! inner reconcile-expectations default-timeout-ms))))
          listener
          (catch Throwable t
            (close-supervised! listener
              {:reason :startup-error, :via 'pubsub-listener, :cause t})
            (throw t)))))))
