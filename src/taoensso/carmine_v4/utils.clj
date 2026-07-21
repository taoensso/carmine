(ns ^:no-doc taoensso.carmine-v4.utils
  "Private utility implementation."
  (:require
   [clojure.string  :as str]
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.trove  :as trove])
  (:import
   [java.util.concurrent Callable ExecutionException FutureTask TimeUnit TimeoutException]
   [java.util.concurrent.atomic AtomicLong]))

(comment (remove-ns 'taoensso.carmine-v4.utils))

(let [thread-id* (AtomicLong. 0)]
  (defn call-with-timeout-ms
    "Calls `f` on a dedicated daemon thread. Returns before `timeout-ms`.
    Exceptions from `f` are rethrown on the caller thread. Internal use only."
    [timeout-ms f]
    (let [task (FutureTask. ^Callable (bound-fn [] (f)))
          thread
          (doto (Thread. task
                  (str "carmine-discovery-" (.incrementAndGet thread-id*)))
            (.setDaemon true)
            (.start))]
      (try
        {:timed-out? false
         :value (.get task (long timeout-ms) TimeUnit/MILLISECONDS)}
        (catch TimeoutException _
          (.cancel task true)
          {:timed-out? true})
        (catch ExecutionException e
          (throw (.getCause e)))
        (catch InterruptedException e
          (.cancel task true)
          (.interrupt (Thread/currentThread))
          (throw e))))))

;;;; Aggregate deadlines (Sentinel resolution, Cluster discovery, etc.)

(defn timeout-deadline-nanos
  "Returns the `System/nanoTime` deadline `timeout-ms` milliseconds from now."
  [timeout-ms]
  (+' (System/nanoTime) (*' (long timeout-ms) 1000000)))

(defn remaining-timeout-ms
  "Returns the milliseconds remaining until `deadline-nanos`, always at least
  1, or nil after the deadline."
  [deadline-nanos]
  (let [remaining-nanos (-' deadline-nanos (System/nanoTime))]
    (when (pos? remaining-nanos)
      (long
        (min Integer/MAX_VALUE
          (max 1 (quot (+' remaining-nanos 999999) 1000000)))))))

(defn conn-opts-before-deadline
  "Returns `conn-opts` with the `:socket-opts` keys `:connect-timeout-ms`,
  `:read-timeout-ms`, `:ready-timeout-ms`, and `:init-timeout-ms` set to the
  smaller of their positive configured value and the remaining deadline. An
  unset or non-positive value uses the remaining deadline. Returns nil after
  the deadline."
  [conn-opts deadline-nanos]
  (when-let [remaining-ms (remaining-timeout-ms deadline-nanos)]
    (update conn-opts :socket-opts
      (fn [socket-opts]
        (reduce
          (fn [socket-opts k]
            (let [configured-ms (get socket-opts k)]
              (assoc socket-opts k
                (if (and (number? configured-ms) (pos? configured-ms))
                  (min (long configured-ms) remaining-ms)
                  remaining-ms))))
          (or socket-opts {})
          [:connect-timeout-ms :read-timeout-ms :ready-timeout-ms
           :init-timeout-ms])))))

(def deadline-exhausted
  "Unique value from [[call-before-deadline]] when its deadline passed before
  or during `f`."
  (Object.))

(defn call-before-deadline
  "Calls `(f)` within the remaining deadline. Returns the result, or
  [[deadline-exhausted]] if the deadline passed before or during `f`."
  [deadline-nanos f]
  (if-let [remaining-ms (remaining-timeout-ms deadline-nanos)]
    (let [{:keys [timed-out? value]}
          (call-with-timeout-ms remaining-ms f)]
      (if timed-out? deadline-exhausted value))
    deadline-exhausted))

(defn backoff!
  "Sleeps for the given backoff time before a retry attempt. Returns nil.
  `backoff-ms` may be nil, non-negative milliseconds, or a function of
  `attempt` that returns either. Nil or zero adds no delay. Restores the thread
  interrupt status before it rethrows an `InterruptedException`."
  [backoff-ms attempt]
  (let [ms
        (if (fn? backoff-ms)
          (try
            (backoff-ms attempt)
            (catch InterruptedException t
              (.interrupt (Thread/currentThread))
              (throw t)))
          backoff-ms)]
    (when (some? ms)
      (when-not (nat-int? ms)
        (truss/ex-info! "[Carmine] Invalid retry backoff value"
          {:eid :carmine/invalid-backoff-ms
           :backoff-ms (enc/typed-val ms)
           :attempt attempt
           :expected :nil-or-non-negative-msecs}))
      (when (pos? ms)
        (try
          (Thread/sleep (long ms))
          (catch InterruptedException t
            (.interrupt (Thread/currentThread))
            (throw t)))))))

(defn get-at
  "Optimized `get-in`."
  ([m k1      ] (when m               (get m k1)))
  ([m k1 k2   ] (when m (when-let [m2 (get m k1)]               (get m2 k2))))
  ([m k1 k2 k3] (when m (when-let [m2 (get m k1)] (when-let [m3 (get m2 k2)] (get m3 k3))))))

;;;;

(def ^:private redacted-secret :carmine/redacted)

(defn redact-secrets
  "Redacts credentials from nested diagnostic configuration values."
  [x]
  (letfn [(redact [x]
            (cond
              (map? x)
              (reduce-kv
                (fn [m k v]
                  (assoc m k
                    (cond
                      (= k :password) redacted-secret
                      (= k :commands) redacted-secret
                      (= k :auth)     (if (map? v) (redact v) redacted-secret)
                      :else           (redact v))))
                (if (record? x) x (empty x)) x)

              (vector? x) (mapv redact x)
              (set?    x) (into (empty x) (map redact) x)
              (seq?    x) (doall (map redact x))
              (string? x)
              (str/replace x
                #"(?i)^(rediss?://)[^/@]*@"
                "$1<carmine-redacted>@")
              :else       x))]
    (redact x)))

(defn redact-command
  "Retains a command name while redacting all arguments."
  [command]
  (if-let [command-name (first command)]
    (cond-> [command-name]
      (next command) (conj redacted-secret))
    command))

;;;;

(defn ^:no-doc report-callback-error! [cb cbid t]
  (trove/log!
    {:level :error
     :id    :carmine.callback/error
     :data  {:callback cb, :cbid cbid}
     :error t}))

(defn- callback-error! [cb data_ t]
  (truss/catching :all
    (report-callback-error! cb
      (truss/catching :all (:cbid (force data_)))
      t))
  (when (instance? InterruptedException t)
    (.interrupt (Thread/currentThread))))

(defn- call-callback! [cb data_]
  (when cb
    (try
      (cb (force data_))
      (catch Throwable t
        (callback-error! cb data_ t)))))

(defn cb-notify!
  "Calls each callback independently with `@data_`. Callback failures are
  logged, never escape, and do not prevent later calls."
  ([cb      data_] (call-callback! cb data_))
  ([cb1 cb2 data_]
   (call-callback! cb1 data_)
   (call-callback! cb2 data_))

  ([cb1 cb2 cb3 data_]
   (call-callback! cb1 data_)
   (call-callback! cb2 data_)
   (call-callback! cb3 data_)))

(let [get-data_
      (fn [error cbid]
        (let [data (assoc (ex-data error) :cbid cbid)
              data
              (if-let [cause (or (get data :cause) (ex-cause error))]
                (assoc data :cause cause)
                (do    data))]
          (delay data)))]

  (defn cb-notify-and-throw!
    "Notifies callbacks with error data, then throws error."
    ([cbid cb          error] (cb-notify! cb          (get-data_ error cbid)) (throw error))
    ([cbid cb1 cb2     error] (cb-notify! cb1 cb2     (get-data_ error cbid)) (throw error))
    ([cbid cb1 cb2 cb3 error] (cb-notify! cb1 cb2 cb3 (get-data_ error cbid)) (throw error))))

(comment
  (cb-notify-and-throw! :cbid1 println
    (truss/ex-info "Error msg" {:x :X} (Exception. "Cause"))))
