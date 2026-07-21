(ns ^:no-doc taoensso.carmine-v4.conns
  "Private connection implementation."
  (:require
   [clojure.string  :as str]
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.carmine-v4.utils :as utils]
   [taoensso.carmine-v4.resp  :as resp]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write :as write]
   [taoensso.carmine-v4.opts  :as opts])

  (:import
   [java.net Socket]
   [java.io DataInputStream BufferedOutputStream]
   [javax.net.ssl SSLSocket]
   [org.apache.commons.pool2 PooledObjectFactory KeyedPooledObjectFactory]
   [org.apache.commons.pool2.impl
    BaseGenericObjectPool GenericObjectPool GenericKeyedObjectPool DefaultPooledObject]
   [java.util.concurrent
    ArrayBlockingQueue ExecutorService RejectedExecutionException
    ScheduledExecutorService ScheduledThreadPoolExecutor ThreadFactory
    ThreadPoolExecutor ThreadPoolExecutor$AbortPolicy TimeUnit]
   [java.util.concurrent.atomic AtomicBoolean AtomicLong]
   [taoensso.carmine_v4.classes ReusableConnError]))

(comment (remove-ns 'taoensso.carmine-v4.conns))

(enc/declare-remote
            taoensso.carmine-v4.sentinel/resolve-addr!
            taoensso.carmine-v4.sentinel/resolved-addr?
  ^:dynamic taoensso.carmine-v4.sentinel/*mgr-cbs*
  ^:dynamic taoensso.carmine-v4.sentinel/*mgr-conn-opts*
  ^:dynamic taoensso.carmine-v4/*conn-cbs*
            taoensso.carmine-v4/default-pool-opts
            taoensso.carmine-v4/default-cluster-pool-opts)

(alias 'core     'taoensso.carmine-v4)
(alias 'sentinel 'taoensso.carmine-v4.sentinel)

(defmacro ^:private debug! [& body] (when #_true false `(enc/println ~@body)))

;;;; Manager options and RESP3 push dispatch

(def ^:private default-push-queue-capacity 1024)

(defn- cause-summary
  "Returns \" (<host>:<port>): <root cause message>\" to add to an error
  message, so that the cause is visible without walking the cause chain."
  [addr ^Throwable t]
  (let [^Throwable root (last (take-while some? (iterate ex-cause t)))]
    (str
      (when (vector? addr) (str " (" (str/join ":" addr) ")"))
      ": " (or
             (some-> (.getMessage root) (str/replace-first "[Carmine] " ""))
             (.getSimpleName (class root))))))

(defn- validate-manager-opts!
  [manager-kind allowed-keys opts]
  (when-not (map? opts)
    (truss/ex-info! "[Carmine] Connection manager options must be a map"
      {:eid :carmine.conns/invalid-manager-opts
       :mgr-kind manager-kind
       :opts {:type (type opts)}})) ; No value, may be a URI with password
  (doseq [k (keys opts)]
    (when-not (and (keyword? k)
                (or (namespace k) (contains? allowed-keys k)))
      (truss/ex-info! (str "[Carmine] Unknown connection manager option" (when (keyword? k) (str ": `" k "`")))
        {:eid :carmine.conns/invalid-manager-opts
         :mgr-kind manager-kind
         :opt-key (enc/typed-val k)
         :allowed-keys allowed-keys})))
  (when-not (or (nil? (:on-push opts)) (fn? (:on-push opts)))
    (truss/ex-info! "[Carmine] Invalid connection manager push handler"
      {:eid :carmine.conns/invalid-manager-opts
       :mgr-kind manager-kind
       :opt-key :on-push
       :opt-val (enc/typed-val (:on-push opts))
       :expected :nil-or-fn}))
  (when-not (let [capacity (get opts :push-queue-capacity default-push-queue-capacity)]
              (and (pos-int? capacity) (<= capacity Integer/MAX_VALUE)))
    (truss/ex-info! "[Carmine] Invalid connection manager push queue capacity"
      {:eid :carmine.conns/invalid-manager-opts
       :mgr-kind manager-kind
       :opt-key :push-queue-capacity
       :opt-val (enc/typed-val (:push-queue-capacity opts))
       :expected :pos-int32}))
  ;; NB `:pool-opts` is deliberately NOT validated here: it's parsed inside
  ;; the constructors' resource guards (after e.g. push-dispatcher creation)
  ;; so that construction failures close any already-created resources.
  opts)

(deftype ^:private PushDispatcher
  [push-fn ^ThreadPoolExecutor executor ^long capacity
   ^AtomicLong received* ^AtomicLong completed* ^AtomicLong rejected*
   ^AtomicLong handler-errors* ^AtomicLong discarded-on-close*])

(let [thread-idx* (AtomicLong. 0)]
  (defn- new-push-dispatcher [mgr-name push-fn push-queue-capacity]
    (when push-fn
      (let [thread-factory
            (reify ThreadFactory
              (newThread [_ runnable]
                (doto
                  (Thread. ^Runnable runnable
                    (str "carmine-v4-push-"
                      (or mgr-name "manager") "-"
                      (.incrementAndGet thread-idx*)))
                  (.setDaemon true))))]
        (PushDispatcher. push-fn
          (ThreadPoolExecutor.
            1 1 0 TimeUnit/MILLISECONDS
            (ArrayBlockingQueue. (int push-queue-capacity))
            thread-factory
            (ThreadPoolExecutor$AbortPolicy.))
          push-queue-capacity
          (AtomicLong. 0) (AtomicLong. 0) (AtomicLong. 0) (AtomicLong. 0)
          (AtomicLong. 0))))))

(defn- push-dispatcher-stats [^PushDispatcher dispatcher]
  (if dispatcher
    (let [^ThreadPoolExecutor executor (.-executor dispatcher)
          ^AtomicLong received* (.-received* dispatcher)
          ^AtomicLong completed* (.-completed* dispatcher)
          ^AtomicLong rejected* (.-rejected* dispatcher)
          ^AtomicLong handler-errors* (.-handler-errors* dispatcher)
          ^AtomicLong discarded-on-close* (.-discarded-on-close* dispatcher)]
      {:enabled? true
       :queue-capacity (.-capacity dispatcher)
       :queue-depth (.size (.getQueue executor))
       :active (.getActiveCount executor)
       :shutdown? (.isShutdown executor)
       :counts
       {:received (.get received*)
        :completed (.get completed*)
        :rejected (.get rejected*)
        :handler-errors (.get handler-errors*)
        :discarded-on-close (.get discarded-on-close*)}})
    {:enabled? false, :queue-capacity nil, :queue-depth 0, :active 0
     :shutdown? false
     :counts {:received 0, :completed 0, :rejected 0, :handler-errors 0
              :discarded-on-close 0}}))

(defn- notify-push-error!
  [global-cb conn-cb eid push-event cause]
  (utils/cb-notify!
    global-cb conn-cb
    (delay
      {:cbid :on-push-error
       :eid eid
       :via 'manager-push-fn
       :push-event push-event
       :cause cause})))

(defn- dispatch-push!
  [^PushDispatcher dispatcher mgr conn-opts server-addr cluster? data]
  (let [^AtomicLong received* (.-received* dispatcher)
        ^AtomicLong completed* (.-completed* dispatcher)
        ^AtomicLong rejected* (.-rejected* dispatcher)
        ^AtomicLong handler-errors* (.-handler-errors* dispatcher)
        push-event
        {:data data
         :type (when (sequential? data) (first data))
         :source
         {:mgr mgr
          :addr (vec server-addr)
          :cluster? (boolean cluster?)}}
        global-cb (get core/*conn-cbs* :on-push-error)
        conn-cb   (utils/get-at conn-opts :cbs :on-push-error)
        task
        (reify Runnable
          (run [_]
            (try
              ((.-push-fn dispatcher) push-event)
              (catch Throwable t
                (.incrementAndGet handler-errors*)
                (notify-push-error! global-cb conn-cb
                  :carmine.push/handler-error push-event t))
              (finally
                (.incrementAndGet completed*)))))]
    (.incrementAndGet received*)
    (try
      (.execute ^ExecutorService (.-executor dispatcher) task)
      (catch RejectedExecutionException t
        (.incrementAndGet rejected*)
        ;; A close racing with reply consumption may reject a final event.
        ;; Keep that failure isolated from RESP framing and observable.
        (notify-push-error! global-cb conn-cb
          :carmine.push/dispatch-rejected push-event t)))
    nil))

(defn- close-push-dispatcher! [^PushDispatcher dispatcher]
  (when dispatcher
    (let [^AtomicLong discarded-on-close* (.-discarded-on-close* dispatcher)
          discarded (count (.shutdownNow ^ExecutorService (.-executor dispatcher)))]
      (.addAndGet discarded-on-close* discarded))
    true))

(defn- close-construction-resources!
  "Closes manager resources after a constructor failure. Preserves the original
  failure and adds cleanup failures as suppressed causes."
  [^BaseGenericObjectPool pool push-dispatcher ^Throwable cause]
  (doseq [close-fn
          [(when pool #(.close pool))
           (when push-dispatcher #(close-push-dispatcher! push-dispatcher))]
          :when close-fn]
    (try
      (close-fn)
      (catch Throwable cleanup-cause
        (when-not (identical? cause cleanup-cause)
          (.addSuppressed cause cleanup-cause)))))
  (throw cause))

;;;; Connections

(defprotocol ^:private IConn
  "Internal protocol, not for public use or extension."

  (conn-open?     [conn] "Returns true iff `Conn` is open.")
  (conn-resolved? [conn use-cache?]
   "Returns true iff `Conn` does not use Sentinel, or its address agrees with
   the current, possibly cached resolution.")

  (conn-ready?
    [conn]
    [conn ping?]
    [conn ping? push-fn]
   "Returns true iff `Conn` is open and healthy. A truthy `ping?` requests a
   fresh-if-stale Sentinel check and a test PING. False skips only the PING.")

  (conn-init! [conn]
   "Initializes `Conn` authentication and protocol. Returns true after success,
   nil when there is no work, or false when `Conn` is closed. Can throw.")

  (conn-close! [conn data]
   "Closes `Conn` and returns true iff this call performed the close, including
   when socket close failed. Returns nil if already closed. Adds the arbitrary
   `data` map to errors and registered callback data."))

(let [idx (java.util.concurrent.atomic.AtomicLong. 0)]
  (defn- next-client-name! [_conn-opts]
    (str "carmine:" (.incrementAndGet idx))))

(comment (next-client-name! nil))

(defn- conn-init-reqs
  [{:keys [auth client-name select-db resp3?] :as init-opts}]
  (not-empty
    (if (contains? init-opts :commands)
      (remove empty? (get init-opts :commands)) ; Complete override

      (let [{:keys [username password]} auth]
        (if resp3?
          (let [hello-req
                (cond-> ["HELLO" 3]
                  (or username password)
                  (into ["AUTH" (or username "default") (or password "")])
                  client-name (into ["SETNAME" client-name]))]

            (cond-> [hello-req]
              select-db (conj ["SELECT" select-db])))

          (cond-> []
            (or username password)
            (conj
              (if username
                ["AUTH" username (or password "")]
                ["AUTH" password]))

            client-name (conj ["CLIENT" "SETNAME" client-name])
            select-db   (conj ["SELECT" select-db])))))))

(defn- redact-init-reply
  "Redis error text can echo request args, so an error reply that includes
  the password keeps only its error code."
  [custom-commands? ^String password reply]
  (enc/cond
    custom-commands? :carmine/redacted
    :let [{:keys [message code]} (when (resp/reply-error? reply) (ex-data reply))
          ;; Prefix, since long args may be echoed truncated
          secret (when password (subs password 0 (min 8 (count password))))]
    (not (and secret message (str/includes? message secret))) reply
    (str/includes? (str code) secret) :carmine/redacted
    :else
    (com/reply-error (str "[Carmine] Redis replied with an error: " code " (details redacted)")
      {:eid :carmine.read/redis-error-reply
       :message code
       :code    code})))

(def ^:private ^:dynamic *new-conn_*
  "Bound to a volatile during a pool borrow, to hold any connection that the
  borrow creates. Such a connection's readiness checks trust the resolution
  that just chose its address: another thread's resolution may already
  disagree, e.g. about whether a replica is usable. It's always checked on
  return."
  nil)

(def ^:private ^:dynamic *deferred-conn-close-cbs*
  "Optional volatile of deferred connection-close callback tasks. Bound while a
  manager holds `clear-lock` around a pool operation that may destroy
  connections, keeping user callbacks outside the lock."
  nil)

(defn- with-deferred-close-cbs*
  "Runs `thunk` with connection-close callback deferral. After `thunk` releases
  its locks, calls the deferred tasks in FIFO order. Protects each call
  independently. Reuses an outer deferral scope and does not flush it. Avoids
  establishing a scope when this manager has no close callbacks to defer."
  [conn-opts thunk]
  (if *deferred-conn-close-cbs*
    (thunk) ; Outer scope owns flushing
    (if-not (or (get core/*conn-cbs* :on-conn-close)
                (utils/get-at conn-opts :cbs :on-conn-close))
      (thunk)
      (let [deferred_ (volatile! [])]
        (try
          (enc/binding* [*deferred-conn-close-cbs* deferred_] (thunk))
          (finally
            (run! (fn [notify!] (truss/catching (notify!))) @deferred_)))))))

(defonce ^:private socket-deadline-executor_
  ;; Shared single-thread scheduler for socket-operation watchdogs.
  ;; Tasks are cheap: schedule + cancel per bounded phase, socket close on
  ;; the rare firing. Remove-on-cancel keeps the work queue empty (cancelled
  ;; tasks would otherwise linger until their original deadlines).
  (delay
    (doto
      (ScheduledThreadPoolExecutor. 1
        (reify ThreadFactory
          (newThread [_ runnable]
            (doto (Thread. ^Runnable runnable "carmine-v4-conn-watchdog")
              (.setDaemon true)))))
      (.setRemoveOnCancelPolicy true))))

(defn- with-socket-deadline!
  "Runs `f` with an aggregate deadline for the given `phase`. Closes `socket`
  if `f` does not finish within `timeout-ms`, stopping a blocked read even when
  the peer trickles bytes. Completion and timeout settle atomically. After this
  function returns success, the watchdog cannot close the socket. A phase whose
  socket the watchdog closed always throws. Nil or zero runs without a deadline."
  [^Socket socket timeout-ms phase f]
  (if (or (nil? timeout-ms) (zero? (long timeout-ms)))
    (f)
    (let [settled* (AtomicBoolean. false)
          watchdog
          (.schedule ^ScheduledExecutorService @socket-deadline-executor_
            ^Runnable
            (fn []
              (when (.compareAndSet settled* false true)
                (truss/catching (.close socket))))
            (long timeout-ms) TimeUnit/MILLISECONDS)

          deadline-error
          (fn [cause]
            (case phase
              :init
              (truss/ex-info "[Carmine] Connection initialization deadline elapsed"
                {:eid :carmine.conns/conn-init-deadline
                 :timeout-ms timeout-ms}
                cause)

              :ready
              (truss/ex-info "[Carmine] Connection readiness deadline elapsed"
                {:eid :carmine.conns/conn-ready-deadline
                 :timeout-ms timeout-ms}
                cause)))]
      (try
        (let [result
              (try
                (f)
                (catch java.io.IOException t
                  (if (.compareAndSet settled* false true)
                    (throw t)
                    ;; Watchdog closed the socket under `f`
                    (throw (deadline-error t)))))]
          (if (.compareAndSet settled* false true)
            result
            (throw (deadline-error nil))))
        (finally (.cancel watchdog false))))))

(defn- valid-ping-reply?
  "Returns true for normal and subscribed-mode Redis PING replies."
  [reply]
  (or (= reply "PONG") (= reply ["pong" ""])))

(def ^:private ^:dynamic *new-conn-generation*
  "Bound by pool managers while creating a connection, to their generation at
  that time. A borrow registers only a conn of the current generation."
  -1)

(deftype ^:private Conn
  [^Socket socket host port addr conn-opts ^DataInputStream in ^BufferedOutputStream out open?_
   ^long generation]

  java.io.Closeable (close [this] (conn-close! this {:via 'java.io.Closeable}))
  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine.Conn"
      {:addr addr, :open? (open?_)}))

  clojure.lang.IDeref
  (deref [this]
    {:socket    socket
     :addr      addr
     :conn-opts (utils/redact-secrets conn-opts)
     :in        in
     :out       out
     :open?     (open?_)})

  IConn
  (conn-open?     [this] (open?_))
  (conn-resolved? [this use-cache?]
    (if-let [{:keys [sentinel-spec master-name sentinel-opts]}
             (when-not ; Readiness checks only, see `*new-conn_*`
                 (and
                   (identical? use-cache? :fresh-if-stale)
                   (when-let [new-conn_ *new-conn_*] (identical? this @new-conn_)))
               (opts/get-sentinel-server conn-opts))]
      (enc/binding* [sentinel/*mgr-conn-opts* conn-opts]
        (sentinel/resolved-addr? sentinel-spec master-name sentinel-opts use-cache? addr))
      true))

  (conn-ready? [this]       (conn-ready? this true nil))
  (conn-ready? [this ping?] (conn-ready? this ping? nil))
  (conn-ready? [this ping? push-fn]
    (cond
      (not (open?_)) false
      (and (not ping?) (vector? (get conn-opts :server))) true

      :else
      (let [t0     (System/currentTimeMillis)
            error_ (volatile! nil)
            pass?
            (and
              (if (conn-resolved? this :fresh-if-stale)
                true
                (do
                  (vreset! error_
                    (truss/ex-info "[Carmine] `Conn` incorrectly resolved"
                      {:eid :carmine.conns/conn-unresolved}))
                  false))

              (if-not ping?
                true
                (let [;; nil => inherit the socket's ambient read timeout
                      ;; (matching `:init-timeout-ms`)
                      ready-timeout-ms (utils/get-at conn-opts :socket-opts :ready-timeout-ms)
                      ;; All socket interaction inside try: a concurrent
                      ;; force-close must fail (not throw from) this predicate
                      current-timeout-ms
                      (try
                        (.getSoTimeout socket)
                        (catch Throwable t (vreset! error_ t) nil))]

                (if (nil? current-timeout-ms)
                  false
                  (let [restored?_ (volatile! true)
                        effective-timeout-ms
                        (if (nil? ready-timeout-ms)
                          current-timeout-ms
                          ready-timeout-ms)
                        reply
                        (try
                          (with-socket-deadline! socket effective-timeout-ms :ready
                            (fn []
                              (try
                                (when (some? ready-timeout-ms)
                                  (.setSoTimeout socket (int ready-timeout-ms)))
                                ;; Nb assume any necessary auth/init already done,
                                ;; otherwise will correctly identify conn as unready
                                (resp/basic-ping! in out push-fn)
                                (finally
                                  (when (some? ready-timeout-ms)
                                    (try
                                      (.setSoTimeout socket (int current-timeout-ms))
                                      (catch Throwable t
                                        (vreset! restored?_ false)
                                        (when (nil? @error_)
                                          (vreset! error_ t)))))))))
                          (catch Throwable t (vreset! error_ t) nil))]

                    (if (and @restored?_ (nil? @error_))
                      (let [;; Ref. <https://github.com/redis/redis/issues/420>
                            ready? (valid-ping-reply? reply)]

                        (debug! :conn-ready? ready?)
                        (if ready?
                          true
                          (do
                            (vreset! error_
                              (truss/ex-info "[Carmine] Unexpected PING reply"
                                {:eid :carmine.conns/unexpected-ping-reply
                                 :reply (enc/typed-val reply)}))
                            false)))
                      false))))))

            elapsed-ms (- (System/currentTimeMillis) t0)]

        (if pass?
          true
          (do
            (utils/cb-notify!
              (get core/*conn-cbs*         :on-conn-error)
              (utils/get-at conn-opts :cbs :on-conn-error)
              (delay
                {:cbid       :on-conn-error
                 :eid        :carmine.conns/conn-not-ready
                 :addr       addr
                 :conn       this
                 :conn-opts  (utils/redact-secrets conn-opts)
                 :via        'conn-ready?
                 :cause      @error_
                 :elapsed-ms elapsed-ms}))
            false)))))

  (conn-init! [this]
    (if-not (open?_)
      false
      (enc/when-let
        [init-opts (not-empty (get conn-opts :init))
         init-opts
         (if (contains? init-opts :commands)
           init-opts
           (assoc init-opts :client-name
             (let [v (get init-opts :client-name ::auto)]
               (enc/cond
                 (identical? v ::auto) (next-client-name! conn-opts)
                 (fn?        v)        (v                 conn-opts)
                 :else       v))))

         reqs (conn-init-reqs init-opts)]

        (let [conn-error_ (volatile! nil)
              t0 (System/currentTimeMillis)
              run-init
              (fn []
                ;; Independent of any outer request context, and mustn't mark
                ;; an outer borrowed conn as non-reusable
                (binding [resp/*ctx* nil, write/*conn-reusable?_ nil]
                  (resp/with-replies in out
                    {:natural-replies? true, :as-vec? true, :error-mode :return} nil
                    (fn [] (run! resp/rcmd* reqs)))))

              init-timeout-ms (utils/get-at conn-opts :socket-opts :init-timeout-ms)
              replies
              (try
                ;; The init exchange is bounded by `:init-timeout-ms` (nil =>
                ;; keep ambient read timeout), both per-read (so-timeout) and
                ;; in aggregate (watchdog vs trickled bytes): a frozen server
                ;; must not hang connection creation, esp. since pool
                ;; `makeObject` isn't reachable by `mgr-close!` interruption.
                ;; A failed timeout RESTORE below fails init even after
                ;; successful replies (finally throw wins).
                (if (nil? init-timeout-ms)
                  (run-init)
                  (with-socket-deadline! socket init-timeout-ms :init
                    (fn []
                      (let [current-timeout-ms (.getSoTimeout socket)]
                        (.setSoTimeout socket (int init-timeout-ms))
                        (try
                          (run-init)
                          (finally
                            (.setSoTimeout socket (int current-timeout-ms))))))))

                (catch Throwable t (vreset! conn-error_ t) nil))

              elapsed-ms (- (System/currentTimeMillis) t0)
              success?
              (and
                replies
                (not (enc/rfirst resp/reply-error? replies)))]

          (if success?
            true ; Common case
            (let [custom-commands? (contains? init-opts :commands)
                  password (not-empty (get-in init-opts [:auth :password]))
                  reqs->replies
                  (when replies
                    (enc/reduce-zip
                      (fn [acc req reply]
                        (conj acc
                          {:request (utils/redact-command req)
                           :reply   (redact-init-reply custom-commands? password reply)}))
                      [] reqs replies))]

              (utils/cb-notify-and-throw!    :on-conn-error
                (get core/*conn-cbs*         :on-conn-error)
                (utils/get-at conn-opts :cbs :on-conn-error)
                (truss/ex-info "[Carmine] Error initializing connection"
                  {:eid :carmine.conns/conn-init-error
                   :via        'conn-init!
                   :addr       addr
                   :conn       this
                   :conn-opts  (utils/redact-secrets conn-opts)
                   :replies    reqs->replies
                   :elapsed-ms elapsed-ms}
                  (or @conn-error_
                    ;; First (redacted) reply error, e.g. WRONGPASS
                    (enc/rsome
                      (fn [m] (let [r (get m :reply)] (when (resp/reply-error? r) r)))
                      reqs->replies))))))))))

  (conn-close! [this data]
    (debug! :conn-close! data)
    (when (compare-and-set! open?_ true false)
      (let [t0         (System/currentTimeMillis)
            closed?
            (truss/catching
              (do
                ;; A TLS socket's close may otherwise wait up to its read
                ;; timeout on an unresponsive peer
                (when (instance? SSLSocket socket)
                  (truss/catching (.setSoTimeout socket 1)))
                (.close socket)
                true))
            elapsed-ms (- (System/currentTimeMillis) t0)
            notify!
            (fn []
              (utils/cb-notify!
                (get core/*conn-cbs*         :on-conn-close)
                (utils/get-at conn-opts :cbs :on-conn-close)
                (delay
                  (enc/assoc-when
                    {:cbid       :on-conn-close
                     :addr       addr
                     :conn       this
                     :conn-opts  (utils/redact-secrets conn-opts)
                     :data       data
                     :elapsed-ms elapsed-ms
                     :closed?    closed?}
                    :via   (get data :via)
                    :cause (get data :cause)))))]

        (if-let [deferred_ *deferred-conn-close-cbs*]
          (vswap! deferred_ conj notify!)
          (notify!))
        true))))

(defn- conn? [x] (instance? Conn x))

(defn conn-addr
  "Returns `[host port]` for the given `conn` without building its redacted
  deref map. Used on hot borrow paths."
  [conn] (.-addr ^Conn conn))

(defn- pooled-conn-ping?
  [borrowed-count idle-time-ms ready-check-after-idle-ms]
  (or
    (nil? ready-check-after-idle-ms)
    (zero? ^long borrowed-count)
    (<= (long ready-check-after-idle-ms) ^long idle-time-ms)))

(defn- pooled-conn-ready?
  [^org.apache.commons.pool2.PooledObject po ready-check-after-idle-ms push-fn]
  (let [ping? (pooled-conn-ping?
                (.getBorrowedCount  po)
                (.getIdleTimeMillis po)
                ready-check-after-idle-ms)]
    (conn-ready? (.getObject po) ping? push-fn)))

(defn- new-ssl-socket
  "Returns a new connected, SSL-encrypted `java.net.Socket` layered over the
  given connected, unencrypted socket. Uses the default `SSLSocketFactory` and
  verifies the certificate hostname. Gets the factory for each call so that it
  honors a runtime `SSLContext/setDefault`. This cost is small compared with the
  TLS handshake."
  [^Socket socket ^String host port]
  (let [^SSLSocket ssl-socket
        (.createSocket
          ^javax.net.ssl.SSLSocketFactory (javax.net.ssl.SSLSocketFactory/getDefault)
          socket host (int port) true)
        ssl-params (.getSSLParameters ssl-socket)]
    (.setEndpointIdentificationAlgorithm ssl-params "HTTPS")
    (.setSSLParameters ssl-socket ssl-params)
    (.startHandshake ssl-socket)
    ssl-socket))

(defn- new-socket ^Socket [conn-opts socket-opts host port]
  (let [{:keys [connect-timeout-ms ssl init-timeout-ms]
         :or ; Defaults relevant only for REPL/tests
         {connect-timeout-ms 2000}} socket-opts

        socket-opts (dissoc socket-opts :connect-timeout-ms :ssl)
        ^Socket socket (Socket.)]

    (try
      (doto socket
        (.setTcpNoDelay   true)
        (.setKeepAlive    true)
        (.setReuseAddress true))

      (when socket-opts
        (opts/set-socket-opts! socket socket-opts))

      (.connect socket
        (java.net.InetSocketAddress. ^String host (int port))
        (int (or connect-timeout-ms 0)))

      (if ssl
        (if (fn? ssl)
          (let [ssl-socket
                (ssl {:conn-opts conn-opts
                      :socket socket
                      :host host
                      :port port})] ; Bounds itself
            (if (instance? Socket ssl-socket)
              ssl-socket
              (truss/ex-info! "[Carmine] Custom SSL function must return a `java.net.Socket`"
                {:eid :carmine.conns/invalid-custom-ssl-socket
                 :return-value (enc/typed-val ssl-socket)})))
          (if (nil? init-timeout-ms)
            (new-ssl-socket socket host port)
            ;; Like the init exchange, the built-in TLS handshake is bounded
            ;; by `:init-timeout-ms`: both per-read (so-timeout) and in
            ;; aggregate (watchdog), so connection construction cannot hang
            (with-socket-deadline! socket init-timeout-ms :init
              (fn []
                (let [current-timeout-ms (.getSoTimeout socket)]
                  (.setSoTimeout socket (int init-timeout-ms))
                  (let [^Socket ssl-socket (new-ssl-socket socket host port)]
                    (.setSoTimeout ssl-socket (int current-timeout-ms))
                    ssl-socket))))))

        socket)

      (catch Throwable t
        (truss/catching (.close socket))
        (throw t)))))

(comment (.close (new-socket nil {:ssl true :connect-timeout-ms 2000} "127.0.0.1" 6379)))

(defn new-conn
  "Returns a new `Conn` for `conn-opts`. Supports Sentinel resolution. Internal."
  (^Conn [conn-opts]
   (let [t0 (System/currentTimeMillis)
         {:keys [server]} conn-opts]

     (enc/cond
       (vector? server) (let [[host port] server] (new-conn conn-opts t0 nil host port))
       (map?    server)
       (cond
         (get server :sentinel-spec)
         (let [{:keys [master-name sentinel-spec sentinel-opts]} server
               [host port]
               (enc/binding* [sentinel/*mgr-conn-opts* conn-opts]
                 (sentinel/resolve-addr! ; May trigger `:on-changed-master` cb
                   sentinel-spec master-name
                   sentinel-opts (not :use-cache)))]

           (new-conn conn-opts t0 master-name host port))

         (get server :cluster-spec)
         (truss/ex-info! "[Carmine] Cluster server options require `conn-manager` or `conn-manager-clustered`"
           {:eid :carmine.cluster/manager-required
            :server server})

         :else
         (truss/ex-info! "[Carmine] Unexpected map `:server`"
           {:eid :carmine.conns/unexpected-server-map
            :server (enc/typed-val server)}))

       ;; Shouldn't be possible after validation
       (truss/ex-info! "[Carmine] Unexpected `:server` type"
         {:eid :carmine.conns/unexpected-server-type
          :server (enc/typed-val server)}))))

  (^Conn [conn-opts t0 master-name host port]
   (let [host (truss/have string? host)
         port (enc/as-int         port)

         {:keys [#_server socket-opts buffer-opts resp-opts]} conn-opts
         {:keys [init-size-in init-size-out]
          :or ; Defaults relevant only for REPL/tests
          {init-size-in  1024
           init-size-out 1024}} buffer-opts]

     (debug! :new-conn [host port])
     (try
       ;; Could use Jedis streams below but initial benching showed little benefit:
       ;;   - `jedis.RedisInputStream`:    readIntCrLf, readLongCrLf, readLineBytes
       ;;   - `jedis.RedisOutputStream`:  writeIntCrLf, writeCrLf
       ;;
       (let [socket (new-socket conn-opts socket-opts host port)
             conn_  (volatile! nil)]
         (try
           (let [in   (com/resp-input (.getInputStream socket) init-size-in (get resp-opts :limits))
                 out  (-> socket .getOutputStream (java.io.BufferedOutputStream. init-size-out))
                 conn (Conn. socket host port [host port] conn-opts in out (enc/latom true)
                        (long *new-conn-generation*))]
             (vreset! conn_ conn)
             (conn-init! conn)
             (do         conn))

           (catch Throwable t
             (if-let [conn @conn_]
               (conn-close! conn {:via 'new-conn, :cause t})
               (truss/catching (.close socket)))
             (throw t))))

       (catch Throwable t
         (utils/cb-notify-and-throw!    :on-conn-error
           (get core/*conn-cbs*         :on-conn-error)
           (utils/get-at conn-opts :cbs :on-conn-error)
           (truss/ex-info (str "[Carmine] Error creating new connection" (cause-summary [host port] t))
             {:eid :carmine.conns/new-conn-error
              :via         'new-conn
              :addr        [host port]
              :master-name master-name
              :conn-opts   (utils/redact-secrets conn-opts)
              :elapsed-ms  (when t0 (- (System/currentTimeMillis) ^long t0))}
             t)))))))

(comment
  (enc/qb 1e3 ; [42.83 55.34], port limited
    (conn-close! (new-conn {}                         0 nil "127.0.0.1" 6379) nil)
    (conn-close! (new-conn {:socket-opts {:ssl true}} 0 nil "127.0.0.1" 6379) nil)))

(defn- with-conn [^Conn conn f]
  (try
    (f conn (.-in conn) (.-out conn))
    (finally (conn-close! conn {:via 'with-new-conn}))))

(defn with-new-conn
  "Opens one connection, calls `f`, and closes the connection. Internal."
  ([conn-opts                       f] (with-conn (new-conn conn-opts) f))
  ([conn-opts host port master-name f]
   (with-conn
     (new-conn conn-opts (System/currentTimeMillis) master-name host port)
     f)))

(comment (with-new-conn {} "127.0.0.1" 6379 nil (fn [conn in out] (conn-ready? conn))))

;;;; Connection managers

(defn- manager-stats-base
  [manager-kind push-dispatcher connections counts timings nodes]
  {:kind manager-kind
   :snapshot-client-time-ms (System/currentTimeMillis)
   :connections connections
   :counts counts
   :timings timings
   :push (push-dispatcher-stats push-dispatcher)
   :nodes nodes})

(defn- keyed-pool-node-stats [^GenericKeyedObjectPool pool]
  (try
    (into []
      (map
        (fn [addr]
          {:addr (vec addr)
           :active (.getNumActive pool addr)
           :idle   (.getNumIdle   pool addr)}))
      (sort-by pr-str (.getKeys pool)))
    (catch IllegalStateException _ [])))

(defprotocol ^:private IConnManager
  "Internal protocol, not currently for public use or extension."

  (^:public mgr-open? [mgr]
   "Returns true iff `ConnManager` is open for borrowing.")

  (mgr-clear! [mgr timeout-ms]
   "Tells `ConnManager` to clear its pooled connections. Destroys connections
   that are idle or later returned. Carmine calls this automatically after a
   Sentinel master change. See `taoensso.carmine-v4/conn-manager-clear!` for
   the public timeout and return contract.")

  (mgr-borrow! [mgr f]
   "Borrows a connection and calls
   `(f <Conn> <java.io.DataInputStream> <java.io.BufferedOutputStream>)`.
   Returns the result of `f`. Always releases the borrowed connection when done:
   an unpooled connection is closed; a pooled connection is returned when it
   remains reusable and current, otherwise invalidated. A Cluster manager does
   not support this operation and throws `:carmine.conns/borrow-not-supported`;
   see `mgr-borrow-addr!`.")

  (mgr-conn-opts [mgr]
   "Returns internal parsed connection options without diagnostic redaction.")

  (^:public mgr-push-fn [mgr server-addr]
   "Returns the manager-scoped RESP3 push dispatch function for `server-addr`,
   or nil.")

  (^:public mgr-cluster-server [mgr]
   "Returns parsed Cluster server options for a Cluster manager, otherwise nil.")

  (^:public mgr-borrow-addr! [mgr addr f]
   "Borrows a Cluster manager connection for `addr` and calls `f`.")

  (^:public mgr-close! [mgr timeout-ms data]
   "Permanently closes the `ConnManager`:
     - Stops accepting new borrow requests.
     - Destroys any idle or returned `Conns`.

   NB interruption after a finite timeout can be dangerous to data integrity: a
   pipeline can be interrupted during execution. See
   `taoensso.carmine-v4/conn-manager-close!` for the public timeout,
   `data`, and return contract."))

(defn ^:public conn-manager?
  "Returns true iff the given `x` is a Carmine v4 connection manager."
  [x]
  (or
    #_(enc/satisfies? IConnManager x)
    (satisfies?       IConnManager x)))

(defn ^:public mgr-stats
  "Returns the statistics snapshot exposed by `mgr`.

  The same map is at `(:stats @mgr)`. All manager types use this top-level
  schema: `:connections`, cumulative `:counts`, `:timings`, `:push`, and
  optional Cluster `:nodes`. Unsupported values are nil. Cluster node rows
  contain the addresses that the pool knows, not the complete topology."
  [mgr]
  (let [mgr (force mgr)]
    (truss/have conn-manager? mgr)
    (:stats @mgr)))

(defn- drain-timeout-ms! [via timeout-ms]
  (if (or (nil? timeout-ms)
        (and (integer? timeout-ms) (<= 0 timeout-ms Long/MAX_VALUE)))
    (some-> timeout-ms long)
    (truss/ex-info! "[Carmine] Invalid connection drain timeout"
      {:eid :carmine.conns/invalid-timeout
       :via via
       :timeout-ms (enc/typed-val timeout-ms)
       :expected :nil-or-nat-int})))

(defn- drain-conns!
  "Waits up to `timeout-ms` for the specified connections to close. Nil sets no
  limit. After a timeout, interrupts connections that remain open. Returns true
  iff no timeout occurred."
  [conns timeout-ms close-data]
  (cond
    (empty? conns) true
    :else
    (let [deadline-nanos
          (when timeout-ms (utils/timeout-deadline-nanos timeout-ms))]
      (loop  [conns conns]
        (let [conns (into #{} (filter #(conn-open? %)) conns) ; Open conns
              remaining-ms
              (when deadline-nanos
                (utils/remaining-timeout-ms deadline-nanos))]
          (cond
            (empty? conns) true

            (and deadline-nanos (nil? remaining-ms))
            (do ; Give up waiting
              (run! #(truss/catching (conn-close! % close-data)) conns)
              false)

            :else
            (let [sleep-ms (if remaining-ms (min 100 remaining-ms) 100)]
              (try
                (Thread/sleep (long sleep-ms))
                (catch InterruptedException t
                  (run! #(truss/catching (conn-close! % close-data)) conns)
                  (.interrupt (Thread/currentThread))
                  (throw t)))
              (recur conns))))))))

(comment
  (let [c1 (new-conn {} 0 nil "127.0.0.1" 6379)]
    (future (Thread/sleep 200) (conn-close! c1 {}))
    (drain-conns! #{c1} 100 {})))

(defn- throw-mgr-closed! [mgr]
  (truss/ex-info! "[Carmine] Cannot borrow from closed `ConnManager`"
    {:eid :carmine.conns/manager-closed, :mgr mgr}))
(defn- throw-borrow-not-supported! [mgr]
  (truss/ex-info! "[Carmine] Connection manager does not support this kind of borrow"
    {:eid :carmine.conns/borrow-not-supported, :mgr mgr}))
(defn- throw-cluster-manager-required! [conn-opts]
  (truss/ex-info! "[Carmine] Cluster server options require `conn-manager` or `conn-manager-clustered`"
    {:eid :carmine.cluster/manager-required
     :conn-opts (utils/redact-secrets conn-opts)}))
(defn- throw-mgr-borrow-error! [mgr conn-opts t0 t]
  (when (instance? InterruptedException t)
    (.interrupt (Thread/currentThread)))
  (let [addr (some #(get (ex-data %) :addr) (take-while some? (iterate ex-cause t)))
        pool-error? ; As thrown by Commons Pool, without a cause
        (and
          (instance? java.util.NoSuchElementException t)
          (nil? (ex-cause t)))]

    (truss/ex-info!
      (enc/cond
        (not pool-error?)
        (str "[Carmine] Error borrowing connection from `ConnManager`" (cause-summary addr t))

        :let [msg (str (ex-message t))]

        (str/starts-with? msg "Unable to validate")
        "[Carmine] New pooled connection failed its check (see `:on-conn-error` callback for the cause)"

        (str/starts-with? msg "Pool exhausted")
        "[Carmine] No pooled connection available (see `:max-total` pool option)"

        :else
        "[Carmine] Timed out waiting for a pooled connection (see `:max-wait-ms` pool option)")

      (enc/assoc-some
        {:eid :carmine.conns/borrow-conn-error
         :mgr        mgr
         :conn-opts  (utils/redact-secrets conn-opts)
         :elapsed-ms (when t0 (- (System/currentTimeMillis) ^long t0))}
        :addr addr)
      t)))

(defn- request-error
  "Returns the error to throw for `t`, thrown while using the borrowed `conn`.
  An IO error (incl. a read timeout) is wrapped to add the server's address.
  Only `t` itself is checked, so an error from a nested borrow is left as is."
  [mgr conn-opts conn t]
  (let [io-error
        (enc/cond
          (instance? java.io.IOException t) t
          (= (get (ex-data t) :eid) :carmine.read/unexpected-read-error)
          (let [cause (ex-cause t)]
            (when (instance? java.io.IOException cause) cause)))]
    (if-not io-error
      t
      (if (instance? java.net.SocketTimeoutException io-error)
        (truss/ex-info "[Carmine] Read timed out during request (see `:read-timeout-ms`)"
          {:eid :carmine.conns/conn-io-error
           :mgr  mgr
           :addr (conn-addr conn)
           :read-timeout-ms (utils/get-at conn-opts :socket-opts :read-timeout-ms)}
          io-error)
        (truss/ex-info "[Carmine] Connection error during request"
          {:eid :carmine.conns/conn-io-error
           :mgr  mgr
           :addr (conn-addr conn)}
          io-error)))))

(defn- flush-outer-reqs!
  "Sends any requests queued by an outer request context (e.g. by an outer
  `wcar`) before a nested borrow. Their conn is still the one in scope here,
  so a failure or a stateful command is attributed to the right conn."
  []
  (when-let [ctx resp/*ctx*] (resp/flush-pending-reqs ctx)))

;; `enc/latom` updates through its IFn convenience arity allocate a capturing
;; function at each borrow/return. These small CAS loops keep the same atomic
;; persistent-collection updates without that per-operation closure.
(defn- active-conj! [active_ conn]
  (loop []
    (let [old @active_]
      (when-not (compare-and-set! active_ old (conj old conn))
        (recur)))))

(defn- active-disj! [active_ conn]
  (loop []
    (let [old @active_]
      (when-not (compare-and-set! active_ old (disj old conn))
        (recur)))))

(deftype ^:private BorrowId [^Thread thread])

(defn- new-borrow-id ^BorrowId [] (BorrowId. (Thread/currentThread)))

(defn- conns-to-drain
  "Returns the set of borrowed conns that a clear should wait on: those of
  other threads. The calling thread's borrows can't progress while it clears
  (e.g. a Sentinel clear from a nested borrow), and they're invalidated on
  return."
  [active]
  (let [thread (Thread/currentThread)]
    (persistent!
      (reduce-kv
        (fn [acc ^BorrowId id conn]
          (if (identical? (.-thread id) thread) acc (conj! acc conn)))
        (transient #{}) active))))

(defn- active-assoc! [active_ borrow-id conn]
  (loop []
    (let [old @active_]
      (when-not (compare-and-set! active_ old (assoc old borrow-id conn))
        (recur)))))

(defn- active-dissoc! [active_ borrow-id]
  (loop []
    (let [old @active_]
      (when-not (compare-and-set! active_ old (dissoc old borrow-id))
        (recur)))))

(deftype ConnManagerUnpooled
  [mgr-opts conn-opts closed?_ active-conns_ close-data_ lifecycle-lock
   ^AtomicLong n-created* ^AtomicLong n-failed*
   push-dispatcher]

  java.io.Closeable (close [this] (mgr-close! this nil {:mgr this, :via 'java.io.Closeable}))
  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine.ConnManagerUnpooled"
      {:name  (get mgr-opts :mgr-name)
       :open? (not (closed?_))}))

  clojure.lang.IDeref
  (deref [_]
    {:open?     (not (closed?_))
     :mgr-opts  mgr-opts
     :conn-opts (utils/redact-secrets conn-opts)
     :stats
     (manager-stats-base :unpooled push-dispatcher
       {:active (count @active-conns_), :idle nil, :waiting nil}
       {:created (.get n-created*), :borrowed (.get n-created*)
        :returned nil, :failed (.get n-failed*), :cleared nil
        :destroyed {:total nil, :by-borrow-validation nil, :by-eviction nil}}
       {:mean-borrow-wait-ms nil, :max-borrow-wait-ms nil
        :mean-idle-ms nil, :mean-active-ms nil}
       nil)})

  IConnManager
  (mgr-open?   [_] (not (closed?_)))
  (mgr-clear!  [_ timeout-ms]
    (drain-timeout-ms! 'mgr-clear! timeout-ms)
    (locking lifecycle-lock
      false)) ; Nothing to clear
  (mgr-conn-opts [_] conn-opts)
  (mgr-push-fn [this server-addr]
    (when push-dispatcher
      #(dispatch-push! push-dispatcher this conn-opts server-addr false %)))
  (mgr-cluster-server [_] nil)
  (mgr-borrow-addr! [this _addr _f] (throw-borrow-not-supported! this))
  (mgr-borrow! [this f]
    (debug! :unpooled/borrow!)
    (flush-outer-reqs!)
    (if (closed?_)
      (throw-mgr-closed! this)
      (let [t0 (System/currentTimeMillis)
            ^Conn conn
            (try
              (new-conn conn-opts)
              (catch Throwable t
                (.getAndIncrement n-failed*)
                (throw-mgr-borrow-error! this conn-opts t0 t)))]

        (.getAndIncrement n-created*)
        (if-not
          (locking lifecycle-lock
            (when-not (closed?_)
              (active-conj! active-conns_ conn)
              true))
          (do
            (conn-close! conn
              (or @close-data_
                {:mgr this, :via 'mgr-borrow!, :mgr-closed? true}))
            (throw-mgr-closed! this))
          (try
            (let [result
                  ;; Not reusable anyway, and mustn't mark an outer borrowed
                  ;; conn as non-reusable
                  (binding [write/*conn-reusable?_ nil]
                    (f conn (.-in conn) (.-out conn)))]
              (with-deferred-close-cbs*
                conn-opts
                (fn []
                  (locking lifecycle-lock
                    (conn-close! conn
                      (or @close-data_ {:mgr this, :via 'mgr-borrow!})))))
              result)

            (catch Throwable t
              (let [t (request-error this conn-opts conn t)]
                (with-deferred-close-cbs*
                  conn-opts
                  (fn []
                    (locking lifecycle-lock
                      (conn-close! conn
                        (assoc
                          (or @close-data_ {:mgr this, :via 'mgr-borrow!})
                          :cause t)))))
                (when (instance? InterruptedException t)
                  (.interrupt (Thread/currentThread)))
                (throw t)))

            (finally (active-disj! active-conns_ conn)))))))

  (mgr-close! [this timeout-ms data]
    (debug! :unpooled/close! timeout-ms data)
    (let [timeout-ms (drain-timeout-ms! 'mgr-close! timeout-ms)
          close-data
          (enc/merge {:mgr this, :via 'mgr-close!, :timeout-ms timeout-ms} data)
          active-conns
          (locking lifecycle-lock
            (when (compare-and-set! closed?_ false true)
              (reset! close-data_ close-data)
              (set @active-conns_)))]
    (if (some? active-conns)
      (do
        (close-push-dispatcher! push-dispatcher)
        (drain-conns! active-conns timeout-ms close-data)
        true)
      false))))

(let [idx (java.util.concurrent.atomic.AtomicLong. 0)
      next-mgr-name! (fn [] (str "unpooled:" (.incrementAndGet idx)))]

  (defn ^:public conn-manager-unpooled
    "Returns a new connection manager that opens a new connection for each
    use, and closes it afterwards. Prefer `conn-manager` in most cases.

    Options:
      - `:conn-opts` - See `default-conn-opts`.
      - `:mgr-name`  - Optional name for logs and stats, generated if omitted.
      - `:on-push`   - Optional `(fn [push-event])` for RESP3 push messages.
          Pushes are read only while a connection is in use, so this isn't
          suitable for client-side caching.
      - `:push-queue-capacity` - Max pushes waiting for `:on-push`
          (default 1024). Extra pushes are dropped and reported to
          `:on-push-error`.

    Deref for status and stats. Close with `conn-manager-close!` or
    `java.io.Closeable`."
    (^ConnManagerUnpooled [] (conn-manager-unpooled {}))
    (^ConnManagerUnpooled [opts]
     (let [opts (validate-manager-opts! :unpooled
                  #{:conn-opts :mgr-name :on-push :push-queue-capacity} opts)
           {:keys [conn-opts mgr-name], push-fn :on-push} opts
           push-queue-capacity (get opts :push-queue-capacity default-push-queue-capacity)
           conn-opts (opts/parse-conn-opts :redis conn-opts)
           _ (when (opts/get-cluster-server conn-opts)
               (throw-cluster-manager-required! conn-opts))
           mgr-name  (let [v (get opts :mgr-name ::auto)] (if (identical? v ::auto) (next-mgr-name!) v))
           mgr-opts
           (not-empty
             (enc/assoc-some (dissoc opts :conn-opts)
               :mgr-name mgr-name))
           push-dispatcher (new-push-dispatcher mgr-name push-fn push-queue-capacity)]

       (ConnManagerUnpooled. mgr-opts conn-opts
         (enc/latom false)
         (enc/latom #{})
         (atom nil)
         (Object.)
         (AtomicLong. 0)
         (AtomicLong. 0)
         push-dispatcher)))))

(def ^:private ^:dynamic *mgr-close-data* nil)

(defn- cleanup-borrowed-conn!
  "Runs `(cleanup!)` to return or invalidate a connection after `primary-error`.
  Does not mask the primary error. Adds cleanup failures as suppressed
  exceptions and restores interrupt status. If pool cleanup fails, tries to
  close the connection directly."
  [^Throwable primary-error conn close-data cleanup!]
  (try
    (cleanup!)
    (catch Throwable t
      (truss/catching (.addSuppressed primary-error t))
      (when (instance? InterruptedException t)
        (.interrupt (Thread/currentThread)))
      (truss/catching (conn-close! conn close-data))))
  (when (instance? InterruptedException primary-error)
    (.interrupt (Thread/currentThread))))

;; Pool operations that may create a conn (and so block, or call user
;; callbacks) never run under `clear-lock`: `returnObject` and
;; `invalidateObject` may create one when borrowers are waiting. So a conn
;; from before a clear may return idle after it, if its return races the
;; clear. Each conn is stamped with the generation at its creation, and a
;; borrow registers only a conn of the current generation, so such a conn is
;; never reused: the borrow invalidates it and tries again.

(declare pooled-return! clustered-return!)

(defn- return-valid?
  "Runs any `:test-on-return?` `validator`. The request has already succeeded,
  so a check that throws is reported and fails, rather than escaping."
  [validator ^Conn conn]
  (or (nil? validator)
    (try
      (boolean (validator conn))
      (catch Throwable t
        (when (instance? InterruptedException t)
          (.interrupt (Thread/currentThread)))
        (utils/cb-notify!
          (get core/*conn-cbs* :on-conn-error)
          (utils/get-at (.-conn-opts conn) :cbs :on-conn-error)
          (delay
            {:cbid      :on-conn-error
             :eid       :carmine.conns/conn-not-ready
             :addr      (conn-addr conn)
             :conn      conn
             :conn-opts (utils/redact-secrets (.-conn-opts conn))
             :via       'return-valid?
             :cause     t}))
        false))))

(defn- invalidate-quietly!
  "Invalidates a borrowed `conn`, closing it directly if that fails. Never throws."
  [pool addr conn close-data]
  (try
    (enc/binding* [*mgr-close-data* close-data]
      (if (nil? addr)
        (.invalidateObject ^GenericObjectPool      pool      conn)
        (.invalidateObject ^GenericKeyedObjectPool pool addr conn)))
    (catch Throwable t
      (when (instance? InterruptedException t)
        (.interrupt (Thread/currentThread)))
      (truss/catching (conn-close! conn close-data)))))

(deftype ConnManagerPooled
  ;; Ref. `org.apache.commons.pool2.impl.GenericObjectPool`,
  ;;      `org.apache.commons.pool2.PooledObjectFactory`
  [mgr-opts conn-opts ^GenericObjectPool pool active-conns_ closed?_ close-data_
   clear-data_ clear-lock ^AtomicLong generation*
   ^AtomicLong n-failed* ^AtomicLong n-cleared*
   push-dispatcher return-validator]

  java.io.Closeable (close [this] (mgr-close! this nil {:mgr this, :via 'java.io.Closeable}))
  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine.ConnManagerPooled"
      {:name  (get mgr-opts :mgr-name)
       :open? (not (closed?_))}))

  clojure.lang.IDeref
  (deref [_]
    {:open?     (and (not (closed?_)) (not (.isClosed pool)))
     :mgr-opts  mgr-opts
     :conn-opts (utils/redact-secrets conn-opts)
     :stats
     (manager-stats-base :pooled push-dispatcher
       {:active (.getNumActive pool), :idle (.getNumIdle pool)
        :waiting (.getNumWaiters pool)}
       {:created (.getCreatedCount pool), :borrowed (.getBorrowedCount pool)
        :returned (.getReturnedCount pool), :failed (.get n-failed*)
        :cleared (.get n-cleared*)
        :destroyed
        {:total (.getDestroyedCount pool)
         :by-borrow-validation (.getDestroyedByBorrowValidationCount pool)
         :by-eviction (.getDestroyedByEvictorCount pool)}}
       {:mean-borrow-wait-ms (.getMeanBorrowWaitTimeMillis pool)
        :max-borrow-wait-ms (.getMaxBorrowWaitTimeMillis pool)
        :mean-idle-ms (.getMeanIdleTimeMillis pool)
        :mean-active-ms (.getMeanActiveTimeMillis pool)}
       nil)})

  IConnManager
  (mgr-open? [_] (and (not (closed?_)) (not (.isClosed pool))))
  (mgr-conn-opts [_] conn-opts)
  (mgr-push-fn [this server-addr]
    (when push-dispatcher
      #(dispatch-push! push-dispatcher this conn-opts server-addr false %)))
  (mgr-cluster-server [_] nil)
  (mgr-borrow-addr! [this _addr _f] (throw-borrow-not-supported! this))
  (mgr-clear! [this timeout-ms]
    (debug! :pooled/clear!)
    (let [timeout-ms (drain-timeout-ms! 'mgr-clear! timeout-ms)
          not-performed (Object.)
          old-conns_ (volatile! not-performed)
          close-data {:mgr this, :via 'mgr-clear!, :timeout-ms timeout-ms}]
      (try
        (with-deferred-close-cbs* ; Never run user cbs under clear-lock
          conn-opts
          (fn []
            (locking clear-lock
              (when-not (closed?_)
                (.getAndIncrement n-cleared*)
                (reset! clear-data_ close-data)
                (.incrementAndGet generation*) ; Odd => clear in progress
                (vreset! old-conns_ (conns-to-drain @active-conns_))
                (try
                  (enc/binding* [*mgr-close-data* close-data] (.clear pool))
                  (finally (.incrementAndGet generation*))))))) ; Even => stable
        (when-not (identical? @old-conns_ not-performed)
          (drain-conns! @old-conns_ timeout-ms close-data))
        (finally
          (when-not (identical? @old-conns_ not-performed)
            (compare-and-set! clear-data_ close-data nil))))
      (not (identical? @old-conns_ not-performed))))

  (mgr-borrow! [this f]
    (debug! :pooled/borrow!)
    ;; NB nested same-manager borrows are deliberately allowed: nested `wcar`
    ;; and `transact!`-in-`wcar` are supported (tested) patterns, with the
    ;; pool-exhaustion caveat documented at the call sites.
    (flush-outer-reqs!)
    (loop []
      (when (closed?_) (throw-mgr-closed! this))
      (let [t0 (System/currentTimeMillis)
            generation (.get generation*)
            ^Conn conn
            (try
              (enc/binding*
                [*mgr-close-data* {:mgr this, :via 'mgr-borrow!, :validation? true}
                 *new-conn_*      (volatile! nil)]
                (.borrowObject pool))
              (catch IllegalStateException t
                (if (or (closed?_) (.isClosed pool))
                  (throw-mgr-closed! this)
                  (throw t)))
              (catch Throwable t
                (if (= (:eid (ex-data t)) :carmine.conns/borrow-conn-error)
                  (throw t)
                  (do
                    (.getAndIncrement n-failed*)
                    (throw-mgr-borrow-error! this conn-opts t0 t)))))
            borrow-id (new-borrow-id)
            stable?
            (locking clear-lock
              (when (and (not (closed?_))
                      (even? (.get generation*))
                      (= generation (.get generation*) (.-generation conn)))
                (active-assoc! active-conns_ borrow-id conn)
                true))]
        (if-not stable?
          (let [close-data (or @clear-data_ @close-data_
                             {:mgr this, :via 'mgr-borrow!})]
            (try
              (enc/binding* [*mgr-close-data* close-data]
                (.invalidateObject pool conn))
              (catch Throwable _
                (conn-close! conn close-data)))
            (if (closed?_) (throw-mgr-closed! this) (recur)))

          (let [conn-reusable?_ (volatile! true)
                result
                (try
                  (enc/binding* [write/*conn-reusable?_ conn-reusable?_]
                    (f conn (.-in conn) (.-out conn)))
                  (catch Throwable t
                    (let [t (request-error this conn-opts conn t)]
                      (if (and @conn-reusable?_ (instance? ReusableConnError t))
                        ;; The request context consumed every expected reply before
                        ;; raising a Redis reply error, so framing remains intact.
                        (pooled-return! this conn generation borrow-id true)
                        ;; Conservatively invalidate on application/IO failures since
                        ;; the connection may be left in an unexpected protocol state.
                        (try
                          (cleanup-borrowed-conn! t conn
                            {:mgr this, :via 'mgr-borrow!, :cause t}
                            (fn []
                              (enc/binding* [*mgr-close-data* {:mgr this, :via 'mgr-borrow!, :cause t}]
                                (.invalidateObject pool conn))))
                          (finally (active-dissoc! active-conns_ borrow-id))))
                      (throw t))))]
            (pooled-return! this conn generation borrow-id @conn-reusable?_)
            result)))))

  (mgr-close! [this timeout-ms data]
    (debug! :pooled/close! timeout-ms data)
    (let [timeout-ms (drain-timeout-ms! 'mgr-close! timeout-ms)
          close-data
          (enc/merge {:mgr this, :via 'mgr-close!, :timeout-ms timeout-ms} data)
          active-conns
          (locking clear-lock
            (when (compare-and-set! closed?_ false true)
              (reset! close-data_ close-data)
              (vec (vals @active-conns_))))]
    (if (some? active-conns)
      (do
        (close-push-dispatcher! push-dispatcher)
        (enc/binding* [*mgr-close-data* close-data]
          (.close pool))
        (drain-conns! active-conns timeout-ms close-data)
        true)
      false))))

(defn- pooled-return!
  "Returns `conn` to its pool after a request that consumed its replies, or
  invalidates it. Never throws: the request's outcome stands."
  [^ConnManagerPooled mgr ^Conn conn generation borrow-id reusable?]
  (let [^GenericObjectPool pool (.-pool mgr)
        ^AtomicLong generation* (.-generation* mgr)
        closed?_ (.-closed?_ mgr)
        valid?
        (and reusable?
          (not (com/conn-broken? (.-in conn))) ; A failed flush, even if its error was caught
          ;; Skip the check for a conn that'll be invalidated anyway
          (= generation (.get generation*)) (not (closed?_)) (conn-open? conn)
          (return-valid? (.-return-validator mgr) conn))

        ;; Deregister before checking the generation: a clear that waits on
        ;; this conn has then already changed it, so the conn is invalidated
        _ (active-dissoc! (.-active-conns_ mgr) borrow-id)
        returned?
        (try
          (when (and valid?
                  (= generation (.get generation*))
                  (not (closed?_))
                  (conn-open? conn)
                  (conn-resolved? conn :use-cache)) ; Cache only
            (.returnObject pool conn)
            true)
          (catch Throwable t (debug! :pooled/return-error t) false))]

    (when-not returned?
      (invalidate-quietly! pool nil conn
        (or @(.-clear-data_ mgr) @(.-close-data_ mgr)
          {:mgr mgr, :via 'mgr-borrow!})))
    nil))

(let [idx (java.util.concurrent.atomic.AtomicLong. 0)
      next-mgr-name! (fn [] (str "pooled:" (.incrementAndGet idx)))]

  (defn conn-manager-pooled
    "Returns a new pooled connection manager for standalone Redis or Sentinel,
    see `conn-manager`."

    (^ConnManagerPooled [] (conn-manager-pooled {}))
    (^ConnManagerPooled [opts]
     (let [opts (validate-manager-opts! :pooled
                 #{:conn-opts :mgr-name :pool-opts :on-push :push-queue-capacity} opts)
          {:keys [conn-opts pool-opts mgr-name], push-fn :on-push} opts
          push-queue-capacity (get opts :push-queue-capacity default-push-queue-capacity)
          conn-opts (opts/parse-conn-opts :redis conn-opts)
          _ (when (opts/get-cluster-server conn-opts)
              (throw-cluster-manager-required! conn-opts))
          mgr-name  (let [v (get opts :mgr-name ::auto)] (if (identical? v ::auto) (next-mgr-name!) v))
          mgr-opts
          (not-empty
            (enc/assoc-some (dissoc opts :conn-opts)
              :mgr-name mgr-name))

          mgr_       (volatile! nil)
          n-failed*  (AtomicLong. 0)
          n-cleared* (AtomicLong. 0)
          close-data_ (atom nil)
          clear-data_ (atom nil)
          clear-lock  (Object.)
          generation* (AtomicLong. 0)
          push-dispatcher (new-push-dispatcher mgr-name push-fn push-queue-capacity)
          return-validator_ (volatile! nil)
          ^GenericObjectPool pool
          (let [created-pool_ (volatile! nil)]
            (try
              (let [pool-opts (enc/nested-merge core/default-pool-opts pool-opts)
                    ready-check-after-idle-ms (get pool-opts :ready-check-after-idle-ms)
                    sentinel-mgr-cbs
                    (when-let [{:keys [master-name sentinel-opts]} (opts/get-sentinel-server conn-opts)]
                      (let [{:keys [clear-timeout-ms]} sentinel-opts]
                        {:on-changed-master
                         (fn [{master-name* :master-name}]
                           (when-let [mgr @mgr_]
                             (when (= master-name* master-name)
                               (mgr-clear! mgr clear-timeout-ms))))}))

                    factory
                    (reify PooledObjectFactory
                      (activateObject  [_ po] nil)
                      (passivateObject [_ po] nil)
                      (validateObject  [_ po]
                        (let [^Conn conn (.getObject po)
                              validation-push-fn
                              (when-let [mgr @mgr_]
                                (mgr-push-fn mgr (conn-addr conn)))
                              ready?
                              #(pooled-conn-ready? po ready-check-after-idle-ms
                                 validation-push-fn)]
                          (if-let [cbs sentinel-mgr-cbs]
                            (enc/binding* [sentinel/*mgr-cbs* cbs] (ready?))
                            (ready?))))
                      (destroyObject   [_ po] (conn-close! (.getObject po) (or *mgr-close-data* @close-data_)))
                      (makeObject      [_]
                        (let [t0 (System/currentTimeMillis)]
                          (try
                            (let [conn
                                  (enc/binding* [*new-conn-generation* (.get generation*)] ; Before any I/O
                                    (if-let [cbs sentinel-mgr-cbs]
                                      (enc/binding* [sentinel/*mgr-cbs* cbs] (new-conn conn-opts))
                                      (do                                    (new-conn conn-opts))))]
                              (when-let [new-conn_ *new-conn_*] (vreset! new-conn_ conn))
                              (DefaultPooledObject. conn))

                            (catch Throwable t
                              (.getAndIncrement n-failed*)
                              (throw-mgr-borrow-error! @mgr_ conn-opts t0 t))))))
                    pool (GenericObjectPool. factory)]

                (vreset! created-pool_ pool)
                (opts/set-pool-opts! pool pool-opts)
                (when (.getTestOnReturn pool)
                  ;; Validate in `pooled-return!` instead
                  (.setTestOnReturn pool false)
                  (vreset! return-validator_
                    (fn [conn]
                      (let [push-fn (when-let [mgr @mgr_] (mgr-push-fn mgr (conn-addr conn)))]
                        (if-let [cbs sentinel-mgr-cbs]
                          (enc/binding* [sentinel/*mgr-cbs* cbs] (conn-ready? conn true push-fn))
                          (do                                    (conn-ready? conn true push-fn)))))))
                pool)
              (catch Throwable t
                (close-construction-resources! @created-pool_ push-dispatcher t))))

          mgr
          (vreset! mgr_
            (ConnManagerPooled. mgr-opts conn-opts pool
              (enc/latom {})
              (enc/latom false)
              close-data_
              clear-data_
              clear-lock
              generation*
              n-failed*
              n-cleared*
              push-dispatcher
              @return-validator_))]

      (try
        (.preparePool pool) ; Ensure that configured min idle instances ready
        (catch Throwable t
          (close-construction-resources! pool push-dispatcher t)))
      mgr))))

;;;; Cluster connection manager

(defn- sweep-if-closed!
  "Invalidations and returns may create a replacement that lands idle after
  `close` swept the pool (`GenericKeyedObjectPool/reuseCapacity` doesn't
  check), so sweeps a closed pool again."
  [^GenericKeyedObjectPool pool]
  (when (.isClosed pool) (truss/catching (.clear pool))))

(deftype ConnManagerClustered
  [mgr-opts conn-opts cluster-server ^GenericKeyedObjectPool pool
   active-conns_ closed?_ close-data_ clear-data_ clear-lock
   ^AtomicLong generation* ^AtomicLong n-failed* ^AtomicLong n-cleared*
   push-dispatcher return-validator]

  java.io.Closeable (close [this] (mgr-close! this nil {:mgr this, :via 'java.io.Closeable}))
  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine.ConnManagerClustered"
      {:name (get mgr-opts :mgr-name), :open? (not (closed?_))}))

  clojure.lang.IDeref
  (deref [_]
    {:open?     (and (not (closed?_)) (not (.isClosed pool)))
     :mgr-opts  mgr-opts
     :conn-opts (utils/redact-secrets conn-opts)
     :stats
     (manager-stats-base :clustered push-dispatcher
       {:active (.getNumActive pool), :idle (.getNumIdle pool)
        :waiting (.getNumWaiters pool)}
       {:created (.getCreatedCount pool), :borrowed (.getBorrowedCount pool)
        :returned (.getReturnedCount pool), :failed (.get n-failed*)
        :cleared (.get n-cleared*)
        :destroyed {:total (.getDestroyedCount pool)
                    :by-borrow-validation (.getDestroyedByBorrowValidationCount pool)
                    :by-eviction (.getDestroyedByEvictorCount pool)}}
       {:mean-borrow-wait-ms (.getMeanBorrowWaitTimeMillis pool)
        :max-borrow-wait-ms (.getMaxBorrowWaitTimeMillis pool)
        :mean-idle-ms (.getMeanIdleTimeMillis pool)
        :mean-active-ms (.getMeanActiveTimeMillis pool)}
       (keyed-pool-node-stats pool))})

  IConnManager
  (mgr-open? [_] (and (not (closed?_)) (not (.isClosed pool))))
  (mgr-conn-opts [_] conn-opts)
  (mgr-push-fn [this server-addr]
    (when push-dispatcher
      #(dispatch-push! push-dispatcher this conn-opts server-addr true %)))
  (mgr-cluster-server [_] cluster-server)
  (mgr-borrow! [this _f] (throw-borrow-not-supported! this))

  (mgr-borrow-addr! [this addr f]
    (let [addr (vec addr)]
      (loop []
        (when (closed?_) (throw-mgr-closed! this))
        (let [t0 (System/currentTimeMillis)
              generation (.get generation*)
              ^Conn conn
              (try
                (enc/binding*
                  [*mgr-close-data* {:mgr this, :via 'mgr-borrow-addr!, :validation? true}]
                  (.borrowObject pool addr))
                (catch IllegalStateException t
                  (if (or (closed?_) (.isClosed pool))
                    (throw-mgr-closed! this)
                    (throw t)))
                (catch Throwable t
                  (if (= (:eid (ex-data t)) :carmine.conns/borrow-conn-error)
                    (throw t)
                    (do
                      (.getAndIncrement n-failed*)
                      (throw-mgr-borrow-error! this
                        (assoc conn-opts :server addr) t0 t)))))
              borrow-id (new-borrow-id)
              stable?
              (locking clear-lock
                (when (and (not (closed?_))
                        (even? (.get generation*))
                        (= generation (.get generation*) (.-generation conn)))
                  (active-assoc! active-conns_ borrow-id conn)
                  true))]
          (if-not stable?
            (let [close-data (or @clear-data_ @close-data_
                               {:mgr this, :via 'mgr-borrow-addr!})]
              (try
                (enc/binding* [*mgr-close-data* close-data]
                  (.invalidateObject pool addr conn))
                (catch Throwable _
                  (conn-close! conn close-data))
                (finally (sweep-if-closed! pool)))
              (if (closed?_) (throw-mgr-closed! this) (recur)))

            (let [conn-reusable?_ (volatile! true)
                  result
                  (try
                    (enc/binding* [write/*conn-reusable?_ conn-reusable?_]
                      (f conn (.-in conn) (.-out conn)))
                    (catch Throwable t
                      (let [t (request-error this conn-opts conn t)]
                        (if (and @conn-reusable?_ (instance? ReusableConnError t))
                          (clustered-return! this addr conn generation borrow-id true)
                          (try
                            (cleanup-borrowed-conn! t conn
                              {:mgr this, :via 'mgr-borrow-addr!, :cause t}
                              (fn []
                                (enc/binding* [*mgr-close-data* {:mgr this, :via 'mgr-borrow-addr!, :cause t}]
                                  (.invalidateObject pool addr conn))))
                            (finally
                              (active-dissoc! active-conns_ borrow-id)
                              (sweep-if-closed! pool))))
                        (throw t))))]
              (clustered-return! this addr conn generation borrow-id @conn-reusable?_)
              result))))))

  (mgr-clear! [this timeout-ms]
    (let [timeout-ms (drain-timeout-ms! 'mgr-clear! timeout-ms)
          not-performed (Object.)
          old-conns_ (volatile! not-performed)
          close-data {:mgr this, :via 'mgr-clear!, :timeout-ms timeout-ms}]
      (try
        (with-deferred-close-cbs* ; Never run user cbs under clear-lock
          conn-opts
          (fn []
            (locking clear-lock
              (when-not (closed?_)
                (.getAndIncrement n-cleared*)
                (reset! clear-data_ close-data)
                (.incrementAndGet generation*) ; Odd => clear in progress
                (vreset! old-conns_ (conns-to-drain @active-conns_))
                (try
                  (enc/binding* [*mgr-close-data* close-data] (.clear pool))
                  (finally (.incrementAndGet generation*))))))) ; Even => stable
        (when-not (identical? @old-conns_ not-performed)
          (drain-conns! @old-conns_ timeout-ms close-data))
        (finally
          (when-not (identical? @old-conns_ not-performed)
            (compare-and-set! clear-data_ close-data nil))))
      (not (identical? @old-conns_ not-performed))))

  (mgr-close! [this timeout-ms data]
    (let [timeout-ms (drain-timeout-ms! 'mgr-close! timeout-ms)
          close-data (enc/merge {:mgr this, :via 'mgr-close!, :timeout-ms timeout-ms} data)
          active-conns
          (locking clear-lock
            (when (compare-and-set! closed?_ false true)
              (reset! close-data_ close-data)
              (vec (vals @active-conns_))))]
    (if (some? active-conns)
      (do
        (close-push-dispatcher! push-dispatcher)
        (enc/binding* [*mgr-close-data* close-data] (.close pool))
        (drain-conns! active-conns timeout-ms close-data)
        true)
      false))))

(defn- clustered-return!
  "Returns `conn` to its pool after a request that consumed its replies, or
  invalidates it. Never throws: the request's outcome stands."
  [^ConnManagerClustered mgr addr ^Conn conn generation borrow-id reusable?]
  (let [^GenericKeyedObjectPool pool (.-pool mgr)
        ^AtomicLong generation* (.-generation* mgr)
        closed?_ (.-closed?_ mgr)
        valid?
        (and reusable?
          (not (com/conn-broken? (.-in conn))) ; A failed flush, even if its error was caught
          ;; Skip the check for a conn that'll be invalidated anyway
          (= generation (.get generation*)) (not (closed?_)) (conn-open? conn)
          (return-valid? (.-return-validator mgr) conn))

        _ (active-dissoc! (.-active-conns_ mgr) borrow-id) ; See `pooled-return!`
        returned?
        (try
          (when (and valid?
                  (= generation (.get generation*))
                  (not (closed?_))
                  (conn-open? conn))
            (.returnObject pool addr conn)
            true)
          (catch Throwable t (debug! :clustered/return-error t) false))]

    (when-not returned?
      (invalidate-quietly! pool addr conn
        (or @(.-clear-data_ mgr) @(.-close-data_ mgr)
          {:mgr mgr, :via 'mgr-borrow-addr!})))

    (sweep-if-closed! pool)
    nil))

(let [idx (AtomicLong. 0)
      next-mgr-name! (fn [] (str "clustered:" (.incrementAndGet idx)))]
  (defn ^:public conn-manager-clustered
    "Returns a new connection manager for Redis Cluster, with one pool of
    connections shared between the cluster's nodes:

      (def my-spec (cluster-spec [[\"127.0.0.1\" 7000] [\"127.0.0.1\" 7001]]))
      (def my-mgr
        (conn-manager-clustered
          {:conn-opts {:server {:cluster-spec my-spec}}}))

    Options:
      - `:conn-opts` - See `default-conn-opts`. Its `:server` must have a
          `:cluster-spec`, and may have `:cluster-opts` (see
          `default-cluster-opts`).
      - `:pool-opts` - See `default-cluster-pool-opts`.
      - `:mgr-name`  - Optional name for logs and stats, generated if omitted.
      - `:on-push`   - Optional `(fn [push-event])` for RESP3 push messages.
          Pushes are read only while a connection is in use, so this isn't
          suitable for client-side caching.
      - `:push-queue-capacity` - Max pushes waiting for `:on-push`
          (default 1024). Extra pushes are dropped and reported to
          `:on-push-error`.

    Deref for status and stats. Close with `conn-manager-close!` or
    `java.io.Closeable`.

    See also `cluster-spec`, `with-cluster-target`, `conn-manager`."
    ^ConnManagerClustered [opts]
    (let [opts (validate-manager-opts! :clustered
                 #{:conn-opts :mgr-name :pool-opts :on-push :push-queue-capacity} opts)
          {:keys [conn-opts pool-opts mgr-name], push-fn :on-push} opts
          push-queue-capacity (get opts :push-queue-capacity default-push-queue-capacity)
          conn-opts      (opts/parse-conn-opts :redis conn-opts)
          cluster-server (or (opts/get-cluster-server conn-opts)
                           (truss/ex-info! "[Carmine] Cluster manager requires Cluster server options"
                             {:eid :carmine.cluster/server-required
                              :conn-opts (utils/redact-secrets conn-opts)}))
          mgr-name (let [v (get opts :mgr-name ::auto)]
                     (if (identical? v ::auto) (next-mgr-name!) v))
          mgr-opts (not-empty (enc/assoc-some (dissoc opts :conn-opts) :mgr-name mgr-name))
          mgr_       (volatile! nil)
          active_    (enc/latom {})
          closed?_   (enc/latom false)
          close-data_ (atom nil)
          clear-data_ (atom nil)
          clear-lock  (Object.)
          generation* (AtomicLong. 0)
          n-failed*  (AtomicLong. 0)
          n-cleared* (AtomicLong. 0)
          push-dispatcher (new-push-dispatcher mgr-name push-fn push-queue-capacity)
          return-validator_ (volatile! nil)
          pool
          (let [created-pool_ (volatile! nil)]
            (try
              (let [pool-opts (enc/nested-merge core/default-cluster-pool-opts pool-opts)
                    ready-check-after-idle-ms (get pool-opts :ready-check-after-idle-ms)
                    factory
                    (reify KeyedPooledObjectFactory
                      (activateObject  [_ _addr _po] nil)
                      (passivateObject [_ _addr _po] nil)
                      (validateObject  [_ addr po]
                        (pooled-conn-ready? po ready-check-after-idle-ms
                          (when-let [mgr @mgr_] (mgr-push-fn mgr addr))))
                      (destroyObject   [_ _addr po]
                        (conn-close! (.getObject po) (or *mgr-close-data* @close-data_)))
                      (makeObject [_ addr]
                        (let [t0 (System/currentTimeMillis)
                              node-opts (assoc conn-opts :server addr)]
                          (try
                            (DefaultPooledObject.
                              (enc/binding* [*new-conn-generation* (.get generation*)]
                                (new-conn node-opts)))
                            (catch Throwable t
                              (.getAndIncrement n-failed*)
                              (throw-mgr-borrow-error! @mgr_ node-opts t0 t))))))
                    pool (GenericKeyedObjectPool. factory)]
                (vreset! created-pool_ pool)
                (opts/set-pool-opts! pool pool-opts)
                (when (.getTestOnReturn pool)
                  ;; Validate in `clustered-return!` instead
                  (.setTestOnReturn pool false)
                  (vreset! return-validator_
                    (fn [conn]
                      (conn-ready? conn true
                        (when-let [mgr @mgr_] (mgr-push-fn mgr (conn-addr conn)))))))
                pool)
              (catch Throwable t
                (close-construction-resources! @created-pool_ push-dispatcher t))))
          mgr  (vreset! mgr_
                 (ConnManagerClustered. mgr-opts conn-opts cluster-server pool
                   active_ closed?_ close-data_ clear-data_ clear-lock
                   generation* n-failed* n-cleared* push-dispatcher
                   @return-validator_))]
      mgr)))

(defn ^:public conn-manager
  "Returns a new connection manager that keeps a pool of reusable connections.
  This is the manager to use in most cases:

    (def my-mgr (conn-manager {:conn-opts {:server [\"127.0.0.1\" 6379]}}))
    (wcar my-mgr (ping)) ; => \"PONG\"

  Options:
    - `:conn-opts` - See `default-conn-opts`.
    - `:pool-opts` - See `default-pool-opts`.
    - `:mgr-name`  - Optional name for logs and stats, generated if omitted.
    - `:on-push`   - Optional `(fn [push-event])` for RESP3 push messages.
        Pushes are read only while a connection is in use, so this isn't
        suitable for client-side caching.
    - `:push-queue-capacity` - Max pushes waiting for `:on-push`
        (default 1024). Extra pushes are dropped and reported to
        `:on-push-error`.

  When the `:server` is a Cluster (see `cluster-spec`), returns a Cluster
  manager as `conn-manager-clustered` does, with `:pool-opts` as
  `default-cluster-pool-opts`.

  Deref for status and stats. Close with `conn-manager-close!` or
  `java.io.Closeable`.

  See also `conn-manager-unpooled`, `wcar`."
  (^java.io.Closeable [] (conn-manager {}))
  (^java.io.Closeable [opts]
   (let [server
         (get-in (when (map? opts) opts) [:conn-opts :server]
           (get core/default-conn-opts :server))]
     (if (and (map? server) (get server :cluster-spec))
       (conn-manager-clustered opts)
       (conn-manager-pooled    opts)))))

(comment
  (let [m1 (conn-manager-unpooled {})
        m2 (conn-manager-pooled   {})]
    (enc/qb 1e3 ; [80.49 19.06], m1 port limited
      (mgr-borrow! m1 (fn [c in out] #_(conn-ready? c)))
      (mgr-borrow! m2 (fn [c in out] #_(conn-ready? c))))))

;;;; Print methods

(do
  (enc/def-print-impl [x Conn]                (str "#" x))
  (enc/def-print-impl [x ConnManagerUnpooled] (str "#" x))
  (enc/def-print-impl [x ConnManagerPooled]   (str "#" x))
  (enc/def-print-impl [x ConnManagerClustered] (str "#" x)))
