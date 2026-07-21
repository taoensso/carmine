(ns ^:no-doc taoensso.carmine-v4.conns
  "Private connection implementation."
  (:require
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
  ^:dynamic taoensso.carmine-v4/*conn-cbs*
            taoensso.carmine-v4/default-pool-opts
            taoensso.carmine-v4/default-cluster-pool-opts)

(alias 'core     'taoensso.carmine-v4)
(alias 'sentinel 'taoensso.carmine-v4.sentinel)

(defmacro ^:private debug! [& body] (when #_true false `(enc/println ~@body)))

;;;; Manager options and RESP3 push dispatch

(def ^:private default-push-queue-capacity 1024)

(defn- validate-manager-opts!
  [manager-kind allowed-keys opts]
  (when-not (map? opts)
    (truss/ex-info! "[Carmine] Connection manager options must be a map"
      {:eid :carmine.conns/invalid-manager-opts
       :manager-kind manager-kind
       :opts (enc/typed-val opts)}))
  (doseq [k (keys opts)]
    (when-not (and (keyword? k)
                (or (namespace k) (contains? allowed-keys k)))
      (truss/ex-info! "[Carmine] Unknown connection manager option"
        {:eid :carmine.conns/invalid-manager-opts
         :manager-kind manager-kind
         :option-key (enc/typed-val k)
         :allowed-keys allowed-keys})))
  (when-not (or (nil? (:push-fn opts)) (fn? (:push-fn opts)))
    (truss/ex-info! "[Carmine] Invalid connection manager push handler"
      {:eid :carmine.conns/invalid-manager-opts
       :manager-kind manager-kind
       :option-key :push-fn
       :option-value (enc/typed-val (:push-fn opts))
       :expected :nil-or-fn}))
  (when-not (let [capacity (get opts :push-queue-capacity default-push-queue-capacity)]
              (and (pos-int? capacity) (<= capacity Integer/MAX_VALUE)))
    (truss/ex-info! "[Carmine] Invalid connection manager push queue capacity"
      {:eid :carmine.conns/invalid-manager-opts
       :manager-kind manager-kind
       :option-key :push-queue-capacity
       :option-value (enc/typed-val (:push-queue-capacity opts))
       :expected :positive-int32}))
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
  [global-cb conn-cb push-event cause]
  (utils/cb-notify!
    global-cb conn-cb
    (delay
      {:cbid :on-push-error
       :eid (if (instance? RejectedExecutionException cause)
              :carmine.push/dispatch-rejected
              :carmine.push/handler-error)
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
         {:manager mgr
          :server-addr (vec server-addr)
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
                (notify-push-error! global-cb conn-cb push-event t))
              (finally
                (.incrementAndGet completed*)))))]
    (.incrementAndGet received*)
    (try
      (.execute ^ExecutorService (.-executor dispatcher) task)
      (catch RejectedExecutionException t
        (.incrementAndGet rejected*)
        ;; A close racing with reply consumption may reject a final event.
        ;; Keep that failure isolated from RESP framing and observable.
        (notify-push-error! global-cb conn-cb push-event t)))
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

(def ^:private ^:dynamic *deferred-conn-close-cbs*
  "Optional volatile of deferred connection-close callback tasks. Bound while a
  manager holds `clear-lock` around a pool operation that may destroy
  connections, keeping user callbacks outside the lock."
  nil)

(defn- with-deferred-close-cbs*
  "Runs `thunk` with connection-close callback deferral. After `thunk` releases
  its locks, calls the deferred tasks in FIFO order. Protects each call
  independently. Reuses an outer deferral scope and does not flush it."
  [thunk]
  (if *deferred-conn-close-cbs*
    (thunk) ; Outer scope owns flushing
    (let [deferred_ (volatile! [])]
      (try
        (enc/binding* [*deferred-conn-close-cbs* deferred_] (thunk))
        (finally
          (run! (fn [notify!] (truss/catching (notify!))) @deferred_))))))

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
            (long timeout-ms) TimeUnit/MILLISECONDS)]
      (try
        (let [result (f)]
          (if (.compareAndSet settled* false true)
            result
            (case phase
              :init
              (truss/ex-info! "[Carmine] Connection initialization deadline elapsed"
                {:eid :carmine.conns/conn-init-deadline
                 :timeout-ms timeout-ms})

              :ready
              (truss/ex-info! "[Carmine] Connection readiness deadline elapsed"
                {:eid :carmine.conns/conn-ready-deadline
                 :timeout-ms timeout-ms}))))
        (finally (.cancel watchdog false))))))

(defn- valid-ping-reply?
  "Returns true for normal and subscribed-mode Redis PING replies."
  [reply]
  (or (= reply "PONG") (= reply ["pong" ""])))

(deftype ^:private Conn
  [^Socket socket host port conn-opts ^DataInputStream in ^BufferedOutputStream out open?_]

  java.io.Closeable (close [this] (conn-close! this {:via 'java.io.Closeable}))
  Object
  (toString [this]
    (enc/str-impl this "taoensso.carmine.Conn"
      {:host host, :port port, :open? (open?_)}))

  clojure.lang.IDeref
  (deref [this]
    {:socket    socket
     :host      host
     :port      port
     :conn-opts (utils/redact-secrets conn-opts)
     :in        in
     :out       out
     :open?     (open?_)})

  IConn
  (conn-open?     [this] (open?_))
  (conn-resolved? [this use-cache?]
    (if-let [{:keys [sentinel-spec master-name sentinel-opts]} (opts/get-sentinel-server conn-opts)]
      (sentinel/resolved-addr? sentinel-spec master-name sentinel-opts use-cache? [host port])
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
                      ;; (matching `:init-timeout-ms`); 0 => explicitly unbounded
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
                 :host       host
                 :port       port
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
                (resp/with-replies in out
                  {:natural-replies? true, :as-vec? true, :error-mode :return} nil
                  (fn [] (run! resp/rcmd* reqs))))

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
            (let [reqs->replies
                  (when replies
                    (enc/reduce-zip
                      (fn [acc req reply]
                        (conj acc
                          {:request (utils/redact-command req)
                           :reply
                           (if (contains? init-opts :commands)
                             :carmine/redacted
                             reply)}))
                      [] reqs replies))]

              (utils/cb-notify-and-throw!    :on-conn-error
                (get core/*conn-cbs*         :on-conn-error)
                (utils/get-at conn-opts :cbs :on-conn-error)
                (truss/ex-info "[Carmine] Error initializing connection"
                  {:eid :carmine.conns/conn-init-error
                   :via        'conn-init!
                   :host       host
                   :port       port
                   :conn       this
                   :conn-opts  (utils/redact-secrets conn-opts)
                   :replies    reqs->replies
                   :elapsed-ms elapsed-ms}
                  @conn-error_))))))))

  (conn-close! [this data]
    (debug! :conn-close! data)
    (when (compare-and-set! open?_ true false)
      (let [t0         (System/currentTimeMillis)
            closed?    (truss/catching (do (.close socket) true))
            elapsed-ms (- (System/currentTimeMillis) t0)
            notify!
            (fn []
              (utils/cb-notify!
                (get core/*conn-cbs*         :on-conn-close)
                (utils/get-at conn-opts :cbs :on-conn-close)
                (delay
                  (enc/assoc-when
                    {:cbid       :on-conn-close
                     :host       host
                     :port       port
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
  [conn] [(.-host ^Conn conn) (.-port ^Conn conn)])

(defn- pooled-conn-ping?
  [borrowed-count idle-time-ms ready-check-after-idle-ms]
  (or
    (nil? ready-check-after-idle-ms)
    (zero? ^long borrowed-count)
    (<= (long ready-check-after-idle-ms) ^long idle-time-ms)))

(defn- pooled-conn-ready?
  [po ready-check-after-idle-ms push-fn]
  (let [ping? (pooled-conn-ping?
                (.getBorrowedCount ^org.apache.commons.pool2.PooledObject po)
                (.getIdleTimeMillis ^org.apache.commons.pool2.PooledObject po)
                ready-check-after-idle-ms)]
    (conn-ready? (.getObject ^org.apache.commons.pool2.PooledObject po) ping? push-fn)))

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
               (sentinel/resolve-addr! ; May trigger `:on-changed-master` cb
                 sentinel-spec master-name
                 sentinel-opts (not :use-cache))]

           (new-conn conn-opts t0 master-name host port))

         (get server :cluster-spec)
         (truss/ex-info! "[Carmine] Cluster server options require `conn-manager-clustered`"
           {:eid :carmine.cluster/conn-not-supported
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
                 conn (Conn. socket host port conn-opts in out (enc/latom true))]
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
           (truss/ex-info "[Carmine] Error creating new connection"
             {:eid :carmine.conns/new-conn-error
              :via         'new-conn
              :host        host
              :port        port
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

(def ^:private manager-stats-schema-version 1)

(defn- manager-stats-base
  [manager-kind push-dispatcher connections counts timings nodes]
  {:schema-version manager-stats-schema-version
   :kind manager-kind
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
          {:server-addr (vec addr)
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
   Sentinel master change. See [[taoensso.carmine-v4/conn-manager-clear!]] for
   the public timeout and return contract.")

  (mgr-borrow! [mgr f]
   "Borrows a connection and calls
   `(f <Conn> <java.io.DataInputStream> <java.io.BufferedOutputStream>)`.
   Returns the result of `f`. Always releases the borrowed connection when done:
   an unpooled connection is closed; a pooled connection is returned when it
   remains reusable and current, otherwise invalidated. A Cluster manager does
   not support this operation and throws `:carmine.cluster/manager-required`;
   see [[mgr-borrow-addr!]].")

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
   [[taoensso.carmine-v4/conn-manager-close!]] for the public timeout,
   `data`, and return contract."))

(defn ^:public conn-manager?
  "Returns true iff the given `x` is a Carmine v4 connection manager."
  [x]
  (or
    #_(enc/satisfies? IConnManager x)
    (satisfies?       IConnManager x)))

(defn ^:public mgr-stats
  "Returns the stable, versioned statistics snapshot exposed by `mgr`.

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
      {:eid :carmine.conns/invalid-timeout-ms
       :via via
       :timeout-ms (enc/typed-val timeout-ms)
       :expected 'nil-or-nat-long})))

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

(defn- mgr-closed-result [graceful?]
  {:action :closed, :graceful? (boolean graceful?)})

(def ^:private mgr-already-closed-result {:action :already-closed})
(def ^:private mgr-clear-not-applicable-result {:action :not-applicable})
(defn- mgr-cleared-result [graceful?]
  {:action :cleared, :graceful? (boolean graceful?)})

(comment
  (let [c1 (new-conn {} 0 nil "127.0.0.1" 6379)]
    (future (Thread/sleep 200) (conn-close! c1 {}))
    (drain-conns! #{c1} 100 {})))

(defn- throw-mgr-closed! [mgr]
  (truss/ex-info! "[Carmine] Cannot borrow from closed `ConnManager`"
    {:eid :carmine.conns/manager-closed, :mgr mgr}))
(defn- throw-not-clustered! [mgr]
  (truss/ex-info! "[Carmine] Connection manager does not support Cluster address borrowing"
    {:eid :carmine.cluster/manager-required, :mgr mgr}))
(defn- throw-cluster-manager-required! [conn-opts]
  (truss/ex-info! "[Carmine] Cluster server options require `conn-manager-clustered`"
    {:eid :carmine.cluster/manager-required
     :conn-opts (utils/redact-secrets conn-opts)}))
(defn- throw-mgr-borrow-error! [mgr conn-opts t0 t]
  (when (instance? InterruptedException t)
    (.interrupt (Thread/currentThread)))
  (truss/ex-info! "[Carmine] Error borrowing connection from `ConnManager`"
    {:eid :carmine.conns/borrow-conn-error
     :mgr        mgr
     :conn-opts  (utils/redact-secrets conn-opts)
     :elapsed-ms (when t0 (- (System/currentTimeMillis) ^long t0))}
    t))

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
      (if (closed?_) mgr-already-closed-result mgr-clear-not-applicable-result)))
  (mgr-conn-opts [_] conn-opts)
  (mgr-push-fn [this server-addr]
    (when push-dispatcher
      #(dispatch-push! push-dispatcher this conn-opts server-addr false %)))
  (mgr-cluster-server [_] nil)
  (mgr-borrow-addr! [this _addr _f] (throw-not-clustered! this))
  (mgr-borrow! [this f]
    (debug! :unpooled/borrow!)
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
              (active-conns_ #(conj % conn))
              true))
          (do
            (conn-close! conn
              (or @close-data_
                {:mgr this, :via 'mgr-borrow!, :manager-closed? true}))
            (throw-mgr-closed! this))
          (try
            (let [result (f conn (.-in conn) (.-out conn))]
              (with-deferred-close-cbs*
                (fn []
                  (locking lifecycle-lock
                    (conn-close! conn
                      (or @close-data_ {:mgr this, :via 'mgr-borrow!})))))
              result)

            (catch Throwable t
              (with-deferred-close-cbs*
                (fn []
                  (locking lifecycle-lock
                    (conn-close! conn
                      (assoc
                        (or @close-data_ {:mgr this, :via 'mgr-borrow!})
                        :cause t)))))
              (when (instance? InterruptedException t)
                (.interrupt (Thread/currentThread)))
              (throw t))

            (finally (active-conns_ #(disj % conn))))))))

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
        (mgr-closed-result
          (drain-conns! active-conns timeout-ms close-data)))
      mgr-already-closed-result))))

(let [idx (java.util.concurrent.atomic.AtomicLong. 0)
      next-mgr-name! (fn [] (str "unpooled:" (.incrementAndGet idx)))]

  (defn ^:public conn-manager-unpooled
    "Returns a stateful unpooled `ConnManager`. In most cases you should prefer
    [[taoensso.carmine-v4/conn-manager]].

    Options:
      - `:conn-opts`: See [[default-conn-opts]].
      - `:mgr-name`: Optional diagnostic name. Omit it for an automatic name.
        Set it to nil for no name.
      - `:push-fn`: Optional unary function that receives RESP3 push events on
        the manager's private serial daemon executor.
      - `:push-queue-capacity`: Maximum queued push events (default 1024).
        Overflow is dropped and reported through `:on-push-error`.

    Connections are read only while borrowed, so `:push-fn` cannot provide
    timely Redis client-tracking invalidations or a client-side cache.

    `ConnManager` API:
      - Deref for status and statistics.
      - Test availability with [[taoensso.carmine-v4/conn-manager-open?]].
      - Close with [[conn-manager-close!]] or `java.io.Closeable`.

    Each borrow creates exactly one connection and closes it on return."
    (^ConnManagerUnpooled [] (conn-manager-unpooled {}))
    (^ConnManagerUnpooled [opts]
     (let [opts (validate-manager-opts! :unpooled
                  #{:conn-opts :mgr-name :push-fn :push-queue-capacity} opts)
           {:keys [conn-opts mgr-name push-fn]} opts
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

(deftype ConnManagerPooled
  ;; Ref. `org.apache.commons.pool2.impl.GenericObjectPool`,
  ;;      `org.apache.commons.pool2.PooledObjectFactory`
  [mgr-opts conn-opts ^GenericObjectPool pool active-conns_ closed?_ close-data_
   clear-data_ clear-lock ^AtomicLong generation*
   ^AtomicLong n-failed* ^AtomicLong n-cleared*
   push-dispatcher]

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
  (mgr-borrow-addr! [this _addr _f] (throw-not-clustered! this))
  (mgr-clear! [this timeout-ms]
    (debug! :pooled/clear!)
    (let [timeout-ms (drain-timeout-ms! 'mgr-clear! timeout-ms)
          not-performed (Object.)
          old-conns_ (volatile! not-performed)
          close-data {:mgr this, :via 'mgr-clear!, :timeout-ms timeout-ms}]
      (let [graceful?
            (try
              (with-deferred-close-cbs* ; Never run user cbs under clear-lock
                (fn []
                  (locking clear-lock
                    (when-not (closed?_)
                      (.getAndIncrement n-cleared*)
                      (reset! clear-data_ close-data)
                      (.incrementAndGet generation*) ; Odd => clear in progress
                      (vreset! old-conns_ (set (vals @active-conns_)))
                      (try
                        (enc/binding* [*mgr-close-data* close-data] (.clear pool))
                        (finally (.incrementAndGet generation*))))))) ; Even => stable
              (when-not (identical? @old-conns_ not-performed)
                (drain-conns! @old-conns_ timeout-ms close-data))
              (finally
                (when-not (identical? @old-conns_ not-performed)
                  (compare-and-set! clear-data_ close-data nil))))]
        (if (identical? @old-conns_ not-performed)
          mgr-already-closed-result
          (mgr-cleared-result graceful?)))))

  (mgr-borrow! [this f]
    (debug! :pooled/borrow!)
    ;; NB nested same-manager borrows are deliberately allowed: nested `wcar`
    ;; and `transact!`-in-`wcar` are supported (tested) patterns, with the
    ;; pool-exhaustion caveat documented at the call sites.
    (loop []
      (when (closed?_) (throw-mgr-closed! this))
      (let [t0 (System/currentTimeMillis)
            generation (.get generation*)
            ^Conn conn
            (try
              (enc/binding*
                [*mgr-close-data* {:mgr this, :via 'mgr-borrow!, :validation? true}]
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
            borrow-id (Object.)
            stable?
            (locking clear-lock
              (when (and (not (closed?_))
                      (even? (.get generation*))
                      (= generation (.get generation*)))
                (active-conns_ #(assoc % borrow-id conn))
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

          (let [conn-reusable?_ (volatile! true)]
            (try
              (let [result
                    (binding [write/*conn-reusable?_ conn-reusable?_]
                      (f conn (.-in conn) (.-out conn)))]
                ;; Lock-order invariant: fresh Sentinel resolution happens only
                ;; in pool validation, before registration. Return holds
                ;; `clear-lock` and therefore performs a cache-only address check.
                (with-deferred-close-cbs*
                  (fn []
                    (locking clear-lock
                      (if (and @conn-reusable?_
                            (= generation (.get generation*))
                            (not (closed?_))
                            (conn-open? conn)
                            (conn-resolved? conn :use-cache))
                        (.returnObject pool conn)
                        (enc/binding*
                          [*mgr-close-data* (or @clear-data_ @close-data_
                                              {:mgr this, :via 'mgr-borrow!})]
                          (.invalidateObject pool conn))))))
                result)

              (catch Throwable t
                (cleanup-borrowed-conn! t conn
                  {:mgr this, :via 'mgr-borrow!, :cause t}
                  (fn []
                    (if (and @conn-reusable?_ (instance? ReusableConnError t))
                      ;; The request context consumed every expected reply before
                      ;; raising a Redis reply error, so framing remains intact.
                      (with-deferred-close-cbs*
                        (fn []
                          (locking clear-lock
                            (if (and (= generation (.get generation*))
                                  (not (closed?_))
                                  (conn-open? conn)
                                  (conn-resolved? conn :use-cache))
                              (.returnObject pool conn)
                              (enc/binding*
                                [*mgr-close-data* (or @clear-data_ @close-data_
                                                    {:mgr this, :via 'mgr-borrow!})]
                                (.invalidateObject pool conn))))))
                      ;; Conservatively invalidate on application/IO failures since
                      ;; the connection may be left in an unexpected protocol state.
                      (enc/binding* [*mgr-close-data* {:mgr this, :via 'mgr-borrow!, :cause t}]
                        (.invalidateObject pool conn)))))
                (throw t))

              (finally (active-conns_ #(dissoc % borrow-id)))))))))

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
        (mgr-closed-result
          (drain-conns! active-conns timeout-ms close-data)))
      mgr-already-closed-result))))

(let [idx (java.util.concurrent.atomic.AtomicLong. 0)
      next-mgr-name! (fn [] (str "pooled:" (.incrementAndGet idx)))]

  (defn ^:public conn-manager-pooled
    "Returns a new stateful pooled `ConnManager` backed by Apache Commons Pool 2.

    This manager should generally be the default choice unless you have unusual
    requirements.

    `ConnManager` API:
      - Deref for status and stats; timing keys have an `-ms` suffix.
      - Close with [[conn-manager-close!]] or `java.io.Closeable`.
      - Clear stale pooled connections with [[conn-manager-clear!]].

    Options:
      - `:conn-opts`: See [[default-conn-opts]].
      - `:mgr-name`: Optional diagnostic name. Omit it for an automatic name.
        Set it to nil for no name.
      - `:push-fn`: Optional unary function that receives RESP3 push events on
        the manager's private serial daemon executor.
      - `:push-queue-capacity`: Maximum queued push events (default 1024).
        Overflow is dropped and reported through `:on-push-error`.
      - `:pool-opts`: Options for the underlying
        `org.apache.commons.pool2.impl.GenericObjectPool`; see
        [[default-pool-opts]] and the `GenericObjectPool` Javadoc.

    A pooled connection is read only while borrowed or validated, so `:push-fn`
    cannot provide timely Redis client-tracking invalidations or a client-side
    cache.

    The manager validates Sentinel-resolved addresses when it borrows or returns
    connections. It clears its stale connections when one of its own Sentinel
    resolutions observes a master change; other managers that share the same
    `SentinelSpec` rely on validation against the shared topology cache."

    (^ConnManagerPooled [] (conn-manager-pooled {}))
    (^ConnManagerPooled [opts]
     (let [opts (validate-manager-opts! :pooled
                 #{:conn-opts :mgr-name :pool-opts :push-fn :push-queue-capacity} opts)
          {:keys [conn-opts pool-opts mgr-name push-fn]} opts
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
          ^GenericObjectPool pool
          (let [created-pool_ (volatile! nil)]
            (try
              (let [pool-opts (opts/merge-pool-opts core/default-pool-opts pool-opts)
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
                                (mgr-push-fn mgr [(.-host conn) (.-port conn)]))
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
                            (if-let [cbs sentinel-mgr-cbs]
                              (enc/binding* [sentinel/*mgr-cbs* cbs] (DefaultPooledObject. (new-conn conn-opts)))
                              (do                                    (DefaultPooledObject. (new-conn conn-opts))))

                            (catch Throwable t
                              (.getAndIncrement n-failed*)
                              (throw-mgr-borrow-error! @mgr_ conn-opts t0 t))))))
                    pool (GenericObjectPool. factory)]

                (vreset! created-pool_ pool)
                (opts/set-pool-opts! pool pool-opts)
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
              push-dispatcher))]

      (try
        (.preparePool pool) ; Ensure that configured min idle instances ready
        (catch Throwable t
          (close-construction-resources! pool push-dispatcher t)))
      mgr))))

;;;; Cluster connection manager

(deftype ConnManagerClustered
  [mgr-opts conn-opts cluster-server ^GenericKeyedObjectPool pool
   active-conns_ closed?_ close-data_ clear-data_ clear-lock
   ^AtomicLong generation* ^AtomicLong n-failed* ^AtomicLong n-cleared*
   push-dispatcher]

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
  (mgr-borrow! [this _f] (throw-not-clustered! this))

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
              borrow-id (Object.)
              stable?
              (locking clear-lock
                (when (and (not (closed?_))
                        (even? (.get generation*))
                        (= generation (.get generation*)))
                  (active-conns_ #(assoc % borrow-id conn))
                  true))]
          (if-not stable?
            (let [close-data (or @clear-data_ @close-data_
                               {:mgr this, :via 'mgr-borrow-addr!})]
              (try
                (enc/binding* [*mgr-close-data* close-data]
                  (.invalidateObject pool addr conn))
                (catch Throwable _
                  (conn-close! conn close-data)))
              (if (closed?_) (throw-mgr-closed! this) (recur)))

            (let [conn-reusable?_ (volatile! true)]
              (try
                (let [result
                      (binding [write/*conn-reusable?_ conn-reusable?_]
                        (f conn (.-in conn) (.-out conn)))]
                  (with-deferred-close-cbs*
                    (fn []
                      (locking clear-lock
                        (if (and @conn-reusable?_
                              (= generation (.get generation*))
                              (not (closed?_)) (conn-open? conn))
                          (.returnObject pool addr conn)
                          (enc/binding*
                            [*mgr-close-data* (or @clear-data_ @close-data_
                                                {:mgr this, :via 'mgr-borrow-addr!})]
                            (.invalidateObject pool addr conn))))))
                  result)
                (catch Throwable t
                  (cleanup-borrowed-conn! t conn
                    {:mgr this, :via 'mgr-borrow-addr!, :cause t}
                    (fn []
                      (if (and @conn-reusable?_ (instance? ReusableConnError t))
                        (with-deferred-close-cbs*
                          (fn []
                            (locking clear-lock
                              (if (and (= generation (.get generation*))
                                    (not (closed?_)) (conn-open? conn))
                                (.returnObject pool addr conn)
                                (enc/binding*
                                  [*mgr-close-data* (or @clear-data_ @close-data_
                                                      {:mgr this, :via 'mgr-borrow-addr!})]
                                  (.invalidateObject pool addr conn))))))
                        (enc/binding* [*mgr-close-data* {:mgr this, :via 'mgr-borrow-addr!, :cause t}]
                          (.invalidateObject pool addr conn)))))
                  (throw t))
                (finally (active-conns_ #(dissoc % borrow-id))))))))))

  (mgr-clear! [this timeout-ms]
    (let [timeout-ms (drain-timeout-ms! 'mgr-clear! timeout-ms)
          not-performed (Object.)
          old-conns_ (volatile! not-performed)
          close-data {:mgr this, :via 'mgr-clear!, :timeout-ms timeout-ms}]
      (let [graceful?
            (try
              (with-deferred-close-cbs* ; Never run user cbs under clear-lock
                (fn []
                  (locking clear-lock
                    (when-not (closed?_)
                      (.getAndIncrement n-cleared*)
                      (reset! clear-data_ close-data)
                      (.incrementAndGet generation*) ; Odd => clear in progress
                      (vreset! old-conns_ (set (vals @active-conns_)))
                      (try
                        (enc/binding* [*mgr-close-data* close-data] (.clear pool))
                        (finally (.incrementAndGet generation*))))))) ; Even => stable
              (when-not (identical? @old-conns_ not-performed)
                (drain-conns! @old-conns_ timeout-ms close-data))
              (finally
                (when-not (identical? @old-conns_ not-performed)
                  (compare-and-set! clear-data_ close-data nil))))]
        (if (identical? @old-conns_ not-performed)
          mgr-already-closed-result
          (mgr-cleared-result graceful?)))))

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
        (mgr-closed-result
          (drain-conns! active-conns timeout-ms close-data)))
      mgr-already-closed-result))))

(let [idx (AtomicLong. 0)
      next-mgr-name! (fn [] (str "clustered:" (.incrementAndGet idx)))]
  (defn ^:public conn-manager-clustered
    "Returns a stateful Cluster connection manager with one keyed pool shared
    across node addresses.

    Options:
      - `:conn-opts`: Must contain `{:server {:cluster-spec spec, ...}}`.
      - `:mgr-name`: Optional diagnostic name.
      - `:push-fn`: Optional unary function that receives RESP3 push events on
        the manager's private serial daemon executor.
      - `:push-queue-capacity`: Maximum queued push events (default 1024).
        Overflow is dropped and reported through `:on-push-error`.
      - `:pool-opts`: Keyed Apache Commons Pool options; see
        [[default-cluster-pool-opts]].

    A pooled connection is read only while borrowed or validated, so `:push-fn`
    cannot provide timely Redis client-tracking invalidations or a client-side
    cache.

    Deref for status and statistics. Timing keys have an `-ms` suffix. Close
    with [[conn-manager-close!]] or `java.io.Closeable`. Clear with
    [[conn-manager-clear!]]. Use an ordinary manager for standalone Redis and
    Sentinel."
    ^ConnManagerClustered [opts]
    (let [opts (validate-manager-opts! :clustered
                 #{:conn-opts :mgr-name :pool-opts :push-fn :push-queue-capacity} opts)
          {:keys [conn-opts pool-opts mgr-name push-fn]} opts
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
                            (DefaultPooledObject. (new-conn node-opts))
                            (catch Throwable t
                              (.getAndIncrement n-failed*)
                              (throw-mgr-borrow-error! @mgr_ node-opts t0 t))))))
                    pool (GenericKeyedObjectPool. factory)]
                (vreset! created-pool_ pool)
                (opts/set-pool-opts! pool pool-opts)
                pool)
              (catch Throwable t
                (close-construction-resources! @created-pool_ push-dispatcher t))))
          mgr  (vreset! mgr_
                 (ConnManagerClustered. mgr-opts conn-opts cluster-server pool
                   active_ closed?_ close-data_ clear-data_ clear-lock
                   generation* n-failed* n-cleared* push-dispatcher))]
      mgr)))

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
