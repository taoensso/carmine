(ns ^:no-doc taoensso.carmine-v4.conns
  "Private ns, implementation detail.
  Carmine connection handling code."
  (:refer-clojure :exclude [binding])
  (:require
   [taoensso.encore :as enc :refer [have have? binding]]
   [taoensso.carmine-v4.utils :as utils]
   [taoensso.carmine-v4.resp  :as resp]
   [taoensso.carmine-v4.opts  :as opts])

  (:import
   [java.net Socket]
   [java.io DataInputStream BufferedOutputStream]
   [org.apache.commons.pool2 PooledObjectFactory]
   [org.apache.commons.pool2.impl GenericObjectPool DefaultPooledObject]
   [java.util.concurrent.atomic AtomicLong]))

(comment (remove-ns 'taoensso.carmine-v4.conns))

(enc/declare-remote
            taoensso.carmine-v4.sentinel/resolve-addr!
            taoensso.carmine-v4.sentinel/resolved-addr?
  ^:dynamic taoensso.carmine-v4.sentinel/*mgr-cbs*
  ^:dynamic taoensso.carmine-v4/*conn-cbs*
            taoensso.carmine-v4/default-pool-opts)

(alias 'core     'taoensso.carmine-v4)
(alias 'sentinel 'taoensso.carmine-v4.sentinel)

(defmacro ^:private debug! [& body] (when #_true false `(enc/println ~@body)))

;;;; Connections

(defprotocol ^:private IConn
  "Internal protocol, not for public use or extension."

  (conn-open?     [conn] "Returns true iff `Conn` is open.")
  (conn-resolved? [conn use-cache?]
   "Returns true iff `Conn` doesn't use Sentinel for server address resolution,
   or if address agrees with current (possibly cached) resolution.")

  (conn-ready? [conn]
   "Returns true iff `Conn` is open and healthy (address agrees with current
   Sentinel resolution cache, and test PING successful).")

  (conn-init! [conn]
   "Initializes `Conn` auth, protocol, etc. Returns true on success, or throws.")

  (conn-close! [conn data]
   "Closes `Conn` and returns true iff successful.
   `data` is an arb map to include in errors, and to provide to registered callbacks."))

(let [idx (java.util.concurrent.atomic.AtomicLong. 0)]
  (defn- next-client-name! [_conn-opts]
    (str "carmine:" (.incrementAndGet idx))))

(comment (next-client-name! nil))

(deftype ^:private Conn
  [^Socket socket host port conn-opts ^DataInputStream in ^BufferedOutputStream out open?_]

  java.io.Closeable (close [this] (conn-close! this {:via 'java.io.Closeable}))
  Object
  (toString [this]
    ;; "taoensso.carmine.Conn[127.0.0.1:6379 open 0x7b9f6831]"
    (str "taoensso.carmine.Conn[" host ":" port " "
      (if (open?_) "open" "closed") " "
      (enc/ident-hex-str this) "]"))

  clojure.lang.IDeref
  (deref [this]
    {:socket    socket
     :host      host
     :port      port
     :conn-opts conn-opts
     :in        in
     :out       out
     :open?     (open?_)})

  IConn
  (conn-open?     [this] (open?_))
  (conn-resolved? [this use-cache?]
    (if-let [{:keys [sentinel-spec master-name sentinel-opts]} (opts/get-sentinel-server conn-opts)]
      (sentinel/resolved-addr? sentinel-spec master-name sentinel-opts [host port] use-cache?)
      true))

  (conn-ready? [this]
    (if-not (open?_)
      false
      (let [t0     (System/currentTimeMillis)
            error_ (volatile! nil)
            pass?
            (and
              (if (conn-resolved? this :use-cache)
                true
                (do
                  (vreset! error_ (ex-info "`Conn` incorrectly resolved" {}))
                  false))

              (let [current-timeout-ms (.getSoTimeout socket)
                    ready-timeout-ms   (or (utils/get-at conn-opts :socket-opts :ready-timeout-ms) 0)]

                (.setSoTimeout socket (int ready-timeout-ms))
                (if-let [reply
                         (try
                           ;; Nb assume any necessary auth/init already done, otherwise
                           ;; will correctly identify connection as unready
                           (resp/basic-ping! in out)
                           (catch Throwable  t (vreset! error_ t) nil)
                           (finally (.setSoTimeout socket current-timeout-ms)))]

                  (let [;; Ref. <https://github.com/redis/redis/issues/420>
                        ready? (or (= reply "PONG") (= reply ["ping" ""]))]

                    (debug! :conn-ready? ready?)
                    (if ready?
                      true
                      (do
                        (vreset! error_
                          (ex-info "Unexpected PING reply"
                            {:reply (enc/typed-val reply)}))
                        false)))
                  false)))

            elapsed-ms (- (System/currentTimeMillis) t0)]

        (if pass?
          true
          (do
            (utils/cb-notify!
              (get core/*conn-cbs*         :on-conn-error)
              (utils/get-at conn-opts :cbs :on-conn-error)
              (delay
                {:cbid       :on-conn-error
                 :host       host
                 :port       port
                 :conn       this
                 :conn-opts  conn-opts
                 :via        'conn-ready?
                 :cause      @error_
                 :elapsed-ms elapsed-ms}))
            false)))))

  (conn-init! [this]
    (if-not (open?_)
      false
      (enc/when-let
        [init-opts (not-empty (get conn-opts :init))
         reqs
         (not-empty
           (enc/cond
             (contains? init-opts :commands)
             (get       init-opts :commands) ; Complete override

             :let [{:keys [auth #_client-name select-db resp3?]} init-opts
                   {:keys [username password]} auth
                   username (or username "default")

                   client-name
                   (let [v (get init-opts :client-name ::auto)]
                     (enc/cond
                       (identical? v ::auto) (next-client-name! conn-opts)
                       (fn?        v)        (v                 conn-opts)
                       :else       v))]

             resp3?
             (let [auth-req
                   (if password
                     ["HELLO" 3 "AUTH" username password "SETNAME" client-name]
                     ["HELLO" 3                          "SETNAME" client-name])]

               (if select-db
                 [auth-req ["SELECT" select-db]]
                 [auth-req]))

             :else
             (let [reqs []
                   reqs (if password    (conj reqs ["AUTH" username password]))
                   reqs (if client-name (conj reqs ["CLIENT" "SETNAME" client-name]))
                   reqs (if select-db   (conj reqs ["SELECT" select-db]))]
               reqs)))]

        (let [conn-error_ (volatile! nil)
              t0 (System/currentTimeMillis)
              replies
              (try
                (resp/with-replies in out :natural-reads :as-vec
                  (fn [] (run! resp/rcall* reqs)))

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
                          {:request req
                           :reply   reply}))
                      [] reqs replies))]

              (utils/cb-notify-and-throw!    :on-conn-error
                (get core/*conn-cbs*         :on-conn-error)
                (utils/get-at conn-opts :cbs :on-conn-error)
                (ex-info "[Carmine] Error initializing connection"
                  {:eid :carmine.conns/conn-init-error
                   :host       host
                   :port       port
                   :conn       this
                   :conn-opts  conn-opts
                   :replies    reqs->replies
                   :elapsed-ms elapsed-ms}
                  @conn-error_))))))))

  (conn-close! [this data]
    (debug! :conn-close! data)
    (when (compare-and-set! open?_ true false)
      (let [t0         (System/currentTimeMillis)
            closed?    (enc/catching (do (.close socket) true))
            elapsed-ms (- (System/currentTimeMillis) t0)]

        (utils/cb-notify!
          (get core/*conn-cbs*         :on-conn-close)
          (utils/get-at conn-opts :cbs :on-conn-close)
          (delay
            {:cbid       :on-conn-close
             :host       host
             :port       port
             :conn       this
             :conn-opts  conn-opts
             :data       data
             :elapsed-ms elapsed-ms
             :closed?    closed?}))
        true))))

(defn- conn? [x] (instance? Conn x))

(let [factory_ (delay (javax.net.ssl.SSLSocketFactory/getDefault))]
  (defn- new-ssl-socket
    "Given an existing connected but unencrypted `java.net.Socket`, returns a
    connected and SSL-encrypted `java.net.Socket` using (SSLSocketFactory/getDefault)."
    [^Socket socket ^String host port]
    (.createSocket ^javax.net.ssl.SSLSocketFactory @factory_
      socket host (int port) true)))

(defn- new-socket ^Socket [conn-opts socket-opts host port]
  (let [socket
        (doto (Socket.)
          (.setTcpNoDelay   true)
          (.setKeepAlive    true)
          (.setReuseAddress true))

        {:keys [connect-timeout-ms ssl]
         :or ; Defaults relevant only for REPL/tests
         {connect-timeout-ms 2000}} socket-opts

        socket-opts (dissoc socket-opts :connect-timeout-ms :ssl)

        ^Socket socket
        (if socket-opts
          (opts/set-socket-opts! socket socket-opts)
          (do                    socket))]

    (.connect socket
      (java.net.InetSocketAddress. ^String host (int port))
      (int (or connect-timeout-ms 0)))

    (if ssl
      (if (fn? ssl)
        (ssl  conn-opts socket host port) ; Custom ssl-fn
        (new-ssl-socket socket host port))

      socket)))

(comment (.close (new-socket nil {:ssl true :connect-timeout-ms 2000} "127.0.0.1" 6379)))

(defn- new-conn
  "Low-level implementation detail.
  Returns a new `Conn` for given `conn-opts` with support for Sentinel resolution."
  (^Conn [conn-opts]
   (let [t0 (System/currentTimeMillis)
         {:keys [server]} conn-opts]

     (enc/cond
       (vector? server) (let [[host port] server] (new-conn conn-opts t0 nil host port))
       (map?    server) ; As `opts/get-sentinel-server`
       (let [{:keys [master-name sentinel-spec sentinel-opts]} server
             [host port]
             (sentinel/resolve-addr! ; May trigger `:on-changed-master` cb
               sentinel-spec master-name
               sentinel-opts (not :use-cache))]

         (new-conn conn-opts t0 master-name host port))

       (throw ; Shouldn't be possible after validation
         (ex-info "[Carmine] Unexpected `:server` type"
           {:server (enc/typed-val server)})))))

  (^Conn [conn-opts t0 master-name host port]
   (let [host (have string? host)
         port (enc/as-int   port)

         {:keys [#_server socket-opts buffer-opts]} conn-opts
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
             in     (-> socket .getInputStream  (java.io.BufferedInputStream.  init-size-in) java.io.DataInputStream.)
             out    (-> socket .getOutputStream (java.io.BufferedOutputStream. init-size-out))
             conn   (Conn. socket host port conn-opts in out (enc/latom true))]

         (conn-init! conn)
         (do         conn))

       (catch Throwable t
         (utils/cb-notify-and-throw!    :on-conn-error
           (get core/*conn-cbs*         :on-conn-error)
           (utils/get-at conn-opts :cbs :on-conn-error)
           (ex-info "[Carmine] Error creating new connection"
             {:eid :carmine.conns/new-conn-error
              :host        host
              :port        port
              :master-name master-name
              :conn-opts   conn-opts
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
  "For internal use only."
  ([conn-opts                       f] (with-conn (new-conn conn-opts) f))
  ([conn-opts host port master-name f]
   (with-conn
     (new-conn conn-opts (System/currentTimeMillis) master-name host port)
     f)))

(comment (with-new-conn {} "127.0.0.1" 6379 nil (fn [conn in out] (conn-ready? conn))))

;;;; Connection managers

(defprotocol ^:private IConnManager
  "Internal protocol, not currently for public use or extension."

  (^:public mgr-ready? [mgr]
   "Returns true iff `ConnManager` is ready for borrowing (not closed, etc.).")

  (mgr-clear! [mgr timeout-ms]
   "Instructs `ConnManager` to clear currently pooled connections, destroying
   idle or returned `Conns`.

   Blocks up to `timeout-ms` (nil => no limit) to await the return of active conns
   before forcibly interrupting any conns still active after wait.

   Returns true iff clearing was performed without forced interruption.
   Automatically called when Redis master changes due to Sentinel failover.")

  (mgr-borrow! [mgr f]
   "Borrows a connection and calls
   (f <Conn> <java.io.DataInputStream> <java.io.BufferedOutputStream>),
   returning the result.

   Returns or invalidates the borrowed connection when done.")

  (^:public mgr-close! [mgr timeout-ms data]
   "Initiates a permanent shutdown of the `ConnManager`:
     - Stops accepting new borrow requests.
     - Destroys any idle or returned `Conns`.

     - Blocks up to `timeout-ms` (nil => no limit) to await the return of active conns
       before forcibly interrupting any conns still active after wait.

       NB interruption can be dangerous to data integrity: pipelines may be interrupted
       mid-execution, etc. Use a non-nil timeout only if you understand the risks!"))

(defn ^:public conn-manager?
  "Returns true iff given argument satisfies the `IConnManager`
  protocol, and so can be used as a Carmine connection manager."
  [x]
  (or
    #_(enc/satisfies? IConnManager x)
    (satisfies?       IConnManager x)))

(defn- drain-conns!
  "Blocks up to `timeout-ms` (nil => no limit) to await the closing of given
  `Conn`s before forcibly interrupting any still open after timeout.
  Returns true iff timeout wasn't triggered."
  [conns timeout-ms close-data]
  (cond
    (empty? conns)                           true
    (and timeout-ms (neg? ^long timeout-ms)) nil ; Undocumented
    :else
    (let [timeout-udt (when timeout-ms (+ (System/currentTimeMillis) ^long timeout-ms))]
      (loop  [conns conns]
        (let [conns (into #{} (filter #(conn-open? %)) conns)] ; Open conns
          (cond
            (empty? conns) true

            (when timeout-udt (>= (System/currentTimeMillis) ^long timeout-udt))
            (do ; Give up waiting
              (run! #(enc/catching (conn-close! % close-data)) conns)
              false)

            :else (do (Thread/sleep 100) (recur conns))))))))

(comment
  (let [c1 (new-conn {} 0 nil "127.0.0.1" 6379)]
    (future (Thread/sleep 200) (conn-close! c1 {}))
    (drain-conns! #{c1} 100 {})))

(defn- throw-mgr-closed! [mgr] (throw (ex-info "[Carmine] Cannot borrow from closed `ConnManager`" {:mgr mgr})))
(defn- throw-mgr-borrow-error! [mgr conn-opts t0 t]
  (throw
    (ex-info "[Carmine] Error borrowing connection from `ConnManager`"
      {:eid :carmine.conns/borrow-conn-error
       :mgr        mgr
       :conn-opts  conn-opts
       :elapsed-ms (when t0 (- (System/currentTimeMillis) ^long t0))}
      t)))

(deftype ConnManagerUnpooled
  [mgr-opts conn-opts closed?_ active-conns_
   ^AtomicLong n-created* ^AtomicLong n-failed*]

  java.io.Closeable (close [this] (mgr-close! this nil {:mgr this, :via 'java.io.Closeable}))
  Object
  (toString [this]
    ;; "taoensso.carmine.ConnManagerUnpooled[ready 0x7b9f6831]"
    (let [status (if (closed?_) "closed" "ready")
          id     (or (get mgr-opts :mgr-name) (enc/ident-hex-str this))]
      (str "taoensso.carmine.ConnManagerUnpooled[" status " " id "]")))

  clojure.lang.IDeref
  (deref [_]
    {:ready?    (not (closed?_))
     :mgr-opts  mgr-opts
     :conn-opts conn-opts
     :stats
     {:counts
      {:active  (count @active-conns_)
       :created (.get n-created*)
       :failed  (.get n-failed*)}}})

  IConnManager
  (mgr-ready?  [_] (not (closed?_)))
  (mgr-clear!  [_ timeout-ms] nil) ; No-op
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
        (active-conns_ #(conj % conn))
        (try
          (let [result (f conn (.-in conn) (.-out conn))]
            (conn-close! conn {:mgr this, :via 'mgr-borrow!})
            result)

          (catch Throwable t
            (conn-close! conn {:mgr this, :via 'mgr-borrow!, :cause t})
            (throw t))

          (finally (active-conns_ #(disj % conn)))))))

  (mgr-close! [this timeout-ms data]
    (debug! :unpooled/close! timeout-ms data)
    (when (compare-and-set! closed?_ false true)
      (drain-conns! @active-conns_ timeout-ms
        (enc/fast-merge {:mgr this, :via 'mgr-close!, :timeout-ms timeout-ms} data))
      true)))

(let [idx (java.util.concurrent.atomic.AtomicLong. 0)
      next-mgr-name! (fn [] (str "unpooled:" (.incrementAndGet idx)))]

  (defn ^:public conn-manager-unpooled
    "Returns a new stateful unpooled `ConnManager`.
    In most cases you should prefer `conn-manager-pooled` instead.

    `ConnManager` API:
      - Deref for status, stats, etc.
      - Close with `conn-manager-close!` or `java.io.Closeable`."
    ^ConnManagerUnpooled [{:as opts :keys [conn-opts mgr-name]}]
    (let [conn-opts (opts/parse-conn-opts false conn-opts)
          mgr-name  (let [v (get opts :mgr-name ::auto)] (if (identical? v ::auto) (next-mgr-name!) v))
          mgr-opts
          (not-empty
            (enc/assoc-some (dissoc opts :conn-opts)
              :mgr-name mgr-name))]

      (ConnManagerUnpooled. mgr-opts conn-opts
        (enc/latom false)
        (enc/latom #{})
        (AtomicLong. 0)
        (AtomicLong. 0)))))

(def ^:private ^:dynamic *mgr-close-data* nil)

(deftype ConnManagerPooled
  ;; Ref. `org.apache.commons.pool2.impl.GenericObjectPool`,
  ;;      `org.apache.commons.pool2.PooledObjectFactory`
  [mgr-opts conn-opts ^GenericObjectPool pool active-conns_ closed?_
   ^AtomicLong n-failed* ^AtomicLong n-cleared*]

  java.io.Closeable (close [this] (mgr-close! this nil {:mgr this, :via 'java.io.Closeable}))
  Object
  (toString [this]
    ;; "taoensso.carmine.ConnManagerPooled[ready 0x7b9f6831]"
    (let [status (if (closed?_) "closed" "ready")
          id     (or (get mgr-opts :mgr-name) (enc/ident-hex-str this))]
      (str "taoensso.carmine.ConnManagerPooled[" status " " id "]")))
  
  clojure.lang.IDeref
  (deref [_]
    {:ready?    (not (closed?_))
     :mgr-opts  mgr-opts
     :conn-opts conn-opts
     :stats
     {:mean-borrow-time (.getMaxBorrowWaitTimeMillis  pool)
      :max-borrow-time  (.getMeanBorrowWaitTimeMillis pool)

      :mean-idle-time   (.getMeanIdleTimeMillis       pool)
      :mean-active-time (.getMeanActiveTimeMillis     pool)

      :counts
      {:created   (.getCreatedCount  pool)
       :borrowed  (.getBorrowedCount pool)
       :returned  (.getReturnedCount pool)
       :destroyed
       {:total                (.getDestroyedCount                   pool)
        :by-borrow-validation (.getDestroyedByBorrowValidationCount pool)
        :by-eviction          (.getDestroyedByEvictorCount          pool)}

       :active  (.getNumActive  pool)
       :waiting (.getNumWaiters pool)
       :idle    (.getNumIdle    pool)
       :failed  (.get           n-failed*)
       :cleared (.get           n-cleared*)}}})

  IConnManager
  (mgr-ready? [_] (and (not (closed?_)) (not (.isClosed pool))))
  (mgr-clear! [this timeout-ms]
    (debug! :pooled/clear!)
    (when-not (closed?_)
      (.getAndIncrement n-cleared*)
      (let [old-conns (into #{} (remove #(conn-resolved? % :use-cache)) (active-conns_))]
        (.clear pool) ; 1. Clear idle conns

        ;; 2. Drain old active conns
        (drain-conns! old-conns timeout-ms
          {:mgr this, :via 'mgr-clear!, :timeout-ms timeout-ms})

        ;; 3. No need to specially invalidate returned conns since all
        ;;    returned conns are anyway always tested for `conn-resolved?`
        )))

  (mgr-borrow! [this f]
    (debug! :pooled/borrow!)
    (if (closed?_)
      (throw-mgr-closed! this)
      (let [t0 (System/currentTimeMillis)
            ^Conn conn (.borrowObject pool)]
        (active-conns_ #(conj % conn))
        (try
          (let [result (f conn (.-in conn) (.-out conn))]

            ;; Always test `conn-resolved?` before returning, it's cheap
            ;; and needed for correct clearing behaviour after master change
            (if (conn-resolved? conn :use-cache)
              (do                                                        (.returnObject     pool conn))
              (binding [*mgr-close-data* {:mgr this, :via 'mgr-borrow!}] (.invalidateObject pool conn)))

            result)

          (catch Throwable t
            ;; We're conservative here and invalidate conn for ANY cause since even if
            ;; conn is intact, it may be in an unexpected state
            (binding [*mgr-close-data* {:mgr this, :via 'mgr-borrow!, :cause t}]
              (.invalidateObject pool conn))
            (throw t))

          (finally (active-conns_ #(disj % conn)))))))

  (mgr-close! [this timeout-ms data]
    (debug! :pooled/close! timeout-ms data)
    (when (compare-and-set! closed?_ false true)
      (drain-conns! @active-conns_ timeout-ms
        (enc/fast-merge {:mgr this, :via 'mgr-close!, :timeout-ms timeout-ms} data))
      (.close pool)
      true)))

(let [idx (java.util.concurrent.atomic.AtomicLong. 0)
      next-mgr-name! (fn [] (str "pooled:" (.incrementAndGet idx)))]

  (defn ^:public conn-manager-pooled
    "Returns a new stateful pooled `ConnManager` backed by Apache Commons Pool 2.

    This is a solid and highly configurable general-purpose connection
    manager that should generally be your default choice unless you have very
    specific/unusual requirements.

    `ConnManager` API:
      - Deref for status, stats, etc.
      - Close with `conn-manager-close!` or `java.io.Closeable`.

    Options:
      `:pool-opts`
        Options for manager's underlying `org.apache.commons.pool2.impl.GenericObjectPool`.
        For more info, see `default-pool-opts` or the `GenericObjectPool` Javadoc."

    ^ConnManagerPooled [{:as opts :keys [conn-opts pool-opts mgr-name]}]
    (let [conn-opts (opts/parse-conn-opts false conn-opts)
          mgr-name  (let [v (get opts :mgr-name ::auto)] (if (identical? v ::auto) (next-mgr-name!) v))
          mgr-opts
          (not-empty
            (enc/assoc-some (dissoc opts :conn-opts)
              :mgr-name mgr-name))

          mgr_       (volatile! nil)
          n-failed*  (AtomicLong. 0)
          n-cleared* (AtomicLong. 0)
          pool
          (let [sentinel-mgr-cbs
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
                  (validateObject  [_ po] (conn-ready? (.getObject po)))
                  (destroyObject   [_ po] (conn-close! (.getObject po) *mgr-close-data*))
                  (makeObject      [_]
                    (let [t0 (System/currentTimeMillis)]
                      (try
                        (if-let [cbs sentinel-mgr-cbs]
                          (binding [sentinel/*mgr-cbs* cbs] (DefaultPooledObject. (new-conn conn-opts)))
                          (do                               (DefaultPooledObject. (new-conn conn-opts))))

                        (catch Throwable t
                          (.getAndIncrement n-failed*)
                          (throw-mgr-borrow-error! @mgr_ conn-opts t0 t))))))

                pool-opts (utils/merge-opts core/default-pool-opts pool-opts)
                pool      (GenericObjectPool. factory)]

            (opts/set-pool-opts! pool pool-opts)
            (do                  pool))

          mgr
          (vreset! mgr_
            (ConnManagerPooled. mgr-opts conn-opts pool
              (enc/latom #{})
              (enc/latom false)
              n-failed*
              n-cleared*))]

      (.preparePool pool) ; Ensure that configured min idle instances ready
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
  (enc/def-print-impl [x ConnManagerPooled]   (str "#" x)))
