(ns taoensso.carmine-v4.conns
  "Carmine connection handling code.
  This ns contains mostly private implementation details,
  but also some low-level stuff for advanced users."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [have have? throws?]]
   [taoensso.carmine-v4.utils :as utils]
   [taoensso.carmine-v4.resp  :as resp]
   [taoensso.carmine-v4.opts  :as opts])

  (:import
   [java.net Socket]
   [java.io InputStream OutputStream]
   [org.apache.commons.pool2 KeyedPooledObjectFactory]
   [org.apache.commons.pool2.impl GenericKeyedObjectPool DefaultPooledObject]
   [java.util.concurrent.atomic AtomicLong]))

(comment
  (remove-ns      'taoensso.carmine-v4.conns)
  (test/run-tests 'taoensso.carmine-v4.conns))

(enc/declare-remote
  taoensso.carmine-v4/default-pool-opts
            taoensso.carmine-v4.sentinel/get-master-addr
            taoensso.carmine-v4.sentinel/resolve-master-addr
  ^:dynamic taoensso.carmine-v4.sentinel/*cbs*)

(alias 'core     'taoensso.carmine-v4)
(alias 'sentinel 'taoensso.carmine-v4.sentinel)

;;;; Pooled connection flow
;; 1. User creates pool with: (conn-manager-pooled <mgr-opts ...)
;; 2. User `wcar`      calls: (get-conn <conn-opts> :use-mgr? true ...)
;; 3. `get-conn`       calls: (mgr-borrow! <mgr> <conn-opts> ...)
;; 4. `mgr-borrow!`    calls: (.borrowObject <kop> <kop-key> ...), creating kop-key
;; 5. `borrowObject`   calls: (.makeObject   <kop> <kop-key> ...)
;; 6. `makeObject`     calls: (get-conn <conn-opts> :use-mgr? false ...)

;;;; Protocols

(defprotocol ^:private IConn
  "Internal protocol, not for public use or extension."
  (^:private -conn-close! [conn clean? force? data] "See `conn-close!`.")
  (^:public   conn-ready? [conn] "Tests Conn, and returns true iff healthy.")
  (^:private  conn-init!  [conn] "Initializes Conn auth, protocol, etc. Returns true on success, or throws.")

  (^:private  conn-resolved-correctly? [conn use-cache?]
   "Returns true iff Conn doesn't use Sentinel for server address resolution,
   or if Conn's address agrees with current (possibly cached) resolution."))

(defn ^:public conn-close!
  "Attempts to close given un/managed Conn (connection), and returns true
  iff successful.

    - `data`   ; Optional arbitrary map to include in errors and to provide
               ; to any registered callbacks.

    - `clean?` ; If the connection is managed, indicates to the relevant
               ; ConnManager whether the connection may be safely retained
               ; for future (re)use."

  ([conn            ] (-conn-close! conn true   false nil))
  ([conn clean?     ] (-conn-close! conn clean? false nil))
  ([conn clean? data] (-conn-close! conn clean? false data)))

(defprotocol IConnManager
  "Protocol for Carmine connection managers.
  EXPERIMENTAL, API subject to change!

  For advanced users:
    If you'd like to implement your own connection manager (e.g.
    a custom connection pool), create a type that implements
    these methods.

    Note that you'll also need to be familiar with the Conn
    type and IConn protocol. See `new-conn`.

  The following connection managers are provided by default:
    ConnManagerUnpooled
    ConnManagerPooled"

  (^:private mgr-return!       [mgr ^Conn conn]      "Releases given Conn back to ConnManager. Returns true iff successful.")
  (^:private mgr-remove!       [mgr ^Conn conn data] "Invalidates and destroys given Conn. Returns true iff successful.")
  (^:private mgr-borrow! ^Conn [mgr conn-opts]
   "Borrows a (possibly new) Conn from ConnManager that conforms to given `conn-opts`.
    Returns the Conn, or throws. Blocking.")

  (^:public mgr-init! [mgr conn-opts]
    "Performs any preparations necessary for borrowing (e.g. spin up connections).
    Blocking. Returns true iff successful.")

  (^:private -mgr-resolve-changed!
   [mgr master-name] [mgr master-name await-active-ms]
   "See `mgr-resolve-changed!`.")

  (^:public mgr-ready? [mgr] "Returns true iff ConnManager is ready for borrowing (e.g. not closed).")
  (^:public mgr-close! [mgr data] [mgr data await-active-ms]
   "Initiates a permanent shutdown of the ConnManager:
     - Stop accepting new borrow requests.
     - Destroy any idle or returned Conns.

     - If `await-active-ms` is provided, also blocks to await the return of any
       active conns - before forcibly interrupting any still active after wait.

       **WARNING** Can be dangerous to data integrity (pipelines may be interrupted
       mid-execution, etc.). Use this only if you understand the risks!"))

(defn ^:public mgr-resolve-changed!
  "Informs ConnManager that the given named Redis master address has changed
  (for example due to Sentinel failover). Returns true iff successful.

  The ConnManager will destroy any idle or returned Conns for the given master.
  If `await-active-ms` is provided, also blocks to await the return of any active
  conns to master - before forcibly interrupting any still active after wait."
  ([mgr master-name                ] (-mgr-resolve-changed! mgr (enc/as-qname master-name)))
  ([mgr master-name await-active-ms] (-mgr-resolve-changed! mgr (enc/as-qname master-name) await-active-ms)))

(defn ^:public conn-manager?
  "Returns true iff given argument satisfies the IConnManager
  protocol, and can thus be used as a Carmine connection manager."
  [x] (satisfies? IConnManager x))

;;;; Basic connections (unmanaged)

(let [idx (java.util.concurrent.atomic.AtomicLong. 0)]
  (defn- conn-name [_conn-opts]
    ;; Allow opts to specify name prefix, etc.?
    (str "carmine:" (.getAndIncrement idx))))

(comment (conn-name nil))

(deftype ^:private Conn [^Socket socket ip port conn-opts in out ^:volatile-mutable closed?]
  java.io.Closeable (close [this] (-conn-close! this nil false {:via 'java.io.Closeable}))

  Object
  (toString [_] ; "Conn[ip:port, managed, ready]"
    (str "Conn[" ip ":" port ", "
      (if (get conn-opts :mgr) "managed" "unmanaged") ", "
      (if closed? "closed" "ready") "]"))

  clojure.lang.IDeref
  (deref [this]
    {:socket    socket
     :ip        ip
     :port      port
     :conn-opts conn-opts
     :in        in
     :out       out
     :managed?  (boolean (get conn-opts :mgr))
     :ready?    (not closed?)})

  IConn
  (-conn-close! [this clean? force? data]
    ;; force => bypass possible manager
    (enc/cond
      closed? false

      ;; As a convenience, we allow managed conns to automatically
      ;; request closing by their manager
      :if-let [mgr-var (if force? nil (get conn-opts :mgr))]
      (if clean?
        (mgr-return! @mgr-var this)
        (mgr-remove! @mgr-var this data))

      :else
      (do
        (set! closed? true)
        (let [t0 (System/currentTimeMillis)
              closed? (try (.close socket) true (catch Exception _ nil))
              elapsed-ms (- (System/currentTimeMillis) t0)]

          (when-let [cb (utils/get-at conn-opts :cbs :on-conn-close)]
            (let [mgr-var (get conn-opts :mgr)]
              (utils/safely
                (cb
                  {:cbid       :on-conn-close
                   :addr       [ip port]
                   :conn       this
                   :conn-opts  conn-opts
                   :data       data
                   :clean?     clean?
                   :force?     force?
                   :elapsed-ms elapsed-ms
                   :closed?    closed?}))))

          closed?))))

  (conn-ready? [this]
    (if closed?
      false
      (let [t0 (System/currentTimeMillis)
            error_ (volatile! nil)
            pass?
            (and
              (if (conn-resolved-correctly? this :use-cache)
                true
                (do
                  (vreset! error_ (ex-info "Conn incorrectly resolved"))
                  false))

              (if-let [reply
                       (try
                         ;; Nb assume any necessary auth/init already done, otherwise
                         ;; will correctly identify connection as unready
                         (resp/basic-ping! in out)
                         (catch Throwable  t
                           (vreset! error_ t)
                           nil))]

                ;; Ref. https://github.com/redis/redis/issues/420
                (if (or (= reply "PONG") (= reply ["ping" ""]))
                  true
                  (do
                    (vreset! error_
                      (ex-info "Unexpected PING reply"
                        {:reply {:value reply :type (type reply)}}))
                    false))
                false))

            elapsed-ms (- (System/currentTimeMillis) t0)]

        (if pass?
          true
          (do
            (when-let [cb (utils/get-at conn-opts :cbs :on-conn-error)]
              (utils/safely
                (cb
                  {:cbid       :on-conn-error
                   :addr       [ip port]
                   :conn       this
                   :conn-opts  conn-opts
                   :via        'conn-ready?
                   :cause      @error_
                   :elapsed-ms elapsed-ms})))

            false)))))

  (conn-init! [this]
    (if closed?
      false
      (enc/when-let
        [init-opts (not-empty (get conn-opts :init))
         reqs
         (not-empty
           (enc/cond
             (contains? init-opts :commands)
             (get       init-opts :commands) ; Complete override

             :let [{:keys [auth client-name select-db resp3?]} init-opts
                   {:keys [username password]} auth
                   client-name
                   (enc/cond
                     (identical? client-name :auto) (conn-name   conn-opts)
                     (var?       client-name)       (client-name conn-opts)
                     :else       client-name)]

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
                  (fn [] (run! resp/redis-call* reqs)))

                (catch Throwable       t
                  (vreset! conn-error_ t)
                  nil))

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
                (utils/get-at conn-opts :cbs :on-conn-error)
                (ex-info "[Carmine] Error initializing connection"
                  {:eid :carmine.conns/conn-init-error
                   :addr       [ip port]
                   :conn       this
                   :conn-opts  conn-opts
                   :replies    reqs->replies
                   :elapsed-ms elapsed-ms}
                  @conn-error_))))))))

  (conn-resolved-correctly? [this use-cache?]
    (if-let [{:keys [master-name sentinel-spec]}
             (opts/get-sentinel-server conn-opts)]
      (let [current-master-addr
            (if use-cache?
              (sentinel/get-master-addr     sentinel-spec master-name)
              (sentinel/resolve-master-addr sentinel-spec master-name nil))]
        (= current-master-addr [ip port]))
      true)))

(defn ^:public conn?
  "Returns true iff given argument is a Carmine Conn (connection)."
  [x] (instance? Conn x))

;;;;

(let [default-factory_ (delay (javax.net.ssl.SSLSocketFactory/getDefault))]
  (defn- new-ssl-socket
    "Given an existing connected but unencrypted `java.net.Socket`, returns a
    connected and SSL-encrypted `java.net.Socket` using (SSLSocketFactory/getDefault)."

    [^Socket existing-socket ip port factory]
    (let [^javax.net.ssl.SSLSocketFactory factory
          (force ; Possible delay
            (if (identical? factory :default)
              default-factory_
              @factory ; var
              ))]

      (.createSocket factory existing-socket ^String ip ^int port true))))

(defn- new-socket ^Socket [ip port socket-opts]
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
          (opts/socket-opts-set! socket socket-opts)
          (do                    socket))]

    (.connect socket
      (java.net.InetSocketAddress. ^String ip (int port))
      (int (or connect-timeout-ms 0)))

    (if ssl
      (if (var? ssl)
        (new-ssl-socket socket ip port ssl)
        (new-ssl-socket socket ip port :default))

      socket)))

(comment (.close (new-socket "127.0.0.1" 6379 {:ssl true :connect-timeout-ms 2000})))

(defn new-conn
  "Low-level. Returns a new Conn to specific [ip port] socket address.
  Doesn't do any parsing, Sentinel resolution, or connection management."
  ^Conn [ip port conn-opts]
  (let [t0   (System/currentTimeMillis)
        ip   (have string? ip)
        port (enc/as-int   port)

        {:keys [#_server socket-opts buffer-opts]} conn-opts
        {:keys [init-size-in init-size-out]
         :or ; Defaults relevant only for REPL/tests
         {init-size-in  1024
          init-size-out 1024}} buffer-opts]

    (try
      (let [socket (new-socket ip port socket-opts)
            in     (-> socket .getInputStream  (java.io.BufferedInputStream.  init-size-in) java.io.DataInputStream.)
            out    (-> socket .getOutputStream (java.io.BufferedOutputStream. init-size-out))
            conn   (Conn. socket ip port conn-opts in out false)]

        (conn-init! conn)
        (do         conn))

      (catch Throwable t
        (utils/cb-notify-and-throw!    :on-conn-error
          (utils/get-at conn-opts :cbs :on-conn-error)
          (ex-info "[Carmine] Error creating new connection"
            {:eid :carmine.conns/new-conn-error
             :addr       [ip port]
             :conn-opts  conn-opts
             :elapsed-ms (- (System/currentTimeMillis) t0)}
            t))))))

(comment
  (enc/qb 1e3 ; [392.72 411.88]
    (-conn-close! (new-conn "127.0.0.1" 6379 {}) true false nil)
    (-conn-close! (new-conn "127.0.0.1" 6379 {:socket-opts {:ssl true}}) true false nil)))

(defn get-conn
  "Main entry point for acquiring a connection.
  Returns a new Conn for given `conn-opts`, with support for Sentinel
  resolution, opts parsing, and connection management."
  ^Conn [conn-opts parse-opts? use-mgr?]
  (let [conn-opts
        (if parse-opts?
          (opts/parse-conn-opts :with-dynamic-default conn-opts)
          (do                                         conn-opts))]

    (if-let [mgr-var (and use-mgr? (get conn-opts :mgr))]

      ;; Request a conn via manager
      (mgr-borrow! (force @mgr-var) conn-opts)

      (let [{:keys [server]} conn-opts]
        (enc/cond
          (vector? server) (let [[ip port] server] (new-conn ip port conn-opts))
          (map?    server) ; As `opts/get-sentinel-server`
          (let [{:keys [master-name sentinel-spec sentinel-opts]} server
                [ip port] (sentinel/resolve-master-addr master-name
                            sentinel-spec sentinel-opts)]
            (new-conn ip port conn-opts))

          (throw ; Shouldn't be possible after validation
            (ex-info "[Carmine] Unexpected :server type"
              {:server {:value server :type (type server)}})))))))

(defn with-conn
  "Calls (f <Conn> <in> <out>), and closes Conn after use."
  [^Conn conn f]
  (try
    (let [result (f conn (.-in conn) (.-out conn))]
      (-conn-close! conn true false {:via 'with-conn}))

    (catch Throwable t
      (-conn-close! conn false false {:via 'with-conn :cause t})
      (throw t))))

(comment (with-conn (new-conn "127.0.0.1" 6379 {}) (fn [c _ _] (conn-ready? c))))

;;;; ConnManagerUnpooled

(defn- try-borrow-conn! [mgr mgr-opts conn-opts body-fn]
  (let [t0 (System/currentTimeMillis)
        error_ (volatile! nil)]
    (or
      (try (body-fn) (catch Throwable t (vreset! error_ t) nil))
      (throw
        (ex-info "[Carmine] Error borrowing connection from manager"
          {:eid :carmine.conns/borrow-conn-error
           :mgr-opts   mgr-opts
           :conn-opts  conn-opts
           :elapsed-ms (- (System/currentTimeMillis) t0)}
          @error_)))))

(declare ^:private await-active-conns)

(deftype ConnManagerUnpooled
  [mgr-opts ^AtomicLong n-created* active-conns_ ^:volatile-mutable closed?]

  Object              (toString [_] (str "ConnManagerUnpooled[" (if closed? "closed" "ready") "]"))
  java.io.Closeable   (close [this] (mgr-close! this {:via 'java.io.Closeable}))
  clojure.lang.IDeref
  (deref [this]
    {:ready?   (not closed?)
     :mgr-opts mgr-opts
     :stats
     {:n-created (.get n-created*)
      :n-active  (count @active-conns_)}})

  IConnManager
  (mgr-ready?  [_] (not closed?)) ; Noop
  (mgr-return! [_ conn]      (do (swap! active-conns_ disj conn) (-conn-close! conn true  true {:via 'mgr-return!})))
  (mgr-remove! [_ conn data] (do (swap! active-conns_ disj conn) (-conn-close! conn false true {:via 'mgr-remove! :data data})))

  (-mgr-resolve-changed! [mgr master-name]                 (not closed?)) ; Noop
  (-mgr-resolve-changed! [mgr master-name await-active-ms] (not closed?)) ; Noop

  (mgr-init!   [_ conn-opts] (not closed?)) ; Noop
  (mgr-borrow! [this conn-opts]
    (try-borrow-conn! this mgr-opts conn-opts
      (fn []
        (if closed?
          (throw (ex-info "[Carmine] Cannot borrow from closed ConnManagerUnpooled" {}))
          (when-let [conn (get-conn conn-opts true false)]
            (.getAndIncrement n-created*)
            (swap! active-conns_ conj conn)
            (do                       conn))))))

  (mgr-close! [this data                ] (mgr-close! this data nil))
  (mgr-close! [this data await-active-ms]
    (if closed?
      false
      (do
        (set! closed? true)
        (if                   await-active-ms
          (await-active-conns await-active-ms
            {:via 'mgr-close! :data data}
            (fn  get-current  []               @active-conns_)
            (fn pull-current! [] (enc/reset-in! active-conns_ #{})))
          true)))))

(defn- await-active-conns
  "Helper for `await-active-ms` cases.
  Loops, waiting up to ~`await-ms` for (fn-get-current) to be empty.
  If still not empty, forcibly closes every conn in (fn-pull-current!)."
  [await-ms close-data fn-get-current fn-pull-current!]
  (if-let [await-ms (enc/as-?pos-int await-ms)]
    (let [timeout-at-ms (+ (System/currentTimeMillis) await-ms)]
      (loop []
        (cond
          (== (count (fn-get-current)) 0) true

          (< (System/currentTimeMillis) timeout-at-ms)
          (do (Thread/sleep 100) (recur))

          :else
          (do ; Give up waiting
            (let [pulled-conns (fn-pull-current!)]
              (run!
                #(utils/safely (-conn-close! % false true close-data))
                pulled-conns))
            true))))
    true))

(defn ^:public conn-manager-unpooled
  "TODO Docstring
  Returns a new stateful ConnManagerUnpooled for use in `conn-opts`."
  ^ConnManagerUnpooled [mgr-opts]
  (ConnManagerUnpooled. mgr-opts (AtomicLong. 0)
    (atom #{}) false))

(comment
  (def my-mgr (conn-manager-unpooled {}))
  (with-conn
    (get-conn {:server ["127.0.0.1" 6379] :mgr #'my-mgr} true true)
    (fn [_ in out]
      (resp/with-replies in out true true
        (fn [] (resp/redis-call "PING")))))

  @my-mgr)

;;;; ConnManagerPooled (using Apache Commons Pool 2)
;; Ref. org.apache.commons.pool2.impl.BaseGenericObjectPool,
;;      org.apache.commons.pool2.impl.GenericKeyedObjectPool,
;;      org.apache.commons.pool2.KeyedPooledObjectFactory

(def ^:private ^:dynamic *kop-counter* "For testing" nil)

;; conn-opts->kop-key
(let [cached ; Opts are pure data => safe to cache
      (enc/cache {:size 128 :gc-every 1000}
        (fn [conn-opts]
          (have? map? conn-opts)
          (let [conn-opts (opts/parse-conn-opts false conn-opts)
                kop-key
                (enc/select-nested-keys conn-opts
                  [:socket-opts
                   {:server [:ip :port :master-name]
                    :init   [:commands :resp3? :auth :client-name]}])

                kop-key
                (utils/dissoc-ks kop-key :socket-opts
                  [:setSoTimeout :so-timeout :read-timeout-ms])]

            (with-meta kop-key
              {:__conn-opts conn-opts}))))]

  (defn- conn-opts->kop-key
    "Our ConnManagerPooled is backed by a KeyedObjectPool that is essentially a
    set of >=1 sub-pools of fungible connections, where fungibility is
    determined by key equality.

    We'll use a simplified submap of `conn-opts` as our kop-keys, with the
    full `conn-opts` map as metadata."
    [conn-opts]
    (if-let [kop-key (get (meta conn-opts) :__kop-key)]
      kop-key
      (do
        (when-let [kc *kop-counter*] (kc))
        (cached conn-opts)))))

(defn- kop-key->conn-opts [kop-key]
  (when-let [conn-opts (get (meta kop-key) :__conn-opts)]
    (with-meta conn-opts {:__kop-key kop-key})))

(deftest ^:private kop-keys
  (let [kc (enc/counter 0)]
    (binding [*kop-counter* kc]
      (let [result (-> {:server ["127.0.0.1" "6379"], :socket-opts {:read-timeout-ms 1000}}
                     conn-opts->kop-key kop-key->conn-opts
                     conn-opts->kop-key kop-key->conn-opts
                     conn-opts->kop-key kop-key->conn-opts)]

        [(is (= result {:server ["127.0.0.1" 6379], :socket-opts {:read-timeout-ms 1000}}))
         (is (= @kc 1))]))))

(comment (enc/qb 1e6 (conn-opts->kop-key {:server ["127.0.0.1" "6379"]}))) ; 198.79

(def ^:private ^:dynamic *mgr-remove-data* nil)
(declare ^:private get-kop-stats)

(defn- new-kop-state []
  {:active-conns_                (atom #{}) ;                #{<Conns>}
   :active-conns-by-master-name_ (atom  {}) ; {<master-name> #{<Conns>}}
   :active-conns-by-kop-key_     (atom  {}) ; {<kop-key>     #{<Conns>}}

   :kop-keys_                    (atom #{}) ;                #{<kop-key>}  for stats
   :kop-keys-by-master-name_     (atom  {}) ; {<master-name> #{<kop-key>}} for idle .clear
   })

(defn- update-kop-state!
  [kop-state conn kop-key update-kind]

  ;; TODO Continue from here
  ;; TODO :active, :inactive

  (if-let [{:keys [master-name]}
           (opts/get-sentinel-server
             (.-conn-opts ^Conn conn))]
    :TODO
    :TODO

    ))

(deftype ConnManagerPooled
  [mgr-opts ^GenericKeyedObjectPool kop kop-state ^:volatile-mutable closed?]

  Object            (toString [_] (str "ConnManagerPooled[" (if closed? "closed" "ready") "]"))
  java.io.Closeable (close [this] (mgr-close! this {:via 'java.io.Closeable}))

  clojure.lang.IDeref
  (deref [this]
    {:ready?   (mgr-ready? this)
     :mgr-opts mgr-opts
     :pool     kop
     :stats_   (delay (get-kop-stats kop @(get kop-state :kop-keys_)))})

  IConnManager
  (mgr-ready?  [_] (and (not closed?) (not (.isClosed kop))))
  (mgr-return! [_ conn]
    (let [kk (conn-opts->kop-key (.-conn-opts ^Conn conn))]
      (update-kop-state! kop-state conn kk :inactive)

      ;; If pool has TestOnBorrow, this check is redundant but inexpensive (cached)
      (if (conn-resolved-correctly? conn :use-cache)
        (.returnObject     kop kk conn)
        (.invalidateObject kop kk conn)))

    true)

  (mgr-remove! [_ conn data]
    (let [kk (conn-opts->kop-key (.-conn-opts ^Conn conn))]
      (update-kop-state! kop-state conn kk :inactive)

      (if data
        (binding [*mgr-remove-data* data] (.invalidateObject kop kk conn))
        (do                               (.invalidateObject kop kk conn))))
    true)

  (mgr-init! [_ unparsed-conn-opts]
    (let [parsed-conn-opts
          (opts/parse-conn-opts :with-dynamic-defaults
            unparsed-conn-opts)

          kk (conn-opts->kop-key parsed-conn-opts)]

      (swap! (get kop-state :kop-keys_) conj kk)
      (.preparePool kop kk) ; Ensure that configured min idle instances ready
      true))

  (-mgr-resolve-changed! [mgr master-name] (-mgr-resolve-changed! mgr master-name nil))
  (-mgr-resolve-changed! [mgr master-name await-active-ms]
    (let [master-name (enc/as-qname master-name)]

      ;; Clear idle Conns to master
      (let [{:keys [kop-keys-by-master-name_]} kop-state
            pulled-kop-keys (enc/pull-val! kop-keys-by-master-name_ master-name)]

        (run! (fn [kk] (.clear kop kk)) pulled-kop-keys))

      ;; Deal with active Conns to master
      (if await-active-ms
        (let [{:keys [active-conns-by-master-name_]} kop-state]
          (await-active-conns await-active-ms
            {:via 'mgr-resolve-changed! :master-name master-name}
            (fn  get-current  [] (get          @active-conns-by-master-name_ master-name))
            (fn pull-current! [] (enc/pull-val! active-conns-by-master-name_ master-name))))
        true)))

  (mgr-borrow! [this parsed-conn-opts]
    (let [using-sentinel? (opts/get-sentinel-server parsed-conn-opts)
          kk              (conn-opts->kop-key       parsed-conn-opts)
          conn
          (if using-sentinel?
            (let [cbs
                  {:on-resolve-change
                   (fn [{:keys [master-name master-addr]}]
                     (-mgr-resolve-changed! this master-name))}]

              (binding [sentinel/*cbs* cbs] (.borrowObject kop kk)))
            (do                             (.borrowObject kop kk)))]

      (update-kop-state! kop-state conn kk :active)

      ;; TODO init conn
      ;;   - (re)set socket-timeout-ms
      ;;   - (re)set client-name, db
      ;;     - Use lazy commands with skip-replies

      #_
      (when (contains? so :read-timeout-ms)
        (.setSoTimeout ^Socket (.-socket conn)
          (int (or (get so :read-timeout-ms) 0))))

      conn))

  (mgr-close! [this data                ] (mgr-close! this data nil))
  (mgr-close! [this data await-active-ms]
    (if closed?
      false
      (do
        (set! closed? true)
        (.close kop)

        (if await-active-ms
          (let [{:keys [active-conns_]} kop-state]
            (await-active-conns await-active-ms
              {:via 'mgr-close! :data data}
              (fn  get-current  []               @active-conns_)
              (fn pull-current! [] (enc/reset-in! active-conns_ #{}))))
          true)))))

(defn ^:public conn-manager-pooled
  "TODO Docstring, incl. :pool-opts {}, mention `default-pool-opts`"
  [mgr-opts]
  (let [pool-opts (utils/merge-opts core/default-pool-opts (get mgr-opts :pool-opts))
        factory
        (reify KeyedPooledObjectFactory
          (activateObject  [_ kk po] nil)
          (passivateObject [_ kk po] nil)
          (validateObject  [_ kk po] (conn-ready?  (.getObject po)))
          (destroyObject   [_ kk po] (-conn-close! (.getObject po) false true
                                       *mgr-remove-data*))
          (makeObject [_ kk]
            (let [conn-opts (kop-key->conn-opts kk)
                  conn (get-conn conn-opts false false)]
              (DefaultPooledObject. conn))))

        kop (GenericKeyedObjectPool. (factory))]

    (opts/kop-opts-set! kop pool-opts)
    (ConnManagerPooled. mgr-opts kop (new-kop-state) false)))

(defn- get-kop-stats [^GenericKeyedObjectPool kop kop-keys]
  {:counts
   {:sub-pools (count kop-keys)
    :created   (.getCreatedCount  kop)
    :borrowed  (.getBorrowedCount kop)
    :returned  (.getReturnedCount kop)
    :destroyed
    {:total                (.getDestroyedCount                   kop)
     :by-borrow-validation (.getDestroyedByBorrowValidationCount kop)
     :by-eviction          (.getDestroyedByEvictorCount          kop)}

    :waiting {:total (.getNumWaiters kop)}
    :idle
    {:total (.getNumIdle kop)
     :by-key
     (persistent!
       (reduce
         (fn [m kk] (assoc! m kk (.getNumIdle kop kk)))
         (transient {})
         kop-keys))}

    :active
    {:total (.getNumActive kop)
     :by-key
     (persistent!
       (reduce
         (fn [m kk] (assoc! m kk (.getNumActive kop kk)))
         (transient {})
         kop-keys))}}

   :borrow-time
   {:mean (.getMeanBorrowWaitTimeMillis kop)
    :max  (.getMaxBorrowWaitTimeMillis  kop)}

   :mean-times
   {:idle   (.getMeanIdleTimeMillis       kop)
    :borrow (.getMeanBorrowWaitTimeMillis kop)
    :active (.getMeanActiveTimeMillis     kop)}})

;;;; Print methods

(let [ns *ns*]
  (defmethod print-method Conn                [^Conn                x ^java.io.Writer w] (.write w (str "#" ns "." x)))
  (defmethod print-method ConnManagerUnpooled [^ConnManagerUnpooled x ^java.io.Writer w] (.write w (str "#" ns "." x)))
  (defmethod print-method ConnManagerPooled   [^ConnManagerPooled   x ^java.io.Writer w] (.write w (str "#" ns "." x))))
