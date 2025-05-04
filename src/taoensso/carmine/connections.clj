(ns taoensso.carmine.connections
  "Handles socket connection lifecycle.
  Pool is implemented with Apache Commons pool.
  Originally adapted from `redis-clojure`."
  {:author "Peter Taoussanis"}
  (:require
   [clojure.string            :as str]
   [taoensso.encore           :as enc]
   [taoensso.truss            :as truss]
   [taoensso.carmine.protocol :as protocol])

  (:import
   [java.net InetSocketAddress Socket URI]
   [org.apache.commons.pool2 KeyedPooledObjectFactory]
   [org.apache.commons.pool2.impl GenericKeyedObjectPool DefaultPooledObject]))

(enc/declare-remote
  taoensso.carmine/ping
  taoensso.carmine/auth
  taoensso.carmine/select)

;;; Outline/nomenclature
;; connection -> Connection object.
;; pool       -> IConnectionPool implementer (for custom pool types, etc.).
;; conn spec  -> map of processed options describing connection properties.
;; conn opts  -> map of unprocessed options that'll be processed to create a spec.
;; pool opts  -> map of options that'll be used to create a pool (memoized).
;; conn opts  -> {:pool <pool-opts> :spec <spec-opts>} as taken by `wcar`, etc.

(defprotocol IConnection
  (-conn-error [this])
  (conn-alive? [this])
  (close-conn  [this]))

(defrecord Connection [^Socket socket spec in out]
  IConnection
  (conn-alive? [this] (nil? (-conn-error this)))
  (-conn-error [this]
    (try
      (let [resp
            (protocol/with-context this
              (protocol/parse nil
                (protocol/with-replies
                  (taoensso.carmine/ping))))]

        ;; Ref. https://github.com/redis/redis/issues/420
        (when (not= resp (if (:listener? this) ["ping" ""] "PONG"))
          (truss/ex-info! "Unexpected PING response" {:resp resp})))

      (catch Exception ex
        (when-let [f (get-in spec [:instrument :on-conn-error])]
          (f {:spec spec :ex ex}))
        ex)))

  (close-conn [_]
    (when-let [f (get-in spec [:instrument :on-conn-close])] (f {:spec spec}))
    (.close socket)))

(defprotocol IConnectionPool
  (get-conn     [this spec])
  (release-conn [this conn] [this conn exception]))

(defrecord ConnectionPool [^GenericKeyedObjectPool pool]
  IConnectionPool
  (get-conn     [_ spec] (.borrowObject pool spec))
  (release-conn [_ conn] (.returnObject pool (:spec conn) conn))
  (release-conn [_ conn exception] (.invalidateObject pool (:spec conn) conn))
  java.io.Closeable
  (close [_] (.close pool)))

;;;

(let [factory_ (delay (javax.net.ssl.SSLSocketFactory/getDefault))]
  (defn default-ssl-fn
    "Takes an unencrypted underlying java.net.Socket and returns an
    encrypted java.net.Socket using the environment's default SSLSocketFactory."
    [{:keys [socket host port]}]
    (.createSocket ^javax.net.ssl.SSLSocketFactory @factory_
      ^Socket socket ^String host ^Integer port true)))

(defn- get-streams [^Socket socket init-read-buffer-size init-write-buffer-size]
  (let [in
        (->
          (.getInputStream socket)
          (java.io.BufferedInputStream. (int init-read-buffer-size))
          (java.io.DataInputStream.))

        out
        (->
          (.getOutputStream socket)
          (java.io.BufferedOutputStream. (int init-write-buffer-size)))]

    ;; Re: choice of stream types:
    ;;   We need the following:
    ;;   - Ability to read & write bytes and byte arrays.
    ;;   - Ability to read lines (chars up to CRLF) as string
    ;;     - Note that `java.io.DataInputStream/readLine` is deprecated
    ;;       due to lack of charset support, but is actually fine for our
    ;;       purposes since we use `readLine` only for simple ascii strings
    ;;       from Redis server (i.e. no user content).
    ;;
    ;;       Benching also shows ~equal performance to specialised implns.
    ;;       like `redis.clients.jedis.util.RedisInputStream`.

    [in out]))

(defn make-new-connection
  [{:keys [host port username password db conn-setup-fn
           conn-timeout-ms read-timeout-ms timeout-ms ssl-fn] :as spec}]

  (let [;; :timeout-ms controls both :conn-timeout-ms and :read-timeout-ms
        ;; unless those are specified individually
        ;; :or   {conn-timeout-ms (or timeout-ms 4000)
        ;;        read-timeout-ms timeout-ms} ; Ref. http://goo.gl/XULHCd
        conn-timeout-ms (get spec :conn-timeout-ms (or timeout-ms 4000))
        read-timeout-ms (get spec :read-timeout-ms     timeout-ms)

        socket-address (InetSocketAddress. ^String host ^Integer port)
        ^Socket socket
        (enc/doto-cond [expr (Socket.)]
          :always         (.setTcpNoDelay   true)
          :always         (.setKeepAlive    true)
          :always         (.setReuseAddress true)
          ;; :always      (.setSoLinger     true 0)
          read-timeout-ms (.setSoTimeout ^Integer expr))

        _ (if conn-timeout-ms
            (.connect socket socket-address conn-timeout-ms)
            (.connect socket socket-address))

        ^Socket socket
        (if ssl-fn
          (let [f (if (identical? ssl-fn :default) default-ssl-fn ssl-fn)]
            (f {:socket socket :host host :port port}))
          socket)

        conn
        (let [[in out] (get-streams socket 8192 8192)]
          (->Connection socket spec in out))

        db (when (and db (not (zero? db))) db)]

    (when-let [f (get-in spec [:instrument :on-conn-open])] (f {:spec spec}))

    (when (or username password db conn-setup-fn)
      (protocol/with-context conn
        (protocol/with-replies ; Discard replies
          (when password
            (if username
              (taoensso.carmine/auth username password)
              (taoensso.carmine/auth          password)))

          (when db (taoensso.carmine/select (str db)))
          (when conn-setup-fn
            (conn-setup-fn {:conn conn :spec spec})))))
    conn))

;; A degenerate connection pool: gives pool-like interface for non-pooled conns
(defrecord NonPooledConnectionPool []
  IConnectionPool
  (get-conn     [_ spec] (make-new-connection spec))
  (release-conn [_ conn] (close-conn conn))
  (release-conn [_ conn exception] (close-conn conn))
  java.io.Closeable
  (close [_] nil))

(defn make-connection-factory []
  (reify KeyedPooledObjectFactory
    (makeObject      [_ spec] (DefaultPooledObject. (make-new-connection spec)))
    (activateObject  [_ spec pooled-obj])
    (validateObject  [_ spec pooled-obj] (let [conn (.getObject pooled-obj)]
                                           (conn-alive? conn)))
    (passivateObject [_ spec pooled-obj])
    (destroyObject   [_ spec pooled-obj] (let [conn (.getObject pooled-obj)]
                                           (close-conn conn)))))

(defn- set-pool-option [^GenericKeyedObjectPool pool k v]
  (case k

    ;;; org.apache.commons.pool2.impl.GenericKeyedObjectPool
    :min-idle-per-key  (.setMinIdlePerKey  pool v) ; 0
    :max-idle-per-key  (.setMaxIdlePerKey  pool v) ; 8
    :max-total-per-key (.setMaxTotalPerKey pool v) ; 8

    ;;; org.apache.commons.pool2.impl.BaseGenericObjectPool
    :block-when-exhausted? (.setBlockWhenExhausted pool v) ; true
    :lifo?       (.setLifo          pool v) ; true
    :max-total   (.setMaxTotal      pool v) ; -1
    :max-wait-ms (.setMaxWaitMillis pool v) ; -1
    :min-evictable-idle-time-ms (.setMinEvictableIdleTimeMillis pool v) ; 1800000
    :num-tests-per-eviction-run (.setNumTestsPerEvictionRun     pool v) ; 3
    :soft-min-evictable-idle-time-ms (.setSoftMinEvictableIdleTimeMillis pool v) ; -1
    :swallowed-exception-listener    (.setSwallowedExceptionListener     pool v)
    :test-on-borrow?  (.setTestOnBorrow  pool v) ; false
    :test-on-return?  (.setTestOnReturn  pool v) ; false
    :test-while-idle? (.setTestWhileIdle pool v) ; false
    :time-between-eviction-runs-ms (.setTimeBetweenEvictionRunsMillis pool v) ; -1

    :instrument nil ; noop (ignore)

    (truss/ex-info! (str "Unknown pool option: " k) {:option k}))
  pool)

(def conn-pool
  (enc/memoize
    (fn [pool-opts]

      (when-let [f (get-in pool-opts [:instrument :on-pool-init])]
        (f {:pool-opts pool-opts}))

      (cond
        (identical? pool-opts :none) (->NonPooledConnectionPool)
        ;; Pass through pre-made pools (note that test reflects):
        (satisfies? IConnectionPool pool-opts) pool-opts
        :else
        (let [pool-opts (dissoc pool-opts :id) ; Support >1 pool with same opts
              jedis-defaults ; Ref. http://goo.gl/y1mDbE
              {:test-while-idle?              true  ; from false
               :num-tests-per-eviction-run    -1    ; from 3
               :min-evictable-idle-time-ms    60000 ; from 1800000
               :time-between-eviction-runs-ms 30000 ; from -1
               }
              carmine-defaults
              {:max-total-per-key 16 ; from 8
               :max-idle-per-key  16 ; Same as above to avoid early connection closing
               }]
          (->ConnectionPool
            (reduce-kv set-pool-option
              (GenericKeyedObjectPool. (make-connection-factory))
              (merge jedis-defaults carmine-defaults pool-opts))))))))

;; (defn uncached-conn-pool [pool-opts] (conn-pool :mem/fresh pool-opts))
(comment (conn-pool :none) (conn-pool {}))

(defn- parse-uri [uri]
  (when uri
    (let [^URI uri (if (instance? URI uri) uri (URI. uri))
          [username password] (.split (str (.getUserInfo uri)) ":")
          port (.getPort uri)
          db   (when-let [[_ db-str] (re-matches #"/(\d+)$" (.getPath uri))]
                 (Integer. ^String db-str))

          ssl-fn
          (when-let [scheme (.getScheme uri)]
            (when (contains? #{"rediss" "https"} (str/lower-case scheme))
              :default))]

      (-> {:host (.getHost uri)}
          (#(if (pos? port)                (assoc % :port     port)     %))
          (#(if (and db (pos? (long db)))  (assoc % :db       db)       %))
          (#(if (enc/nempty-str? username) (assoc % :username username) %))
          (#(if (enc/nempty-str? password) (assoc % :password password) %))
          (#(if ssl-fn                     (assoc % :ssl-fn   ssl-fn)   %))))))

(comment
  [(parse-uri  "redis://user:pass@x.y.com:9475/3")
   (parse-uri  "redis://:pass@x.y.com.com:9475/3")
   (parse-uri      "redis://user:@x.y.com:9475/3")
   (parse-uri     "rediss://user:@x.y.com:9475/3")])

(def conn-spec
  (enc/memoize
   (fn [{:keys [uri host port username password timeout-ms db
               conn-setup-fn ; nb must be var-level for fn equality
               ] :as spec-opts}]
     (let [defaults  {:host "127.0.0.1" :port 6379}
           spec-opts (if-let [timeout (:timeout spec-opts)] ; Deprecated opt
                       (assoc spec-opts :timeout-ms timeout)
                       spec-opts)]
       (merge defaults spec-opts (parse-uri uri))))))

(defn pooled-conn "Returns [<open-pool> <pooled-connection>]"
  [{:as conn-opts pool-opts :pool spec-opts :spec}]
  (let [spec (conn-spec spec-opts)
        pool (conn-pool pool-opts)]
    (try
      (try
        [pool (get-conn pool spec)]
        (catch IllegalStateException e ; Cached pool's gone bad
          (let [pool (conn-pool :mem/fresh pool-opts)]
            [pool (get-conn pool spec)])))
      (catch Exception e
        (truss/ex-info! "Carmine connection error" {} e)))))
