(ns carmine.connections
  "Connection pooling implemented using Apache Commons Pool, adapted from
  redis-clojure."
  (:import [java.net Socket]
           [java.io OutputStream BufferedInputStream DataInputStream]
           [org.apache.commons.pool KeyedPoolableObjectFactory]
           [org.apache.commons.pool.impl GenericKeyedObjectPool]))

(defmacro ^:private declare-remote
  "Declare the given ns-qualified names. Useful for circular dependencies."
  [& names]
  (let [orig-ns (str *ns*)]
    `(do ~@(map (fn [n]
                  (let [ns (namespace n)
                        v (name n)]
                    `(do (in-ns '~(symbol ns))
                         (declare ~(symbol v))))) names)
         (in-ns '~(symbol orig-ns)))))

(declare-remote carmine.core/request carmine.core/ping carmine.core/auth)

;; Interface for socket connections to Redis server
(defprotocol IConnection
  (get-spec      [conn])
  (close-conn    [conn])
  (input-stream  [conn])
  (output-stream [conn])
  (conn-alive?   [conn]))

;; Interface for socket connection pools
(defprotocol IConnectionPool
  (get-conn     [pool spec])
  (release-conn [pool conn] [pool conn exception]))

(defrecord Connection [^Socket socket spec]
  IConnection
  (get-spec     [this] spec)
  (close-conn   [this] (.close socket))
  (input-stream [this] (-> (.getInputStream socket)
                           (BufferedInputStream.)
                           (DataInputStream.)))
  (output-stream [this] (.getOutputStream socket))
  (conn-alive?   [this]
    (if (:pubsub-conn? spec) true ; TODO See .org
        (= "PONG" (try (#'carmine.core/request this (carmine.core/ping))
                       (catch Exception _))))))

(defrecord ConnectionPool [^GenericKeyedObjectPool pool]
  IConnectionPool
  (get-conn     [this spec] (.borrowObject pool spec))
  (release-conn [this conn] (.returnObject pool (get-spec conn) conn))
  (release-conn [this conn exception] (.invalidateObject pool (get-spec conn)
                                                         conn)))

(defn- make-new-connection
  "Actually create and return a new socket connection."
  [{:keys [host port password timeout] :as spec}]
  (let [socket (doto (Socket. ^String host ^Integer port)
                 (.setTcpNoDelay true)
                 (.setKeepAlive true)
                 (.setSoTimeout ^Integer timeout))
        conn   (Connection. socket spec)]
    (when password (#'carmine.core/request conn (carmine.core/auth password)))
    conn))

(defrecord NonPooledConnectionPool []
  IConnectionPool
  (get-conn     [this spec] (make-new-connection spec))
  (release-conn [this conn] (close-conn conn))
  (release-conn [this conn exception] (close-conn conn)))

(defn- make-connection-factory []
  (reify KeyedPoolableObjectFactory
    (makeObject      [this spec] (make-new-connection spec))
    (activateObject  [this spec conn])
    (validateObject  [this spec conn] (conn-alive? conn))
    (passivateObject [this spec conn])
    (destroyObject   [this spec conn] (close-conn conn))))

(defn- set-pool-option [^GenericKeyedObjectPool pool [opt v]]
  (case opt
    :max-active                    (.setMaxActive pool v)
    :max-total                     (.setMaxTotal pool v)
    :min-idle                      (.setMinIdle pool v)
    :max-idle                      (.setMaxIdle pool v)
    :max-wait                      (.setMaxWait pool v)
    :lifo                          (.setLifo pool v)
    :test-on-borrow                (.setTestOnBorrow pool v)
    :test-on-return                (.setTestOnReturn pool v)
    :test-while-idle               (.setTestWhileIdle pool v)
    :when-exhausted-action         (.setWhenExhaustedAction pool v)
    :num-tests-per-eviction-run    (.setNumTestsPerEvictionRun pool v)
    :time-between-eviction-runs-ms (.setTimeBetweenEvictionRunsMillis pool v)
    :min-evictable-idle-time-ms    (.setMinEvictableIdleTimeMillis pool v)
    (throw (Exception. (str "Unknown pool option: " opt))))
  pool)

(defn make-conn-pool
  "For option documentation see http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericKeyedObjectPool.html"
  [& options]
  (let [;; Defaults stolen from Jedis
        default {:test-while-idle               true
                 :num-tests-per-eviction-run    -1
                 :min-evictable-idle-time-ms    60000
                 :time-between-eviction-runs-ms 30000}]
    (ConnectionPool.
     (reduce set-pool-option (GenericKeyedObjectPool. (make-connection-factory))
             (merge default (apply hash-map options))))))

(def non-pooled-connection-pool
  "Degenerate connection pool for backwards compatibility with Accession."
  (NonPooledConnectionPool.))

(defmacro with-conn
  "Evaluate pipelined Redis commands in the context of a pooled connection to
  Redis server. When done, release the connection back to pool and return the
  server's response. Use 'make-conn-pool' and 'make-conn-spec' to generate the
  required arguments."
  [connection-pool connection-spec & commands]
  `(try
     (let [conn# (get-conn ~connection-pool ~connection-spec)]
       (try
         (let [server-response# (#'carmine.core/request conn# ~@commands)]
           (release-conn ~connection-pool conn#)
           server-response#)
         ;; Failed to execute body
         (catch Exception e# (release-conn ~connection-pool conn# e#)
                (throw e#))))
     ;; Failed to get connection from pool
     (catch Exception e# (throw e#))))

;; For backwards compatibility with Accession
(defmacro with-connection
  "DEPRECATED. Use with-conn instead. Evaluate pipelined Redis commands in the
  context of a NON-pooled connection to Redis server. When done, close the
  connection and return the server's response."
  [connection-spec & commands]
  `(with-pooled-connection non-pooled-connection-pool
     ~connection-spec ~@commands))

(defn make-conn-spec
  [& {:keys [host port password timeout]
      :or   {host "127.0.0.1" port 6379 password nil timeout 0}}]
  {:host host :port port :password password :timeout timeout})