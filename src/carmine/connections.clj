(ns carmine.connections
  "Handles life cycle of socket connections to Redis server. Connection pool is
  implemented using Apache Commons pool. Adapted from redis-clojure."
  (:require [carmine.utils    :as utils]
            [carmine.protocol :as protocol])
  (:import  [java.net Socket]
            [java.io  OutputStream BufferedInputStream DataInputStream]
            [org.apache.commons.pool      KeyedPoolableObjectFactory]
            [org.apache.commons.pool.impl GenericKeyedObjectPool]))

;; Hack to allow cleaner separation of ns concerns
(utils/declare-remote carmine.core/ping
                      carmine.core/auth
                      carmine.core/select)

;; Interface for socket connections to Redis server
(defprotocol IConnection
  (get-spec    [conn])
  (close-conn  [conn])
  (in-stream   [conn])
  (out-stream  [conn])
  (conn-alive? [conn]))

(defrecord Connection [^Socket socket spec]
  IConnection
  (get-spec    [this] spec)
  (close-conn  [this] (.close socket))
  (in-stream   [this] (-> (.getInputStream socket)
                          (BufferedInputStream.)
                          (DataInputStream.)))
  (out-stream  [this] (-> (.getOutputStream socket)))
  (conn-alive? [this]
    (if (:pubsub? spec) true ; TODO See .org file
        (= "PONG" (try (carmine.protocol/with-context-and-response this
                         (carmine.core/ping))
                       (catch Exception _))))))

;; Interface for a pool of socket connections
(defprotocol IConnectionPool
  (get-conn     [pool spec])
  (release-conn [pool conn] [pool conn exception]))

(defrecord ConnectionPool [^GenericKeyedObjectPool pool]
  IConnectionPool
  (get-conn     [this spec] (.borrowObject pool spec))
  (release-conn [this conn] (.returnObject pool (get-spec conn) conn))
  (release-conn [this conn exception] (.invalidateObject pool (get-spec conn)
                                                         conn)))

(defn make-new-connection
  "Actually creates and returns a new socket connection."
  [{:keys [host port password timeout db] :as spec}]
  (let [socket (doto (Socket. ^String host ^Integer port)
                 (.setTcpNoDelay true)
                 (.setKeepAlive true)
                 (.setSoTimeout ^Integer timeout))
        conn (Connection. socket spec)]
    (when password (carmine.protocol/with-context-and-response conn
                     (carmine.core/auth password)))
    (when (not (zero? db)) (carmine.protocol/with-context-and-response conn
                             (carmine.core/select db)))
    conn))

(defrecord NonPooledConnectionPool []
  IConnectionPool
  (get-conn     [this spec] (make-new-connection spec))
  (release-conn [this conn] (close-conn conn))
  (release-conn [this conn exception] (close-conn conn)))

(def non-pooled-connection-pool
  "A degenerate connection pool. Gives us a pool-like interface for non-pooled
  connections."
  (NonPooledConnectionPool.))

(defn make-connection-factory []
  (reify KeyedPoolableObjectFactory
    (makeObject      [this spec] (make-new-connection spec))
    (activateObject  [this spec conn])
    (validateObject  [this spec conn] (conn-alive? conn))
    (passivateObject [this spec conn])
    (destroyObject   [this spec conn] (close-conn conn))))

(defn set-pool-option [^GenericKeyedObjectPool pool [opt v]]
  (case opt
    :max-active                    (.setMaxActive pool v)
    :max-total                     (.setMaxTotal pool v)
    :min-idle                      (.setMinIdle pool v)
    :max-idle                      (.setMaxIdle pool v)
    :max-wait                      (.setMaxWait pool v)
    :lifo?                         (.setLifo pool v)
    :test-on-borrow?               (.setTestOnBorrow pool v)
    :test-on-return?               (.setTestOnReturn pool v)
    :test-while-idle?              (.setTestWhileIdle pool v)
    :when-exhausted-action         (.setWhenExhaustedAction pool v)
    :num-tests-per-eviction-run    (.setNumTestsPerEvictionRun pool v)
    :time-between-eviction-runs-ms (.setTimeBetweenEvictionRunsMillis pool v)
    :min-evictable-idle-time-ms    (.setMinEvictableIdleTimeMillis pool v)
    (throw (Exception. (str "Unknown pool option: " opt))))
  pool)