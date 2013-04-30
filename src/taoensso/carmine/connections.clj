(ns taoensso.carmine.connections
  "Handles life cycle of socket connections to Redis server. Connection pool is
  implemented using Apache Commons pool. Adapted from redis-clojure."
  {:author "Peter Taoussanis"}
  (:require [taoensso.carmine (utils :as utils) (protocol :as protocol)])
  (:import  java.net.Socket
            [java.io BufferedInputStream DataInputStream BufferedOutputStream]
            [org.apache.commons.pool KeyedPoolableObjectFactory]
            [org.apache.commons.pool.impl GenericKeyedObjectPool]))

;; TODO Implement Redis Sentinel client draft spec:
;; http://redis.io/topics/sentinel-clients

;; Hack to allow cleaner separation of ns concerns
(utils/declare-remote taoensso.carmine/ping
                      taoensso.carmine/auth
                      taoensso.carmine/select)

;; Interface for socket connections to Redis server
(defprotocol IConnection
  (get-spec    [conn])
  (in-stream   [conn])
  (out-stream  [conn])
  (conn-alive? [conn])
  (close-conn  [conn]))

(defrecord Connection [^Socket socket spec]
  IConnection
  (get-spec    [this] spec)
  (in-stream   [this] (-> (.getInputStream socket)
                          (BufferedInputStream.)
                          (DataInputStream.)))
  (out-stream  [this] (-> (.getOutputStream socket)
                          (BufferedOutputStream.)))
  (conn-alive? [this]
    (if (:listener? spec)
      true ; TODO Waiting on Redis update, Ref. http://goo.gl/LPhIO
      (= "PONG" (try (protocol/with-context this (taoensso.carmine/ping))
                     (catch Exception _)))))
  (close-conn  [this] (.close socket)))

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
  [{:keys [host port password timeout-ms db] :as spec}]
  (let [socket (doto (Socket. ^String host ^Integer port)
                 (.setTcpNoDelay true)
                 (.setKeepAlive true)
                 (.setSoTimeout ^Integer (or timeout-ms 0)))
        conn (Connection. socket spec)]
    (when password (protocol/with-context conn (taoensso.carmine/auth password)))
    (when (and db (not (zero? db))) (protocol/with-context conn
                                      (taoensso.carmine/select (str db))))
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