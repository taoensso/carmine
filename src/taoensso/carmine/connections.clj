(ns taoensso.carmine.connections
  "Handles life cycle of socket connections to Redis server. Connection pool is
  implemented using Apache Commons pool. Adapted from redis-clojure."
  {:author "Peter Taoussanis"}
  (:require [taoensso.carmine (utils :as utils) (protocol :as protocol)])
  (:import  [java.net Socket URI]
            [java.io BufferedInputStream DataInputStream BufferedOutputStream]
            [org.apache.commons.pool KeyedPoolableObjectFactory]
            [org.apache.commons.pool.impl GenericKeyedObjectPool]))

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
  ;; TODO Could in- & out- stream not be a pre-computed field?
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
                                                         conn))
  java.io.Closeable
  (close [this] (.close pool)))

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

;; A degenerate connection pool: gives pool-like interface for non-pooled conns
(defrecord NonPooledConnectionPool []
  IConnectionPool
  (get-conn     [this spec] (make-new-connection spec))
  (release-conn [this conn] (close-conn conn))
  (release-conn [this conn exception] (close-conn conn)))

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

(def ^:private pool-cache (atom {}))

(defn conn-pool
  [opts & [cached?]]
  (if (instance? ConnectionPool opts)
    opts ; 1.x backwards compatiblity, testing
    (if-let [dp (and cached? (@pool-cache opts))]
      @dp
      (let [dp (delay
                (if (= opts :none) (NonPooledConnectionPool.)
                    (let [defaults {:test-while-idle?              true
                                    :num-tests-per-eviction-run    -1
                                    :min-evictable-idle-time-ms    60000
                                    :time-between-eviction-runs-ms 30000}]
                      (ConnectionPool.
                       (reduce set-pool-option
                               (GenericKeyedObjectPool. (make-connection-factory))
                               (merge defaults opts))))))]
        (swap! pool-cache assoc opts dp)
        @dp))))

(comment (conn-pool :none) (conn-pool {}))

(defn- parse-uri [uri]
  (when uri
    (let [^URI uri (if (instance? URI uri) uri (URI. uri))
          [user password] (.split (str (.getUserInfo uri)) ":")
          port (.getPort uri)]
      (-> {:host (.getHost uri)}
          (#(if (pos? port) (assoc % :port     port)     %))
          (#(if password    (assoc % :password password) %))))))

(comment (parse-uri "redis://redistogo:pass@panga.redistogo.com:9475/"))

(def conn-spec
  (memoize
   (fn [{:keys [uri host port password timeout-ms db] :as opts}]
     (let [defaults {:host "127.0.0.1" :port 6379}
           opts     (if-let [timeout (:timeout opts)] ; Support deprecated opt
                      (assoc opts :timeout-ms timeout)
                      opts)]
       (merge defaults opts (parse-uri uri))))))

(defn pooled-conn "Returns [<open-pool> <pooled-connection>]."
  [pool-opts spec-opts]
  (let [spec (conn-spec spec-opts)]
    (let [pool (conn-pool pool-opts true)]
      (try (try [pool (get-conn pool spec)]
                (catch IllegalStateException e
                  (let [pool (conn-pool pool-opts)]
                    [pool (get-conn pool spec)])))
        (catch Exception e
          (throw (Exception. "Carmine connection error" e)))))))