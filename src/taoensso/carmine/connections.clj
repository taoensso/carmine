(ns taoensso.carmine.connections
  "Handles socket connection lifecycle. Pool is implemented with Apache Commons
  pool. Originally adapted from redis-clojure."
  {:author "Peter Taoussanis"}
  (:require [taoensso.encore           :as enc]
            [taoensso.carmine.protocol :as protocol])
  (:import  [java.net InetSocketAddress Socket URI]
            [java.io BufferedInputStream DataInputStream BufferedOutputStream]
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
  (conn-alive? [this])
  (close-conn  [this]))

(defrecord Connection [^Socket socket spec in out]
  IConnection
  (conn-alive? [this]
    (if (:listener? spec)
      true ; TODO Waiting on Ref. http://goo.gl/LPhIO
      (= "PONG"
        (try
          (protocol/with-context this
            (protocol/with-replies
              (taoensso.carmine/ping)))
          (catch Exception _)))))
  (close-conn [_] (.close socket)))

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

(defn make-new-connection
  [{:keys [host port password db conn-setup-fn
           conn-timeout-ms read-timeout-ms timeout-ms] :as spec}]
  (let [;; :timeout-ms controls both :conn-timeout-ms and :read-timeout-ms
        ;; unless those are specified individually
        ;; :or   {conn-timeout-ms (or timeout-ms 4000)
        ;;        read-timeout-ms timeout-ms} ; Ref. http://goo.gl/XULHCd
        conn-timeout-ms (get spec :conn-timeout-ms (or timeout-ms 4000))
        read-timeout-ms (get spec :read-timeout-ms     timeout-ms)

        socket-address (InetSocketAddress. ^String host ^Integer port)
        socket (enc/doto-cond [expr (Socket.)]
                 :always         (.setTcpNoDelay   true)
                 :always         (.setKeepAlive    true)
                 :always         (.setReuseAddress true)
                 ;; :always      (.setSoLinger     true 0)
                 read-timeout-ms (.setSoTimeout ^Integer expr))
        _ (if conn-timeout-ms
            (.connect socket socket-address conn-timeout-ms)
            (.connect socket socket-address))

        buff-size 16384 ; Err on the large size since we're pooling
        conn (Connection. socket spec
               (-> (.getInputStream socket)
                   (BufferedInputStream. buff-size)
                   (DataInputStream.))
               (-> (.getOutputStream socket)
                   (BufferedOutputStream. buff-size)))

        db (when (and db (not (zero? db))) db)]

    (when (or password db conn-setup-fn)
      (protocol/with-context conn
        (protocol/with-replies ; Discard replies
          (when password (taoensso.carmine/auth password))
          (when db       (taoensso.carmine/select (str db)))
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

    (throw (ex-info (str "Unknown pool option: " k) {:option k})))
  pool)

(def conn-pool
  (enc/memoize_
    (fn [pool-opts]
      (cond
        (identical? pool-opts :none) (->NonPooledConnectionPool)
        ;; Pass through pre-made pools (note that test reflects):
        (satisfies? IConnectionPool pool-opts) pool-opts
        :else
        (let [jedis-defaults ; Ref. http://goo.gl/y1mDbE
              {:test-while-idle?              true  ; from false
               :num-tests-per-eviction-run    -1    ; from 3
               :min-evictable-idle-time-ms    60000 ; from 1800000
               :time-between-eviction-runs-ms 30000 ; from -1
               }
              carmine-defaults
              {:max-total-per-key 16 ; from 8
               }]
          (ConnectionPool.
            (reduce-kv set-pool-option
              (GenericKeyedObjectPool. (make-connection-factory))
              (merge jedis-defaults carmine-defaults pool-opts))))))))

;; (defn uncached-conn-pool [pool-opts] (conn-pool :mem/fresh pool-opts))
(comment (conn-pool :none) (conn-pool {}))

(defn- parse-uri [uri]
  (when uri
    (let [^URI uri (if (instance? URI uri) uri (URI. uri))
          [user password] (.split (str (.getUserInfo uri)) ":")
          port (.getPort uri)
          db (if-let [[_ db-str] (re-matches #"/(\d+)$" (.getPath uri))]
               (Integer. ^String db-str))]
      (-> {:host (.getHost uri)}
          (#(if (pos? port)        (assoc % :port     port)     %))
          (#(if (and db (pos? db)) (assoc % :db       db)       %))
          (#(if password           (assoc % :password password) %))))))

(comment (parse-uri "redis://redistogo:pass@panga.redistogo.com:9475/7"))

(def conn-spec
  (enc/memoize_
   (fn [{:keys [uri host port password timeout-ms db
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
        (throw (ex-info "Carmine connection error" {} e))))))
