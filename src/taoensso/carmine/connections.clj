(ns taoensso.carmine.connections
  "Handles socket connection lifecycle. Pool is implemented with Apache Commons
  pool. Originally adapted from redis-clojure."
  {:author "Peter Taoussanis"}
  (:require [taoensso.encore           :as encore]
            [taoensso.carmine.protocol :as protocol])
  (:import  [java.net Socket URI]
            [java.io BufferedInputStream DataInputStream BufferedOutputStream]
            [org.apache.commons.pool2 KeyedPooledObjectFactory]
            [org.apache.commons.pool2.impl GenericKeyedObjectPool DefaultPooledObject]))

(encore/declare-remote taoensso.carmine/ping
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
    (if (:listener? spec) true ; TODO Waiting on Ref. http://goo.gl/LPhIO
      (= "PONG" (try (->> (taoensso.carmine/ping)
                          (protocol/with-replies*)
                          (protocol/with-context this))
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

(defn make-new-connection [{:keys [host port password timeout-ms db] :as spec}]
  (let [buff-size 16384 ; Err on the large size since we're pooling
        socket (doto (Socket. ^String host ^Integer port)
                 (.setTcpNoDelay true)
                 (.setKeepAlive true)
                 (.setSoTimeout ^Integer (or timeout-ms 0)))
        conn (->Connection socket spec (-> (.getInputStream socket)
                                           (BufferedInputStream. buff-size)
                                           (DataInputStream.))
                                       (-> (.getOutputStream socket)
                                           (BufferedOutputStream. buff-size)))]
    (when password (->> (taoensso.carmine/auth password)
                        (protocol/with-replies*)
                        (protocol/with-context conn)))
    (when (and db (not (zero? db))) (->> (taoensso.carmine/select (str db))
                                         (protocol/with-replies*)
                                         (protocol/with-context conn)))
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

(defn set-pool-option [^GenericKeyedObjectPool pool [opt v]]
  (case opt
    ;; :max-active                 (.setMaxActive pool v)
    :max-total                     (.setMaxTotal pool v)
    ;; :min-idle                   (.setMinIdle pool v)
    ;; :max-idle                   (.setMaxIdle pool v)
    ;; :max-wait                   (.setMaxWait pool v)
    :lifo?                         (.setLifo pool v)
    :test-on-borrow?               (.setTestOnBorrow pool v)
    :test-on-return?               (.setTestOnReturn pool v)
    :test-while-idle?              (.setTestWhileIdle pool v)
    ;; :when-exhausted-action      (.setWhenExhaustedAction pool v)
    :num-tests-per-eviction-run    (.setNumTestsPerEvictionRun pool v)
    :time-between-eviction-runs-ms (.setTimeBetweenEvictionRunsMillis pool v)
    :min-evictable-idle-time-ms    (.setMinEvictableIdleTimeMillis pool v)
    (throw (Exception. (str "Unknown pool option: " opt))))
  pool)

(def ^:private pool-cache "{<pool-opts> <pool>}" (atom {}))
(defn conn-pool ^java.io.Closeable [pool-opts & [use-cache?]]
  @(encore/swap-val! pool-cache pool-opts
     (fn [?dv]
       (if (and ?dv use-cache?) ?dv
         (delay
          (cond
           (identical? pool-opts :none) (->NonPooledConnectionPool)
           ;; Pass through pre-made pools (note that test reflects):
           (satisfies? IConnectionPool pool-opts) pool-opts
           :else
           (let [defaults {:test-while-idle?              true
                           :num-tests-per-eviction-run    -1
                           :min-evictable-idle-time-ms    60000
                           :time-between-eviction-runs-ms 30000}]
             (->ConnectionPool
              (reduce set-pool-option
                (GenericKeyedObjectPool. (make-connection-factory))
                (merge defaults pool-opts))))))))))

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
   (fn [{:keys [uri host port password timeout-ms db] :as spec-opts}]
     (let [defaults  {:host "127.0.0.1" :port 6379}
           spec-opts (if-let [timeout (:timeout spec-opts)] ; Deprecated opt
                       (assoc spec-opts :timeout-ms timeout)
                       spec-opts)]
       (merge defaults spec-opts (parse-uri uri))))))

(defn pooled-conn "Returns [<open-pool> <pooled-connection>]."
  ;; [pool-opts spec-opts]
  [{:as conn-opts pool-opts :pool spec-opts :spec}]
  (let [spec (conn-spec spec-opts)
        pool (conn-pool pool-opts :use-cache)]
    (try (try [pool (get-conn pool spec)]
              (catch IllegalStateException e ; Cached pool's gone bad
                (let [pool (conn-pool pool-opts)]
                  [pool (get-conn pool spec)])))
         (catch Exception e
           (throw (Exception. "Carmine connection error" e))))))
