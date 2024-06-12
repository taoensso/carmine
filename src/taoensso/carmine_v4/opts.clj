(ns ^:no-doc taoensso.carmine-v4.opts
  "Private ns, implementation detail.
  Carmine has a lot of options, so we do our best to:
    - Coerce and validate early when possible.
    - Throw detailed error messages when issues occur."
  (:require
   [clojure.string  :as str]
   [taoensso.encore :as enc :refer [have have?]]
   [taoensso.carmine-v4.utils :as utils])

  (:import
   [java.net Socket]
   [org.apache.commons.pool2.impl
    BaseGenericObjectPool
    GenericObjectPool GenericKeyedObjectPool]))

(comment (remove-ns 'taoensso.carmine-v4.opts))

(enc/declare-remote
  taoensso.carmine-v4/default-conn-opts
  taoensso.carmine-v4/default-sentinel-opts
  taoensso.carmine-v4.conns/conn-manager?
  taoensso.carmine-v4.sentinel/sentinel-spec?
  taoensso.carmine-v4.sentinel/sentinel-opts)

(do
  (alias 'core     'taoensso.carmine-v4)
  (alias 'conns    'taoensso.carmine-v4.conns)
  (alias 'sentinel 'taoensso.carmine-v4.sentinel))

;;;; Mutators

(defn set-socket-opts!
  ^Socket [^Socket s socket-opts]
  (enc/run-kv!
    (fn [k v]
      (case k
        ;; Carmine options, noop and pass through
        (:ssl :connect-timeout-ms :ready-timeout-ms) nil

        (:setKeepAlive    :keep-alive?)    (.setKeepAlive    s (boolean v))
        (:setOOBInline    :oob-inline?)    (.setOOBInline    s (boolean v))
        (:setTcpNoDelay   :tcp-no-delay?)  (.setTcpNoDelay   s (boolean v))
        (:setReuseAddress :reuse-address?) (.setReuseAddress s (boolean v))

        (:setReceiveBufferSize     :receive-buffer-size) (.setReceiveBufferSize s (int     v))
        (:setSendBufferSize        :send-buffer-size)    (.setSendBufferSize    s (int     v))
        (:setSoTimeout :so-timeout :read-timeout-ms)     (.setSoTimeout         s (int (or v 0)))

        ;; (:setSocketImplFactory :socket-impl-factory) (.setSocketImplFactory s v)
        (:setTrafficClass         :traffic-class)       (.setTrafficClass      s v)

        (:setSoLinger :so-linger)
        (let [[on? linger] (have vector? v)]
          (.setSoLinger s (boolean on?) (int linger)))

        (:setPerformancePreferences :performance-preferences)
        (let [[conn-time latency bandwidth] (have vector? v)]
          (.setPerformancePreferences s (int conn-time) (int latency) (int bandwidth)))

        (throw
          (ex-info "[Carmine] Unknown socket option specified"
            {:eid      :carmine.conns/unknown-socket-option
             :opt-key  (enc/typed-val k)
             :opt-val  (enc/typed-val v)
             :all-opts socket-opts}))))
    socket-opts)
  s)

(defn set-pool-opts!
  [^BaseGenericObjectPool p pool-opts]
  (let [neg-duration (java.time.Duration/ofSeconds -1)]
    (enc/run-kv!
      (fn [k v]
        (case k
          ;;; org.apache.commons.pool2.impl.GenericObjectPool
          (:setMinIdle  :min-idle) (.setMinIdle ^GenericObjectPool p (int (or v -1)))
          (:setMaxIdle  :max-idle) (.setMaxIdle ^GenericObjectPool p (int (or v -1)))

          ;;; org.apache.commons.pool2.impl.GenericKeyedObjectPool
          (:setMinIdlePerKey  :min-idle-per-key)  (.setMinIdlePerKey  ^GenericKeyedObjectPool p (int (or v -1)))
          (:setMaxIdlePerKey  :max-idle-per-key)  (.setMaxIdlePerKey  ^GenericKeyedObjectPool p (int (or v -1)))
          (:setMaxTotalPerKey :max-total-per-key) (.setMaxTotalPerKey ^GenericKeyedObjectPool p (int (or v -1)))

          ;;; org.apache.commons.pool2.impl.BaseGenericObjectPool
          (:setBlockWhenExhausted :block-when-exhausted?) (.setBlockWhenExhausted p (boolean v))
          (:setLifo               :lifo?)                 (.setLifo               p (boolean v))

          (:setMaxTotal      :max-total)   (.setMaxTotal      p (int  (or v -1)))
          (:setMaxWaitMillis :max-wait-ms) (.setMaxWaitMillis p (long (or v -1)))
          (:setMaxWait       :max-wait)    (.setMaxWait       p       (or v neg-duration))

          (:setMinEvictableIdleTimeMillis     :min-evictable-idle-time-ms)      (.setMinEvictableIdleTimeMillis     p (long (or v -1)))
          (:setMinEvictableIdle               :min-evictable-idle)              (.setMinEvictableIdle               p       (or v neg-duration))
          (:setSoftMinEvictableIdleTimeMillis :soft-min-evictable-idle-time-ms) (.setSoftMinEvictableIdleTimeMillis p (long (or v -1)))
          (:setSoftMinEvictableIdle           :soft-min-evictable-idle)         (.setSoftMinEvictableIdle           p       (or v neg-duration))
          (:setNumTestsPerEvictionRun         :num-tests-per-eviction-run)      (.setNumTestsPerEvictionRun         p (int  (or v 0)))
          (:setTimeBetweenEvictionRunsMillis  :time-between-eviction-runs-ms)   (.setTimeBetweenEvictionRunsMillis  p (long (or v -1)))
          (:setTimeBetweenEvictionRuns        :time-between-eviction-runs)      (.setTimeBetweenEvictionRuns        p       (or v neg-duration))

          (:setEvictorShutdownTimeoutMillis :evictor-shutdown-timeout-ms) (.setEvictorShutdownTimeoutMillis p (long v))
          (:setEvictorShutdownTimeout       :evictor-shutdown-timeout)    (.setEvictorShutdownTimeout       p       v)

          (:setTestOnCreate  :test-on-create?)  (.setTestOnCreate  p (boolean v))
          (:setTestWhileIdle :test-while-idle?) (.setTestWhileIdle p (boolean v))
          (:setTestOnBorrow  :test-on-borrow?)  (.setTestOnBorrow  p (boolean v))
          (:setTestOnReturn  :test-on-return?)  (.setTestOnReturn  p (boolean v))

          (:setSwallowedExceptionListener :swallowed-exception-listener)
          (.setSwallowedExceptionListener p v)
          (throw
            (ex-info "[Carmine] Unknown pool option specified"
              {:eid      :carmine.conns/unknown-pool-option
               :opt-key  (enc/typed-val k)
               :opt-val  (enc/typed-val v)
               :all-opts pool-opts}))))
      pool-opts))
  p)

;;;; Misc

(defn parse-sock-addr
  "Returns valid [<host-string> <port-int>] socket address pair, or throws.
  Retains metadata (server name, comments, etc.)."
  ( [host port         ]            [(have string? host) (enc/as-int port)])
  ( [host port metadata] (with-meta [(have string? host) (enc/as-int port)] metadata))
  ([[host port :as addr]]
   (have? string? host)
   (assoc addr 1 (enc/as-int port))))

(defn descr-sock-addr
  "Returns [<host-string> <port-int> <?meta>] socket address."
  [addr] (if-let [m (meta addr)] (conj addr m) addr))

(defn get-sentinel-server [conn-opts]
  (let [{:keys [server]}   conn-opts]
    (when (and (map? server) (get server :sentinel-spec))
      server)))

;;;;

(declare ^:private parse-string-server ^:private parse-sentinel-server)

(defn parse-conn-opts
  "Returns valid parsed conn-opts, or throws."
  [in-sentinel-opts? conn-opts]
  (try
    (have? [:or nil? map?] conn-opts)
    (let [default-conn-opts
          (if in-sentinel-opts?
            (dissoc core/default-conn-opts :server)
            (do     core/default-conn-opts))

          conn-opts (utils/merge-opts default-conn-opts conn-opts)
          {:keys [server cbs socket-opts buffer-opts init]}  conn-opts
          {:keys [auth]} init]

      (if in-sentinel-opts?
        ;; [host port] of Sentinel server will be auto added by resolver
        (have? [:ks<= #{:id #_:server :cbs :socket-opts :buffer-opts :init}] conn-opts)
        (have? [:ks<= #{:id   :server :cbs :socket-opts :buffer-opts :init}] conn-opts))

      (have? [:ks<= #{:on-conn-close :on-conn-error}] cbs)
      (have? [:or nil? fn?] :in                 (vals cbs))

      (when socket-opts (set-socket-opts! (java.net.Socket.) socket-opts)) ; Dry run
      (have? [:ks<= #{:init-size-in :init-size-out}] buffer-opts)

      (if in-sentinel-opts?
        (have? [:ks<= #{:commands :auth :resp3? #_:client-name #_:select-db}] init)
        (have? [:ks<= #{:commands :auth :resp3?   :client-name   :select-db}] init))

      (have? [:ks<= #{:username :password}] auth)

      (if in-sentinel-opts?
        (do               conn-opts) ; Doesn't have :server
        (utils/merge-opts conn-opts
          (try
            (enc/cond
              (vector?         server) {:server   (parse-sock-addr     server)}
              (string?         server) (have map? (parse-string-server server))
              (map?            server)
              (case (set (keys server))
                #{:host :port}
                (let [{:keys [host port]} server]               {:server (parse-sock-addr host port (meta server))})
                (#{:master-name :sentinel-spec               }
                 #{:master-name :sentinel-spec :sentinel-opts}) {:server (parse-sentinel-server server)}

                (do (throw (ex-info "Unexpected `:server` keys" {:keys (keys server)}))))
              :else (throw (ex-info "Unexpected `:server` type" {:type (type server)})))

            (catch Throwable t
              (throw
                (ex-info "[Carmine] Invalid Redis server specification in connection options"
                  {:eid      :carmine.conn-opts/invalid-server
                   :server   (enc/typed-val server)
                   :expected '(or uri-string [host port] {:keys [host port]}
                                {:keys [master-name sentinel-spec sentinel-opts]})}
                  t)))))))

    (catch Throwable t
      (throw
        (ex-info "[Carmine] Invalid connection options"
          {:eid       :carmine.conn-opts/invalid
           :conn-opts (assoc (enc/typed-val conn-opts) :id (get conn-opts :id))
           :purpose
           (if in-sentinel-opts?
             :conn-to-sentinel-server
             :conn-to-redis-server)}
          t)))))

;;;;

(defn- parse-string-server
  "\"rediss://user:pass@x.y.com:9475/3\" ->
    {:keys [server init socket-opts]}, etc."
  [s]
  (let [uri    (java.net.URI. (have string? s))
        server [(.getHost uri) (.getPort uri)]
        init
        (enc/assoc-some nil
          :auth
          (let [[username password] (.split (str (.getUserInfo uri)) ":")]
            (enc/assoc-some nil
              :username (enc/as-?nempty-str username)
              :password (enc/as-?nempty-str password)))

          :select-db
          (when-let [[_ db-str] (re-matches #"/(\d+)$" (.getPath uri))]
            (Integer. ^String db-str)))

        socket-opts
        (when-let [scheme (.getScheme uri)]
          (when (contains? #{"rediss" "https"} (str/lower-case scheme))
            {:ssl true}))]

    (enc/assoc-some {:server server}
      :init        init
      :socket-opts socket-opts)))

(comment
  [(parse-string-server "redis://user:pass@x.y.com:9475/3")
   (parse-string-server "redis://:pass@x.y.com.com:9475/3")
   (parse-string-server     "redis://user:@x.y.com:9475/3")
   (parse-string-server    "rediss://user:@x.y.com:9475/3")])

(defn- parse-sentinel-server [server]
  (have? map? server)
  (let [{:keys [master-name sentinel-spec sentinel-opts]} server

        master-name (enc/as-qname (have [:or string? enc/named?] master-name))
        sentinel-opts
        (let [sentinel-spec (have sentinel/sentinel-spec? sentinel-spec)
              sentinel-opts
              (utils/merge-opts core/default-sentinel-opts
                (sentinel/sentinel-opts sentinel-spec)
                sentinel-opts)]

          (try
            (have? map? sentinel-opts)
            (have? [:ks<= #{:id :conn-opts :cbs
                            :retry-delay-ms :resolve-timeout-ms :clear-timeout-ms
                            :update-sentinels? :update-replicas? :prefer-read-replica?}]
              sentinel-opts)

            (let [{:keys [cbs]} sentinel-opts]
              (have? [:ks<= #{:on-resolve-success :on-resolve-error
                              :on-changed-master :on-changed-replicas :on-changed-sentinels}] cbs)
              (have? [:or nil? fn?] :in                                                 (vals cbs)))

            (if-let [conn-opts (not-empty (get sentinel-opts :conn-opts))]
              (assoc sentinel-opts :conn-opts (parse-conn-opts true conn-opts))
              (do    sentinel-opts))

            (catch Throwable t
              (throw
                (ex-info "[Carmine] Invalid Sentinel options"
                  {:eid :carmine.sentinel-opts/invalid
                   :sentinel-opts
                   (assoc (enc/typed-val sentinel-opts)
                     :id (get sentinel-opts :id))}
                  t)))))]

    (assoc server
      :master-name   master-name
      :sentinel-opts sentinel-opts)))
