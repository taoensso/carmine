(ns ^:no-doc taoensso.carmine-v4.opts
  "Private option parsing implementation. Coerces and validates options early.
  Throws detailed errors for invalid options."
  (:require
   [clojure.string  :as str]
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.carmine-v4.utils :as utils])

  (:import
   [java.net Socket URI URLDecoder]
   [org.apache.commons.pool2.impl
    BaseGenericObjectPool
    GenericObjectPool GenericKeyedObjectPool]))

(comment (remove-ns 'taoensso.carmine-v4.opts))

(enc/declare-remote
  taoensso.carmine-v4/default-conn-opts
  taoensso.carmine-v4/default-sentinel-spec-opts
  taoensso.carmine-v4/default-sentinel-opts
  taoensso.carmine-v4/default-cluster-spec-opts
  taoensso.carmine-v4/default-cluster-opts
  taoensso.carmine-v4.conns/conn-manager?
  taoensso.carmine-v4.sentinel/sentinel-spec?
  taoensso.carmine-v4.cluster/cluster-spec?)

(do
  (alias 'core     'taoensso.carmine-v4)
  (alias 'conns    'taoensso.carmine-v4.conns)
  (alias 'sentinel 'taoensso.carmine-v4.sentinel)
  (alias 'cluster  'taoensso.carmine-v4.cluster))

;;;; Mutators

(defn set-socket-opts!
  ^Socket [^Socket s socket-opts]
  (enc/run-kv!
    (fn [k v]
      (case k
        ;; Carmine options: validated here (so that config errors surface
        ;; eagerly at parse time), applied elsewhere
        :ssl
        (when-not (or (nil? v) (boolean? v) (fn? v))
          (truss/ex-info! "[Carmine] Expected `:ssl` socket option to be nil, boolean, or fn"
            {:eid     :carmine.conns/invalid-socket-option
             :opt-key k, :opt-val (enc/typed-val v)}))

        (:connect-timeout-ms :read-timeout-ms :ready-timeout-ms :init-timeout-ms)
        (do
          (when-not (or (nil? v) (and (enc/pos-int? v) (<= v Integer/MAX_VALUE)))
            (truss/ex-info! "[Carmine] Expected socket timeout to be nil or a positive integer <= 2147483647"
              {:eid     :carmine.conns/invalid-socket-option
               :opt-key k, :opt-val (enc/typed-val v)
               :expected :nil-or-pos-int32}))
          (when (= k :read-timeout-ms) (.setSoTimeout s (int (or v 0)))))

        :keep-alive?    (.setKeepAlive    s (boolean v))
        :oob-inline?    (.setOOBInline    s (boolean v))
        :tcp-no-delay?  (.setTcpNoDelay   s (boolean v))
        :reuse-address? (.setReuseAddress s (boolean v))

        :receive-buffer-size          (.setReceiveBufferSize s (int     v))
        :send-buffer-size             (.setSendBufferSize    s (int     v))

        ;; :socket-impl-factory (.setSocketImplFactory s v)
        :traffic-class (.setTrafficClass s v)

        :so-linger
        (let [[on? linger] (truss/have vector? v)]
          (.setSoLinger s (boolean on?) (int linger)))

        :performance-preferences
        (let [[conn-time latency bandwidth] (truss/have vector? v)]
          (.setPerformancePreferences s (int conn-time) (int latency) (int bandwidth)))

        (truss/ex-info! "[Carmine] Unknown socket option specified"
          {:eid      :carmine.conns/unknown-socket-option
           :opt-key  (if (keyword? k) k (enc/typed-val k))
           :opt-val  (enc/typed-val v)
           :all-opts socket-opts})))
    socket-opts)
  s)

(defn set-pool-opts!
  [^BaseGenericObjectPool p pool-opts]
  (enc/run-kv!
    (fn [k v]
      (case k
        ;; Carmine validation policy; consumed by connection managers.
        :ready-check-after-idle-ms
        (when-not (or (nil? v)
                    (and (integer? v) (<= 0 v Long/MAX_VALUE)))
          (truss/ex-info! "[Carmine] Invalid pooled connection readiness interval"
            {:eid :carmine.conns/invalid-pool-option
             :opt-key k, :opt-val (enc/typed-val v)
             :expected :nil-or-nat-int}))

        ;;; org.apache.commons.pool2.impl.GenericObjectPool
        (:min-idle :max-idle)
        (if (instance? GenericObjectPool p)
          (case k
            :min-idle (.setMinIdle ^GenericObjectPool p (int (or v -1)))
            :max-idle (.setMaxIdle ^GenericObjectPool p (int (or v -1))))
          (truss/ex-info! "[Carmine] Pool option isn't supported by Cluster managers, use its `-per-key` version"
            {:eid :carmine.conns/pool-option-not-supported
             :opt-key k, :opt-val (enc/typed-val v)}))

        ;;; org.apache.commons.pool2.impl.GenericKeyedObjectPool
        (:min-idle-per-key :max-idle-per-key :max-total-per-key)
        (if (instance? GenericKeyedObjectPool p)
          (case k
            :min-idle-per-key  (.setMinIdlePerKey  ^GenericKeyedObjectPool p (int (or v -1)))
            :max-idle-per-key  (.setMaxIdlePerKey  ^GenericKeyedObjectPool p (int (or v -1)))
            :max-total-per-key (.setMaxTotalPerKey ^GenericKeyedObjectPool p (int (or v -1))))
          (truss/ex-info! "[Carmine] Pool option is only for Cluster managers, use its version without `-per-key`"
            {:eid :carmine.conns/pool-option-not-supported
             :opt-key k, :opt-val (enc/typed-val v)}))

        ;;; org.apache.commons.pool2.impl.BaseGenericObjectPool
        :block-when-exhausted? (.setBlockWhenExhausted p (boolean v))
        :lifo?                 (.setLifo               p (boolean v))

        :max-total   (.setMaxTotal      p (int  (or v -1)))
        :max-wait-ms (.setMaxWaitMillis p (long (or v -1)))

        :min-evictable-idle-time-ms      (.setMinEvictableIdleTimeMillis     p (long (or v -1)))
        :soft-min-evictable-idle-time-ms (.setSoftMinEvictableIdleTimeMillis p (long (or v -1)))
        :num-tests-per-eviction-run      (.setNumTestsPerEvictionRun         p (int  (or v 0)))
        :time-between-eviction-runs-ms   (.setTimeBetweenEvictionRunsMillis  p (long (or v -1)))

        ;; Nb nil intentionally retains the Commons Pool default (10 secs),
        ;; which differs from the negative (no-wait) sentinel:
        :evictor-shutdown-timeout-ms (when (some? v) (.setEvictorShutdownTimeoutMillis p (long v)))

        :test-on-create?  (.setTestOnCreate  p (boolean v))
        :test-while-idle? (.setTestWhileIdle p (boolean v))
        :test-on-borrow?  (.setTestOnBorrow  p (boolean v))
        :test-on-return?  (.setTestOnReturn  p (boolean v))

        :swallowed-exception-listener
        (.setSwallowedExceptionListener p v)
        (truss/ex-info! "[Carmine] Unknown pool option specified"
          {:eid      :carmine.conns/unknown-pool-option
           :opt-key  (enc/typed-val k)
           :opt-val  (enc/typed-val v)
           :all-opts pool-opts})))
    pool-opts)
  p)

;;;; Misc

(defn parse-sock-addr
  "Returns a valid `[host-string port-int]` socket address, or throws. Keeps
  metadata such as the server name and comments."
  ([host port] (parse-sock-addr host port nil))
  ([host port metadata]
   (let [host (truss/have string? host)
         port (enc/as-int port)]
     (when-not (<= 1 port 65535)
       (truss/ex-info! "[Carmine] Invalid socket address port"
         {:eid :carmine.conns/invalid-socket-address
          :host host, :port port, :expected :integer-from-1-to-65535}))
     (cond-> [host port] metadata (with-meta metadata))))
  ([addr]
   (when-not (and (vector? addr) (= (count addr) 2))
     (truss/ex-info! "[Carmine] Invalid socket address"
       {:eid :carmine.conns/invalid-socket-address
        :address (enc/typed-val addr)
        :expected '[host port]}))
   (let [[host port] (parse-sock-addr (nth addr 0) (nth addr 1))]
     (with-meta [host port] (meta addr)))))

(defn descr-sock-addr
  "Returns `[host-string port-int optional-metadata]` for a socket address."
  [addr] (if-let [m (meta addr)] (conj addr m) addr))

(defn- have-pos-timeout! [k v]
  (when-not (pos-int? v)
    (truss/ex-info! "[Carmine] Expected timeout to be a positive integer"
      {:eid :carmine.conn-opts/invalid-timeout
       :opt-key k, :opt-val (enc/typed-val v)
       :expected :pos-int})))

(defn get-sentinel-server [conn-opts]
  (let [{:keys [server]}   conn-opts]
    (when (and (map? server) (get server :sentinel-spec))
      server)))

(defn get-cluster-server [conn-opts]
  (let [{:keys [server]} conn-opts]
    (when (and (map? server) (get server :cluster-spec))
      server)))

;;;;

(declare
  ^:private parse-string-server
  ^:private parse-sentinel-server
  ^:private parse-cluster-server)

(def ^:private resp-limit-keys
  #{:max-line-bytes :max-nesting-depth :max-blob-bytes
    :max-aggregate-elements :max-frame-bytes})

(defn sanitized-opts-cause
  "Returns a diagnostic cause without retaining raw validation input."
  [^Throwable t]
  (let [data (ex-data t)
        safe-data
        (utils/redact-secrets
          (select-keys data
            [:eid :opt-key :opt-val :expected :server :select-db :keys]))
        safe-data
        (cond-> (assoc safe-data
                  :eid (or (:eid safe-data) :carmine.conn-opts/validation-failed)
                  :error-class (.getName (class t)))
          (#{:auth :commands :password} (:opt-key data))
          (assoc :opt-val :carmine/redacted))]
    ;; This exception is a redaction boundary: do not reintroduce caller context.
    (binding [truss/*ctx* nil]
      (truss/ex-info "[Carmine] Option validation failed" safe-data))))

(defn- have-known-keys! [known-keys opts]
  (when-let [unknown (seq (remove known-keys (keys opts)))]
    (truss/ex-info! "[Carmine] Unknown option keys"
      {:eid :carmine.conn-opts/unknown-keys
       :keys (vec unknown), :expected known-keys})))

(defn- have-opt! [opt-key expected pred opt-val]
  (when-not (pred opt-val)
    (truss/ex-info! (str "[Carmine] `" opt-key "` should be " expected)
      {:eid :carmine.conn-opts/invalid-option
       :opt-key opt-key, :opt-val (enc/typed-val opt-val)})))

(defn invalid-opts-msg
  "Returns `base-msg` with a summary of validation error `t` when possible.
  Only Carmine's own (static) messages are used: others could include
  secrets, e.g. from a URI."
  [base-msg t]
  (let [{:keys [eid opt-key] :as data} (ex-data t)]
    (enc/cond
      (= eid :carmine.conn-opts/unknown-keys)
      (str base-msg ": unknown keys " (:keys data))

      (and (keyword? eid) (str/starts-with? (str eid) ":carmine."))
      (let [msg (str/replace-first (ex-message t) "[Carmine] " "")
            msg (str (str/lower-case (subs msg 0 1)) (subs msg 1))]
        (str base-msg ": " msg
          (when (and (keyword? opt-key) (not (str/includes? msg (str opt-key))))
            (str " (`" opt-key "`)"))))

      :else base-msg)))

(defn- parse-resp-opts [resp-opts]
  (truss/have? map? resp-opts)
  (have-known-keys! #{:limits} resp-opts)
  (let [limits (get resp-opts :limits)]
    (truss/have? map? limits)
    (have-known-keys! resp-limit-keys limits)
    (assoc resp-opts :limits
      (reduce-kv
        (fn [m k v]
          (if (nil? v)
            (assoc m k nil)
            (do
              (when-not (and (integer? v) (not (neg? v)) (<= v Long/MAX_VALUE))
                (truss/ex-info! "[Carmine] Invalid RESP resource limit"
                  {:eid :carmine.conn-opts/invalid-resp-limit
                   :opt-key k, :opt-val (enc/typed-val v)
                   :expected :nil-or-nat-int}))
              (assoc m k (long v)))))
        {} limits))))

(defn parse-conn-opts
  "Returns valid parsed connection options, or throws."
  [purpose conn-opts]
  (try
    (truss/have? [:el #{:redis :sentinel :cluster-discovery}] purpose)
    (when-not (or (nil? conn-opts) (map? conn-opts))
      (truss/ex-info! "[Carmine] Expected connection options to be nil or a map"
        {:eid :carmine.conn-opts/unexpected-type, :type (type conn-opts)}))
    (let [nested-purpose? (not= purpose :redis)
          default-conn-opts
          (if nested-purpose?
            (-> core/default-conn-opts (dissoc :server) (update :init dissoc :select-db))
            (do core/default-conn-opts))

          invalid-server!
          (fn [server t]
            (truss/ex-info! (invalid-opts-msg "[Carmine] Invalid `:server`" t)
              {:eid      :carmine.conn-opts/invalid-server
               :server   (get (utils/redact-secrets {:server server}) :server)
               :expected '(or uri-string [host port] {:keys [host port]}
                            {:keys [master-name sentinel-spec sentinel-opts]}
                            {:keys [cluster-spec cluster-opts]})}
              t))

          ;; URI parts (auth, db, TLS) act as defaults: explicit options win
          uri-opts
          (when-not nested-purpose?
            (let [server (get conn-opts :server (get default-conn-opts :server))]
              (when (string? server)
                (try
                  (truss/have map? (parse-string-server server))
                  (catch Throwable t (invalid-server! server t))))))

          conn-opts (enc/nested-merge default-conn-opts (dissoc uri-opts :server) conn-opts)
          {:keys [server cbs socket-opts buffer-opts resp-opts init]} conn-opts
          {:keys [auth]} init]

      (if nested-purpose?
        ;; [host port] of discovery server will be injected by resolver
        (have-known-keys! #{:id #_:server :cbs :socket-opts :buffer-opts :resp-opts :init} conn-opts)
        (have-known-keys! #{:id   :server :cbs :socket-opts :buffer-opts :resp-opts :init} conn-opts))

      (have-known-keys! #{:on-conn-close :on-conn-error :on-push-error} cbs)
      (truss/have? [:or nil? fn?] :in                 (vals cbs))

      (when socket-opts
        (with-open [socket (java.net.Socket.)]
          (set-socket-opts! socket socket-opts))) ; Dry run
      (have-known-keys! #{:init-size-in :init-size-out} buffer-opts)
      (doseq [[k v] buffer-opts]
        (when-not (and (pos-int? v) (<= v Integer/MAX_VALUE))
          (truss/ex-info! "[Carmine] Invalid initial connection buffer size"
            {:eid :carmine.conns/invalid-buffer-option
             :opt-key k, :opt-val (enc/typed-val v)
             :expected :pos-int32})))
      (let [resp-opts (parse-resp-opts resp-opts)]

        (if nested-purpose?
          (have-known-keys! #{:commands :auth :resp3? :client-name #_:select-db} init)
          (have-known-keys! #{:commands :auth :resp3? :client-name   :select-db} init))

        ;; Validate `:init` values eagerly so that config errors surface at
        ;; parse time rather than on first connection
        (let [{:keys [select-db resp3?]} init]
          (when-not (or (nil? select-db) (nat-int? select-db))
            (truss/ex-info! "[Carmine] Expected nil or non-negative integer `:select-db` init option"
              {:eid :carmine.conn-opts/invalid-init-option
               :opt-key :select-db, :opt-val (enc/typed-val select-db)}))

          (when-not (or (nil? resp3?) (boolean? resp3?))
            (truss/ex-info! "[Carmine] Expected nil or boolean `:resp3?` init option"
              {:eid :carmine.conn-opts/invalid-init-option
               :opt-key :resp3?, :opt-val (enc/typed-val resp3?)}))

          (when (some? auth)
            (when-not (map? auth)
              (truss/ex-info! "[Carmine] Expected nil or map `:auth` init option"
                {:eid :carmine.conn-opts/invalid-init-option
                 :opt-key :auth, :opt-val (enc/typed-val auth)}))

            (let [{:keys [username password]} auth]
              (when-not (or (nil? username) (string? username))
                (truss/ex-info! "[Carmine] Expected nil or string `:auth` `:username` init option"
                  {:eid :carmine.conn-opts/invalid-init-option
                   :opt-key :username, :opt-val (enc/typed-val username)}))

              (when-not (or (nil? password) (string? password))
                (truss/ex-info! "[Carmine] Expected nil or string `:auth` `:password` init option"
                  {:eid :carmine.conn-opts/invalid-init-option
                   :opt-key :password, :opt-val :carmine/redacted}))))

          (let [client-name (get init :client-name ::absent)]
            (when-not (or (identical? client-name ::absent) (nil? client-name)
                        (string? client-name) (fn? client-name))
              (truss/ex-info! "[Carmine] Expected nil, string, or fn `:client-name` init option"
                {:eid :carmine.conn-opts/invalid-init-option
                 :opt-key :client-name, :opt-val (enc/typed-val client-name)})))

          (let [commands (get init :commands)]
            (when (some? commands)
              (when-not (and (sequential? commands) (every? sequential? commands))
                (truss/ex-info! "[Carmine] Expected nil or sequence of command sequences `:commands` init option"
                  {:eid :carmine.conn-opts/invalid-init-option
                   :opt-key :commands, :opt-val (enc/typed-val commands)})))))

        (have-known-keys! #{:username :password} auth)

        (let [conn-opts (assoc conn-opts :resp-opts resp-opts)]
          (if nested-purpose?
            (do               conn-opts) ; Doesn't have :server
            (let [conn-opts
                  (enc/nested-merge conn-opts
                    (try
                      (enc/cond
                        (vector?         server) {:server   (parse-sock-addr     server)}
                        (string?         server) (select-keys uri-opts [:server])
                        (map?            server)
                        (case (set (keys server))
                          #{:host :port}
                          (let [{:keys [host port]} server]               {:server (parse-sock-addr host port (meta server))})
                          (#{:master-name :sentinel-spec               }
                           #{:master-name :sentinel-spec :sentinel-opts}) {:server (parse-sentinel-server server)}
                          (#{:cluster-spec              }
                           #{:cluster-spec :cluster-opts})                {:server (parse-cluster-server server)}

                          (do (truss/ex-info! "[Carmine] Unexpected `:server` keys"
                                {:eid :carmine.conn-opts/unexpected-server-keys
                                 :keys (keys server)})))
                        :else (truss/ex-info! "[Carmine] Unexpected `:server` type"
                                {:eid :carmine.conn-opts/unexpected-server-type
                                 :type (type server)}))

                      (catch Throwable t (invalid-server! server t))))]

              (when-let [_cluster-server (get-cluster-server conn-opts)]
                (let [select-db (get-in conn-opts [:init :select-db])]
                  (when-not (or (nil? select-db) (= select-db 0))
                    (truss/ex-info! "[Carmine] Redis Cluster supports only database zero"
                      {:eid :carmine.cluster/invalid-select-db
                       :select-db (enc/typed-val select-db)}))))
              conn-opts)))))

    (catch Throwable t
      (truss/ex-info! (invalid-opts-msg "[Carmine] Invalid connection options" t)
        {:eid       :carmine.conn-opts/invalid
         :conn-opts (utils/redact-secrets
                      (assoc (enc/typed-val conn-opts) :id (get conn-opts :id)))
         :purpose
         (case purpose
           :sentinel          :conn-to-sentinel-server
           :cluster-discovery :conn-to-cluster-discovery
           :conn-to-redis-server)}
        (sanitized-opts-cause t)))))

;;;;

(defn- uri-decode [^String s]
  ;; URLDecoder implements form semantics where "+" means space. URI userinfo
  ;; doesn't, so protect literal plus signs before using it for percent-decoding.
  (URLDecoder/decode (str/replace s "+" "%2B") "UTF-8"))

(defn- parse-string-server
  "Parses a Redis URI into `{:keys [server init socket-opts]}`.
  Supports `redis://` and `rediss://`, with a default port of 6379."
  [s]
  (let [^URI uri (URI. (truss/have string? s))
        scheme   (some-> (.getScheme uri) str/lower-case)
        _        (truss/have? #{"redis" "rediss"} scheme)
        host     (truss/have string? (.getHost uri))
        uri-port (.getPort uri)
        port     (if (== uri-port -1)
                   6379
                   (do (truss/have? #(<= 1 % 65535) uri-port) uri-port))
        _        (truss/have? nil? (.getQuery    uri))
        _        (truss/have? nil? (.getFragment uri))
        path     (.getPath uri)
        select-db
        (enc/cond
          (or (= path "") (= path "/")) nil
          :else
          (if-let [[_ db-str] (re-matches #"/(\d+)" path)]
            (Integer/parseInt ^String db-str)
            (truss/ex-info! "[Carmine] Unexpected Redis URI path"
              {:eid :carmine.conn-opts/invalid-uri-path
               :path path :expected "/<db-number>"})))

        server [host port]
        init
        (enc/assoc-some nil
          :auth
          (when-let [raw-user-info (.getRawUserInfo uri)]
            (let [[username password] (.split ^String raw-user-info ":" 2)]
              (enc/assoc-some nil
                :username (enc/as-?nempty-str (uri-decode username))
                :password (enc/as-?nempty-str (some-> password uri-decode)))))

          :select-db select-db)

        socket-opts
        (when (= scheme "rediss") {:ssl true})]

    (enc/assoc-some {:server server}
      :init        init
      :socket-opts socket-opts)))

(comment
  [(parse-string-server "redis://user:pass@x.y.com:9475/3")
   (parse-string-server "redis://:pass@x.y.com.com:9475/3")
   (parse-string-server     "redis://user:@x.y.com:9475/3")
   (parse-string-server    "rediss://user:@x.y.com:9475/3")])

(defn ^:no-doc parse-sentinel-spec-opts
  "Parses the discovery, cache, and callback options of a `SentinelSpec`."
  [sentinel-spec-opts]
  (let [sentinel-spec-opts
        (enc/nested-merge core/default-sentinel-spec-opts sentinel-spec-opts)]
    (try
      (truss/have? map? sentinel-spec-opts)
      (have-known-keys!
        #{:id :conn-opts :node-conn-opts :cbs
          :retry-backoff-ms :resolve-timeout-ms :resolve-cache-ttl-ms
          :update-sentinels? :update-replicas?}
        sentinel-spec-opts)

      (let [{:keys [cbs retry-backoff-ms resolve-timeout-ms resolve-cache-ttl-ms
                    update-sentinels? update-replicas?]}
            sentinel-spec-opts]
        (have-known-keys!
          #{:on-resolve-success :on-resolve-error
            :on-changed-master :on-changed-replicas :on-changed-sentinels}
          cbs)
        (truss/have? [:or nil? fn?] :in (vals cbs))
        (have-opt! :retry-backoff-ms "a non-negative integer" nat-int? retry-backoff-ms)
        (have-pos-timeout! :resolve-timeout-ms resolve-timeout-ms)
        (have-opt! :resolve-cache-ttl-ms "nil or a non-negative integer"
          (utils/nil-or nat-int?) resolve-cache-ttl-ms)
        (have-opt! :update-sentinels? "a boolean" boolean? update-sentinels?)
        (have-opt! :update-replicas?  "a boolean" boolean? update-replicas?))

      (let [parsed
            (if-let [conn-opts (not-empty (get sentinel-spec-opts :conn-opts))]
              (assoc sentinel-spec-opts :conn-opts
                (parse-conn-opts :sentinel conn-opts))
              sentinel-spec-opts)]
        ;; Optional conn-opts override for the data-node ROLE-verification
        ;; conns (e.g. authed data nodes behind unauthed Sentinels). Deeply
        ;; merged OVER `:conn-opts` so a small auth-only override doesn't
        ;; discard Sentinel-tuned socket/buffer/TLS settings. A nil/empty
        ;; override is REMOVED so use sites fall back to `:conn-opts` (with
        ;; the manager's auth and TLS options).
        (if-let [node-conn-opts (not-empty (get sentinel-spec-opts :node-conn-opts))]
          (assoc parsed :node-conn-opts
            (parse-conn-opts :sentinel
              (enc/nested-merge
                (get sentinel-spec-opts :conn-opts)
                node-conn-opts)))
          (dissoc parsed :node-conn-opts)))

      (catch Throwable t
        (truss/ex-info! (invalid-opts-msg "[Carmine] Invalid SentinelSpec options" t)
          {:eid :carmine.sentinel/invalid-spec-opts
           :sentinel-spec-opts
           (utils/redact-secrets
             (assoc (enc/typed-val sentinel-spec-opts)
               :id (get sentinel-spec-opts :id)))}
          (sanitized-opts-cause t))))))

(defn- parse-sentinel-server [server]
  (truss/have? map? server)
  (let [{:keys [master-name sentinel-spec sentinel-opts]} server
        master-name (enc/as-qname (truss/have [:or string? enc/named?] master-name))
        _sentinel-spec (truss/have sentinel/sentinel-spec? sentinel-spec)
        sentinel-opts (enc/nested-merge core/default-sentinel-opts sentinel-opts)]

    (try
      (truss/have? map? sentinel-opts)
      (have-known-keys! #{:id :clear-timeout-ms :prefer-read-replica?} sentinel-opts)
      (let [{:keys [clear-timeout-ms prefer-read-replica?]} sentinel-opts]
        (have-opt! :clear-timeout-ms "nil or a non-negative integer"
          (utils/nil-or nat-int?) clear-timeout-ms)
        (have-opt! :prefer-read-replica? "a boolean" boolean? prefer-read-replica?))

      (assoc server
        :master-name master-name
        :sentinel-opts sentinel-opts)

      (catch Throwable t
        (truss/ex-info! (invalid-opts-msg "[Carmine] Invalid `:sentinel-opts`" t)
          {:eid :carmine.sentinel/invalid-opts
           :sentinel-opts
           (utils/redact-secrets
             (assoc (enc/typed-val sentinel-opts)
               :id (get sentinel-opts :id)))}
          t)))))

(defn ^:no-doc parse-cluster-spec-opts
  "Parses the topology discovery options of a `ClusterSpec`."
  [cluster-spec-opts]
  (let [cluster-spec-opts
        (enc/nested-merge core/default-cluster-spec-opts cluster-spec-opts)]
    (try
      (truss/have? map? cluster-spec-opts)
      (have-known-keys! #{:id :conn-opts :cbs :refresh-timeout-ms} cluster-spec-opts)
      (let [{:keys [cbs refresh-timeout-ms]} cluster-spec-opts]
        (have-known-keys! #{:on-refresh-success :on-refresh-error :on-changed-topology} cbs)
        (truss/have? [:or nil? fn?] :in (vals cbs))
        (have-pos-timeout! :refresh-timeout-ms refresh-timeout-ms))

      (if-let [conn-opts (not-empty (get cluster-spec-opts :conn-opts))]
        (assoc cluster-spec-opts :conn-opts
          (parse-conn-opts :cluster-discovery conn-opts))
        cluster-spec-opts)

      (catch Throwable t
        (truss/ex-info! (invalid-opts-msg "[Carmine] Invalid ClusterSpec options" t)
          {:eid :carmine.cluster/invalid-spec-opts
           :cluster-spec-opts
           (utils/redact-secrets
             (assoc (enc/typed-val cluster-spec-opts)
               :id (get cluster-spec-opts :id)))}
          (sanitized-opts-cause t))))))

(defn- parse-cluster-server [server]
  (truss/have? map? server)
  (let [{:keys [cluster-spec cluster-opts]} server
        cluster-opts
        (let [cluster-spec (truss/have cluster/cluster-spec? cluster-spec)
              cluster-opts
              (enc/nested-merge core/default-cluster-opts cluster-opts)]

          (try
            (truss/have? map? cluster-opts)
            (have-known-keys!
              #{:id :max-retry-rounds :retry-backoff-ms :max-concurrent-nodes}
              cluster-opts)
            (let [{:keys [max-retry-rounds retry-backoff-ms
                          max-concurrent-nodes]} cluster-opts]
              (have-opt! :max-retry-rounds     "a non-negative integer" nat-int? max-retry-rounds)
              (have-opt! :retry-backoff-ms     "a non-negative integer" nat-int? retry-backoff-ms)
              (have-opt! :max-concurrent-nodes "a positive integer"     pos-int? max-concurrent-nodes))
            cluster-opts

            (catch Throwable t
              (truss/ex-info! (invalid-opts-msg "[Carmine] Invalid `:cluster-opts`" t)
                {:eid :carmine.cluster/invalid-opts
                 :cluster-opts
                 (utils/redact-secrets
                   (assoc (enc/typed-val cluster-opts)
                     :id (get cluster-opts :id)))}
                t))))]

    (assoc server :cluster-opts cluster-opts)))
