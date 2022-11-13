(ns taoensso.carmine-v4.opts
  "Private ns, implementation detail.
  Collected options stuff.

  Carmine necessarily has a lot of options in some contexts, so we
  do our best to coerce and validate early where possible. To help
  with debugging, detailed error messages will be provided when issues
  do occur.

  Notes:

    - To facilitate caching and other uses of `=`, we generally restrict
      most opts content to pure data. In particular, fn opt vals are
      usually prohibited - but var opt vals are allowed.

    - In some cases, we may may use metdata on opts maps to hold:
      - Private options (e.g. for implementation details).
      - Options that we don't want to affect equality/fungibility,
        or that might not support equality.

    - We generally avoid dynamic *vars* and other defaults (e.g. `:or`)
      within inner implementation code. Instead, we try explicitly capture
      and reify all relevant config up-front."

  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [have have? throws?]]
   [taoensso.carmine-v4.utils :as utils])

  (:import
   [java.net Socket]
   [org.apache.commons.pool2.impl GenericKeyedObjectPool]))

(comment
  (remove-ns      'taoensso.carmine-v4.opts)
  (test/run-tests 'taoensso.carmine-v4.opts))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*default-conn-opts*
  ^:dynamic taoensso.carmine-v4/*default-sentinel-opts*
            taoensso.carmine-v4/default-pool-opts
            taoensso.carmine-v4.conns/conn-manager?
            taoensso.carmine-v4.sentinel/sentinel-spec?)

(do
  (alias 'core     'taoensso.carmine-v4)
  (alias 'conns    'taoensso.carmine-v4.conns)
  (alias 'sentinel 'taoensso.carmine-v4.sentinel))

;;;; Reference opts

(let [dummy-val (Object.)]
  ;; Used only for REPL/tests, `#'dummy-var` will pass all var validation checks
  (enc/defonce ^:private dummy-var                        dummy-val)
  (enc/defonce ^:private dummy-val? (fn [x] (identical? x dummy-val))))

(def ^:private ref-servers
  [[    "127.0.0.1"       "6379"]
   {:ip "127.0.0.1" :port "6379"}
   {:master        "master-name"
    :sentinel-spec dummy-var
    :sentinel-opts {}}])

(def ^:private ref-conn-opts
  {:mgr         nil ; var
   :server      ["127.0.0.1" "6379"]
   :cbs         {:on-close nil :on-error nil} ; vars
   :buffer-opts {:init-size-in 8192 :init-size-out 8192}
   :socket-opts {:ssl true :connect-timeout-ms 1000 :read-timeout-ms 4000}
   :init
   {:commands [#_["HELLO" 3 "AUTH" "my-username" "my-password" "SETNAME" "client-name"]
               #_["auth" "my-username" "my-password"]]
    :resp3? true
    :auth {:username "" :password ""}
    :client-name "carmine"
    :select-db 5}})

(def ^:private ref-sentinel-conn-opts
  {:mgr         nil ; var
   #_:server ; [ip port] of Sentinel server will be auto added by resolver
   :cbs         {:on-close nil :on-error nil} ; vars
   :buffer-opts {:init-size-in 1024 :init-size-out 512}
   :socket-opts {:ssl true :connect-timeout-ms 1000 :read-timeout-ms 4000}
   :init
   {:commands []
    :resp3? true
    :auth {:username "" :password ""}
    #_:client-name
    #_:select-db}})

(def ^:private ref-sentinel-opts
  {:conn-opts ref-sentinel-conn-opts
   :cbs
   {:on-error            nil
    :on-success          nil
    :on-sentinels-change nil
    :on-master-change    nil} ; vars

   :add-missing-sentinels? true
   :retry-delay-ms         250
   :resolve-timeout-ms     2000})

;;;; Default opts

(def default-conn-opts
  "Used by `core/*default-conn-opts*`"
  {:server      ["127.0.0.1" 6379]
   :cbs         {:on-close nil, :on-error nil}
   :buffer-opts {:init-size-in 8192, :init-size-out 8192}
   :socket-opts {:ssl false, :connect-timeout-ms 400, :read-timeout-ms nil}
   :init
   {:auth {:username "default" :password nil}
    :client-name :auto
    :select-db   nil
    :resp3?      true}})

(def ^:private default-sentinel-conn-opts
  {:cbs         {:on-close nil, :on-error nil}
   :buffer-opts {:init-size-in 512, :init-size-out 256}
   :socket-opts {:ssl false, :connect-timeout-ms 200, :read-timeout-ms 200}})

(def default-sentinel-opts
  "Used by `core/*default-sentinel-opts*`"
  {:cbs
   {:on-error            nil
    :on-success          nil
    :on-sentinels-change nil
    :on-master-change    nil}

   :add-missing-sentinels? true
   :retry-delay-ms         250
   :resolve-timeout-ms     2000

   :conn-opts default-sentinel-conn-opts})

(def default-pool-opts
  "Used by `core/default-pool-opts`"
  {:test-on-create?               true
   :test-while-idle?              true
   :test-on-borrow?               true
   :test-on-return?               false
   :num-tests-per-eviction-run    -1
   :min-evictable-idle-time-ms    60000
   :time-between-eviction-runs-ms 30000
   :max-total-per-key             16
   :max-idle-per-key              16})

;;;; Socket addresses

(defn parse-sock-addr
  "Returns valid [<ip-string> <port-int>] socket address pair, or throws.
  Retains metadata (server name, comments, etc.)."
  ( [ip port         ]            [(have string? ip) (enc/as-int port)])
  ( [ip port metadata] (with-meta [(have string? ip) (enc/as-int port)] metadata))
  ([[ip port :as addr]]
   (have? string? ip)
   (assoc addr 1 (enc/as-int port))))

(defn descr-sock-addr
  "Returns [<ip-string> <port-int> <?meta>] socket address."
  [addr] (if-let [m (meta addr)] (conj addr m) addr))

(deftest ^:private _sock-addrs
  [(is (= (descr-sock-addr (parse-sock-addr            "ip" "80"))  ["ip" 80                ]))
   (is (= (descr-sock-addr (parse-sock-addr ^:my-meta ["ip" "80"])) ["ip" 80 {:my-meta true}]))])

;;;; Servers

(defn- parse-string-server->opts [s]
  (let [uri (java.net.URI. (have string? s))
        server [(.getHost uri) (.getPort uri)]
        init
        (let [auth
              (let [[username password] (.split (str (.getUserInfo uri)) ":")]
                (when password
                  (if username
                    {:username username :password password}
                    {                   :password password})))

              select-db
              (when-let [[_ db-str] (re-matches #"/(\d+)$" (.getPath uri))]
                (Integer. ^String db-str))]

          (when (or auth select-db)
            (enc/assoc-when {}
              :auth      auth
              :select-db select-db)))]

    (if init
      {:server server :init init}
      {:server server})))

(deftest ^:private _parse-string-server->opts
  [(is (= (parse-string-server->opts "redis://redistogo:pass@panga.redistogo.com:9475/0")
          {:server ["panga.redistogo.com" 9475]
           :init   {:auth {:username "redistogo", :password "pass"}
                    :select-db 0}}))])

(declare ^:private -parse-sentinel-opts)

(defn- parse-server->opts [server]
  (try
    (enc/cond
      (vector?         server) {:server (parse-sock-addr server)}
      (string?         server) (have map? (parse-string-server->opts server))
      (map?            server)
      (case (set (keys server))
        #{:ip :port}
        (let [{:keys [ip port]} server]
          {:server (parse-sock-addr ip port (meta server))})

        (#{:master :sentinel-spec}
         #{:master :sentinel-spec :sentinel-opts})

        (let [{:keys [master sentinel-spec sentinel-opts]} server]
          (have? [:or string? enc/named?] master)
          (have? var?                                      sentinel-spec)
          (have? [:or sentinel/sentinel-spec? dummy-val?] @sentinel-spec)
          (have? [:or nil? map?]                           sentinel-opts)
          (let [server (assoc server :master (enc/as-qname master))
                server
                (if sentinel-opts
                  (assoc server :sentinel-opts (-parse-sentinel-opts sentinel-opts))
                  (do    server))]
            {:server server}))

        (throw     (ex-info "Unexpected :server keys" {:keys (keys server)})))
      :else (throw (ex-info "Unexpected :server type" {:type (type server)})))

    (catch Throwable t
      (throw
        (ex-info "[Carmine] Invalid Redis server specification in connection options"
          {:eid :carmine.conn-opts/invalid-server
           :server   {:value server :type (type server)}
           :expected '(or uri-string [ip port] {:keys [ip port]}
                        {:keys [master sentinel-spec sentinel-opts]})}
          t)))))

(deftest ^:private _parse-server->opts
  [(is (= (parse-server->opts ["127.0.0.1" "80"])           {:server ["127.0.0.1" 80]}))
   (is (= (parse-server->opts {:ip "127.0.0.1" :port "80"}) {:server ["127.0.0.1" 80]}))
   (is (= (parse-server->opts {:sentinel-spec #'dummy-var, :master :foo/bar})
          {:server            {:sentinel-spec #'dummy-var, :master "foo/bar"}}))

   (is (->> (parse-server->opts {:ip "127.0.0.1" :port "80" :invalid true})
            (enc/throws? :any {:eid :carmine.conn-opts/invalid-server})))

   (is (= (parse-server->opts "redis://redistogo:pass@panga.redistogo.com:9475/0")
          {:server ["panga.redistogo.com" 9475],
           :init {:auth {:username "redistogo", :password "pass"},
                  :select-db 0}}))])

;;;; Config mutators

;; socket-opts
(let [throw!
      (fn [k v opts]
        (throw
          (ex-info "[Carmine] Unknown socket option specified"
            {:eid      :carmine.conns/unknown-socket-option
             :opt-key  {:value k :type (type k)}
             :opt-val  {:value v :type (type v)}
             :all-opts opts})))]

  (defn- socket-opts-dry-run! [socket-opts]
    (enc/run-kv!
      (fn [k v]
        (case k
          (:ssl :connect-timeout-ms) nil ; Carmine options, noop and pass through

          (:setKeepAlive    :keep-alive?)    nil
          (:setOOBInline    :oob-inline?)    nil
          (:setTcpNoDelay   :tcp-no-delay?)  nil
          (:setReuseAddress :reuse-address?) nil

          (:setReceiveBufferSize :receive-buffer-size) nil
          (:setSendBufferSize    :send-buffer-size)    nil
          (:setSoTimeout         :read-timeout-ms)     nil

          ;; (:setSocketImplFactory :socket-impl-factory) nil
          (:setTrafficClass         :traffic-class)       nil

          (:setSoLinger :so-linger) nil

          (:setPerformancePreferences :performance-preferences) nil
          (throw! k v socket-opts)))
       socket-opts)
    nil)

  (defn socket-opts-set!
    ^Socket [^Socket s socket-opts]
    (enc/run-kv!
      (fn [k v]
        (case k
          (:ssl :connect-timeout-ms) nil ; Carmine options, noop and pass through

          (:setKeepAlive    :keep-alive?)    (.setKeepAlive    s (boolean v))
          (:setOOBInline    :oob-inline?)    (.setOOBInline    s (boolean v))
          (:setTcpNoDelay   :tcp-no-delay?)  (.setTcpNoDelay   s (boolean v))
          (:setReuseAddress :reuse-address?) (.setReuseAddress s (boolean v))

          (:setReceiveBufferSize :receive-buffer-size) (.setReceiveBufferSize s (int     v))
          (:setSendBufferSize    :send-buffer-size)    (.setSendBufferSize    s (int     v))
          (:setSoTimeout         :read-timeout-ms)     (.setSoTimeout         s (int (or v 0)))

          ;; (:setSocketImplFactory :socket-impl-factory) (.setSocketImplFactory s v)
          (:setTrafficClass         :traffic-class)       (.setTrafficClass      s v)

          (:setSoLinger :so-linger)
          (let [[on? linger] v]
            (.setSoLinger s (boolean on?) (int linger)))

          (:setPerformancePreferences :performance-preferences)
          (let [[conn-time latency bandwidth] v]
            (.setPerformancePreferences s (int conn-time) (int latency) (int bandwidth)))

          (throw! k v socket-opts)))
      socket-opts)
    s))

;; kop-opts
(let [neg-duration (java.time.Duration/ofSeconds -1)
      throw!
      (fn [k v opts]
        (throw
          (ex-info "[Carmine] Unknown pool option specified"
            {:eid      :carmine.conns/unknown-pool-option
             :opt-key  {:value k :type (type k)}
             :opt-val  {:value v :type (type v)}
             :all-opts opts})))]

  (defn kop-opts-dry-run! [kop-opts]
    (enc/run-kv!
      (fn [k v]
        (case k
          ;;; org.apache.commons.pool2.impl.GenericKeyedObjectPool
          (:setMinIdlePerKey  :min-idle-per-key)  nil
          (:setMaxIdlePerKey  :max-idle-per-key)  nil
          (:setMaxTotalPerKey :max-total-per-key) nil

          ;;; org.apache.commons.pool2.impl.BaseGenericObjectPool
          (:setBlockWhenExhausted :block-when-exhausted?) nil
          (:setLifo               :lifo?)                 nil

          (:setMaxTotal      :max-total)   nil
          (:setMaxWaitMillis :max-wait-ms) nil
          (:setMaxWait       :max-wait)    nil

          (:setMinEvictableIdleTimeMillis     :min-evictable-idle-time-ms)      nil
          (:setMinEvictableIdle               :min-evictable-idle)              nil
          (:setSoftMinEvictableIdleTimeMillis :soft-min-evictable-idle-time-ms) nil
          (:setSoftMinEvictableIdle           :soft-min-evictable-idle)         nil
          (:setNumTestsPerEvictionRun         :num-tests-per-eviction-run)      nil
          (:setTimeBetweenEvictionRunsMillis  :time-between-eviction-runs-ms)   nil
          (:setTimeBetweenEvictionRuns        :time-between-eviction-runs)      nil

          (:setTestOnCreate  :test-on-create?)  nil
          (:setTestWhileIdle :test-while-idle?) nil
          (:setTestOnBorrow  :test-on-borrow?)  nil
          (:setTestOnReturn  :test-on-return?)  nil

          (:setSwallowedExceptionListener :swallowed-exception-listener) nil
          (throw! k v kop-opts)))
      kop-opts)
    nil)

  (defn kop-opts-set!
    ^GenericKeyedObjectPool [^GenericKeyedObjectPool kop kop-opts]
    (enc/run-kv!
      (fn [k v]
        (case k
           ;;; org.apache.commons.pool2.impl.GenericKeyedObjectPool
          (:setMinIdlePerKey  :min-idle-per-key)  (.setMinIdlePerKey  kop (int (or v -1)))
          (:setMaxIdlePerKey  :max-idle-per-key)  (.setMaxIdlePerKey  kop (int (or v -1)))
          (:setMaxTotalPerKey :max-total-per-key) (.setMaxTotalPerKey kop (int (or v -1)))

           ;;; org.apache.commons.pool2.impl.BaseGenericObjectPool
          (:setBlockWhenExhausted :block-when-exhausted?) (.setBlockWhenExhausted kop (boolean v))
          (:setLifo               :lifo?)                 (.setLifo               kop (boolean v))

          (:setMaxTotal      :max-total)   (.setMaxTotal      kop (int  (or v -1)))
          (:setMaxWaitMillis :max-wait-ms) (.setMaxWaitMillis kop (long (or v -1)))
          (:setMaxWait       :max-wait)    (.setMaxWait       kop       (or v neg-duration))

          (:setMinEvictableIdleTimeMillis     :min-evictable-idle-time-ms)      (.setMinEvictableIdleTimeMillis     kop (long (or v -1)))
          (:setMinEvictableIdle               :min-evictable-idle)              (.setMinEvictableIdle               kop       (or v neg-duration))
          (:setSoftMinEvictableIdleTimeMillis :soft-min-evictable-idle-time-ms) (.setSoftMinEvictableIdleTimeMillis kop (long (or v -1)))
          (:setSoftMinEvictableIdle           :soft-min-evictable-idle)         (.setSoftMinEvictableIdle           kop       (or v neg-duration))
          (:setNumTestsPerEvictionRun         :num-tests-per-eviction-run)      (.setNumTestsPerEvictionRun         kop (int  (or v 0)))
          (:setTimeBetweenEvictionRunsMillis  :time-between-eviction-runs-ms)   (.setTimeBetweenEvictionRunsMillis  kop (long (or v -1)))
          (:setTimeBetweenEvictionRuns        :time-between-eviction-runs)      (.setTimeBetweenEvictionRuns        kop       (or v neg-duration))

          (:setTestOnCreate  :test-on-create?)  (.setTestOnCreate  kop (boolean v))
          (:setTestWhileIdle :test-while-idle?) (.setTestWhileIdle kop (boolean v))
          (:setTestOnBorrow  :test-on-borrow?)  (.setTestOnBorrow  kop (boolean v))
          (:setTestOnReturn  :test-on-return?)  (.setTestOnReturn  kop (boolean v))

          (:setSwallowedExceptionListener :swallowed-exception-listener)
          (.setSwallowedExceptionListener kop v)
          (throw! k v kop-opts)))
      kop-opts)
    kop))

;;;; conn-opts

(defn- -parse-conn-opts
  "Returns valid parsed conn-opts, or throws.
  Uncached and expensive."
  [in-sentinel-opts? opts]
  (if (and (map? opts) (get opts :skip-validation?)) ; Undocumented
    opts
    (try
      (have? map? opts)
      (let [{:keys [mgr server cbs socket-opts buffer-opts init]} opts
            {:keys [auth]} init]

        (if in-sentinel-opts?
          ;; [ip port] of Sentinel server will be auto added by resolver
          (have? [:ks<= #{:id :mgr #_:server :cbs :socket-opts :buffer-opts :init}] opts)
          (have? [:ks<= #{:id :mgr   :server :cbs :socket-opts :buffer-opts :init}] opts))

        (when-let [mgr (have [:or nil? var?] mgr)]
          ;; (have? [:or conns/conn-manager? dummy-val?] (force @mgr)) ; Don't realise
          (have?    [:or conns/conn-manager? dummy-val? delay?] @mgr))

        (have? [:ks<= #{:on-close :on-error}] cbs)
        (have? [:or var? nil?] :in      (vals cbs))

        (when socket-opts (socket-opts-dry-run! socket-opts))

        ;; socket-opts will be verified during socket creation
        (have? [:ks<= #{:init-size-in :init-size-out}] buffer-opts)

        (if in-sentinel-opts?
          (have? [:ks<= #{:commands :auth :resp3? #_:client-name #_:select-db}] init)
          (have? [:ks<= #{:commands :auth :resp3?   :client-name   :select-db}] init))

        (have? [:ks<= #{:username :password}] auth)

        (if in-sentinel-opts?
          (do    opts)
          (conj  opts (parse-server->opts server))))

      (catch Throwable t
        (throw
          (ex-info "[Carmine] Invalid connection options"
            {:eid  :carmine.conn-opts/invalid
             :opts {:id (get opts :id), :value opts, :type (type opts)}
             :purpose
             (if in-sentinel-opts?
               :conn-to-sentinel-server
               :conn-to-redis-server)}
            t))))))

(deftest ^:private _parse-conn-opts
  [(is (map? (-parse-conn-opts false     ref-conn-opts)))
   (is (map? (-parse-conn-opts false default-conn-opts)))

   (is (map? (-parse-conn-opts true      ref-sentinel-conn-opts)))
   (is (map? (-parse-conn-opts true  default-sentinel-conn-opts)))

   (is (= (-parse-conn-opts false {:server ["127.0.0.1" "6379"]})
          {:server ["127.0.0.1" 6379]}))

   (is (->> (-parse-conn-opts false {:server ["127.0.0.1" "invalid-port"]})
            (enc/throws? :any)))

   (is (->> (-parse-conn-opts false ^:my-meta {:server ["127.0.0.1" "6379"]})
            (meta) :my-meta) "Retains metadata")])

(let [;; Opts are pure data => safe to cache
      cached1 (enc/cache {:size 128 :gc-every 1000} (partial -parse-conn-opts false))
      cached2
      (enc/cache {:size 256 :gc-every 4000}
        (fn [opts1 opts2]
          (have? [:or nil? map?]         opts1 opts2)
          (cached1 (or (utils/merge-opts opts1 opts2) {}))))]

  (defn parse-conn-opts
    "Returns validated `conn-opts` merged over defaults, or throws.
    NB: no dynamic *vars* or defaults should apply to config after parsing.
    Cached."
    [with-dynamic-defaults? conn-opts]
    (if with-dynamic-defaults?
      (cached2 core/*default-conn-opts* conn-opts)
      (cached1                          conn-opts))))

(comment (enc/qb 1e6 (parse-conn-opts true ref-conn-opts))) ; 218

(deftest ^:private _parse-conn-opts
  [(is (map? (parse-conn-opts true  {})))
   (is (map? (parse-conn-opts false {:server ["127.0.0.1" "6379"]})))
   (is (->>  (parse-conn-opts false {}) (enc/throws? :any)))])

;;;; sentinel-opts

(defn- -parse-sentinel-opts
  "Returns valid parsed sentinel-opts, or throws.
  Uncached and expensive."
  [opts]
  (if (and (map? opts) (get opts :skip-validation?)) ; Undocumented
    opts
    (try
      (have? map? opts)
      (have? [:ks<= #{:id :conn-opts :cbs :add-missing-sentinels?
                      :retry-delay-ms :resolve-timeout-ms}] opts)

      (let [{:keys [cbs]} opts]
        (have? [:ks<= #{:on-error :on-success :on-master-change :on-sentinels-change}] cbs)
        (have? [:or nil? var?] :in                                               (vals cbs)))

      (if-let [conn-opts (not-empty (get opts :conn-opts))]
        (assoc opts :conn-opts (-parse-conn-opts :in-sentinel-opts conn-opts))
        (do    opts))

      (catch Throwable t
        (throw
          (ex-info "[Carmine] Invalid Sentinel options"
            {:eid :carmine.sentinel-opts/invalid
             :opts {:id (get opts :id), :value opts, :type (type opts)}}
            t))))))

(let [;; Opts are pure data => safe to cache
      cached1 (enc/cache {:size 128 :gc-every 1000} -parse-sentinel-opts)
      cached3
      (enc/cache {:size 512 :gc-every 4000}
        (fn [opts1 opts2 opts3]
          (have? [:or nil? map?]         opts1 opts2 opts3)
          (cached1 (or (utils/merge-opts opts1 opts2 opts3) {}))))]

  (defn parse-sentinel-opts
    "Returns validated `sentinel-opts` merged over defaults, or throws.
    NB: no dynamic *vars* or defaults should apply to config after parsing.
    Cached."
    [with-dynamic-defaults? spec-sentinel-opts sentinel-opts]
    (if with-dynamic-defaults?
      (cached3 core/*default-sentinel-opts* spec-sentinel-opts sentinel-opts)
      (cached3 nil                          spec-sentinel-opts sentinel-opts))))

(comment (enc/qb 1e6 (parse-sentinel-opts true {} ref-sentinel-opts))) ; 234.71

(deftest ^:private _parse-sentinel-opts
  [(is (map? (parse-sentinel-opts true  {} {})))
   (is (map? (parse-sentinel-opts false {} {})))])
