(ns taoensso.carmine-v4
  "Carmine v4 API for Redis RESP2/3, Sentinel, and Cluster.

  Preview: this API may still change before the final v4 release.

  Quick start

    (require '[taoensso.carmine-v4 :as car])

    (defonce mgr (car/conn-manager))
    (car/wcar mgr
      (car/set \"user:1\" \"Ada\")
      (car/get \"user:1\"))
    ;; => [\"OK\" \"Ada\"]

    ;; During application shutdown:
    (car/conn-manager-close! mgr)

  Connection managers

  Create one long-lived manager for each Redis configuration and pass it to
  operations. `conn-manager` is the usual default, also for Sentinel and
  Cluster. See also `conn-manager-unpooled`. Managers own their
  connections and background resources; close them during application
  shutdown. Deref a manager for redacted configuration and diagnostics, or use
  `conn-manager-stats` for its stable statistics snapshot.

  `default-conn-opts` documents connection, authentication, TLS, RESP,
  callback, and protocol-limit options. A server can be a standalone URI or
  address, or a server map containing a `sentinel-spec` or `cluster-spec`.
  Carmine has no hidden global pool.

  Requests and replies

  Redis command functions queue requests inside a `wcar` or `with-car`
  request context. The boundary flushes them as a pipeline and returns their
  replies. `with-replies` creates a nested boundary when an earlier reply is
  needed to construct later requests.

  Reply processing has three phases:

    1. Decode the complete wire reply, optionally using a special read mode
       such as `as-bytes`, `thaw`, or `skip-replies`.
    2. Apply at most one parser such as `parse`, `as-long`, or
       `parse-aggregates`. A nested parser replaces the outer parser;
       `unparsed` cancels it.
    3. Complete the request boundary using its `:as-vec?` and `:error-mode`
       options.

  Set reply transformations before queueing the request they should affect.
  `natural-replies` is a hard bypass for internal reads that caller bindings
  must not change. Parsers receive a complete aggregate as one value. RESP3
  attributes, top-level server pushes, and protocol errors have their own
  handling; see the relevant function docstrings for exact contracts. Parsers
  used with Cluster must be thread-safe and non-blocking.

  Serialization

  Non-native arguments are frozen with Nippy, and Carmine-marked replies are
  thawed. Custom Nippy options use the same vars as Carmine v3:
  `with-thaw-opts` and `with-freeze-opts`. Requests capture them when queued.
  `freeze` wraps a value with explicit options; `thaw` also force-thaws
  unmarked blobs.

  Dedicated APIs

  Connection-affine operations do not use ordinary command pipelines. Use
  `transact!` for WATCH/MULTI/EXEC and `pubsub-listener` for Pub/Sub.
  Scripting helpers include `eval*`, `prepare-lua`, and `lua`. Arbitrary
  commands use `rcmd`, with `cluster-key` and `with-cluster-target` when
  Cluster routing needs explicit information."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:refer-clojure
   :exclude [bytes time get set key keys type sync sort eval
             parse-long parse-double])
  (:require
   [taoensso.encore  :as enc]
   [taoensso.truss   :as truss]
   [taoensso.nippy.tools :as nippy-tools]

   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.resp        :as resp]
   ;;
   [taoensso.carmine-v4.utils       :as utils]
   [taoensso.carmine-v4.opts        :as opts]
   [taoensso.carmine-v4.conns       :as conns]
   [taoensso.carmine-v4.sentinel    :as sentinel]
   [taoensso.carmine-v4.cluster     :as cluster]
   [taoensso.carmine-v4.tx          :as tx]
   [taoensso.carmine-v4.pubsub      :as pubsub]
   [taoensso.carmine-v4.scripts     :as scripts]
   [taoensso.carmine-v4.scan        :as scan]
   [taoensso.carmine-v4.commands    :as commands]))

(enc/assert-min-encore-version [3 172 1])

(comment (remove-ns 'taoensso.carmine-v4))

;;;; Aliases

(enc/defaliases
  ;;; Read opts
  com/skip-replies
  com/natural-replies
  com/as-bytes
  com/thaw

  ;;; Reply parsing
  com/reply-error?
  com/reply-content
  com/reply-attributes
  com/unparsed
  com/parse
  com/parse-aggregates
  ;;
  com/as-?long
  com/as-?double
  com/as-?kw
  ;;
  com/as-long
  com/as-double
  com/as-kw

  ;;; Write wrapping
  write/bytes
  write/freeze

  ;;; Nippy options (shared with Carmine v3)
  {:alias with-thaw-opts   :src nippy-tools/with-thaw-opts
   :doc
   "Runs `body` with the given Nippy options for thawing replies:
    (with-thaw-opts {:password [:salted \"my-password\"]}
      (wcar mgr (get \"my-key\")))

  Requests use the options in effect when they're queued, and Pub/Sub
  listeners the options in effect when they're created. Same macro as
  Carmine v3's.

  A thaw error's cause may include these options in its data (incl. any
  `:password`), so take care when logging it.

  See also `with-freeze-opts`, `thaw`, `*auto-thaw?*`."}

  {:alias with-freeze-opts :src nippy-tools/with-freeze-opts
   :doc
   "Runs `body` with the given Nippy options for freezing arguments:
    (with-freeze-opts {:password [:salted \"my-password\"]}
      (wcar mgr (set \"my-key\" {:my \"data\"})))

  Arguments use the options in effect when their request is queued. Same
  macro as Carmine v3's.

  See also `with-thaw-opts`, `freeze`, `*auto-freeze?*`."}

  ;;; RESP3
  resp/rcmd
  resp/rcmd*
  resp/local-echo

  ;;; Scripting
  scripts/script-hash
  scripts/prepare-lua
  scripts/evalsha*
  scripts/eval*
  scripts/eval-ro*
  scripts/lua
  scripts/lua-ro
  scripts/lua-local
  scripts/compare-and-set
  scripts/compare-and-delete
  scripts/compare-and-hset
  scripts/compare-and-hdel

  ;;; Atomic swaps
  scripts/swap
  scripts/hswap

  ;;; Transactions
  tx/transact!

  ;;; Scans
  scan/scan-reduce
  scan/scan-reduce-kv
  scan/scan-keys

  ;;; Connections
  #_conns/conn?
  #_conns/conn-ready?
  #_conns/conn-close!
  ;;
  sentinel/sentinel-spec
  sentinel/sentinel-spec?
  ;;
  conns/conn-manager?
  conns/conn-manager
  conns/conn-manager-unpooled
  conns/conn-manager-clustered

  ;;; Cluster
  cluster/cluster-spec
  cluster/cluster-spec?
  cluster/cluster-key
  cluster/cluster-slot
  cluster/cluster-cached-topology
  cluster/cluster-refresh-topology!

  ;;; Pub/Sub
  pubsub/pubsub-listener
  pubsub/pubsub-listener?
  {:alias pubsub-stats :src pubsub/pubsub-listener-stats}
  pubsub/pubsub-subscribe!
  pubsub/pubsub-unsubscribe!
  pubsub/pubsub-await-synced!
  pubsub/pubsub-ping!
  pubsub/pubsub-close!)

;; Stable batch conveniences for DSL/dynamic-call use. These intentionally stay
;; out of the recommended API index; the singular forms cover ordinary calls.
(enc/defalias ^:no-doc rcmds        resp/rcmds)
(enc/defalias ^:no-doc rcmds*       resp/rcmds*)
(enc/defalias ^:no-doc local-echos  resp/local-echos)
(enc/defalias ^:no-doc local-echos* resp/local-echos*)

(defn- have-conn-manager [via mgr]
  (let [mgr (force mgr)]
    (when-not (conns/conn-manager? mgr)
      (truss/ex-info! "[Carmine] Expected a connection manager"
        {:eid :carmine.conns/invalid-manager
         :via via
         :mgr (enc/typed-val mgr)}))
    mgr))

(defn- have-close-opts [via allowed-keys opts]
  (when-not (or (nil? opts) (and (map? opts) (every? allowed-keys (clojure.core/keys opts))))
    (truss/ex-info! "[Carmine] Expected an options map with only known keys"
      {:eid :carmine.conns/invalid-opts
       :via via
       :allowed-keys allowed-keys
       :opts (enc/typed-val opts)})))

(defn conn-manager-stats
  "Returns a snapshot of stats for `conn-mgr`, same as `(:stats @conn-mgr)`.
  Includes `:kind`, `:connections`, cumulative `:counts`, `:timings`, `:push`,
  and for Cluster managers per-node `:nodes`."
  [conn-mgr]
  (conns/mgr-stats (have-conn-manager 'conn-manager-stats conn-mgr)))

(defn conn-manager-open?
  "Returns true iff `conn-mgr` hasn't been closed."
  [conn-mgr]
  (conns/mgr-open? (have-conn-manager 'conn-manager-open? conn-mgr)))

(defn conn-manager-clear!
  "Closes all of `conn-mgr`'s pooled connections, including those in use
  (the calling thread's own close when returned). The manager stays open,
  and will create new connections as needed. A connection returned during
  the clear may stay open until next borrowed or evicted, but is never reused.

  Options:
    - `:timeout-ms` - Max time to wait for connections in use to be returned
      before forcibly closing them. Nil (default) means no limit.

  Returns true iff connections were cleared (false for a closed or unpooled
  manager)."
  ([conn-mgr] (conn-manager-clear! conn-mgr nil))
  ([conn-mgr opts]
   (have-close-opts 'conn-manager-clear! #{:timeout-ms} opts)
   (conns/mgr-clear!
     (have-conn-manager 'conn-manager-clear! conn-mgr) (:timeout-ms opts))))

(defn conn-manager-close!
  "Permanently closes `conn-mgr` and its connections.

  Options:
    - `:timeout-ms` - Max time to wait for connections in use to be returned
      before forcibly closing them. Nil (default) means no limit.
    - `:data` - Optional map to include in `:on-conn-close` callback data.
      Can't use the keys `:mgr`, `:via` or `:timeout-ms`.

  Returns true iff this call closed the manager (false if it was already
  closed)."
  ([conn-mgr] (conn-manager-close! conn-mgr nil))
  ([conn-mgr opts]
   (have-close-opts 'conn-manager-close! #{:timeout-ms :data} opts)
   (let [{:keys [timeout-ms data]} opts]
     (when-not (or (nil? data) (map? data))
       (truss/ex-info! "[Carmine] Expected connection close data to be a map or nil"
         {:eid :carmine.conns/invalid-close-data
          :via 'conn-manager-close!
          :data (enc/typed-val data)}))
     (when-let [reserved (not-empty (filterv #(contains? data %) [:mgr :via :timeout-ms]))]
       (truss/ex-info! "[Carmine] Connection close data can't use reserved keys"
         {:eid :carmine.conns/invalid-close-data
          :via 'conn-manager-close!
          :keys reserved}))
     (conns/mgr-close!
       (have-conn-manager 'conn-manager-close! conn-mgr) timeout-ms data))))

;;;; Config

(def default-conn-opts
  "Default connection options. The `:conn-opts` given to a connection manager
  are deeply merged over these.

  `:server` - Where to connect (default `[\"127.0.0.1\" 6379]`), one of:
    - `[host port]` or `{:host host, :port port}`.
    - A `redis://` or `rediss://` (TLS) URI string.
    - `{:master-name name, :sentinel-spec spec, :sentinel-opts opts}`,
      see `sentinel-spec`.
    - `{:cluster-spec spec, :cluster-opts opts}`, see `cluster-spec`.

  `:init` - What to send when a connection is created:
    - `:resp3?`      - Use RESP3 via `HELLO 3` (default true, needs Redis 6+).
    - `:auth`        - Optional `{:username _, :password _}`.
    - `:client-name` - String or `(fn [conn-opts])`. Nil sets no name. When
                       omitted, a unique name is generated.
    - `:select-db`   - Optional Redis database number.
    - `:commands`    - Sequence of command vectors to send in place of all of
                       the above.

  `:socket-opts` - Timeouts are in milliseconds, nil means no limit:
    - `:connect-timeout-ms` - For connecting (default 400).
    - `:read-timeout-ms`    - For each socket read (default 10000). Use a
      higher value or nil for blocking commands like `BLPOP`.
    - `:ssl` - True to use TLS with the JVM trust store and hostname
      verification (default false). May also be a
      `(fn [{:keys [conn-opts socket host port]}])` that returns a
      `java.net.Socket` wrapping `socket`. The fn must verify the server, limit
      its own handshake time, and return a socket whose `close` also closes
      `socket`.

    Rarely needed:
    - `:init-timeout-ms`  - For the TLS handshake and for `:init`
                            (default 5000).
    - `:ready-timeout-ms` - For the `PING` that checks a pooled connection
                            (default 200).
    - A nil init or ready timeout adds no limit of its own, but
      `:read-timeout-ms` still applies.
    - Java socket options: `:keep-alive?`, `:tcp-no-delay?`, `:reuse-address?`,
      `:receive-buffer-size`, `:send-buffer-size`, etc.

  `:buffer-opts` - Initial `:init-size-in` and `:init-size-out` in bytes
    (default 8192 each).

  `:resp-opts :limits` - Size limits for replies, nil means no limit. A reply
    over a limit closes its connection:
    - `:max-line-bytes`         - Bytes before CRLF (default 1 MiB).
    - `:max-nesting-depth`      - Nested aggregates (default 128).
    - `:max-blob-bytes`         - Bytes in one blob (default nil).
    - `:max-aggregate-elements` - Elements or map pairs in one aggregate
                                  (default nil).
    - `:max-frame-bytes`        - Bytes in one top-level reply (default nil).
    Consider setting the last three if replies may be untrusted or huge.

  `:cbs` - `:on-conn-close`, `:on-conn-error`, and `:on-push-error` callbacks,
    see `*conn-cbs*`.

  `:id` - Optional value to identify these options, e.g. in errors. Spec,
    Sentinel, and Cluster options also accept one.

  Defaults may be overridden with an EDN map at `:taoensso.carmine.default-conn-opts`
  (JVM property, environment variable, or classpath resource)."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-conn-opts)
        base
        {:server ["127.0.0.1" 6379]
         #_{:host "127.0.0.1" :port "6379"}
         #_{:master-name  "my-master"
            :sentinel-spec my-spec
            :sentinel-opts {}}

         :cbs         {:on-conn-close nil, :on-conn-error nil, :on-push-error nil}
         :buffer-opts {:init-size-in 8192, :init-size-out 8192}
         :resp-opts
         {:limits
          {:max-line-bytes         1048576
           :max-nesting-depth      128
           :max-blob-bytes         nil
           :max-aggregate-elements nil
           :max-frame-bytes        nil}}
         :socket-opts {:ssl false, :connect-timeout-ms 400, :read-timeout-ms 10000
                       :ready-timeout-ms 200, :init-timeout-ms 5000}
         :init
         {;; :commands [["HELLO" 3 "AUTH" "default" "my-password" "SETNAME" "client-name"]
          ;;            ["auth" "default" "my-password"]]
          :resp3? true
          :auth {:username nil :password nil}
          ;; :client-name "carmine"
          ;; :select-db   5
          }}]

    (enc/nested-merge base from-env)))

(def default-pool-opts
  "Default options for the connection pool used by `conn-manager` (for
  Cluster, see `default-cluster-pool-opts`). The
  `:pool-opts` given to the manager are merged over these.

    - `:max-total` - Max connections (default 16).
    - `:max-idle`  - Max idle connections (default 16).
    - `:max-wait-ms` - How long to wait for a connection when all are in use.
      Nil waits without limit (default), zero doesn't wait.
    - `:test-on-borrow?`, `:test-while-idle?` (default true),
      `:test-on-create?`, `:test-on-return?` (default false) - When to check
      that a connection is still usable.
    - `:ready-check-after-idle-ms` - A check also sends `PING` when the
      connection has been idle for this long (default 5000). Nil or zero
      always sends `PING`, as does a check on return.
    - `:min-evictable-idle-time-ms` - Close connections idle for this long
      (default 60000).
    - `:time-between-eviction-runs-ms` - How often to look for idle
      connections to check or close (default 30000).

  Times are in milliseconds. The pool is an Apache Commons Pool 2
  `GenericObjectPool`, and most of its options are supported.

  Defaults may be overridden with an EDN map at `:taoensso.carmine.default-pool-opts`
  (JVM property, environment variable, or classpath resource)."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-pool-opts)
        base
        {:ready-check-after-idle-ms      5000
         :test-on-create?               false
         :test-while-idle?              true
         :test-on-borrow?               true
         :test-on-return?               false
         :num-tests-per-eviction-run    -1
         :min-evictable-idle-time-ms    60000
         :time-between-eviction-runs-ms 30000
         :max-total                     16
         :max-idle                      16}]

    (enc/nested-merge base from-env)))

(def default-cluster-pool-opts
  "Default options for the connection pool used by Cluster managers,
  as `default-pool-opts` but with limits per node:

    - `:max-total`         - Max connections (default 64).
    - `:max-total-per-key` - Max connections per node (default 16).
    - `:max-idle-per-key`  - Max idle connections per node (default 16).

  Defaults may be overridden with an EDN map at `:taoensso.carmine.default-cluster-pool-opts`
  (JVM property, environment variable, or classpath resource)."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-cluster-pool-opts)
        base
        {:ready-check-after-idle-ms      5000
         :test-on-create?               false
         :test-while-idle?              true
         :test-on-borrow?               true
         :test-on-return?               false
         :num-tests-per-eviction-run    -1
         :min-evictable-idle-time-ms    60000
         :time-between-eviction-runs-ms 30000
         :max-total                     64
         :max-total-per-key             16
         :max-idle-per-key              16}]
    (enc/nested-merge base from-env)))

(def default-sentinel-spec-opts
  "Default options for `sentinel-spec`. Options given to `sentinel-spec` are
  deeply merged over these.

    - `:update-replicas?`  - Keep track of each master's replicas
                             (default true).
    - `:update-sentinels?` - Learn about other Sentinels (default true).
    - `:resolve-timeout-ms` - Max time to resolve an address, across all
      Sentinels and retries (default 2000).
    - `:retry-backoff-ms` - Wait between rounds of trying each Sentinel
      (default 250).
    - `:resolve-cache-ttl-ms` - How long a resolved address may be reused when
      checking pooled connections (default 5000). Nil reuses it without
      limit. New connections always ask Sentinel. If no Sentinel can be
      reached, pooled connections to the last known servers stay in use
      (see Redis's `min-replicas-to-write` to limit writes to an isolated
      master).
    - `:conn-opts` - For the short-lived connections to Sentinels, and to
      Redis servers when checking their role. The latter use the auth and TLS
      options of the manager in use.
    - `:node-conn-opts` - Rarely needed. Merged over `:conn-opts` and used
      as is for the role checks, in place of the manager's auth and TLS.
    - `:cbs` - `:on-resolve-success`, `:on-resolve-error`,
      `:on-changed-master`, `:on-changed-replicas`, and
      `:on-changed-sentinels` callbacks, see `*conn-cbs*`.

  Times are in milliseconds.

  Defaults may be overridden with an EDN map at `:taoensso.carmine.default-sentinel-spec-opts`
  (JVM property, environment variable, or classpath resource)."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-sentinel-spec-opts)
        base
        {:cbs
         {:on-resolve-success   nil
          :on-resolve-error     nil
          :on-changed-master    nil
          :on-changed-replicas  nil
          :on-changed-sentinels nil}

         :update-sentinels?     true
         :update-replicas?      true

         :retry-backoff-ms      250
         :resolve-timeout-ms    2000
         :resolve-cache-ttl-ms  5000

         :conn-opts
         {:cbs         {:on-conn-close nil, :on-conn-error nil}
          :buffer-opts {:init-size-in 512, :init-size-out 256}
          :socket-opts {:ssl false, :connect-timeout-ms 200, :read-timeout-ms 200
                        :ready-timeout-ms 200, :init-timeout-ms 500}}}]

    (enc/nested-merge base from-env)))

(def default-sentinel-opts
  "Default `:sentinel-opts`, the Sentinel options of one connection manager.
  The manager's `:conn-opts :server :sentinel-opts` are merged over these.

    - `:prefer-read-replica?` - Connect to a random replica when one is usable,
      otherwise to the master (default false). Pooled connections to the
      master are replaced once a replica is usable again. Needs the spec's
      `:update-replicas?` (on by default).
    - `:clear-timeout-ms` - After a master change, how long a pooled manager
      waits for connections in use before closing them (default 10000). Nil
      waits without limit.

  Connections to an old master are found when the pool checks them, so keep
  the pool's `:test-on-borrow?` enabled.

  Defaults may be overridden with an EDN map at `:taoensso.carmine.default-sentinel-opts`
  (JVM property, environment variable, or classpath resource)."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-sentinel-opts)
        base {:prefer-read-replica? false, :clear-timeout-ms 10000}]
    (enc/nested-merge base from-env)))

(def default-cluster-spec-opts
  "Default options for `cluster-spec`. Options given to `cluster-spec` are
  deeply merged over these.

    - `:refresh-timeout-ms` - Max time to fetch the cluster topology, across
      all nodes tried (default 2000).
    - `:conn-opts` - For the short-lived connections that fetch the topology,
      e.g. timeouts. Their auth and TLS options are those of the manager in
      use.
    - `:cbs` - `:on-refresh-success`, `:on-refresh-error`, and
      `:on-changed-topology` callbacks, see `*conn-cbs*`.

  Defaults may be overridden with an EDN map at `:taoensso.carmine.default-cluster-spec-opts`
  (JVM property, environment variable, or classpath resource)."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-cluster-spec-opts)
        base
        {:cbs
         {:on-refresh-success  nil
          :on-refresh-error    nil
          :on-changed-topology nil}

         :refresh-timeout-ms 2000

         :conn-opts
         {:cbs         {:on-conn-close nil, :on-conn-error nil}
          :buffer-opts {:init-size-in 4096, :init-size-out 256}
          :socket-opts {:ssl false, :connect-timeout-ms 200, :read-timeout-ms 200
                        :ready-timeout-ms 200, :init-timeout-ms 500}}}]

    (enc/nested-merge base from-env)))

(def default-cluster-opts
  "Default `:cluster-opts`, the Cluster options of one connection manager. The
  manager's `:conn-opts :server :cluster-opts` are merged over these.

    - `:max-retry-rounds` - Max retries of requests that were redirected or
      got `TRYAGAIN`/`CLUSTERDOWN` (default 4).
    - `:retry-backoff-ms` - Wait before a `TRYAGAIN`/`CLUSTERDOWN` retry
      (default 25).
    - `:max-concurrent-nodes` - Max nodes contacted in parallel for one
      set of requests (default 1).

  Requests are not retried after a connection error, since they may have
  been executed.

  Defaults may be overridden with an EDN map at `:taoensso.carmine.default-cluster-opts`
  (JVM property, environment variable, or classpath resource)."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-cluster-opts)
        base
        {:max-retry-rounds 4
         :retry-backoff-ms 25
         :max-concurrent-nodes 1}]

    (enc/nested-merge base from-env)))

;;;;

(def ^:dynamic *auto-freeze?*
  "When true (default), arguments that aren't native Redis types are serialized
  with Nippy. When false, such arguments throw unless wrapped with `freeze`.

  Initial value may be set with `:taoensso.carmine.auto-freeze` (JVM property,
  environment variable, or classpath resource).

  See also `freeze`, `with-freeze-opts`, `*auto-thaw?*`."
  (enc/get-env {:as :bool, :default true}
    :taoensso.carmine.auto-freeze))

(def ^:dynamic *auto-thaw?*
  "When true (default), replies that were serialized with Nippy are thawed
  automatically. Bind to false to get the serialized payloads.

  Initial value may be set with `:taoensso.carmine.auto-thaw` (JVM property,
  environment variable, or classpath resource).

  See also `thaw`, `with-thaw-opts`, `*auto-freeze?*`."
  (enc/get-env {:as :bool, :default true}
    :taoensso.carmine.auto-thaw))

(def ^:dynamic *raw-verbatim-strings?*
  "When true, RESP3 verbatim strings are returned as a record with `:format`
  and `:content`. When false (default), as their content string."
  false)

(def ^:dynamic *keywordize-maps?*
  "When true, string keys in RESP3 map replies are returned as keywords.
  False by default. Useful when you know the keys, e.g. `HELLO` or hashes
  with fixed field names."
  false)

(def ^:dynamic *issue-83-workaround?*
  "Enable iff you might read data written by Carmine before v2.6.1
  (2014-05-01). Carmine v2.6.0 to v2.6.1 wrote incorrect markers for some
  Nippy blobs, Ref. <https://github.com/ptaoussanis/carmine/issues/83>.

  When true, and `*auto-thaw?*` is true, Carmine tries to thaw binary replies
  that start with a valid Nippy header. A failed attempt is a reply error.

  False by default. Initial value may be set with
  `:taoensso.carmine.issue-83-workaround` (JVM property, environment variable,
  or classpath resource)."
  (enc/get-env {:as :bool, :default false}
    :taoensso.carmine.issue-83-workaround))

(def ^:dynamic *conn-cbs*
  "Map of global callbacks, called in addition to any matching `:cbs` in
  connection, `sentinel-spec`, or `cluster-spec` options. Useful for logging,
  debugging, and tests.

  Each callback is a `(fn [data])`. `data` always has `:cbid` (the callback's
  key), and may have `:eid`, `:via`, `:addr`, `:cause`, `:elapsed-ms`, etc.

  Connections:
    `:on-conn-close` - A connection was closed, once per connection.
    `:on-conn-error` - A connection couldn't be created, or isn't usable.
    `:on-push-error` - A RESP3 push was dropped, or its handler threw.

  Sentinel:
    `:on-resolve-success`, `:on-resolve-error`
    `:on-changed-master`, `:on-changed-replicas`, `:on-changed-sentinels`
      Data has `:master-name` and `:changed {:old _, :new _}`. Also called
      when first resolved, with nil `:old`.

  Cluster:
    `:on-refresh-success`, `:on-refresh-error`, `:on-changed-topology`

  Callbacks are called on the thread that triggered them, so should return
  quickly and shouldn't use Carmine. Errors they throw are logged."
  nil)

;;;; Core API (main entry point to Carmine)

(defmacro with-cluster-target
  "Sends the requests in `body` to the given Redis Cluster target:
  (wcar mgr (with-cluster-target {:node-id id} (info)))

  Targets:
    `{:key     key}`         - Master serving the key's slot
    `{:slot    slot}`        - Master serving the slot
    `{:addr    [host port]}` - Node at address
    `{:node-id id}`          - Node with id
    `:any`                   - Any master

  Commands with keys are routed automatically and need no target.
  See also `cluster-cached-topology`."
  [target & body]
  `(cluster/with-cluster-target* ~target (fn [] ~@body)))

(defn with-car
  "Function version of `wcar`: calls `(body-fn)` and returns the replies to
  the requests it makes. Use when reply options are computed at runtime:
    (with-car mgr {:as-vec? true} (fn [] (ping))) => [\"PONG\"]"
  ([conn-mgr                                  body-fn] (with-car conn-mgr nil body-fn))
  ([conn-mgr reply-opts body-fn]
   (let [mgr (have-conn-manager 'with-car conn-mgr)
         _ (truss/have fn? body-fn)
         reply-opts (resp/parse-reply-opts reply-opts)]
     (if-let [cluster-server (conns/mgr-cluster-server mgr)]
       (resp/with-replies {:mgr mgr, :cluster-server cluster-server}
         reply-opts nil body-fn)
       (conns/mgr-borrow! mgr
         (fn [conn in out]
           (resp/with-replies in out
             (conns/mgr-push-fn mgr (conns/conn-addr conn))
             reply-opts nil body-fn)))))))

(defmacro wcar
  "Sends the Redis requests in `body` as one pipeline and returns their
  replies:
    (wcar mgr (ping))                      => \"PONG\"
    (wcar mgr (set \"k\" \"v\") (get \"k\")) => [\"OK\" \"v\"]
    (wcar mgr :as-vec (ping))              => [\"PONG\"]

  `conn-mgr` is a connection manager (see `conn-manager`), or a delay
  of one.

  Reply options, given as a literal map before `body`:
    `:as-vec?`    - When true, replies are returned as a vector, or nil if
                    there are none. When false (default), a single reply is
                    returned unwrapped.
    `:error-mode` - `:throw` (default) throws after reading all replies, with
                    `:replies` and `:error-indexes` in the ex-data, and the
                    first reply error as cause.
                    `:return` returns reply errors in place.

  `:as-vec` is shorthand for `{:as-vec? true}`. Use it when the number of
  requests may vary, e.g. when built from a collection.

  Reply errors nested inside aggregate replies aren't detected. Use `with-car`
  for reply options computed at runtime.

  A nested `wcar` or `with-replies` first sends any requests already queued,
  so its replies can be used to build later requests.

  With a Cluster manager, each request goes to the node serving its keys.
  Replies keep request order, but requests to different nodes may execute in
  any order, as may requests that are redirected or retried while a slot
  migrates. When using `rcmd`, mark keys with `cluster-key`.

  See also `with-replies`, `skip-replies`, `parse`, `transact!`."
  {:arglists
   '([conn-mgr            & body]
     [conn-mgr reply-opts & body])}

  [conn-mgr & body]
  (let [[reply-opts body] (resp/parse-body-reply-opts body)]
    `(with-car ~conn-mgr ~reply-opts
       (fn [] ~@body))))

(comment
  [(wcar mgr (resp/rcmd :PING))
   (wcar mgr (resp/rcmd :SET "k1" 3))
   (wcar mgr (resp/rcmd :GET "k1"))

   (wcar mgr                 (resp/ping))
   (wcar mgr {:as-vec? true} (resp/ping))
   (wcar mgr  :as-vec        (resp/ping))])

(defmacro with-replies
  "Like `wcar`, but for use within a `wcar` body without borrowing another
  connection. Returns the replies to `body`, e.g. to build later requests:
    (wcar mgr
      (let [k (with-replies (get \"pointer\"))]
        (get k)))

  Any requests already queued are sent first. Takes the same reply options
  as `wcar`."
  {:arglists '([& body] [reply-opts & body])}
  [& body]
  (let [[reply-opts body] (resp/parse-body-reply-opts body)]
    `(resp/with-replies (resp/parse-reply-opts ~reply-opts) nil
       (fn [] ~@body))))

;;;; Native Redis command API

(commands/defcommands)
