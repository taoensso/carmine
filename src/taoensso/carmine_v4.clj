(ns taoensso.carmine-v4
  "Experimental Carmine v4 API for Redis RESP2/3, Sentinel, and Cluster.

  This public API can change before the final v4 release. See
  `doc/v4/README.md` for setup, contracts, limitations, and migration."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:refer-clojure
   :exclude [bytes time get set key keys type sync sort eval
             parse-long parse-double])
  (:require
   [taoensso.encore  :as enc]
   [taoensso.truss   :as truss]

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

(enc/assert-min-encore-version [3 169 1])

(comment (remove-ns 'taoensso.carmine-v4))

;;;; Aliases

(enc/defaliases
  ;;; Read opts
  com/skip-replies
  com/normal-replies
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
  {:alias conn-manager :src conns/conn-manager-pooled}
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

;; Low-level scan machinery, aliased for the unit tests and advanced use
(enc/defalias ^:no-doc dummy-scan-fn        scan/dummy-scan-fn)
(enc/defalias ^:no-doc scan-reduce-elements scan/scan-reduce-elements)

(defn- have-conn-manager [via mgr]
  (let [mgr (force mgr)]
    (when-not (conns/conn-manager? mgr)
      (truss/ex-info! "[Carmine] Expected a connection manager"
        {:eid :carmine.conns/invalid-manager
         :via via
         :manager (enc/typed-val mgr)}))
    mgr))

(defn conn-manager-stats
  "Returns the stable statistics snapshot for `conn-mgr`. The same map is at
  `(:stats @conn-mgr)`. `conn-mgr` may be a manager or a delay that resolves to
  one."
  [conn-mgr]
  (conns/mgr-stats (have-conn-manager 'conn-manager-stats conn-mgr)))

(defn conn-manager-open?
  "Returns true iff `conn-mgr` is open for borrowing. `conn-mgr` may be a
  manager or a delay that resolves to one."
  [conn-mgr]
  (conns/mgr-open? (have-conn-manager 'conn-manager-open? conn-mgr)))

(defn conn-manager-clear!
  "Closes all pooled connections owned by `conn-mgr`, including connections in
  use. The manager stays open. An unpooled manager has nothing to clear.

  `timeout-ms` must be nil or a non-negative long. Nil sets no limit. A finite
  timeout waits, then forcibly interrupts connections that remain active. This
  may interrupt an executing pipeline.

  Returns one of:
    `{:action :cleared, :graceful? boolean}`.
    `{:action :not-applicable}` for an unpooled manager.
    `{:action :already-closed}` if shutdown started.

  The one-argument form sets no timeout. `conn-mgr` may be a manager or a delay
  that resolves to one."
  ([conn-mgr]
   (conn-manager-clear! conn-mgr nil))
  ([conn-mgr timeout-ms]
   (conns/mgr-clear!
     (have-conn-manager 'conn-manager-clear! conn-mgr) timeout-ms)))

(defn conn-manager-close!
  "Permanently closes `conn-mgr` and stops new borrows.

  `timeout-ms` must be nil or a non-negative long. Nil sets no limit. A finite
  timeout waits, then forcibly interrupts connections that remain active. This
  may interrupt an executing pipeline. `data` must be nil or a map and is added
  to the close callback data. Carmine's reserved `:mgr`, `:via`, and
  `:timeout-ms` keys take precedence over conflicting keys in `data`.

  The first close returns `{:action :closed, :graceful? boolean}`. A concurrent
  or later close returns `{:action :already-closed}`. The one-argument form sets
  no timeout. The two-argument form adds no callback data. `conn-mgr` may be a
  manager or a delay that resolves to one."
  ([conn-mgr]
   (conn-manager-close! conn-mgr nil nil))
  ([conn-mgr timeout-ms]
   (conn-manager-close! conn-mgr timeout-ms nil))
  ([conn-mgr timeout-ms data]
   (when-not (or (nil? data) (map? data))
     (truss/ex-info! "[Carmine] Expected connection close data to be a map or nil"
       {:eid :carmine.conns/invalid-close-data
        :via 'conn-manager-close!
        :data (enc/typed-val data)}))
   (let [data (some-> data (dissoc :mgr :via :timeout-ms))]
     (conns/mgr-close!
       (have-conn-manager 'conn-manager-close! conn-mgr) timeout-ms data))))

;;;; Config

(def default-conn-opts
  "Default connection options, deeply merged with caller `:conn-opts`.

  `:server` may be:
    - A `redis://` or `rediss://` URI string.
    - `[host port]` or `{:host host, :port port}`.
    - `{:master-name name, :sentinel-spec spec, :sentinel-opts opts}`.
    - `{:cluster-spec spec, :cluster-opts opts}`.

  `:init` controls connection initialization:
    - `:resp3?` uses RESP3 through `HELLO 3` (default true, Redis 6+).
    - `:auth` is an optional `{:username _, :password _}` map.
    - `:client-name` is a string, `(fn [conn-opts])`, or nil. Omit it to
      generate a unique Carmine name. Nil disables the client name.
    - `:select-db` is an optional Redis database number.
    - `:commands` is a sequence of Redis command sequences that replaces all
      default initialization commands. Empty command sequences are ignored.

  `:socket-opts` controls connection, read, readiness, SSL, and standard Java
  socket options. `:ssl true` uses the JVM trust store and verifies the server
  hostname. An `:ssl` function receives a
  `{:keys [conn-opts socket host port]}` map and must return a `java.net.Socket`
  whose `close` also closes the given raw `socket` (for example, pass `true` for
  `autoClose` to `SSLSocketFactory.createSocket`). It is responsible for
  certificate and hostname verification.

  Within `:socket-opts`, a positive `:init-timeout-ms` (default 5000) limits
  connection initialization. It is a per-read socket timeout and a deadline
  for each built-in TLS handshake and `:init` reply exchange, so an unresponsive
  or byte-trickling server cannot block connection creation indefinitely. Nil
  installs no initialization deadline; the ambient `:read-timeout-ms`, if any,
  still applies per read. Zero makes initialization explicitly unbounded. A
  custom `:ssl` function must limit its own handshake.

  Within `:socket-opts`, `:ready-timeout-ms` (default 200) limits each readiness
  `PING` reply both per read and in aggregate. Nil uses the ambient
  `:read-timeout-ms` for both limits. Zero removes the limit. The top-level
  `:buffer-opts` sets the initial input and output sizes.

  `:resp-opts :limits` sets connection-level RESP limits. Each may be nil to
  disable it:
    - `:max-line-bytes`: Bytes before CRLF (default 1 MiB).
    - `:max-nesting-depth`: Nested aggregate containers (default 128).
    - `:max-blob-bytes`: Fixed or cumulative streamed blob payload
      (default nil/unbounded).
    - `:max-aggregate-elements`: Values, or key/value pairs for maps
      (default nil/unbounded).
    - `:max-frame-bytes`: All wire bytes in one top-level RESP value
      (default nil/unbounded).

  A limit breach is connection-fatal and invalidates a pooled connection.
  These options limit space. Use `:socket-opts :read-timeout-ms` to limit time.
  Applications exposed to untrusted or unusually large replies should set
  finite blob, aggregate, and frame limits. `:cbs` may contain unary
  `:on-conn-close`, `:on-conn-error`, and `:on-push-error` callbacks. See
  [[*conn-cbs*]] for callback lifecycle semantics.

  The EDN value at configuration key
  `:taoensso.carmine.default-conn-opts` is deeply merged over built-in
  defaults."
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
         :socket-opts {:ssl false, :connect-timeout-ms 400, :read-timeout-ms nil
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
  "Default options for the Apache Commons Pool backing [[conn-manager]].

  By default, connections are validated when borrowed and while idle.
  Borrow validation always checks that the socket is open
  and its Sentinel address is current. It sends `PING` only after the connection
  was idle for at least `:ready-check-after-idle-ms` (default 5000). A nil or
  zero value sends `PING` during each validation.
  `:socket-opts :ready-timeout-ms` limits the `PING` reply per read and in
  aggregate (default 200). Nil uses the ambient `:read-timeout-ms` for both
  limits. Zero removes the limit.

  The pool permits 16 total connections and 16 idle connections. Time options
  accept milliseconds or the `java.time.Duration` types that Commons Pool
  supports. See [[conn-manager]] for the pool type.

  The EDN value at configuration key
  `:taoensso.carmine.default-pool-opts` is deeply merged over built-in defaults."
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

    (opts/merge-pool-opts base from-env)))

(def default-cluster-pool-opts
  "Default keyed-pool options used by [[conn-manager-clustered]].

  The pool permits 64 total connections. Each node address permits 16 total
  connections and 16 idle connections. Connections are validated when created,
  borrowed, and while idle. The EDN value at key
  `:taoensso.carmine.default-cluster-pool-opts` is deeply merged over built-in
  defaults."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-cluster-pool-opts)
        base
        {:ready-check-after-idle-ms      5000
         :test-on-create?               true
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
  "Default options owned by each stateful `SentinelSpec`.

  Discovery policy:
    - `:update-replicas?` refreshes the shared replica cache (default true).
    - `:update-sentinels?` learns additional Sentinel addresses (default true).
  A manager selects preferred replicas from this shared cache. Its preference
  does not change what the spec discovers.

  Timing options are in milliseconds:
    - `:retry-delay-ms` between failed resolution rounds.
    - `:resolve-timeout-ms` for an entire resolution attempt.
    - `:resolve-cache-ttl-ms` limits how long pooled borrow validation may use
      cached topology without a single-flight refresh (default 5000). Nil
      disables proactive refresh. This cache policy applies only when a pool
      validates an existing connection. Creating a pooled connection always
      performs a full single-flight Sentinel resolution.
  `:conn-opts` configures the short-lived connections used to query Sentinel
  and, by default, to verify node roles. Sentinel servers and data nodes may use
  different options (e.g. authenticated data nodes behind unauthenticated
  Sentinels). `:node-conn-opts` is deeply merged over `:conn-opts` only
  for data-node ROLE verification. It does not affect application connections.
  `:cbs` supports `:on-resolve-success`, `:on-resolve-error`,
  `:on-changed-master`, `:on-changed-replicas`, and `:on-changed-sentinels`.
  Each value is a unary function that receives a data map.

  The EDN value at configuration key
  `:taoensso.carmine.default-sentinel-spec-opts` is deeply merged over
  built-in defaults. Constructor options then override it. Options are parsed
  once when the `SentinelSpec` is created."
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

         :retry-delay-ms        250
         :resolve-timeout-ms    2000
         :resolve-cache-ttl-ms  5000

         :conn-opts
         {:cbs         {:on-conn-close nil, :on-conn-error nil}
          :buffer-opts {:init-size-in 512, :init-size-out 256}
          :socket-opts {:ssl false, :connect-timeout-ms 200, :read-timeout-ms 200
                        :ready-timeout-ms 200}}}]

    (enc/nested-merge base from-env)))

(def default-sentinel-opts
  "Default per-manager Redis Sentinel options.

  `:prefer-read-replica?` selects a random known replica and falls back to the
  master (default false). The shared `SentinelSpec` controls replica discovery.

  `:clear-timeout-ms` controls how long a pooled manager waits while draining
  stale connections after one of its resolutions observes a verified master
  change. Nil waits without limit; after a finite timeout, remaining connections
  are interrupted. Managers that share a `SentinelSpec` otherwise reject stale
  shared-cache addresses during pool validation, so keep the default
  `:test-on-borrow? true` unless the application provides equivalent validation.

  The EDN value at configuration key
  `:taoensso.carmine.default-sentinel-opts` is deeply merged over built-in
  defaults. Per-manager `:sentinel-opts` then override it."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-sentinel-opts)
        base {:prefer-read-replica? false, :clear-timeout-ms 10000}]
    (enc/nested-merge base from-env)))

(def default-cluster-spec-opts
  "Default Redis Cluster topology discovery options stored by `ClusterSpec`.

  `:topology-source` may be `:auto`, `:cluster-shards`, or `:cluster-slots`.
  `:auto` prefers `CLUSTER SHARDS` and uses `CLUSTER SLOTS` if necessary.
  `:refresh-timeout-ms` is the deadline for a synchronous candidate sweep
  (default 2000). Zero prevents discovery I/O. The deadline applies only to I/O;
  a complete topology reply is always parsed and used.
  `:conn-opts` configures short-lived discovery connections and inherits the
  ordinary connection defaults except `:server`. The remaining sweep time
  limits each discovery connection's socket timeouts.

  `:cbs` supports unary `:on-refresh-success`, `:on-refresh-error`, and
  `:on-changed-topology` callbacks. The EDN value at configuration key
  `:taoensso.carmine.default-cluster-spec-opts` is deeply merged over built-in
  defaults. Options passed to [[cluster-spec]] then override it. All managers
  that use a spec share its discovery policy."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-cluster-spec-opts)
        base
        {:cbs
         {:on-refresh-success  nil
          :on-refresh-error    nil
          :on-changed-topology nil}

         :topology-source    :auto
         :refresh-timeout-ms 2000

         :conn-opts
         {:cbs         {:on-conn-close nil, :on-conn-error nil}
          :buffer-opts {:init-size-in 4096, :init-size-out 256}
          :socket-opts {:ssl false, :connect-timeout-ms 200, :read-timeout-ms 200
                        :ready-timeout-ms 200}}}]

    (enc/nested-merge base from-env)))

(def default-cluster-opts
  "Default per-manager Redis Cluster execution options.

  These options limit redirection handling for each flush:
    - `:max-retry-rounds` limits retries after the initial attempt (default 4).
    - `:retry-backoff-ms` delays TRYAGAIN/CLUSTERDOWN rounds (default 25 ms).

  `:max-concurrent-partitions` limits the Redis nodes contacted in parallel in
  one execution round. The default 1 uses serial node execution. Independent
  callers may still execute concurrently.

  MOVED retries the advertised node and refreshes the topology. ASK sends
  ASKING and retries the advertised node without changing
  cached routes. Transport failures are not replayed because the execution
  outcome may be ambiguous.

  The EDN value at configuration key
  `:taoensso.carmine.default-cluster-opts` is deeply merged over built-in
  defaults. A manager's `:server :cluster-opts` then override it. Discovery
  options belong to [[cluster-spec]], not to individual managers."
  (let [from-env (enc/get-env {:as :edn} :taoensso.carmine.default-cluster-opts)
        base
        {:max-retry-rounds 4
         :retry-backoff-ms 25
         :max-concurrent-partitions 1}]

    (enc/nested-merge base from-env)))

;;;;

(def ^:dynamic *auto-freeze?*
  "When true, automatically serializes non-native Redis arguments with Nippy.

  The default is true. Configure it with `:taoensso.carmine.auto-freeze`. When
  false, unsupported arguments throw unless you convert them to a native type
  or wrap them with [[freeze]].

  See also [[freeze]], [[bytes]], and [[*auto-thaw?*]]."
  (enc/get-env {:as :bool, :default true}
    :taoensso.carmine.auto-freeze))

(def ^:dynamic *auto-thaw?*
  "When true, automatically thaws replies marked as serialized with Nippy.

  The default is true. Configure it with `:taoensso.carmine.auto-thaw`. Bind it
  to false to keep marked payloads encoded. [[thaw]] can supply explicit Nippy
  options for blob replies; marked payloads still require this setting to be
  true.

  See also [[*auto-freeze?*]], [[freeze]], and [[thaw]]."
  (enc/get-env {:as :bool, :default true}
    :taoensso.carmine.auto-thaw))

(def ^:dynamic *raw-verbatim-strings?*
  "When true, each RESP3 verbatim string decodes to a record with `:format` and
  `:content`. When false (default), it decodes to its content text. The
  [[as-bytes]] and [[thaw]] read modes do not change either result."
  false)

(def ^:dynamic *keywordize-maps?*
  "When true, keywordize string keys in RESP3 map replies. Non-string keys are
  unchanged. False by default so generic RESP3 decoding preserves wire values."
  false)

(def ^:dynamic *freeze-opts*
  "Default Nippy options for [[freeze]]. Nil uses the Nippy defaults. [[freeze]]
  captures this binding when it wraps a value, before requests are written."
  nil)

(def ^:dynamic *issue-83-workaround?*
  "Compatibility workaround for incorrectly marked Nippy blobs. It applies only
  when [[*auto-thaw?*]] is true.

  A Carmine v2.6.0 to v2.6.1 bug (2014-04-01 to 2014-05-01) wrote incorrect
  markers. See <https://github.com/ptaoussanis/carmine/issues/83>.

  When enabled, Carmine tries to thaw binary-marked blob payloads that start
  with a valid Nippy header. A failed attempt produces a reply error, thrown or
  returned according to `:error-mode`.

  Enable iff you might read data written by Carmine before v2.6.1 (2014-05-01).
  The default is false. Configure it with
  `:taoensso.carmine.issue-83-workaround`."
  (enc/get-env {:as :bool, :default false}
    :taoensso.carmine.issue-83-workaround))

(def ^:dynamic *conn-cbs*
  "Map of global callback functions, called in addition to matching callbacks in
  `conn-opts`, `SentinelSpec`, or `ClusterSpec`. They are useful
  for REPL work, debugging, and tests.

  Possible keys:
    `:on-conn-close`
    `:on-conn-error`
    `:on-resolve-success`
    `:on-resolve-error`
    `:on-changed-master`
    `:on-changed-replicas`
    `:on-changed-sentinels`
    `:on-refresh-success`
    `:on-refresh-error`
    `:on-changed-topology`
    `:on-push-error`

  Each value should be a unary function that receives a data map. Every map
  contains `:cbid`. Operation-specific maps may also contain `:eid`, `:via`,
  `:cause`, and `:elapsed-ms`.

  Callbacks run synchronously on the triggering thread, which may be a borrow,
  eviction, shutdown, or push-dispatch thread. They must be fast and
  non-blocking, and must not perform Carmine operations.

  `:on-conn-close` reports that a constructed connection transitioned to
  closed and socket cleanup was attempted. It fires at most once per
  connection, including when protocol initialization fails before the
  connection is returned to user code. Connect, SSL wrapping, and stream setup
  failures occur before a connection is constructed; they emit
  `:on-conn-error` but no close callback. Close callbacks may be deferred until
  internal connection-manager locks have been released.

  Callback failures are logged and isolated."
  nil)

;;;; Core API (main entry point to Carmine)

(defmacro with-cluster-target
  "Evaluates `body` with an explicit Redis Cluster target.

  Target selectors are `{:key key}`, `{:slot slot}`, `{:addr [host port]}`,
  `{:node-id id}`, `:any`, `:masters`, and `:nodes`. Keyed commands usually
  infer their target. They reject a conflicting explicit target. `:masters` and
  `:nodes` broadcast each targetless command in `body`. Each such command
  returns one source-associated collection at its logical reply position."
  [target & body]
  `(cluster/with-cluster-target* ~target (fn [] ~@body)))

(defn with-car
  "Borrows from `conn-mgr` and calls `(body-fn)` in a Redis request context.
  Then it flushes the queued requests as one pipeline and returns their replies.
  `conn-mgr` is required. It may be a manager or a delay that resolves to one.

  Canonical reply options:
    - `:as-vec?`: Return one or more replies as a vector.
    - `:natural-replies?`: Hard-bypass special read modes, reply parsers, and
      Carmine's default decoding in this reply boundary, regardless of inner
      dynamic bindings. A nested reply boundary must repeat the option. See
      [[natural-replies]] for complete dynamic-extent coverage.
    - `:error-mode`: Use `:throw` (default) or `:return` for reply errors.

  With `:as-vec?`, zero replies return nil and one or more replies return a
  non-empty vector. Nil supports forms such as
  `(when-let [[a b] replies] ...)`. Without `:as-vec?`, one reply returns
  directly and multiple replies return a vector. With the default
  `:error-mode :throw`, all replies are consumed before an error is thrown. The
  error data contains the full `:replies` vector and `:error-indexes`. Use
  `:return` to receive reply errors in their original positions.

  For ordinary/Sentinel managers, the borrowed connection is owned by the
  manager. `body-fn` must not retain or close it. Nested, same-manager [[wcar]]
  calls inside `body-fn` are supported, but borrow a second connection. They can
  block if the pool is exhausted. Use [[with-replies]] for a nested reply
  boundary on the same connection.

  For [[conn-manager-clustered]], no single connection represents the request
  context. Each flush borrows the required node connections. A cross-node
  pipeline preserves reply order, but not global Redis execution order.
  Separate and nested flushes have no connection affinity. Use [[wcar]] unless
  you need a function API."
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
  "Queues the Redis requests in `body`, flushes them as one pipeline, and
  returns their replies. `conn-mgr` is required. It may be a manager or a delay
  that resolves to one.

  An optional leading reply-options map supports `:as-vec?`,
  `:natural-replies?` (a hard bypass of special read modes, parsers, and
  Carmine's default decoding in this reply boundary, regardless of inner
  dynamic bindings; a nested reply boundary must repeat the option; see
  [[natural-replies]] for complete dynamic-extent coverage), and `:error-mode`.
  `:as-vec` is shorthand for `{:as-vec? true}`.
  See [[with-car]] for return/error semantics.
  The reply options must be a literal map or `:as-vec`. A symbol or expression
  that returns a map is a body form, and its value is discarded. Use
  [[with-car]] for runtime reply options.

  A nested [[wcar]] or [[with-replies]] first flushes its parent context, so
  inner replies can be used to construct later outer requests.

  In Cluster contexts, native commands use Redis key specifications to route
  ordinary string keys. They reject known cross-slot calls before I/O. In an
  arbitrary [[rcmd]] call, routing keys should be wrapped with [[cluster-key]]."
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
  "Creates a nested reply boundary in an active [[wcar]] or [[with-car]] body.
  First, it flushes pending parent requests. Then it flushes the requests in
  `body` and returns their replies. You can use these replies to construct later
  requests in the parent context.

  It accepts the same reply options and shorthand as [[wcar]]. Call it only
  from an existing request context. See [[with-car]] for return and error rules."
  {:arglists '([& body] [reply-opts & body])}
  [& body]
  (let [[reply-opts body] (resp/parse-body-reply-opts body)]
    `(resp/with-replies ~reply-opts nil
       (fn [] ~@body))))

;;;; Native Redis command API

(commands/defcommands)
