(ns taoensso.carmine-v4.message-queue
  "Carmine v4 message queue for Redis 7+.

  Requirements and delivery contract

  Redis 7.0 or later is checked when a queue handle is opened. `WAITAOF`
  durability needs Redis 7.2 or later. Standalone, Sentinel-master, and Cluster
  connection managers are supported. One queue occupies one Cluster slot;
  distribute hot workloads across queue names. Cluster managers reject
  durability barriers before writing.

  Delivery uses Redis server time, leases, and at-least-once semantics. Lease
  tokens fence settlement and extension, so a stale handler cannot change a
  newer claim. They cannot make an external side effect exactly once: handlers
  must be idempotent. Use a stable explicit message ID (`:mid`) as the
  idempotency key when an enqueue may be retried.

  Queue state uses persistent Redis keys without expiry. Do not use an
  `allkeys-*` eviction policy. Prefer `noeviction`, a dedicated deployment, or
  a volatile-only policy that cannot select these keys. Dead letters have no
  automatic retention limit.

  Quick start

    (require '[taoensso.carmine-v4 :as car]
             '[taoensso.carmine-v4.message-queue :as mq])

    (defonce mgr (car/conn-manager))
    (def emails (mq/queue mgr \"emails\" {:max-attempts 8}))

    (mq/msg-enqueue! emails {:recipient \"person@example.com\"}
      {:mid \"welcome:123\", :priority :high, :delay-ms 5000})

    (def worker
      (mq/worker-create emails
        (fn [{:keys [msg]}]
          (send-email! msg)
          (mq/outcome:ack))
        {:concurrency 4}))

    (mq/worker-start! worker)
    ;; During application shutdown:
    (mq/worker-stop! worker)
    (mq/worker-await-stopped! worker 10000)
    (car/conn-manager-close! mgr)

  Handlers return [[outcome:ack]], [[outcome:retry]], [[outcome:dead]], or
  [[outcome:discard]]. An ordinary exception or invalid return causes a retry;
  the message's exhaustion policy eventually makes it terminal. By default a
  handler extends a long lease explicitly through its `:extend-lease!` input.
  [[worker-create]] can instead enable an automatic heartbeat.

  Queue handles borrow a [[taoensso.carmine-v4/conn-manager?]] without closing
  it. Stop and await every worker before closing its manager. Public option maps
  reject unknown unqualified keys; namespaced keyword keys are reserved.

  Public API organization

    - `msg-*` functions act on one queued message.
    - `queue-*` functions inspect or administer a queue.
    - `worker-*` functions manage a process-local consumer.
    - `dead-*` functions inspect or administer retained dead messages.
    - `v3-*` migration functions are in
      [[taoensso.carmine-v4.message-queue.migration]].

  Use [[queue-status]] for exact Redis counts and head-of-line gauges, and
  [[worker-stats]] for process-local worker counters and timings. Monitor worker
  state and errors, dead letters, and overdue scheduled or expired leased work.
  Use [[dead-page]] for large retained sets and schedule [[dead-purge!]] when
  policy permits permanent deletion.

  Redis key schema

  MID means message ID.

  Each queue owns 16 keys. Their prefix is
  `carmine:mq:v4:{<first-16-hex-of-SHA1(qname)>}:<qname>:`. The digest-derived
  hash tag puts all keys for one queue in the same Redis Cluster slot, so braces
  in a queue name do not affect routing. No expiry is set.

    - `config`             HASH   Durable queue configuration. It is retained
                                  by [[queue-clear!!]].
    - `seq`                STRING Monotonic integer used for ready FIFO order.
    - `payloads`           HASH   Active MID -> Nippy payload envelope.
    - `meta`               HASH   Active MID -> packed active metadata.
    - `successor-payloads` HASH   Coalesced successor payload by active MID.
    - `successor-meta`     HASH   Packed successor metadata by active MID.
    - `ready-high`         ZSET   High-priority MID -> FIFO sequence.
    - `ready-normal`       ZSET   Normal-priority MID -> FIFO sequence.
    - `ready-low`          ZSET   Low-priority MID -> FIFO sequence.
    - `scheduled`          ZSET   MID -> available-at Redis epoch milliseconds.
    - `leased`             ZSET   MID -> lease-expiry Redis epoch milliseconds.
    - `lease-tokens`       HASH   Leased MID -> fencing token.
    - `dead`               ZSET   Dead MID -> failed-at Redis epoch milliseconds.
    - `dead-payloads`      HASH   Retained dead-letter payload by MID.
    - `failures`           HASH   Packed failure metadata by dead MID.
    - `signal`             LIST   Advisory wake baton. Its size is zero or one.

  The config hash identifies an initialized queue. If it is missing but another
  queue key exists, [[queue]] does not open or change the queue. The signal list
  only optimizes wake-up and is not required for correctness.

  Stored message state machine

  An active generation is in one state: `ready`, `scheduled`, or `leased`.
  Absence is the fourth normal state. `overdue` and `lease-expired` are
  diagnostic views, not stored states. An expired lease is eligible for a
  token-fenced transition, but expiry does not revoke its token. `corrupt` is
  also diagnostic, and a dead letter may coexist with a new active generation
  of the same MID.

    absent    -> ready/scheduled  enqueue or dead-letter redrive
    scheduled -> ready            bounded due maintenance
    ready     -> leased           claim
    leased    -> ready/scheduled  retry, expiry maintenance, or release
    leased    -> absent           acknowledge or discard
    leased    -> dead             explicit or exhausted failure

  While the active generation's attempt is positive, duplicate coalescing may
  keep one successor. A new coalescing enqueue atomically replaces it. Terminal
  settlement, exhaustion, or release atomically promotes it to `ready` or
  `scheduled`.

  Core durable invariants

    - `payloads` MIDs must equal `meta` MIDs.
    - Every active MID occurs in exactly one ready/scheduled/leased index.
    - `lease-tokens` MIDs must equal `leased` MIDs.
    - Successor payload and metadata MIDs must match and be a subset of active
      MIDs.
    - `dead` MIDs must equal `dead-payloads` and `failures` MIDs.

  Packed schema version 1 uses additive positional layouts:

    active    [schema priority attempt max-attempts enqueued-at
               on-exhaustion revision lease-override]
    successor [schema priority max-attempts enqueued-at on-exhaustion revision
               available-at lease-override]
    failure   [schema reason failed-at attempt priority max-attempts
               enqueued-at on-exhaustion revision lease-override]

  These packed representations are private implementation details. Public
  inspection does not expose them; use [[msg-info]], [[msg-active-page]],
  [[queue-status]], and [[dead-info]]. Use [[dead-redrive!]] and
  [[queue-clear!!]] for recovery.
  A worker lifecycle is `new -> running -> stopping -> stopped -> closed`;
  stop-before-start gives `new -> stopped`, and a fatal runner failure gives
  `running -> failed`. Workers cannot restart. See [[worker-create]],
  [[worker-stats]], and [[worker-await-stopped!]].

  A durability-barrier miss is still a committed write. A transport failure
  during the barrier makes the requested durability unknown; neither condition
  means Redis rolled back the write. See [[queue]] for the full durability
  contract."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string        :as str]
   [taoensso.encore       :as enc]
   [taoensso.truss        :as truss]
   [taoensso.trove        :as trove]
   [taoensso.carmine-v4             :as car]
   [taoensso.carmine-v4.cluster     :as cluster]
   [taoensso.carmine-v4.conns       :as conns]
   [taoensso.carmine-v4.resp        :as resp]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write  :as write])
  (:import
   [java.nio.charset StandardCharsets]
   [java.util Base64 Base64$Decoder Base64$Encoder]
   [java.util.concurrent ExecutorService Executors ScheduledExecutorService
    ScheduledFuture ScheduledThreadPoolExecutor ThreadFactory TimeUnit]
   [java.util.concurrent.atomic AtomicLong]
   [taoensso.carmine_v4.classes ReusableConnError]))

(def ^:private schema-version 1)
(def ^:private redis-7-version-num 458752)   ; 0x070000
(def ^:private redis-7-2-version-num 459264) ; 0x070200
(def ^:private priority->code {:high 0, :normal 1, :low 2})
(def ^:private code->priority {0 :high, 1 :normal, 2 :low})
(def ^:private key-suffixes
  [:config :seq :payloads :meta :successor-payloads :successor-meta
   :ready-high :ready-normal :ready-low :scheduled :leased :lease-tokens
   :dead :dead-payloads :failures :signal])

(defn- validate-opts!
  "Returns an options map after rejecting unknown unqualified keys.
  Namespaced keyword keys are reserved for extensions and metadata."
  [context opts allowed]
  (let [opts (if (nil? opts) {} opts)]
    (when-not (map? opts)
      (truss/ex-info! "[Carmine] Options must be a map or nil"
        {:eid :carmine.mq/invalid-option, :context context, :value opts}))
    (when-let [unexpected
               (seq
                 (remove
                   #(or (contains? allowed %)
                      (and (keyword? %) (namespace %)))
                   (keys opts)))]
      (truss/ex-info! "[Carmine] Unexpected options"
        {:eid :carmine.mq/invalid-option, :context context
         :unexpected-keys unexpected, :allowed-keys allowed}))
    opts))

(defn- nil-default
  "Returns default only for nil. Unlike `or`, preserves explicit false so the
  receiving option validator can reject it with the correct contract."
  [value default]
  (if (nil? value) default value))

(def ^:private queue-option-keys
  #{:lease-ms :max-attempts :retry-base-ms :retry-max-ms :retry-jitter
    :on-exhaustion :on-duplicate :revision-mode :durability})
(def ^:private msg-enqueue-option-keys
  #{:mid :priority :delay-ms :lease-ms :max-attempts :on-exhaustion
    :on-duplicate :revision :durability})
(def ^:private msg-claim-option-keys #{:maintenance-batch-size})
(def ^:private msg-settle-option-keys #{:durability})
(def ^:private queue-config-update-option-keys
  #{:lease-ms :max-attempts :retry-base-ms :retry-max-ms :retry-jitter
    :on-exhaustion})
(def ^:private msg-active-page-option-keys
  #{:status :limit :cursor :include-related?})
(def ^:private dead-page-option-keys #{:limit :cursor})
(def ^:private dead-purge-option-keys #{:older-than-ms :limit})
(def ^:private worker-option-keys
  #{:concurrency :maintenance-batch-size :idle-min-ms :idle-max-ms
    :close-timeout-ms :durability :include-msg? :lease-extend-every-ms
    :on-event})

(defn- reply-long [x]
  (if (number? x) (long x) (Long/parseLong (str x))))

(defn- reply-?long [x]
  (when-not (or (nil? x) (= x ""))
    (reply-long x)))

(defn- well-formed-utf16?
  "Returns true iff each UTF-16 surrogate in `s` is in a valid pair."
  [^String s]
  (let [length (.length s)]
    (loop [idx 0]
      (if (>= idx length)
        true
        (let [ch (.charAt s idx)]
          (cond
            (Character/isHighSurrogate ch)
            (if (and (< (inc idx) length)
                  (Character/isLowSurrogate (.charAt s (inc idx))))
              (recur (+ idx 2))
              false)

            (Character/isLowSurrogate ch) false
            :else (recur (inc idx))))))))

(defn- sanitize-utf16
  "Replaces unpaired UTF-16 surrogate code units with U+FFFD."
  ^String [^String s]
  (if (well-formed-utf16? s)
    s
    (let [^chars chars (.toCharArray s)
          length (alength chars)]
      (loop [idx 0]
        (when (< idx length)
          (let [ch (aget chars idx)]
            (cond
              (Character/isHighSurrogate ch)
              (if (and (< (inc idx) length)
                    (Character/isLowSurrogate (aget chars (inc idx))))
                (recur (+ idx 2))
                (do (aset chars idx \ufffd) (recur (inc idx))))

              (Character/isLowSurrogate ch)
              (do (aset chars idx \ufffd) (recur (inc idx)))

              :else (recur (inc idx))))))
      (String. chars))))

(defn- truncate-utf16
  "Truncates well-formed `s` to at most `limit` UTF-16 code units without
  splitting a surrogate pair."
  ^String [^String s ^long limit]
  (let [length (.length s)]
    (if (<= length limit)
      s
      (let [end
            (if (and (pos? limit)
                  (Character/isHighSurrogate (.charAt s (dec limit)))
                  (Character/isLowSurrogate  (.charAt s limit)))
              (dec limit)
              limit)]
        (.substring s 0 end)))))

(defn- safe-internal-reason [x]
  (truncate-utf16 (sanitize-utf16 (str x)) 1024))

(defn- throwable-reason [^Throwable throwable]
  (safe-internal-reason
    (str (.getName (class throwable)) ": "
      (or (.getMessage throwable) ""))))

(defn- qname-str [qname]
  (let [s (enc/as-qname qname)]
    (when (str/blank? s)
      (truss/ex-info! "[Carmine] Queue name cannot be blank"
        {:eid :carmine.mq/invalid-queue-name, :qname qname}))
    (when-not (well-formed-utf16? s)
      (truss/ex-info! "[Carmine] Queue name contains malformed UTF-16"
        {:eid :carmine.mq/invalid-queue-name, :qname qname
         :reason "Text contains an unpaired UTF-16 surrogate"}))
    (when (> (count s) 256)
      (truss/ex-info! "[Carmine] Queue name exceeds 256 UTF-16 code units"
        {:eid :carmine.mq/invalid-queue-name, :length (count s)}))
    (when (str/starts-with? s "\u0000")
      (truss/ex-info! "[Carmine] Queue name cannot begin with U+0000"
        {:eid :carmine.mq/invalid-queue-name, :qname qname
         :reason "The first U+0000 character is reserved by Carmine"}))
    s))

(defn- mid-str [mid]
  (let [s
        (cond
          (string? mid) mid
          (keyword? mid) (subs (str mid) 1)
          (symbol? mid)  (str mid)
          (uuid? mid)    (str mid)
          (integer? mid) (str mid)
          :else
          (truss/ex-info! "[Carmine] MID must be textual"
            {:eid :carmine.mq/invalid-mid, :mid mid}))]
    (when (str/blank? s)
      (truss/ex-info! "[Carmine] MID cannot be blank"
        {:eid :carmine.mq/invalid-mid, :mid mid}))
    (when-not (well-formed-utf16? s)
      (truss/ex-info! "[Carmine] MID contains malformed UTF-16"
        {:eid :carmine.mq/invalid-mid, :mid mid
         :reason "Text contains an unpaired UTF-16 surrogate"}))
    (when (> (count s) 512)
      (truss/ex-info! "[Carmine] MID exceeds 512 UTF-16 code units"
        {:eid :carmine.mq/invalid-mid, :length (count s)}))
    (when (str/starts-with? s "\u0000")
      (truss/ex-info! "[Carmine] MID cannot begin with U+0000"
        {:eid :carmine.mq/invalid-mid, :mid mid
         :reason "The first U+0000 character is reserved by Carmine"}))
    s))

(defn- qkeys [qname]
  (let [qname (qname-str qname)
        ;; The first hash tag is digest-derived, so braces in user queue names
        ;; cannot alter Cluster routing.
        tag (subs (car/script-hash qname) 0 16)
        prefix (str "carmine:mq:v4:{" tag "}:" qname ":")]
    (persistent!
      (reduce
        (fn [m suffix] (assoc! m suffix (str prefix (name suffix))))
        (transient {}) key-suffixes))))

(defn- checked-mgr [mgr]
  (let [mgr (some-> mgr force)]
    (when-not mgr
      (truss/ex-info! "[Carmine] A v4 connection manager is required"
        {:eid :carmine.mq/manager-required}))
    (truss/have conns/conn-manager? mgr)
    (when (get-in (conns/mgr-conn-opts mgr)
            [:server :sentinel-opts :prefer-read-replica?])
      (truss/ex-info! "[Carmine] Queue operations require a Redis master"
        {:eid :carmine.mq/replica-manager-not-supported}))
    mgr))

(deftype Queue [manager name opts keys]
  Object
  (toString [_] (str "#<CarmineV4Queue " (pr-str name) ">")))
(alter-meta! #'->Queue assoc :private true)

(defn- new-queue [manager name opts keys] (Queue. manager name opts keys))
(defn- queue-manager [^Queue queue] (.-manager queue))
(defn- queue-name    [^Queue queue] (.-name    queue))
(defn- queue-opts    [^Queue queue] (.-opts    queue))
(defn- queue-keys    [^Queue queue] (.-keys    queue))

(defn queue?
  "Returns true iff `x` is a Carmine v4 MQ queue handle."
  [x] (instance? Queue x))

(defmethod print-method Queue [q ^java.io.Writer w]
  (.write w (.toString ^Queue q)))

(defn- mq-script [resource common?]
  (str (enc/slurp-resource "taoensso/carmine/lua/mq_v4/schema.lua") "\n"
    (when common?
      (str (enc/slurp-resource "taoensso/carmine/lua/mq_v4/common.lua") "\n"))
    (enc/slurp-resource resource)))

(do
  (def ^:private lua-ensure       (delay (enc/slurp-resource "taoensso/carmine/lua/mq_v4/ensure.lua")))
  (def ^:private lua-enqueue      (delay (mq-script "taoensso/carmine/lua/mq_v4/enqueue.lua" true)))
  (def ^:private lua-claim        (delay (mq-script "taoensso/carmine/lua/mq_v4/claim.lua" true)))
  (def ^:private lua-contain-corrupt-payload
    (delay (mq-script "taoensso/carmine/lua/mq_v4/contain-corrupt-payload.lua" true)))
  (def ^:private lua-settle       (delay (mq-script "taoensso/carmine/lua/mq_v4/settle.lua" true)))
  (def ^:private lua-release      (delay (mq-script "taoensso/carmine/lua/mq_v4/release.lua" true)))
  (def ^:private lua-extend       (delay (mq-script "taoensso/carmine/lua/mq_v4/extend-lease.lua" false)))
  (def ^:private lua-status       (delay (mq-script "taoensso/carmine/lua/mq_v4/status.lua" false)))
  (def ^:private lua-active-page  (delay (mq-script "taoensso/carmine/lua/mq_v4/active-page.lua" false)))
  (def ^:private lua-remove       (delay (enc/slurp-resource "taoensso/carmine/lua/mq_v4/remove.lua")))
  (def ^:private lua-clear        (delay (enc/slurp-resource "taoensso/carmine/lua/mq_v4/clear.lua")))
  (def ^:private lua-redrive      (delay (mq-script "taoensso/carmine/lua/mq_v4/redrive.lua" true)))
  (def ^:private lua-config-update (delay (mq-script "taoensso/carmine/lua/mq_v4/config-update.lua" false)))
  (def ^:private lua-queue-status (delay (mq-script "taoensso/carmine/lua/mq_v4/queue-status.lua" false)))
  (def ^:private lua-dead-letter  (delay (mq-script "taoensso/carmine/lua/mq_v4/dead-letter.lua" false)))
  (def ^:private lua-dead-page    (delay (mq-script "taoensso/carmine/lua/mq_v4/dead-page.lua" false)))
  (def ^:private lua-purge-dead   (delay (enc/slurp-resource "taoensso/carmine/lua/mq_v4/purge-dead.lua"))))

(defmacro ^:private with-mq-protocol-bindings [& body]
  `(binding [car/*auto-freeze?*     true
             car/*freeze-opts*      nil
             car/*auto-thaw?*       true
             car/*keywordize-maps?* false
             com/*natural-replies?* false
             com/*read-mode*        nil
             com/*parser*           nil]
     ~@body))

(defn- run-lua [queue script keys args]
  (with-mq-protocol-bindings
    (car/wcar (queue-manager queue) (car/lua @script keys args))))

(defn- fatal-throwable? [t]
  (or (instance? VirtualMachineError t) (instance? ThreadDeath t)
      (instance? LinkageError t)))

(defn- mq-payload-decode-error?
  "Returns true only for a Carmine Nippy reply-decoding failure.

  An ordinary serialized ExceptionInfo value remains a valid message even if
  its ex-data coincidentally matches, because it is not a Carmine ReplyError. A
  fatal decoder cause is a local process failure, not stored payload corruption.
  This function rethrows that cause before containment."
  [x]
  (let [decode-error?
        (com/reply-error? {:eid :carmine.read/nippy-thaw-error} x)]
    (when (and decode-error? (instance? Throwable x))
      (let [cause (.getCause ^Throwable x)]
        (when (fatal-throwable? cause)
          (throw cause))))
    decode-error?))

(defn- mq-decode-error-stored-payload ^bytes [decode-error]
  (let [^bytes body (get-in (ex-data decode-error) [:bytes :content])
        n (alength body)
        stored (byte-array (+ n 2))]
    (aset-byte stored 0 (byte 0))
    (aset-byte stored 1 (byte 62))
    (System/arraycopy body 0 stored 2 n)
    stored))

(declare positive-long nonnegative-long)

(defn- normalize-durability [context durability]
  (when (some? durability)
    (when-not (map? durability)
      (truss/ex-info! "[Carmine] Durability policy must be a map or nil"
        {:eid :carmine.mq/invalid-durability, :context context
         :value durability}))
    (let [wait? (contains? durability :replicas)
          aof? (or (contains? durability :aof-local)
                 (contains? durability :aof-replicas))
          allowed (if wait? #{:replicas :timeout-ms}
                    #{:aof-local :aof-replicas :timeout-ms})
          unexpected (seq (remove allowed (keys durability)))]
      (when (or (= wait? aof?) unexpected)
        (truss/ex-info! "[Carmine] Expected exactly one durability mode"
          {:eid :carmine.mq/invalid-durability, :context context
           :value durability, :unexpected-keys unexpected}))
      (let [timeout-ms (positive-long :durability/timeout-ms
                         (nil-default (:timeout-ms durability) 1000))]
        (if wait?
          {:mode :wait
           :replicas (positive-long :durability/replicas (:replicas durability))
           :timeout-ms timeout-ms}
          (let [aof-local (nonnegative-long :durability/aof-local
                            (nil-default (:aof-local durability) 0))
                aof-replicas (nonnegative-long :durability/aof-replicas
                               (nil-default (:aof-replicas durability) 0))]
            (when (or (> aof-local 1) (zero? (+ aof-local aof-replicas)))
              (truss/ex-info! "[Carmine] Invalid WAITAOF durability threshold"
                {:eid :carmine.mq/invalid-durability, :context context
                 :value durability}))
            {:mode :waitaof, :aof-local aof-local
             :aof-replicas aof-replicas, :timeout-ms timeout-ms}))))))

(defn- assert-durability-topology! [queue durability]
  (when (and durability
          (conns/mgr-cluster-server (queue-manager queue)))
    (truss/ex-info! "[Carmine] Durability barriers require connection affinity and are not supported by Cluster managers"
      {:eid :carmine.mq/cluster-durability-unsupported
       :qname (queue-name queue), :durability durability}))
  durability)

(defn- durability-error [t]
  (let [data (ex-data t)]
    (cond-> {:message (or (.getMessage ^Throwable t) (.getName (class t)))}
      (:code data) (assoc :code (:code data))
      (:eid data)  (assoc :eid  (:eid data)))))

(defn- durability-command-error? [t]
  (instance? ReusableConnError t))

(defn- mark-durability-transport-error! []
  (when-let [reusable?_ write/*conn-reusable?_]
    (vreset! reusable?_ false)))

(defn- preserve-interruption! [t]
  (when (instance? InterruptedException t)
    (.interrupt (Thread/currentThread))))

(defn- await-durability [durability]
  (let [started (System/nanoTime)
        requested
        (case (:mode durability)
          :wait {:replicas (:replicas durability), :timeout-ms (:timeout-ms durability)}
          :waitaof {:aof-local (:aof-local durability)
                    :aof-replicas (:aof-replicas durability)
                    :timeout-ms (:timeout-ms durability)})]
    (try
      (let [reply
            (case (:mode durability)
              :wait
              (car/with-replies
                (resp/rcmd* ["WAIT" (:replicas durability) (:timeout-ms durability)]))

              :waitaof
              (car/with-replies
                (resp/rcmd* ["WAITAOF" (:aof-local durability)
                              (:aof-replicas durability) (:timeout-ms durability)])))
            observed
            (case (:mode durability)
              :wait {:replicas (reply-long reply)}
              :waitaof {:aof-local (reply-long (nth reply 0))
                        :aof-replicas (reply-long (nth reply 1))})
            satisfied?
            (case (:mode durability)
              :wait (>= (:replicas observed) (:replicas durability))
              :waitaof (and (>= (:aof-local observed) (:aof-local durability))
                         (>= (:aof-replicas observed) (:aof-replicas durability))))]
        {:mode (:mode durability), :requested requested, :observed observed
         :satisfied? satisfied?, :ambiguous? false
         :elapsed-ms (/ (- (System/nanoTime) started) 1e6)})
      (catch Exception t
        ;; A fully-drained Redis error implements ReusableConnError. A bare
        ;; ReplyError may instead represent a parser/framing failure, in which
        ;; case the input position is unknown and the connection must not be
        ;; returned to a pool.
        (let [command-error? (durability-command-error? t)]
          (preserve-interruption! t)
          (when-not command-error?
            (mark-durability-transport-error!))
          {:mode (:mode durability), :requested requested
           :satisfied? false, :ambiguous? (not command-error?)
           :error-kind (if command-error? :command :transport)
           :error (durability-error t)
           :elapsed-ms (/ (- (System/nanoTime) started) 1e6)})))))

(defn- run-lua-durable
  [queue script keys args durability write-effecting?]
  (if-not durability
    [(run-lua queue script keys args) nil]
    (with-mq-protocol-bindings
      (car/wcar (queue-manager queue)
        (let [reply (car/with-replies (car/lua @script keys args))
              durability-result
              (when (write-effecting? reply)
                (await-durability durability))]
          (resp/local-echo [reply durability-result]))))))

(defn- lua-keys [queue suffixes]
  (reduce
    (fn [m suffix]
      (assoc m (keyword (str "qk-" (name suffix))) (get (queue-keys queue) suffix)))
    (array-map) suffixes))

(def ^:private max-lua-int
  "Redis Lua numbers are IEEE-754 doubles. Integers are exact only through
  2^53-1, so larger options would lose precision (e.g. distinct revisions could
  compare equal)."
  9007199254740991) ; 2^53-1

(def ^:private max-duration-ms
  "Conservative maximum for durations added to epoch time in Lua. The headroom
  below `max-lua-int` keeps resulting timestamps exactly representable and
  exactly printable by Lua `tostring`. 10^13 ms is approximately 317 years."
  10000000000000) ; 10^13

(def ^:private max-worker-idle-ms
  ;; `await-wake!` adds 500ms before narrowing to Socket#setSoTimeout's int.
  (- Integer/MAX_VALUE 500))

(defn- signed-long [context x]
  (when-not (and (integer? x) (<= (- max-lua-int) x max-lua-int))
    (truss/ex-info! "[Carmine] Expected an integer in Lua-safe signed range"
      {:eid :carmine.mq/invalid-option, :option context, :value x
       :min (- max-lua-int), :max max-lua-int}))
  (long x))

(defn- positive-long [context x]
  (when-not (and (integer? x) (<= 1 x max-lua-int))
    (truss/ex-info! "[Carmine] Expected a positive integer in Lua-safe range"
      {:eid :carmine.mq/invalid-option, :option context, :value x
       :max max-lua-int}))
  (long x))

(defn- nonnegative-long [context x]
  (when-not (and (integer? x) (<= 0 x max-lua-int))
    (truss/ex-info! "[Carmine] Expected a non-negative integer in Lua-safe range"
      {:eid :carmine.mq/invalid-option, :option context, :value x
       :max max-lua-int}))
  (long x))

(defn- bounded-positive-long [context x maximum]
  (let [x (positive-long context x)]
    (when (> x (long maximum))
      (truss/ex-info! "[Carmine] Integer option exceeds its maximum"
        {:eid :carmine.mq/invalid-option, :option context
         :value x, :maximum maximum}))
    x))

(defn- bounded-nonnegative-long [context x maximum]
  (let [x (nonnegative-long context x)]
    (when (> x (long maximum))
      (truss/ex-info! "[Carmine] Integer option exceeds its maximum"
        {:eid :carmine.mq/invalid-option, :option context
         :value x, :maximum maximum}))
    x))

(defn- ensure! [queue]
  (let [{:keys [lease-ms max-attempts retry-base-ms retry-max-ms retry-jitter
                on-exhaustion on-duplicate revision-mode]} (queue-opts queue)
        waitaof? (= (get-in (queue-opts queue) [:durability :mode]) :waitaof)
        required-version (if waitaof? "7.2.0" "7.0.0")
        required-version-num (if waitaof? redis-7-2-version-num redis-7-version-num)
        [action x1 x2 x3 x4 :as reply]
        (run-lua queue lua-ensure
          (lua-keys queue key-suffixes)
          {:required-redis-version required-version
           :required-redis-version-num required-version-num
           :schema schema-version, :lease-ms lease-ms
           :max-attempts max-attempts, :retry-base-ms retry-base-ms
           :retry-max-ms retry-max-ms
           :retry-jitter (name retry-jitter)
           :on-exhaustion-default (name on-exhaustion)
           :on-duplicate-default (name on-duplicate)
           :revision-mode (name revision-mode)})]
    (case action
      ("created" "existing")
      (new-queue (queue-manager queue) (queue-name queue)
        (assoc (queue-opts queue)
          ::redis-version x1, ::redis-version-num (reply-long x2))
        (queue-keys queue))

      "mismatch"
      (truss/ex-info! "[Carmine] Queue configuration does not match Redis"
        {:eid :carmine.mq/config-mismatch, :qname (queue-name queue)
         :field x1, :actual x2, :expected x3})

      "missing-config"
      (truss/ex-info!
        "[Carmine] Queue config is missing while queue artifacts remain"
        {:eid :carmine.mq/config-missing, :qname (queue-name queue)
         :artifact-key-count (reply-long x1)})

      "unsupported-version"
      (truss/ex-info!
        (if waitaof?
          "[Carmine] WAITAOF durability requires Redis 7.2+"
          "[Carmine] Redis 7.0+ is required")
        {:eid :carmine.mq/unsupported-redis-version
         :qname (queue-name queue)
         :feature (if waitaof? :waitaof :message-queue)
         :actual-version x1, :actual-version-num (reply-long x2)
         :required-version x3, :required-version-num (reply-long x4)})

      (truss/ex-info! "[Carmine] Unexpected queue initialization reply"
        {:eid :carmine.mq/unexpected-reply, :reply reply}))))

(defn- assert-durability-version! [queue durability]
  (when (and (= (:mode durability) :waitaof)
          (< (long (get (queue-opts queue) ::redis-version-num -1))
            redis-7-2-version-num))
    (truss/ex-info! "[Carmine] WAITAOF durability requires Redis 7.2+"
      {:eid :carmine.mq/unsupported-redis-version
       :qname (queue-name queue), :feature :waitaof
       :actual-version (get (queue-opts queue) ::redis-version)
       :actual-version-num (get (queue-opts queue) ::redis-version-num)
       :required-version "7.2.0", :required-version-num redis-7-2-version-num})))

(defn queue
  "Creates or opens a named queue that uses `manager`.

  A queue name must be well-formed UTF-16. It may contain U+0000 except as its
  first character, which Carmine reserves for binary wire markers.

  If the durable configuration is missing but other queue keys remain, this
  function throws with `:eid :carmine.mq/config-missing` without changing those
  keys.

  Durable options must be equal on all handles for the queue:

  - `:lease-ms` (60000). [[msg-enqueue!]] may override it for one message.
  - `:max-attempts` (8), `:retry-base-ms` (1000), and `:retry-max-ms` (60000).
  - `:retry-jitter` (`:none` or `:full`).
  - `:on-exhaustion` (`:dead` or `:discard`).
  - `:on-duplicate` (`:reject` or `:coalesce`).
  - `:revision-mode` (`:none` or `:required`). Required revisions need
    `:on-duplicate :coalesce`.

  Local `:durability` adds a barrier on the same connection after an effective
  producer or administration write. `{:replicas n :timeout-ms ms}` uses WAIT.
  `{:aof-local local :aof-replicas n :timeout-ms ms}` uses WAITAOF, where
  `local` is 0 or 1. The default `:timeout-ms` is 1000. Cluster managers reject
  any `:durability` policy before writing. Carmine also rejects unsupported
  Redis versions before writing. The message queue needs Redis 7.0+; WAITAOF
  needs Redis 7.2+ and an applicable AOF configuration.

  A write result includes the barrier result as `:durability`. A barrier miss,
  command error, or configuration error has `:ambiguous? false`; a transport
  error has `:ambiguous? true` because the durability result is unknown. A
  barrier failure does not undo the write, and a no-write result has no barrier.
  The queue borrows the given `manager` without closing it."
  ([manager qname] (queue manager qname nil))
  ([manager qname opts]
   (let [opts (validate-opts! :queue opts queue-option-keys)
         opts (merge {:lease-ms 60000, :max-attempts 8
                      :retry-base-ms 1000, :retry-max-ms 60000
                      :retry-jitter :none
                      :on-exhaustion :dead, :on-duplicate :reject
                      :revision-mode :none}
                opts)
         opts (assoc opts
                :lease-ms       (bounded-positive-long :lease-ms (:lease-ms opts) max-duration-ms)
                :max-attempts    (bounded-positive-long :max-attempts (:max-attempts opts) 1000000)
                :retry-base-ms  (bounded-nonnegative-long :retry-base-ms (:retry-base-ms opts) max-duration-ms)
                :retry-max-ms   (bounded-nonnegative-long :retry-max-ms (:retry-max-ms opts) max-duration-ms)
                :durability     (normalize-durability :queue (:durability opts)))
         _ (when-not (#{:dead :discard} (:on-exhaustion opts))
             (truss/ex-info! "[Carmine] Unexpected exhaustion policy"
               {:eid :carmine.mq/invalid-option, :option :on-exhaustion
                :value (:on-exhaustion opts)}))
         _ (when-not (#{:none :full} (:retry-jitter opts))
             (truss/ex-info! "[Carmine] Unexpected retry jitter policy"
               {:eid :carmine.mq/invalid-option, :option :retry-jitter
                :value (:retry-jitter opts)}))
         _ (when-not (#{:reject :coalesce} (:on-duplicate opts))
             (truss/ex-info! "[Carmine] Unexpected duplicate policy"
               {:eid :carmine.mq/invalid-option, :option :on-duplicate
                :value (:on-duplicate opts)}))
         _ (when-not (#{:none :required} (:revision-mode opts))
             (truss/ex-info! "[Carmine] Unexpected revision mode"
               {:eid :carmine.mq/invalid-option, :option :revision-mode
                :value (:revision-mode opts)}))
         _ (when (and (= (:revision-mode opts) :required)
                   (= (:on-duplicate opts) :reject))
             (truss/ex-info!
               "[Carmine] Required revisions need duplicate coalescing"
               {:eid :carmine.mq/incompatible-options
                :problem :revision-requires-coalescing
                :revision-mode :required, :on-duplicate :reject
                :required-on-duplicate :coalesce}))
         _ (when (> (:retry-base-ms opts) (:retry-max-ms opts))
             (truss/ex-info! "[Carmine] Retry base cannot exceed retry maximum"
               {:eid :carmine.mq/invalid-option, :opts opts}))
         qname (qname-str qname)
         queue (new-queue (checked-mgr manager) qname opts (qkeys qname))]
     (assert-durability-topology! queue (:durability opts))
     (let [queue (ensure! queue)]
       (assert-durability-version! queue (:durability opts))
       queue))))

(defn- assert-queue [q] (truss/have queue? q))
(defn- priority-code [priority]
  (or (get priority->code priority)
    (truss/ex-info! "[Carmine] Unexpected priority"
      {:eid :carmine.mq/invalid-priority, :priority priority
       :expected (set (keys priority->code))})))

(defn- msg-enqueue* [queue msg opts frozen?]
   (let [queue (assert-queue queue)
         opts (validate-opts! :msg-enqueue opts msg-enqueue-option-keys)
         mid (mid-str (if (nil? (:mid opts)) (enc/uuid-str) (:mid opts)))
         priority-set? (contains? opts :priority)
         delay-set? (contains? opts :delay-ms)
         lease-set? (contains? opts :lease-ms)
         max-attempts-set? (contains? opts :max-attempts)
         on-exhaustion-set? (contains? opts :on-exhaustion)
         priority (nil-default (:priority opts) :normal)
         max-attempts (bounded-positive-long :max-attempts
                        (nil-default (:max-attempts opts)
                          (get (queue-opts queue) :max-attempts))
                        1000000)
         delay-ms (bounded-nonnegative-long :delay-ms
                    (nil-default (:delay-ms opts) 0) max-duration-ms)
         ;; nil means the queue default, i.e. no per-message override.
         lease-ms (when (some? (:lease-ms opts))
                    (bounded-positive-long :lease-ms (:lease-ms opts)
                      max-duration-ms))
         on-exhaustion (nil-default (:on-exhaustion opts)
                         (get (queue-opts queue) :on-exhaustion))
         _ (when-not (#{:dead :discard} on-exhaustion)
             (truss/ex-info! "[Carmine] Unexpected exhaustion policy"
               {:eid :carmine.mq/invalid-option, :option :on-exhaustion
                :value on-exhaustion}))
         on-duplicate (nil-default (:on-duplicate opts)
                        (get (queue-opts queue) :on-duplicate))
         _ (when-not (#{:reject :coalesce} on-duplicate)
             (truss/ex-info! "[Carmine] Unexpected duplicate policy"
               {:eid :carmine.mq/invalid-option, :option :on-duplicate
                :value on-duplicate}))
         revision-mode (get (queue-opts queue) :revision-mode)
         _ (when (and (= revision-mode :required) (= on-duplicate :reject))
             (truss/ex-info!
               "[Carmine] Required revisions need duplicate coalescing"
               {:eid :carmine.mq/incompatible-options, :mid mid
                :problem :revision-requires-coalescing
                :revision-mode :required, :on-duplicate :reject
                :required-on-duplicate :coalesce}))
         revision
         (if (= revision-mode :required)
           (if (contains? opts :revision)
             (nonnegative-long :revision (:revision opts))
             (truss/ex-info! "[Carmine] Queue requires message revisions"
               {:eid :carmine.mq/revision-required, :mid mid}))
           (do
             (when (contains? opts :revision)
               (truss/ex-info! "[Carmine] Queue does not accept message revisions"
                 {:eid :carmine.mq/revision-not-enabled, :mid mid}))
             nil))
         durability (if (contains? opts :durability)
                      (normalize-durability :msg-enqueue (:durability opts))
                      (get (queue-opts queue) :durability))
         _ (assert-durability-topology! queue durability)
         _ (assert-durability-version! queue durability)
         [[action now available-at prior-dead] durability-result]
         (run-lua-durable queue lua-enqueue
           (lua-keys queue
             [:config :payloads :meta :successor-payloads :successor-meta
              :ready-high :ready-normal :ready-low :scheduled :leased
              :lease-tokens :dead :dead-payloads :failures :seq :signal])
           {:mid mid, :payload (if frozen? msg (car/freeze nil msg))
            :priority (priority-code priority)
            :max-attempts max-attempts, :delay-ms delay-ms
            :lease-ms (or lease-ms "")
            :on-exhaustion (name on-exhaustion), :on-duplicate (name on-duplicate)
            :revision (or revision "")
            :priority-set (if priority-set? "1" "0")
            :delay-set (if delay-set? "1" "0")
            :lease-set (if lease-set? "1" "0")
            :max-attempts-set (if max-attempts-set? "1" "0")
            :on-exhaustion-set (if on-exhaustion-set? "1" "0")}
           durability #(contains? #{"added" "coalesced" "coalesced-successor"} (first %)))
         result
         (case action
           "added"    {:success? true, :action :added, :mid mid
                        :enqueued-at-ms (reply-long now), :available-at-ms (reply-long available-at)
                        :prior-dead? (= prior-dead "1")}
           "coalesced" {:success? true, :action :coalesced, :mid mid
                         :enqueued-at-ms (reply-long now), :available-at-ms (reply-long available-at)
                         :prior-dead? (= prior-dead "1")}
           "coalesced-successor"
           {:success? true, :action :coalesced-successor, :mid mid
            :enqueued-at-ms (reply-long now), :available-at-ms (reply-long available-at)
            :prior-dead? (= prior-dead "1")}
           "existing" {:success? true, :action :existing, :mid mid}
           "stale-revision" {:success? true, :action :stale-revision, :mid mid}
           "revision-conflict" {:success? false, :error :revision-conflict, :mid mid}
           "conflict" {:success? false, :error :mid-conflict, :mid mid}
           "error"    {:success? false, :error (keyword now), :mid mid}
           {:success? false, :error :unexpected-reply, :mid mid, :reply [action now available-at]})]
     (cond-> result durability-result (assoc :durability durability-result))))

(defn msg-enqueue!
  "Enqueues `msg` and returns a result map.

  Options:

  - `:mid` (a generated UUID by default).
  - `:priority` (`:high`, `:normal`, or `:low`) and `:delay-ms`.
  - Per-message `:lease-ms`, `:max-attempts`, and `:on-exhaustion` (`:dead` or
    `:discard`).
  - `:on-duplicate` (`:reject` or `:coalesce`) to override the queue policy.
  - `:revision` when the queue requires revisions.
  - `:durability` to override the local producer policy. An explicit nil
    disables it; omission uses the queue policy.

  Results:

  - A successful write has action `:added`, `:coalesced`, or
    `:coalesced-successor`. It includes `:mid`, `:enqueued-at-ms`,
    `:available-at-ms`, and `:prior-dead?`. It includes `:durability` when a
    barrier ran.
  - A successful no-write result has action `:existing` or `:stale-revision`.
  - A rejected no-write result has `:success? false` and error `:mid-conflict`
    or `:revision-conflict`.
  - A stored-state rejection has `:success? false` and error `:uninitialized`,
    `:invalid-payload`, `:corrupt-dead`, `:corrupt-active`, `:corrupt-meta`,
    `:corrupt-successor`, `:corrupt-index`, `:corrupt-seq`, or
    `:seq-exhausted`. An unknown reply has error `:unexpected-reply` and
    includes `:reply`.

  Every result includes `:mid`.

  Per-message `:lease-ms` controls claims and extensions for this generation
  without changing an active lease. Each coalescing write fully describes its
  message: omitting `:lease-ms` uses the queue default and clears an earlier
  override. A no-write `:existing` result preserves the earlier override. A dead
  letter retains the override, and [[dead-redrive!]] restores it. Before a
  producer uses this option, update all processes to the same or a newer Carmine
  version: older scripts ignore the override and drop it during successor
  promotion. This rule also applies to processes that open a [[queue-move!]]
  destination.

  A revisioned queue needs coalescing and rejects an `:on-duplicate :reject`
  override.

  For a retry-safe enqueue, reuse an explicit `:mid`, because a generated UUID
  changes on each call. With `:on-duplicate :reject`, the same active MID and
  serialized payload returns `:action :existing`, while a different payload
  returns `:error :mid-conflict`.
  Message-queue serialization always uses Carmine defaults and ignores the
  [[taoensso.carmine-v4/*auto-freeze?*]] and
  [[taoensso.carmine-v4/*freeze-opts*]] bindings, making payload comparison
  deterministic and byte-exact.
  A [[taoensso.carmine-v4/bytes]] value is equivalent to its byte array.
  `:existing` keeps the original schedule, priority, and attempt options. A MID
  must be well-formed UTF-16 and may contain U+0000 except as its first
  character.

  While the attempt is zero, coalescing updates the generation in place. When
  the attempt is positive, it keeps only the newest successor. Without
  revisions, a retry then creates a successor even for the same payload, and so
  causes one extra delivery. With revisions, the same revision and payload
  returns `:action :existing`; a different payload returns
  `:error :revision-conflict`.
  Revision ordering is scoped to one active and successor lifecycle. It resets
  when that lifecycle is fully removed; it is not a permanent per-MID
  tombstone. Priority bands are strict, and each band is FIFO, so continuous
  higher-priority work can starve lower bands."
  ([queue msg] (msg-enqueue* queue msg nil false))
  ([queue msg opts] (msg-enqueue* queue msg opts false)))

(defn- msg-enqueue-frozen! [queue frozen-msg opts]
  (msg-enqueue* queue frozen-msg opts true))

(defn- parse-info-reply
  [[state attempt max-attempts timestamp successor prior-dead priority enqueued-at :as reply]]
  (case state
    "absent" nil
    "corrupt" {:status :corrupt}
    ("ready" "scheduled" "leased" "lease-expired" "dead")
    (let [status (keyword state)
          timestamp (some-> timestamp reply-long)
          base {:status status, :priority (get code->priority (reply-long priority))
                :attempt (reply-long attempt)
                :max-attempts (reply-long max-attempts)
                :enqueued-at-ms (reply-long enqueued-at)
                :successor? (= successor "1"), :prior-dead? (= prior-dead "1")}]
      (if timestamp
        (assoc base
          (case status
            :scheduled :available-at-ms
            (:leased :lease-expired) :lease-expiry-ms
            :dead :failed-at-ms)
          timestamp)
        base))
    {:status :unknown, :reply reply}))

(defn msg-info
  "Returns indexed queue state for message `mid`, or nil when absent.

  The result includes status, attempts, maximum attempts, the relevant
  timestamp, and whether successor or retained-dead roles coexist. A partial or
  malformed active, successor, or retained-dead role returns
  `{:status :corrupt}`. This includes an active index without its hashes, an
  invalid durable payload envelope, multiple active indexes, or an invalid
  lease-token relationship."
  [queue mid]
  (let [queue (assert-queue queue), mid (mid-str mid)]
    (parse-info-reply
      (run-lua queue lua-status
        (lua-keys queue
          [:payloads :meta :successor-payloads :successor-meta
           :ready-high :ready-normal :ready-low :scheduled :leased
           :lease-tokens :dead :dead-payloads :failures])
        {:mid mid}))))

(defn msg-status
  "Returns the status keyword for message `mid`, or nil when absent."
  [queue mid]
  (:status (msg-info queue mid)))

(defn queue-status
  "Returns exact index counts and payload-free head-of-line time gauges.

  Due scheduled and expired leased messages are reported separately until
  bounded claim maintenance moves them; future scheduled and leased entries
  remain in separate counts. `:heads` gives the earliest scheduled, leased, and
  dead times, plus the enqueue time at the head of each ready priority band.
  `:lags-ms` contains `:ready-age` for each priority band,
  `:scheduled-overdue`, `:next-scheduled-in`, `:lease-expired`,
  `:next-lease-expiry-in`, and `:dead-age`. These are non-negative ages or times
  until due, relative to Redis `:server-time-ms`. An absent or damaged head
  reports nil."
  [queue]
  (let [queue (assert-queue queue)
        [nready noverdue nsched nleased nexpired ndead nsuccessors ntotal now
         ready-high-at ready-normal-at ready-low-at
         scheduled-at leased-at dead-at
         scheduled-overdue-at next-scheduled-at
         lease-expired-at next-lease-expiry-at]
        (run-lua queue lua-queue-status
          (lua-keys queue
            [:ready-high :ready-normal :ready-low :scheduled :leased :dead
             :successor-payloads :payloads :meta]) {})
        now (reply-long now)
        elapsed #(when-let [t (reply-?long %)] (max 0 (- now t)))
        remaining #(when-let [t (reply-?long %)] (max 0 (- t now)))
        ready-high-at   (reply-?long ready-high-at)
        ready-normal-at (reply-?long ready-normal-at)
        ready-low-at    (reply-?long ready-low-at)
        scheduled-at    (reply-?long scheduled-at)
        leased-at       (reply-?long leased-at)
        dead-at         (reply-?long dead-at)
        scheduled-overdue-at (reply-?long scheduled-overdue-at)
        next-scheduled-at    (reply-?long next-scheduled-at)
        lease-expired-at     (reply-?long lease-expired-at)
        next-lease-expiry-at (reply-?long next-lease-expiry-at)]
    {:counts
     {:ready (reply-long nready), :overdue (reply-long noverdue)
      :scheduled (reply-long nsched), :leased (reply-long nleased)
      :lease-expired (reply-long nexpired), :dead (reply-long ndead)
      :successors (reply-long nsuccessors), :active (reply-long ntotal)}
     :heads
     {:ready-enqueued-at-ms
      {:high ready-high-at, :normal ready-normal-at, :low ready-low-at}
      :scheduled-available-at-ms scheduled-at
      :leased-expiry-at-ms leased-at
      :dead-failed-at-ms dead-at}
     :lags-ms
     {:ready-age
      {:high (elapsed ready-high-at), :normal (elapsed ready-normal-at)
       :low (elapsed ready-low-at)}
      :scheduled-overdue (elapsed scheduled-overdue-at)
      :next-scheduled-in (remaining next-scheduled-at)
      :lease-expired (elapsed lease-expired-at)
      :next-lease-expiry-in (remaining next-lease-expiry-at)
      :dead-age (elapsed dead-at)}
     :server-time-ms now}))

(def ^:private active-page-statuses
  #{:ready :scheduled :overdue :leased :lease-expired})
(def ^:private active-page-limit-max 250)
(def ^:private active-page-cursor-version "2")
(def ^:private active-page-legacy-cursor-version "1")
;; A max-length UTF-8 MID can occupy 2048 Base64 characters.
(def ^:private active-page-cursor-max-chars 4096)
(def ^:private active-page-score-max-chars 64)
(def ^:private ^Base64$Encoder active-page-cursor-encoder
  (.withoutPadding (Base64/getUrlEncoder)))
(def ^:private ^Base64$Decoder active-page-cursor-decoder
  (Base64/getUrlDecoder))

(defn- active-page-cursor-tag [queue]
  (subs (car/script-hash (queue-name queue)) 0 16))

(defn- encode-active-page-field [^String value]
  (.encodeToString active-page-cursor-encoder
    (.getBytes value StandardCharsets/UTF_8)))

(defn- decode-active-page-field [^String encoded]
  (let [^bytes decoded (.decode active-page-cursor-decoder encoded)
        value (String. decoded StandardCharsets/UTF_8)]
    ;; Reject non-canonical Base64 and malformed UTF-8 replacement. This keeps
    ;; every accepted cursor bounded and gives it one opaque representation.
    (when-not (= encoded (encode-active-page-field value))
      (throw (IllegalArgumentException. "Non-canonical cursor field")))
    value))

(defn- canonical-active-page-score [score]
  (when-not (and (string? score)
              (<= 1 (count score) active-page-score-max-chars))
    (throw (IllegalArgumentException. "Expected a bounded Redis score")))
  (let [value
        (case score
          ("inf" "+inf") Double/POSITIVE_INFINITY
          "-inf" Double/NEGATIVE_INFINITY
          (Double/parseDouble score))]
    (when (Double/isNaN value)
      (throw (IllegalArgumentException. "NaN is not a Redis sorted-set score")))
    (cond
      (= value Double/POSITIVE_INFINITY) "inf"
      (= value Double/NEGATIVE_INFINITY) "-inf"
      :else (Double/toString value))))

(defn- active-page-cursor
  [queue status group score same-score-count mid]
  (let [encoded-score (encode-active-page-field
                        (canonical-active-page-score score))
        encoded-mid (encode-active-page-field mid)]
    (str active-page-cursor-version "." (active-page-cursor-tag queue) "."
      (name status) "." group "." encoded-score "." same-score-count "." encoded-mid)))

(defn- invalid-active-page-cursor! [queue status cursor cause]
  (truss/ex-info!
    (if (= status :dead)
      "[Carmine] Invalid dead-letter page cursor"
      "[Carmine] Invalid active-message page cursor")
    {:eid :carmine.mq/invalid-cursor, :qname (queue-name queue)
     :status status, :cursor cursor}
    cause))

(defn- parse-active-page-cursor [queue status cursor]
  (if (nil? cursor)
    {:group 0, :score nil, :same-score-count 0, :mid nil}
    (try
      (when-not (and (string? cursor)
                  (<= (count cursor) active-page-cursor-max-chars))
        (throw (IllegalArgumentException. "Expected a bounded string cursor")))
      (let [[version tag cursor-status group encoded-score
             same-score-count encoded-mid :as parts]
            (str/split cursor #"\." -1)
            _ (when-not (= (count parts) 7)
                (throw (IllegalArgumentException. "Unexpected cursor field count")))
            group (Long/parseLong group)
            score
            (case version
              "1" (let [legacy-score (Long/parseLong encoded-score)]
                    (when (neg? legacy-score)
                      (throw (IllegalArgumentException.
                               "Legacy cursor score must be non-negative")))
                    (str legacy-score))
              "2" (canonical-active-page-score
                    (decode-active-page-field encoded-score))
              (throw (IllegalArgumentException. "Unexpected cursor version")))
            same-score-count (Long/parseLong same-score-count)
            mid (decode-active-page-field encoded-mid)
            max-group (if (= status :ready) 2 0)]
        (when-not (and (or (= version active-page-cursor-version)
                          (= version active-page-legacy-cursor-version))
                    (= tag (active-page-cursor-tag queue))
                    (= cursor-status (name status))
                    (<= 0 group max-group)
                    (<= 1 same-score-count max-lua-int)
                    (= mid (mid-str mid)))
          (throw (IllegalArgumentException. "Cursor fields did not match this page")))
        {:group group, :score score, :same-score-count same-score-count, :mid mid})
      (catch Exception t
        (invalid-active-page-cursor! queue status cursor t)))))

(defn- parse-active-page-item
  [include-related?
   [mid state indexed-status timestamp priority attempt max-attempts enqueued-at
    successor prior-dead overdue]]
  (let [state (keyword state)
        indexed-status (keyword indexed-status)
        timestamp (some-> timestamp reply-long)
        timestamp-entry
        (case indexed-status
          :scheduled [:available-at-ms timestamp]
          :leased    [:lease-expiry-ms timestamp]
          nil)]
    (if (#{:orphan :corrupt} state)
      (cond-> {:mid mid, :status state, :indexed-status indexed-status}
        timestamp-entry (assoc (first timestamp-entry) (second timestamp-entry)))
      (cond->
        {:mid mid, :status state
         :priority (get code->priority (reply-long priority))
         :attempt (reply-long attempt), :max-attempts (reply-long max-attempts)
         :enqueued-at-ms (reply-long enqueued-at)}
        timestamp-entry (assoc (first timestamp-entry) (second timestamp-entry))
        (= indexed-status :scheduled) (assoc :overdue? (= overdue "1"))
        include-related? (assoc :successor? (= successor "1")
                           :prior-dead? (= prior-dead "1"))))))

(defn msg-active-page
  "Returns one bounded, payload-free page of indexed active messages.

  Options:

  - `:status` is `:ready` (default), `:scheduled`, `:overdue`, `:leased`, or
    `:lease-expired`. An `:overdue` item has status `:scheduled` and
    `:overdue? true`.
  - `:limit` defaults to 100 and has a maximum of 250.
  - `:cursor` is nil for the first page. Otherwise, use the opaque cursor from
    the previous page for the same queue and status.
  - `:include-related?` adds `:successor?` and `:prior-dead?` at extra cost.

  Each page is an atomic Redis snapshot. Ready items use priority and FIFO
  order. Scheduled and leased items use deadline order. Multiple calls do not
  form one snapshot. Concurrent changes may cause repeated or omitted items,
  especially when scores are equal. The result identifies orphaned and corrupt
  indexes. Cross-index duplicates and missing or extraneous lease tokens are
  corrupt. The page checks that a payload exists, but does not validate its
  envelope; use [[msg-info]] for that check. A nil result cursor means that no
  page remains."
  ([queue] (msg-active-page queue nil))
  ([queue opts]
   (let [queue (assert-queue queue)
         opts (validate-opts! :msg-active-page opts msg-active-page-option-keys)
         status (nil-default (:status opts) :ready)
         _ (when-not (active-page-statuses status)
             (truss/ex-info! "[Carmine] Unexpected active-message page status"
               {:eid :carmine.mq/invalid-option, :option :status
                :value status, :expected active-page-statuses}))
         limit (bounded-positive-long :limit (nil-default (:limit opts) 100)
                 active-page-limit-max)
         include-related? (if (contains? opts :include-related?)
                            (:include-related? opts)
                            false)
         _ (when-not (boolean? include-related?)
             (truss/ex-info! "[Carmine] Expected a boolean page option"
               {:eid :carmine.mq/invalid-option, :option :include-related?
                :value include-related?}))
         {:keys [group score same-score-count mid]}
         (parse-active-page-cursor queue status (:cursor opts))
         [now more next-group next-score next-same-score-count next-mid items]
         (run-lua queue lua-active-page
           (lua-keys queue
             [:payloads :meta :successor-payloads :successor-meta
              :ready-high :ready-normal :ready-low :scheduled :leased
              :lease-tokens :dead :dead-payloads :failures])
           {:status (name status), :limit limit
            :cursor-group group, :cursor-score (or score "")
            :cursor-same-score-count same-score-count, :cursor-mid (or mid "")
            :include-related (if include-related? "1" "0")})
         cursor
         (when (= more "1")
           (active-page-cursor queue status
             (reply-long next-group) next-score
             (reply-long next-same-score-count) next-mid))]
     {:items (mapv #(parse-active-page-item include-related? %) items)
      :cursor cursor, :server-time-ms (reply-long now)})))

(defn msg-remove!
  "Atomically removes an active or dead message. This fences a running handler,
  which cannot settle the removed message. Removal also discards any stored
  successor instead of promoting it. Returns
  `{:success? boolean, :action :removed|:absent}`. The result includes a
  durability barrier after an effective removal when configured."
  [queue mid]
  (let [queue (assert-queue queue), mid (mid-str mid)
        durability (get (queue-opts queue) :durability)
        [[action] durability-result]
        (run-lua-durable queue lua-remove
          (lua-keys queue
            [:payloads :meta :successor-payloads :successor-meta
             :ready-high :ready-normal :ready-low :scheduled :leased
             :lease-tokens :dead :dead-payloads :failures])
          {:mid mid} durability #(= (first %) "removed"))
        action (keyword action)]
    (cond-> {:success? (= action :removed), :action action}
      durability-result (assoc :durability durability-result))))

(defn dead-redrive!
  "Moves a dead letter back to ready state and resets its attempts.

  Returns `{:success? boolean, :action keyword}`. The action is
  `:redriven`, `:not-dead`, `:active-exists`, `:changed`, `:corrupt`,
  `:corrupt-seq`, or `:seq-exhausted`. The function does not change a complete
  or partial corrupt record, and it does not create active state from it. The
  codec check and write apply to the same dead generation."
  [queue mid]
  (let [queue (assert-queue queue), mid (mid-str mid)
        durability (get (queue-opts queue) :durability)
        keys
        (lua-keys queue
          [:config :payloads :meta :successor-payloads :successor-meta
           :ready-high :ready-normal :ready-low :scheduled :leased
           :lease-tokens :dead :dead-payloads :failures :seq :signal])
        [preflight candidate expected-payload expected-packed expected-score]
        (run-lua queue lua-redrive keys
          {:mid mid, :mode "inspect", :expected-payload ""
           :expected-packed "", :expected-score ""})]
    (if-not (= preflight "candidate")
      {:success? false, :action (keyword preflight)}
      (if (mq-payload-decode-error? candidate)
        {:success? false, :action :corrupt}
        (let [[[action] durability-result]
              (run-lua-durable queue lua-redrive keys
                {:mid mid, :mode "commit"
                 :expected-payload (car/bytes expected-payload)
                 :expected-packed (car/bytes expected-packed)
                 :expected-score expected-score}
                durability #(= (first %) "redriven"))
              action (keyword action)]
          (cond-> {:success? (= action :redriven), :action action}
            durability-result (assoc :durability durability-result)))))))

(defn dead-info
  "Returns dead-letter information with the payload at `:msg`, or nil.

  Failure metadata is bounded. A partial or invalid record returns
  `{:status :corrupt ...}`. Its `:corruption` value identifies the problem."
  [queue mid]
  (let [queue (assert-queue queue), mid (mid-str mid)
        [action msg failed-at attempt reason priority max-attempts enqueued-at]
        (run-lua queue lua-dead-letter
          (lua-keys queue [:dead :failures :dead-payloads]) {:mid mid})]
    (case action
      "absent" nil
      "corrupt" {:mid mid, :status :corrupt, :corruption (keyword msg)}
      "dead"
      (if (mq-payload-decode-error? msg)
        {:mid mid, :status :corrupt, :corruption :invalid-payload-codec}
        {:mid mid, :status :dead, :msg msg, :failed-at-ms (reply-long failed-at)
         :attempt (reply-long attempt), :reason reason
         :priority (get code->priority (reply-long priority))
         :max-attempts (reply-long max-attempts), :enqueued-at-ms (reply-long enqueued-at)})
      (truss/ex-info! "[Carmine] Unexpected dead-letter reply"
        {:eid :carmine.mq/unexpected-reply, :reply action, :mid mid}))))

(defn dead-mids
  "Returns dead-letter MIDs in failure-time order.

  `start` and `stop` are Redis ZRANGE indexes. They default to 0 and -1. The
  default arity returns all MIDs in one reply. Use [[dead-page]] for large
  sets."
  ([queue] (dead-mids queue 0 -1))
  ([queue start stop]
   (let [queue (assert-queue queue)
         start (signed-long :start start)
         stop  (signed-long :stop  stop)]
     (with-mq-protocol-bindings
       (car/wcar (queue-manager queue)
         (car/zrange (get (queue-keys queue) :dead) start stop))))))

(defn dead-page
  "Returns a bounded, payload-free page of dead letters by failure time.

  Options:

  - `:limit` defaults to 100 and has a maximum of 250.
  - `:cursor` is nil for the first page. Otherwise, use the cursor from the
    previous page for the same queue.

  Each page is an atomic Redis snapshot. An item is
  `{:mid ..., :failed-at-ms ...}`. An invalid index score gives
  `{:mid ..., :status :corrupt}`. Multiple calls do not form one snapshot.
  Concurrent changes may cause repeated or omitted items, especially when
  scores are equal. Use [[dead-info]] for details. A nil result cursor means
  that no page remains."
  ([queue] (dead-page queue nil))
  ([queue opts]
   (let [queue (assert-queue queue)
         opts (validate-opts! :dead-page opts dead-page-option-keys)
         limit (bounded-positive-long :limit (nil-default (:limit opts) 100)
                 active-page-limit-max)
         {:keys [score same-score-count mid]}
         (parse-active-page-cursor queue :dead (:cursor opts))
         [now more next-score next-same-score-count next-mid items]
         (run-lua queue lua-dead-page
           (lua-keys queue [:dead])
           {:limit limit
            :cursor-score (or score "")
            :cursor-same-score-count same-score-count
            :cursor-mid (or mid "")})
         cursor
         (when (= more "1")
           (active-page-cursor queue :dead 0 next-score
             (reply-long next-same-score-count) next-mid))]
     {:items
      (mapv
        (fn [[mid score]]
          (if score
            {:mid mid, :failed-at-ms (reply-long score)}
            {:mid mid, :status :corrupt}))
        items)
      :cursor cursor, :server-time-ms (reply-long now)})))

(defn dead-purge!
  "Permanently removes a bounded batch of dead letters.

  `:older-than-ms` is a required age relative to Redis server time. Use 0 to
  include all dead letters. `:limit` defaults to 1000 and has a maximum of
  10000. Returns the removal count and whether another matching batch remains."
  [queue opts]
  (let [queue (assert-queue queue)
        opts (validate-opts! :dead-purge opts dead-purge-option-keys)
        _ (when-not (contains? opts :older-than-ms)
            (truss/ex-info! "[Carmine] Dead-letter purge requires an age"
              {:eid :carmine.mq/purge-age-required}))
        older-than-ms (bounded-nonnegative-long :older-than-ms (:older-than-ms opts) max-duration-ms)
        limit (bounded-positive-long :limit
                (nil-default (:limit opts) 1000) 10000)
        durability (get (queue-opts queue) :durability)
        [[removed more now] durability-result]
        (run-lua-durable queue lua-purge-dead
          (lua-keys queue [:dead :failures :dead-payloads])
          {:older-than-ms older-than-ms, :limit limit}
          durability #(pos? (reply-long (first %))))]
    (cond-> {:removed (reply-long removed), :more? (= more "1")
             :server-time-ms (reply-long now)}
      durability-result (assoc :durability durability-result))))

(defn queue-clear!!
  "Permanently removes active work and dead letters. Retains durable queue
  configuration. Returns `{:success? true, :action :cleared}`. The result
  includes a durability barrier after an effective deletion when configured."
  [queue]
  (let [queue (assert-queue queue)
        durability (get (queue-opts queue) :durability)
        [[action removed] durability-result]
        (run-lua-durable queue lua-clear
          (lua-keys queue (remove #{:config} key-suffixes)) {}
          durability #(pos? (reply-long (second %))))]
    (cond-> {:success? (= action "cleared"), :action (keyword action)}
      durability-result (assoc :durability durability-result))))

(defn queue-move!
  "Renames or moves an offline queue to `target-qname` using the same manager.

  Preserves all Redis state, including schedules, leases, fencing tokens,
  successors, and dead letters. It copies each key with DUMP/RESTORE. After all
  restores succeed, it atomically unlinks the source keys. The operation works
  across Redis Cluster slots, but the full copy-and-delete operation is not
  atomic.

  The queue must be offline. Stop and await all source workers. Pause source
  producers and administration calls. Keep the target name unused for the
  complete call. These conditions cannot be verified across processes. An
  unexpired lease does not prove that a worker is running, and the move keeps
  it. A concurrent source write can be lost or make the target inconsistent.
  An active handler can cause duplicate delivery. Concurrent target activity
  can conflict with the copy or cleanup. Use the returned `:queue` and do not
  use the source handle again.

  A copy failure leaves the source intact and makes a best-effort removal of all
  destination keys. A source-delete command failure is definite; a transport
  failure reports `:eid :carmine.mq/move-delete-ambiguous`. A complete destination is
  retained in either source-delete case for inspection.

  DUMP/RESTORE may transfer a complete Redis hash through the client. Large
  queues may need much client memory and bandwidth. Schedule and lease times
  are absolute Redis times. Synchronize Redis Cluster node clocks."
  [source-queue target-qname]
  (let [source-queue (assert-queue source-queue)
        source-qname (queue-name source-queue)
        target-qname (qname-str target-qname)
        _ (when (= source-qname target-qname)
            (truss/ex-info! "[Carmine] Move target must have a different name"
              {:eid :carmine.mq/move-invalid-target
               :source-queue source-qname, :target-queue target-qname}))
        manager (queue-manager source-queue)
        source-key-map (queue-keys source-queue)
        target-key-map (qkeys target-qname)
        ;; Restore config last so a partial destination never looks initialized.
        suffixes (conj (vec (remove #{:config} key-suffixes)) :config)
        source-keys (mapv source-key-map suffixes)
        target-keys (mapv target-key-map suffixes)
        target-key-count
        (car/wcar manager (apply car/exists target-keys))]
    (when (pos? (long target-key-count))
      (truss/ex-info! "[Carmine] Move target already contains queue keys"
        {:eid :carmine.mq/move-target-not-empty
         :source-queue source-qname, :target-queue target-qname
         :existing-key-count (long target-key-count)}))
    (when (zero? (long (car/wcar manager (car/exists (:config source-key-map)))))
      (truss/ex-info! "[Carmine] Move source queue is uninitialized"
        {:eid :carmine.mq/move-source-missing
         :source-queue source-qname, :target-queue target-qname}))
    (let [restored-keys_ (atom [])
          target-queue_ (volatile! nil)
          copy-error
          (try
            (doseq [[source-key target-key] (map vector source-keys target-keys)]
              (when-let [dump (car/wcar manager (car/as-bytes (car/dump source-key)))]
                (car/wcar manager (car/restore target-key 0 (car/bytes dump)))
                (swap! restored-keys_ conj target-key)))
            ;; Validate the copied config and target-slot Redis version before
            ;; detaching the source. This also refreshes internal version
            ;; metadata for a mixed-version Cluster during a rolling upgrade.
            ;; Reuse the already-normalized queue options: public `queue`
            ;; expects the user-facing durability shape and would normalize
            ;; these twice.
            (vreset! target-queue_
              (ensure!
                (new-queue manager target-qname
                  (queue-opts source-queue) target-key-map)))
            nil
            (catch Throwable t t))]
      (when copy-error
        (let [cleanup-error
              ;; Also remove a RESTORE whose success reply may have been lost
              ;; before its key could be recorded locally. The target must be
              ;; reserved for this operation, so every target queue key is ours.
              (try
                (car/wcar manager (apply car/unlink target-keys))
                nil
                (catch Throwable cleanup-t cleanup-t))]
          (when (or (instance? InterruptedException copy-error)
                  (instance? InterruptedException cleanup-error))
            (.interrupt (Thread/currentThread)))
          (cond
            ;; Best-effort cleanup should not translate VM/linkage failures
            ;; into ordinary move exceptions. Preserve the fatal throwable,
            ;; attaching any cleanup failure for diagnosis.
            (instance? Error copy-error)
            (do
              (when (and cleanup-error (not (identical? cleanup-error copy-error)))
                (.addSuppressed ^Throwable copy-error cleanup-error))
              (throw copy-error))

            (instance? Error cleanup-error)
            (do
              (when-not (identical? cleanup-error copy-error)
                (.addSuppressed ^Throwable cleanup-error copy-error))
              (throw cleanup-error))

            cleanup-error
            (let [move-error
                  (truss/ex-info "[Carmine] Queue move copy and cleanup failed"
                    {:eid :carmine.mq/move-copy-cleanup-failed
                     :ambiguous? true
                     :source-queue source-qname, :target-queue target-qname
                     :restored-key-count (count @restored-keys_)
                     :cleanup-error-class (.getName (class cleanup-error))}
                    copy-error)]
              (when-not (identical? move-error cleanup-error)
                (.addSuppressed ^Throwable move-error cleanup-error))
              (throw move-error))

            (instance? InterruptedException copy-error)
            (throw copy-error)

            :else
            (truss/ex-info! "[Carmine] Queue move copy failed"
              {:eid :carmine.mq/move-copy-failed
               :ambiguous? false
               :source-queue source-qname, :target-queue target-qname
               :restored-key-count (count @restored-keys_)}
              copy-error))))
      (let [deleted
            (try
              (long (car/wcar manager (apply car/unlink source-keys)))
              (catch Exception t
                (when (instance? InterruptedException t)
                  (.interrupt (Thread/currentThread)))
                (let [ambiguous? (not (instance? ReusableConnError t))]
                  (truss/ex-info!
                    (if ambiguous?
                      "[Carmine] Queue move source deletion is ambiguous"
                      "[Carmine] Queue move source deletion failed")
                    {:eid (if ambiguous?
                            :carmine.mq/move-delete-ambiguous
                            :carmine.mq/move-delete-failed)
                     :ambiguous? ambiguous?
                     :source-queue source-qname, :target-queue target-qname
                     :restored-key-count (count @restored-keys_)}
                    t))))
            target-queue @target-queue_]
        {:success? true, :action :moved
         :source-queue source-qname, :target-queue target-qname
         :moved-key-count (count @restored-keys_)
         :deleted-key-count deleted, :queue target-queue}))))

;;;; Durable queue configuration

(def ^:private config-field->option
  {"lease_ms"              :lease-ms
   "max_attempts"          :max-attempts
   "retry_base_ms"         :retry-base-ms
   "retry_max_ms"          :retry-max-ms
   "retry_jitter"          :retry-jitter
   "on_exhaustion_default" :on-exhaustion})

(defn- parse-config-value [option value]
  (when-not (or (nil? value) (false? value) (= value ""))
    (case option
      (:lease-ms :max-attempts :retry-base-ms :retry-max-ms) (reply-long value)
      (:retry-jitter :on-exhaustion) (keyword value))))

(defn- durable-config-?long
  "Returns the value of a canonical non-negative durable integer string, or
  nil for anything else. Damaged spellings like `1e3` or `1000.0` must report
  nil rather than a silently coerced number."
  [x]
  (when (and (string? x) (re-matches #"0|[1-9][0-9]*" x))
    (try
      (Long/parseLong ^String x)
      (catch NumberFormatException _ nil))))

(defn queue-config
  "Returns the queue's parsed durable configuration, or nil if the durable
  configuration does not exist.

  Reads these values from Redis: `:lease-ms`, `:max-attempts`,
  `:retry-base-ms`, `:retry-max-ms`, `:retry-jitter`, `:on-exhaustion`,
  `:on-duplicate`, `:revision-mode`, and `:schema-version`. Missing or invalid
  fields are nil. This function is read-only; it does not validate or change
  the handle options."
  [queue]
  (let [queue (assert-queue queue)
        [schema lease-ms max-attempts retry-base-ms retry-max-ms
         retry-jitter on-exhaustion on-duplicate revision-mode]
        (with-mq-protocol-bindings
          (car/wcar (queue-manager queue)
            (car/hmget (get (queue-keys queue) :config)
              "schema" "lease_ms" "max_attempts" "retry_base_ms"
              "retry_max_ms" "retry_jitter" "on_exhaustion_default"
              "on_duplicate_default" "revision_mode")))]
    (when (some? schema)
      {:schema-version (durable-config-?long schema)
       :lease-ms       (durable-config-?long lease-ms)
       :max-attempts   (durable-config-?long max-attempts)
       :retry-base-ms  (durable-config-?long retry-base-ms)
       :retry-max-ms   (durable-config-?long retry-max-ms)
       :retry-jitter   (some-> retry-jitter   keyword)
       :on-exhaustion  (some-> on-exhaustion  keyword)
       :on-duplicate   (some-> on-duplicate   keyword)
       :revision-mode  (some-> revision-mode  keyword)})))

(defn- parse-config-changes [triples]
  (reduce
    (fn [changes [field old new]]
      (let [option (get config-field->option field)]
        (when-not option
          (truss/ex-info! "[Carmine] Unexpected config-update change field"
            {:eid :carmine.mq/unexpected-reply, :field field}))
        (assoc changes option
          {:old (parse-config-value option old)
           :new (parse-config-value option new)})))
    {} (partition 3 triples)))

(defn queue-config-update!
  "Atomically updates durable queue configuration. Returns a result map with a
  new `:queue` handle that uses the updated configuration.

  Updatable: `:lease-ms`, `:max-attempts`, `:retry-base-ms`, `:retry-max-ms`,
  `:retry-jitter`, and `:on-exhaustion`. At least one is required, and the
  merged retry window must have `base <= max`; the function checks this
  atomically against the current values. `:revision-mode` is not updatable
  because a change during a lifecycle has undefined ordering; drain the queue
  or use [[queue-move!]] to move to a new queue. `:on-duplicate` is not
  updatable because active producer handles could use different policies.

  Lease duration (for messages without a per-enqueue `:lease-ms` override),
  retry backoff, and jitter changes apply in every process to future claims,
  extensions, retries, and expired-lease maintenance. They do not change an
  active lease or scheduled retry. `:max-attempts` and `:on-exhaustion` are
  fixed when a message is enqueued. The update changes only the defaults for
  new handles and never rewrites existing messages.

  Before lowering `:lease-ms`, stop or reconfigure every worker using
  `:lease-extend-every-ms` whose interval is not shorter than the new lease.
  Existing workers retain their heartbeat interval while new claims immediately
  use the new durable lease. Without reconfiguration, a claim can expire before
  its first heartbeat; token fencing prevents stale settlement, but not
  concurrent handlers.

  Other live handles keep their enqueue defaults until reopened, and a later
  [[queue]] call must supply the updated values or fail with
  `:eid :carmine.mq/config-mismatch`. This includes process restarts: between
  this update and a deploy of matching construction options, a restarting
  producer fleet cannot open the queue. Update the queue, change the shared
  construction options, and then reopen handles. After an update, call
  [[queue-move!]] only with the returned new handle.

  Returns `{:success? true, :action :updated|:unchanged, :changed {...},
  :server-time-ms ..., :queue ...}` where `:changed` maps each effectively
  changed option to `{:old ..., :new ...}`. It is empty for `:unchanged`. An
  effective write includes the configured `:durability` barrier."
  [queue updates]
  (let [queue (assert-queue queue)
        updates (validate-opts! :queue-config-update updates
                  queue-config-update-option-keys)
        _ (when-not (some #(contains? updates %) queue-config-update-option-keys)
            (truss/ex-info! "[Carmine] Config update requires at least one updatable option"
              {:eid :carmine.mq/invalid-option, :context :queue-config-update
               :allowed-keys queue-config-update-option-keys}))
        lease-set? (contains? updates :lease-ms)
        max-attempts-set? (contains? updates :max-attempts)
        retry-base-set? (contains? updates :retry-base-ms)
        retry-max-set? (contains? updates :retry-max-ms)
        retry-jitter-set? (contains? updates :retry-jitter)
        on-exhaustion-set? (contains? updates :on-exhaustion)
        lease-ms (when lease-set?
                   (bounded-positive-long :lease-ms (:lease-ms updates)
                     max-duration-ms))
        max-attempts (when max-attempts-set?
                       (bounded-positive-long :max-attempts
                         (:max-attempts updates) 1000000))
        retry-base-ms (when retry-base-set?
                        (bounded-nonnegative-long :retry-base-ms
                          (:retry-base-ms updates) max-duration-ms))
        retry-max-ms (when retry-max-set?
                       (bounded-nonnegative-long :retry-max-ms
                         (:retry-max-ms updates) max-duration-ms))
        retry-jitter (:retry-jitter updates)
        _ (when (and retry-jitter-set? (not (#{:none :full} retry-jitter)))
            (truss/ex-info! "[Carmine] Unexpected retry jitter policy"
              {:eid :carmine.mq/invalid-option, :option :retry-jitter
               :value retry-jitter}))
        on-exhaustion (:on-exhaustion updates)
        _ (when (and on-exhaustion-set? (not (#{:dead :discard} on-exhaustion)))
            (truss/ex-info! "[Carmine] Unexpected exhaustion policy"
              {:eid :carmine.mq/invalid-option, :option :on-exhaustion
               :value on-exhaustion}))
        _ (when (and retry-base-set? retry-max-set?
                  (> (long retry-base-ms) (long retry-max-ms)))
            (truss/ex-info! "[Carmine] Retry base cannot exceed retry maximum"
              {:eid :carmine.mq/invalid-option, :updates updates}))
        durability (get (queue-opts queue) :durability)
        [[action x1 x2 x3 x4 x5 x6 x7 x8 x9 & change-triples :as reply]
         durability-result]
        (run-lua-durable queue lua-config-update
          (lua-keys queue [:config])
          {:schema (str schema-version)
           :lease-ms (or lease-ms "")
           :max-attempts (or max-attempts "")
           :retry-base-ms (or retry-base-ms "")
           :retry-max-ms (or retry-max-ms "")
           :retry-jitter (if retry-jitter (name retry-jitter) "")
           :on-exhaustion (if on-exhaustion (name on-exhaustion) "")
           :lease-set (if lease-set? "1" "0")
           :max-attempts-set (if max-attempts-set? "1" "0")
           :retry-base-set (if retry-base-set? "1" "0")
           :retry-max-set (if retry-max-set? "1" "0")
           :retry-jitter-set (if retry-jitter-set? "1" "0")
           :on-exhaustion-set (if on-exhaustion-set? "1" "0")}
          durability #(= (first %) "updated"))]
    (case action
      ("updated" "unchanged")
      (let [durable-config
            {:lease-ms      (reply-long x2)
             :max-attempts  (reply-long x3)
             :retry-base-ms (reply-long x4)
             :retry-max-ms  (reply-long x5)
             :retry-jitter  (some-> x6 keyword)
             :on-exhaustion (some-> x7 keyword)
             :on-duplicate  (some-> x8 keyword)
             :revision-mode (some-> x9 keyword)}
            ;; Fields absent from durable config (older-preview queues) keep
            ;; this handle's constructed values, per ensure's HSETNX contract.
            fresh-opts
            (merge (queue-opts queue)
              (into {} (filter (comp some? val)) durable-config))
            fresh-queue
            (new-queue (queue-manager queue) (queue-name queue)
              fresh-opts (queue-keys queue))]
        (cond->
          {:success? true, :action (keyword action)
           :changed (parse-config-changes change-triples)
           :server-time-ms (reply-long x1)
           :queue fresh-queue}
          durability-result (assoc :durability durability-result)))

      "missing-config"
      (truss/ex-info!
        "[Carmine] Queue config is missing"
        {:eid :carmine.mq/config-missing, :qname (queue-name queue)})

      "corrupt-config"
      (truss/ex-info! "[Carmine] Queue config is corrupt"
        {:eid :carmine.mq/corrupt-config, :qname (queue-name queue)
         :field x1})

      "retry-window-inverted"
      (truss/ex-info!
        "[Carmine] Merged retry base cannot exceed merged retry maximum"
        {:eid :carmine.mq/incompatible-options, :qname (queue-name queue)
         :problem :retry-window-inverted
         :retry-base-ms (reply-long x1), :retry-max-ms (reply-long x2)})

      (truss/ex-info! "[Carmine] Unexpected config-update reply"
        {:eid :carmine.mq/unexpected-reply, :reply reply}))))

;;;; Internal atomic worker transitions

(defn- contain-corrupt-payload! [queue mid token expected-payload]
  (let [[action detail]
        (run-lua queue lua-contain-corrupt-payload
          (lua-keys queue
            [:config :payloads :meta :successor-payloads :successor-meta
             :ready-high :ready-normal :ready-low :scheduled :leased
             :lease-tokens :dead :dead-payloads :failures :seq :signal])
          {:mid mid, :lease-token token
           :expected-payload (car/bytes expected-payload)})]
    (case action
      "contained" {:action :contained, :successor-promoted? (= detail "1")}
      "stale" {:action :stale}
      "error" (truss/ex-info! "[Carmine] Corrupt payload containment failed"
                {:eid :carmine.mq/corrupt-payload-containment-failed
                 :stage :contain-corrupt-payload
                 :error (keyword detail), :mid mid})
      (truss/ex-info! "[Carmine] Unexpected corrupt-payload containment reply"
        {:eid :carmine.mq/unexpected-reply, :reply [action detail]
         :mid mid}))))

(def ^:private claim-cleanup-reasons
  [:orphan :corrupt-meta :corrupt-payload :corrupt-index])

(defn- parse-claim-cleanup-counts [reply]
  (let [counts (peek reply)]
    (when-not (and (vector? counts)
                (= (count counts) (count claim-cleanup-reasons)))
      (truss/ex-info! "[Carmine] Unexpected claim cleanup tallies"
        {:eid :carmine.mq/unexpected-reply}))
    (zipmap claim-cleanup-reasons (mapv reply-long counts))))

(defn- claim!
  ([queue maintenance-batch-size]
   (claim! queue maintenance-batch-size nil))
  ([queue maintenance-batch-size observe-cleanups!]
   (let [reply
         (run-lua queue lua-claim
           (lua-keys queue
             [:config :payloads :meta :successor-payloads :successor-meta
              :ready-high :ready-normal :ready-low :scheduled :leased
              :lease-tokens :dead :dead-payloads :failures :seq :signal])
           {:lease-token (enc/uuid-str), :maintenance-limit maintenance-batch-size})
         cleanup-counts (parse-claim-cleanup-counts reply)
         _ (when observe-cleanups! (observe-cleanups! cleanup-counts))
         [action mid msg attempt token lease-expiry enqueued-at priority now] reply]
     (case action
       "handle"
       (if (mq-payload-decode-error? msg)
         (do
           ;; Lua has already leased this generation. Fence its removal with the
           ;; returned token, promote a valid successor, and never expose the
           ;; decoder's ReplyError as a user message.
           (contain-corrupt-payload! queue mid token
             (mq-decode-error-stored-payload msg))
           {:action :skip, :reason :corrupt-payload})
         {:action :handle, :mid mid, :msg msg, :attempt (reply-long attempt)
          :token token, :lease-expiry-ms (reply-long lease-expiry)
          :enqueued-at-ms (reply-long enqueued-at)
          :priority (get code->priority (reply-long priority))
          :server-time-ms (reply-long now)})
       "idle" {:action :idle, :next-at-ms (when mid (reply-long mid)), :server-time-ms (reply-long msg)}
       "skip" {:action :skip, :reason (keyword mid)}
       "error" (truss/ex-info! "[Carmine] Queue claim failed"
                 {:eid :carmine.mq/claim-failed, :stage :claim
                  :error (keyword mid)})
       (truss/ex-info! "[Carmine] Unexpected claim reply"
         {:eid :carmine.mq/unexpected-reply, :reply action})))))

(defn- extend-lease! [queue mid token]
  (let [[action expiry]
        (run-lua queue lua-extend
          (lua-keys queue [:config :leased :lease-tokens :meta])
          {:mid mid, :lease-token token})]
    (case action
      "extended" (reply-long expiry)
      "stale" nil
      "corrupt"
      (truss/ex-info! "[Carmine] Cannot extend a corrupt lease"
        {:eid :carmine.mq/corrupt-lease, :mid mid
         :corruption (keyword expiry)})
      (truss/ex-info! "[Carmine] Unexpected lease-extension reply"
        {:eid :carmine.mq/unexpected-reply, :reply [action expiry]
         :mid mid}))))

(defn- release! [queue mid token]
  (let [[action detail]
        (run-lua queue lua-release
          (lua-keys queue
            [:config :payloads :meta :successor-payloads :successor-meta
             :ready-high :ready-normal :ready-low :scheduled :leased
             :lease-tokens :dead :dead-payloads :failures :seq :signal])
          {:mid mid, :lease-token token})]
    (case action
      "released"           :released
      "released-successor" :released-successor
      "stale"              :stale
      "corrupt"            :corrupt
      "error"
      (truss/ex-info! "[Carmine] Queue release failed"
        {:eid :carmine.mq/release-failed, :error (keyword detail), :mid mid})
      (truss/ex-info! "[Carmine] Unexpected release reply"
        {:eid :carmine.mq/unexpected-reply, :reply [action detail]
         :mid mid}))))

(deftype ^:private HandlerOutcome [action delay-ms reason])
(alter-meta! #'->HandlerOutcome assoc :private true)

(defmethod print-method HandlerOutcome [outcome ^java.io.Writer w]
  (.write w (str "#<CarmineV4MQOutcome " (pr-str (.-action ^HandlerOutcome outcome)) ">")))

(defn- handler-outcome? [x] (instance? HandlerOutcome x))
(defn- handler-outcome [action delay-ms reason]
  (HandlerOutcome. action delay-ms reason))

(def ^:private outcome-ack     (handler-outcome :ack nil nil))
(def ^:private outcome-discard (handler-outcome :discard nil nil))

(defn- settle!
  ([queue mid token outcome] (settle! queue mid token outcome nil))
  ([queue mid token ^HandlerOutcome outcome durability]
   (let [intent (.-action outcome)
         delay-ms (.-delay-ms outcome)
         reason (.-reason outcome)
         action (case intent
                  :ack "ack", :retry "retry"
                  :dead "dead", :discard "discard")
         [[result timestamp promoted retry-delay] durability-result]
         (run-lua-durable queue lua-settle
           (lua-keys queue
             [:config :payloads :meta :successor-payloads :successor-meta
              :ready-high :ready-normal :ready-low :scheduled :leased
              :lease-tokens :dead :dead-payloads :failures :seq :signal])
           {:mid mid, :lease-token token, :action action
            :delay-ms (if (some? delay-ms) (bounded-nonnegative-long :delay-ms delay-ms max-duration-ms) -1)
            :reason
            (safe-internal-reason (nil-default reason "unspecified"))}
           durability #(not (contains? #{"stale" "error"} (first %))))]
     (if (= result "error")
       (truss/ex-info! "[Carmine] Queue settlement failed"
         {:eid :carmine.mq/settlement-failed, :error (keyword timestamp), :mid mid})
       ;; A retried settlement's timestamp is the retry due time (now + delay),
       ;; not the settlement time; label the two cases distinctly.
       (let [action (keyword result)]
         (cond-> {:action action, :successor-promoted? (= promoted "1")}
           (= action :retried)    (assoc :retry-at-ms (reply-long timestamp))
           (not= action :retried) (assoc :server-time-ms (some-> timestamp reply-long))
           retry-delay (assoc :retry-delay-ms (reply-long retry-delay))
           durability-result (assoc :durability durability-result)))))))

(defn- check-outcome-opts [context opts allowed]
  (when-not (map? opts)
    (truss/ex-info! "[Carmine] Handler outcome options must be a map"
      {:eid :carmine.mq/invalid-outcome-options, :context context
       :value opts}))
  (when-let [unexpected
             (seq
               (remove
                 #(or (contains? allowed %)
                    (and (keyword? %) (namespace %)))
                 (keys opts)))]
    (truss/ex-info! "[Carmine] Unexpected handler outcome options"
      {:eid :carmine.mq/invalid-outcome-options, :context context
       :unexpected-keys unexpected}))
  opts)

(defn- outcome-delay-ms [x]
  (when (some? x)
    (when-not (and (integer? x) (<= 0 x max-duration-ms))
      (truss/ex-info! "[Carmine] Outcome delay must be a non-negative integer within the duration maximum"
        {:eid :carmine.mq/invalid-outcome-options, :option :delay-ms
         :value x, :max max-duration-ms}))
    (long x)))

(defn- outcome-reason [context x]
  (when (some? x)
    (when-not (string? x)
      (truss/ex-info! "[Carmine] Outcome reason must be a string"
        {:eid :carmine.mq/invalid-outcome-options, :context context
         :option :reason, :value x}))
    (when (or (str/blank? x) (> (count x) 1024))
      (truss/ex-info! "[Carmine] Outcome reason must contain 1 to 1024 UTF-16 code units"
        {:eid :carmine.mq/invalid-outcome-options, :context context
         :option :reason, :length (count x)}))
    (when-not (well-formed-utf16? x)
      (truss/ex-info! "[Carmine] Outcome reason contains malformed UTF-16"
        {:eid :carmine.mq/invalid-outcome-options, :context context
         :option :reason, :value x
         :reason "Text contains an unpaired UTF-16 surrogate"}))
    (when (str/starts-with? x "\u0000")
      (truss/ex-info! "[Carmine] Outcome reason cannot begin with U+0000"
        {:eid :carmine.mq/invalid-outcome-options, :context context
         :option :reason, :value x
         :reason "The first U+0000 character is reserved by Carmine"}))
    x))

(defn- worker-idle-timeout-ms
  ([idle-min-ms idle-max-ms until-next]
   (worker-idle-timeout-ms idle-min-ms idle-max-ms until-next (rand)))
  ([idle-min-ms idle-max-ms until-next random-sample]
   (let [base (if (some? until-next)
                (min idle-max-ms until-next)
                idle-max-ms)
         ;; Wake at or before the known deadline. The idle minimum remains a
         ;; deliberate lower polling bound when a deadline is already closer.
         early-factor (min (Math/nextDown 1.0)
                        (+ 0.85 (* 0.15 (double random-sample))))]
     (min idle-max-ms
       (max idle-min-ms (long (* base early-factor)))))))

(defn outcome:ack
  "Returns the handler outcome for successful processing.

  This permanently removes the handled generation. External operations must be
  idempotent because delivery is at least once."
  [] outcome-ack)

(defn outcome:retry
  "Returns the handler outcome for another attempt.

  With no options, uses the queue's exponential backoff. `:delay-ms` sets a
  non-negative delay. `:reason`, which is retained if this attempt exhausts the
  message, must contain 1-1024 well-formed UTF-16 code units and may contain
  U+0000 except as its first character."
  ([] (handler-outcome :retry nil nil))
  ([{:keys [delay-ms reason] :as opts}]
   (check-outcome-opts :retry opts #{:delay-ms :reason})
   (handler-outcome :retry (outcome-delay-ms delay-ms)
     (outcome-reason :retry reason))))

(defn outcome:dead
  "Returns the handler outcome that immediately creates a dead letter.

  `:reason` defaults to `\"handler-requested\"`. It must contain 1-1024
  well-formed UTF-16 code units and may contain U+0000 except as its first
  character. This outcome ignores the automatic exhaustion policy."
  ([] (handler-outcome :dead nil "handler-requested"))
  ([{:keys [reason] :as opts}]
   (check-outcome-opts :dead opts #{:reason})
   (handler-outcome :dead nil
     (outcome-reason :dead (nil-default reason "handler-requested")))))

(defn outcome:discard
  "Returns the handler outcome that removes the handled generation without a
  dead letter. This outcome ignores the automatic exhaustion policy."
  [] outcome-discard)

;;;; Public claim/settle surface

(defn- lease-token-str [lease-token]
  (when-not (and (string? lease-token) (pos? (count ^String lease-token)))
    (truss/ex-info! "[Carmine] Expected a non-empty lease-token string"
      {:eid :carmine.mq/invalid-option, :option :lease-token
       :value lease-token}))
  lease-token)

(defn msg-claim!
  "Atomically claims at most one ready message and returns a result map. This is
  the same operation used by the recommended high-level consumer,
  [[worker-create]], and is public for manual or mixed deployments.

  The result includes an opaque `:lease-token` for this generation. Supply the
  exact token to [[msg-settle!]], [[msg-release!]], or [[msg-extend-lease!]]. A
  stale token prevents a change to a newer generation. Lease expiry makes the
  claim eligible for a token-fenced transition, but does not revoke the token.
  Because delivery is at least once, make handler operations idempotent before
  settling the message.

  Results:

  - `{:success? true, :action :claimed, ...}` with `:mid`, `:msg`, `:attempt`,
    `:priority`, `:lease-token`, `:lease-expiry-ms`, `:enqueued-at-ms`,
    and `:server-time-ms`.
  - `{:success? false, :action :idle}` with `:server-time-ms` and
    `:next-at-ms`, the earliest known schedule or lease deadline, or nil.
  - `{:success? false, :action :skipped, :reason
    :orphan|:corrupt-meta|:corrupt-payload|:corrupt-index}` after a containment
    attempt on an invalid head item. Index corruption that may represent an
    in-flight delivery removes only the stray ready occurrence. Another
    transition may have won; call again for the next message.

  Each result includes `:maintenance`, the bounded expired-lease and
  due-schedule cleanup tallies for `:orphan`, `:corrupt-meta`,
  `:corrupt-payload`, and `:corrupt-index` from this claim round. A `:skipped`
  head-of-line containment is reported by `:reason`, not counted here.
  `:maintenance-batch-size` defaults to 64 and has a maximum of 1000. Manual
  claims compete safely with workers but do not change [[worker-stats]]."
  ([queue] (msg-claim! queue nil))
  ([queue opts]
   (let [queue (assert-queue queue)
         opts (validate-opts! :msg-claim opts msg-claim-option-keys)
         maintenance-batch-size
         (bounded-positive-long :maintenance-batch-size
           (nil-default (:maintenance-batch-size opts) 64) 1000)
         maintenance_ (volatile! nil)
         claim (claim! queue maintenance-batch-size #(vreset! maintenance_ %))
         maintenance @maintenance_]
     (case (:action claim)
       :handle
       {:success? true, :action :claimed, :mid (:mid claim), :msg (:msg claim)
        :attempt (:attempt claim), :priority (:priority claim)
        :lease-token (:token claim), :lease-expiry-ms (:lease-expiry-ms claim)
        :enqueued-at-ms (:enqueued-at-ms claim)
        :server-time-ms (:server-time-ms claim), :maintenance maintenance}

       :idle
       {:success? false, :action :idle, :next-at-ms (:next-at-ms claim)
        :server-time-ms (:server-time-ms claim), :maintenance maintenance}

       :skip
       {:success? false, :action :skipped, :reason (:reason claim)
        :maintenance maintenance}

       (truss/ex-info! "[Carmine] Unexpected claim result"
         {:eid :carmine.mq/unexpected-reply, :action (:action claim)})))))

(defn msg-settle!
  "Settles a claim with the given outcome from [[outcome:ack]],
  [[outcome:retry]], [[outcome:dead]], or [[outcome:discard]]. Returns a result
  map.

  `lease-token` must be the exact token from the claim. A stale token returns
  `{:success? false, :action :stale}` and changes nothing. Otherwise, returns
  `{:success? true, :action :acked|:retried|:dead|:discarded}` with
  `:successor-promoted?`, plus `:retry-at-ms` and `:retry-delay-ms` when
  retried. A dead letter or exhausted settlement also has `:server-time-ms`. A
  retry at the attempt limit becomes `:dead` or `:discarded`, as specified by
  the message policy.

  `:durability` adds a local settlement barrier, independent of producer
  durability; nil, the default, disables it. The result includes the barrier as
  `:durability`, and a barrier failure does not undo the settlement."
  ([queue mid lease-token outcome]
   (msg-settle! queue mid lease-token outcome nil))
  ([queue mid lease-token outcome opts]
   (let [queue (assert-queue queue), mid (mid-str mid)
         lease-token (lease-token-str lease-token)
         opts (validate-opts! :msg-settle opts msg-settle-option-keys)
         _ (when-not (handler-outcome? outcome)
             (truss/ex-info!
               "[Carmine] Expected an outcome constructor result"
               {:eid :carmine.mq/invalid-option, :option :outcome
                :context :msg-settle, :value outcome}))
         durability (normalize-durability :msg-settle (:durability opts))
         _ (assert-durability-topology! queue durability)
         _ (assert-durability-version! queue durability)
         settled (settle! queue mid lease-token outcome durability)
         action (:action settled)]
     (if (= action :stale)
       {:success? false, :action :stale}
       (cond-> {:success? true, :action action
                :successor-promoted? (:successor-promoted? settled)}
         (contains? settled :retry-at-ms)
         (assoc :retry-at-ms (:retry-at-ms settled))

         (some? (:server-time-ms settled))
         (assoc :server-time-ms (:server-time-ms settled))

         (:retry-delay-ms settled)
         (assoc :retry-delay-ms (:retry-delay-ms settled))

         (:durability settled)
         (assoc :durability (:durability settled)))))))

(defn msg-release!
  "Returns a claimed but unhandled generation to the queue without consuming an
  attempt.

  The caller asserts that the message did not reach application code. Releasing
  after delivery causes immediate redelivery and refunds the attempt, so misuse
  can defeat `:max-attempts`.

  Uses `lease-token` fencing as in [[msg-settle!]]. Returns
  `{:success? true, :action :released}`, or
  `{:success? true, :action :released-successor}` when a newer coalesced
  generation replaces the released one, or
  `{:success? false, :action :stale|:corrupt}`."
  [queue mid lease-token]
  (let [queue (assert-queue queue), mid (mid-str mid)
        lease-token (lease-token-str lease-token)
        released (release! queue mid lease-token)]
    (case released
      :released           {:success? true,  :action :released}
      :released-successor {:success? true,  :action :released-successor}
      :stale              {:success? false, :action :stale}
      :corrupt            {:success? false, :action :corrupt})))

(defn msg-extend-lease!
  "Extends the lease of the given claimed generation and returns a result map.
  The new expiry cannot be earlier than the current expiry and uses the
  per-enqueue `:lease-ms` override when present.

  Returns `{:success? true, :action :extended, :lease-expiry-ms ...}` or
  `{:success? false, :action :stale}` when fenced. Throws
  `:eid :carmine.mq/corrupt-lease` for an invalid lease index or active
  metadata."
  [queue mid lease-token]
  (let [queue (assert-queue queue), mid (mid-str mid)
        lease-token (lease-token-str lease-token)]
    (if-let [expiry (extend-lease! queue mid lease-token)]
      {:success? true, :action :extended, :lease-expiry-ms expiry}
      {:success? false, :action :stale})))

(defn- idle-sleep! [state_ sleeping-threads_ sleep-ms]
  (let [thread (Thread/currentThread)]
    (swap! sleeping-threads_ conj thread)
    (try
      (when (= @state_ :running)
        (java.util.concurrent.locks.LockSupport/parkNanos
          (* (long sleep-ms) 1000000)))
      (finally (swap! sleeping-threads_ disj thread)))))

(defn- wake-sleepers! [sleeping-threads_]
  (run! #(java.util.concurrent.locks.LockSupport/unpark ^Thread %)
    @sleeping-threads_))

(defn- wake-one-sleeper! [sleeping-threads_]
  (when-let [thread (first @sleeping-threads_)]
    (java.util.concurrent.locks.LockSupport/unpark ^Thread thread)))

(defn- cluster-wake-addr [queue cluster-server]
  (let [{:keys [cluster-spec]} cluster-server
        topology (or (cluster/cached-topology cluster-spec)
                   (cluster/refresh-topology! cluster-spec))
        slot (cluster/cluster-slot (cluster/cluster-key (get (queue-keys queue) :signal)))
        shard
        (some
          (fn [[[lo hi] shard]] (when (<= (long lo) (long slot) (long hi)) shard))
          (:slot-ranges topology))
        ssl (get-in (conns/mgr-conn-opts (queue-manager queue)) [:socket-opts :ssl])
        master (:master shard)
        addr (if ssl (:tls-addr master) (:addr master))]
    (or addr
      (truss/ex-info! "[Carmine] No Cluster route for queue wake signal"
        {:eid :carmine.mq/wake-route-missing, :qname (queue-name queue), :slot slot}))))

(defn- new-wake-conn [queue stats_]
  (let [mgr (queue-manager queue)
        conn-opts (conns/mgr-conn-opts mgr)
        conn
        (if-let [cluster-server (conns/mgr-cluster-server mgr)]
          (let [[host port] (cluster-wake-addr queue cluster-server)]
            (conns/new-conn conn-opts (System/currentTimeMillis) nil host port))
          (conns/new-conn conn-opts))
        [host port] (conns/conn-addr conn)
        {:keys [in out]} @conn
        push-fn (conns/mgr-push-fn mgr [host port])]
    (try
      ;; Redis 7+: keep the one dedicated blocking client out of the optional
      ;; maxmemory-clients eviction pool.
      (resp/with-replies in out push-fn true false
        #(resp/rcmd* ["CLIENT" "NO-EVICT" "ON"]))
      conn
      (catch Throwable t
        (if (instance? ReusableConnError t)
          (do
            ;; Redis fully framed and drained this command error (commonly ACL
            ;; NOPERM). BLPOP can safely reuse the connection; record the lost
            ;; best-effort protection without entering a reconnect loop.
            (swap! stats_ update-in [:counts :wake-errors] inc)
            (trove/log! {:level :warn, :id :carmine.mq/wake-no-evict-error
                         :data {:qname (queue-name queue), :host host, :port port
                                :command-error (durability-error t)}})
            conn)
          (do
            (conns/conn-close! conn {:via 'new-wake-conn, :cause t})
            (throw t)))))))

(defn- close-wake-conn! [wake-lock wake-conn_ data]
  (when-let [conn
             (locking wake-lock
               (first (reset-vals! wake-conn_ nil)))]
    (conns/conn-close! conn data)))

(defn- ensure-wake-conn! [queue state_ wake-lock wake-conn_ stats_]
  (or @wake-conn_
    (when (= @state_ :running)
      ;; Network creation deliberately occurs outside `wake-lock`, so worker-stop!
      ;; never waits for DNS/Sentinel resolution or a TCP connect timeout.
      (let [candidate (new-wake-conn queue stats_)
            [selected close-candidate?]
            (locking wake-lock
              (cond
                @wake-conn_ [@wake-conn_ true]
                (= @state_ :running) (do (reset! wake-conn_ candidate) [candidate false])
                :else [nil true]))]
        (when close-candidate?
          (conns/conn-close! candidate
            {:via 'ensure-wake-conn!, :reason (if selected :duplicate :stopping)}))
        selected))))

(defn- await-wake!
  [queue state_ sleeping-threads_ wake-lock wake-conn_ wake-blocker_ stats_ timeout-ms]
  (if-not (compare-and-set! wake-blocker_ false true)
    (idle-sleep! state_ sleeping-threads_ timeout-ms)
    (try
      (when (= @state_ :running)
        (try
          (let [conn (ensure-wake-conn! queue state_ wake-lock wake-conn_ stats_)
                [host port] (when conn (conns/conn-addr conn))
                {:keys [socket in out]} (when conn @conn)]
            (when conn
              ;; Redis owns the normal timeout; the slightly longer socket
              ;; timeout prevents an indefinitely wedged blocking connection.
              (.setSoTimeout ^java.net.Socket socket (int (+ (long timeout-ms) 500)))
              (swap! stats_ update-in [:counts :wake-waits] inc)
              (let [reply
                    (resp/with-replies in out
                      (conns/mgr-push-fn (queue-manager queue) [host port]) true false
                      #(resp/rcmd* ["BLPOP" (get (queue-keys queue) :signal)
                                    (/ (double timeout-ms) 1000.0)]))]
                (when reply (swap! stats_ update-in [:counts :wake-signals] inc)))))
          (catch Throwable t
            (if (fatal-throwable? t)
              (throw t)
              (do
                ;; Advisory only: close the epoch and let the next ordinary
                ;; claim refresh Sentinel/Cluster routing. Polling remains the bound.
                (close-wake-conn! wake-lock wake-conn_
                  {:via 'await-wake!, :cause t, :reason :wake-error})
                ;; A graceful worker-stop! closes the wake conn under the blocked
                ;; BLPOP; that expected failure is not a wake error.
                (when (= @state_ :running)
                  (swap! stats_ update-in [:counts :wake-errors] inc)
                  (idle-sleep! state_ sleeping-threads_ timeout-ms)))))))
      (finally (reset! wake-blocker_ false)))))

(def ^:private worker-stats-schema-version 1)

(def ^:private empty-timing
  {:count 0, :sum-ms 0.0, :min-ms nil, :max-ms nil, :last-ms nil})

(defn- new-worker-stats
  ([] (new-worker-stats (System/currentTimeMillis)))
  ([since-client-time-ms]
   {:schema-version worker-stats-schema-version
    :since-client-time-ms since-client-time-ms
    :counts
    {:claims 0
     :handler-calls 0
     :handler-errors 0
     :invalid-handler-returns 0
     :handler-intents {:ack 0, :retry 0, :dead 0, :discard 0}
     :settlements {:acked 0, :retried 0, :dead 0, :discarded 0, :stale 0}
     :releases {:released 0, :released-successor 0, :stale 0, :corrupt 0}
     :lease-extensions {:extended 0, :stale 0, :errors 0}
     :lease-heartbeats {:extended 0, :stale 0, :errors 0}
     :durability-misses {:wait 0, :waitaof 0}
     :claim-skips
     {:orphan 0, :corrupt-meta 0, :corrupt-payload 0, :corrupt-index 0}
     :wake-waits 0
     :wake-signals 0
     :wake-errors 0
     :worker-errors 0
     :worker-failures 0
     :event-callback-errors 0}
    :timings
    {:claim-round-trip empty-timing
     :settlement-round-trip empty-timing
     :handler empty-timing
     :claim-age empty-timing
     :first-claim-age empty-timing}}))

(defn- stats-snapshot
  ([stats] (stats-snapshot stats (System/currentTimeMillis)))
  ([stats snapshot-client-time-ms]
   (assoc stats :snapshot-client-time-ms snapshot-client-time-ms)))

(defn- add-timing [timing elapsed-ms]
  (let [elapsed-ms (double elapsed-ms)
        count      (inc (long (:count timing)))
        sum-ms     (+ (double (:sum-ms timing)) elapsed-ms)
        min-ms     (:min-ms timing)
        max-ms     (:max-ms timing)]
    {:count count
     :sum-ms sum-ms
     :min-ms (if (nil? min-ms) elapsed-ms (min (double min-ms) elapsed-ms))
     :max-ms (if (nil? max-ms) elapsed-ms (max (double max-ms) elapsed-ms))
     :last-ms elapsed-ms}))

(defn- record-elapsed! [stats_ timing-key started-ns]
  (let [elapsed-ms (/ (- (System/nanoTime) started-ns) 1e6)]
    (swap! stats_ update-in [:timings timing-key] add-timing elapsed-ms)))

(defn- record-claim-cleanups! [stats_ cleanup-counts]
  (when (some pos? (vals cleanup-counts))
    (swap! stats_
      (fn [stats]
        (reduce-kv
          (fn [stats reason n]
            (update-in stats [:counts :claim-skips reason] (fnil + 0) n))
          stats cleanup-counts)))))

(defn- tracked-extend-lease! [stats_ queue mid token]
  (try
    (let [expiry (extend-lease! queue mid token)]
      (swap! stats_ update-in
        [:counts :lease-extensions (if expiry :extended :stale)] inc)
      expiry)
    (catch Throwable t
      (swap! stats_ update-in [:counts :lease-extensions :errors] inc)
      (throw t))))

(defn- record-claim!
  ;; `handling?` false for claims immediately released by a stopping worker:
  ;; `release!` refunds the attempt, so the next delivery is attempt 1 again
  ;; and would otherwise double-count `:first-claim-age`.
  [stats_ {:keys [attempt enqueued-at-ms server-time-ms]} handling?]
  (let [age-ms (- server-time-ms enqueued-at-ms)]
    (swap! stats_
      (fn [stats]
        (cond-> (-> stats
                  (update-in [:counts :claims] inc)
                  (update-in [:timings :claim-age] add-timing age-ms))
          (and handling? (= attempt 1))
          (update-in [:timings :first-claim-age] add-timing age-ms))))))

(defn- record-handler-finish! [stats_ handler-ms intent]
  (swap! stats_
    (fn [stats]
      (-> stats
        (update-in [:timings :handler] add-timing handler-ms)
        (update-in [:counts :handler-intents intent] inc)))))

(defn- record-worker-event [stats event data]
  (case event
    :handler-error
    (update-in stats [:counts :handler-errors] inc)

    :invalid-handler-return
    (update-in stats [:counts :invalid-handler-returns] inc)

    :worker-error
    (update-in stats [:counts :worker-errors] inc)

    :worker-failed
    (update-in stats [:counts :worker-failures] inc)

    :settled
    (let [stats (update-in stats [:counts :settlements (:action data)] (fnil inc 0))
          durability (get-in data [:result :durability])]
      (if (and durability (not (:satisfied? durability)))
        (update-in stats [:counts :durability-misses (:mode durability)] (fnil inc 0))
        stats))

    stats))

(def ^:private worker-event-context
  ;; Unlike Clojure dynamic bindings, a plain ThreadLocal is not captured by
  ;; futures/binding-conveyor. Callback self-detection must describe only the
  ;; thread that is synchronously inside this worker's callback.
  (ThreadLocal.))

(defn- in-worker-event-callback? [worker-id]
  (boolean (some #(= worker-id %) (.get ^ThreadLocal worker-event-context))))

(defn- notify-worker! [queue worker-id opts stats_ event data]
  (swap! stats_ record-worker-event event data)
  (when-let [on-event (:on-event opts)]
    (let [prior (.get ^ThreadLocal worker-event-context)]
      (.set ^ThreadLocal worker-event-context (cons worker-id prior))
      (try
        (try
          (on-event
            (merge {:event event, :queue (queue-name queue), :worker-id worker-id
                    :client-time-ms (System/currentTimeMillis)}
              data))
          (catch Throwable t
            (if (fatal-throwable? t)
              (throw t)
              (do
                (swap! stats_ update-in [:counts :event-callback-errors] inc)
                (trove/log! {:level :error, :id :carmine.mq/event-callback-error
                             :data (merge
                                     {:qname (queue-name queue), :worker-id worker-id
                                      :event event}
                                     (select-keys data [:runner :phase :mid :attempt]))
                             :error t})))))
        (finally
          (if prior
            (.set ^ThreadLocal worker-event-context prior)
            (.remove ^ThreadLocal worker-event-context)))))))

(defn- thread-name-component [qname]
  (let [readable (str/replace qname #"[^A-Za-z0-9._-]" "_")]
    (if (<= (count readable) 32)
      readable
      (str (subs readable 0 23) "-" (subs (car/script-hash qname) 0 8)))))

(defn- worker-thread-factory [queue worker-id]
  (let [counter (AtomicLong. 0)
        prefix (str "carmine-mq-v4-" (thread-name-component (queue-name queue))
                 "-" (subs worker-id 0 8) "-")]
    (reify ThreadFactory
      (newThread [_ runnable]
        (let [thread (Thread. ^Runnable runnable
                       (str prefix (.incrementAndGet counter)))]
          (.setDaemon thread true)
          (.setUncaughtExceptionHandler thread
            (reify Thread$UncaughtExceptionHandler
              (uncaughtException [_ thread error]
                (trove/log! {:level :error, :id :carmine.mq/runner-failed
                             :data {:qname (queue-name queue), :worker-id worker-id
                                    :thread (.getName ^Thread thread)}
                             :error error}))))
          thread)))))

(defn- new-worker-executor [queue worker-id concurrency]
  (Executors/newFixedThreadPool (int concurrency)
    (worker-thread-factory queue worker-id)))

(defn- notify-worker-stopped!
  [queue worker-id opts stop-notification_ stats_ data]
  ;; Awaiters should not observe a fully stopped lifecycle until its final
  ;; synchronous callback has returned (or thrown). A Thread marker also makes
  ;; self-await from that callback fail fast.
  (reset! stop-notification_ (Thread/currentThread))
  (try
    (notify-worker! queue worker-id opts stats_ :worker-stopped data)
    (finally (reset! stop-notification_ :stopped))))

(defn- record-worker-throwable! [error_ ^Throwable throwable]
  (if-let [^Throwable primary @error_]
    (when-not (identical? primary throwable)
      (.addSuppressed primary throwable))
    (vreset! error_ throwable)))

(defn- attempt-worker-step! [error_ f]
  (try
    (f)
    (catch Throwable t
      (record-worker-throwable! error_ t))))

(defn- fail-running-worker!
  [queue worker-id opts state_ stats_ wake-lock wake-conn_ sleeping-threads_
   failure-data close-data error_]
  ;; Only the running->failed CAS winner owns terminal observability and wake
  ;; cleanup. This makes the helper safe both inside the loop and at its outer
  ;; runner boundary: a re-caught fatal, concurrent stop, or peer failure cannot
  ;; duplicate terminal phases. The escaping throwable remains primary while
  ;; phase failures are retained in deterministic occurrence order.
  (when (compare-and-set! state_ :running :failed)
    (let [primary @error_]
      (attempt-worker-step! error_
        #(notify-worker! queue worker-id opts stats_ :worker-failed
           (assoc failure-data :error primary)))
      (attempt-worker-step! error_
        #(close-wake-conn! wake-lock wake-conn_
           (assoc close-data :cause primary)))
      (attempt-worker-step! error_ #(wake-sleepers! sleeping-threads_)))))

(defn- throw-recorded-worker-error! [error_]
  (when-let [error @error_]
    (throw error)))

(defn- complete-worker-stop!
  [queue worker-id opts state_ stop-notification_ remaining_ stats_]
  ;; The stop winner publishes `true` only after the synchronous
  ;; :worker-stopping callback has completed (normally or exceptionally).
  ;; Both it and the last retiring runner call this helper, closing either
  ;; side of the flag/count race without blocking a runner on user code.
  (when (and (true? @stop-notification_)
          (zero? @remaining_)
          (compare-and-set! state_ :stopping :stopped))
    (notify-worker-stopped! queue worker-id opts stop-notification_ stats_ nil)))

(defn- run-worker-stop-phases!
  [queue worker-id opts state_ stop-notification_ remaining_ stats_
   wake-lock wake-conn_ sleeping-threads_ error_]
  ;; The caller has atomically claimed `stop-notification_` with its Thread.
  ;; Attempt every required phase even after a fatal cleanup/callback error;
  ;; publish stopping completion before allowing the final stopped transition.
  (attempt-worker-step! error_
    #(close-wake-conn! wake-lock wake-conn_
       {:via 'worker-stop!, :reason :worker-stopping
        :qname (queue-name queue)}))
  (attempt-worker-step! error_ #(wake-sleepers! sleeping-threads_))
  (attempt-worker-step! error_
    #(notify-worker! queue worker-id opts stats_ :worker-stopping nil))
  (reset! stop-notification_ true)
  (attempt-worker-step! error_
    #(complete-worker-stop! queue worker-id opts state_
       stop-notification_ remaining_ stats_)))

(defn- shutdown-lease-heartbeats!
  ;; Retires the per-worker heartbeat scheduler with its worker, so no beat
  ;; thread or queued task can outlive the worker's terminal state.
  [heartbeat-executor_]
  (when-let [executor (first (reset-vals! heartbeat-executor_ nil))]
    (.shutdownNow ^ScheduledThreadPoolExecutor executor)))

(defn- retire-runner!
  [queue worker-id opts state_ stop-notification_ remaining_ stats_
   heartbeat-executor_]
  (when (zero? (swap! remaining_ dec))
    ;; The last slot's retirement also retires the per-worker heartbeat
    ;; scheduler: no handler remains that could still hold a lease.
    (shutdown-lease-heartbeats! heartbeat-executor_)
    (loop []
      (let [state @state_]
        (cond
          (= state :stopping)
          (complete-worker-stop! queue worker-id opts state_
            stop-notification_ remaining_ stats_)

          (= state :running)
          (if (compare-and-set! state_ :running :failed)
            (notify-worker! queue worker-id opts stats_ :worker-failed
              {:reason :runners-exited})
            (recur)))))))

(defn- retire-unsubmitted-runners!
  [queue worker-id opts state_ stop-notification_ remaining_ stats_
   heartbeat-executor_ unsubmitted error_]
  (dotimes [_ unsubmitted]
    (attempt-worker-step! error_
      #(retire-runner! queue worker-id opts state_
         stop-notification_ remaining_ stats_ heartbeat-executor_))))

(defn- worker-await-timeout-ms [timeout-ms]
  (if (or (nil? timeout-ms)
        (and (integer? timeout-ms) (<= 0 timeout-ms Long/MAX_VALUE)))
    (some-> timeout-ms long)
    (truss/ex-info! "[Carmine] Invalid worker await timeout"
      {:eid :carmine.mq/invalid-timeout-ms
       :operation :worker-await-stopped!
       :timeout-ms (enc/typed-val timeout-ms)
       :expected 'nil-or-nat-long})))

(defn- begin-worker-start!
  [lifecycle-lock state_ start-notification_ remaining_ concurrency]
  (locking lifecycle-lock
    (when (compare-and-set! state_ :new :running)
      ;; Stop selection uses this same short lock, so :running and ownership of
      ;; every future runner slot are atomic from worker-stop!'s perspective.
      (reset! remaining_ concurrency)
      (reset! start-notification_ (Thread/currentThread))
      true)))

(defn- finish-worker-start-notification!
  [lifecycle-lock state_ start-notification_ stop-notification_
   started-error?]
  (locking lifecycle-lock
    (reset! start-notification_ :complete)
    (let [_ (when started-error?
              (compare-and-set! state_ :running :failed))
          stop-phases-owner?
          (and (= @state_ :stopping)
            (compare-and-set! stop-notification_ false
              (Thread/currentThread)))]
      {:stop-phases-owner? (boolean stop-phases-owner?)})))

(defn- begin-worker-stop!
  [worker-id lifecycle-lock state_ start-notification_ stop-notification_]
  (locking lifecycle-lock
    (cond
      (compare-and-set! state_ :new :stopped)
      :never-started

      (compare-and-set! state_ :running :stopping)
      (let [started? (= @start-notification_ :complete)
            reentrant-start? (in-worker-event-callback? worker-id)
            stop-phases-owner?
            (and (or started? reentrant-start?)
              (compare-and-set! stop-notification_ false
                (Thread/currentThread)))]
        (if stop-phases-owner? :run-stop-phases :deferred-stop-phases))

      :else nil)))

(defprotocol ^:private IWorker
  "Lifecycle and observability operations for a Carmine v4 MQ worker."
  (worker-start! [worker]
    "Starts a new worker once. Returns true iff started, otherwise false.")
  (worker-stop! [worker]
    "Stops new claims without interrupting handlers. Returns true iff
    transitioned.")
  (worker-await-stopped! [worker timeout-ms]
    "Stops the worker and waits up to `timeout-ms` for handlers. Nil sets no
    limit.")
  (worker-stats [worker]
    "Returns a versioned snapshot of this process-local worker's counters and
    timings. Schema 1 `:claim-skips` counters include both primary ready-head
    skips and expired/scheduled maintenance cleanups, classified as `:orphan`,
    `:corrupt-meta`, `:corrupt-payload`, or `:corrupt-index`.")
  (worker-clear-stats! [worker]
    "Atomically starts a new statistics window and returns the prior
    snapshot."))

(defn- new-lease-heartbeat-executor
  ;; Per-worker single-thread daemon scheduler for optional lease heartbeats.
  ;; Beats run their Redis round trip on this thread, so one wedged call
  ;; (e.g. an unbounded read timeout or a blocked pool borrow) delays only
  ;; this worker's beats, never another worker's. Remove-on-cancel keeps
  ;; completed deliveries out of the queue.
  [queue worker-id]
  (doto
    (ScheduledThreadPoolExecutor. 1
      (reify ThreadFactory
        (newThread [_ runnable]
          (doto (Thread. ^Runnable runnable
                  (str "carmine-mq-v4-"
                    (thread-name-component (queue-name queue))
                    "-" (subs worker-id 0 8) "-heartbeat"))
            (.setDaemon true)))))
    (.setRemoveOnCancelPolicy true)))

(defn- worker-terminal-state? [state]
  (contains? #{:stopped :failed :closed} state))

(defn- cancel-lease-heartbeat! [heartbeat]
  (when heartbeat
    (vreset! (:stop?_ heartbeat) true)
    (some-> ^ScheduledFuture @(:future_ heartbeat) (.cancel false))))

(defn- fail-worker-from-heartbeat!
  ;; A scheduled beat's future is never observed, so a fatal error must
  ;; terminalize the worker here (same discipline as the runner's fatal
  ;; handling) and be logged rather than escape into the unobserved future.
  [{:keys [queue worker-id opts state_ stats_ wake-lock wake-conn_
           sleeping-threads_]} idx mid attempt throwable]
  (let [error_ (volatile! throwable)]
    (attempt-worker-step! error_
      #(fail-running-worker! queue worker-id opts state_ stats_
         wake-lock wake-conn_ sleeping-threads_
         {:runner idx, :phase :lease-heartbeat, :mid mid, :attempt attempt}
         {:via 'lease-heartbeat, :reason :fatal-error}
         error_))
    (trove/log! {:level :error, :id :carmine.mq/lease-heartbeat-error
                 :data {:qname (queue-name queue), :worker-id worker-id
                        :runner idx, :phase :lease-heartbeat
                        :mid mid, :attempt attempt
                        :worker-failed? (= @state_ :failed)}
                 :error @error_})))

(defn- run-lease-heartbeat-beat!
  ;; One periodic extension for one claimed delivery. The stop flag and the
  ;; worker's terminal state are checked at entry and again immediately
  ;; before publishing any stat or event, so a cancelled delivery or a
  ;; terminal worker observes no further heartbeat effects; the error-once
  ;; CAS keeps failures at most one `:worker-error` per delivery even when a
  ;; beat runs before its own future is published.
  [{:keys [queue worker-id opts state_ stats_] :as ctx} idx {:keys [mid token attempt]}
   {:keys [stop?_ error-published?*] :as heartbeat}]
  (let [stopped? #(or @stop?_ (worker-terminal-state? @state_))]
    (when-not (stopped?)
      (try
        (if (extend-lease! queue mid token)
          (when-not (stopped?)
            (swap! stats_ update-in [:counts :lease-heartbeats :extended] inc))
          ;; A competing transition won; stop silently.
          (let [publish? (not (stopped?))]
            (cancel-lease-heartbeat! heartbeat)
            (when publish?
              (swap! stats_ update-in [:counts :lease-heartbeats :stale] inc))))
        (catch Throwable t
          (let [publish? (not (stopped?))]
            (cancel-lease-heartbeat! heartbeat)
            (if (fatal-throwable? t)
              (fail-worker-from-heartbeat! ctx idx mid attempt t)
              (when (and publish?
                      (compare-and-set! error-published?* false true))
                (swap! stats_ update-in [:counts :lease-heartbeats :errors] inc)
                (trove/log! {:level :error, :id :carmine.mq/lease-heartbeat-error
                             :data {:qname (queue-name queue)
                                    :worker-id worker-id
                                    :runner idx, :phase :lease-heartbeat
                                    :mid mid, :attempt attempt
                                    :worker-failed? false}
                             :error t})
                (try
                  (notify-worker! queue worker-id opts stats_ :worker-error
                    {:runner idx, :phase :lease-heartbeat
                     :mid mid, :attempt attempt, :error t})
                  (catch Throwable callback-fatal
                    (fail-worker-from-heartbeat! ctx idx mid attempt
                      callback-fatal)))))))))))

(defn- start-lease-heartbeat!
  "Schedules periodic lease extension for one claimed delivery and returns
  the heartbeat handle, or nil when the worker has no
  `:lease-extend-every-ms`. A stale extension result stops the heartbeat
  silently: the competing transition won and lease state is no longer this
  delivery's to keep alive. A failed beat stops the heartbeat and surfaces
  as at most one `:worker-error` event per delivery; lease expiry then
  governs. The caller cancels the heartbeat when its handler completes,
  always before the settlement attempt; an already in-flight beat is fenced
  by the settlement's token removal."
  [{:keys [opts heartbeat-executor_] :as ctx} idx claim]
  (when-let [every-ms (:lease-extend-every-ms opts)]
    (when-let [executor @heartbeat-executor_]
      (let [heartbeat {:future_ (volatile! nil), :stop?_ (volatile! false)
                       :error-published?* (atom false)}
            task
            (.scheduleWithFixedDelay
              ^ScheduledExecutorService executor
              ^Runnable
              (fn [] (run-lease-heartbeat-beat! ctx idx claim heartbeat))
              (long every-ms) (long every-ms) TimeUnit/MILLISECONDS)]
        (vreset! (:future_ heartbeat) task)
        heartbeat))))

(defn- settle-and-notify!
  ;; Makes this delivery's one settlement attempt and publishes its :settled
  ;; event, which reports the settlement result (possibly :stale). A thrown
  ;; settlement attempt escapes to the runner loop's ordinary error handling.
  [{:keys [queue worker-id opts stats_]} claim event-data
   ^HandlerOutcome outcome handler-ms]
  (let [settlement-started (System/nanoTime)
        settled
        (try
          (settle! queue (:mid claim) (:token claim) outcome (:durability opts))
          (finally
            (record-elapsed! stats_ :settlement-round-trip
              settlement-started)))]
    (notify-worker! queue worker-id opts stats_ :settled
      (cond->
        (assoc event-data
          :handler-ms handler-ms
          :intent (.-action outcome)
          :action (:action settled), :result settled)
        (:retry-delay-ms settled)
        (assoc :retry-delay-ms (:retry-delay-ms settled))

        (.-reason outcome)
        (assoc :reason (.-reason outcome))

        (some? (.-delay-ms outcome))
        (assoc :delay-ms (.-delay-ms outcome))

        (:include-msg? opts)
        (assoc :msg (:msg claim))))))

(defn- handle-worker-claim!
  ;; Runs the delivery cycle for one claimed message: handler invocation,
  ;; outcome normalization, error events, then settlement. A claim raced by a
  ;; concurrent stop is instead released unhandled, refunding its attempt.
  [{:keys [queue worker-id handler opts state_ stats_ sleeping-threads_
           active_] :as ctx}
   idx {:keys [mid token attempt server-time-ms] :as claim}]
  (let [handling? (= @state_ :running)]
    (record-claim! stats_ claim handling?)
    ;; Wake local concurrency slots before this handler starts; Redis carries
    ;; the same baton to blockers in other worker processes.
    (wake-one-sleeper! sleeping-threads_)
    (if-not handling?
      (let [released (release! queue mid token)]
        (swap! stats_ update-in [:counts :releases released] (fnil inc 0)))
      (let [age-ms (- server-time-ms (:enqueued-at-ms claim))
            extend! #(tracked-extend-lease! stats_ queue mid token)
            handler-arg
            {:queue queue, :mid mid, :msg (:msg claim)
             :attempt attempt, :priority (:priority claim)
             :enqueued-at-ms (:enqueued-at-ms claim)
             :age-ms age-ms
             :lease-expiry-ms (:lease-expiry-ms claim)
             :server-time-ms server-time-ms
             :extend-lease! extend!}
            event-data
            {:runner idx, :mid mid, :attempt attempt
             :priority (:priority claim)
             :enqueued-at-ms (:enqueued-at-ms claim)
             :age-ms age-ms, :server-time-ms server-time-ms}
            _ (swap! stats_ update-in [:counts :handler-calls] inc)
            handler-started (System/nanoTime)
            heartbeat (start-lease-heartbeat! ctx idx claim)
            [result throwable]
            (do
              (swap! active_ inc)
              (try
                (try [(handler handler-arg) nil]
                     (catch Throwable t
                       (if (fatal-throwable? t)
                         (throw t)
                         (do
                           ;; Restore interrupt status only while a stop is
                           ;; in progress, cooperating with stop-side
                           ;; interruption. With no stop pending, a
                           ;; handler-local InterruptedException is an
                           ;; ordinary handler error and deliberately does
                           ;; not poison this long-lived runner thread: an
                           ;; interrupted runner's later parks would return
                           ;; immediately and subsequent handlers would
                           ;; inherit interrupted state.
                           (when (not= @state_ :running)
                             (preserve-interruption! t))
                           [nil t]))))
                (finally
                  (swap! active_ dec)
                  ;; Cancel before any settlement attempt, so a heartbeat
                  ;; can never keep a settled lease alive.
                  (cancel-lease-heartbeat! heartbeat))))
            handler-ms (/ (- (System/nanoTime) handler-started) 1e6)
            invalid-return? (and (nil? throwable)
                              (not (handler-outcome? result)))
            outcome
            (cond
              throwable
              (handler-outcome :retry nil
                (throwable-reason throwable))

              invalid-return?
              (handler-outcome :retry nil "invalid-handler-return")

              :else result)
            return-type (when invalid-return?
                          (if (nil? result) "nil"
                            (.getName (class result))))]
        (record-handler-finish! stats_ handler-ms
          (.-action ^HandlerOutcome outcome))
        (when throwable
          (notify-worker! queue worker-id opts stats_ :handler-error
            (assoc event-data :handler-ms handler-ms
              :error throwable)))
        (when invalid-return?
          (notify-worker! queue worker-id opts stats_ :invalid-handler-return
            (assoc event-data :handler-ms handler-ms
              :return-type return-type)))
        (settle-and-notify! ctx claim event-data outcome handler-ms)))))

(defn- run-worker-loop!
  ;; One runner slot's claim/handle/settle loop. Returns when the worker
  ;; leaves :running; fatal errors terminalize the worker and rethrow.
  [{:keys [queue worker-id opts state_ stats_ sleeping-threads_
           wake-lock wake-conn_ wake-blocker_] :as ctx} idx]
  (let [{:keys [maintenance-batch-size idle-min-ms idle-max-ms]} opts]
    (loop []
      (when (= @state_ :running)
        (try
          (let [claim-started (System/nanoTime)
                claim
                (try
                  (claim! queue maintenance-batch-size
                    #(record-claim-cleanups! stats_ %))
                  (finally
                    (record-elapsed! stats_ :claim-round-trip claim-started)))
                {:keys [action next-at-ms server-time-ms]} claim]
            (case action
              :handle (handle-worker-claim! ctx idx claim)

              :idle
              (let [until-next (when next-at-ms
                                 (max 0 (- next-at-ms server-time-ms)))
                    sleep-ms (worker-idle-timeout-ms
                               idle-min-ms idle-max-ms until-next)]
                (await-wake! queue state_ sleeping-threads_
                  wake-lock wake-conn_ wake-blocker_ stats_ sleep-ms))

              :skip
              (swap! stats_ update-in
                [:counts :claim-skips (:reason claim)] (fnil inc 0))))
          (catch Throwable t
            (if (fatal-throwable? t)
              (let [error_ (volatile! t)]
                (fail-running-worker! queue worker-id opts state_ stats_
                  wake-lock wake-conn_ sleeping-threads_
                  {:runner idx, :phase :runner}
                  {:via 'worker-loop, :reason :fatal-error}
                  error_)
                ;; Neither a fatal failure callback nor a close error may
                ;; replace the handler primary or skip waking a
                ;; blocked/sleeping peer.
                (throw-recorded-worker-error! error_))
              (do
                (trove/log! {:level :error, :id :carmine.mq/worker-error
                             :data {:qname (queue-name queue), :worker-id worker-id
                                    :runner idx, :phase :loop}
                             :error t})
                (try
                  (notify-worker! queue worker-id opts stats_ :worker-error
                    {:runner idx, :phase :loop, :error t})
                  (catch Throwable callback-fatal
                    (let [error_ (volatile! callback-fatal)]
                      (fail-running-worker! queue worker-id opts state_
                        stats_ wake-lock wake-conn_
                        sleeping-threads_
                        {:runner idx, :phase :event-callback}
                        {:via 'worker-loop
                         :reason :fatal-event-callback}
                        error_)
                      (throw-recorded-worker-error! error_))))
                (idle-sleep! state_ sleeping-threads_ idle-max-ms)))))
        (recur)))))

(defn- new-runner-task
  ;; Builds one runner slot's executor task: thread registration, the
  ;; claim/handle/settle loop with its terminal boundary, then retirement.
  ^Runnable
  [{:keys [queue worker-id opts state_ stats_ runner-threads_
           sleeping-threads_ wake-lock wake-conn_ stop-notification_
           remaining_ heartbeat-executor_] :as ctx} idx]
  (reify Runnable
    (run [_]
      (let [thread (Thread/currentThread)
            runner-error_ (volatile! nil)]
        (swap! runner-threads_ conj thread)
        (try
          (try
            (run-worker-loop! ctx idx)
            (catch Throwable t
              (record-worker-throwable! runner-error_ t)
              ;; Catch bodies and other unexpected loop escapes are not
              ;; recaught by the loop's own catch. Terminalize at the task
              ;; boundary before this slot retires, or a multi-slot worker
              ;; could remain running at reduced capacity indefinitely.
              (fail-running-worker! queue worker-id opts state_ stats_
                wake-lock wake-conn_ sleeping-threads_
                {:runner idx, :phase :runner-boundary}
                {:via 'worker-loop, :reason :unexpected-error}
                runner-error_)))
          (finally
            (attempt-worker-step! runner-error_
              #(swap! runner-threads_ disj thread))
            (attempt-worker-step! runner-error_
              #(retire-runner! queue worker-id opts state_
                 stop-notification_ remaining_ stats_ heartbeat-executor_))))
        (throw-recorded-worker-error! runner-error_)))))

(defn- create-worker-executor!
  ;; Creates the fixed runner pool. On failure, terminalizes (unless a
  ;; concurrent stop's transition won), retires every never-submitted slot,
  ;; and rethrows with phase failures retained as suppressed exceptions.
  [{:keys [queue worker-id opts state_ lifecycle-lock stats_
           stop-notification_ remaining_ heartbeat-executor_]}]
  (let [{:keys [concurrency]} opts]
    (try
      (new-worker-executor queue worker-id concurrency)
      (catch Throwable t
        (let [error_ (volatile! t)
              failed?
              (locking lifecycle-lock
                (compare-and-set! state_ :running :failed))]
          (attempt-worker-step! error_
            #(notify-worker! queue worker-id opts stats_
               (if failed? :worker-failed :worker-error)
               {:phase :executor-create, :error t}))
          (retire-unsubmitted-runners! queue worker-id opts state_
            stop-notification_ remaining_ stats_ heartbeat-executor_
            concurrency error_)
          (throw-recorded-worker-error! error_))))))

(defn- launch-worker-runners!
  ;; Submits the fixed runner set to `pool`, then shuts the pool down
  ;; gracefully: no new tasks are needed after submission, and shutdown never
  ;; interrupts a running handler. A submission or custom-executor failure
  ;; follows the same primary/suppressed cleanup rules as elsewhere, using an
  ;; ordinary stop plus :worker-error rather than a terminal event of its own.
  [{:keys [worker queue worker-id opts state_ stats_ stop-notification_
           remaining_ heartbeat-executor_] :as ctx} ^ExecutorService pool]
  (let [{:keys [concurrency]} opts
        failure_ (atom nil)
        submitted
        (loop [idx 0]
          (if (and (< idx concurrency) (= @state_ :running))
            (if-let [failure
                     (try
                       (.execute pool (new-runner-task ctx idx))
                       nil
                       (catch Throwable t t))]
              (do (reset! failure_ failure) idx)
              (recur (inc idx)))
            idx))]
    (let [error_ (volatile! @failure_)]
      (attempt-worker-step! error_
        #(.shutdown pool))
      (let [executor-failure @error_]
        (when executor-failure
          (attempt-worker-step! error_ #(worker-stop! worker)))
        ;; Each successful execute transferred exactly one slot to its task.
        ;; Locally retire every slot never submitted, including an ordinary
        ;; stop observed between submissions.
        (when (< submitted concurrency)
          (retire-unsubmitted-runners! queue worker-id opts state_
            stop-notification_ remaining_ stats_ heartbeat-executor_
            (- concurrency submitted) error_))
        (when executor-failure
          (attempt-worker-step! error_
            #(notify-worker! queue worker-id opts stats_ :worker-error
               {:phase (if @failure_
                         :executor-submit :executor-shutdown)
                :error executor-failure})))
        (throw-recorded-worker-error! error_)
        true))))

(deftype Worker
  [worker-id queue handler opts state_ lifecycle-lock start-notification_
   executor_ heartbeat-executor_ runner-threads_ sleeping-threads_ active_
   wake-lock wake-conn_ wake-blocker_ stop-notification_ remaining_ stats_]
  java.io.Closeable
  (close [this]
    (worker-stop! this)
    (when (and (worker-await-stopped! this (long (:close-timeout-ms opts)))
            (compare-and-set! state_ :stopped :closed))
      true))

  clojure.lang.IDeref
  (deref [_]
    {:queue (queue-name queue), :worker-id worker-id, :state @state_, :opts opts
     :stats (stats-snapshot @stats_)
     :threads {:names (->> @runner-threads_ (map #(.getName ^Thread %)) sort vec)
               :busy @active_, :concurrency (:concurrency opts)}})

  IWorker
  (worker-start! [this]
    (boolean
      (when (begin-worker-start! lifecycle-lock state_ start-notification_
              remaining_ (:concurrency opts))
        (let [{:keys [concurrency]} opts
              ctx
              {:worker this, :queue queue, :worker-id worker-id
               :handler handler, :opts opts, :state_ state_
               :lifecycle-lock lifecycle-lock, :stats_ stats_
               :runner-threads_ runner-threads_
               :sleeping-threads_ sleeping-threads_, :active_ active_
               :wake-lock wake-lock, :wake-conn_ wake-conn_
               :wake-blocker_ wake-blocker_
               :stop-notification_ stop-notification_, :remaining_ remaining_
               :heartbeat-executor_ heartbeat-executor_}
              started-error_ (volatile! nil)
              _ (attempt-worker-step! started-error_
                  #(notify-worker! queue worker-id opts stats_ :worker-started nil))
              {:keys [stop-phases-owner?]}
              (finish-worker-start-notification! lifecycle-lock state_
                start-notification_ stop-notification_
                (boolean @started-error_))]
          (when @started-error_
            ;; Calling the callback again to report its own fatal failure would
            ;; recurse. Preserve the existing direct accounting behaviour.
            (swap! stats_ record-worker-event :worker-failed
              {:phase :worker-started-callback, :error @started-error_}))
          (when stop-phases-owner?
            (run-worker-stop-phases! queue worker-id opts state_ stop-notification_
              remaining_ stats_ wake-lock wake-conn_ sleeping-threads_
              started-error_))
          (if-not (= @state_ :running)
            (do
              ;; No task owns a slot yet. Fatal started/stopping/stopped
              ;; callbacks cannot leave this pre-executor worker non-terminal.
              (retire-unsubmitted-runners! queue worker-id opts state_
                stop-notification_ remaining_ stats_ heartbeat-executor_
                concurrency started-error_)
              (throw-recorded-worker-error! started-error_)
              true)
            (let [pool (create-worker-executor! ctx)]
              (reset! executor_ pool)
              (when (:lease-extend-every-ms opts)
                (reset! heartbeat-executor_
                  (new-lease-heartbeat-executor queue worker-id)))
              (launch-worker-runners! ctx pool)))))))

  (worker-stop! [_]
    (case (begin-worker-stop! worker-id lifecycle-lock state_
            start-notification_ stop-notification_)
      :never-started
      (do
        (notify-worker-stopped! queue worker-id opts stop-notification_ stats_
          {:reason :never-started})
        true)

      :run-stop-phases
      (let [error_ (volatile! nil)]
        (run-worker-stop-phases! queue worker-id opts state_ stop-notification_
          remaining_ stats_ wake-lock wake-conn_ sleeping-threads_ error_)
        (throw-recorded-worker-error! error_)
        true)

      :deferred-stop-phases true
      false))

  (worker-await-stopped! [this timeout-ms]
    (let [timeout-ms (worker-await-timeout-ms timeout-ms)]
      (worker-stop! this)
      (if (or (in-worker-event-callback? worker-id)
            (contains? @runner-threads_ (Thread/currentThread))
            (identical? @stop-notification_ (Thread/currentThread)))
        false
        (let [deadline (when timeout-ms
                         (+' (System/nanoTime) (*' timeout-ms 1000000)))
              terminal?
              (fn []
                (and (zero? @remaining_)
                  (let [state @state_]
                    (or (= state :failed)
                      (and (#{:stopped :closed} state)
                        (= @stop-notification_ :stopped))))))
              ;; Condition-based waiting: every terminal transition mutates a
              ;; watched atom, and the pre-wait recheck holds the monitor the
              ;; watches notify under, so no transition can be missed.
              signal (Object.)
              watch-key (Object.)]
          (doseq [a [state_ remaining_ stop-notification_]]
            (add-watch a watch-key
              (fn [_ _ _ _] (locking signal (.notifyAll signal)))))
          (try
            (loop []
              (if (terminal?)
                true
                (let [wait-ms
                      (if deadline
                        (let [remaining-ns (-' deadline (System/nanoTime))]
                          (when (pos? remaining-ns)
                            (long (min (max 1 (quot remaining-ns 1000000))
                                    Long/MAX_VALUE))))
                        0)] ; Wait without a timeout
                  (if (nil? wait-ms)
                    false
                    (do
                      (locking signal
                        (when-not (terminal?) (.wait signal wait-ms)))
                      (recur))))))
            (finally
              (doseq [a [state_ remaining_ stop-notification_]]
                (remove-watch a watch-key))))))))

  (worker-stats [_]
    (stats-snapshot @stats_))

  (worker-clear-stats! [_]
    (let [snapshot-client-time-ms (System/currentTimeMillis)
          prior (first
                  (reset-vals! stats_
                    (new-worker-stats snapshot-client-time-ms)))]
      (stats-snapshot prior snapshot-client-time-ms))))

(defmethod print-method Worker [w ^java.io.Writer out]
  (.write out (str "#<CarmineV4MQWorker " (pr-str (:queue @w)) " " (name (:state @w)) ">")))

(defn worker?
  "Returns true iff the given `x` is a Carmine v4 MQ worker."
  [x] (instance? Worker x))

(defn worker-create
  "Creates a stopped worker. A concurrency slot claims only when it is ready to
  call the given `handler`, so a lease does not wait in a local executor.

  The handler must return a result from [[outcome:ack]], [[outcome:retry]],
  [[outcome:dead]], or [[outcome:discard]]. An ordinary exception or other
  result causes a retry, which the message policy changes to a terminal result
  at the attempt limit. JVM-fatal errors propagate after cleanup and fail the
  worker. Handler input contains exactly `:queue`, `:mid`, `:msg`,
  `:attempt`, `:priority`, `:enqueued-at-ms`, `:age-ms`, `:lease-expiry-ms`,
  `:server-time-ms`, and `:extend-lease!`. The extension function returns a new
  expiry, returns nil for a stale token, or throws
  `:eid :carmine.mq/corrupt-lease` for invalid lease state.

  Options:

  - `:concurrency` (1) and `:maintenance-batch-size` (64).
  - `:idle-min-ms` (10), `:idle-max-ms` (1000), and `:close-timeout-ms` (5000).
  - Settlement-only `:durability` (nil).
  - `:include-msg?` (false) to add `:msg` to settled events.
  - `:lease-extend-every-ms` (nil) for an automatic lease heartbeat.
  - Synchronous, isolated `:on-event`.

  `:on-event` receives at least `:event`, `:queue`, `:worker-id`, and
  `:client-time-ms`. Current events are `:worker-started`, `:worker-stopping`,
  `:worker-stopped`, `:worker-failed`, `:worker-error`, `:handler-error`,
  `:invalid-handler-return`, and `:settled`. New kinds and keys may be added, so
  ignore unrecognized ones. Ordinary callback exceptions are counted and logged
  without stopping message processing, while JVM-fatal errors propagate after
  cleanup.

  A JVM-fatal handler error skips settlement and leaves the message leased until
  expiry. Otherwise, each delivery that reaches the handler gets exactly one
  settlement attempt. Its `:settled` event follows any applicable
  `:handler-error` or `:invalid-handler-return` event and may report action
  `:stale`; a failed settlement gives `:worker-error`, after which lease expiry
  controls redelivery.

  Except for a JVM-fatal callback error, a started or stopped worker sends
  exactly one terminal event, `:worker-stopped` or `:worker-failed`, after any
  applicable start and stopping events. Operational failures may surface as
  `:worker-error` before a normal `:worker-stopped`, so a stopped worker is not
  necessarily error-free. [[worker-await-stopped!]] returns false when called
  from any of the worker's own threads, including its handler, `:on-event`
  callback, or an in-progress stop phase.

  `:lease-extend-every-ms` automatically extends a claimed delivery's lease
  while its handler runs. The interval must be shorter than the queue's default
  `:lease-ms`, although a shorter per-message lease can still expire between
  heartbeats. A failed or fenced heartbeat stops, and lease expiry then applies.
  While heartbeats continue to succeed, a deadlocked handler's lease is renewed
  indefinitely. Use manual `:extend-lease!` when its work must be bounded by the
  lease. Use a finite connection `:read-timeout-ms` to bound each Redis call
  independently.

  [[worker-stats]] is a process-local, eventually consistent, versioned
  snapshot; keys may be added within a schema version. Use [[queue-status]] for
  exact Redis counts. Stopping waits for handlers without
  interrupting them, and `close` returns after its timeout even if a handler is
  stuck. Lease expiry permits a token-fenced transition but does not revoke the
  token."
  ([queue handler] (worker-create queue handler nil))
  ([queue handler opts]
   (let [queue (assert-queue queue)
         _ (truss/have fn? handler)
         opts (validate-opts! :worker-create opts worker-option-keys)
         opts (merge {:concurrency 1, :maintenance-batch-size 64
                      :idle-min-ms 10, :idle-max-ms 1000
                      :close-timeout-ms 5000, :include-msg? false}
                opts)
         opts (assoc opts
                :concurrency (bounded-positive-long :concurrency (:concurrency opts) 1024)
                :maintenance-batch-size
                (bounded-positive-long :maintenance-batch-size
                  (:maintenance-batch-size opts) 1000)
                :idle-min-ms
                (bounded-positive-long :idle-min-ms
                  (:idle-min-ms opts) max-worker-idle-ms)
                :idle-max-ms
                (bounded-positive-long :idle-max-ms
                  (:idle-max-ms opts) max-worker-idle-ms)
                :close-timeout-ms (positive-long :close-timeout-ms (:close-timeout-ms opts))
                :lease-extend-every-ms
                (when-some [every-ms (:lease-extend-every-ms opts)]
                  (bounded-positive-long :lease-extend-every-ms every-ms
                    max-duration-ms))
                :durability (normalize-durability :worker-create (:durability opts)))
         _ (when-let [every-ms (:lease-extend-every-ms opts)]
             (let [lease-ms (get (queue-opts queue) :lease-ms)]
               (when (and lease-ms (>= (long every-ms) (long lease-ms)))
                 (truss/ex-info!
                   "[Carmine] Lease heartbeat interval must be shorter than the queue's default lease"
                   {:eid :carmine.mq/invalid-option
                    :option :lease-extend-every-ms
                    :value every-ms, :lease-ms lease-ms}))))
         _ (when-not (boolean? (:include-msg? opts))
             (truss/ex-info! "[Carmine] Worker include-msg? must be boolean"
               {:eid :carmine.mq/invalid-option, :option :include-msg?
                :value (:include-msg? opts)}))
         _ (when-not (or (nil? (:on-event opts)) (fn? (:on-event opts)))
             (truss/ex-info! "[Carmine] Worker on-event must be a function or nil"
               {:eid :carmine.mq/invalid-option, :option :on-event
                :value (:on-event opts)}))
         _ (when (> (:idle-min-ms opts) (:idle-max-ms opts))
             (truss/ex-info! "[Carmine] Idle minimum cannot exceed maximum"
               {:eid :carmine.mq/invalid-option, :opts opts}))
         _ (assert-durability-topology! queue (:durability opts))
         _ (assert-durability-version! queue (:durability opts))]
     (Worker. (enc/uuid-str) queue handler opts (atom :new) (Object.)
       (atom nil) (atom nil) (atom nil) (atom #{}) (atom #{}) (atom 0)
       (Object.) (atom nil) (atom false) (atom false) (atom 0)
       (atom (new-worker-stats))))))

(alter-meta! #'->Worker assoc :private true)
