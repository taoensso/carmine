(ns taoensso.carmine-v4.message-queue.migration
  "Explicit v3-to-v4 queue migration utilities.

  Migration makes a non-destructive snapshot copy. It is not a live or atomic
  bridge, and there is no rolling compatibility between the queue formats. The
  v3 source and v4 target use separate keyspaces.

  Cutover procedure

    1. Pause v3 producers and workers. Drain or explicitly resolve leases and
       pending requeues, then keep the source quiescent through the copy.
    2. Run [[v3-inspect]] and review every rejected or skipped entry.
    3. Run [[v3-plan]], then [[v3-migrate!]] with `:dry-run? true`.
    4. Run the copy, start v4 consumers, switch producers, and validate
       application-level counts and behaviour.
    5. Retain the stopped v3 keyspace until rollback is no longer required.

  The utilities never delete or change v3 keys. By default, ambiguous lease or
  requeue state prevents a copy. Attempts and age restart in v4, and the target
  queue's delivery policies replace v3 worker and per-message lock settings.
  Stored payloads are decoded with the current v3 thaw options and reserialized
  with the default v4 codec, so source encryption is not retained.

  Use pure deterministic priority mapping and a deterministic, collision-free
  textual MID mapping. The target must use `:on-duplicate :reject` and
  `:revision-mode :none`. Keep every mapped MID unused and quiescent during the
  copy and any verification rerun. See each function's docstring for result
  shapes, clock requirements, quiescence checks, and rerun semantics.

  Option maps reject unknown unqualified keys and reserve namespaced keyword
  keys for extensions."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [taoensso.encore :as enc]
   [taoensso.nippy.tools :as nippy-tools]
   [taoensso.truss  :as truss]
   [taoensso.carmine :as v3]
   [taoensso.carmine.message-queue :as v3-mq]
   [taoensso.carmine-v4 :as v4]
   [taoensso.carmine-v4.message-queue :as v4-mq])
  (:import
   [java.nio.charset StandardCharsets]
   [java.util Arrays]))

(def ^:private snapshot-script
  (delay (enc/slurp-resource "taoensso/carmine/lua/mq_v4/v3-snapshot.lua")))

(def ^:private plan-option-keys #{:mid-fn :priority-fn :on-ineligible})
(def ^:private migrate-option-keys
  (into plan-option-keys #{:require-quiescence? :dry-run?}))

(defn- validate-opts! [context opts allowed]
  (let [opts (if (nil? opts) {} opts)]
    (when-not (map? opts)
      (truss/ex-info! "[Carmine] Migration options must be a map or nil"
        {:eid :carmine.mq.migration/invalid-option
         :context context, :value opts}))
    (when-let [unexpected
               (seq
                 (remove
                   #(or (contains? allowed %)
                      (and (keyword? %) (namespace %)))
                   (keys opts)))]
      (truss/ex-info! "[Carmine] Unexpected migration options"
        {:eid :carmine.mq.migration/invalid-option, :context context
         :unexpected-keys unexpected, :allowed-keys allowed}))
    opts))

(defn- validate-boolean-option! [opts option]
  (let [value (get opts option)]
    (when-not (boolean? value)
      (truss/ex-info! "[Carmine] Migration flag must be boolean"
        {:eid :carmine.mq.migration/invalid-option
         :context :v3-migrate, :option option, :value value
         :expected 'boolean}))
    value))

(defn- v3-qkey [qname suffix]
  ((deref #'v3-mq/qkey) qname suffix))

(defn- v4-queue-name [queue]
  ((deref #'v4-mq/queue-name) queue))

(defn- v4-queue-opts [queue]
  ((deref #'v4-mq/queue-opts) queue))

(defn- snapshot-v3 [conn-opts qname]
  (v3/wcar conn-opts
    ;; Raw-bulk mode applies recursively to the aggregate and survives the
    ;; EVALSHA -> EVAL fallback. Every stored field/value and set member thus
    ;; arrives as exact bytes, before marker decoding or issue-83 guessing.
    (v3/parse-raw
      (v3/lua @snapshot-script
        {:qk-messages    (v3-qkey qname :messages)
         :qk-messages-rq (v3-qkey qname :messages-rq)
         :qk-locks       (v3-qkey qname :locks)
         :qk-backoffs    (v3-qkey qname :backoffs)
         :qk-nattempts   (v3-qkey qname :nattempts)
         :qk-udts        (v3-qkey qname :udts)
         :qk-done        (v3-qkey qname :done)
         :qk-requeue     (v3-qkey qname :requeue)} {}))))

(def ^:private max-v4-timestamp-ms 99999999999999)
(def ^:private max-v4-delay-ms     10000000000000)
(def ^:private max-lua-int         9007199254740991)

(def ^:private lua-decimal-pattern
  ;; Deliberately excludes Lua's hex form, whitespace, NaN, and infinities.
  ;; V3 writes Java decimal/scientific forms and Redis Lua consumes these with
  ;; tonumber, whose relevant domain is IEEE-754 double.
  #"^[+-]?(?:(?:[0-9]+(?:\.[0-9]*)?)|(?:\.[0-9]+))(?:[eE][+-]?[0-9]+)?$")

(defn- source-str ^String [x]
  (cond
    (enc/bytes? x) (String. ^bytes x StandardCharsets/UTF_8)
    (string? x) x
    :else (str x)))

(defn- parse-source-long [x min-value max-value]
  (when (some? x)
    (try
      (let [s (source-str x)]
        (when (<= 1 (count s) 64)
          (let [n (Long/parseLong s)]
            (when (<= min-value n max-value) n))))
      (catch Exception _ nil))))

(defn- parse-source-deadline
  "Parses a finite v3 Lua timestamp and rounds up, so a fractional deadline does
  not become ready early in v4."
  [x]
  (when (some? x)
    (try
      (let [s (source-str x)]
        (when (and (<= 1 (count s) 128)
                (re-matches lua-decimal-pattern s))
          (let [n (Double/parseDouble s)]
            (when (and (not (Double/isNaN n)) (not (Double/isInfinite n))
                    (<= 0.0 n (double max-v4-timestamp-ms)))
              (long (Math/ceil n))))))
      (catch Exception _ nil))))

(defn- decode-stored-v3
  "Decodes the given v3 bytes from their Carmine marker. A binary value remains
  a byte array, including NPY-prefixed content. Nippy decoding uses nippy-tools
  and the current [[taoensso.carmine/with-thaw-opts]]."
  [^bytes stored]
  (try
    (let [n (alength stored)
          value
          (if (and (>= n 2) (zero? (aget stored 0)))
            (case (int (aget stored 1))
              95 nil
              60 (Arrays/copyOfRange stored 2 n)
              62 (nippy-tools/thaw (Arrays/copyOfRange stored 2 n))
              (throw (IllegalArgumentException.
                       "Unknown v3 stored-value marker")))
            (String. stored StandardCharsets/UTF_8))]
      {:success? true, :value value})
    (catch InterruptedException t
      (.interrupt (Thread/currentThread))
      (throw t))
    (catch Exception t
      ;; Error messages and exception objects may retain payload/config data.
      {:success? false, :error-class (.getName (class t))})))

(defn- now-ms [] (System/currentTimeMillis))

(defn- classify-entry
  [now [raw-mid raw-msg messages-rq? locks? lock-value backoffs? backoff-value
        nattempts? nattempts-value udts? udt-value done? requeue?]]
  (let [mid-result (decode-stored-v3 raw-mid)
        msg-result (decode-stored-v3 raw-msg)
        lock-expiry (when (= locks? 1) (parse-source-deadline lock-value))
        backoff-expiry
        (when (= backoffs? 1) (parse-source-deadline backoff-value))
        invalid-lock? (and (= locks? 1) (nil? lock-expiry))
        invalid-backoff?
        (and (= backoffs? 1)
          (or (nil? backoff-expiry)
            (and (> backoff-expiry now)
              (> (- backoff-expiry now) max-v4-delay-ms))))
        messages-rq-present? (= messages-rq? 1)
        legacy-requeue? (= requeue? 1)
        requeue? (or messages-rq-present? legacy-requeue?)
        done? (= done? 1)
        locked? (and lock-expiry (> lock-expiry now))
        backoff? (and (not invalid-backoff?) backoff-expiry
                   (> backoff-expiry now))
        source-roles
        (cond-> [:messages]
          messages-rq-present? (conj :messages-rq)
          (= locks?     1) (conj :locks)
          (= backoffs?  1) (conj :backoffs)
          (= nattempts? 1) (conj :nattempts)
          (= udts?      1) (conj :udts)
          done?            (conj :done)
          legacy-requeue?  (conj :requeue))
        quiescence-reasons
        (cond-> []
          messages-rq-present? (conj :pending-requeue-payload)
          (= locks? 1)         (conj :lease-record-present)
          legacy-requeue?      (conj :pending-legacy-requeue))
        state
        (cond
          done? (cond requeue? :done-with-requeue, backoff? :done-with-backoff
                  :else :done-awaiting-gc)
          locked? (if requeue? :locked-with-requeue :locked)
          backoff? :queued-with-backoff
          :else :queued)
        reason
        (cond
          requeue? :pending-requeue-payload
          (= locks? 1) :lease-record-present
          (contains? #{:done-awaiting-gc :done-with-backoff :done-with-requeue} state)
          :already-processed
          :else nil)
        disposition
        (cond
          (= reason :already-processed) :skip
          reason :reject
          (contains? #{:queued :queued-with-backoff} state) :copy
          :else :reject)
        attempts
        (when (= nattempts? 1)
          (parse-source-long nattempts-value 0 max-lua-int))
        source-udt
        (when (= udts? 1)
          (parse-source-long udt-value 0 max-v4-timestamp-ms))
        source-errors
        (cond-> []
          (not (:success? mid-result)) (conj :invalid-source-mid)
          (not (:success? msg-result)) (conj :invalid-source-msg)
          invalid-lock?                (conj :invalid-source-lock)
          invalid-backoff?             (conj :invalid-source-backoff))
        metadata-errors
        (cond-> []
          (and (= nattempts? 1) (nil? attempts))
          (conj :invalid-source-attempts)
          (and (= udts? 1) (nil? source-udt))
          (conj :invalid-source-enqueued-at))
        entry
        (cond-> {:state state, :disposition disposition
                 :attempts (or attempts 0), :source-roles source-roles}
          (:success? mid-result) (assoc :source-mid (:value mid-result))
          (:success? msg-result) (assoc :msg (:value msg-result))
          (seq quiescence-reasons)
          (assoc :quiescence-reasons quiescence-reasons)
          reason (assoc :reason reason)
          backoff? (assoc
                     :delay-ms (max 0 (- backoff-expiry now))
                     :source-backoff-expiry-ms backoff-expiry)
          (some? source-udt) (assoc :source-enqueued-at-ms source-udt)
          (seq metadata-errors) (assoc :source-metadata-errors metadata-errors)
          (not (:success? mid-result))
          (assoc :source-mid-error-class (:error-class mid-result))
          (not (:success? msg-result))
          (assoc :source-msg-error-class (:error-class msg-result)))]
    (if-let [source-error (first source-errors)]
      (assoc entry :disposition :reject, :reason source-error)
      entry)))

(def ^:private orphan-role-slots
  [[:messages-rq 1] [:locks 2] [:backoffs 3] [:nattempts 4]
   [:udts 5] [:done 6] [:requeue 7]])

(def ^:private blocking-orphan-roles #{:messages-rq :locks :requeue})

(defn- orphan-summary [orphan-records]
  (let [entries
        (mapv
          (fn [ordinal record]
            (let [roles
                  (reduce
                    (fn [acc [role slot]]
                      (if (= (nth record slot) 1) (conj acc role) acc))
                    [] orphan-role-slots)]
              ;; Exact raw MIDs are used only for Lua grouping/sorting. Public
              ;; diagnostics expose stable ordinals and roles, never MIDs or
              ;; orphan messages-rq payloads.
              {:source-ordinal ordinal, :roles roles
               :blocking? (boolean (some blocking-orphan-roles roles))}))
          (range) orphan-records)
        by-role (frequencies (mapcat :roles entries))
        blocking (count (filter :blocking? entries))]
    {:total (count entries), :blocking blocking
     :nonblocking (- (count entries) blocking)
     :by-role by-role, :entries entries}))

(defn- checked-snapshot-v3 [conn-opts qname]
  (let [reply (snapshot-v3 conn-opts qname)]
    (when-not (and (vector? reply) (= (count reply) 3)
                (= (nth reply 0) 2)
                (vector? (nth reply 1)) (vector? (nth reply 2)))
      (truss/ex-info! "[Carmine] Unexpected v3 snapshot reply"
        {:eid :carmine.mq.migration/unexpected-snapshot-reply}))
    [(nth reply 1) (nth reply 2)]))

(defn- v3-inspect*
  [conn-opts qname]
  (let [[snapshot-records orphan-records] (checked-snapshot-v3 conn-opts qname)
        now (now-ms)
        entries (->> snapshot-records
                  ;; The Lua reply is already ordered by raw serialized MID.
                  ;; Retain that content-stable order as the final tiebreaker;
                  ;; decoded JVM arrays do not provide value-stable `str` or
                  ;; equality semantics.
                  (map-indexed
                    (fn [raw-order record]
                      [raw-order (classify-entry now record)]))
                  ;; Deterministic, approximate v3 enqueue order: timestamped
                  ;; entries first (by source enqueue time, then raw serialized
                  ;; MID), then untimestamped entries by raw serialized MID.
                  (sort-by
                    (fn [[raw-order {:keys [source-enqueued-at-ms]}]]
                      [(if (some? source-enqueued-at-ms) 0 1)
                       (or source-enqueued-at-ms 0)
                       raw-order]))
                  (mapv second))
        by-disposition (frequencies (map :disposition entries))]
    {:queue (enc/as-qname qname), :inspected-at now, :total (count entries)
     :by-state (frequencies (map :state entries))
     :eligible (get by-disposition :copy 0)
     :skipped (get by-disposition :skip 0)
     :rejected (get by-disposition :reject 0)
     :orphans (orphan-summary orphan-records)
     :entries entries}))

(defn- redact-entry [entry] (dissoc entry :msg :frozen-msg))
(defn- redact-plan [plan] (update plan :entries #(mapv redact-entry %)))

(defn v3-inspect
  "Atomically snapshots a v3 queue's migration-relevant Redis structures.

  This read-only operation omits payloads from the report. One Lua call reads
  exact stored bytes, joins message roles, and reports redacted orphan roles.
  Nippy decoding uses the current [[taoensso.carmine/with-thaw-opts]], including
  encryption passwords. Lease and backoff classification compares v3 client
  times with the migration JVM time. Synchronize their clocks. A large snapshot
  can block Redis and use much memory. Keep v3 producers and workers paused
  through cutover."
  [conn-opts qname]
  (redact-plan (v3-inspect* conn-opts qname)))

(defn- target-mid [mid-fn source-mid]
  (#'v4-mq/mid-str (mid-fn source-mid)))

(defn- assert-compatible-target! [queue]
  (let [{:keys [on-duplicate revision-mode]} (v4-queue-opts queue)
        incompatible
        (cond-> {}
          (not= on-duplicate :reject)
          (assoc :on-duplicate
            {:actual on-duplicate, :required :reject})

          (not= revision-mode :none)
          (assoc :revision-mode
            {:actual revision-mode, :required :none}))]
    (when (seq incompatible)
      (truss/ex-info! "[Carmine] Migration target queue is incompatible"
        {:eid :carmine.mq.migration/incompatible-target
         :target-queue (v4-queue-name queue)
         :incompatible incompatible})))
  queue)

(defn- rejected-target-entry [entry reason t]
  (cond-> (assoc entry :disposition :reject, :reason reason)
    t (assoc :error-class (.getName (class t)))))

(defn- capture-result [f]
  (try
    {:value (f)}
    (catch InterruptedException t
      (.interrupt (Thread/currentThread))
      (throw t))
    (catch Exception t
      {:error t})))

(defn- plan-copy-entry [opts entry]
  (let [{:keys [source-mid msg delay-ms]} entry
        mid-result (capture-result #(target-mid (:mid-fn opts) source-mid))]
    (if-let [t (:error mid-result)]
      (rejected-target-entry entry :invalid-target-mid t)
      (let [mid (:value mid-result)
            priority-result
            (capture-result
              (fn []
                (let [priority ((:priority-fn opts) entry)]
                  (#'v4-mq/priority-code priority)
                  priority)))]
        (if-let [t (:error priority-result)]
          (rejected-target-entry entry :invalid-target-priority t)
          (let [msg-result
                (capture-result #(v4/freeze nil msg))]
            (if-let [t (:error msg-result)]
              (rejected-target-entry entry :invalid-target-msg t)
              (let [priority (:value priority-result)]
                (assoc entry :mid mid, :frozen-msg (:value msg-result)
                  :enqueue-opts
                  (cond-> {:mid mid, :priority priority}
                    delay-ms (assoc :delay-ms delay-ms)))))))))))

(defn- enqueue-copy-entry!
  [v4-queue {:keys [source-mid mid frozen-msg enqueue-opts
                    source-backoff-expiry-ms]}]
  ;; Planning and earlier copies may take significant time. Rebase a v3
  ;; backoff against its original absolute deadline immediately before this
  ;; enqueue so migration duration cannot extend the source delay.
  (let [enqueue-opts
        (if (some? source-backoff-expiry-ms)
          (assoc enqueue-opts :delay-ms
            (max 0 (- (long source-backoff-expiry-ms) (long (now-ms)))))
          enqueue-opts)]
    (assoc (#'v4-mq/msg-enqueue-frozen! v4-queue frozen-msg enqueue-opts)
      :source-mid source-mid, :mid mid)))

(defn- v3-plan*
  "Builds an internal v3-to-v4 copy plan without writing.

  Options are `:mid-fn` (identity), `:priority-fn` (always `:normal`), and
  `:on-ineligible` (`:skip` or `:throw`, default `:skip`). The given MID and
  priority functions must be pure and deterministic. Target MIDs must be
  textual and unique. V4 enqueue resets source attempts and age."
  ([conn-opts v3-qname v4-queue]
   (v3-plan* conn-opts v3-qname v4-queue nil))
  ([conn-opts v3-qname v4-queue opts]
   (let [opts (merge {:mid-fn identity, :priority-fn (constantly :normal)
                      :on-ineligible :skip} opts)
         _ (truss/have v4-mq/queue? v4-queue)
         _ (assert-compatible-target! v4-queue)
         _ (truss/have fn? (:mid-fn opts))
         _ (truss/have fn? (:priority-fn opts))
         _ (when-not (#{:skip :throw} (:on-ineligible opts))
             (truss/ex-info! "[Carmine] Unexpected migration policy"
               {:eid :carmine.mq.migration/invalid-option, :opts opts}))
         inspection (v3-inspect* conn-opts v3-qname)
         entries
         (mapv
           (fn [{:keys [disposition] :as entry}]
             (if (= disposition :copy)
               (plan-copy-entry opts entry)
               entry))
           (:entries inspection))
         collisions
         (->> entries
           (filter #(= (:disposition %) :copy))
           (group-by :mid)
           (keep (fn [[mid xs]] (when (> (count xs) 1) [mid (mapv :source-mid xs)])))
           (into {}))
         entries
         (mapv
           (fn [entry]
             (if-let [source-mids (get collisions (:mid entry))]
               (assoc entry :disposition :reject, :reason :target-mid-collision
                 :colliding-source-mids source-mids)
               entry))
           entries)
         rejected (filterv #(= (:disposition %) :reject) entries)]
     (when (and (= (:on-ineligible opts) :throw) (seq rejected))
       (truss/ex-info! "[Carmine] Migration plan contains ineligible work"
         {:eid :carmine.mq.migration/ineligible
          :rejected (mapv redact-entry rejected)}))
     (assoc inspection :target-queue (v4-queue-name v4-queue), :entries entries
       :eligible (count (filter #(= (:disposition %) :copy) entries))
       :skipped (count (filter #(= (:disposition %) :skip) entries))
       :rejected (count rejected), :collisions collisions))))

(defn v3-plan
  "Builds a payload-redacted v3-to-v4 plan without writing.

  Options are `:mid-fn` (identity), `:priority-fn` (always `:normal`), and
  `:on-ineligible` (`:skip` or `:throw`, default `:skip`). The given functions
  must be pure and deterministic. `:mid-fn` must return unique textual v4 MIDs.
  The default identity function rejects nontextual v3 MIDs. The target must use
  `:on-duplicate :reject` and `:revision-mode :none`. V4 enqueue resets attempts
  and age. The target delivery policy would apply. V3 worker and lock settings
  do not transfer. Nippy decoding uses the current
  [[taoensso.carmine/with-thaw-opts]]. Decoded payloads are reserialized with
  the default v4 codec; source encryption is not retained."
  ([conn-opts v3-qname v4-queue]
   (v3-plan conn-opts v3-qname v4-queue nil))
  ([conn-opts v3-qname v4-queue opts]
   (redact-plan
     (v3-plan* conn-opts v3-qname v4-queue
       (validate-opts! :v3-plan opts plan-option-keys)))))

(defn v3-migrate!
  "Non-destructively copies eligible work from a paused v3 queue into v4.

  By default, a lease or requeue ambiguity prevents the copy. This includes
  orphan `messages-rq`, `locks`, and deprecated `requeue` roles. `:dry-run?`
  returns the plan without writing. It and `:require-quiescence?` must be
  actual booleans. The given `:mid-fn` and `:priority-fn` must be pure and
  deterministic. `:mid-fn` must return unique textual v4 MIDs. The default
  identity function rejects nontextual MIDs. The target must use
  `:on-duplicate :reject` and `:revision-mode :none`.

  The target may contain other work, but no producer, consumer, or
  administrative action may touch a mapped MID during the copy or any rerun.
  A mapped MID should be absent or be an unchanged active generation from the
  immediately prior deterministic copy. The same stored payload returns
  `:existing` and keeps its schedule, priority, and options. This proves neither
  its origin nor whether this copy's priority or rebased backoff was applied. If
  that generation is terminally settled, removed, or cleared, a rerun adds new
  deliverable work.

  Attempts and age reset. Target `:lease-ms`, `:max-attempts`, `:retry-base-ms`,
  `:retry-max-ms`, `:retry-jitter`, and `:on-exhaustion` policies apply. V3
  worker settings and per-message lock times (`lock-ms` and `lock-times`) do not
  transfer. Carmine rebases only a stored source backoff deadline. Synchronize
  the migration host clock with the paused v3 clients.

  This function does not change v3 keys. Nippy decoding uses the current
  [[taoensso.carmine/with-thaw-opts]]. Payloads are reserialized with the
  default v4 codec; source encryption is not retained.

  Eligible entries use approximate v3 enqueue order. Strict v3 FIFO order is
  not preserved."
  ([conn-opts v3-qname v4-queue]
   (v3-migrate! conn-opts v3-qname v4-queue nil))
  ([conn-opts v3-qname v4-queue opts]
   (let [opts (validate-opts! :v3-migrate opts migrate-option-keys)
         opts (merge {:require-quiescence? true, :dry-run? false} opts)
         _ (validate-boolean-option! opts :require-quiescence?)
         _ (validate-boolean-option! opts :dry-run?)
         plan (v3-plan* conn-opts v3-qname v4-queue opts)
         ambiguous
         (filterv #(seq (:quiescence-reasons %)) (:entries plan))
         blocking-orphans
         (filterv :blocking? (get-in plan [:orphans :entries]))]
     (when (and (:require-quiescence? opts)
             (or (seq ambiguous) (seq blocking-orphans)))
       (truss/ex-info! "[Carmine] V3 queue is not quiescent"
         {:eid :carmine.mq.migration/not-quiescent
          :entries (mapv redact-entry ambiguous)
          :orphans blocking-orphans
          :orphan-counts (dissoc (:orphans plan) :entries)}))
     (if (:dry-run? opts)
       (assoc (redact-plan plan) :dry-run? true)
       (let [results
             (mapv
               #(enqueue-copy-entry! v4-queue %)
               (filter #(= (:disposition %) :copy) (:entries plan)))
             by-action (frequencies (map #(or (:action %) (:error %)) results))]
         {:source-queue (:queue plan), :target-queue (v4-queue-name v4-queue)
          :added (get by-action :added 0), :existing (get by-action :existing 0)
          :orphans (:orphans plan)
          :conflicts (filterv #(or (false? (:success? %))
                                 (not (contains? #{:added :existing} (:action %))))
                       results)
          :skipped (mapv redact-entry
                     (filter #(= (:disposition %) :skip) (:entries plan)))
          :rejected (mapv redact-entry
                      (filter #(= (:disposition %) :reject) (:entries plan)))
          :results results})))))
