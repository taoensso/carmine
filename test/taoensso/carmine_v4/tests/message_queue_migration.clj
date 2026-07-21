(ns taoensso.carmine-v4.tests.message-queue-migration
  (:require
   [clojure.test :refer [deftest is testing]]
   [taoensso.encore :as enc]
   [taoensso.nippy :as nippy]
   [taoensso.nippy.tools :as nippy-tools]
   [taoensso.truss :as truss :refer [throws?]]
   [taoensso.carmine :as v3]
   [taoensso.carmine.message-queue :as v3-mq]
   [taoensso.carmine-v4 :as car]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.message-queue :as mq]
   [taoensso.carmine-v4.message-queue.migration :as migration])
  (:import [taoensso.carmine_v4.resp.write WriteFrozen]))

(deftest _public-api-contract
  (let [publics (ns-publics 'taoensso.carmine-v4.message-queue.migration)
        actual-arglists
        (into {} (map (fn [[sym v]] [sym (:arglists (meta v))])) publics)
        expected-arglists
        '{v3-inspect ([conn-opts qname])
          v3-migrate!
          ([conn-opts v3-qname v4-queue]
           [conn-opts v3-qname v4-queue opts])
          v3-plan
          ([conn-opts v3-qname v4-queue]
           [conn-opts v3-qname v4-queue opts])}]
    [(is (= actual-arglists expected-arglists)
       "The intentional MQ migration symbols and call shapes change explicitly")
     (is (every? (comp not-empty :doc meta val) publics))
     (is (every? (comp seq :arglists meta val) publics))]))

(def v3-conn-opts {})
(defonce mgr_ (delay (conns/conn-manager-pooled {})))
(defn qname [prefix] (str prefix "-" (java.util.UUID/randomUUID)))
(defn- v3-key [queue suffix] ((deref #'v3-mq/qkey) queue suffix))

(defn- binary-mid [tag]
  (byte-array [0 tag -1 78 80 89 13]))

(defn- source-mid-label [source-mid]
  (cond
    (enc/bytes? source-mid)
    [:bytes (mapv #(bit-and 0xff (int %)) source-mid)]

    (string? source-mid) [:string source-mid]
    (keyword? source-mid) [:keyword source-mid]
    (symbol? source-mid) [:symbol (str source-mid)]
    (vector? source-mid) [:vector source-mid]
    :else [:other (pr-str source-mid)]))

(defn- same-source-mid? [x y]
  (if (and (enc/bytes? x) (enc/bytes? y))
    (enc/ba= x y)
    (= x y)))

(defn- entry-for [entries source-mid]
  (some #(when (same-source-mid? (:source-mid %) source-mid) %) entries))

(defn- utf8-bytes [x] (.getBytes (str x) "UTF-8"))

(defn- marked-bytes [marker ^bytes payload]
  (let [out (byte-array (+ 2 (alength payload)))]
    (aset-byte out 0 (byte 0))
    (aset-byte out 1 (byte marker))
    (System/arraycopy payload 0 out 2 (alength payload))
    out))

(defn- stored-nippy [x]
  (marked-bytes 62 (nippy/freeze x)))

(deftest _migration-option-validation
  [(is (throws? :ex-info {:eid :carmine.mq.migration/invalid-option}
         (migration/v3-plan nil nil nil {:priority-fnn identity})))
   (is (throws? :ex-info {:eid :carmine.mq.migration/invalid-option}
         (migration/v3-plan nil nil nil {:id-fn identity})))
   (is (throws? :ex-info {:eid :carmine.mq.migration/invalid-option}
         (migration/v3-migrate! nil nil nil {:dry-run false})))
   (doseq [[option value]
           [[:dry-run? nil] [:dry-run? 0] [:dry-run? :yes]
            [:require-quiescence? nil] [:require-quiescence? 1]
            [:require-quiescence? "false"]]]
     (is (throws? :ex-info
           {:eid :carmine.mq.migration/invalid-option
            :option option, :value value}
           (migration/v3-migrate! nil nil nil {option value}))))
   (is (throws? :ex-info {:eid :carmine.mq.migration/invalid-option}
         (migration/v3-migrate! nil nil nil false)))])

(deftest _migration-rebases-source-backoff-deadline
  (let [now_ (atom 1200)
        seen-opts_ (atom [])
        entry
        {:source-mid "delayed", :mid "delayed", :frozen-msg :frozen
         :source-backoff-expiry-ms 1500
         :enqueue-opts {:mid "delayed", :priority :normal, :delay-ms 500}}]
    (with-redefs-fn
      {#'migration/now-ms #(deref now_)
       #'mq/msg-enqueue-frozen!
       (fn [_queue _frozen-msg opts]
         (swap! seen-opts_ conj opts)
         {:success? true, :action :added})}
      (fn []
        (let [before-expiry (#'migration/enqueue-copy-entry! :queue entry)
              _ (reset! now_ 1700)
              after-expiry (#'migration/enqueue-copy-entry! :queue entry)]
          [(is (= (mapv :delay-ms @seen-opts_) [300 0])
             "Each copy is rebased against the original source deadline")
           (is (= (mapv #(select-keys % [:mid :priority]) @seen-opts_)
                 [{:mid "delayed", :priority :normal}
                  {:mid "delayed", :priority :normal}])
             "Rebasing preserves the remaining enqueue options")
           (is (= (select-keys before-expiry [:source-mid :mid])
                 {:source-mid "delayed", :mid "delayed"}))
           (is (= (select-keys after-expiry [:source-mid :mid])
                 {:source-mid "delayed", :mid "delayed"}))])))))

(deftest _migration-preflight-propagates-interruption-and-fatal-errors
  (let [entry {:source-mid "source", :msg :msg, :disposition :copy}
        opts  {:mid-fn identity, :priority-fn (constantly :normal)}]
    (testing "Interrupted callbacks preserve cancellation"
      (let [expected (InterruptedException. "stop")]
        (try
          (let [{:keys [actual interrupted?]}
                (try
                  (#'migration/plan-copy-entry
                    (assoc opts :mid-fn (fn [_] (throw expected))) entry)
                  nil
                  (catch Throwable t
                    {:actual t
                     :interrupted? (.isInterrupted (Thread/currentThread))}))]
            [(is (identical? actual expected))
             (is interrupted?)])
          (finally
            (Thread/interrupted)))))

    (testing "JVM-fatal callback errors are never classified as bad input"
      (let [expected (LinkageError. "fatal")
            actual
            (try
              (#'migration/plan-copy-entry
                (assoc opts :priority-fn (fn [_] (throw expected))) entry)
              nil
              (catch Throwable t t))]
        (is (identical? actual expected))))

    (testing "Payload serialization errors use msg vocabulary"
      (with-redefs [car/freeze
                    (fn [_freeze-opts _msg]
                      (throw (Exception. "invalid payload")))]
        (is (= (:reason (#'migration/plan-copy-entry opts entry))
              :invalid-target-msg))))

    (testing "Migration planning explicitly selects the default MQ codec"
      (let [calls_ (atom [])]
        (with-redefs [car/freeze
                      (fn [freeze-opts msg]
                        (swap! calls_ conj [freeze-opts msg])
                        ::frozen)]
          (let [planned (#'migration/plan-copy-entry opts entry)]
            [(is (= @calls_ [[nil :msg]]))
             (is (= (:frozen-msg planned) ::frozen))]))))

    (testing "Migration planning normalizes a logical bytes wrapper"
      (let [ba (byte-array [0 60 78 80 89 0 13 -1 127 -128])
            planned (#'migration/plan-copy-entry opts
                      (assoc entry :msg (car/bytes ba)))
            ^WriteFrozen frozen (:frozen-msg planned)
            roundtrip (nippy/thaw (.-?frozen-ba frozen))]
        [(is (enc/bytes? roundtrip))
         (is (enc/ba= roundtrip ba)
           "The migration helper plans the byte array, not WriteBytes")]))

    (testing "Migrated MIDs share the public leading-NUL reservation"
      (is (= (select-keys
               (#'migration/plan-copy-entry
                 (assoc opts :mid-fn (constantly "\u0000reserved")) entry)
               [:disposition :reason])
            {:disposition :reject, :reason :invalid-target-mid})))))

(deftest _migration-decodes-exact-raw-source-payloads
  (let [source (qname "carmine-v3-migration-raw-payload")
        target (mq/queue mgr_ (qname "carmine-v4-migration-raw-payload"))
        password [:salted "migration-source-codec-secret-74f3"]
        wrong-password [:salted "migration-source-codec-wrong-09b1"]
        encrypted-msg {:kind :encrypted, :value [1 2 3]}
        legitimate-ex (ex-info "legitimate payload" {:kind :payload-ex})
        npy-bytes (byte-array [78 80 89 0 13 -1 127 -128])
        damaged-stored
        (marked-bytes 62 (byte-array [78 80 89 0 1 2 3 4 5]))]
    (try
      (v3/wcar v3-conn-opts
        (v3-mq/enqueue source legitimate-ex {:mid "legitimate-ex"})
        (v3-mq/enqueue source npy-bytes {:mid "npy-bytes"})
        (v3-mq/enqueue source :will-be-damaged {:mid "damaged"}))
      (binding [nippy-tools/*freeze-opts* {:password password}]
        (v3/wcar v3-conn-opts
          (v3-mq/enqueue source encrypted-msg {:mid "encrypted"})))
      (v3/wcar v3-conn-opts
        (v3/hset (v3-key source :messages) "damaged" (v3/raw damaged-stored)))

      (testing "Thaw failure is data, not a nested exception payload"
        (let [inspection (#'migration/v3-inspect* v3-conn-opts source)
              public-inspection (migration/v3-inspect v3-conn-opts source)
              by-mid (into {} (keep #(when-let [mid (:source-mid %)] [mid %]))
                      (:entries inspection))
              encrypted (get by-mid "encrypted")
              damaged (get by-mid "damaged")
              legitimate (get by-mid "legitimate-ex")
              raw-bytes (get by-mid "npy-bytes")]
          [(is (= (select-keys encrypted [:disposition :reason])
                 {:disposition :reject, :reason :invalid-source-msg})
             "A missing source password is an explicit decode rejection")
           (is (= (select-keys damaged [:disposition :reason])
                 {:disposition :reject, :reason :invalid-source-msg}))
           (is (not (contains? encrypted :msg)))
           (is (not (contains? damaged :msg)))
           (is (instance? clojure.lang.ExceptionInfo (:msg legitimate))
             "A successfully decoded ExceptionInfo remains a payload value")
           (is (= (ex-data (:msg legitimate)) {:kind :payload-ex}))
           (is (enc/ba= (:msg raw-bytes) npy-bytes)
             "A logical NPY-prefixed byte[] payload remains exact")
           (is (every? #(not (contains? % :msg)) (:entries public-inspection)))
           (is (not (.contains (pr-str public-inspection) (second password)))
             "Codec failures never retain passwords")]))

      (testing "Wrong and correct ambient v3 thaw options are unambiguous"
        (let [missing-plan
              (migration/v3-plan v3-conn-opts source target)
              wrong
              (v3/with-thaw-opts {:password wrong-password}
                (migration/v3-inspect v3-conn-opts source))
              wrong-dry-run
              (v3/with-thaw-opts {:password wrong-password}
                (migration/v3-migrate! v3-conn-opts source target
                  {:dry-run? true}))
              correct
              (v3/with-thaw-opts {:password password}
                (#'migration/v3-inspect* v3-conn-opts source))
              encrypted (entry-for (:entries correct) "encrypted")]
          [(is (= (:reason (entry-for (:entries missing-plan) "encrypted"))
                 :invalid-source-msg))
           (is (= (:reason (entry-for (:entries wrong) "encrypted"))
                 :invalid-source-msg))
           (is (= (:reason (entry-for (:entries wrong-dry-run) "encrypted"))
                 :invalid-source-msg)
             "Inspect, plan, and migration dry-run classify the same failure")
           (is (true? (:dry-run? wrong-dry-run)))
           (is (zero? (get-in (mq/queue-status target) [:counts :active])))
           (is (= (:msg encrypted) encrypted-msg))
           (is (= (:disposition encrypted) :copy))
           (is (= (:reason (entry-for (:entries correct) "damaged"))
                 :invalid-source-msg)
             "Correct credentials do not hide independent corruption")]))

      (testing "Planning and migration use the same successful decode results"
        (let [plan
              (v3/with-thaw-opts {:password password}
                (migration/v3-plan v3-conn-opts source target))
              result
              (v3/with-thaw-opts {:password password}
                (migration/v3-migrate! v3-conn-opts source target))
              claimed
              (loop [remaining 3, out {}]
                (if (zero? remaining)
                  out
                  (let [{:keys [mid msg token]} (#'mq/claim! target 64)]
                    (#'mq/settle! target mid token (mq/outcome:ack))
                    (recur (dec remaining) (assoc out mid msg)))))]
          [(is (truss/submap? plan {:eligible 3, :rejected 1}))
           (is (every? #(not (contains? % :msg)) (:entries plan)))
           (is (truss/submap? result {:added 3, :existing 0}))
           (is (= (mapv :reason (:rejected result)) [:invalid-source-msg]))
           (is (= (get claimed "encrypted") encrypted-msg))
           (is (= (ex-data (get claimed "legitimate-ex")) {:kind :payload-ex}))
           (is (enc/ba= (get claimed "npy-bytes") npy-bytes))]))
      (finally
        (v3-mq/queues-clear!! v3-conn-opts [source])
        (mq/queue-clear!! target)))))

(deftest _migration-preserves-npy-prefixed-binary-mid-identity
  (let [source (qname "carmine-v3-migration-npy-mid")
        target (mq/queue mgr_ (qname "carmine-v4-migration-npy-mid"))
        role-mid (nippy/freeze :looks-like-serialized-keyword)
        copy-mid-1 (nippy/freeze :copy-one)
        copy-mid-2 (nippy/freeze :copy-two)
        ambiguous-mid (byte-array [0 33 1 2 3])
        now (System/currentTimeMillis)]
    (try
      (v3/wcar v3-conn-opts
        (v3-mq/enqueue source :role-msg {:mid role-mid})
        (v3-mq/enqueue source :copy-1 {:mid copy-mid-1})
        (v3-mq/enqueue source :copy-2 {:mid copy-mid-2})
        (v3/hset (v3-key source :messages) (v3/raw ambiguous-mid)
          (v3/freeze :ambiguous-mid-payload))
        ;; Fresh byte arrays are intentionally supplied for every role. Lua
        ;; must correlate their exact stored fields before JVM decoding.
        (v3/hset (v3-key source :messages-rq) (aclone role-mid) :newer)
        (v3/hset (v3-key source :locks) (aclone role-mid) (+ now 60000))
        (v3/hset (v3-key source :backoffs) (aclone role-mid) (+ now 50000))
        (v3/hset (v3-key source :nattempts) (aclone role-mid) 7)
        (v3/hset (v3-key source :udts) (aclone role-mid) (- now 1000))
        (v3/sadd (v3-key source :done) (aclone role-mid))
        (v3/sadd (v3-key source :requeue) (aclone role-mid)))

      (let [inspection (migration/v3-inspect v3-conn-opts source)
            role-entry (entry-for (:entries inspection) role-mid)
            ambiguous-entry
            (some #(when (= (:reason %) :invalid-source-mid) %)
              (:entries inspection))
            default-plan (migration/v3-plan v3-conn-opts source target)
            seen_ (atom [])
            unique-plan
            (migration/v3-plan v3-conn-opts source target
              {:mid-fn
               (fn [source-mid]
                 (swap! seen_ conj source-mid)
                 (str "npy-mid-" (count @seen_)))})
            collision-plan
            (migration/v3-plan v3-conn-opts source target
              {:mid-fn (constantly "same-target")})]
        [(is (enc/ba= (:source-mid role-entry) role-mid))
         (is (= (:source-roles role-entry)
               [:messages :messages-rq :locks :backoffs :nattempts
                :udts :done :requeue])
           "Every role joins against the exact NPY-prefixed binary field")
         (is (= (:reason role-entry) :pending-requeue-payload))
         (is (every? enc/bytes?
               (keep #(when (contains? % :source-mid) (:source-mid %))
                 (:entries inspection)))
           "Known binary markers are never issue-83 guessed into keywords")
         (is (truss/submap? ambiguous-entry
               {:disposition :reject, :reason :invalid-source-mid})
           "A genuinely unknown legacy marker rejects instead of changing identity")
         (is (not (contains? ambiguous-entry :source-mid)))
         (is (= (set (map :reason
                       (filter #(= (:disposition %) :reject)
                         (:entries default-plan))))
               #{:invalid-target-mid :invalid-source-mid
                 :pending-requeue-payload}))
         (is (every? enc/bytes? @seen_)
           "A custom mid-fn receives the original binary identity")
         (is (truss/submap? unique-plan {:eligible 2, :rejected 2}))
         (is (= (count (get (:collisions collision-plan) "same-target")) 2))
         (is (every? enc/bytes?
               (get (:collisions collision-plan) "same-target")))])
      (finally
        (v3-mq/queues-clear!! v3-conn-opts [source])
        (mq/queue-clear!! target)))))

(deftest _migration-parses-fractional-v3-deadlines-conservatively
  (let [record
        (fn [mid lock-value backoff-value]
          [(utf8-bytes mid) (stored-nippy {:mid mid})
           0
           (if (some? lock-value) 1 0) (some-> lock-value utf8-bytes)
           (if (some? backoff-value) 1 0) (some-> backoff-value utf8-bytes)
           0 nil, 0 nil, 0, 0])
        classify
        (fn [now mid lock-value backoff-value]
          (#'migration/classify-entry now
            (record mid lock-value backoff-value)))]
    (testing "Finite decimal/exponent values use ceiling semantics"
      (let [now 2000
            fractional (classify now "fractional" nil "2000.01")
            exponent (classify now "exponent" nil "2.00001e3")
            exact (classify now "exact" nil "2001")
            past (classify now "past" nil "1999.75")
            fractional-lock (classify now "lock" "2000.01" nil)]
        [(is (= (#'migration/parse-source-deadline (utf8-bytes "1234.01"))
               1235))
         (is (= (#'migration/parse-source-deadline (utf8-bytes "1.23401e3"))
               1235))
         (is (truss/submap? fractional
               {:state :queued-with-backoff, :delay-ms 1
                :source-backoff-expiry-ms 2001}))
         (is (= (select-keys exponent
                  [:state :delay-ms :source-backoff-expiry-ms])
               (select-keys fractional
                  [:state :delay-ms :source-backoff-expiry-ms])))
         (is (= (select-keys exact
                  [:state :delay-ms :source-backoff-expiry-ms])
               (select-keys fractional
                  [:state :delay-ms :source-backoff-expiry-ms])))
         (is (= (select-keys past [:state :disposition])
               {:state :queued, :disposition :copy})
           "A past fractional deadline rounds to now, never into the future")
         (is (not (contains? past :delay-ms)))
         (is (truss/submap? fractional-lock
               {:state :locked, :disposition :reject
                :reason :lease-record-present})
           "Fractional active leases are not mistaken for unlocked work")]))

    (testing "Malformed, non-finite, negative, and out-of-range values reject"
      (doseq [value ["NaN" "Inf" "+Inf" "-1" "oops" "1e999"
                     "100000000000000" " 2001" "0x7d1"]]
        (let [entry (classify 2000 "bad" nil value)]
          [(is (= (select-keys entry [:disposition :reason])
                 {:disposition :reject, :reason :invalid-source-backoff})
             value)
           (is (not (.contains (pr-str entry) value))
             "Invalid raw numeric content is not retained in diagnostics")]))
      (let [too-long (classify 0 "too-long" nil "10000000000001")
            bad-lock (classify 2000 "bad-lock" "NaN" nil)
            bad-udt
            (#'migration/classify-entry 2000
              (assoc (record "bad-udt" nil nil)
                9 1, 10 (utf8-bytes "NaN")))]
        [(is (= (:reason too-long) :invalid-source-backoff)
           "A valid timestamp outside v4's delay bound is preflight-rejected")
         (is (truss/submap? bad-lock
               {:disposition :reject, :reason :invalid-source-lock
                :quiescence-reasons [:lease-record-present]}))
         (is (truss/submap? bad-udt
               {:disposition :copy
                :source-metadata-errors [:invalid-source-enqueued-at]})
           "Malformed ordering-only UDT metadata is reported but cannot invent work")
         (is (not (contains? bad-udt :source-enqueued-at-ms)))]))

    (testing "The public v3 fractional option remains scheduled after copy"
      (let [source (qname "carmine-v3-migration-fractional-public")
            target (mq/queue mgr_
                     (qname "carmine-v4-migration-fractional-public"))]
        (try
          (v3/wcar v3-conn-opts
            (v3-mq/enqueue source {:kind :fractional}
              {:mid "fractional", :init-backoff-ms 120000.25}))
          (let [inspection (migration/v3-inspect v3-conn-opts source)
                entry (entry-for (:entries inspection) "fractional")
                result (migration/v3-migrate! v3-conn-opts source target)]
            [(is (truss/submap? entry
                   {:state :queued-with-backoff, :disposition :copy}))
             (is (pos? (:delay-ms entry)))
             (is (integer? (:source-backoff-expiry-ms entry)))
             (is (truss/submap? result {:added 1, :existing 0}))
             (is (= (mq/msg-status target "fractional") :scheduled)
               "Ceiling conversion cannot make the copied message ready early")])
          (finally
            (v3-mq/queues-clear!! v3-conn-opts [source])
            (mq/queue-clear!! target)))))))

(deftest _migration-reports-unmatched-source-roles
  (let [source (qname "carmine-v3-migration-orphans")
        target (mq/queue mgr_ (qname "carmine-v4-migration-orphans"))
        secret "orphan-message-payload-secret-15c9"
        mixed-mid (nippy/freeze :npy-looking-orphan)
        now (System/currentTimeMillis)]
    (try
      (v3/wcar v3-conn-opts
        (v3-mq/enqueue source {:kind :base-work} {:mid "base"})
        (v3/hset (v3-key source :messages-rq) "orphan-rq"
          (v3/freeze {:secret secret}))
        (v3/hset (v3-key source :locks) "orphan-lock" (+ now 60000))
        (v3/sadd (v3-key source :requeue) "orphan-requeue")
        (v3/hset (v3-key source :backoffs) "orphan-backoff" (+ now 50000))
        (v3/hset (v3-key source :nattempts) "orphan-attempts" 4)
        (v3/hset (v3-key source :udts) "orphan-udt" (- now 1000))
        (v3/sadd (v3-key source :done) "orphan-done")
        ;; One exact raw binary identity occupies multiple unmatched roles.
        (v3/hset (v3-key source :messages-rq) (aclone mixed-mid) :mixed)
        (v3/hset (v3-key source :backoffs) (aclone mixed-mid) (+ now 40000))
        (v3/sadd (v3-key source :done) (aclone mixed-mid)))

      (testing "Inspection and planning expose bounded, redacted role diagnostics"
        (let [inspection-1 (migration/v3-inspect v3-conn-opts source)
              inspection-2 (migration/v3-inspect v3-conn-opts source)
              plan (migration/v3-plan v3-conn-opts source target)
              summary (:orphans inspection-1)
              mixed (some #(when (= (:roles %)
                                   [:messages-rq :backoffs :done]) %)
                      (:entries summary))]
          [(is (= (dissoc summary :entries)
                 {:total 8, :blocking 4, :nonblocking 4
                  :by-role {:messages-rq 2, :locks 1, :backoffs 2
                            :nattempts 1, :udts 1, :done 2, :requeue 1}}))
           (is (true? (:blocking? mixed)))
           (is (= (:orphans inspection-1) (:orphans inspection-2))
             "Raw sorting gives repeated snapshots stable redacted ordinals")
           (is (= (:orphans plan) (:orphans inspection-1)))
           (is (every? #(and (contains? % :source-ordinal)
                          (not (contains? % :source-mid))
                          (not (contains? % :msg)))
                 (get-in inspection-1 [:orphans :entries])))
           (is (not (.contains (pr-str inspection-1) secret))
             "The unmatched messages-rq payload is never returned")]))

      (testing "Pending-work/lease orphans refuse default migration before writes"
        (let [error (truss/throws :ex-info
                      (migration/v3-migrate! v3-conn-opts source target))
              data (ex-data error)]
          [(is (= (:eid data) :carmine.mq.migration/not-quiescent))
           (is (empty? (:entries data)))
           (is (= (count (:orphans data)) 4))
           (is (every? :blocking? (:orphans data)))
           (is (= (:orphan-counts data)
                 {:total 8, :blocking 4, :nonblocking 4
                  :by-role {:messages-rq 2, :locks 1, :backoffs 2
                            :nattempts 1, :udts 1, :done 2, :requeue 1}}))
           (is (zero? (get-in (mq/queue-status target) [:counts :active]))
             "Quiescence preflight performs no target write")]))

      (testing "Explicit override copies only real base work and retains diagnostics"
        (let [result (migration/v3-migrate! v3-conn-opts source target
                       {:require-quiescence? false})
              {:keys [mid msg token]} (#'mq/claim! target 64)]
          [(is (truss/submap? result {:added 1, :existing 0}))
           (is (= (select-keys (:orphans result)
                    [:total :blocking :nonblocking])
                 {:total 8, :blocking 4, :nonblocking 4}))
           (is (= mid "base"))
           (is (= msg {:kind :base-work}))
           (#'mq/settle! target mid token (mq/outcome:ack))]))
      (finally
        (v3-mq/queues-clear!! v3-conn-opts [source])
        (mq/queue-clear!! target)))))

(deftest _migration-correlates-binary-v3-message-mid-roles
  (let [source (qname "carmine-v3-migration-binary-roles")
        target (mq/queue mgr_ (qname "carmine-v4-migration-binary-roles")
                 {:retry-base-ms 0, :retry-max-ms 0})
        live-mid (binary-mid 1)
        requeue-mid (binary-mid 2)
        done-mid (binary-mid 3)
        stats-mid (binary-mid 4)
        copy-mid (binary-mid 5)
        legacy-requeue-mid (binary-mid 6)
        now (System/currentTimeMillis)
        lock-expiry (+ now 120000)
        backoff-expiry (+ now 90000)
        stats-udt (- now 6000)
        copy-udt (- now 5000)
        live-udt (- now 4000)
        requeue-udt (- now 3000)
        legacy-requeue-udt (- now 2000)
        done-udt (- now 1000)
        seen-source-mids_ (atom [])
        mid-fn
        (fn [source-mid]
          (swap! seen-source-mids_ conj source-mid)
          (if (enc/bytes? source-mid)
            (str "binary-" (bit-and 0xff (aget ^bytes source-mid 1)))
            source-mid))]
    (try
      ;; Seed through the public v3 enqueue API, using fresh JVM arrays for
      ;; every role mutation. Equal Redis fields must correlate by content,
      ;; never by Java array identity.
      (v3/wcar v3-conn-opts
        (v3-mq/enqueue source {:kind :live-base} {:mid (binary-mid 1)})
        (v3-mq/enqueue source {:kind :stale-base} {:mid (binary-mid 2)})
        (v3-mq/enqueue source {:kind :done-base} {:mid (binary-mid 3)})
        (v3-mq/enqueue source {:kind :stats-base} {:mid (binary-mid 4)})
        (v3-mq/enqueue source {:kind :copy-base} {:mid (binary-mid 5)})
        (v3-mq/enqueue source {:kind :legacy-requeue-base} {:mid (binary-mid 6)})

        (v3/hset (v3-key source :locks) (binary-mid 1) lock-expiry)
        (v3/hset (v3-key source :locks) (binary-mid 2) lock-expiry)
        (v3-mq/enqueue source {:kind :newer-requeue}
          {:mid (binary-mid 2), :can-requeue? true, :can-update? true})
        (v3/sadd (v3-key source :done) (binary-mid 3))
        (v3/sadd (v3-key source :requeue) (binary-mid 6))
        (v3/hset (v3-key source :backoffs) (binary-mid 4) backoff-expiry)
        (v3/hset (v3-key source :nattempts) (binary-mid 4) 7)
        (v3/hset (v3-key source :udts) (binary-mid 4) stats-udt)
        (v3/hset (v3-key source :udts) (binary-mid 5) copy-udt)
        (v3/hset (v3-key source :udts) (binary-mid 1) live-udt)
        (v3/hset (v3-key source :udts) (binary-mid 2) requeue-udt)
        (v3/hset (v3-key source :udts) (binary-mid 6) legacy-requeue-udt)
        (v3/hset (v3-key source :udts) (binary-mid 3) done-udt))

      (testing "Inspection joins every binary-MID role before RESP thaw"
        (let [inspection (migration/v3-inspect v3-conn-opts source)
              entries (:entries inspection)
              live (entry-for entries live-mid)
              requeue (entry-for entries requeue-mid)
              done (entry-for entries done-mid)
              stats (entry-for entries stats-mid)
              copy (entry-for entries copy-mid)
              legacy-requeue (entry-for entries legacy-requeue-mid)]
          [(is (truss/submap? inspection
                 {:total 6, :eligible 2, :skipped 1, :rejected 3}))
           (is (= (mapv (comp source-mid-label :source-mid) entries)
                 (mapv source-mid-label
                   [stats-mid copy-mid live-mid requeue-mid
                    legacy-requeue-mid done-mid]))
             "Joined UDTs still determine approximate enqueue order")
           (is (truss/submap? live
                 {:state :locked, :disposition :reject
                  :reason :lease-record-present, :attempts 0
                  :source-enqueued-at-ms live-udt}))
           (is (truss/submap? requeue
                 {:state :locked-with-requeue, :disposition :reject
                  :reason :pending-requeue-payload, :attempts 0
                  :source-enqueued-at-ms requeue-udt}))
           (is (truss/submap? legacy-requeue
                 {:state :queued, :disposition :reject
                  :reason :pending-requeue-payload, :attempts 0
                  :source-enqueued-at-ms legacy-requeue-udt})
             "The deprecated v3 requeue set also correlates by raw MID")
           (is (truss/submap? done
                 {:state :done-awaiting-gc, :disposition :skip
                  :reason :already-processed, :attempts 0
                  :source-enqueued-at-ms done-udt}))
           (is (truss/submap? stats
                 {:state :queued-with-backoff, :disposition :copy
                  :attempts 7, :source-enqueued-at-ms stats-udt
                  :source-backoff-expiry-ms backoff-expiry}))
           (is (= (:delay-ms stats)
                 (- backoff-expiry (:inspected-at inspection))))
           (is (= (- (:inspected-at inspection)
                    (:source-enqueued-at-ms stats))
                 (- (:inspected-at inspection) stats-udt))
             "The reported source timestamp retains the exact source age")
           (is (truss/submap? copy
                 {:state :queued, :disposition :copy, :attempts 0
                  :source-enqueued-at-ms copy-udt}))
           (is (= (v3/wcar v3-conn-opts
                    (v3/hget (v3-key source :messages-rq) (binary-mid 2)))
                 {:kind :newer-requeue})
             "The rejected pending requeue really differs from the stale base payload")]))

      (testing "Planning retains state and validates decoded binary MIDs"
        (let [default-plan (migration/v3-plan v3-conn-opts source target)
              copy (entry-for (:entries default-plan) copy-mid)
              custom-plan (migration/v3-plan v3-conn-opts source target
                            {:mid-fn mid-fn})
              live (entry-for (:entries custom-plan) live-mid)
              requeue (entry-for (:entries custom-plan) requeue-mid)
              legacy-requeue
              (entry-for (:entries custom-plan) legacy-requeue-mid)]
          [(is (= (select-keys copy [:disposition :reason])
                 {:disposition :reject, :reason :invalid-target-mid})
             "The default identity mapping safely rejects a non-textual v4 MID")
           (is (truss/submap? live
                 {:state :locked, :reason :lease-record-present}))
           (is (truss/submap? requeue
                 {:state :locked-with-requeue
                  :reason :pending-requeue-payload}))
           (is (truss/submap? legacy-requeue
                 {:state :queued, :reason :pending-requeue-payload}))
           (is (= (:eligible custom-plan) 2))
           (is (every? enc/bytes? @seen-source-mids_)
             "The custom mid-fn receives decoded v3 byte arrays")
           (is (= (set (map source-mid-label @seen-source-mids_))
                 #{(source-mid-label stats-mid) (source-mid-label copy-mid)}))]))

      (testing "Default quiescence sees binary leases/requeues and writes nothing"
        (let [error (truss/throws :ex-info
                      (migration/v3-migrate! v3-conn-opts source target))
              ambiguous-labels
              (set (map (comp source-mid-label :source-mid)
                     (:entries (ex-data error))))]
          [(is (= (:eid (ex-data error))
                 :carmine.mq.migration/not-quiescent))
           (is (= ambiguous-labels
                 #{(source-mid-label live-mid) (source-mid-label requeue-mid)
                   (source-mid-label legacy-requeue-mid)}))
           (is (zero? (get-in (mq/queue-status target) [:counts :active]))
             "Quiescence rejection leaves the target untouched")]))

      (testing "A custom mapping copies only eligible binary-MID work exactly once"
        (let [first-run
              (migration/v3-migrate! v3-conn-opts source target
                {:mid-fn mid-fn, :require-quiescence? false})
              copy-info (mq/msg-info target "binary-5")
              second-run
              (migration/v3-migrate! v3-conn-opts source target
                {:mid-fn mid-fn, :require-quiescence? false})]
          [(is (truss/submap? first-run {:added 2, :existing 0}))
           (is (= (mapv (comp source-mid-label :source-mid) (:skipped first-run))
                 [(source-mid-label done-mid)]))
           (is (= (set (map (comp source-mid-label :source-mid)
                         (:rejected first-run)))
                 #{(source-mid-label live-mid) (source-mid-label requeue-mid)
                   (source-mid-label legacy-requeue-mid)}))
           (is (= (select-keys copy-info [:status :priority :attempt])
                 {:status :ready, :priority :normal, :attempt 0}))
           (is (= (mq/msg-status target "binary-4") :scheduled))
           (is (nil? (mq/msg-status target "binary-1")))
           (is (nil? (mq/msg-status target "binary-2")))
           (is (nil? (mq/msg-status target "binary-3"))
             "Done source work is never copied")
           (is (nil? (mq/msg-status target "binary-6")))
           (is (truss/submap? second-run {:added 0, :existing 2}))
           (is (empty? (:conflicts second-run)))])

        (let [{:keys [mid msg token]} (#'mq/claim! target 64)]
          [(is (= mid "binary-5"))
           (is (= msg {:kind :copy-base})
             "The public migration path copies the intended base payload")
           (#'mq/settle! target mid token (mq/outcome:ack))]))
      (finally
        (v3-mq/queues-clear!! v3-conn-opts [source])
        (mq/queue-clear!! target)))))

(deftest _migration-binary-mid-order-and-supported-mid-parity
  (let [source (qname "carmine-v3-migration-mid-order")
        target (mq/queue mgr_ (qname "carmine-v4-migration-mid-order"))
        binary-1 (binary-mid 11)
        binary-2 (binary-mid 12)
        binary-3 (binary-mid 13)
        string-mid "plain-string"
        keyword-mid :plain-keyword
        symbol-mid 'plain-symbol
        vector-mid [:plain :vector]
        nil-mid "nil-payload"
        same-udt (- (System/currentTimeMillis) 10000)
        custom-mid-fn
        (fn [source-mid]
          (cond
            (enc/bytes? source-mid)
            (str "bytes-" (bit-and 0xff (aget ^bytes source-mid 1)))
            (string? source-mid) (str "text-" source-mid)
            (symbol? source-mid) (str "symbol-" (name source-mid))
            (vector? source-mid) "vector-mid"))]
    (try
      (v3/wcar v3-conn-opts
        (v3-mq/enqueue source :binary-3 {:mid (binary-mid 13)})
        (v3-mq/enqueue source :binary-1 {:mid (binary-mid 11)})
        (v3-mq/enqueue source :binary-2 {:mid (binary-mid 12)})
        (v3-mq/enqueue source :string {:mid string-mid})
        (v3-mq/enqueue source :keyword {:mid keyword-mid})
        (v3-mq/enqueue source :symbol {:mid symbol-mid})
        (v3-mq/enqueue source :vector {:mid vector-mid})
        (v3-mq/enqueue source nil {:mid nil-mid})
        (v3/hset (v3-key source :udts) (binary-mid 11) same-udt)
        (v3/hset (v3-key source :udts) (binary-mid 12) same-udt)
        (v3/hset (v3-key source :udts) (binary-mid 13) same-udt)
        (v3/hset (v3-key source :udts) string-mid same-udt)
        (v3/hset (v3-key source :udts) keyword-mid same-udt)
        (v3/hset (v3-key source :udts) symbol-mid same-udt)
        (v3/hset (v3-key source :udts) vector-mid same-udt)
        (v3/hdel (v3-key source :udts) nil-mid))

      (testing "Raw serialized MIDs provide a stable content order"
        (let [inspections
              (repeatedly 10 #(migration/v3-inspect v3-conn-opts source))
              label-orders
              (mapv #(mapv (comp source-mid-label :source-mid) (:entries %))
                inspections)
              first-mids (mapv :source-mid (:entries (first inspections)))
              second-mids (mapv :source-mid (:entries (second inspections)))
              first-binary (filterv enc/bytes? first-mids)
              second-binary (filterv enc/bytes? second-mids)]
          [(is (apply = label-orders)
             "Repeated snapshots never depend on JVM array identity hashes")
           (is (= (mapv source-mid-label first-binary)
                 (mapv source-mid-label [binary-1 binary-2 binary-3])))
           (is (every? true?
                 (map (fn [a b]
                        (and (not (identical? a b)) (enc/ba= a b)))
                   first-binary second-binary))
             "RESP may allocate new arrays while content order remains identical")]))

      (testing "Ordinary and serialized v3 MIDs retain their decoded semantics"
        (let [inspection (migration/v3-inspect v3-conn-opts source)
              entries (:entries inspection)
              nil-entry (entry-for
                          (:entries (#'migration/v3-inspect* v3-conn-opts source))
                          nil-mid)]
          [(is (= (:total inspection) 8))
           (is (= (:source-mid (entry-for entries string-mid)) string-mid))
           (is (= (:source-mid (entry-for entries "plain-keyword"))
                 "plain-keyword")
             "V3 keywords retain their established Redis text decoding")
           (is (= (:source-mid (entry-for entries symbol-mid)) symbol-mid))
           (is (= (:source-mid (entry-for entries vector-mid)) vector-mid))
           (is (contains? nil-entry :msg))
           (is (nil? (:msg nil-entry))
             "A nil payload does not collide with missing optional placeholders")
           (is (not (contains? nil-entry :source-enqueued-at-ms)))
           (is (truss/submap? nil-entry
                 {:state :queued, :disposition :copy, :attempts 0}))]))

      (testing "Default/custom target validation and collisions remain value based"
        (let [default-plan (migration/v3-plan v3-conn-opts source target)
              custom-plan (migration/v3-plan v3-conn-opts source target
                            {:mid-fn custom-mid-fn})
              custom-order
              (mapv (comp source-mid-label :source-mid) (:entries custom-plan))
              collision-plan
              (migration/v3-plan v3-conn-opts source target
                {:mid-fn
                 (fn [source-mid]
                   (if (enc/bytes? source-mid)
                     "same-binary-target"
                     (custom-mid-fn source-mid)))})
              colliding-source-mids
              (get (:collisions collision-plan) "same-binary-target")]
          [(is (truss/submap? default-plan {:eligible 4, :rejected 4}))
           (is (every? #(= (:reason %) :invalid-target-mid)
                 (filter (fn [entry]
                           (or (enc/bytes? (:source-mid entry))
                             (vector? (:source-mid entry))))
                   (:entries default-plan))))
           (is (truss/submap? custom-plan {:eligible 8, :rejected 0}))
           (is (= custom-order
                 (mapv (comp source-mid-label :source-mid)
                   (:entries (migration/v3-inspect v3-conn-opts source))))
             "Planning preserves the inspection's deterministic order")
           (is (= (set (map source-mid-label colliding-source-mids))
                 #{(source-mid-label binary-1)
                   (source-mid-label binary-2)
                   (source-mid-label binary-3)}))
           (is (= (count
                    (filter #(= (:reason %) :target-mid-collision)
                      (:entries collision-plan)))
                 3))]))
      (finally
        (v3-mq/queues-clear!! v3-conn-opts [source])
        (mq/queue-clear!! target)))))

(defn- seed-v3! [queue]
  (let [now (System/currentTimeMillis)]
    (v3/wcar v3-conn-opts
      (v3-mq/enqueue queue {:kind :queued} {:mid "queued"})
      (v3-mq/enqueue queue {:kind :delayed} {:mid "delayed", :init-backoff-ms 60000})
      (v3-mq/enqueue queue {:kind :locked} {:mid "locked"})
      (v3-mq/enqueue queue {:kind :done} {:mid "done"})
      (v3-mq/enqueue queue {:kind :requeue-base} {:mid "requeue"})
      (v3/hset (v3-key queue :locks) "locked" (+ now 60000))
      (v3/sadd (v3-key queue :done) "done")
      (v3/hset (v3-key queue :messages-rq) "requeue" {:kind :requeue-latest})
      (v3/sadd (v3-key queue :requeue) "requeue"))))

(deftest _explicit-v3-to-v4-migration
  (let [source (qname "carmine-v3-migration-source")
        target-name (qname "carmine-v4-migration-target")
        target (mq/queue mgr_ target-name
                 {:retry-base-ms 0, :retry-max-ms 0})]
    (try
      (seed-v3! source)
      (testing "Inspection classifies safe, processed, and ambiguous states"
        (let [inspection (migration/v3-inspect v3-conn-opts source)
              by-mid (into {} (map (juxt :source-mid identity)) (:entries inspection))]
          [(is (truss/submap? inspection
                 {:total 5, :eligible 2, :skipped 1, :rejected 2}))
           (is (every? #(truss/submap? % {:msg :submap/nx})
                 (:entries inspection)))
           (is (= (:state (get by-mid "queued")) :queued))
           (is (= (:state (get by-mid "delayed")) :queued-with-backoff))
           (is (= (:delay-ms (get by-mid "delayed"))
                 (- (:source-backoff-expiry-ms (get by-mid "delayed"))
                    (:inspected-at inspection)))
             "Inspection retains the absolute source backoff deadline")
           (is (= (:reason (get by-mid "locked")) :lease-record-present))
           (is (= (:reason (get by-mid "done")) :already-processed))
           (is (= (:reason (get by-mid "requeue")) :pending-requeue-payload))]))

      (testing "Dry-run and quiescence refusal never write"
        (let [dry-run (migration/v3-migrate! v3-conn-opts source target
                        {:dry-run? true, :require-quiescence? false})]
          [(is (:dry-run? dry-run))
           (is (= (:target-queue dry-run) target-name))
           (is (= (:eligible dry-run) 2))
           (is (zero? (get-in (mq/queue-status target) [:counts :active])))])
        (is (throws? :ex-info {:eid :carmine.mq.migration/not-quiescent}
              (migration/v3-migrate! v3-conn-opts source target))))

      (testing "Non-destructive copy is idempotent and preserves delay"
        (let [first-run
              (binding [car/*auto-freeze?* false
                        car/*auto-thaw?* false
                        car/*freeze-opts*
                        {:password [:salted "ambient-migration-secret"]}]
                (migration/v3-migrate! v3-conn-opts source target
                  {:require-quiescence? false}))]
          [(is (= (:target-queue first-run) target-name))
           (is (truss/submap? first-run {:added 2, :existing 0}))
           (is (= (mq/msg-status target "queued") :ready))
           (is (= (mq/msg-status target "delayed") :scheduled))
           (is (= (:total (migration/v3-inspect v3-conn-opts source)) 5)
             "The v3 source remains untouched")])
        (let [second-run (migration/v3-migrate! v3-conn-opts source target
                           {:require-quiescence? false})]
          [(is (truss/submap? second-run {:added 0, :existing 2}))
           (is (empty? (:conflicts second-run)))])
        (v3/wcar v3-conn-opts
          (v3/hset (v3-key source :messages) "queued" {:kind :changed}))
        (let [conflicted (migration/v3-migrate! v3-conn-opts source target
                           {:require-quiescence? false})]
          [(is (= (mapv :error (:conflicts conflicted)) [:mid-conflict]))
           (is (every? #(truss/submap? % {:msg :submap/nx})
                 (concat (:skipped conflicted) (:rejected conflicted))))])

        (let [{:keys [mid msg token]}
              (binding [car/*auto-freeze?* false
                        car/*auto-thaw?* false
                        car/*freeze-opts*
                        {:password [:salted "ambient-migration-secret"]}]
                (#'mq/claim! target 64))]
          [(is (= mid "queued"))
           (is (= msg {:kind :queued})
             "Migration ignores ambient codecs and round-trips the original payload")
           (#'mq/settle! target mid token (mq/outcome:ack))]))

      (testing "Target MID collisions and strict ineligible policy are explicit"
        (let [plan (migration/v3-plan v3-conn-opts source target
                     {:mid-fn (constantly "same")})]
          [(is (= (:eligible plan) 0))
           (is (= (set (keys (:collisions plan))) #{"same"}))])
        (is (throws? :ex-info {:eid :carmine.mq.migration/ineligible}
              (migration/v3-plan v3-conn-opts source target
                {:on-ineligible :throw})))
        (let [plan (migration/v3-plan v3-conn-opts source target
                     {:priority-fn (constantly :urgent)})]
          [(is (zero? (:eligible plan)))
           (is (= (set (map :reason
                         (filter #(= (:disposition %) :reject) (:entries plan))))
                 #{:invalid-target-priority :lease-record-present
                   :pending-requeue-payload}))])
        (let [payload-secret "migration-payload-secret-9d32"
              plan (migration/v3-plan v3-conn-opts source target
                     {:priority-fn
                      (fn [{:keys [source-mid]}]
                        (if (= source-mid "queued")
                          (throw (Exception.
                                   (str "Cannot derive priority: " payload-secret)))
                          :normal))})]
          [(is (= (:reason
                    (some #(when (= (:source-mid %) "queued") %)
                      (:entries plan)))
                 :invalid-target-priority)
             "A thrown priority function is reported separately from its return value")
           (is (not (.contains (pr-str plan) payload-secret))
             "Payload-bearing exception messages are redacted from plans")
           (let [error
                 (truss/throws :ex-info
                   (migration/v3-plan v3-conn-opts source target
                     {:on-ineligible :throw
                      :priority-fn
                      (fn [_]
                        (throw (Exception. payload-secret)))}))]
             (is (not (.contains (pr-str (ex-data error)) payload-secret))
               "Payload-bearing exception messages are redacted from strict errors"))])
        (let [strict-target
              (mq/queue mgr_ (qname "carmine-v4-migration-strict-target"))]
          (try
            [(is (throws? :ex-info {:eid :carmine.mq.migration/ineligible}
                   (migration/v3-migrate! v3-conn-opts source strict-target
                     {:on-ineligible :throw
                      :require-quiescence? false
                      :priority-fn
                      (fn [{:keys [source-mid]}]
                        (if (= source-mid "delayed") :urgent :normal))})))
               (is (zero? (get-in (mq/queue-status strict-target)
                            [:counts :active]))
                 "Strict preflight rejection performs no target writes")]
            (finally (mq/queue-clear!! strict-target)))))

      (testing "Incompatible target queue policies fail during preflight"
        (doseq [target-opts [{:on-duplicate :coalesce, :revision-mode :required}
                             {:on-duplicate :coalesce}]]
          (let [incompatible-target
                (mq/queue mgr_ (qname "carmine-v4-migration-incompatible")
                  target-opts)]
            (try
              (is (throws? :ex-info
                    {:eid :carmine.mq.migration/incompatible-target}
                    (migration/v3-plan
                      v3-conn-opts source incompatible-target)))
              (finally (mq/queue-clear!! incompatible-target))))))
      (finally
        (v3-mq/queues-clear!! v3-conn-opts [source])
        (mq/queue-clear!! target)))))
