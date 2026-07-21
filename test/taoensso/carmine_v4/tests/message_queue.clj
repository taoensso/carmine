(ns taoensso.carmine-v4.tests.message-queue
  (:require
   [clojure.set :as set]
   [clojure.test :refer [deftest testing is]]
   [taoensso.encore :as enc]
   [taoensso.nippy :as nippy]
   [taoensso.truss :as truss :refer [throws?]]
   [taoensso.trove :as trove]
   [taoensso.carmine-v4 :as car]
   [taoensso.carmine-v4.cluster :as cluster]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.message-queue :as mq]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write :as write]))

(defonce mgr_
  (delay
    (conns/conn-manager-pooled
      {:conn-opts
       {:server ["127.0.0.1"
                 (Long/parseLong (or (System/getenv "CARMINE_TEST_REDIS_PORT") "6379"))]}})))
(defn qname [] (str "carmine-v4-mq-test-" (java.util.UUID/randomUUID)))

(defn wait-for [timeout-ms pred]
  (let [deadline (+ (System/nanoTime) (* timeout-ms 1000000))]
    (loop []
      (cond
        (pred) true
        (< (System/nanoTime) deadline) (do (Thread/sleep 10) (recur))
        :else false))))

(defn- wait-for-server-time! [queue target-ms]
  (when-not
    (wait-for 5000
      #(>= (long (:server-time-ms (mq/queue-status queue))) (long target-ms)))
    (throw
      (ex-info "Timed out waiting for Redis server time"
        {:target-ms target-ms, :status (mq/queue-status queue)}))))

(defn- inert-queue []
  (#'mq/new-queue nil "option-validation" {} {}))

(defn- uncaught-capturing-executor [uncaught submissions_ shutdown?_]
  (proxy [java.util.concurrent.AbstractExecutorService] []
    (execute [runnable]
      (swap! submissions_ inc)
      (let [thread (Thread. ^Runnable runnable "carmine-v4-mq-test-runner")]
        (.setDaemon thread true)
        (.setUncaughtExceptionHandler thread
          (reify java.lang.Thread$UncaughtExceptionHandler
            (uncaughtException [_ _ error]
              (deliver uncaught error))))
        (.start thread)))
    (shutdown [] (reset! shutdown?_ true))
    (shutdownNow [] (reset! shutdown?_ true) [])
    (isShutdown [] @shutdown?_)
    (isTerminated [] @shutdown?_)
    (awaitTermination [_ _] @shutdown?_)))

(defn- queue-name [queue] (#'mq/queue-name queue))
(defn- queue-keys [queue] (#'mq/queue-keys queue))

(defn- signal-count [queue]
  (long (car/wcar @mgr_ (car/llen (get (queue-keys queue) :signal)))))

(defn- raw-payload-digest [queue mid]
  (car/wcar @mgr_
    (car/lua
      "local v=redis.call('hget', KEYS[1], ARGV[1]);
       if not v then return false; end;
       return redis.sha1hex(v)"
      [(get (queue-keys queue) :payloads)] [mid])))

(defn- inject-raw-payload! [queue role mid raw]
  (car/wcar @mgr_
    (car/hset (get (queue-keys queue) role) mid raw)))

(defn- inject-raw-payload-bytes! [queue role mid byte-values]
  (car/wcar @mgr_
    (car/lua
      "local bytes={};
       for idx=2,#ARGV do bytes[idx-1]=tonumber(ARGV[idx]); end;
       redis.call('hset', KEYS[1], ARGV[1], string.char(unpack(bytes)));
       return 1"
      [(get (queue-keys queue) role)]
      (into [mid] (map #(bit-and (long %) 0xff)) byte-values))))

(defn- make-signal-wrongtype! [queue]
  (let [signal (get (queue-keys queue) :signal)]
    (car/wcar @mgr_
      (car/del signal)
      (car/hset signal "wrongtype" "advisory"))))

(defn- raw-seq [queue]
  (car/wcar @mgr_
    (car/lua
      "local t=redis.call('type',KEYS[1]);
       local v=nil;
       if t.ok == 'string' then v=redis.call('get',KEYS[1]); end;
       return {t.ok,v or false}"
      [(get (queue-keys queue) :seq)] [])))

(defn- set-seq! [queue value]
  (let [seq-key (get (queue-keys queue) :seq)]
    (car/wcar @mgr_
      (car/del seq-key)
      (if (= value :wrongtype)
        (car/hset seq-key "wrongtype" "fifo")
        (car/set seq-key value)))))

(defn- key-dump-digest [key]
  (car/wcar @mgr_
    (car/lua
      "local v=redis.call('dump', KEYS[1]);
       if not v then return false; end;
       return redis.sha1hex(v)"
      [key] [])))

(defn- move-qname [prefix]
  (str prefix "-" (java.util.UUID/randomUUID)))

(defn- v4-keys [queue]
  (vals (queue-keys queue)))

(defn- remove-v4-queue!! [queue]
  (mq/queue-clear!! queue)
  (car/wcar @mgr_ (apply car/unlink (v4-keys queue))))

(defn- mutate-packed! [queue role mid lua-mutation]
  (car/wcar @mgr_
    (car/lua
      (str "local v=cmsgpack.unpack(redis.call('hget', KEYS[1], ARGV[1])); "
        lua-mutation
        " redis.call('hset', KEYS[1], ARGV[1], cmsgpack.pack(v)); return 1")
      [(get (queue-keys queue) role)] [mid])))

(defn- seed-dead! [queue mid]
  (mq/msg-enqueue! queue {:dead mid} {:mid mid})
  (let [claim (#'mq/claim! queue 64)]
    (#'mq/settle! queue mid (:token claim)
      (mq/outcome:dead {:reason "test-failure"}))))

(defn- raw-mid-fingerprint [queue mid]
  (let [keys (queue-keys queue)]
    (car/wcar @mgr_
      (car/lua
        "local d=function(v) if v then return redis.sha1hex(v) else return '' end end
         local z=function(k) return redis.call('zscore', k, ARGV[1]) or '' end
         return {
           d(redis.call('hget', KEYS[1], ARGV[1])),
           d(redis.call('hget', KEYS[2], ARGV[1])), z(KEYS[3]),
           d(redis.call('hget', KEYS[4], ARGV[1])),
           d(redis.call('hget', KEYS[5], ARGV[1])),
           d(redis.call('hget', KEYS[6], ARGV[1])),
           d(redis.call('hget', KEYS[7], ARGV[1])),
           d(redis.call('hget', KEYS[8], ARGV[1])),
           z(KEYS[9]), z(KEYS[10]), z(KEYS[11]), z(KEYS[12]), z(KEYS[13])
         }"
        [(:failures keys) (:dead-payloads keys) (:dead keys)
         (:payloads keys) (:meta keys) (:successor-payloads keys)
         (:successor-meta keys) (:lease-tokens keys)
         (:ready-high keys) (:ready-normal keys) (:ready-low keys)
         (:scheduled keys) (:leased keys)]
        [mid]))))

(deftest _queue-move
  (let [source-name (move-qname "carmine-v4-move-source")
        target-name (move-qname "carmine-v4-move-target")
        source (mq/queue mgr_ source-name
                 {:on-duplicate :coalesce, :lease-ms 60000
                  :retry-base-ms 0, :retry-max-ms 0})]
    (try
      (mq/msg-enqueue! source :dead {:mid "dead"})
      (let [claim (#'mq/claim! source 64)]
        (#'mq/settle! source "dead" (:token claim)
          (mq/outcome:dead {:reason "move"})))
      (mq/msg-enqueue! source :leased {:mid "leased"})
      (let [leased (#'mq/claim! source 64)]
        (mq/msg-enqueue! source :successor {:mid "leased"})
        (mq/msg-enqueue! source :ready {:mid "ready"})
        (mq/msg-enqueue! source :scheduled {:mid "scheduled", :delay-ms 60000})
        (let [before (mq/queue-status source)
              moved (mq/queue-move! source target-name)
              target (:queue moved)]
          (try
            [(is (truss/submap? moved
                   {:success? true, :action :moved
                    :source-queue source-name, :target-queue target-name}))
             (is (pos-int? (:moved-key-count moved)))
             (is (= (:moved-key-count moved) (:deleted-key-count moved)))
             (is (= (:counts (mq/queue-status target)) (:counts before)))
             (is (truss/submap? (mq/dead-info target "dead")
                   {:status :dead, :msg :dead, :reason "move"}))
             (is (truss/submap? (mq/msg-info target "leased")
                   {:status :leased, :successor? true}))
             (is (= (:action (#'mq/settle! target "leased" (:token leased)
                               (mq/outcome:ack)))
                   :acked)
               "Lease tokens remain valid after the stopped-queue move")
             (is (zero? (car/wcar @mgr_ (apply car/exists (v4-keys source))))
               "Every physical source key is removed")
             (is (= (mq/msg-enqueue! source :invalid-after-move {:mid "old-handle"})
                   {:success? false, :error :uninitialized, :mid "old-handle"}))]
            (finally (remove-v4-queue!! target)))))
      (finally
        (car/wcar @mgr_ (apply car/unlink (v4-keys source))))))

  (testing "Same-name and occupied targets are rejected without mutation"
    (let [source (mq/queue mgr_ (move-qname "carmine-v4-move-collision-source"))
          target (mq/queue mgr_ (move-qname "carmine-v4-move-collision-target"))]
      (try
        (mq/msg-enqueue! source :source {:mid "source"})
        [(is (throws? :ex-info {:eid :carmine.mq/move-invalid-target}
               (mq/queue-move! source (#'mq/queue-name source))))
         (is (throws? :ex-info {:eid :carmine.mq/move-target-not-empty}
               (mq/queue-move! source (#'mq/queue-name target))))
         (is (= (mq/msg-status source "source") :ready))
         (is (zero? (get-in (mq/queue-status target) [:counts :active])))]
        (finally
          (remove-v4-queue!! source)
          (remove-v4-queue!! target)))))

  (testing "Normalized durability policy survives the move"
    (let [source (mq/queue mgr_ (move-qname "carmine-v4-move-durable-source")
                   {:durability {:replicas 1, :timeout-ms 10}})
          target-name (move-qname "carmine-v4-move-durable-target")
          target-keys (vals (#'mq/qkeys target-name))]
      (try
        (is (= (:action (mq/msg-enqueue! source :durable {:mid "durable"})) :added))
        (let [target (:queue (mq/queue-move! source target-name))]
          [(is (= (mq/msg-status target "durable") :ready))
           (is (= (:durability (#'mq/queue-opts target))
                 {:mode :wait, :replicas 1, :timeout-ms 10}))])
        (finally
          (car/wcar @mgr_ (apply car/unlink (v4-keys source)))
          (car/wcar @mgr_ (apply car/unlink target-keys))))))

  (testing "A copy failure keeps the source and removes the partial target"
    (let [source (mq/queue mgr_ (move-qname "carmine-v4-move-failure-source"))
          target-name (move-qname "carmine-v4-move-failure-target")
          target-keys (vals (#'mq/qkeys target-name))
          original-restore car/restore
          restore-calls_ (atom 0)]
      (try
        (mq/msg-enqueue! source :source {:mid "source"})
        (let [error
              (try
                (with-redefs-fn
                  {#'car/restore
                   (fn [key ttl serialized-value & args]
                     (if (= (swap! restore-calls_ inc) 2)
                       (throw (ex-info "Injected RESTORE failure" {:injected? true}))
                       (apply original-restore key ttl serialized-value args)))}
                  #(mq/queue-move! source target-name))
                nil
                (catch Exception t t))]
          [(is (truss/submap? (ex-data error)
                 {:eid :carmine.mq/move-copy-failed
                  :ambiguous? false}))
           (is (= (:injected? (ex-data (ex-cause error))) true))
           (is (<= 2 @restore-calls_))
           (is (= (mq/msg-status source "source") :ready))
           (is (zero? (car/wcar @mgr_ (apply car/exists target-keys))))])
        (finally
          (remove-v4-queue!! source)
          (car/wcar @mgr_ (apply car/unlink target-keys))))))

  (testing "A copy-and-cleanup failure retains both diagnostic causes"
    (let [source (mq/queue mgr_ (move-qname "carmine-v4-move-cleanup-source"))
          target-name (move-qname "carmine-v4-move-cleanup-target")
          target-keys (vals (#'mq/qkeys target-name))
          original-restore car/restore
          restore-calls_ (atom 0)
          copy-error (ex-info "Injected RESTORE failure" {:stage :copy})
          cleanup-error (ex-info "Injected UNLINK failure" {:stage :cleanup})]
      (try
        (mq/msg-enqueue! source :source {:mid "source"})
        (let [thrown
              (try
                (with-redefs-fn
                  {#'car/restore
                   (fn [key ttl serialized-value & args]
                     (if (= (swap! restore-calls_ inc) 2)
                       (throw copy-error)
                       (apply original-restore key ttl serialized-value args)))
                   #'car/unlink (fn [& _] (throw cleanup-error))}
                  #(mq/queue-move! source target-name))
                nil
                (catch Exception t t))]
          [(is (truss/submap? (ex-data thrown)
                 {:eid :carmine.mq/move-copy-cleanup-failed
                  :ambiguous? true
                  :cleanup-error-class "clojure.lang.ExceptionInfo"}))
           (is (identical? (ex-cause thrown) copy-error))
           (is (some #(identical? % cleanup-error)
                 (.getSuppressed ^Throwable thrown)))
           (is (= (mq/msg-status source "source") :ready))
           (is (pos? (car/wcar @mgr_ (apply car/exists target-keys))))])
        (finally
          (remove-v4-queue!! source)
          (car/wcar @mgr_ (apply car/unlink target-keys))))))

  (testing "A definite source-delete failure retains both complete names"
    (let [source (mq/queue mgr_ (move-qname "carmine-v4-move-delete-source"))
          target-name (move-qname "carmine-v4-move-delete-target")
          redis-error
          (com/drained-reply-errors
            [(com/reply-error "NOPERM injected"
               {:eid :carmine.read/redis-error-reply, :code "NOPERM"})]
            [0])
          error
          (try
            (mq/msg-enqueue! source :source {:mid "source"})
            (with-redefs [car/unlink (fn [& _] (throw redis-error))]
              (mq/queue-move! source target-name))
            nil
            (catch Exception t t))
          target (mq/queue mgr_ target-name)]
      (try
        [(is (truss/submap? (ex-data error)
               {:eid :carmine.mq/move-delete-failed
                :ambiguous? false}))
         (is (= (mq/msg-status source "source") :ready))
         (is (= (mq/msg-status target "source") :ready))]
        (finally
          (remove-v4-queue!! source)
          (remove-v4-queue!! target)))))

  (testing "A transport-classified source-delete failure is ambiguous"
    (let [source (mq/queue mgr_ (move-qname "carmine-v4-move-ambiguous-source"))
          target-name (move-qname "carmine-v4-move-ambiguous-target")
          transport-error (ex-info "Injected UNLINK transport failure" {:injected? true})
          error
          (try
            (mq/msg-enqueue! source :source {:mid "source"})
            (with-redefs [car/unlink (fn [& _] (throw transport-error))]
              (mq/queue-move! source target-name))
            nil
            (catch Exception t t))
          target (mq/queue mgr_ target-name)]
      (try
        [(is (truss/submap? (ex-data error)
               {:eid :carmine.mq/move-delete-ambiguous
                :ambiguous? true}))
         (is (identical? (ex-cause error) transport-error))
         (is (= (mq/msg-status source "source") :ready))
         (is (= (mq/msg-status target "source") :ready))]
        (finally
          (remove-v4-queue!! source)
          (remove-v4-queue!! target))))))

(deftest _public-api-contract
  (let [publics (ns-publics 'taoensso.carmine-v4.message-queue)
        expected-arglists
        '{dead-mids ([queue] [queue start stop])
          dead-info ([queue mid])
          dead-page ([queue] [queue opts])
          dead-purge! ([queue opts])
          dead-redrive! ([queue mid])
          msg-active-page ([queue] [queue opts])
          msg-claim! ([queue] [queue opts])
          msg-enqueue! ([queue msg] [queue msg opts])
          msg-extend-lease! ([queue mid lease-token])
          msg-info ([queue mid])
          msg-release! ([queue mid lease-token])
          msg-remove! ([queue mid])
          msg-settle! ([queue mid lease-token outcome]
                       [queue mid lease-token outcome opts])
          msg-status ([queue mid])
          outcome:ack ([])
          outcome:dead ([] [{:keys [reason], :as opts}])
          outcome:discard ([])
          outcome:retry ([] [{:keys [delay-ms reason], :as opts}])
          queue ([manager qname] [manager qname opts])
          queue-clear!! ([queue])
          queue-config ([queue])
          queue-config-update! ([queue updates])
          queue-move! ([source-queue target-qname])
          queue-status ([queue])
          queue? ([x])
          worker-await-stopped! ([worker timeout-ms])
          worker-clear-stats! ([worker])
          worker-create ([queue handler] [queue handler opts])
          worker-start! ([worker])
          worker-stop! ([worker])
          worker-stats ([worker])
          worker? ([x])}
        actual-arglists
        (into {} (map (fn [[sym v]] [sym (:arglists (meta v))])) publics)]
    [(is (= actual-arglists expected-arglists)
       "The intentional MQ symbols and call shapes change explicitly")
     (is (every? (comp not-empty :doc meta val) publics))
     (is (every? (comp seq :arglists meta val) publics))]))

(deftest _public-option-validation
  (let [queue (inert-queue)
        invalid-option?
        (fn [f]
          (throws? :ex-info {:eid :carmine.mq/invalid-option} (f)))]
    [(is (invalid-option? #(mq/queue nil "option-validation" {:lease-mz 1})))
     (is (invalid-option? #(mq/msg-enqueue! queue :msg {:priorit :high})))
     (is (invalid-option? #(mq/msg-enqueue! queue :msg {:id "legacy"})))
     (is (throws? :ex-info {:eid :carmine.mq/invalid-mid}
           (mq/msg-info queue "")))
     (is (invalid-option? #(mq/msg-active-page queue {:limt 10})))
     (is (invalid-option? #(mq/dead-purge! queue {:older-than-ms 0, :limt 10})))
     (is (invalid-option? #(mq/worker-create queue identity {:concurreny 2})))
     (is (invalid-option? #(mq/worker-create queue identity false)))
     (is (invalid-option? #(mq/worker-create queue identity {:include-msg? "false"})))
     (is (invalid-option? #(mq/worker-create queue identity {:on-event :callback})))
     (is (invalid-option? #(mq/worker-create queue identity
                             {:idle-max-ms (inc (deref #'mq/max-worker-idle-ms))})))
     (is (invalid-option? #(mq/worker-create queue identity
                             {:lease-extend-every-ms 0})))
     (let [lease-queue
           (#'mq/new-queue nil "option-validation" {:lease-ms 1000} {})]
       (is (invalid-option?
             #(mq/worker-create lease-queue identity
                {:lease-extend-every-ms 1000}))
         "Heartbeat intervals must be shorter than the queue's default lease"))
     (is (throws? :ex-info
           {:eid :carmine.mq/incompatible-options
            :problem :revision-requires-coalescing}
           (mq/queue nil "option-validation" {:revision-mode :required})))
     (let [revision-queue
           (#'mq/new-queue nil "option-validation"
             {:max-attempts 8, :on-exhaustion :dead
              :on-duplicate :coalesce, :revision-mode :required, :durability nil}
             {})]
       (is (throws? :ex-info
             {:eid :carmine.mq/incompatible-options
              :problem :revision-requires-coalescing}
             (mq/msg-enqueue! revision-queue :msg
               {:mid "mid", :revision 1, :on-duplicate :reject}))))
     (is (not (map? queue)) "Queue handles do not expose physical state as maps")
     (is (not (associative? queue)))
     (is (= (class (mq/outcome:retry {:example/trace-id "trace"}))
            (class (mq/outcome:retry))))]))

(deftest _leading-nul-is-reserved-in-public-text-domains
  (let [leading "\u0000reserved"
        embedded "accepted\u0000inside"
        queue (inert-queue)
        enqueue-queue
        (#'mq/new-queue nil "nul-validation"
          {:max-attempts 8, :on-exhaustion :dead
           :on-duplicate :reject, :revision-mode :none, :durability nil}
          {})]
    (testing "Queue names fail with queue-specific diagnostics"
      (is (throws? :ex-info
            {:eid :carmine.mq/invalid-queue-name, :qname leading}
            (mq/queue nil leading))))

    (testing "MID reads and mutations share the explicit MID contract"
      (doseq [operation [#(mq/msg-enqueue! enqueue-queue :msg {:mid leading})
                         #(mq/msg-info queue leading)]]
        (is (throws? :ex-info
              {:eid :carmine.mq/invalid-mid, :mid leading}
              (operation)))))

    (testing "Both reason constructors fail before an internal settlement"
      (doseq [[context outcome]
              [[:retry #(mq/outcome:retry {:reason leading})]
               [:dead  #(mq/outcome:dead  {:reason leading})]]]
        (is (throws? :ex-info
              {:eid :carmine.mq/invalid-outcome-options
               :context context, :option :reason, :value leading}
              (outcome)))))

    (testing "Only the first character is reserved"
      [(is (= (#'mq/qname-str embedded) embedded))
       (is (= (#'mq/mid-str embedded) embedded))
       (is (= (.-reason ^taoensso.carmine_v4.message_queue.HandlerOutcome
                (mq/outcome:retry {:reason embedded}))
              embedded))])))

(deftest _mq-public-text-requires-well-formed-utf16
  (let [lone-high (String. (char-array [(char 0xd800)]))
        lone-low  (String. (char-array [(char 0xdc00)]))
        malformed (str "prefix-" lone-high "-suffix")
        queue (inert-queue)]
    (testing "Each public text validator rejects malformed UTF-16"
      [(is (throws? :ex-info {:eid :carmine.mq/invalid-queue-name}
             (mq/queue nil malformed)))
       (is (throws? :ex-info {:eid :carmine.mq/invalid-mid}
             (mq/msg-info queue malformed)))
       (is (throws? :ex-info
             {:eid :carmine.mq/invalid-outcome-options
              :context :retry, :option :reason}
             (mq/outcome:retry {:reason malformed})))
       (is (not (#'mq/well-formed-utf16? lone-low)))])

    (testing "Internal reasons replace malformed code units and truncate only at code-point boundaries"
      (let [astral (String. (Character/toChars 0x1f680))
            crosses-boundary (str (apply str (repeat 1023 \a)) astral "tail")
            crossed (#'mq/safe-internal-reason crosses-boundary)
            throwable-reason
            (#'mq/throwable-reason
              (Exception. (str "bad-" lone-high "-reason")))]
        [(is (= (count crossed) 1023)
           "The high surrogate at the 1024-code-unit cut is not retained alone")
         (is (#'mq/well-formed-utf16? crossed))
         (is (= throwable-reason "java.lang.Exception: bad-�-reason"))
         (is (#'mq/well-formed-utf16? throwable-reason))]))))

(deftest _mq-valid-astral-public-text-round-trips
  (let [astral (String. (Character/toChars 0x1f680))
        queue-name (str (qname) "-" astral)
        mid (str "mid-" astral "-job")
        reason (str "reason-" astral "-failed")
        q (mq/queue mgr_ queue-name)]
    (try
      (is (= (#'mq/queue-name q) queue-name))
      (let [added (mq/msg-enqueue! q {:astral astral} {:mid mid})]
        [(is (truss/submap? added
               {:success? true, :action :added, :mid mid, :prior-dead? false}))
         (is (= (mq/msg-enqueue! q {:astral astral} {:mid mid})
               {:success? true, :action :existing, :mid mid})
           "Astral MIDs remain retry-idempotent")])
      (is (= (:status (mq/msg-info q mid)) :ready))
      (is (= (:mid (first (:items (mq/msg-active-page q)))) mid))
      (let [claim (#'mq/claim! q 64)]
        [(is (= (:mid claim) mid))
         (is (= (:msg claim) {:astral astral}))
         (is (= (:action
                  (#'mq/settle! q mid (:token claim)
                    (mq/outcome:dead {:reason reason})))
                :dead))])
      [(is (= (mq/msg-status q mid) :dead))
       (is (truss/submap? (mq/dead-info q mid)
             {:mid mid, :status :dead, :msg {:astral astral}, :reason reason}))
       (is (= (mq/dead-redrive! q mid)
             {:success? true, :action :redriven}))]
      (let [redriven (#'mq/claim! q 64)]
        [(is (= (:mid redriven) mid))
         (is (= (:action (#'mq/settle! q mid (:token redriven)
                           (mq/outcome:ack)))
               :acked))])
      (finally (remove-v4-queue!! q)))))

(deftest _mq-internal-exception-reason-is-durable-safe
  (let [lone-high (String. (char-array [(char 0xd800)]))
        q (mq/queue mgr_ (qname) {:max-attempts 1})]
    (try
      (mq/msg-enqueue! q :payload {:mid "internal-reason"})
      (let [{:keys [token]} (#'mq/claim! q 64)
            reason (#'mq/throwable-reason
                     (Exception. (str "malformed-" lone-high "-message")))
            outcome (#'mq/handler-outcome :retry nil reason)]
        [(is (= (:action (#'mq/settle! q "internal-reason" token outcome)) :dead))
         (is (= (:reason (mq/dead-info q "internal-reason"))
               "java.lang.Exception: malformed-�-message"))
         (is (#'mq/well-formed-utf16?
               (:reason (mq/dead-info q "internal-reason"))))])
      (finally (remove-v4-queue!! q)))))

(deftest _mq-payload-encoding-is-protocol-owned
  (let [q (mq/queue mgr_ (qname))
        payload {:kind :durable, :nested [1 {:two #{:a :b}}]}
        ambient-opts {:password [:salted "ambient-only-secret"]}
        seen_ (atom {})
        handled (promise)
        worker
        (mq/worker-create q
          (fn [{:keys [mid msg]}]
            (when (= (count (swap! seen_ assoc mid msg)) 2)
              (deliver handled true))
            (mq/outcome:ack))
          {:idle-min-ms 10, :idle-max-ms 1000})]
    (try
      (mq/msg-enqueue! q payload {:mid "default-wire"})
      (let [default-digest (raw-payload-digest q "default-wire")]
        (mq/msg-remove! q "default-wire")
        (binding [car/*auto-freeze?* false
                  car/*auto-thaw?* false
                  car/*freeze-opts* ambient-opts]
          (is (= (:action (mq/msg-enqueue! q payload {:mid "ambient-wire"}))
                :added))
          (is (= (raw-payload-digest q "ambient-wire") default-digest)
            "Ambient marker and encrypted Nippy settings cannot alter durable MQ bytes")
          (is (= (:action
                   (mq/msg-enqueue! q payload {:mid "ambient-wire"}))
                :existing)
            "Retry-safe duplicate equality retains the default byte contract")
          (is (= (:action
                   (mq/msg-enqueue! q "\u0000frozen-payload"
                     {:mid "leading-nul-payload"}))
                :added)
            "The public text reservation does not apply inside frozen payloads")))
      (is (mq/worker-start! worker))
      [(is (= (deref handled 3000 :timeout) true))
       (is (= @seen_
             {"ambient-wire" payload
              "leading-nul-payload" "\u0000frozen-payload"})
         "Handlers receive the original values under the protocol-owned encoding")
       (is (wait-for 2000
             #(zero? (get-in (mq/queue-status q) [:counts :active]))))]
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _mq-byte-wrapper-payload-normalization
  (let [payload (byte-array [0 60 78 80 89 0 13 -1 127 -128])
        ambient-opts {:password [:salted "ambient-byte-wrapper-secret"]}
        q (mq/queue mgr_ (qname))
        seen_ (atom {})
        handled (promise)
        worker
        (mq/worker-create q
          (fn [{:keys [mid msg]}]
            (when (= (count (swap! seen_ assoc mid msg)) 2)
              (deliver handled true))
            (if (= mid "wrapped-dead")
              (mq/outcome:dead {:reason "wrapped-bytes"})
              (mq/outcome:ack)))
          {:idle-min-ms 10, :idle-max-ms 1000})]
    (try
      (binding [car/*auto-freeze?* false
                car/*auto-thaw?* false
                car/*freeze-opts* ambient-opts]
        [(is (= (:action (mq/msg-enqueue! q payload {:mid "raw-retry"}))
               :added))
         (is (= (:action
                  (mq/msg-enqueue! q (car/bytes (aclone payload))
                    {:mid "raw-retry"}))
               :existing)
           "Raw bytes and the public bytes wrapper have identical retry bytes")
         (is (= (:action
                  (mq/msg-enqueue! q (car/bytes (aclone payload))
                    {:mid "wrapped-dead"}))
               :added))])
      (is (mq/worker-start! worker))
      [(is (= (deref handled 3000 :timeout) true))
       (is (wait-for 2000 #(nil? (mq/msg-status q "raw-retry"))))
       (is (wait-for 2000 #(= (mq/msg-status q "wrapped-dead") :dead)))
       (is (= (set (keys @seen_)) #{"raw-retry" "wrapped-dead"}))
       (doseq [[mid msg] @seen_]
         [(is (enc/bytes? msg) (str mid " handler payload is a byte array"))
          (is (enc/ba= msg payload)
            (str mid " preserves marker-like and NUL bytes exactly"))])
       (let [dead-msg (:msg (mq/dead-info q "wrapped-dead"))]
         [(is (enc/bytes? dead-msg))
          (is (enc/ba= dead-msg payload)
            "Dead-letter reads retain the normalized byte-array value")])]
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q))))

  (let [payload (byte-array [0 60 78 80 89 0 13 -1 127 -128])
        q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :revision-mode :required})]
    (try
      (binding [car/*auto-freeze?* false
                car/*auto-thaw?* false
                car/*freeze-opts*
                {:password [:salted "ambient-revision-byte-wrapper-secret"]}]
        (is (= (:action
                 (mq/msg-enqueue! q payload {:mid "revision", :revision 1}))
              :added))
        (let [raw-digest (raw-payload-digest q "revision")]
          [(is (= (:action
                    (mq/msg-enqueue! q (car/bytes (aclone payload))
                      {:mid "revision", :revision 1}))
                 :existing)
             "Same-revision wrapper replay is byte-identical")
           (is (= (raw-payload-digest q "revision") raw-digest))
           (is (= (:action
                    (mq/msg-enqueue! q (car/bytes (aclone payload))
                      {:mid "revision", :revision 2}))
                 :coalesced))
           (is (= (raw-payload-digest q "revision") raw-digest)
             "A higher revision retains the same logical byte-array payload")
           (is (= (:action
                    (mq/msg-enqueue! q (aclone payload)
                      {:mid "revision", :revision 2}))
                 :existing)
             "Raw-byte replay is equivalent in the opposite direction")]))
      (let [claim (#'mq/claim! q 64)]
        [(is (enc/bytes? (:msg claim)))
         (is (enc/ba= (:msg claim) payload))
         (#'mq/settle! q "revision" (:token claim) (mq/outcome:ack))])
      (finally (mq/queue-clear!! q)))))

(deftest _mq-payload-envelope-corruption-is-contained
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :lease-ms 60000
             :max-attempts 1, :retry-base-ms 0, :retry-max-ms 0})
        raw "out-of-band-unmarked-payload"
        page-item
        (fn [status include-related? mid]
          (some #(when (= (:mid %) mid) %)
            (:items
              (mq/msg-active-page q
                {:status status, :limit 250
                 :include-related? include-related?}))))]
    (try
      (testing "The internal frozen-input path refuses an invalid envelope before writing"
        [(is (= (#'mq/msg-enqueue-frozen! q raw {:mid "invalid-input"})
               {:success? false, :error :invalid-payload, :mid "invalid-input"}))
         (is (nil? (mq/msg-info q "invalid-input")))])

      (testing "A valid frozen byte-array envelope passes diagnostics and delivery"
        (let [payload (byte-array [0 62 78 80 89 -1 0 127])]
          (mq/msg-enqueue! q payload {:mid "valid-bytes"})
          [(is (= (:status (mq/msg-info q "valid-bytes")) :ready))
           (let [claim (#'mq/claim! q 64)]
             [(is (= (:mid claim) "valid-bytes"))
              (is (enc/ba= (:msg claim) payload))
              (is (= (:action (#'mq/settle! q "valid-bytes" (:token claim)
                                (mq/outcome:ack)))
                    :acked))])]))

      (testing "A ready invalid envelope is observable, never delivered, and contained"
        (let [mid "corrupt-ready"]
          (mq/msg-enqueue! q :payload {:mid mid})
          (inject-raw-payload! q :payloads mid raw)
          (let [before (raw-mid-fingerprint q mid)]
            [(is (= (mq/msg-info q mid) {:status :corrupt}))
             (is (= (:status (page-item :ready false mid)) :ready)
               "The explicitly payload-free page checks presence, not envelope bytes")
             (is (= (mq/msg-enqueue! q :replacement {:mid mid})
                   {:success? false, :error :corrupt-active, :mid mid}))
             (is (= (raw-mid-fingerprint q mid) before)
               "Diagnostics and enqueue preflight do not mutate corrupt work")])
          [(is (= (#'mq/claim! q 64)
                 {:action :skip, :reason :corrupt-payload}))
           (is (nil? (mq/msg-info q mid))
             "Claim follows existing malformed-active cleanup containment")]))

      (testing "Due scheduled invalid envelopes are cleaned without delivery"
        (let [mid "corrupt-scheduled"
              keys (queue-keys q)]
          (mq/msg-enqueue! q :payload {:mid mid, :delay-ms 60000})
          (inject-raw-payload! q :payloads mid raw)
          (car/wcar @mgr_ (car/zadd (:scheduled keys) 0 mid))
          (is (= (:action (#'mq/claim! q 64)) :idle))
          (is (nil? (mq/msg-info q mid)))))

      (testing "Settlement and release refuse an invalid leased envelope read-only"
        (let [mid "corrupt-leased"]
          (mq/msg-enqueue! q :payload {:mid mid})
          (let [claim (#'mq/claim! q 64)]
            (inject-raw-payload! q :payloads mid raw)
            (let [before (raw-mid-fingerprint q mid)]
              [(is (= (mq/msg-info q mid) {:status :corrupt}))
               (is (throws? :ex-info
                     {:eid :carmine.mq/settlement-failed
                      :error :corrupt-payload, :mid mid}
                     (#'mq/settle! q mid (:token claim) (mq/outcome:ack))))
               (is (= (#'mq/release! q mid (:token claim)) :corrupt))
               (is (= (raw-mid-fingerprint q mid) before)
                 "Neither fenced mutation consumes invalid payload bytes")]))
          (mq/msg-remove! q mid)))

      (testing "An invalid successor is never promoted and every preflight refuses it"
        (let [mid "corrupt-successor"]
          (mq/msg-enqueue! q :active {:mid mid})
          (let [claim (#'mq/claim! q 64)]
            (mq/msg-enqueue! q :successor {:mid mid})
            (inject-raw-payload! q :successor-payloads mid raw)
            (let [before (raw-mid-fingerprint q mid)]
              [(is (= (mq/msg-info q mid) {:status :corrupt}))
               (is (= (:status (page-item :leased false mid)) :leased)
                 "Base pagination remains active-payload-free")
               (is (= (:status (page-item :leased true mid)) :corrupt)
                 "The already-fetched related successor envelope is validated")
               (is (= (mq/msg-enqueue! q :newer {:mid mid})
                     {:success? false, :error :corrupt-successor, :mid mid}))
               (is (throws? :ex-info
                     {:eid :carmine.mq/settlement-failed
                      :error :corrupt-successor, :mid mid}
                     (#'mq/settle! q mid (:token claim) (mq/outcome:ack))))
               (is (= (#'mq/release! q mid (:token claim)) :corrupt))
               (is (= (raw-mid-fingerprint q mid) before))]))
          (mq/msg-remove! q mid)))

      (testing "Expired-lease maintenance drops an invalid successor before exhaustion"
        (let [mid "corrupt-successor-expiry"
              keys (queue-keys q)]
          (mq/msg-enqueue! q :active {:mid mid})
          (#'mq/claim! q 64)
          (mq/msg-enqueue! q :successor {:mid mid})
          (inject-raw-payload! q :successor-payloads mid raw)
          (car/wcar @mgr_ (car/zadd (:leased keys) 0 mid))
          (is (= (:action (#'mq/claim! q 64)) :idle))
          [(is (= (:msg (mq/dead-info q mid)) :active))
           (is (nil? (car/wcar @mgr_
                      (car/hget (:successor-payloads keys) mid)))
             "The malformed successor was not promoted")]
          (mq/msg-remove! q mid)))

      (testing "Expired invalid active work may promote only a valid successor"
        (let [mid "corrupt-active-valid-successor"
              keys (queue-keys q)]
          (mq/msg-enqueue! q :active {:mid mid})
          (#'mq/claim! q 64)
          (mq/msg-enqueue! q :successor {:mid mid})
          (inject-raw-payload! q :payloads mid raw)
          (car/wcar @mgr_ (car/zadd (:leased keys) 0 mid))
          (let [claim (#'mq/claim! q 64)]
            [(is (= (:mid claim) mid))
             (is (= (:msg claim) :successor)
               "The invalid active payload was removed rather than delivered")
             (is (= (:action (#'mq/settle! q mid (:token claim)
                               (mq/outcome:ack)))
                   :acked))])))

      (testing "A dead invalid envelope is explicit and every mutation preflight is read-only"
        (let [mid "corrupt-dead-envelope"]
          (seed-dead! q mid)
          (mq/msg-enqueue! q :coexisting-active {:mid mid})
          (inject-raw-payload! q :dead-payloads mid raw)
          (let [before (raw-mid-fingerprint q mid)]
            [(is (= (mq/msg-info q mid) {:status :corrupt}))
             (is (= (mq/dead-info q mid)
                   {:mid mid, :status :corrupt
                    :corruption :invalid-payload-envelope}))
             (is (= (:status (page-item :ready false mid)) :ready))
             (is (= (:status (page-item :ready true mid)) :corrupt)
               "The already-fetched related dead envelope is validated")
             (is (= (mq/msg-enqueue! q :replacement {:mid mid})
                   {:success? false, :error :corrupt-dead, :mid mid}))
             (is (= (mq/dead-redrive! q mid)
                   {:success? false, :action :corrupt}))
             (is (= (raw-mid-fingerprint q mid) before)
               "Inspection, enqueue, and redrive preserve every artifact")])
          (mq/msg-remove! q mid)))
      (finally (remove-v4-queue!! q)))))

(deftest _claim-maintenance-cleanups-are-exactly-observable
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 60000, :retry-base-ms 60000, :retry-max-ms 60000})
        keys (queue-keys q)
        corrupt-envelope "out-of-band-unmarked-payload"]
    (try
      (testing "Scheduled cleanup tallies survive a healthy claim in the same round"
        (let [orphan-mid "scheduled-orphan"
              corrupt-payload-mid "scheduled-corrupt-payload"
              corrupt-meta-mid "scheduled-corrupt-meta"
              healthy-mid "scheduled-healthy"
              mids [orphan-mid corrupt-payload-mid corrupt-meta-mid healthy-mid]
              cleanups_ (atom nil)]
          (doseq [mid mids]
            (mq/msg-enqueue! q mid {:mid mid, :delay-ms 60000}))
          (car/wcar @mgr_
            (car/hdel (:payloads keys) orphan-mid)
            (car/zadd (:scheduled keys) 0 orphan-mid)
            (car/zadd (:scheduled keys) 0 corrupt-payload-mid)
            (car/zadd (:scheduled keys) 0 corrupt-meta-mid)
            (car/zadd (:scheduled keys) 0 healthy-mid))
          (inject-raw-payload! q :payloads corrupt-payload-mid corrupt-envelope)
          (mutate-packed! q :meta corrupt-meta-mid "v[2]=9;")
          (let [claim (#'mq/claim! q 64 #(reset! cleanups_ %))]
            [(is (= @cleanups_
                   {:orphan 1, :corrupt-meta 1, :corrupt-payload 1
                    :corrupt-index 0}))
             (is (= (select-keys claim [:action :mid :msg])
                    {:action :handle, :mid healthy-mid, :msg healthy-mid}))
             (is (= (:action (#'mq/settle! q healthy-mid (:token claim)
                               (mq/outcome:ack)))
                    :acked))])
          (doseq [mid [orphan-mid corrupt-payload-mid corrupt-meta-mid]]
            (is (nil? (mq/msg-info q mid)) mid))))

      (testing "Expired cleanup reaches idle, updates worker stats, and calls no handler"
        (let [orphan-mid "expired-orphan"
              corrupt-payload-mid "expired-corrupt-payload"
              corrupt-meta-mid "expired-corrupt-meta"
              mids [orphan-mid corrupt-payload-mid corrupt-meta-mid]]
          (doseq [mid mids]
            (mq/msg-enqueue! q mid {:mid mid})
            (let [claim (#'mq/claim! q 64)]
              (is (= (:mid claim) mid))))
          (car/wcar @mgr_
            (car/hdel (:payloads keys) orphan-mid)
            (car/zadd (:leased keys) 0 orphan-mid)
            (car/zadd (:leased keys) 0 corrupt-payload-mid)
            (car/zadd (:leased keys) 0 corrupt-meta-mid))
          (inject-raw-payload! q :payloads corrupt-payload-mid corrupt-envelope)
          (mutate-packed! q :meta corrupt-meta-mid "v[2]=9;")
          (let [handler-calls_ (atom 0)
                processed (promise)
                worker
                (mq/worker-create q
                  (fn [_]
                    (swap! handler-calls_ inc)
                    (mq/outcome:ack))
                  {:idle-min-ms 1, :idle-max-ms 1})]
            (try
              (with-redefs [mq/await-wake!
                            (fn [& _]
                              (deliver processed true)
                              (mq/worker-stop! worker))]
                [(is (mq/worker-start! worker))
                 (is (= (deref processed 2000 :timeout) true))
                 (is (mq/worker-await-stopped! worker 2000))])
              [(is (zero? @handler-calls_))
               (is (= (get-in (mq/worker-stats worker)
                        [:counts :claim-skips])
                      {:orphan 1, :corrupt-meta 1, :corrupt-payload 1
                       :corrupt-index 0})
                 "Timed cleanup is counted once; idle adds no primary skip")]
              (finally (.close ^java.io.Closeable worker))))
          (doseq [mid mids]
            (is (nil? (mq/msg-info q mid)) mid))))

      (testing "An error reply retains tallies for cleanups already committed"
        (let [orphan-mid "a-error-orphan"
              healthy-mid "z-error-healthy"
              cleanups_ (atom nil)]
          (mq/msg-enqueue! q :orphan {:mid orphan-mid, :delay-ms 60000})
          (mq/msg-enqueue! q :healthy {:mid healthy-mid, :delay-ms 60000})
          (car/wcar @mgr_
            (car/hdel (:payloads keys) orphan-mid)
            (car/zadd (:scheduled keys) 0 orphan-mid)
            (car/zadd (:scheduled keys) 0 healthy-mid))
          (set-seq! q "malformed")
          (let [error (truss/throws
                        (#'mq/claim! q 2 #(reset! cleanups_ %)))]
            [(is (truss/submap? (ex-data error)
                   {:eid :carmine.mq/claim-failed, :stage :claim, :error :corrupt-seq}))
             (is (= @cleanups_
                    {:orphan 1, :corrupt-meta 0, :corrupt-payload 0
                     :corrupt-index 0}))
             (is (nil? (mq/msg-info q orphan-mid)))
             (is (= (mq/msg-status q healthy-mid) :scheduled))])
          (set-seq! q "1000")
          (mq/msg-remove! q healthy-mid)))
      (finally (remove-v4-queue!! q)))))

(deftest _claim-preserves-possible-in-flight-roles
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 60000, :max-attempts 3
             :retry-base-ms 60000, :retry-max-ms 60000})
        keys (queue-keys q)]
    (try
      (testing "A duplicate ready index cannot overwrite a live lease token"
        (mq/msg-enqueue! q :leased {:mid "ready-plus-leased"})
        (car/wcar @mgr_
          (car/zadd (:ready-high keys) 999 "ready-plus-leased"))
        (let [held (#'mq/claim! q 64)
              token (:token held)]
          (let [skipped (#'mq/claim! q 64)]
            [(is (= skipped {:action :skip, :reason :corrupt-index}))
             (is (= (:status (mq/msg-info q "ready-plus-leased")) :leased))
             (is (nil? (car/wcar @mgr_
                         (car/zscore (:ready-high keys) "ready-plus-leased"))))
             (is (= (car/wcar @mgr_
                      (car/hget (:lease-tokens keys) "ready-plus-leased"))
                    token))
             (is (= (:action (#'mq/settle! q "ready-plus-leased" token
                               (mq/outcome:ack)))
                    :acked))])))

      (testing "A token can preserve a handler whose lease index was damaged"
        (mq/msg-enqueue! q :token-only {:mid "ready-plus-token"})
        (let [held (#'mq/claim! q 64)
              token (:token held)]
          (car/wcar @mgr_
            (car/zrem (:leased keys) "ready-plus-token")
            (car/zadd (:ready-normal keys) 1000 "ready-plus-token"))
          (let [skipped (#'mq/claim! q 64)]
            [(is (= skipped {:action :skip, :reason :corrupt-index}))
             (is (nil? (car/wcar @mgr_
                         (car/zscore (:ready-normal keys) "ready-plus-token"))))
             (is (= (car/wcar @mgr_
                      (car/hget (:lease-tokens keys) "ready-plus-token"))
                    token))
             (is (= (:action (#'mq/settle! q "ready-plus-token" token
                               (mq/outcome:ack)))
                    :acked))])))

      (testing "Due maintenance removes only a stray scheduled occurrence"
        (mq/msg-enqueue! q :scheduled {:mid "scheduled-plus-leased"})
        (let [held (#'mq/claim! q 64)
              token (:token held)]
          (car/wcar @mgr_
            (car/zadd (:scheduled keys) 0 "scheduled-plus-leased"))
          (let [claim (mq/msg-claim! q)]
            [(is (= (:action claim) :idle))
             (is (= (:maintenance claim)
                   {:orphan 0, :corrupt-meta 0, :corrupt-payload 0
                    :corrupt-index 1}))
             (is (= (:status (mq/msg-info q "scheduled-plus-leased")) :leased))
             (is (= (:action (#'mq/settle! q "scheduled-plus-leased" token
                               (mq/outcome:ack)))
                    :acked))])))

      (testing "Expiry maintenance removes producer indexes before backoff"
        (mq/msg-enqueue! q :expired {:mid "expired-with-strays"})
        (let [held (#'mq/claim! q 64)
              future (+ (:server-time-ms held) 120000)]
          (car/wcar @mgr_
            (car/zadd (:ready-high keys) 1001 "expired-with-strays")
            (car/zadd (:scheduled keys) future "expired-with-strays")
            (car/zadd (:leased keys) 0 "expired-with-strays"))
          (let [claim (mq/msg-claim! q)
                counts (:counts (mq/queue-status q))]
            [(is (= (:action claim) :idle))
             (is (= (:maintenance claim)
                   {:orphan 0, :corrupt-meta 0, :corrupt-payload 0
                    :corrupt-index 1}))
             (is (= (select-keys counts [:ready :scheduled :leased])
                   {:ready 0, :scheduled 1, :leased 0}))
             (is (= (mq/msg-status q "expired-with-strays") :scheduled))])))
      (finally (remove-v4-queue!! q)))))

(deftest _mq-unthawable-payloads-are-token-fenced-and-never-delivered
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :lease-ms 60000
             :max-attempts 3, :retry-base-ms 0, :retry-max-ms 0})
        bare-marker [0 62]
        invalid-nippy [0 62 78 80 89 0 255]]
    (try
      (testing "A bare Carmine marker fails the cheap Lua envelope preflight"
        (mq/msg-enqueue! q :poison {:mid "bare-marker"})
        (inject-raw-payload-bytes! q :payloads "bare-marker" bare-marker)
        [(is (= (mq/msg-info q "bare-marker") {:status :corrupt}))
         (is (= (#'mq/claim! q 64)
               {:action :skip, :reason :corrupt-payload}))
         (is (nil? (mq/msg-info q "bare-marker")))])

      (testing "A valid structural header with an invalid body is contained after thaw"
        (mq/msg-enqueue! q :active {:mid "codec-successor"})
        (let [active (#'mq/claim! q 64)]
          (mq/msg-enqueue! q :successor {:mid "codec-successor"})
          (#'mq/settle! q "codec-successor" (:token active)
            (mq/outcome:retry {:delay-ms 0})))
        (inject-raw-payload-bytes! q :payloads "codec-successor" invalid-nippy)
        (let [contained (#'mq/claim! q 64)]
          [(is (= contained {:action :skip, :reason :corrupt-payload}))
           (is (not (com/reply-error? (:msg contained)))
             "The nested thaw ReplyError is never exposed as :msg")])
        (let [successor (#'mq/claim! q 64)]
          [(is (= (select-keys successor [:action :mid :msg])
                 {:action :handle, :mid "codec-successor", :msg :successor})
             "Containment promotes the valid successor generation")
           (is (= (:action (#'mq/settle! q "codec-successor" (:token successor)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "A stale containment token cannot remove a newer leased generation"
        (mq/msg-enqueue! q :stale {:mid "codec-stale"})
        (let [claim (#'mq/claim! q 64)
              before (raw-mid-fingerprint q "codec-stale")]
          [(is (= (#'mq/contain-corrupt-payload!
                    q "codec-stale" "definitely-stale" (byte-array 0))
                 {:action :stale}))
           (is (= (raw-mid-fingerprint q "codec-stale") before))
           (is (= (:action (#'mq/settle! q "codec-stale" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "Workers count and contain codec poison without calling handlers"
        (mq/msg-enqueue! q :poison {:mid "worker-codec-poison"})
        (inject-raw-payload-bytes! q :payloads "worker-codec-poison" invalid-nippy)
        (let [handler-calls_ (atom 0)
              worker (mq/worker-create q
                       (fn [_]
                         (swap! handler-calls_ inc)
                         (mq/outcome:ack))
                       {:idle-min-ms 5, :idle-max-ms 20})]
          (try
            (mq/worker-start! worker)
            [(is (wait-for 2000 #(nil? (mq/msg-info q "worker-codec-poison"))))
             (is (zero? @handler-calls_))
             (is (= (get-in (mq/worker-stats worker)
                      [:counts :claim-skips :corrupt-payload])
                   1))]
            (finally
              (mq/worker-stop! worker)
              (mq/worker-await-stopped! worker 2000)))))

      (testing "Dead inspection and redrive reject unthawable bodies read-only"
        (seed-dead! q "dead-codec-poison")
        (inject-raw-payload-bytes! q :dead-payloads
          "dead-codec-poison" invalid-nippy)
        (let [before (raw-mid-fingerprint q "dead-codec-poison")]
          [(is (= (mq/dead-info q "dead-codec-poison")
                 {:mid "dead-codec-poison", :status :corrupt
                  :corruption :invalid-payload-codec}))
           (is (= (mq/dead-redrive! q "dead-codec-poison")
                 {:success? false, :action :corrupt}))
           (is (= (raw-mid-fingerprint q "dead-codec-poison") before))])
        (mq/msg-remove! q "dead-codec-poison"))

      (testing "Redrive cannot commit a dead generation changed after codec inspection"
        (seed-dead! q "redrive-generation-race")
        (let [original @#'mq/run-lua-durable]
          (is (=
                (with-redefs [mq/run-lua-durable
                              (fn [queue script keys args durability write-effecting?]
                                (mutate-packed! q :failures
                                  "redrive-generation-race"
                                  "v[2]='changed-after-inspect';")
                                (original queue script keys args durability
                                  write-effecting?))]
                  (mq/dead-redrive! q "redrive-generation-race"))
                {:success? false, :action :changed})))
        [(is (= (:reason (mq/dead-info q "redrive-generation-race"))
               "changed-after-inspect"))
         (is (= (mq/msg-status q "redrive-generation-race") :dead))]
        (mq/msg-remove! q "redrive-generation-race"))

      (testing "Fatal local decoder failures are never durable corruption"
        (let [mid "fatal-claim-decode"
              fatal (LinkageError. "simulated fatal Nippy decoder failure")]
          (mq/msg-enqueue! q :valid-payload {:mid mid})
          (let [payload-digest (raw-payload-digest q mid)
                error
                (with-redefs [nippy/thaw (fn [& _] (throw fatal))]
                  (truss/throws (#'mq/claim! q 64)))]
            [(is (identical? error fatal))
             (is (= (mq/msg-status q mid) :leased)
               "The successfully leased generation is not contained as poison")
             (is (= (select-keys (:counts (mq/queue-status q)) [:active :leased])
                    {:active 1, :leased 1}))
             (is (= (raw-payload-digest q mid) payload-digest)
               "The exact valid payload remains durable")])
          (mq/msg-remove! q mid))

        (let [mid "fatal-dead-decode"
              fatal (LinkageError. "simulated fatal dead decoder failure")]
          (seed-dead! q mid)
          (let [before (raw-mid-fingerprint q mid)
                [info-error redrive-error]
                (with-redefs [nippy/thaw (fn [& _] (throw fatal))]
                  [(truss/throws (mq/dead-info q mid))
                   (truss/throws (mq/dead-redrive! q mid))])]
            [(is (identical? info-error fatal))
             (is (identical? redrive-error fatal))
             (is (= (raw-mid-fingerprint q mid) before)
               "Inspection and redrive preflight leave the dead generation exact")
             (is (= (mq/msg-status q mid) :dead))])
          (mq/msg-remove! q mid)))

      (testing "Byte arrays and ordinary ExceptionInfo remain legitimate payloads"
        (let [bytes (byte-array [0 62 78 80 89 0 -1])
              ordinary (ex-info "ordinary message"
                         {:eid :carmine.read/nippy-thaw-error})]
          (mq/msg-enqueue! q bytes {:mid "valid-byte-array"})
          (mq/msg-enqueue! q ordinary {:mid "valid-ex-info"})
          (let [byte-claim (#'mq/claim! q 64)]
            [(is (enc/ba= (:msg byte-claim) bytes))
             (is (= (:action (#'mq/settle! q (:mid byte-claim) (:token byte-claim)
                               (mq/outcome:ack)))
                   :acked))])
          (let [ex-claim (#'mq/claim! q 64)
                msg (:msg ex-claim)]
            [(is (instance? clojure.lang.ExceptionInfo msg))
             (is (not (com/reply-error? msg)))
             (is (= (ex-data msg) (ex-data ordinary)))
             (is (= (:action (#'mq/settle! q (:mid ex-claim) (:token ex-claim)
                               (mq/outcome:ack)))
                   :acked))])))
      (finally (remove-v4-queue!! q)))))

(deftest _mq-advisory-signal-wrongtype-never-breaks-durable-transitions
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :lease-ms 60000
             :retry-base-ms 60000, :retry-max-ms 60000})]
    (try
      (testing "Ready and delayed enqueue repair only the advisory key"
        (make-signal-wrongtype! q)
        [(is (= (:action (mq/msg-enqueue! q :ready {:mid "signal-enqueue"})) :added))
         (is (= (mq/msg-status q "signal-enqueue") :ready))
         (is (= (signal-count q) 1))]
        (let [claim (#'mq/claim! q 64)]
          (#'mq/settle! q (:mid claim) (:token claim) (mq/outcome:ack)))

        (make-signal-wrongtype! q)
        [(is (= (:action (mq/msg-enqueue! q :scheduled
                         {:mid "signal-enqueue-delayed", :delay-ms 60000}))
               :added))
         (is (= (mq/msg-status q "signal-enqueue-delayed") :scheduled))
         (is (= (signal-count q) 1))]
        (mq/msg-remove! q "signal-enqueue-delayed"))

      (testing "Idle and ready claims repair wrongtype without partial pops"
        (make-signal-wrongtype! q)
        [(is (= (:action (#'mq/claim! q 64)) :idle))
         (is (zero? (signal-count q))
           "An idle round removes wrongtype without manufacturing a baton")]

        (mq/msg-enqueue! q :claim {:mid "signal-claim"})
        (make-signal-wrongtype! q)
        (let [claim (#'mq/claim! q 64)]
          [(is (= (select-keys claim [:action :mid :msg])
                 {:action :handle, :mid "signal-claim", :msg :claim}))
           (is (= (mq/msg-status q "signal-claim") :leased))
           (is (= (signal-count q) 1))]
          (#'mq/settle! q (:mid claim) (:token claim) (mq/outcome:ack))))

      (testing "Immediate/delayed retry and release remain coherent"
        (mq/msg-enqueue! q :retry-now {:mid "signal-retry-now"})
        (let [claim (#'mq/claim! q 64)]
          (make-signal-wrongtype! q)
          [(is (= (:action (#'mq/settle! q (:mid claim) (:token claim)
                             (mq/outcome:retry {:delay-ms 0})))
                 :retried))
           (is (= (mq/msg-status q "signal-retry-now") :ready))
           (is (= (signal-count q) 1))])
        (let [claim (#'mq/claim! q 64)]
          (#'mq/settle! q (:mid claim) (:token claim) (mq/outcome:ack)))

        (mq/msg-enqueue! q :retry-later {:mid "signal-retry-later"})
        (let [claim (#'mq/claim! q 64)]
          (make-signal-wrongtype! q)
          [(is (= (:action (#'mq/settle! q (:mid claim) (:token claim)
                             (mq/outcome:retry {:delay-ms 60000})))
                 :retried))
           (is (= (mq/msg-status q "signal-retry-later") :scheduled))
           (is (= (signal-count q) 1))])
        (mq/msg-remove! q "signal-retry-later")

        (mq/msg-enqueue! q :release {:mid "signal-release"})
        (let [claim (#'mq/claim! q 64)]
          (make-signal-wrongtype! q)
          [(is (= (#'mq/release! q (:mid claim) (:token claim)) :released))
           (is (= (mq/msg-status q "signal-release") :ready))
           (is (= (signal-count q) 1))])
        (let [claim (#'mq/claim! q 64)]
          (#'mq/settle! q (:mid claim) (:token claim) (mq/outcome:ack))))

      (testing "Scheduled and expired maintenance keep their durable transition"
        (let [keys (queue-keys q)]
          (mq/msg-enqueue! q :due {:mid "signal-due", :delay-ms 60000})
          (car/wcar @mgr_ (car/zadd (:scheduled keys) 0 "signal-due"))
          (make-signal-wrongtype! q)
          (let [claim (#'mq/claim! q 64)]
            [(is (= (:mid claim) "signal-due"))
             (is (= (signal-count q) 1))]
            (#'mq/settle! q (:mid claim) (:token claim) (mq/outcome:ack)))

          (mq/msg-enqueue! q :expired {:mid "signal-expired"})
          (#'mq/claim! q 64)
          (car/wcar @mgr_ (car/zadd (:leased keys) 0 "signal-expired"))
          (make-signal-wrongtype! q)
          [(is (= (:action (#'mq/claim! q 64)) :idle))
           (is (= (mq/msg-status q "signal-expired") :scheduled))
           (is (= (signal-count q) 1))]
          (mq/msg-remove! q "signal-expired")))

      (testing "Settle/release successor promotion and redrive repair the baton"
        (mq/msg-enqueue! q :active {:mid "signal-settle-successor"})
        (let [active (#'mq/claim! q 64)]
          (mq/msg-enqueue! q :successor {:mid "signal-settle-successor"})
          (make-signal-wrongtype! q)
          [(is (:successor-promoted?
                 (#'mq/settle! q (:mid active) (:token active)
                   (mq/outcome:ack))))
           (is (= (mq/msg-status q "signal-settle-successor") :ready))
           (is (= (signal-count q) 1))])
        (let [claim (#'mq/claim! q 64)]
          (#'mq/settle! q (:mid claim) (:token claim) (mq/outcome:ack)))

        (mq/msg-enqueue! q :active {:mid "signal-release-successor"})
        (let [active (#'mq/claim! q 64)]
          (mq/msg-enqueue! q :successor {:mid "signal-release-successor"})
          (make-signal-wrongtype! q)
          [(is (= (#'mq/release! q (:mid active) (:token active))
                 :released-successor))
           (is (= (mq/msg-status q "signal-release-successor") :ready))
           (is (= (signal-count q) 1))])
        (let [claim (#'mq/claim! q 64)]
          (#'mq/settle! q (:mid claim) (:token claim) (mq/outcome:ack)))

        (seed-dead! q "signal-redrive")
        (make-signal-wrongtype! q)
        [(is (= (mq/dead-redrive! q "signal-redrive")
               {:success? true, :action :redriven}))
         (is (= (mq/msg-status q "signal-redrive") :ready))
         (is (= (signal-count q) 1))]
        (mq/msg-remove! q "signal-redrive"))

      (testing "Expired terminal maintenance may promote a valid successor"
        (let [terminal-q (mq/queue mgr_ (qname)
                           {:on-duplicate :coalesce, :lease-ms 60000
                            :max-attempts 1})
              keys (queue-keys terminal-q)]
          (try
            (mq/msg-enqueue! terminal-q :active {:mid "signal-expired-successor"})
            (#'mq/claim! terminal-q 64)
            (mq/msg-enqueue! terminal-q :successor
              {:mid "signal-expired-successor"})
            (car/wcar @mgr_
              (car/zadd (:leased keys) 0 "signal-expired-successor"))
            (make-signal-wrongtype! terminal-q)
            (let [claim (#'mq/claim! terminal-q 64)]
              [(is (= (:mid claim) "signal-expired-successor"))
               (is (= (:msg claim) :successor))
               (is (= (signal-count terminal-q) 1))]
              (#'mq/settle! terminal-q (:mid claim) (:token claim)
                (mq/outcome:ack)))
            (finally (remove-v4-queue!! terminal-q)))))
      (finally (remove-v4-queue!! q)))))

(deftest _mq-ready-sequence-is-reserved-before-every-producing-transition
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :lease-ms 60000
             :retry-base-ms 60000, :retry-max-ms 60000})]
    (try
      (testing "Raw sequence validation distinguishes corruption from exhaustion"
        (doseq [[idx value expected]
                (map vector (range)
                  [:wrongtype "" "01" "-1" "1.0" "not-a-sequence"
                   "9007199254740991" "9007199254740992"
                   "9223372036854775807"]
                  [:corrupt-seq :corrupt-seq :corrupt-seq :corrupt-seq
                   :corrupt-seq :corrupt-seq :seq-exhausted :seq-exhausted
                   :seq-exhausted])]
          (mq/queue-clear!! q)
          (set-seq! q value)
          (let [mid (str "seq-invalid-enqueue-" idx)
                before (raw-seq q)]
            [(is (= (mq/msg-enqueue! q :payload {:mid mid})
                   {:success? false, :error expected, :mid mid})
               (str value))
             (is (nil? (mq/msg-info q mid)) (str value))
             (is (= (raw-seq q) before)
               "A refused reservation never rewrites the sequence")]))

        (mq/queue-clear!! q)
        (set-seq! q "9007199254740990")
        [(is (= (:action (mq/msg-enqueue! q :last-exact
                          {:mid "seq-last-exact"}))
               :added))
         (is (= (raw-seq q) ["string" "9007199254740991"]))
         (is (= (mq/msg-enqueue! q :too-late {:mid "seq-after-last"})
               {:success? false, :error :seq-exhausted
                :mid "seq-after-last"}))
         (is (nil? (mq/msg-info q "seq-after-last")))]
        (mq/queue-clear!! q))

      (testing "New/coalesced enqueue reserves before writing or reindexing"
        (mq/msg-enqueue! q :old {:mid "seq-coalesce"})
        (set-seq! q "malformed")
        (let [before (raw-mid-fingerprint q "seq-coalesce")]
          [(is (= (mq/msg-enqueue! q :new
                    {:mid "seq-coalesce", :priority :high})
                 {:success? false, :error :corrupt-seq, :mid "seq-coalesce"}))
           (is (= (raw-mid-fingerprint q "seq-coalesce") before))
           (is (= (mq/msg-status q "seq-coalesce") :ready))])
        (set-seq! q "100")
        (mq/msg-remove! q "seq-coalesce"))

      (testing "Immediate retry and release leave the original lease untouched"
        (mq/msg-enqueue! q :retry {:mid "seq-retry"})
        (let [claim (#'mq/claim! q 64)]
          (set-seq! q "malformed")
          (let [before (raw-mid-fingerprint q "seq-retry")]
            [(is (throws? :ex-info
                   {:eid :carmine.mq/settlement-failed
                    :error :corrupt-seq, :mid "seq-retry"}
                   (#'mq/settle! q "seq-retry" (:token claim)
                     (mq/outcome:retry {:delay-ms 0}))))
             (is (= (raw-mid-fingerprint q "seq-retry") before))])
          (set-seq! q "200")
          (#'mq/settle! q "seq-retry" (:token claim) (mq/outcome:ack)))

        (mq/msg-enqueue! q :release {:mid "seq-release"})
        (let [claim (#'mq/claim! q 64)]
          (set-seq! q :wrongtype)
          (let [before (raw-mid-fingerprint q "seq-release")]
            [(is (throws? :ex-info
                   {:eid :carmine.mq/release-failed
                    :error :corrupt-seq, :mid "seq-release"}
                   (#'mq/release! q "seq-release" (:token claim))))
             (is (= (raw-mid-fingerprint q "seq-release") before))])
          (set-seq! q "300")
          (#'mq/settle! q "seq-release" (:token claim) (mq/outcome:ack)))

        (mq/msg-enqueue! q :delayed-retry {:mid "seq-delayed-retry"})
        (let [claim (#'mq/claim! q 64)]
          (set-seq! q "malformed")
          [(is (= (:action (#'mq/settle! q "seq-delayed-retry" (:token claim)
                             (mq/outcome:retry {:delay-ms 60000})))
                 :retried)
             "A transition that produces only scheduled work needs no FIFO score")
           (is (= (mq/msg-status q "seq-delayed-retry") :scheduled))
           (is (= (raw-seq q) ["string" "malformed"]))]
          (mq/msg-remove! q "seq-delayed-retry")))

      (testing "Due maintenance refuses the current record before removing its old role"
        (mq/msg-enqueue! q :due {:mid "seq-due", :delay-ms 60000})
        (let [keys (queue-keys q)]
          (car/wcar @mgr_ (car/zadd (:scheduled keys) 0 "seq-due"))
          (set-seq! q "malformed")
          (let [before (raw-mid-fingerprint q "seq-due")]
            [(is (throws? :ex-info
                   {:eid :carmine.mq/claim-failed, :stage :claim, :error :corrupt-seq}
                   (#'mq/claim! q 64)))
             (is (= (raw-mid-fingerprint q "seq-due") before))
             (is (= (mq/msg-status q "seq-due") :scheduled))])
          (set-seq! q "400")
          (let [claim (#'mq/claim! q 64)]
            (is (= (:mid claim) "seq-due"))
            (#'mq/settle! q (:mid claim) (:token claim) (mq/outcome:ack)))))

      (testing "A bounded maintenance batch retains earlier commits and stops at the refusal"
        (mq/msg-enqueue! q :a {:mid "seq-batch-a", :delay-ms 60000})
        (mq/msg-enqueue! q :b {:mid "seq-batch-b", :delay-ms 60000})
        (let [scheduled (get (queue-keys q) :scheduled)]
          (car/wcar @mgr_
            (car/zadd scheduled 0 "seq-batch-a")
            (car/zadd scheduled 0 "seq-batch-b"))
          (set-seq! q "9007199254740990")
          (let [before-b (raw-mid-fingerprint q "seq-batch-b")]
            [(is (throws? :ex-info
                   {:eid :carmine.mq/claim-failed, :stage :claim, :error :seq-exhausted}
                   (#'mq/claim! q 2)))
             (is (= (mq/msg-status q "seq-batch-a") :ready)
               "The earlier item committed with the last exact score")
             (is (= (raw-mid-fingerprint q "seq-batch-b") before-b)
               "The refusing current item remains in its scheduled role")
             (is (= (mq/msg-status q "seq-batch-b") :scheduled))
             (is (= (raw-seq q) ["string" "9007199254740991"]))])
          (set-seq! q "450")
          (mq/msg-remove! q "seq-batch-a")
          (mq/msg-remove! q "seq-batch-b")))

      (testing "Settle and release reserve before successor promotion"
        (mq/msg-enqueue! q :active {:mid "seq-settle-successor"})
        (let [active (#'mq/claim! q 64)]
          (mq/msg-enqueue! q :successor {:mid "seq-settle-successor"})
          (set-seq! q "9007199254740991")
          (let [before (raw-mid-fingerprint q "seq-settle-successor")]
            [(is (throws? :ex-info
                   {:eid :carmine.mq/settlement-failed
                    :error :seq-exhausted}
                   (#'mq/settle! q "seq-settle-successor" (:token active)
                     (mq/outcome:ack))))
             (is (= (raw-mid-fingerprint q "seq-settle-successor") before))])
          (set-seq! q "500")
          (is (:successor-promoted?
                (#'mq/settle! q "seq-settle-successor" (:token active)
                  (mq/outcome:ack))))
          (mq/msg-remove! q "seq-settle-successor"))

        (mq/msg-enqueue! q :active {:mid "seq-release-successor"})
        (let [active (#'mq/claim! q 64)]
          (mq/msg-enqueue! q :successor {:mid "seq-release-successor"})
          (set-seq! q "malformed")
          (let [before (raw-mid-fingerprint q "seq-release-successor")]
            [(is (throws? :ex-info
                   {:eid :carmine.mq/release-failed
                    :error :corrupt-seq, :mid "seq-release-successor"}
                   (#'mq/release! q "seq-release-successor" (:token active))))
             (is (= (raw-mid-fingerprint q "seq-release-successor") before))])
          (set-seq! q "600")
          (is (= (#'mq/release! q "seq-release-successor" (:token active))
                :released-successor))
          (mq/msg-remove! q "seq-release-successor")))

      (testing "Expired maintenance reserves before terminal successor promotion"
        (let [terminal-q (mq/queue mgr_ (qname)
                           {:on-duplicate :coalesce, :lease-ms 60000
                            :max-attempts 1})
              keys (queue-keys terminal-q)]
          (try
            (mq/msg-enqueue! terminal-q :active {:mid "seq-expired-successor"})
            (#'mq/claim! terminal-q 64)
            (mq/msg-enqueue! terminal-q :successor
              {:mid "seq-expired-successor"})
            (car/wcar @mgr_ (car/zadd (:leased keys) 0 "seq-expired-successor"))
            (set-seq! terminal-q "malformed")
            (let [before (raw-mid-fingerprint terminal-q "seq-expired-successor")]
              [(is (throws? :ex-info
                     {:eid :carmine.mq/claim-failed, :stage :claim, :error :corrupt-seq}
                     (#'mq/claim! terminal-q 64)))
               (is (= (raw-mid-fingerprint terminal-q "seq-expired-successor") before))
               (is (= (mq/msg-status terminal-q "seq-expired-successor")
                     :lease-expired))])
            (set-seq! terminal-q "700")
            (let [successor (#'mq/claim! terminal-q 64)]
              [(is (= (:msg successor) :successor))
               (is (= (:action (#'mq/settle! terminal-q (:mid successor)
                                 (:token successor) (mq/outcome:ack)))
                     :acked))])
            (finally (remove-v4-queue!! terminal-q)))))

      (testing "Expired retry can remain scheduled while the FIFO sequence is corrupt"
        (mq/msg-enqueue! q :expired {:mid "seq-expired-scheduled"})
        (#'mq/claim! q 64)
        (car/wcar @mgr_
          (car/zadd (get (queue-keys q) :leased) 0 "seq-expired-scheduled"))
        (set-seq! q "malformed")
        [(is (= (:action (#'mq/claim! q 64)) :idle))
         (is (= (mq/msg-status q "seq-expired-scheduled") :scheduled))
         (is (= (raw-seq q) ["string" "malformed"]))]
        (mq/msg-remove! q "seq-expired-scheduled"))

      (testing "An immediate expired retry reserves before removing its lease"
        (let [immediate-q (mq/queue mgr_ (qname)
                            {:lease-ms 60000, :max-attempts 2
                             :retry-base-ms 0, :retry-max-ms 0})
              leased (get (queue-keys immediate-q) :leased)]
          (try
            (mq/msg-enqueue! immediate-q :expired
              {:mid "seq-expired-immediate"})
            (#'mq/claim! immediate-q 64)
            (car/wcar @mgr_ (car/zadd leased 0 "seq-expired-immediate"))
            (set-seq! immediate-q "malformed")
            (let [before (raw-mid-fingerprint immediate-q
                           "seq-expired-immediate")]
              [(is (throws? :ex-info
                     {:eid :carmine.mq/claim-failed, :stage :claim, :error :corrupt-seq}
                     (#'mq/claim! immediate-q 64)))
               (is (= (raw-mid-fingerprint immediate-q
                        "seq-expired-immediate")
                     before))
               (is (= (mq/msg-status immediate-q "seq-expired-immediate")
                     :lease-expired))])
            (set-seq! immediate-q "850")
            (let [claim (#'mq/claim! immediate-q 64)]
              [(is (= (:mid claim) "seq-expired-immediate"))
               (is (= (:attempt claim) 2))
               (#'mq/settle! immediate-q (:mid claim) (:token claim)
                 (mq/outcome:ack))])
            (finally (remove-v4-queue!! immediate-q)))))

      (testing "Dead redrive reserves before deleting the retained generation"
        (set-seq! q "800")
        (seed-dead! q "seq-redrive")
        (set-seq! q "9223372036854775807")
        (let [before (raw-mid-fingerprint q "seq-redrive")]
          [(is (= (mq/dead-redrive! q "seq-redrive")
                 {:success? false, :action :seq-exhausted}))
           (is (= (raw-mid-fingerprint q "seq-redrive") before))])
        (set-seq! q "900")
        [(is (= (mq/dead-redrive! q "seq-redrive")
               {:success? true, :action :redriven}))
         (is (= (mq/msg-status q "seq-redrive") :ready))]
        (mq/msg-remove! q "seq-redrive"))
      (finally (remove-v4-queue!! q)))))

(deftest _integer-option-validation
  (let [invalid-values
        [1.0 1.5 1M 3/2 0.9 -0.9 Double/NaN
         Double/POSITIVE_INFINITY Double/NEGATIVE_INFINITY
         nil "1" :one (inc (bigint Long/MAX_VALUE))]
        invalid
        (fn [validator option value]
          (ex-data (truss/throws :ex-info (validator option value))))
        enqueue-queue
        (#'mq/new-queue nil "option-validation"
          {:max-attempts 8, :on-exhaustion :dead,
           :on-duplicate :reject, :revision-mode :none, :durability nil}
          {})]
    ;; NB numeric options flow into Lua arithmetic/comparisons, so bounds
    ;; are the Lua-exact 2^53-1 rather than Long/MAX_VALUE:
    [(is (= (#'mq/positive-long :limit 1N) 1))
     (is (= (#'mq/positive-long :limit (java.math.BigInteger/valueOf 9007199254740991))
           9007199254740991))
     (is (= (#'mq/nonnegative-long :delay-ms 0N) 0))
     (is (= (#'mq/nonnegative-long :delay-ms (bigint 9007199254740991)) 9007199254740991))
     (is (= (#'mq/signed-long :start (bigint -9007199254740991)) -9007199254740991))
     (is (= (#'mq/signed-long :stop  (bigint  9007199254740991))  9007199254740991))]

    (testing "The shared integer-type guard rejects non-integers"
      (doseq [value invalid-values]
        (let [data (invalid #'mq/signed-long :start value)]
          [(is (= (:eid data) :carmine.mq/invalid-option))
           (is (= (:option data) :start))])))

    (testing "Diagnostics retain a representative invalid input"
      (let [data (invalid #'mq/nonnegative-long :delay-ms 1.5)]
        [(is (= (:eid data) :carmine.mq/invalid-option))
         (is (= (select-keys data [:option :value])
               {:option :delay-ms, :value 1.5}))]))

    (doseq [[validator option value]
            [[#'mq/positive-long :limit 0]
             [#'mq/positive-long :limit -1]
             [#'mq/positive-long :limit (inc 9007199254740991)]
             [#'mq/nonnegative-long :delay-ms -1]
             [#'mq/nonnegative-long :delay-ms (inc 9007199254740991)]
             [#'mq/signed-long :start (dec -9007199254740991)]
             [#'mq/signed-long :start Long/MAX_VALUE]]]
      (is (= (:eid (invalid validator option value))
            :carmine.mq/invalid-option)))

    (doseq [[f expected]
            [[#(mq/queue nil "option-validation" {:lease-ms 1.5})
              {:option :lease-ms, :value 1.5}]
             [#(mq/msg-enqueue! enqueue-queue :msg {:delay-ms 0.9})
              {:option :delay-ms, :value 0.9}]
             [#(mq/worker-create (inert-queue) identity {:concurrency 1.0})
              {:option :concurrency, :value 1.0}]
             [#(mq/dead-mids (inert-queue) 0.5 -1)
              {:option :start, :value 0.5}]]]
      (is (throws? :ex-info
            (assoc expected :eid :carmine.mq/invalid-option)
            (f))))))

(deftest _explicit-false-is-never-a-default-sentinel
  (let [enqueue-queue
        (#'mq/new-queue nil "false-option-validation"
          {:max-attempts 8, :on-exhaustion :dead
           :on-duplicate :reject, :revision-mode :none, :durability nil}
          {})]
    (testing "Enqueue defaults distinguish nil from false"
      (is (throws? :ex-info
            {:eid :carmine.mq/invalid-option
             :option :delay-ms, :value false}
            (mq/msg-enqueue! enqueue-queue :msg {:delay-ms false}))))

    (testing "Nested durability defaults distinguish nil from false"
      (is (throws? :ex-info
            {:eid :carmine.mq/invalid-option
             :option :durability/timeout-ms, :value false}
            (#'mq/normalize-durability :test
              {:replicas 1, :timeout-ms false}))))

    (testing "Administrative defaults distinguish false"
      (is (throws? :ex-info
            {:eid :carmine.mq/invalid-option
             :option :status, :value false}
            (mq/msg-active-page enqueue-queue {:status false}))))

    (testing "Outcome defaults distinguish nil from false"
      [(is (= (.-reason ^taoensso.carmine_v4.message_queue.HandlerOutcome
                    (mq/outcome:dead {:reason nil}))
              "handler-requested"))
       (is (throws? :ex-info
             {:eid :carmine.mq/invalid-outcome-options
              :context :dead, :option :reason, :value false}
             (mq/outcome:dead {:reason false})))])

    (testing "Nil still uses the documented optional defaults"
      (is (= (#'mq/normalize-durability :test
               {:replicas 1, :timeout-ms nil})
             {:mode :wait, :replicas 1, :timeout-ms 1000})))))

(deftest _worker-stats-schema-and-reset
  (let [worker (mq/worker-create (inert-queue) (constantly (mq/outcome:ack)))
        empty-timing
        {:count 0, :sum-ms 0.0, :min-ms nil, :max-ms nil, :last-ms nil}
        empty-counts
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
         :wake-waits 0, :wake-signals 0, :wake-errors 0
         :worker-errors 0, :worker-failures 0, :event-callback-errors 0}]
    (try
      [(is (throws? :ex-info {:eid :carmine.mq/invalid-timeout-ms}
             (mq/worker-await-stopped! worker -1)))
       (is (= (:state @worker) :new)
         "Invalid lifecycle input cannot stop the worker")]
      (let [stats (mq/worker-stats worker)]
        [(is (= (:schema-version stats) 1))
         (is (truss/submap? (:counts stats) empty-counts))
         (is (truss/submap? (:timings stats)
               {:claim-round-trip empty-timing
                :settlement-round-trip empty-timing
                :handler empty-timing
                :claim-age empty-timing
                :first-claim-age empty-timing}))
         (is (pos-int? (:since-client-time-ms stats)))
         (is (<= (:since-client-time-ms stats) (:snapshot-client-time-ms stats)))])
      (let [prior (mq/worker-clear-stats! worker)
            current (mq/worker-stats worker)]
        [(is (= (:snapshot-client-time-ms prior)
               (:since-client-time-ms current)))
         (is (truss/submap? (:counts current) empty-counts))
         (is (<= (:since-client-time-ms current)
               (:snapshot-client-time-ms current)))])
      [(is (mq/worker-stop! worker))
       (is (mq/worker-await-stopped! worker Long/MAX_VALUE)
         "Every accepted timeout remains usable without deadline overflow")]
      (finally (.close ^java.io.Closeable worker)))))

(deftest _worker-deadline-jitter-is-early-only
  (let [timeout #'mq/worker-idle-timeout-ms
        just-below-one (Math/nextDown 1.0)
        idle-min 10
        idle-max 1000]
    (testing "The idle minimum remains positive at and below its boundary"
      (doseq [until-next [0 9 10]
              sample [0.0 just-below-one]]
        (is (= (timeout idle-min idle-max until-next sample) idle-min)
          (str {:until-next until-next, :sample sample}))))

    (testing "Known deadlines at or above the minimum are never crossed"
      (doseq [until-next [11 500 1000 2000]
              sample [0.0 just-below-one]]
        (let [actual (timeout idle-min idle-max until-next sample)
              effective-deadline (min idle-max until-next)]
          [(is (<= idle-min actual effective-deadline)
             (str {:until-next until-next, :sample sample, :actual actual}))
           (is (pos? actual))])))))

(deftest _worker-lease-extension-stats
  (let [stats_ (atom (#'mq/new-worker-stats 1))
        expected-error (Exception. "Expected")]
    (with-redefs [mq/extend-lease! (fn [& _] 123)]
      (is (= (#'mq/tracked-extend-lease! stats_ nil "mid" "token") 123)))
    (with-redefs [mq/extend-lease! (fn [& _] nil)]
      (is (nil? (#'mq/tracked-extend-lease! stats_ nil "mid" "token"))))
    (with-redefs [mq/extend-lease! (fn [& _] (throw expected-error))]
      (is (identical? expected-error
            (truss/throws
              (#'mq/tracked-extend-lease! stats_ nil "mid" "token")))))
    (is (= (get-in @stats_ [:counts :lease-extensions])
          {:extended 1, :stale 1, :errors 1}))))

(deftest _worker-failed-round-trips-are-timed
  (testing "Claim failure"
    (let [worker_ (atom nil)
          failed (promise)
          worker (mq/worker-create (inert-queue) (constantly (mq/outcome:ack))
                   {:idle-min-ms 1, :idle-max-ms 1})]
      (reset! worker_ worker)
      (try
        (with-redefs [mq/claim!
                      (fn [& _]
                        (deliver failed true)
                        (mq/worker-stop! @worker_)
                        (throw (Exception. "Expected claim failure")))]
          (is (mq/worker-start! worker))
          (is (= (deref failed 2000 :timeout) true))
          (is (mq/worker-await-stopped! worker 2000)))
        (let [stats (mq/worker-stats worker)]
          [(is (= (get-in stats [:timings :claim-round-trip :count]) 1))
           (is (pos? (get-in stats [:timings :claim-round-trip :sum-ms])))
           (is (= (get-in stats [:counts :worker-errors]) 1))])
        (finally (.close ^java.io.Closeable worker)))))

  (testing "Settlement failure"
    (let [worker_ (atom nil)
          failed (promise)
          worker (mq/worker-create (inert-queue) (constantly (mq/outcome:ack))
                   {:idle-min-ms 1, :idle-max-ms 1})
          now (System/currentTimeMillis)
          claim
          {:action :handle, :mid "mid", :msg :msg, :attempt 1
           :token "token", :lease-expiry-ms (+ now 1000)
           :enqueued-at-ms now, :priority :normal, :server-time-ms now}]
      (reset! worker_ worker)
      (try
        (with-redefs [mq/claim! (fn [& _] claim)
                      mq/settle!
                      (fn [& _]
                        (deliver failed true)
                        (mq/worker-stop! @worker_)
                        (throw (Exception. "Expected settlement failure")))]
          (is (mq/worker-start! worker))
          (is (= (deref failed 2000 :timeout) true))
          (is (mq/worker-await-stopped! worker 2000)))
        (let [stats (mq/worker-stats worker)]
          [(is (= (get-in stats [:timings :claim-round-trip :count]) 1))
           (is (= (get-in stats [:timings :settlement-round-trip :count]) 1))
           (is (pos? (get-in stats [:timings :settlement-round-trip :sum-ms])))
           (is (= (get-in stats [:counts :handler-calls]) 1))
           (is (= (get-in stats [:counts :worker-errors]) 1))])
        (finally (.close ^java.io.Closeable worker))))))

(deftest _key-schema-contract
  (let [stem (str "carmine-v4-mq-key-schema-" (java.util.UUID/randomUUID))
        queue-name (str stem "{injected}")
        other-name (str stem "{other}")
        suffixes
        [:config :seq :payloads :meta :successor-payloads :successor-meta
         :ready-high :ready-normal :ready-low :scheduled :leased :lease-tokens
         :dead :dead-payloads :failures :signal]
        prefix
        (str "carmine:mq:v4:{"
          (subs (car/script-hash queue-name) 0 16) "}:" queue-name ":")
        expected-keys
        (into {} (map (fn [suffix] [suffix (str prefix (name suffix))])) suffixes)
        expected-types
        {:config "hash", :seq "string", :payloads "hash", :meta "hash"
         :successor-payloads "hash", :successor-meta "hash"
         :ready-high "zset", :ready-normal "zset", :ready-low "zset"
         :scheduled "zset", :leased "zset", :lease-tokens "hash"
         :dead "zset", :dead-payloads "hash", :failures "hash"
         :signal "list"}
        ns-doc (:doc (meta (the-ns 'taoensso.carmine-v4.message-queue)))
        keys-a (#'mq/qkeys queue-name)
        keys-b (#'mq/qkeys other-name)
        q (mq/queue mgr_ queue-name
            {:on-duplicate :coalesce, :lease-ms 60000
             :retry-base-ms 0, :retry-max-ms 0})]
    (try
      [(is (= keys-a expected-keys)
         "Physical key names and the complete suffix set are stable")
       (is (every?
             (fn [[suffix redis-type]]
               (re-find
                 (re-pattern
                   (str "(?m)^    - `" (name suffix) "`\\s+"
                     (.toUpperCase ^String redis-type java.util.Locale/ROOT)
                     "\\s"))
                 ns-doc))
             expected-types)
         "The top-level namespace documents every physical key and Redis type")
       (is (= 1
             (count
               (set
                 (map #(cluster/cluster-slot (cluster/cluster-key %))
                   (vals keys-a)))))
         "Every queue key occupies one Cluster slot")
       (is (not=
             (cluster/cluster-slot (cluster/cluster-key (:payloads keys-a)))
             (cluster/cluster-slot (cluster/cluster-key (:payloads keys-b))))
         "User-supplied braces cannot select the Cluster hash tag")]

      (mq/msg-enqueue! q :leased {:mid "leased"})
      (let [claim (#'mq/claim! q 64)]
        (is (= (:mid claim) "leased"))
        (mq/msg-enqueue! q :successor {:mid "leased"}))

      (mq/msg-enqueue! q :dead {:mid "dead"})
      (let [claim (#'mq/claim! q 64)]
        (is (= (:mid claim) "dead"))
        (#'mq/settle! q "dead" (:token claim)
          (mq/outcome:dead {:reason "key-schema"})))

      (mq/msg-enqueue! q :high   {:mid "high",   :priority :high})
      (mq/msg-enqueue! q :normal {:mid "normal", :priority :normal})
      (mq/msg-enqueue! q :low    {:mid "low",    :priority :low})
      (mq/msg-enqueue! q :scheduled {:mid "scheduled", :delay-ms 60000})

      (let [types
            (zipmap suffixes
              (car/wcar @mgr_ {:as-vec? true}
                (doseq [suffix suffixes]
                  (car/type (get keys-a suffix)))))
            pttls
            (car/wcar @mgr_ {:as-vec? true}
              (doseq [suffix suffixes]
                (car/pttl (get keys-a suffix))))]
        [(is (= types expected-types)
           "Every documented role uses its contracted Redis type")
         (is (= pttls (vec (repeat (count suffixes) -1)))
           "Every extant queue key is non-expiring")])

      (mq/queue-clear!! q)
      (let [types
            (zipmap suffixes
              (car/wcar @mgr_ {:as-vec? true}
                (doseq [suffix suffixes]
                  (car/type (get keys-a suffix)))))]
        (is (= types
              (assoc (zipmap suffixes (repeat "none")) :config "hash"))
          "Clear retains authoritative config and removes all queue state"))
      (finally
        (remove-v4-queue!! q)))))

(deftest _handler-outcome-helpers
  (let [outcomes
        [(mq/outcome:ack)
         (mq/outcome:retry)
         (mq/outcome:retry {:delay-ms 0, :reason "temporary"})
         (mq/outcome:dead)
         (mq/outcome:dead {:reason "terminal"})
         (mq/outcome:discard)]]
    [(is (apply = (map class outcomes)))
     (is (not-any? map? outcomes))])

  (doseq [f
          [#(mq/outcome:retry nil)
           #(mq/outcome:retry {:unexpected true})
           #(mq/outcome:retry {"delay-ms" 1})
           #(mq/outcome:retry {:delay-ms -1})
           #(mq/outcome:retry {:delay-ms 1.5})
           #(mq/outcome:retry {:reason ""})
           #(mq/outcome:retry {:reason (apply str (repeat 1025 "x"))})
           #(mq/outcome:dead {:reason 42})
           #(mq/outcome:dead {:unexpected true})]]
    (is (throws? :ex-info {:eid :carmine.mq/invalid-outcome-options} (f)))))

(defn- queue-index-snapshot [q]
  (let [keys (queue-keys q)
        [payloads meta successor-payloads successor-meta lease-tokens
         ready-high ready-normal ready-low scheduled leased
         dead dead-payloads failures]
        (car/wcar @mgr_ {:as-vec? true}
          (car/hkeys (:payloads keys))
          (car/hkeys (:meta keys))
          (car/hkeys (:successor-payloads keys))
          (car/hkeys (:successor-meta keys))
          (car/hkeys (:lease-tokens keys))
          (car/zrange (:ready-high keys) 0 -1)
          (car/zrange (:ready-normal keys) 0 -1)
          (car/zrange (:ready-low keys) 0 -1)
          (car/zrange (:scheduled keys) 0 -1)
          (car/zrange (:leased keys) 0 -1)
          (car/zrange (:dead keys) 0 -1)
          (car/hkeys (:dead-payloads keys))
          (car/hkeys (:failures keys)))]
    {:payloads (set payloads), :meta (set meta)
     :successor-payloads (set successor-payloads)
     :successor-meta (set successor-meta)
     :lease-tokens (set lease-tokens)
     :ready-high (set ready-high), :ready-normal (set ready-normal)
     :ready-low (set ready-low), :scheduled (set scheduled)
     :leased (set leased), :dead (set dead)
     :dead-payloads (set dead-payloads), :failures (set failures)}))

(defn- queue-invariant-errors [q]
  (let [{:keys [payloads meta successor-payloads successor-meta lease-tokens
                ready-high ready-normal ready-low scheduled leased dead
                dead-payloads failures] :as snapshot}
        (queue-index-snapshot q)
        active-indexes [ready-high ready-normal ready-low scheduled leased]
        indexed-active (apply set/union #{} active-indexes)
        indexed-count (reduce + (map count active-indexes))]
    (cond-> []
      (not= payloads meta)
      (conj {:invariant :active-payload-meta, :snapshot snapshot})

      (not= successor-payloads successor-meta)
      (conj {:invariant :successor-payload-meta, :snapshot snapshot})

      (not (set/subset? successor-payloads payloads))
      (conj {:invariant :successor-requires-active, :snapshot snapshot})

      (not= lease-tokens leased)
      (conj {:invariant :lease-token-index, :snapshot snapshot})

      (or (not= indexed-active payloads)
        (not= indexed-count (count indexed-active)))
      (conj {:invariant :exactly-one-active-index, :snapshot snapshot})

      (not= dead dead-payloads failures)
      (conj {:invariant :dead-indexes, :snapshot snapshot}))))

(defn- random-element [^java.util.Random rng xs]
  (nth (vec (sort xs)) (.nextInt rng (count xs))))

(deftest _generated-queue-state-model
  (let [seed (Long/parseLong
               (or (System/getenv "CARMINE_MQ_MODEL_SEED") "424242"))
        steps (Long/parseLong
                (or (System/getenv "CARMINE_MQ_MODEL_STEPS") "300"))
        rng (java.util.Random. seed)
        q (mq/queue mgr_ (qname)
            {:max-attempts 3, :lease-ms (* 60 60 1000)
             :retry-base-ms 0, :retry-max-ms 0})
        model_ (atom {:next-mid 0, :active {}, :ready [], :dead #{}
                      :payloads {}})
        context #(str "seed=" seed ", step=" %)]
    (try
      (dotimes [step steps]
        (let [{:keys [active ready dead]} @model_
              leased (into #{} (keep (fn [[mid v]] (when (= (:state v) :leased) mid))) active)
              dead-only (set/difference dead (set (keys active)))
              choices
              (cond-> [:enqueue :enqueue]
                (seq active)    (conj :duplicate :remove)
                (seq ready)     (conj :claim)
                (seq leased)    (into [:ack :retry :dead :release])
                (seq dead-only) (conj :redrive :remove))
              op (nth choices (.nextInt rng (count choices)))]
          (case op
            :enqueue
            (let [mid (str "m" (:next-mid @model_))
                  payload {:mid mid, :seed seed}
                  result (mq/msg-enqueue! q payload {:mid mid})]
              (is (= (:action result) :added) (context step))
              (swap! model_
                (fn [m]
                  (-> m
                    (update :next-mid inc)
                    (assoc-in [:payloads mid] payload)
                    (assoc-in [:active mid] {:state :ready, :payload payload})
                    (update :ready conj mid)))))

            :duplicate
            (let [mid (random-element rng (keys active))
                  payload (get-in active [mid :payload])]
              (is (= (:action (mq/msg-enqueue! q payload {:mid mid})) :existing)
                (context step)))

            :claim
            (let [expected-mid (first ready)
                  claim (#'mq/claim! q 64)]
              (is (= (:mid claim) expected-mid) (context step))
              (swap! model_
                (fn [m]
                  (-> m
                    (update :ready #(vec (rest %)))
                    (assoc-in [:active expected-mid :state] :leased)
                    (assoc-in [:active expected-mid :claim] claim)))))

            :ack
            (let [mid (random-element rng leased)
                  claim (get-in active [mid :claim])]
              (is (= (:action (#'mq/settle! q mid (:token claim) (mq/outcome:ack)))
                    :acked)
                (context step))
              (swap! model_
                #(-> % (update :active dissoc mid) (update :payloads dissoc mid))))

            :retry
            (let [mid (random-element rng leased)
                  claim (get-in active [mid :claim])
                  exhausted? (>= (:attempt claim) 3)
                  result (#'mq/settle! q mid (:token claim)
                           (mq/outcome:retry {:reason "generated"}))]
              (is (= (:action result) (if exhausted? :dead :retried))
                (context step))
              (swap! model_
                (fn [m]
                  (if exhausted?
                    (-> m (update :active dissoc mid) (update :dead conj mid))
                    (-> m
                      (assoc-in [:active mid :state] :ready)
                      (update-in [:active mid] dissoc :claim)
                      (update :ready conj mid))))))

            :dead
            (let [mid (random-element rng leased)
                  claim (get-in active [mid :claim])]
              (is (= (:action (#'mq/settle! q mid (:token claim)
                               (mq/outcome:dead {:reason "generated"})))
                    :dead)
                (context step))
              (swap! model_
                #(-> % (update :active dissoc mid) (update :dead conj mid))))

            :release
            (let [mid (random-element rng leased)
                  claim (get-in active [mid :claim])]
              (is (= (#'mq/release! q mid (:token claim)) :released) (context step))
              (is (= (:action (#'mq/settle! q mid (:token claim) (mq/outcome:ack)))
                    :stale)
                (context step))
              (swap! model_
                (fn [m]
                  (-> m
                    (assoc-in [:active mid :state] :ready)
                    (update-in [:active mid] dissoc :claim)
                    (update :ready conj mid)))))

            :redrive
            (let [mid (random-element rng dead-only)
                  payload (get-in @model_ [:payloads mid])]
              (is (= (mq/dead-redrive! q mid) {:success? true, :action :redriven})
                (context step))
              (swap! model_
                (fn [m]
                  (-> m
                    (update :dead disj mid)
                    (assoc-in [:active mid]
                      {:state :ready, :payload payload})
                    (update :ready conj mid)))))

            :remove
            (let [mids (set/union (set (keys active)) dead-only)
                  mid (random-element rng mids)]
              (is (:success? (mq/msg-remove! q mid)) (context step))
              (swap! model_
                (fn [m]
                  (-> m
                    (update :active dissoc mid)
                    (update :dead disj mid)
                    (update :payloads dissoc mid)
                    (update :ready #(vec (remove #{mid} %))))))))

          (let [{:keys [active ready dead]} @model_
                status (mq/queue-status q)
                expected-counts
                {:ready (count ready), :overdue 0, :scheduled 0
                 :leased (count (filter #(= (:state %) :leased) (vals active)))
                 :lease-expired 0, :dead (count dead), :successors 0
                 :active (count active)}]
            [(is (empty? (queue-invariant-errors q)) (context step))
             (is (= (:counts status) expected-counts) (context step))
             (doseq [[mid {:keys [state]}] active]
               (is (= (mq/msg-status q mid) state) (context step)))
             (doseq [mid (set/difference dead (set (keys active)))]
               (is (= (mq/msg-status q mid) :dead) (context step)))])))
      (finally (mq/queue-clear!! q)))))

(deftest _generated-coalescing-state-model
  (let [seed (Long/parseLong
               (or (System/getenv "CARMINE_MQ_COALESCE_MODEL_SEED") "1424245"))
        steps (Long/parseLong
                (or (System/getenv "CARMINE_MQ_COALESCE_MODEL_STEPS") "300"))
        rng (java.util.Random. seed)
        mid "coalesced"
        q (mq/queue mgr_ (qname)
            {:max-attempts 3, :lease-ms (* 60 60 1000)
             :retry-base-ms 0, :retry-max-ms 0
             :on-duplicate :coalesce, :revision-mode :required})
        keys (queue-keys q)
        model_
        (atom {:state nil, :attempt 0, :claim nil
               :next-revision 0, :active-revision nil
               :successor-revision nil, :successor-state nil
               :dead? false, :dead-revision nil})
        context #(str "seed=" seed ", step=" %)]
    (try
      (dotimes [step steps]
        (let [{:keys [state dead?]} @model_
              choices
              (case state
                nil (if dead?
                      [:redrive :fresh-ready :fresh-scheduled :remove]
                      [:add-ready :add-scheduled])
                :ready
                [:claim :update-ready :update-scheduled :stale :replay :remove]
                :scheduled
                [:make-due :update-ready :update-scheduled :stale :replay :remove]
                :leased
                [:update-running :update-running :stale :replay :ack :retry
                 :release :expire :extend-expired :remove])
              op (nth choices (.nextInt rng (count choices)))]
          (case op
            (:add-ready :add-scheduled :fresh-ready :fresh-scheduled)
            (let [revision (inc (:next-revision @model_))
                  scheduled? (contains? #{:add-scheduled :fresh-scheduled} op)
                  result (mq/msg-enqueue! q {:revision revision}
                           {:mid mid, :revision revision
                            :delay-ms (if scheduled? 60000 0)})]
              [(is (= (:action result) :added) (context step))
               (is (= (:prior-dead? result) (:dead? @model_)) (context step))]
              (swap! model_ assoc
                :state (if scheduled? :scheduled :ready)
                :attempt 0, :claim nil
                :next-revision revision, :active-revision revision
                :successor-revision nil, :successor-state nil))

            (:update-ready :update-scheduled)
            (let [{:keys [attempt state]} @model_
                  revision (inc (:next-revision @model_))
                  scheduled? (= op :update-scheduled)
                  result (mq/msg-enqueue! q {:revision revision}
                           {:mid mid, :revision revision
                            :delay-ms (if scheduled? 60000 0)})]
              ;; Once a generation has ever been attempted, retry/release may
              ;; make it ready again but coalescing must still preserve it and
              ;; update only its latest successor.
              (if (zero? attempt)
                (do
                  (is (= (:action result) :coalesced) (context step))
                  (swap! model_ assoc
                    :state (if scheduled? :scheduled :ready)
                    :attempt 0, :claim nil
                    :next-revision revision, :active-revision revision
                    :successor-revision nil, :successor-state nil))
                (do
                  (is (= (:action result) :coalesced-successor) (context step))
                  (swap! model_ assoc
                    :state state, :next-revision revision
                    :successor-revision revision
                    :successor-state (if scheduled? :scheduled :ready)))))

            :update-running
            (let [revision (inc (:next-revision @model_))
                  result (mq/msg-enqueue! q {:revision revision}
                           {:mid mid, :revision revision})]
              (is (= (:action result) :coalesced-successor) (context step))
              (swap! model_ assoc
                :next-revision revision, :successor-revision revision
                :successor-state (or (:successor-state @model_) :ready)))

            :stale
            (let [{:keys [active-revision successor-revision]} @model_
                  latest (or successor-revision active-revision)
                  revision (max 0 (dec latest))]
              (is (= (:action
                       (mq/msg-enqueue! q {:revision revision}
                         {:mid mid, :revision revision}))
                    :stale-revision)
                (context step)))

            :replay
            (let [{:keys [active-revision successor-revision]} @model_
                  revision (or successor-revision active-revision)]
              (is (= (:action
                       (mq/msg-enqueue! q {:revision revision}
                         {:mid mid, :revision revision}))
                    :existing)
                (context step)))

            (:claim :make-due)
            (do
              (when (= op :make-due)
                (car/wcar @mgr_ (car/zadd (:scheduled keys) 0 mid)))
              (let [claim (#'mq/claim! q 64)
                    expected-attempt (inc (:attempt @model_))]
                [(is (= (:mid claim) mid) (context step))
                 (is (= (:msg claim)
                       {:revision (:active-revision @model_)})
                   (context step))
                 (is (= (:attempt claim) expected-attempt) (context step))]
                (swap! model_ assoc
                  :state :leased, :attempt expected-attempt, :claim claim)))

            :ack
            (let [{:keys [claim successor-revision successor-state]} @model_
                  result (#'mq/settle! q mid (:token claim) (mq/outcome:ack))]
              (is (= (:action result) :acked) (context step))
              (if successor-revision
                (swap! model_ assoc
                  :state successor-state, :attempt 0, :claim nil
                  :active-revision successor-revision
                  :successor-revision nil, :successor-state nil)
                (swap! model_ assoc
                  :state nil, :attempt 0, :claim nil
                  :active-revision nil
                  :successor-revision nil, :successor-state nil)))

            :retry
            (let [{:keys [claim attempt active-revision
                          successor-revision successor-state]} @model_
                  exhausted? (>= attempt 3)
                  result (#'mq/settle! q mid (:token claim)
                           (mq/outcome:retry {:reason "generated-coalescing"}))]
              (is (= (:action result)
                    (if exhausted? :dead :retried))
                (context step))
              (if exhausted?
                (if successor-revision
                  (swap! model_ assoc
                    :state successor-state, :attempt 0, :claim nil
                    :active-revision successor-revision
                    :successor-revision nil, :successor-state nil
                    :dead? true, :dead-revision active-revision)
                  (swap! model_ assoc
                    :state nil, :attempt 0, :claim nil
                    :active-revision nil
                    :successor-revision nil, :successor-state nil
                    :dead? true, :dead-revision active-revision))
                (swap! model_ assoc :state :ready, :claim nil)))

            :release
            (let [{:keys [claim attempt successor-revision successor-state]} @model_
                  result (#'mq/release! q mid (:token claim))]
              (is (= result (if successor-revision
                              :released-successor :released))
                (context step))
              (if successor-revision
                (swap! model_ assoc
                  :state successor-state, :attempt 0, :claim nil
                  :active-revision successor-revision
                  :successor-revision nil, :successor-state nil)
                (swap! model_ assoc
                  :state :ready, :attempt (dec attempt), :claim nil)))

            :expire
            (let [{:keys [attempt active-revision
                          successor-revision successor-state]} @model_]
              (car/wcar @mgr_ (car/zadd (:leased keys) 0 mid))
              (let [claim (#'mq/claim! q 64)]
                (cond
                  (< attempt 3)
                  (do
                    [(is (= (:mid claim) mid) (context step))
                     (is (= (:attempt claim) (inc attempt)) (context step))
                     (is (= (:msg claim) {:revision active-revision})
                       (context step))]
                    (swap! model_ assoc
                      :state :leased, :attempt (inc attempt), :claim claim))

                  (and successor-revision (= successor-state :ready))
                  (do
                    [(is (= (:mid claim) mid) (context step))
                     (is (= (:attempt claim) 1) (context step))
                     (is (= (:msg claim) {:revision successor-revision})
                       (context step))]
                    (swap! model_ assoc
                      :state :leased, :attempt 1, :claim claim
                      :active-revision successor-revision
                      :successor-revision nil, :successor-state nil
                      :dead? true, :dead-revision active-revision))

                  successor-revision
                  (do
                    (is (= (:action claim) :idle) (context step))
                    (swap! model_ assoc
                      :state successor-state, :attempt 0, :claim nil
                      :active-revision successor-revision
                      :successor-revision nil, :successor-state nil
                      :dead? true, :dead-revision active-revision))

                  :else
                  (do
                    (is (= (:action claim) :idle) (context step))
                    (swap! model_ assoc
                      :state nil, :attempt 0, :claim nil
                      :active-revision nil
                      :successor-revision nil, :successor-state nil
                      :dead? true, :dead-revision active-revision)))))

            :extend-expired
            (let [token (get-in @model_ [:claim :token])]
              (car/wcar @mgr_ (car/zadd (:leased keys) 0 mid))
              (is (pos-int? (#'mq/extend-lease! q mid token)) (context step))
              (is (= (mq/msg-status q mid) :leased) (context step)))

            :redrive
            (do
              (is (= (mq/dead-redrive! q mid) {:success? true, :action :redriven})
                (context step))
              (swap! model_ assoc
                :state :ready, :attempt 0, :claim nil
                :active-revision (:dead-revision @model_)
                :successor-revision nil, :successor-state nil
                :dead? false, :dead-revision nil))

            :remove
            (do
              (is (:success? (mq/msg-remove! q mid)) (context step))
              (swap! model_ assoc
                :state nil, :attempt 0, :claim nil
                :active-revision nil
                :successor-revision nil, :successor-state nil
                :dead? false, :dead-revision nil)))

          (let [{:keys [state successor-revision dead?]} @model_
                counts (:counts (mq/queue-status q))
                expected-status (or state (when dead? :dead))]
            [(is (empty? (queue-invariant-errors q)) (context step))
             (is (= (mq/msg-status q mid) expected-status) (context step))
             (is (= counts
                   {:ready (if (= state :ready) 1 0)
                    :overdue 0, :scheduled (if (= state :scheduled) 1 0)
                    :leased (if (= state :leased) 1 0), :lease-expired 0
                    :dead (if dead? 1 0)
                    :successors (if successor-revision 1 0)
                    :active (if state 1 0)})
               (context step))])))
      (finally (mq/queue-clear!! q)))))

(deftest _retry-jitter
  (let [qopts {:max-attempts 3, :retry-base-ms 10000, :retry-max-ms 10000
               :retry-jitter :full}
        q (mq/queue mgr_ (qname) qopts)]
    (try
      (mq/msg-enqueue! q :automatic {:mid "automatic"})
      (let [{:keys [mid token attempt]} (#'mq/claim! q 1)
            sample (Long/parseLong
                     (subs (car/script-hash (str token ":" attempt)) 0 13) 16)
            expected (long (Math/floor
                             (* (/ (double sample) 4503599627370496.0) 10001.0)))
            settled (#'mq/settle! q mid token (mq/outcome:retry))]
        [(is (= (:action settled) :retried))
         (is (= (:retry-delay-ms settled) expected))
         (is (<= 0 expected 10000))])
      (mq/msg-remove! q "automatic")

      (testing "Explicit retry delays remain exact"
        (mq/msg-enqueue! q :explicit {:mid "explicit"})
        (let [{:keys [mid token]} (#'mq/claim! q 1)
              settled (#'mq/settle! q mid token
                        (mq/outcome:retry {:delay-ms 1234}))]
          [(is (= (:action settled) :retried))
           (is (= (:retry-delay-ms settled) 1234))])
        (mq/msg-remove! q "explicit"))

      (testing "The durable policy upgrades additively and must match"
        (car/wcar @mgr_ (car/hdel (get (queue-keys q) :config) "retry_jitter"))
        (is (mq/queue mgr_ (queue-name q) qopts))
        (is (= (car/wcar @mgr_
                 (car/hget (get (queue-keys q) :config) "retry_jitter"))
              "full"))
        (is (throws? :ex-info {:eid :carmine.mq/config-mismatch}
              (mq/queue mgr_ (queue-name q) (assoc qopts :retry-jitter :none)))))

      (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
            (mq/queue mgr_ (qname) {:retry-jitter :unexpected})))
      (finally (mq/queue-clear!! q)))))

(deftest _reaper-retry-jitter
  ;; The expired-lease (reaper) requeue in claim.lua must apply the same
  ;; deterministic full jitter as handler-requested retries in settle.lua,
  ;; seeded by the expired lease token.
  (let [qopts {:lease-ms 1, :max-attempts 3, :retry-base-ms 10000
               :retry-max-ms 10000, :retry-jitter :full}
        q (mq/queue mgr_ (qname) qopts)]
    (try
      (mq/msg-enqueue! q :reaped {:mid "reaped"})
      (let [{:keys [mid token attempt lease-expiry-ms]} (#'mq/claim! q 1)
            sample (Long/parseLong
                     (subs (car/script-hash (str token ":" attempt)) 0 13) 16)
            expected (long (Math/floor
                             (* (/ (double sample) 4503599627370496.0) 10001.0)))
            _ (wait-for-server-time! q (inc (long lease-expiry-ms)))
            {:keys [action next-at-ms server-time-ms] :as reap-reply}
            (#'mq/claim! q 8)]
        [(is (= mid "reaped"))
         (is (= attempt 1))
         (is (<= 0 expected 10000))
         (if (zero? expected)
           ;; ~1/10001 chance: a zero delay is already due within the same
           ;; maintenance pass and may be re-claimed immediately
           (is (contains? #{:idle :handle} action))
           [(is (= action :idle)
              "Reaped message is rescheduled with a delay, not immediately ready")
            (is (= (- (long next-at-ms) (long server-time-ms)) expected)
              "Reaper requeue delay matches settle.lua-style full jitter seeded by the expired lease token")])])
      (finally (mq/queue-clear!! q)))))

(deftest _claim-leaves-coalesced-deadline-wake-batons
  (testing "A sole successful claim publishes its new lease deadline"
    (let [q (mq/queue mgr_ (qname) {:lease-ms 60000})]
      (try
        (mq/msg-enqueue! q :only {:mid "only"})
        (is (= (signal-count q) 1))
        (let [claim (#'mq/claim! q 64)]
          [(is (= (select-keys claim [:action :mid])
                 {:action :handle, :mid "only"}))
           (is (= (signal-count q) 1)
             "The consumed ready signal is replaced by one lease-deadline baton")
           (is (= (:action (#'mq/settle! q "only" (:token claim)
                             (mq/outcome:ack)))
                 :acked))])
        (finally (mq/queue-clear!! q)))))

  (testing "Expired-lease retry maintenance publishes its new schedule"
    (let [q (mq/queue mgr_ (qname)
              {:lease-ms 60000, :max-attempts 2
               :retry-base-ms 5000, :retry-max-ms 5000})]
      (try
        (mq/msg-enqueue! q :retry {:mid "retry"})
        (let [claim (#'mq/claim! q 64)
              signal-key (get (queue-keys q) :signal)]
          (car/wcar @mgr_
            (car/del signal-key)
            (car/zadd (get (queue-keys q) :leased) 0 "retry"))
          (is (zero? (signal-count q)))
          (let [reaped (#'mq/claim! q 64)]
            [(is (= (:action reaped) :idle))
             (is (= (- (:next-at-ms reaped) (:server-time-ms reaped)) 5000))
             (is (= (signal-count q) 1)
               "A caller crash after reaping cannot strand the retry deadline")
             (is (= (:action (#'mq/settle! q "retry" (:token claim)
                               (mq/outcome:ack)))
                   :stale))]))
        (finally (mq/queue-clear!! q))))))

(deftest _reaper-preserves-valid-work-without-a-lease-token
  (testing "A retryable expired lease is requeued and the old handler is fenced"
    (let [q (mq/queue mgr_ (qname)
              {:lease-ms 60000, :max-attempts 2
               :retry-base-ms 0, :retry-max-ms 0})]
      (try
        (mq/msg-enqueue! q :retryable {:mid "missing-token-retry"})
        (let [old (#'mq/claim! q 64)]
          (car/wcar @mgr_
            (car/hdel (get (queue-keys q) :lease-tokens) (:mid old))
            (car/zadd (get (queue-keys q) :leased) 0 (:mid old)))
          (let [retried (#'mq/claim! q 64)]
            [(is (truss/submap? retried
                   {:action :handle, :mid "missing-token-retry"
                    :msg :retryable, :attempt 2}))
             (is (not= (:token retried) (:token old)))
             (is (= (:action (#'mq/settle! q (:mid old) (:token old)
                               (mq/outcome:ack)))
                   :stale))
             (is (= (:action (#'mq/settle! q (:mid retried) (:token retried)
                               (mq/outcome:ack)))
                   :acked))]))
        (finally (mq/queue-clear!! q)))))

  (testing "An exhausted expired lease still reaches its terminal policy"
    (let [q (mq/queue mgr_ (qname)
              {:lease-ms 60000, :max-attempts 1})]
      (try
        (mq/msg-enqueue! q :terminal {:mid "missing-token-terminal"})
        (let [old (#'mq/claim! q 64)]
          (car/wcar @mgr_
            (car/hdel (get (queue-keys q) :lease-tokens) (:mid old))
            (car/zadd (get (queue-keys q) :leased) 0 (:mid old)))
          (#'mq/claim! q 64)
          [(is (= (mq/msg-status q (:mid old)) :dead))
           (is (truss/submap? (mq/dead-info q (:mid old))
                 {:msg :terminal, :attempt 1, :reason "lease-expired"}))
           (is (= (:action (#'mq/settle! q (:mid old) (:token old)
                             (mq/outcome:ack)))
                 :stale))])
        (finally (mq/queue-clear!! q)))))

  (testing "Terminal reaping still promotes a valid successor"
    (let [q (mq/queue mgr_ (qname)
              {:lease-ms 60000, :max-attempts 1
               :on-duplicate :coalesce})]
      (try
        (mq/msg-enqueue! q :active {:mid "missing-token-successor"})
        (let [old (#'mq/claim! q 64)]
          (is (= (:action
                   (mq/msg-enqueue! q :successor
                     {:mid (:mid old)}))
                :coalesced-successor))
          (car/wcar @mgr_
            (car/hdel (get (queue-keys q) :lease-tokens) (:mid old))
            (car/zadd (get (queue-keys q) :leased) 0 (:mid old)))
          (let [successor (#'mq/claim! q 64)]
            [(is (truss/submap? successor
                   {:action :handle, :mid "missing-token-successor"
                    :msg :successor, :attempt 1}))
             (is (= (:msg (mq/dead-info q (:mid old))) :active))
             (is (= (:action (#'mq/settle! q (:mid old) (:token old)
                               (mq/outcome:ack)))
                   :stale))
             (is (= (:action (#'mq/settle! q (:mid successor)
                               (:token successor) (mq/outcome:ack)))
                   :acked))]))
        (finally (mq/queue-clear!! q))))))

(deftest _config-additive-upgrade-is-two-pass
  (let [qopts {:retry-jitter :full, :on-exhaustion :discard
               :on-duplicate :coalesce, :revision-mode :required}
        additive ["retry_jitter" "on_exhaustion_default"
                  "on_duplicate_default" "revision_mode"]]
    (doseq [mismatched additive]
      (let [q (mq/queue mgr_ (qname) qopts)
            config (get (queue-keys q) :config)]
        (try
          (car/wcar @mgr_
            (apply car/hdel config additive)
            (car/hset config mismatched "intentionally-mismatched"))
          (let [before (car/wcar @mgr_ (car/hgetall config))]
            [(is (throws? :ex-info
                   {:eid :carmine.mq/config-mismatch}
                   (mq/queue mgr_ (queue-name q) qopts))
               (str "Mismatch in " mismatched " is rejected"))
             (is (= (car/wcar @mgr_ (car/hgetall config)) before)
               (str "Mismatch in " mismatched
                 " leaves every missing additive field untouched"))])
          (finally (remove-v4-queue!! q)))))))

(deftest _queue-config-is-not-recreated-over-artifacts
  (testing "A genuinely unused name still creates a queue"
    (let [queue-name* (qname)
          keys (#'mq/qkeys queue-name*)]
      (try
        (car/wcar @mgr_ (apply car/unlink (vals keys)))
        (let [q (mq/queue mgr_ queue-name* {:lease-ms 111})]
          [(is (= (queue-name q) queue-name*))
           (is (= (car/wcar @mgr_ (car/exists (:config keys))) 1))])
        (finally
          (car/wcar @mgr_ (apply car/unlink (vals keys)))))))

  (testing "Every non-config physical role prevents read-write reinitialization"
    (let [zset-roles
          #{:ready-high :ready-normal :ready-low :scheduled :leased :dead}]
      (doseq [suffix
              (remove #{:config} (keys (#'mq/qkeys "artifact-role-list")))]
        (let [queue-name* (qname)
              q (mq/queue mgr_ queue-name* {:lease-ms 111})
              keys (queue-keys q)
              artifact-key (get keys suffix)]
          (try
            (car/wcar @mgr_
              (car/del (:config keys))
              (cond
                (= suffix :seq)     (car/set artifact-key "artifact")
                (= suffix :signal)  (car/lpush artifact-key "artifact")
                (zset-roles suffix) (car/zadd artifact-key 1 "artifact")
                :else               (car/hset artifact-key "artifact" "value")))
            (let [before (key-dump-digest artifact-key)
                  error
                  (truss/throws :ex-info
                    (mq/queue mgr_ queue-name* {:lease-ms 222}))]
              [(is (truss/submap? (ex-data error)
                     {:eid :carmine.mq/config-missing
                      :qname queue-name*, :artifact-key-count 1})
                 (name suffix))
               (is (= (key-dump-digest artifact-key) before)
                 (str (name suffix) " remains byte-for-byte unchanged"))
               (is (= (car/wcar @mgr_ (car/exists (:config keys))) 0)
                 (str (name suffix) " does not recreate config"))
               (is (= (car/wcar @mgr_ (apply car/exists (vals keys))) 1)
                 (str (name suffix) " does not create another queue key"))])
            (finally
              (car/wcar @mgr_ (apply car/unlink (vals keys))))))))))

(deftest _active-message-pagination
  (testing "Ready pages are priority/FIFO ordered, bounded, and payload-free"
    (let [q (mq/queue mgr_ (qname))
          other (mq/queue mgr_ (qname))
          secret "active-page-must-not-return-this-payload"]
      (try
        (mq/msg-enqueue! q secret {:mid "high-1", :priority :high})
        (mq/msg-enqueue! q :high-2 {:mid "high.2/λ", :priority :high})
        (mq/msg-enqueue! q :normal {:mid "normal", :priority :normal})
        (mq/msg-enqueue! q :low {:mid "low", :priority :low})
        (let [page-1 (mq/msg-active-page q {:limit 2})
              cursor-1 (:cursor page-1)
              legacy-score
              (long (car/wcar @mgr_
                      (car/zscore (get (queue-keys q) :ready-high) "high-1")))
              legacy-cursor
              (str "1." (#'mq/active-page-cursor-tag q) ".ready.0."
                legacy-score ".1."
                (#'mq/encode-active-page-field "high-1"))]
          [(is (= (mapv :mid (:items page-1)) ["high-1" "high.2/λ"]))
           (is (= (mapv :priority (:items page-1)) [:high :high]))
           (is (every? #(= (:status %) :ready) (:items page-1)))
           (is (every? pos-int? (map :enqueued-at-ms (:items page-1))))
           (is (string? cursor-1))
           (is (not (.contains (pr-str page-1) secret)))
           (is (every? #(truss/submap? % {:successor? :submap/nx})
                 (:items page-1)))
           (is (truss/submap? (mq/msg-info q "high-1")
                 {:status :ready, :priority :high
                  :enqueued-at-ms (:enqueued-at-ms (first (:items page-1)))}))
           (is (throws? :ex-info {:eid :carmine.mq/invalid-cursor}
                 (mq/msg-active-page other {:cursor cursor-1})))
           (is (throws? :ex-info {:eid :carmine.mq/invalid-cursor}
                 (mq/msg-active-page q
                   {:status :scheduled, :cursor cursor-1})))
           (is (= (mapv :mid
                    (:items (mq/msg-active-page q
                              {:limit 1, :cursor legacy-cursor})))
                 ["high.2/λ"])
             "Version-1 integer cursors remain resumable")]

          ;; Removing entries below the keyset cursor must not skip later work.
          (mq/msg-remove! q "high-1")
          (mq/msg-remove! q "high.2/λ")
          (let [page-2 (mq/msg-active-page q {:limit 1, :cursor cursor-1})
                page-3 (mq/msg-active-page q
                         {:limit 1, :cursor (:cursor page-2)})]
            [(is (= (mapv :mid (:items page-2)) ["normal"]))
             (is (string? (:cursor page-2)))
             (is (= (mapv :mid (:items page-3)) ["low"]))
             (is (nil? (:cursor page-3)))]))

        [(is (throws? :ex-info {:eid :carmine.mq/invalid-cursor}
               (mq/msg-active-page q {:cursor "invalid"})))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/msg-active-page q {:status :dead})))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/msg-active-page q {:limit 251})))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/msg-active-page q {:include-related? :yes})))]
        (finally
          (mq/queue-clear!! q)
          (mq/queue-clear!! other)))))

  (testing "Long multibyte message MIDs round-trip through cursors"
    (let [q (mq/queue mgr_ (qname))
          long-mid (apply str (repeat 400 "界"))]
      (try
        (mq/msg-enqueue! q :long-mid {:mid long-mid})
        (mq/msg-enqueue! q :after {:mid "after-long-mid"})
        (let [page-1 (mq/msg-active-page q {:limit 1})
              cursor (:cursor page-1)
              page-2 (mq/msg-active-page q {:limit 1, :cursor cursor})]
          [(is (= (mapv :mid (:items page-1)) [long-mid]))
           (is (< 1024 (count cursor)))
           (is (= (mapv :mid (:items page-2)) ["after-long-mid"]))
           (is (nil? (:cursor page-2)))])
        (finally (mq/queue-clear!! q)))))

  (testing "Scheduled and overdue are separate deadline views"
    (let [q (mq/queue mgr_ (qname))]
      (try
        (doseq [mid ["scheduled-a" "scheduled-b" "scheduled-c" "overdue"]]
          (mq/msg-enqueue! q mid {:mid mid, :delay-ms 60000}))
        (let [now (:server-time-ms (mq/queue-status q))
              future (+ now 60000)
              past (- now 1000)
              scheduled-key (get (queue-keys q) :scheduled)]
          (car/wcar @mgr_
            (car/zadd scheduled-key future "scheduled-a")
            (car/zadd scheduled-key future "scheduled-b")
            (car/zadd scheduled-key future "scheduled-c")
            (car/zadd scheduled-key past "overdue"))
          (let [page-1 (mq/msg-active-page q {:status :scheduled, :limit 1})
                page-2 (mq/msg-active-page q
                         {:status :scheduled, :limit 1, :cursor (:cursor page-1)})
                page-3 (mq/msg-active-page q
                         {:status :scheduled, :limit 1, :cursor (:cursor page-2)})
                overdue (mq/msg-active-page q {:status :overdue})]
            [(is (= (mapv :mid (:items page-1)) ["scheduled-a"]))
             (is (= (mapv :mid (:items page-2)) ["scheduled-b"]))
             (is (= (mapv :mid (:items page-3)) ["scheduled-c"]))
             (is (nil? (:cursor page-3)))
             (is (every? false? (map :overdue?
                                  (mapcat :items [page-1 page-2 page-3]))))
             (is (= (mapv #(select-keys % [:mid :status :overdue?]) (:items overdue))
                    [{:mid "overdue", :status :scheduled, :overdue? true}]))
             (is (= (:available-at-ms (first (:items overdue))) past))])

          (testing "A deleted cursor member in a same-score group stays bounded"
            (mq/queue-clear!! q)
            (doseq [mid ["delete-a" "delete-b" "delete-c"]]
              (mq/msg-enqueue! q mid {:mid mid, :delay-ms 60000}))
            (let [same-score (+ (:server-time-ms (mq/queue-status q)) 60000)]
              (car/wcar @mgr_
                (doseq [mid ["delete-a" "delete-b" "delete-c"]]
                  (car/zadd scheduled-key same-score mid))))
            (let [first-page (mq/msg-active-page q
                               {:status :scheduled, :limit 1})
                  _ (mq/msg-remove! q (:mid (first (:items first-page))))
                  traversal
                  (loop [cursor (:cursor first-page), pages []]
                    (cond
                      (nil? cursor) {:terminated? true, :pages pages}
                      (= (count pages) 4) {:terminated? false, :pages pages}
                      :else
                      (let [page (mq/msg-active-page q
                                   {:status :scheduled, :limit 1, :cursor cursor})]
                        (recur (:cursor page) (conj pages page)))))]
              [(is (= (mapv :mid (:items first-page)) ["delete-a"]))
               (is (:terminated? traversal))
               (is (every? #(<= (count (:items %)) 1) (:pages traversal)))])))
        (finally (mq/queue-clear!! q)))))

  (testing "Corrupt raw double scores paginate without exceptions or loops"
    (letfn [(traverse [q opts]
              (loop [page (mq/msg-active-page q (assoc opts :limit 1))
                     pages []
                     remaining 32]
                (when (zero? remaining)
                  (throw (ex-info "Active-page traversal did not terminate"
                           {:opts opts, :pages pages})))
                (let [pages (conj pages page)]
                  (if-let [cursor (:cursor page)]
                    (recur (mq/msg-active-page q
                             (assoc opts :limit 1, :cursor cursor))
                      pages (dec remaining))
                    {:pages pages, :items (vec (mapcat :items pages))}))))]
      (testing "Adjacent doubles keep exact cursor bounds with the anchor present or deleted"
        (doseq [delete-anchor? [false true]]
          (let [q (mq/queue mgr_ (qname))
                mids ["a-anchor" "b-next" "c-after"]
                scores ["1.0000000000000002" "1.0000000000000004"
                        "1.0000000000000007"]]
            (try
              (doseq [mid mids] (mq/msg-enqueue! q mid {:mid mid}))
              (let [ready (get (queue-keys q) :ready-normal)]
                (car/wcar @mgr_
                  (doseq [[mid score] (map vector mids scores)]
                    (car/zadd ready score mid))))
              (let [first-page (mq/msg-active-page q {:limit 1})
                    _ (when delete-anchor?
                        (mq/msg-remove! q "a-anchor"))
                    traversal
                    (loop [page first-page, pages [], remaining 8]
                      (let [pages (conj pages page)]
                        (cond
                          (nil? (:cursor page))
                          {:terminated? true, :pages pages}

                          (zero? remaining)
                          {:terminated? false, :pages pages}

                          :else
                          (recur (mq/msg-active-page q
                                   {:limit 1, :cursor (:cursor page)})
                            pages (dec remaining)))))
                    returned-mids (mapv :mid (mapcat :items (:pages traversal)))]
                [(is (:terminated? traversal) (str "deleted anchor: " delete-anchor?))
                 (is (= returned-mids mids) (str "deleted anchor: " delete-anchor?))
                 (is (= (count returned-mids) (count (distinct returned-mids))))
                 (is (every? #(<= (count (:items %)) 1) (:pages traversal)))])
              (finally (mq/queue-clear!! q))))))

      (testing "Ready sequence scores include fractional, negative, and tied infinities"
        (let [q (mq/queue mgr_ (qname))
              mids ["a-neg-inf" "b-neg-inf" "c-negative" "d-fractional"
                   "e-valid" "f-pos-inf" "g-pos-inf"]]
          (try
            (doseq [mid mids] (mq/msg-enqueue! q mid {:mid mid}))
            (let [ready (get (queue-keys q) :ready-normal)]
              (car/wcar @mgr_
                (car/zadd ready "-inf" "a-neg-inf")
                (car/zadd ready "-inf" "b-neg-inf")
                (car/zadd ready "-1.5" "c-negative")
                (car/zadd ready "1.25" "d-fractional")
                (car/zadd ready 2 "e-valid")
                (car/zadd ready "+inf" "f-pos-inf")
                (car/zadd ready "+inf" "g-pos-inf")))
            (let [{:keys [pages items]} (traverse q {})
                  by-mid (into {} (map (juxt :mid identity)) items)]
              [(is (= (mapv :mid items) mids))
               (is (= (:status (get by-mid "e-valid")) :ready))
               (is (every? #(= (:status (get by-mid %)) :corrupt)
                     (remove #{"e-valid"} mids)))
               (is (every? #(<= (count (:items %)) 1) pages))
               (is (every? string? (keep :cursor (butlast pages))))])
            (finally (mq/queue-clear!! q)))))

      (testing "Scheduled views traverse fractional, negative, and infinite deadlines"
        (let [q (mq/queue mgr_ (qname))]
          (try
            (let [scheduled (get (queue-keys q) :scheduled)
                  future (+ (:server-time-ms (mq/queue-status q)) 1000000)
                  future-fraction (str future ".5")
                  future-mids ["a-fractional" "b-valid" "c-pos-inf" "d-pos-inf"]]
              (doseq [mid future-mids]
                (mq/msg-enqueue! q mid {:mid mid, :delay-ms 1000000}))
              (car/wcar @mgr_
                (car/zadd scheduled future-fraction "a-fractional")
                (car/zadd scheduled (inc future) "b-valid")
                (car/zadd scheduled "+inf" "c-pos-inf")
                (car/zadd scheduled "+inf" "d-pos-inf"))
              (let [{:keys [items]} (traverse q {:status :scheduled})
                    by-mid (into {} (map (juxt :mid identity)) items)]
                [(is (= (mapv :mid items) future-mids))
                 (is (= (:status (get by-mid "b-valid")) :scheduled))
                 (is (every? #(= (:status (get by-mid %)) :corrupt)
                       ["a-fractional" "c-pos-inf" "d-pos-inf"]))]))

            (mq/queue-clear!! q)
            (let [scheduled (get (queue-keys q) :scheduled)
                  overdue-mids ["a-neg-inf" "b-neg-inf" "c-negative" "d-valid"]]
              (doseq [mid overdue-mids]
                (mq/msg-enqueue! q mid {:mid mid, :delay-ms 1000000}))
              (car/wcar @mgr_
                (car/zadd scheduled "-inf" "a-neg-inf")
                (car/zadd scheduled "-inf" "b-neg-inf")
                (car/zadd scheduled "-1.5" "c-negative")
                (car/zadd scheduled 0 "d-valid"))
              (let [{:keys [items]} (traverse q {:status :overdue})
                    by-mid (into {} (map (juxt :mid identity)) items)]
                [(is (= (mapv :mid items) overdue-mids))
                 (is (= (:status (get by-mid "d-valid")) :scheduled))
                 (is (every? #(= (:status (get by-mid %)) :corrupt)
                       ["a-neg-inf" "b-neg-inf" "c-negative"]))]))
            (finally (mq/queue-clear!! q)))))

      (testing "Leased pages traverse fractional deadlines and tied positive infinity"
        (let [q (mq/queue mgr_ (qname) {:lease-ms 60000})
              mids ["a-fractional" "b-valid" "c-pos-inf" "d-pos-inf"]]
          (try
            (doseq [mid mids]
              (mq/msg-enqueue! q mid {:mid mid})
              (#'mq/claim! q 64))
            (let [leased (get (queue-keys q) :leased)
                  future (+ (:server-time-ms (mq/queue-status q)) 1000000)]
              (car/wcar @mgr_
                (car/zadd leased (str future ".5") "a-fractional")
                (car/zadd leased (inc future) "b-valid")
                (car/zadd leased "+inf" "c-pos-inf")
                (car/zadd leased "+inf" "d-pos-inf")))
            (let [{:keys [items]} (traverse q {:status :leased})
                  by-mid (into {} (map (juxt :mid identity)) items)]
              [(is (= (mapv :mid items) mids))
               (is (= (:status (get by-mid "b-valid")) :leased))
               (is (every? #(= (:status (get by-mid %)) :corrupt)
                     ["a-fractional" "c-pos-inf" "d-pos-inf"]))])
            (finally (mq/queue-clear!! q)))))))

  (testing "Live and expired leases are distinct views"
    (let [q (mq/queue mgr_ (qname) {:lease-ms 60000})]
      (try
        (mq/msg-enqueue! q :live {:mid "live"})
        (#'mq/claim! q 1)
        (mq/msg-enqueue! q :expired {:mid "expired"})
        (#'mq/claim! q 1)
        (let [now (:server-time-ms (mq/queue-status q))]
          (car/wcar @mgr_
            (car/zadd (get (queue-keys q) :leased) (- now 1000) "expired"))
          (let [leased (mq/msg-active-page q {:status :leased})
                expired (mq/msg-active-page q {:status :lease-expired})]
            [(is (= (mapv :mid (:items leased)) ["live"]))
             (is (= (:status (first (:items leased))) :leased))
             (is (= (mapv :mid (:items expired)) ["expired"]))
             (is (= (:status (first (:items expired))) :lease-expired))]))
        (finally (mq/queue-clear!! q)))))

  (testing "Related roles are opt-in and damaged indexes remain visible"
    (let [q (mq/queue mgr_ (qname) {:on-duplicate :coalesce})]
      (try
        (mq/msg-enqueue! q :failed {:mid "related"})
        (let [{:keys [mid token]} (#'mq/claim! q 1)]
          (#'mq/settle! q mid token (mq/outcome:dead {:reason "old"})))
        (mq/msg-enqueue! q :fresh {:mid "related"})
        (let [plain (first (:items (mq/msg-active-page q)))
              enriched (first (:items
                                (mq/msg-active-page q
                                  {:include-related? true})))]
          [(is (truss/submap? plain {:prior-dead? :submap/nx}))
           (is (truss/submap? enriched
                 {:successor? false, :prior-dead? true}))])

        (mq/msg-enqueue! q :orphan {:mid "orphan"})
        (mq/msg-enqueue! q :corrupt {:mid "corrupt"})
        (car/wcar @mgr_
          (car/hdel (get (queue-keys q) :payloads) "orphan")
          (car/hset (get (queue-keys q) :meta) "corrupt" "invalid-msgpack"))
        (let [items (:items (mq/msg-active-page q {:limit 10}))
              by-mid (into {} (map (juxt :mid identity)) items)]
          [(is (truss/submap? (get by-mid "orphan")
                 {:status :orphan, :indexed-status :ready}))
           (is (truss/submap? (get by-mid "corrupt")
                 {:status :corrupt, :indexed-status :ready}))])
        (finally (mq/queue-clear!! q))))))

(deftest _dead-letter-pagination
  (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
        mids (mapv #(str "dead-" %) (range 5))
        page-all
        (fn [limit]
          (loop [cursor nil, pages []]
            (let [page (mq/dead-page q (cond-> {:limit limit}
                                         cursor (assoc :cursor cursor)))
                  pages (conj pages page)]
              (if-let [next-cursor (:cursor page)]
                (recur next-cursor pages)
                pages))))]
    (try
      (doseq [mid mids] (seed-dead! q mid))
      (let [pages (page-all 2)
            items (into [] (mapcat :items) pages)]
        [(is (= (count pages) 3))
         (is (= (mapv (comp count :items) pages) [2 2 1]))
         (is (= (set (map :mid items)) (set mids))
           "Pagination visits every dead letter exactly once")
         (is (every? pos-int? (map :failed-at-ms items)))
         (is (= (map :failed-at-ms items) (sort (map :failed-at-ms items)))
           "Pages follow failure-time order")
         (is (= (map :mid items) (mq/dead-mids q))
           "Paged traversal matches the one-shot listing")
         (is (every? #(pos-int? (:server-time-ms %)) pages))
         (is (nil? (:cursor (peek pages))))])

      (testing "Equal-score entries are traversed exactly once"
        (let [tied-at (:server-time-ms (mq/queue-status q))]
          (car/wcar @mgr_
            (apply car/zadd (get (queue-keys q) :dead)
              (interleave (repeat tied-at) mids)))
          (let [items (into [] (mapcat :items) (page-all 2))]
            [(is (= (mapv :mid items) mids)
               "Ties resume in member order across page boundaries")
             (is (every? #(= (:failed-at-ms %) tied-at) items))])))

      (testing "Bounded cursor and option validation"
        [(is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/dead-page q {:limt 1})))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/dead-page q {:limit 251})))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-cursor}
               (mq/dead-page q {:cursor "not-a-cursor"})))
         (let [other (mq/queue mgr_ (qname))]
           (try
             (is (throws? :ex-info {:eid :carmine.mq/invalid-cursor}
                   (mq/dead-page other
                     {:cursor (:cursor (mq/dead-page q {:limit 2}))}))
               "Cursors are bound to their queue")
             (finally (remove-v4-queue!! other))))])

      (testing "A damaged dead-index score reports :corrupt"
        (car/wcar @mgr_
          (car/zadd (get (queue-keys q) :dead) "XX" 1.5 "dead-0"))
        (let [items (:items (mq/dead-page q))]
          [(is (= (some #(when (= (:mid %) "dead-0") %) items)
                 {:mid "dead-0", :status :corrupt}))
           (is (= (count (filter :failed-at-ms items)) 4)
             "Other entries keep their timestamps")]))
      (finally (remove-v4-queue!! q)))))

(deftest _read-diagnostics-validate-active-index-roles
  (testing "Ready scores must be exact positive FIFO sequence values"
    (doseq [[score-name score]
            [[:zero "0"]
             [:negative "-1"]
             [:fractional "1.5"]
             [:above-exact-double-range "9007199254740992"]
             [:positive-infinity "+inf"]]]
      (let [q (mq/queue mgr_ (qname))
            mid (str "ready-score-" (name score-name))]
        (try
          (mq/msg-enqueue! q score-name {:mid mid})
          (car/wcar @mgr_
            (car/zadd (get (queue-keys q) :ready-normal) score mid))
          (let [item (first (:items (mq/msg-active-page q)))
                status (mq/queue-status q)]
            [(is (= (mq/msg-info q mid) {:status :corrupt}) (name score-name))
             (is (= (select-keys item [:mid :status :indexed-status])
                    {:mid mid, :status :corrupt, :indexed-status :ready})
               (name score-name))
             (is (nil? (get-in status
                         [:heads :ready-enqueued-at-ms :normal]))
               (name score-name))
             (is (nil? (get-in status [:lags-ms :ready-age :normal]))
               (name score-name))])
          (finally (mq/queue-clear!! q))))))

  (testing "Attempts are validated against the selected active role"
    (let [ready-q (mq/queue mgr_ (qname) {:max-attempts 3})]
      (try
        (mq/msg-enqueue! ready-q :ready-limit {:mid "ready-limit"})
        (mutate-packed! ready-q :meta "ready-limit" "v[3]=v[4];")
        [(is (= (mq/msg-info ready-q "ready-limit") {:status :corrupt}))
         (is (= (:status (first (:items (mq/msg-active-page ready-q))))
                :corrupt))
         (is (nil? (get-in (mq/queue-status ready-q)
                     [:heads :ready-enqueued-at-ms :normal])))]
        (finally (mq/queue-clear!! ready-q))))

    (let [scheduled-q (mq/queue mgr_ (qname) {:max-attempts 3})]
      (try
        (mq/msg-enqueue! scheduled-q :scheduled-limit
          {:mid "scheduled-limit", :delay-ms 60000})
        (mutate-packed! scheduled-q :meta "scheduled-limit" "v[3]=v[4];")
        [(is (= (mq/msg-info scheduled-q "scheduled-limit")
                {:status :corrupt}))
         (is (= (:status
                  (first (:items
                           (mq/msg-active-page scheduled-q
                             {:status :scheduled}))))
                :corrupt))]
        (finally (mq/queue-clear!! scheduled-q))))

    (let [leased-q (mq/queue mgr_ (qname)
                     {:lease-ms 60000, :max-attempts 1})]
      (try
        (mq/msg-enqueue! leased-q :leased-limit {:mid "leased-limit"})
        (is (= (:mid (#'mq/claim! leased-q 64)) "leased-limit"))
        [(is (= (:status (mq/msg-info leased-q "leased-limit")) :leased)
           "A lease at max attempts is a valid final delivery")
         (is (= (:status
                  (first (:items
                           (mq/msg-active-page leased-q {:status :leased}))))
                :leased))]
        (mutate-packed! leased-q :meta "leased-limit" "v[3]=0;")
        [(is (= (mq/msg-info leased-q "leased-limit") {:status :corrupt}))
         (is (= (:status
                  (first (:items
                           (mq/msg-active-page leased-q {:status :leased}))))
                :corrupt))]
        (finally (mq/queue-clear!! leased-q)))))

  (testing "A ready occurrence must match its priority band"
    (let [q (mq/queue mgr_ (qname))]
      (try
        (let [{:keys [enqueued-at-ms]}
              (mq/msg-enqueue! q :wrong-band {:mid "wrong-band"})]
          ;; Keep the valid normal occurrence as well. Both page occurrences
          ;; must expose the exact-one-index violation.
          (car/wcar @mgr_
            (car/zadd (get (queue-keys q) :ready-high) 2 "wrong-band"))
          (let [items (:items (mq/msg-active-page q))
                heads (get-in (mq/queue-status q)
                        [:heads :ready-enqueued-at-ms])]
            [(is (= (mq/msg-info q "wrong-band") {:status :corrupt}))
             (is (= (mapv #(select-keys % [:mid :status :indexed-status]) items)
                    [{:mid "wrong-band", :status :corrupt,
                      :indexed-status :ready}
                     {:mid "wrong-band", :status :corrupt,
                      :indexed-status :ready}]))
             (is (= heads
                    {:high nil, :normal enqueued-at-ms, :low nil}))]))
        (finally (mq/queue-clear!! q)))))

  (testing "Index-only active roles are corrupt rather than absent or dead"
    (doseq [[role score]
            [[:ready-high 1]
             [:ready-normal 1]
             [:ready-low 1]
             [:scheduled 20000000000000]
             [:leased 20000000000000]]]
      (let [q (mq/queue mgr_ (qname))
            mid (str "orphan-" (name role))]
        (try
          (car/wcar @mgr_ (car/zadd (get (queue-keys q) role) score mid))
          (is (= (mq/msg-info q mid) {:status :corrupt}) (name role))
          (finally (mq/queue-clear!! q)))))

    (let [q (mq/queue mgr_ (qname))]
      (try
        (seed-dead! q "dead-with-orphan-index")
        (car/wcar @mgr_
          (car/zadd (get (queue-keys q) :ready-high)
            1 "dead-with-orphan-index"))
        (is (= (mq/msg-info q "dead-with-orphan-index")
               {:status :corrupt})
          "A retained-dead role cannot hide a partial active role")
        (finally (mq/queue-clear!! q))))))

(deftest _read-diagnostics-enforce-active-index-token-bijection
  (testing "A ready and scheduled duplicate is corrupt in both diagnostics"
    (let [q (mq/queue mgr_ (qname))]
      (try
        (mq/msg-enqueue! q :duplicate-role {:mid "ready-and-scheduled"})
        (let [available-at (+ (:server-time-ms (mq/queue-status q)) 60000)]
          (car/wcar @mgr_
            (car/zadd (get (queue-keys q) :scheduled)
              available-at "ready-and-scheduled"))
          (let [ready-item (first (:items (mq/msg-active-page q)))
                scheduled-item
                (first (:items
                         (mq/msg-active-page q {:status :scheduled})))]
            [(is (= (mq/msg-info q "ready-and-scheduled")
                    {:status :corrupt}))
             (is (= (select-keys ready-item [:mid :status :indexed-status])
                    {:mid "ready-and-scheduled", :status :corrupt
                     :indexed-status :ready}))
             (is (= (select-keys scheduled-item [:mid :status :indexed-status])
                    {:mid "ready-and-scheduled", :status :corrupt
                     :indexed-status :scheduled}))]))
        (finally (remove-v4-queue!! q)))))

  (testing "A leased record without its fencing token is corrupt"
    (let [q (mq/queue mgr_ (qname) {:lease-ms 60000})]
      (try
        (mq/msg-enqueue! q :missing-token {:mid "missing-token"})
        (is (= (:mid (#'mq/claim! q 64)) "missing-token"))
        (car/wcar @mgr_
          (car/hdel (get (queue-keys q) :lease-tokens) "missing-token"))
        [(is (= (mq/msg-info q "missing-token") {:status :corrupt}))
         (is (= (select-keys
                  (first (:items
                           (mq/msg-active-page q {:status :leased})))
                  [:mid :status :indexed-status])
                {:mid "missing-token", :status :corrupt
                 :indexed-status :leased}))]
        (finally (remove-v4-queue!! q)))))

  (testing "A non-leased active record with a token is corrupt"
    (let [q (mq/queue mgr_ (qname))]
      (try
        (mq/msg-enqueue! q :extraneous-token {:mid "extraneous-token"})
        (car/wcar @mgr_
          (car/hset (get (queue-keys q) :lease-tokens)
            "extraneous-token" "orphan-token"))
        [(is (= (mq/msg-info q "extraneous-token") {:status :corrupt}))
         (is (= (select-keys (first (:items (mq/msg-active-page q)))
                  [:mid :status :indexed-status])
                {:mid "extraneous-token", :status :corrupt
                 :indexed-status :ready}))]
        (finally (remove-v4-queue!! q)))))

  (testing "Token-only and index-free active artifacts cannot look absent or healthy"
    (let [token-q (mq/queue mgr_ (qname))
          index-free-q (mq/queue mgr_ (qname))]
      (try
        (car/wcar @mgr_
          (car/hset (get (queue-keys token-q) :lease-tokens)
            "token-only" "orphan-token"))
        (mq/msg-enqueue! index-free-q :index-free {:mid "index-free"})
        (car/wcar @mgr_
          (car/zrem (get (queue-keys index-free-q) :ready-normal)
            "index-free"))
        [(is (= (mq/msg-info token-q "token-only") {:status :corrupt}))
         (is (empty? (:items (mq/msg-active-page token-q)))
           "Pages remain index-driven; msg-info exposes a token-only orphan")
         (is (= (mq/msg-info index-free-q "index-free")
                {:status :corrupt}))]
        (finally
          (remove-v4-queue!! token-q)
          (remove-v4-queue!! index-free-q))))))

(deftest _enqueue-preflights-active-index-roles
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :lease-ms 60000, :max-attempts 3})
        keys (queue-keys q)
        variants
        [{:name "index-free"
          :damage #(car/wcar @mgr_ (car/zrem (:ready-normal keys) %))}
         {:name "multiple-ready"
          :damage #(car/wcar @mgr_ (car/zadd (:ready-high keys) 2 %))}
         {:name "wrong-priority-band"
          :damage #(car/wcar @mgr_
                     (car/zrem (:ready-normal keys) %)
                     (car/zadd (:ready-high keys) 2 %))}
         {:name "invalid-ready-score"
          :damage #(car/wcar @mgr_ (car/zadd (:ready-normal keys) 0 %))}
         {:name "ready-with-token"
          :damage #(car/wcar @mgr_
                     (car/hset (:lease-tokens keys) % "stray-token"))}
         {:name "ready-at-attempt-limit"
          :damage #(mutate-packed! q :meta % "v[3]=v[4];")}
         {:name "invalid-scheduled-score"
          :enqueue-opts {:delay-ms 60000}
          :damage #(car/wcar @mgr_ (car/zadd (:scheduled keys) "+inf" %))}
         {:name "leased-without-token"
          :damage #(do
                     (#'mq/claim! q 64)
                     (car/wcar @mgr_ (car/hdel (:lease-tokens keys) %)))}
         {:name "leased-and-ready"
          :damage #(do
                     (#'mq/claim! q 64)
                     (car/wcar @mgr_ (car/zadd (:ready-normal keys) 3 %)))}
         {:name "leased-at-attempt-zero"
          :damage #(do
                     (#'mq/claim! q 64)
                     (mutate-packed! q :meta % "v[3]=0;"))}]]
    (try
      (doseq [{:keys [name enqueue-opts damage]} variants]
        (let [mid (str "enqueue-index-" name)]
          (is (= (:action (mq/msg-enqueue! q :original
                           (merge {:mid mid} enqueue-opts)))
                :added)
            name)
          (damage mid)
          (is (= (mq/msg-info q mid) {:status :corrupt}) name)
          (let [before (raw-mid-fingerprint q mid)
                before-seq (raw-seq q)
                before-signal (signal-count q)
                expected {:success? false, :error :corrupt-index, :mid mid}]
            [(is (= (mq/msg-enqueue! q :original
                      {:mid mid, :on-duplicate :reject})
                   expected)
               (str name " / reject"))
             (is (= (mq/msg-enqueue! q :replacement {:mid mid}) expected)
               (str name " / coalesce"))
             (is (= (raw-mid-fingerprint q mid) before)
               (str name " / durable role unchanged"))
             (is (= (raw-seq q) before-seq)
               (str name " / sequence unchanged"))
             (is (= (signal-count q) before-signal)
               (str name " / signal unchanged"))])
          (mq/msg-remove! q mid)))
      (finally (remove-v4-queue!! q)))))

(deftest _active-page-validates-a-maximum-size-batch
  (let [q (mq/queue mgr_ (qname))]
    (try
      (dotimes [n 250]
        (mq/msg-enqueue! q n {:mid (str "batched-" n)}))
      (let [{:keys [items cursor]} (mq/msg-active-page q {:limit 250})]
        [(is (= (count items) 250))
         (is (every? #(= (:status %) :ready) items))
         (is (nil? cursor))])
      (finally (remove-v4-queue!! q)))))

(deftest _corrupt-future-heads-produce-safe-claim-idle-deadlines
  (doseq [corrupt-role [:scheduled :leased]
          [score-name score-fn]
          [[:fractional #(str (+ % 1000000) ".5")]
           [:over-domain (constantly "100000000000000")]
           [:positive-infinity (constantly "+inf")]]]
    (let [q (mq/queue mgr_ (qname) {:lease-ms 60000})
          context (str (name corrupt-role) " / " (name score-name))]
      (try
        (let [now (:server-time-ms (mq/queue-status q))
              healthy-at (+ now 500000)
              corrupt-score (score-fn now)
              corrupt-mid (str "corrupt-" (name corrupt-role))
              other-role (if (= corrupt-role :scheduled) :leased :scheduled)
              other-mid (str "healthy-" (name other-role))]
          (if (= corrupt-role :scheduled)
            (mq/msg-enqueue! q :corrupt {:mid corrupt-mid, :delay-ms 500000})
            (do
              (mq/msg-enqueue! q :corrupt {:mid corrupt-mid})
              (is (= (:mid (#'mq/claim! q 64)) corrupt-mid) context)))
          (car/wcar @mgr_
            (car/zadd (get (queue-keys q) corrupt-role) corrupt-score corrupt-mid))

          (let [idle (#'mq/claim! q 64)
                expected (when (= score-name :fractional)
                           (inc (+ now 1000000)))]
            [(is (= (:action idle) :idle) context)
             (is (= (:next-at-ms idle) expected) context)
             (is (= (mq/msg-status q corrupt-mid) :corrupt) context)])

          (if (= other-role :scheduled)
            (mq/msg-enqueue! q :other {:mid other-mid, :delay-ms 500000})
            (do
              (mq/msg-enqueue! q :other {:mid other-mid})
              (is (= (:mid (#'mq/claim! q 64)) other-mid) context)))
          (car/wcar @mgr_
            (car/zadd (get (queue-keys q) other-role) healthy-at other-mid))

          (let [idle (#'mq/claim! q 64)]
            [(is (= (:action idle) :idle) context)
             (is (= (:next-at-ms idle) healthy-at) context)
             (is (= (mq/msg-status q corrupt-mid) :corrupt)
               "A valid opposite index still supplies the sleep deadline")])

          (mq/msg-enqueue! q :live {:mid "live"})
          (let [claim (#'mq/claim! q 64)]
            [(is (= (select-keys claim [:action :mid])
                   {:action :handle, :mid "live"}) context)
             (is (= (:action (#'mq/settle! q "live" (:token claim)
                               (mq/outcome:ack)))
                   :acked) context)
             (is (= (mq/msg-status q corrupt-mid) :corrupt)
               "Claiming healthy work leaves the bad index diagnosable")]))
        (finally (mq/queue-clear!! q))))))

(deftest _due-corrupt-heads-keep-bounded-maintenance-awake
  (doseq [role [:scheduled :leased]]
    (let [q (mq/queue mgr_ (qname)
              {:lease-ms 60000, :max-attempts 2
               :retry-base-ms 0, :retry-max-ms 0})
          keys (queue-keys q)
          role-key (get keys role)
          healthy-mid (str "healthy-" (name role))]
      (try
        (if (= role :scheduled)
          (mq/msg-enqueue! q :healthy
            {:mid healthy-mid, :delay-ms 60000})
          (do
            (mq/msg-enqueue! q :healthy {:mid healthy-mid})
            (is (= (:mid (#'mq/claim! q 64)) healthy-mid))))
        ;; Two index-only corrupt heads force two separate maintenance rounds;
        ;; the valid already-due record behind them proves eventual progress.
        (car/wcar @mgr_
          (car/del (:signal keys))
          (car/zadd role-key "-inf" (str "minus-inf-" (name role)))
          (car/zadd role-key -1 (str "negative-" (name role)))
          (car/zadd role-key 0 healthy-mid))
        (doseq [expected-orphan
                [(str "minus-inf-" (name role))
                 (str "negative-" (name role))]]
          (let [{:keys [action next-at-ms server-time-ms]} (#'mq/claim! q 1)]
            [(is (= action :idle) (str role " / " expected-orphan))
             (is (= next-at-ms server-time-ms)
               "Every due negative/-inf head requests an immediate maintenance round")
             (is (nil? (mq/msg-info q expected-orphan)))]))
        (let [claim (#'mq/claim! q 1)]
          [(is (= (select-keys claim [:action :mid :msg])
                 {:action :handle, :mid healthy-mid, :msg :healthy})
             (str role " eventually reaches healthy due work"))
           (is (= (:attempt claim) (if (= role :leased) 2 1)))
           (is (= (:action (#'mq/settle! q healthy-mid (:token claim)
                             (mq/outcome:ack)))
                 :acked))])
        (finally (mq/queue-clear!! q))))))

(deftest _fractional-timed-heads-wake-same-role-maintenance
  (testing "A fractional scheduled head wakes, promotes, and reveals the later deadline"
    (let [q (mq/queue mgr_ (qname))]
      (try
        (mq/msg-enqueue! q :fractional {:mid "fractional-scheduled",
                                        :delay-ms 60000})
        (mq/msg-enqueue! q :healthy {:mid "healthy-scheduled",
                                     :delay-ms 60000})
        (let [now (:server-time-ms (mq/queue-status q))
              fractional-score (str (+ now 250) ".5")
              wake-at (+ now 251)
              healthy-at (+ now 2000)
              scheduled (get (queue-keys q) :scheduled)]
          (car/wcar @mgr_
            (car/zadd scheduled fractional-score "fractional-scheduled")
            (car/zadd scheduled healthy-at "healthy-scheduled"))
          [(is (= (:next-at-ms (#'mq/claim! q 64)) wake-at))
           (is (= (mq/msg-status q "fractional-scheduled") :corrupt))]
          (wait-for-server-time! q wake-at)
          (let [claim (#'mq/claim! q 64)]
            [(is (= (select-keys claim [:action :mid])
                    {:action :handle, :mid "fractional-scheduled"}))
             (is (= (:action
                      (#'mq/settle! q (:mid claim) (:token claim)
                        (mq/outcome:ack)))
                    :acked))])
          (let [idle (#'mq/claim! q 64)]
            [(is (= (:action idle) :idle))
             (is (= (:next-at-ms idle) healthy-at))
             (is (= (mq/msg-status q "healthy-scheduled") :scheduled))]))
        (finally (mq/queue-clear!! q)))))

  (testing "A fractional leased head reaps and reveals the later expiry"
    (let [q (mq/queue mgr_ (qname)
              {:lease-ms 60000, :max-attempts 1,
               :on-exhaustion :discard})]
      (try
        (mq/msg-enqueue! q :fractional {:mid "fractional-leased"})
        (is (= (:mid (#'mq/claim! q 64)) "fractional-leased"))
        (mq/msg-enqueue! q :healthy {:mid "healthy-leased"})
        (is (= (:mid (#'mq/claim! q 64)) "healthy-leased"))
        (let [now (:server-time-ms (mq/queue-status q))
              fractional-score (str (+ now 250) ".5")
              wake-at (+ now 251)
              healthy-at (+ now 2000)
              leased (get (queue-keys q) :leased)]
          (car/wcar @mgr_
            (car/zadd leased fractional-score "fractional-leased")
            (car/zadd leased healthy-at "healthy-leased"))
          [(is (= (:next-at-ms (#'mq/claim! q 64)) wake-at))
           (is (= (mq/msg-status q "fractional-leased") :corrupt))]
          (wait-for-server-time! q wake-at)
          (let [idle (#'mq/claim! q 64)]
            [(is (= (:action idle) :idle))
             (is (= (:next-at-ms idle) healthy-at))
             (is (nil? (mq/msg-info q "fractional-leased")))
             (is (= (mq/msg-status q "healthy-leased") :leased))]))
        (finally (mq/queue-clear!! q))))))

(deftest _lease-extension-validates-the-current-index-score
  (let [q (mq/queue mgr_ (qname) {:lease-ms 60000})
        leased-key (get (queue-keys q) :leased)]
    (try
      (doseq [[score-name score-fn]
              [[:fractional #(str (+ % 1000000) ".5")]
               [:over-domain (constantly "100000000000000")]
               [:positive-infinity (constantly "+inf")]]]
        (let [mid (str "bad-extension-" (name score-name))]
          (mq/msg-enqueue! q score-name {:mid mid})
          (let [{:keys [token server-time-ms]} (#'mq/claim! q 64)
                corrupt-score (score-fn server-time-ms)
                canonical-score (if (= score-name :positive-infinity)
                                  "inf" corrupt-score)]
            (car/wcar @mgr_ (car/zadd leased-key corrupt-score mid))
            [(is (nil? (#'mq/extend-lease! q mid "stale-token"))
               "Token fencing takes precedence over damaged lease data")
             (is (throws? :ex-info
                   {:eid :carmine.mq/corrupt-lease, :mid mid
                    :corruption :lease-expiry}
                   (#'mq/extend-lease! q mid token))
               (name score-name))
             (is (= (mq/msg-status q mid) :corrupt) (name score-name))
             (is (= (car/wcar @mgr_
                      (car/lua
                        "return redis.call('zscore', KEYS[1], ARGV[1])"
                        [leased-key] [mid]))
                    canonical-score)
               "A refused extension preserves the corrupt score")])
          (mq/msg-remove! q mid)))

      (testing "Negative infinity retains its safe self-repair behavior"
        (mq/msg-enqueue! q :repair {:mid "repair-negative-infinity"})
        (let [{:keys [token server-time-ms]} (#'mq/claim! q 64)]
          (car/wcar @mgr_
            (car/zadd leased-key "-inf" "repair-negative-infinity"))
          (let [expiry (#'mq/extend-lease! q "repair-negative-infinity" token)]
            [(is (pos-int? expiry))
             (is (>= expiry (+ server-time-ms 60000)))
             (is (= (mq/msg-status q "repair-negative-infinity") :leased))])))
      (finally (mq/queue-clear!! q)))))

(deftest _packed-active-schema-corruption-is-contained
  (let [q (mq/queue mgr_ (qname) {:on-duplicate :coalesce})
        variants
        [["missing"    "v={1,1,0,3};"]
         ["fractional" "v[3]=0.5;"]
         ["priority"   "v[2]=9;"]
         ["attempt-max" "v[3]=4; v[4]=3;"]
         ["max-range"  "v[4]=1000001;"]
         ["timestamp"  "v[5]=100000000000000;"]
         ["policy"     "v[6]='unexpected';"]
         ["revision"   "v[7]=-1;"]
         ["lease-zero" "v[8]=0;"]
         ["lease-type" "v[8]='60000';"]
         ["lease-range" "v[8]=10000000000001;"]]]
    (try
      (doseq [[mid mutation] variants]
        (mq/msg-enqueue! q mid {:mid mid})
        (mutate-packed! q :meta mid mutation))

      (doseq [[mid _] variants]
        [(is (= (:status (mq/msg-info q mid)) :corrupt) mid)
         (is (= (:error (mq/msg-enqueue! q :replacement {:mid mid}))
               :corrupt-meta) mid)])

      (let [items (:items (mq/msg-active-page q {:limit 32}))]
        [(is (= (set (map :mid items)) (set (map first variants))))
         (is (every? #(= (:status %) :corrupt) items))])
      (is (nil? (get-in (mq/queue-status q) [:heads :ready-enqueued-at-ms :normal]))
        "A corrupt ready head cannot leak an unparsable timestamp")

      (dotimes [_ (count variants)]
        (is (= (#'mq/claim! q 64) {:action :skip, :reason :corrupt-meta})))
      (is (zero? (get-in (mq/queue-status q) [:counts :active])))

      (testing "Invalid index timestamps stay inspectable"
        (mq/msg-enqueue! q :scheduled {:mid "bad-scheduled", :delay-ms 60000})
        (car/wcar @mgr_
          (car/zadd (get (queue-keys q) :scheduled) 100000000000000
            "bad-scheduled"))
        (let [item (first (:items (mq/msg-active-page q {:status :scheduled})))]
          [(is (= (select-keys item [:mid :status :indexed-status])
                 {:mid "bad-scheduled", :status :corrupt
                  :indexed-status :scheduled}))
           (is (nil? (:available-at-ms item)))])
        (mq/msg-remove! q "bad-scheduled"))

      (testing "Legacy omissions and unknown additive tails remain valid"
        (doseq [[mid mutation]
                [["legacy-active" "v[6]=nil; v[7]=nil; v[8]=nil;"]
                 ["additive-active" "v[9]='future-field';"]]]
          (mq/msg-enqueue! q mid {:mid mid})
          (mutate-packed! q :meta mid mutation)
          (let [claim (#'mq/claim! q 64)]
            [(is (= (:mid claim) mid))
             (is (= (:action (#'mq/settle! q mid (:token claim)
                               (mq/outcome:ack)))
                   :acked))])))
      (finally (mq/queue-clear!! q)))))

(deftest _packed-successor-schema-preflights-mutations
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :lease-ms 60000})
        variants
        [["partial"     :delete]
         ["missing"     "v={1};"]
         ["priority"    "v[2]=9;"]
         ["fractional"  "v[3]=1.5;"]
         ["policy"      "v[5]='unexpected';"]
         ["revision"    "v[6]=-1;"]
         ["timestamp"   "v[7]=100000000000000;"]
         ["lease-zero"  "v[8]=0;"]
         ["lease-type"  "v[8]='60000';"]
         ["lease-range" "v[8]=10000000000001;"]]]
    (try
      (doseq [[mid mutation] variants]
        (mq/msg-enqueue! q :active {:mid mid})
        (let [claim (#'mq/claim! q 64)]
          (mq/msg-enqueue! q :successor {:mid mid})
          (if (= mutation :delete)
            (car/wcar @mgr_
              (car/hdel (get (queue-keys q) :successor-meta) mid))
            (mutate-packed! q :successor-meta mid mutation))
          (let [before (raw-mid-fingerprint q mid)]
            [(is (= (:status (mq/msg-info q mid)) :corrupt) mid)
             (is (= (:error (mq/msg-enqueue! q :newer {:mid mid}))
                   :corrupt-successor) mid)
             (is (= (#'mq/release! q mid (:token claim)) :corrupt) mid)
             (is (throws? :ex-info
                   {:eid :carmine.mq/settlement-failed
                    :error :corrupt-successor}
                   (#'mq/settle! q mid (:token claim) (mq/outcome:ack))) mid)
             (is (= (raw-mid-fingerprint q mid) before)
               (str mid " successor preflight is read-only"))])
          (mq/msg-remove! q mid)))

      (testing "Reject mode refuses an impossible attempt-zero successor"
        (let [mid "attempt-zero-successor"]
          (mq/msg-enqueue! q :active {:mid mid})
          (#'mq/claim! q 64)
          (mq/msg-enqueue! q :successor {:mid mid})
          (mutate-packed! q :meta mid "v[3]=0;")
          (let [keys (queue-keys q)]
            (car/wcar @mgr_
              (car/zrem (:leased keys) mid)
              (car/hdel (:lease-tokens keys) mid)
              (car/zadd (:ready-normal keys) 1000 mid)))
          (let [before (raw-mid-fingerprint q mid)
                expected {:success? false, :error :corrupt-successor
                          :mid mid}]
            [(is (= (mq/msg-info q mid) {:status :corrupt}))
             (is (= (mq/msg-enqueue! q :active
                      {:mid mid, :on-duplicate :reject})
                   expected))
             (is (= (mq/msg-enqueue! q :different
                      {:mid mid, :on-duplicate :reject})
                   expected))
             (is (= (raw-mid-fingerprint q mid) before)
               "Both reject results leave the impossible roles unchanged")])
          (mq/msg-remove! q mid)))

      (testing "Legacy and additive successor layouts promote safely"
        (doseq [[mid mutation]
                [["legacy-successor" "v[5]=nil; v[6]=nil; v[7]=nil; v[8]=nil;"]
                 ["additive-successor" "v[9]='future-field';"]]]
          (mq/msg-enqueue! q :active {:mid mid})
          (let [active (#'mq/claim! q 64)]
            (mq/msg-enqueue! q {:generation :successor} {:mid mid})
            (mutate-packed! q :successor-meta mid mutation)
            (is (:successor-promoted?
                  (#'mq/settle! q mid (:token active) (mq/outcome:ack))))
            (let [successor (#'mq/claim! q 64)]
              [(is (= (:msg successor) {:generation :successor}))
               (is (= (:action (#'mq/settle! q mid (:token successor)
                                 (mq/outcome:ack)))
                     :acked))]))))

      (testing "A reaper never promotes malformed successor work"
        (let [reaper-q (mq/queue mgr_ (qname)
                         {:on-duplicate :coalesce, :lease-ms 1
                          :max-attempts 1})]
          (try
            (mq/msg-enqueue! reaper-q :active {:mid "reaper-corrupt"})
            (let [active (#'mq/claim! reaper-q 64)]
              (mq/msg-enqueue! reaper-q :successor {:mid "reaper-corrupt"})
              (mutate-packed! reaper-q :successor-meta "reaper-corrupt" "v[2]=9;")
              (wait-for-server-time! reaper-q (inc (:lease-expiry-ms active)))
              (#'mq/claim! reaper-q 64)
              [(is (= (mq/msg-status reaper-q "reaper-corrupt") :dead))
               (is (= (:msg (mq/dead-info reaper-q "reaper-corrupt")) :active))
               (is (nil? (car/wcar @mgr_
                           (car/hget (get (queue-keys reaper-q) :successor-meta)
                             "reaper-corrupt"))))
               (is (nil? (car/wcar @mgr_
                           (car/hget (get (queue-keys reaper-q) :successor-payloads)
                             "reaper-corrupt"))))])
            (finally (mq/queue-clear!! reaper-q)))))
      (finally (mq/queue-clear!! q)))))

(deftest _per-enqueue-lease-overrides
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :lease-ms 1000
             :retry-base-ms 0, :retry-max-ms 0})
        claimed-lease-ms
        (fn [expected-mid]
          (let [claim (#'mq/claim! q 64)]
            (is (= (:mid claim) expected-mid))
            [claim (- (long (:lease-expiry-ms claim))
                     (long (:server-time-ms claim)))]))]
    (try
      (testing "A fresh enqueue's override governs claim and every extension"
        (mq/msg-enqueue! q :fresh {:mid "fresh", :lease-ms 60000})
        (let [[claim lease-ms] (claimed-lease-ms "fresh")]
          [(is (= lease-ms 60000))
           (is (>= (long (#'mq/extend-lease! q "fresh" (:token claim)))
                 (+ (long (:server-time-ms claim)) 60000))
             "Extension renews by the override, not the 1s queue default")
           (is (= (:action (#'mq/settle! q "fresh" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "Invalid values are rejected client-side"
        (doseq [lease-ms [0 -1 1.5 "60000" 10000000000001]]
          (is (throws? :ex-info
                {:eid :carmine.mq/invalid-option, :option :lease-ms}
                (mq/msg-enqueue! q :bad {:mid "bad", :lease-ms lease-ms}))
            (str lease-ms))))

      (testing "An explicit :lease-ms coalesces instead of reporting :existing"
        (mq/msg-enqueue! q :same {:mid "same"})
        [(is (= (:action (mq/msg-enqueue! q :same {:mid "same"})) :existing))
         (is (= (:action (mq/msg-enqueue! q :same {:mid "same", :lease-ms 60000}))
               :coalesced))
         (let [[claim lease-ms] (claimed-lease-ms "same")]
           [(is (= lease-ms 60000))
            (is (= (:action (#'mq/settle! q "same" (:token claim)
                              (mq/outcome:ack)))
                  :acked))])])

      (testing "An in-place coalesce fully re-describes the message"
        (mq/msg-enqueue! q :original {:mid "in-place", :lease-ms 60000})
        (mq/msg-enqueue! q :replacement {:mid "in-place"})
        (let [[claim lease-ms] (claimed-lease-ms "in-place")]
          [(is (= lease-ms 1000)
             "Omitting :lease-ms on coalesce clears the previous override")
           (is (= (:action (#'mq/settle! q "in-place" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "A first successor's own override wins at settle promotion"
        (mq/msg-enqueue! q :active {:mid "first-successor"})
        (let [active (#'mq/claim! q 64)]
          (is (= (:mid active) "first-successor"))
          (mq/msg-enqueue! q :successor
            {:mid "first-successor", :lease-ms 60000})
          (is (:successor-promoted?
                (#'mq/settle! q "first-successor" (:token active)
                  (mq/outcome:ack)))))
        (let [[claim lease-ms] (claimed-lease-ms "first-successor")]
          [(is (= lease-ms 60000))
           (is (= (:action (#'mq/settle! q "first-successor" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "A replacement successor without :lease-ms clears the override"
        (mq/msg-enqueue! q :active {:mid "replacement", :lease-ms 60000})
        (let [active (#'mq/claim! q 64)]
          (is (= (:mid active) "replacement"))
          (mq/msg-enqueue! q :successor-1 {:mid "replacement", :lease-ms 60000})
          (mq/msg-enqueue! q :successor-2 {:mid "replacement"})
          (is (:successor-promoted?
                (#'mq/settle! q "replacement" (:token active)
                  (mq/outcome:ack)))))
        (let [[claim lease-ms] (claimed-lease-ms "replacement")]
          [(is (= (:msg claim) :successor-2))
           (is (= lease-ms 1000)
             "The successor's absent override replaces the displaced one")
           (is (= (:action (#'mq/settle! q "replacement" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "Release promotion carries the successor's override"
        (mq/msg-enqueue! q :active {:mid "release-promote"})
        (let [active (#'mq/claim! q 64)]
          (is (= (:mid active) "release-promote"))
          (mq/msg-enqueue! q :successor
            {:mid "release-promote", :lease-ms 60000})
          (is (= (#'mq/release! q "release-promote" (:token active))
                :released-successor)))
        (let [[claim lease-ms] (claimed-lease-ms "release-promote")]
          [(is (= lease-ms 60000))
           (is (= (:action (#'mq/settle! q "release-promote" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "Corrupt-active cleanup still promotes the successor's override"
        (mq/msg-enqueue! q :active {:mid "corrupt-promote"})
        (let [active (#'mq/claim! q 64)]
          (is (= (:mid active) "corrupt-promote"))
          (mq/msg-enqueue! q :successor
            {:mid "corrupt-promote", :lease-ms 60000})
          (mutate-packed! q :meta "corrupt-promote" "v[2]=9;")
          (car/wcar @mgr_
            (car/zadd (get (queue-keys q) :leased) 0 "corrupt-promote")))
        ;; The same maintenance round contains the corrupt active, promotes
        ;; the successor to ready, and claims it.
        (let [[claim lease-ms] (claimed-lease-ms "corrupt-promote")]
          [(is (= (:msg claim) :successor))
           (is (= lease-ms 60000))
           (is (= (:action (#'mq/settle! q "corrupt-promote" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "Reaper exhaustion promotion carries the successor's override"
        (mq/msg-enqueue! q :active
          {:mid "reaper-promote", :max-attempts 1, :on-exhaustion :discard})
        (let [active (#'mq/claim! q 64)]
          (is (= (:mid active) "reaper-promote"))
          (mq/msg-enqueue! q :successor
            {:mid "reaper-promote", :lease-ms 60000})
          (car/wcar @mgr_
            (car/zadd (get (queue-keys q) :leased) 0 "reaper-promote")))
        ;; The same round exhausts the expired claim, promotes the successor,
        ;; and claims it.
        (let [[claim lease-ms] (claimed-lease-ms "reaper-promote")]
          [(is (= (:msg claim) :successor))
           (is (= lease-ms 60000))
           (is (= (:action (#'mq/settle! q "reaper-promote" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "A dead letter retains the override and redrive restores it"
        (mq/msg-enqueue! q :durable {:mid "dead-retain", :lease-ms 60000})
        (let [claim (#'mq/claim! q 64)]
          (is (= (:action (#'mq/settle! q "dead-retain" (:token claim)
                            (mq/outcome:dead {:reason "retention"})))
                :dead)))
        (is (= (mq/dead-redrive! q "dead-retain")
              {:success? true, :action :redriven}))
        (let [[claim lease-ms] (claimed-lease-ms "dead-retain")]
          [(is (= lease-ms 60000))
           (is (= (:action (#'mq/settle! q "dead-retain" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "Old records without the override field use the queue default"
        (mq/msg-enqueue! q :old {:mid "old-record"})
        (mutate-packed! q :meta "old-record" "v[8]=nil;")
        (let [[claim lease-ms] (claimed-lease-ms "old-record")
              packed-meta-shape
              (fn []
                (car/wcar @mgr_
                  (car/lua
                    "local v=cmsgpack.unpack(redis.call('hget', KEYS[1], ARGV[1]));
                     return {tostring(#v), tostring(v[8] == false)}"
                    [(get (queue-keys q) :meta)] ["old-record"])))]
          [(is (= lease-ms 1000))
           (is (pos-int? (#'mq/extend-lease! q "old-record" (:token claim))))
           (is (= (packed-meta-shape) ["8" "true"])
             "The claim rewrite densifies the legacy record")
           ;; Re-strip so the release rewrite also starts from a legacy record.
           (mutate-packed! q :meta "old-record" "v[8]=nil;")
           (is (= (#'mq/release! q "old-record" (:token claim)) :released))
           (is (= (packed-meta-shape) ["8" "true"])
             "The release rewrite densifies the legacy record")])
        (mq/msg-remove! q "old-record"))

      (testing "A corrupt-meta lease refuses extension"
        (mq/msg-enqueue! q :damaged {:mid "corrupt-extend"})
        (let [claim (#'mq/claim! q 64)]
          (is (= (:mid claim) "corrupt-extend"))
          (mutate-packed! q :meta "corrupt-extend" "v[2]=9;")
          [(is (nil? (#'mq/extend-lease! q "corrupt-extend" "stale-token"))
             "Token fencing takes precedence over damaged metadata")
           (is (throws? :ex-info
                 {:eid :carmine.mq/corrupt-lease, :mid "corrupt-extend"
                  :corruption :corrupt-meta}
                 (#'mq/extend-lease! q "corrupt-extend" (:token claim))))])
        (mq/msg-remove! q "corrupt-extend"))

      (testing "A role-corrupt attempt-0 lease refuses extension"
        (mq/msg-enqueue! q :damaged {:mid "attempt-zero-extend"})
        (let [claim (#'mq/claim! q 64)]
          (is (= (:mid claim) "attempt-zero-extend"))
          ;; Structurally valid metadata, but impossible for a leased role:
          ;; leased generations must have consumed an attempt.
          (mutate-packed! q :meta "attempt-zero-extend" "v[3]=0;")
          (is (throws? :ex-info
                {:eid :carmine.mq/corrupt-lease, :mid "attempt-zero-extend"
                 :corruption :corrupt-meta}
                (#'mq/extend-lease! q "attempt-zero-extend" (:token claim)))))
        (mq/msg-remove! q "attempt-zero-extend"))
      (finally (mq/queue-clear!! q)))))

(deftest _failure-reasons-with-resp-markers-are-classified-safely
  (let [q (mq/queue mgr_ (qname))]
    (try
      (testing "A raw leading-NUL reason is corrupt and every preflight is read-only"
        (let [mid "leading-nul-failure"]
          (seed-dead! q mid)
          (mutate-packed! q :failures mid
            "v[2]=string.char(0)..'_x';")
          (let [before (raw-mid-fingerprint q mid)]
            [(is (= (mq/msg-info q mid) {:status :corrupt}))
             (is (= (mq/dead-info q mid)
                    {:mid mid, :status :corrupt
                     :corruption :invalid-failure-meta}))
             (is (= (mq/msg-enqueue! q :replacement {:mid mid})
                    {:success? false, :error :corrupt-dead, :mid mid}))
             (is (= (mq/dead-redrive! q mid)
                    {:success? false, :action :corrupt}))
             (is (= (raw-mid-fingerprint q mid) before)
               "Inspection, enqueue, and redrive preserve every raw artifact")])
          (mq/msg-remove! q mid)))

      (testing "An embedded NUL remains a supported public reason"
        (let [mid "embedded-nul-failure"
              reason "accepted\u0000inside"]
          (mq/msg-enqueue! q :payload {:mid mid})
          (let [claim (#'mq/claim! q 64)]
            (is (= (:action
                     (#'mq/settle! q mid (:token claim)
                       (mq/outcome:dead {:reason reason})))
                  :dead)))
          [(is (= (:reason (mq/dead-info q mid)) reason))
           (is (= (:status (mq/msg-info q mid)) :dead))
           (is (= (mq/dead-redrive! q mid)
                  {:success? true, :action :redriven}))
           (is (= (mq/msg-status q mid) :ready))]
          (mq/msg-remove! q mid)))
      (finally (remove-v4-queue!! q)))))

(deftest _dead-redrive-corruption-is-byte-preserving
  (let [q (mq/queue mgr_ (qname))
        variants
        [["missing-index" #(car/zrem (get (queue-keys q) :dead) %)]
         ["missing-payload" #(car/hdel (get (queue-keys q) :dead-payloads) %)]
         ["missing-failure" #(car/hdel (get (queue-keys q) :failures) %)]
         ["missing-field" #(mutate-packed! q :failures % "v={1,'reason',v[3]};")]
         ["fractional" #(mutate-packed! q :failures % "v[4]=1.5;")]
         ["priority" #(mutate-packed! q :failures % "v[5]=9;")]
         ["attempt-max" #(mutate-packed! q :failures % "v[4]=3; v[6]=2;")]
         ["max-range" #(mutate-packed! q :failures % "v[6]=1000001;")]
         ["dead-score" #(mutate-packed! q :failures % "v[3]=v[3]+1;")]
         ["policy" #(mutate-packed! q :failures % "v[8]='unexpected';")]
         ["revision" #(mutate-packed! q :failures % "v[9]=-1;")]
         ["lease-zero" #(mutate-packed! q :failures % "v[10]=0;")]
         ["lease-type" #(mutate-packed! q :failures % "v[10]='60000';")]
         ["lease-range" #(mutate-packed! q :failures % "v[10]=10000000000001;")]]]
    (try
      (doseq [[mid mutation] variants]
        (seed-dead! q mid)
        (car/wcar @mgr_ (mutation mid))
        (let [before (raw-mid-fingerprint q mid)]
          [(is (= (mq/dead-redrive! q mid)
                 {:success? false, :action :corrupt}) mid)
           (is (= (raw-mid-fingerprint q mid) before)
             (str mid " remains byte-for-byte unchanged"))
           (is (zero? (reduce +
                        (map #(if (empty? %) 0 1) (drop 3 before))))
             (str mid " began without active/index state"))])
        (mq/msg-remove! q mid))

      (testing "Malformed dead metadata wins over a coexisting active role"
        (seed-dead! q "coexisting")
        (mq/msg-enqueue! q :active {:mid "coexisting"})
        (mutate-packed! q :failures "coexisting" "v[5]=9;")
        (let [before (raw-mid-fingerprint q "coexisting")]
          [(is (= (mq/dead-redrive! q "coexisting")
                 {:success? false, :action :corrupt}))
           (is (= (raw-mid-fingerprint q "coexisting") before))])
        (mq/msg-remove! q "coexisting"))

      (testing "Corrupt dead preflight preserves every impossible active artifact"
        (let [mid "corrupt-with-orphan-indexes"
              keys (queue-keys q)]
          (seed-dead! q mid)
          (mutate-packed! q :failures mid "v[5]=9;")
          (car/wcar @mgr_
            (car/hset (:successor-payloads keys) mid "orphan-successor-payload")
            (car/hset (:successor-meta keys) mid "orphan-successor-meta")
            (car/hset (:lease-tokens keys) mid "orphan-token")
            (car/zadd (:ready-high keys) 1 mid)
            (car/zadd (:ready-normal keys) 2 mid)
            (car/zadd (:ready-low keys) 3 mid)
            (car/zadd (:scheduled keys) 4 mid)
            (car/zadd (:leased keys) 5 mid))
          (let [before (raw-mid-fingerprint q mid)]
            [(is (= (mq/dead-redrive! q mid)
                   {:success? false, :action :corrupt}))
             (is (= (raw-mid-fingerprint q mid) before)
               "No cleanup runs before a corrupt dead record is refused")])
          (mq/msg-remove! q mid)))

      (testing "Valid redrive removes all orphan active indexes and tokens"
        (doseq [[mid artifacts]
                [["orphan-ready-high" #{:ready-high}]
                 ["orphan-ready-normal" #{:ready-normal}]
                 ["orphan-ready-low" #{:ready-low}]
                 ["orphan-scheduled" #{:scheduled}]
                 ["orphan-leased" #{:leased}]
                 ["orphan-token" #{:lease-token}]
                 ["orphan-combined"
                  #{:ready-high :ready-normal :ready-low :scheduled :leased
                    :lease-token}]]]
          (let [keys (queue-keys q)]
            (seed-dead! q mid)
            (car/wcar @mgr_
              (when (:ready-high artifacts) (car/zadd (:ready-high keys) 1 mid))
              (when (:ready-normal artifacts) (car/zadd (:ready-normal keys) 2 mid))
              (when (:ready-low artifacts) (car/zadd (:ready-low keys) 3 mid))
              (when (:scheduled artifacts) (car/zadd (:scheduled keys) 4 mid))
              (when (:leased artifacts) (car/zadd (:leased keys) 5 mid))
              (when (:lease-token artifacts)
                (car/hset (:lease-tokens keys) mid "orphan-token")))
            (is (= (mq/dead-redrive! q mid)
                  {:success? true, :action :redriven}) mid)
            (let [[failure dead-payload dead-score payload meta
                   successor-payload successor-meta token
                   ready-high ready-normal ready-low scheduled leased]
                  (raw-mid-fingerprint q mid)]
              [(is (every? empty?
                     [failure dead-payload dead-score successor-payload
                      successor-meta token ready-high ready-low scheduled leased]) mid)
               (is (every? seq [payload meta ready-normal]) mid)
               (is (= (count (filter seq
                              [ready-high ready-normal ready-low scheduled leased]))
                     1)
                 (str mid " has exactly one active index"))
               (is (= (mq/msg-status q mid) :ready) mid)])
            (mq/msg-remove! q mid))))

      (testing "Valid legacy and additive failures still redrive"
        (doseq [[mid mutation]
                [["legacy-failure" "v[8]=nil; v[9]=nil; v[10]=nil;"]
                 ["additive-failure" "v[11]='future-field';"]]]
          (seed-dead! q mid)
          (mutate-packed! q :failures mid mutation)
          [(is (= (mq/dead-redrive! q mid)
                 {:success? true, :action :redriven}))
           (is (= (mq/msg-status q mid) :ready))]
          (mq/msg-remove! q mid)))
      (finally (mq/queue-clear!! q)))))

(deftest _durability-barriers
  (testing "WAITAOF capability is rejected before its first write"
    (let [base-opts
          {:lease-ms 60000, :max-attempts 8
           :retry-base-ms 1000, :retry-max-ms 60000
           :retry-jitter :none, :on-exhaustion :dead
           :on-duplicate :reject, :revision-mode :none}
          q (#'mq/new-queue nil "redis-7.0"
              {:taoensso.carmine-v4.message-queue/redis-version "7.0.15"
               :taoensso.carmine-v4.message-queue/redis-version-num 458767}
              {})
          ensure-args_ (atom nil)
          ensure-error
          (with-redefs [mq/run-lua
                        (fn [_queue _script _keys args]
                          (reset! ensure-args_ args)
                          ["unsupported-version" "7.0.15" "458767"
                           (:required-redis-version args)
                           (str (:required-redis-version-num args))])]
            (truss/throws :ex-info
              (#'mq/ensure!
                (#'mq/new-queue nil "redis-7.0"
                  (assoc base-opts :durability {:mode :waitaof})
                  {:config "config"}))))]
      [(is (throws? :ex-info
             {:eid :carmine.mq/unsupported-redis-version, :feature :waitaof}
             (#'mq/assert-durability-version! q {:mode :waitaof})))
       (is (nil? (#'mq/assert-durability-version! q {:mode :wait})))
       (is (= (select-keys @ensure-args_
                [:required-redis-version :required-redis-version-num])
              {:required-redis-version "7.2.0"
               :required-redis-version-num 459264}))
       (is (truss/submap? (ex-data ensure-error)
             {:eid :carmine.mq/unsupported-redis-version
              :feature :waitaof, :required-version "7.2.0"}))]))

  (testing "Only fully-drained Redis errors leave connection framing reusable"
    (let [redis-error
          (com/drained-reply-errors
            [(com/reply-error "ERR unsupported"
               {:eid :carmine.read/redis-error-reply, :code "ERR"})]
            [0])
          framing-error
          (com/reply-error "Unexpected reply error"
            {:eid :carmine.read/unexpected-read-error})
          reusable?_ (volatile! true)]
      [(is (#'mq/durability-command-error? redis-error))
       (is (not (#'mq/durability-command-error? framing-error)))
       (binding [write/*conn-reusable?_ reusable?_]
         (#'mq/mark-durability-transport-error!))
       (is (false? @reusable?_))]))

  (testing "A swallowed durability interruption preserves cancellation"
    (let [interrupted?_
          (volatile! nil)]
      (try
        (#'mq/preserve-interruption!
          (InterruptedException. "Injected WAIT interruption"))
        (finally
          ;; Observe and clear the flag so this test cannot poison its runner.
          (vreset! interrupted?_ (Thread/interrupted))))
      (is (true? @interrupted?_))))

  (testing "A barrier read timeout cannot leak its late reply to the next borrower"
    (with-open [mgr
                (conns/conn-manager-pooled
                  {:conn-opts {:socket-opts {:read-timeout-ms 50}}
                   :pool-opts {:max-total 1, :max-idle 1}})]
      (let [q (mq/queue mgr (qname)
                {:durability {:replicas 1000000, :timeout-ms 300}})]
        (try
          (let [result (mq/msg-enqueue! q :framing {:mid "framing"})]
            [(is (= (:action result) :added))
             (is (truss/submap? (:durability result)
                   {:error-kind :transport, :ambiguous? true}))])
          ;; Let Redis emit the timed-out WAIT reply. A poisoned one-connection
          ;; pool would return this integer to PING instead of PONG.
          (Thread/sleep 350)
          (is (= (car/wcar mgr (car/ping)) "PONG"))
          (finally (mq/queue-clear!! q))))))

  (testing "WAIT reports a miss without undoing or hiding the write"
    (let [q (mq/queue mgr_ (qname)
              {:retry-base-ms 0, :retry-max-ms 0
               :durability {:replicas 1, :timeout-ms 5}})]
      (try
        (let [added (mq/msg-enqueue! q :durable {:mid "durable"})
              durability (:durability added)]
          [(is (= (:action added) :added))
           (is (= (:mode durability) :wait))
           (is (= (:requested durability) {:replicas 1, :timeout-ms 5}))
           (is (= (:observed durability) {:replicas 0}))
           (is (false? (:satisfied? durability)))
           (is (false? (:ambiguous? durability)))
           (is (= (mq/msg-status q "durable") :ready))])
        (let [existing (mq/msg-enqueue! q :durable {:mid "durable"})]
          (is (truss/submap? existing
                {:action :existing, :durability :submap/nx})
            "A no-write result must not confirm an older pooled-connection write"))
        (let [removed (mq/msg-remove! q "durable")]
          [(is (= (:action removed) :removed))
           (is (false? (get-in removed [:durability :satisfied?])))])
        (let [absent (mq/msg-remove! q "durable")]
          [(is (= absent {:success? false, :action :absent}))])
        (mq/msg-enqueue! q :redrive {:mid "redrive", :durability nil})
        (let [claim (#'mq/claim! q 1)]
          (#'mq/settle! q "redrive" (:token claim)
            (mq/outcome:dead {:reason "test"})))
        (let [redriven (mq/dead-redrive! q "redrive")]
          [(is (= (:action redriven) :redriven))
           (is (= (get-in redriven [:durability :mode]) :wait))])
        (mq/msg-remove! q "redrive")
        (mq/msg-enqueue! q :purge {:mid "purge", :durability nil})
        (let [claim (#'mq/claim! q 1)]
          (#'mq/settle! q "purge" (:token claim)
            (mq/outcome:dead {:reason "test"})))
        (let [purged (mq/dead-purge! q {:older-than-ms 0, :limit 1})]
          [(is (= (:removed purged) 1))
           (is (= (get-in purged [:durability :mode]) :wait))
           (is (pos-int? (:server-time-ms purged)))])
        (mq/msg-enqueue! q :clear {:mid "clear", :durability nil})
        (let [cleared (mq/queue-clear!! q)]
          [(is (truss/submap? cleared
                 {:success? true, :action :cleared}))
           (is (= (get-in cleared [:durability :mode]) :wait))])
        (finally (mq/queue-clear!! q)))))

  (testing "Per-enqueue nil disables a queue durability default"
    (let [q (mq/queue mgr_ (qname) {:durability {:replicas 1, :timeout-ms 5}})]
      (try
        (is (truss/submap? (mq/msg-enqueue! q :fast {:mid "fast", :durability nil})
              {:durability :submap/nx}))
        (finally (mq/queue-clear!! q)))))

  (testing "WAITAOF reports either observed persistence or a committed ambiguity"
    (let [q (mq/queue mgr_ (qname)
              {:durability {:aof-local 1, :aof-replicas 0, :timeout-ms 5}})]
      (try
        (let [result (mq/msg-enqueue! q :aof {:mid "aof"})
              durability (:durability result)]
          [(is (= (:action result) :added))
           (is (= (:mode durability) :waitaof))
           (is (or (contains? durability :observed)
                 (and (= (:error-kind durability) :command)
                   (false? (:ambiguous? durability))
                   (contains? durability :error))))
           (is (= (mq/msg-status q "aof") :ready))])
        (finally (mq/queue-clear!! q)))))

  (testing "Worker settle durability is independent from producer durability"
    (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
          settled (promise)
          worker (mq/worker-create q (constantly (mq/outcome:ack))
                   {:idle-max-ms 20
                    :durability {:replicas 1, :timeout-ms 5}
                    :on-event #(when (= (:event %) :settled) (deliver settled %))})]
      (try
        (mq/worker-start! worker)
        (mq/msg-enqueue! q :work {:mid "settle-durable"})
        (let [event (deref settled 2000 nil)]
          [(is (= (:action event) :acked))
           (is (= (get-in event [:result :durability :mode]) :wait))
           (is (false? (get-in event [:result :durability :satisfied?])))
           (is (= (get-in (mq/worker-stats worker)
                    [:counts :durability-misses :wait]) 1))])
        (finally
          (.close ^java.io.Closeable worker)
          (mq/queue-clear!! q)))))

  (testing "Invalid and Cluster durability policies are rejected before I/O"
    [(is (throws? :ex-info {:eid :carmine.mq/invalid-durability}
           (mq/queue mgr_ (qname)
             {:durability {:replicas 1, :aof-local 1}})))
     (let [spec (cluster/cluster-spec [["unused.invalid" 7000]])]
       (with-open [mgr (conns/conn-manager-clustered
                         {:conn-opts {:server {:cluster-spec spec}}
                          :pool-opts {:test-while-idle? false}})]
         (is (throws? :ex-info {:eid :carmine.mq/cluster-durability-unsupported}
               (mq/queue mgr (qname)
                 {:durability {:replicas 1, :timeout-ms 5}})))))]))

(deftest _wake-no-evict-command-errors-preserve-framing
  (let [redis-error
        (com/drained-reply-errors
          [(com/reply-error "NOPERM this user has no permissions"
             {:eid :carmine.read/redis-error-reply, :code "NOPERM"})]
          [0])
        transport-error (java.io.EOFException. "truncated reply")
        queue (#'mq/new-queue :fake-manager "wake-test" {} {:signal "signal"})
        conn (atom {:host "127.0.0.1", :port 6379, :in :in, :out :out})
        closed_ (atom [])
        stats_ (atom (#'mq/new-worker-stats 1))
        common-redefs
        {#'conns/mgr-conn-opts (constantly {})
         #'conns/mgr-cluster-server (constantly nil)
         #'conns/mgr-push-fn (fn [& _] nil)
         #'conns/new-conn (fn [& _] conn)
         #'conns/conn-addr (fn [c] [(:host @c) (:port @c)])
         #'conns/conn-close! (fn [candidate data]
                               (swap! closed_ conj [candidate data]))}]
    (with-redefs-fn
      (assoc common-redefs #'mq/fatal-throwable? (constantly false)
        #'taoensso.carmine-v4.resp/with-replies
        (fn [& _] (throw redis-error)))
      (fn []
        [(is (identical? (#'mq/new-wake-conn queue stats_) conn))
         (is (empty? @closed_)
           "A fully drained Redis command error keeps the wake connection")
         (is (= (get-in @stats_ [:counts :wake-errors]) 1)
           "Missing NO-EVICT protection remains observable")]))
    (with-redefs-fn
      (assoc common-redefs
        #'taoensso.carmine-v4.resp/with-replies
        (fn [& _] (throw transport-error)))
      (fn []
        [(is (identical? transport-error
               (truss/throws (#'mq/new-wake-conn queue stats_))))
         (is (= (mapv first @closed_) [conn])
           "Transport/framing failure still discards the connection")]))))

(deftest _queue-config-update
  (let [qn (qname)
        base-opts {:lease-ms 60000, :max-attempts 8
                   :retry-base-ms 1000, :retry-max-ms 60000}
        q (mq/queue mgr_ qn base-opts)]
    (try
      (testing "Read-only inspection returns parsed durable truth"
        (is (= (mq/queue-config q)
              {:schema-version 1, :lease-ms 60000, :max-attempts 8
               :retry-base-ms 1000, :retry-max-ms 60000
               :retry-jitter :none, :on-exhaustion :dead
               :on-duplicate :reject, :revision-mode :none})))

      (testing "Option validation fails before any I/O"
        [(is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/queue-config-update! q nil)))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/queue-config-update! q {})))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/queue-config-update! q {:example/reserved true}))
           "Namespaced extension keys alone do not satisfy the update")
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/queue-config-update! q {:lease-mz 1})))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/queue-config-update! q {:on-duplicate :coalesce}))
           "Duplicate policy is deliberately not updatable")
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/queue-config-update! q {:revision-mode :required}))
           "Revision mode is deliberately not updatable")
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option, :option :lease-ms}
               (mq/queue-config-update! q {:lease-ms 0})))
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option, :option :retry-jitter}
               (mq/queue-config-update! q {:retry-jitter :sometimes})))
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option, :option :on-exhaustion}
               (mq/queue-config-update! q {:on-exhaustion :requeue})))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/queue-config-update! q
                 {:retry-base-ms 10, :retry-max-ms 5})))])

      (testing "An unchanged update is explicit and write-free"
        (let [result (mq/queue-config-update! q {:lease-ms 60000})]
          [(is (truss/submap? result
                 {:success? true, :action :unchanged, :changed {}}))
           (is (pos-int? (:server-time-ms result)))
           (is (mq/queue? (:queue result)))]))

      (testing "The merged retry window is validated against durable values"
        (is (throws? :ex-info
              {:eid :carmine.mq/incompatible-options
               :retry-base-ms 1000, :retry-max-ms 500}
              (mq/queue-config-update! q {:retry-max-ms 500})))
        (is (= (:retry-max-ms (mq/queue-config q)) 60000)
          "A refused update is read-only"))

      (testing "Unrelated updates refuse an inverted durable retry window"
        (let [config-key (get (queue-keys q) :config)]
          (car/wcar @mgr_ (car/hset config-key "retry_base_ms" "70000"))
          (try
            [(is (throws? :ex-info
                   {:eid :carmine.mq/incompatible-options
                    :retry-base-ms 70000, :retry-max-ms 60000}
                   (mq/queue-config-update! q {:lease-ms 30000})))
             (is (= (car/wcar @mgr_ (car/hget config-key "lease_ms")) "60000")
               "A refused unrelated update writes nothing")]
            (finally
              (car/wcar @mgr_
                (car/hset config-key "retry_base_ms" "1000"))))))

      (testing "Effective updates return durable truth and a fresh handle"
        (let [result (mq/queue-config-update! q
                       {:lease-ms 30000, :max-attempts 3
                        :retry-jitter :full})]
          [(is (= (:action result) :updated))
           (is (= (:changed result)
                 {:lease-ms {:old 60000, :new 30000}
                  :max-attempts {:old 8, :new 3}
                  :retry-jitter {:old :none, :new :full}}))
           (is (pos-int? (:server-time-ms result)))
           (is (truss/submap? (mq/queue-config q)
                 {:lease-ms 30000, :max-attempts 3, :retry-jitter :full
                  :retry-base-ms 1000, :retry-max-ms 60000}))
           (let [fresh (:queue result)]
             [(is (= (select-keys (#'mq/queue-opts fresh)
                       [:lease-ms :max-attempts :retry-jitter])
                    {:lease-ms 30000, :max-attempts 3, :retry-jitter :full}))
              (is (= (:action (mq/msg-enqueue! fresh :defaults
                                {:mid "defaults"}))
                    :added))
              (is (= (:max-attempts (mq/msg-info fresh "defaults")) 3)
                "The fresh handle bakes the updated per-message default")
              (is (= (:max-attempts (mq/msg-info q "defaults")) 3))
              (mq/msg-remove! fresh "defaults")])
           (testing "Ensure's hard match now enforces the updated values"
             [(is (throws? :ex-info {:eid :carmine.mq/config-mismatch}
                    (mq/queue mgr_ qn base-opts)))
              (is (mq/queue?
                    (mq/queue mgr_ qn
                      (merge base-opts
                        {:lease-ms 30000, :max-attempts 3
                         :retry-jitter :full}))))])]))

      (testing "New claims from a stale handle use the updated lease at once"
        (mq/msg-enqueue! q :lease {:mid "lease-probe"})
        (let [claim (#'mq/claim! q 64)]
          [(is (= (- (long (:lease-expiry-ms claim))
                    (long (:server-time-ms claim)))
                 30000)
             "Lease duration is read from durable config per claim")
           (is (= (:action (#'mq/settle! q "lease-probe" (:token claim)
                             (mq/outcome:ack)))
                 :acked))]))

      (testing "The returned handle takes duplicate/revision durable truth"
        (let [config-key (get (queue-keys q) :config)]
          (car/wcar @mgr_
            (car/hset config-key "on_duplicate_default" "coalesce"
              "revision_mode" "required"))
          (let [fresh (:queue (mq/queue-config-update! q {:lease-ms 30000}))]
            (is (= (select-keys (#'mq/queue-opts fresh)
                     [:on-duplicate :revision-mode])
                  {:on-duplicate :coalesce, :revision-mode :required})
              "Live Lua reads revision_mode; the handle must agree with it"))
          (car/wcar @mgr_
            (car/hset config-key "on_duplicate_default" "reject"
              "revision_mode" "none"))))

      (testing "Corrupt durable config is refused read-only"
        (let [config-key (get (queue-keys q) :config)]
          (doseq [[field damaged]
                  [["lease_ms" "sixty"]
                   ["lease_ms" "1e3"]
                   ["lease_ms" "1000.0"]
                   ["lease_ms" "030000"]
                   ["retry_base_ms" "-1"]
                   ["on_duplicate_default" "sometimes"]
                   ["revision_mode" "maybe"]]]
            (let [restore (car/wcar @mgr_ (car/hget config-key field))]
              (car/wcar @mgr_ (car/hset config-key field damaged))
              [(is (throws? :ex-info {:eid :carmine.mq/corrupt-config}
                     (mq/queue-config-update! q {:lease-ms 1000}))
                 (str field "=" damaged))
               (is (= (car/wcar @mgr_ (car/hget config-key field)) damaged)
                 "The refused update wrote nothing")
               (when (= field "lease_ms")
                 (is (nil? (:lease-ms (mq/queue-config q)))
                   "Inspection reports damaged spellings as nil"))]
              (car/wcar @mgr_ (car/hset config-key field restore))))))
      (finally (remove-v4-queue!! q))))

  (testing "An effective write honors the queue durability barrier"
    (let [q (mq/queue mgr_ (qname)
              {:durability {:replicas 1, :timeout-ms 5}})]
      (try
        (let [updated (mq/queue-config-update! q {:lease-ms 45000})]
          [(is (= (:action updated) :updated))
           (is (= (get-in updated [:durability :mode]) :wait))
           (is (false? (get-in updated [:durability :satisfied?])))])
        (is (truss/submap?
              (mq/queue-config-update!
                (mq/queue mgr_ (queue-name q)
                  {:lease-ms 45000
                   :durability {:replicas 1, :timeout-ms 5}})
                {:lease-ms 45000})
              {:action :unchanged, :durability :submap/nx})
          "A no-write outcome omits the barrier")
        (finally (remove-v4-queue!! q)))))

  (testing "A missing config is a distinct error and nil inspection"
    (let [q (mq/queue mgr_ (qname))]
      (remove-v4-queue!! q)
      [(is (throws? :ex-info {:eid :carmine.mq/config-missing}
             (mq/queue-config-update! q {:lease-ms 1000})))
       (is (nil? (mq/queue-config q)))])))

(deftest _public-claim-settle-surface
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :lease-ms 60000, :max-attempts 2
             :retry-base-ms 0, :retry-max-ms 0})]
    (try
      (testing "Idle claims report the next deadline and maintenance tallies"
        (let [idle (mq/msg-claim! q)]
          [(is (= (select-keys idle [:success? :action :next-at-ms])
                 {:success? false, :action :idle, :next-at-ms nil}))
           (is (pos-int? (:server-time-ms idle)))
           (is (= (:maintenance idle)
                 {:orphan 0, :corrupt-meta 0, :corrupt-payload 0
                  :corrupt-index 0}))]))

      (testing "Claim, extension, and settlement round-trip"
        (mq/msg-enqueue! q {:job 1} {:mid "manual"})
        (let [claim (mq/msg-claim! q {:maintenance-batch-size 8})]
          [(is (truss/submap? claim
                 {:success? true, :action :claimed, :mid "manual"
                  :msg {:job 1}, :attempt 1, :priority :normal
                  :maintenance {:orphan 0, :corrupt-meta 0
                                :corrupt-payload 0, :corrupt-index 0}}))
           (is (not (contains? claim :more?)))
           (is (string? (:lease-token claim)))
           (is (pos-int? (:lease-expiry-ms claim)))
           (is (pos-int? (:enqueued-at-ms claim)))
           (is (pos-int? (:server-time-ms claim)))
           (let [extended (mq/msg-extend-lease! q "manual"
                            (:lease-token claim))]
             [(is (truss/submap? extended
                    {:success? true, :action :extended}))
              (is (>= (long (:lease-expiry-ms extended))
                    (long (:lease-expiry-ms claim))))])
           (is (= (mq/msg-extend-lease! q "manual" "not-the-token")
                 {:success? false, :action :stale}))
           (is (= (mq/msg-settle! q "manual" "not-the-token"
                    (mq/outcome:ack))
                 {:success? false, :action :stale}))
           (is (= (mq/msg-settle! q "manual" (:lease-token claim)
                    (mq/outcome:ack))
                 {:success? true, :action :acked
                  :successor-promoted? false}))
           (is (nil? (mq/msg-status q "manual")))]))

      (testing "Retried settles report the retry due time, not a settle time"
        (mq/msg-enqueue! q :retry {:mid "retry"})
        (let [claim (mq/msg-claim! q)
              settled (mq/msg-settle! q "retry" (:lease-token claim)
                        (mq/outcome:retry {:delay-ms 60000}))]
          [(is (truss/submap? settled
                 {:success? true, :action :retried, :retry-delay-ms 60000
                  :server-time-ms :submap/nx}))
           (is (= (:retry-at-ms settled)
                 (:available-at-ms (mq/msg-info q "retry"))))])
        (mq/msg-remove! q "retry"))

      (testing "A retry outcome at the attempt limit settles terminally"
        (mq/msg-enqueue! q :exhaust {:mid "exhaust", :max-attempts 1})
        (let [claim (mq/msg-claim! q)
              settled (mq/msg-settle! q "exhaust" (:lease-token claim)
                        (mq/outcome:retry))]
          [(is (truss/submap? settled {:success? true, :action :dead}))
           (is (pos-int? (:server-time-ms settled)))
           (is (= (mq/msg-status q "exhaust") :dead))])
        (mq/msg-remove! q "exhaust"))

      (testing "Release refunds the attempt"
        (mq/msg-enqueue! q :release {:mid "release"})
        (let [claim (mq/msg-claim! q)]
          [(is (= (mq/msg-release! q "release" "not-the-token")
                 {:success? false, :action :stale}))
           (is (= (mq/msg-release! q "release" (:lease-token claim))
                 {:success? true, :action :released}))])
        (let [reclaim (mq/msg-claim! q)]
          [(is (= (:attempt reclaim) 1)
             "The released claim did not consume an attempt")
           (is (truss/submap?
                 (mq/msg-settle! q "release" (:lease-token reclaim)
                   (mq/outcome:ack))
                 {:action :acked}))]))

      (testing "Release promotes a newer coalesced generation"
        (mq/msg-enqueue! q :original {:mid "successor"})
        (let [claim (mq/msg-claim! q)]
          (mq/msg-enqueue! q :newer {:mid "successor"})
          (is (= (mq/msg-release! q "successor" (:lease-token claim))
                {:success? true, :action :released-successor})))
        (let [reclaim (mq/msg-claim! q)]
          [(is (= (:msg reclaim) :newer))
           (is (truss/submap?
                 (mq/msg-settle! q "successor" (:lease-token reclaim)
                   (mq/outcome:ack))
                 {:action :acked}))]))

      (testing "Settlement reports successor promotion"
        (mq/msg-enqueue! q :first {:mid "promote"})
        (let [claim (mq/msg-claim! q)]
          (mq/msg-enqueue! q :second {:mid "promote"})
          (is (truss/submap?
                (mq/msg-settle! q "promote" (:lease-token claim)
                  (mq/outcome:ack))
                {:success? true, :action :acked, :successor-promoted? true})))
        (let [reclaim (mq/msg-claim! q)]
          (is (truss/submap?
                (mq/msg-settle! q "promote" (:lease-token reclaim)
                  (mq/outcome:ack))
                {:action :acked}))))

      (testing "Damaged head-of-line records surface as :skipped"
        (mq/msg-enqueue! q :damaged {:mid "damaged"})
        (mutate-packed! q :meta "damaged" "v[2]=9;")
        (let [skipped (mq/msg-claim! q)]
          [(is (= (select-keys skipped [:success? :action :reason])
                 {:success? false, :action :skipped, :reason :corrupt-meta}))
           (is (= (:maintenance skipped)
                 {:orphan 0, :corrupt-meta 0, :corrupt-payload 0
                  :corrupt-index 0})
             "Head-of-line containment is a skip, not a maintenance tally")]))

      (testing "Maintenance tallies count due-schedule cleanups"
        (mq/msg-enqueue! q :scheduled {:mid "scheduled", :delay-ms 60000})
        (mutate-packed! q :meta "scheduled" "v[2]=9;")
        (car/wcar @mgr_
          (car/zadd (get (queue-keys q) :scheduled) 0 "scheduled"))
        (let [result (mq/msg-claim! q)]
          [(is (= (:action result) :idle))
           (is (= (:maintenance result)
                 {:orphan 0, :corrupt-meta 1, :corrupt-payload 0
                  :corrupt-index 0}))]))

      (testing "Client-side validation fails before any I/O"
        [(is (throws? :ex-info
               {:eid :carmine.mq/invalid-option, :option :lease-token}
               (mq/msg-settle! q "mid" nil (mq/outcome:ack))))
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option, :option :lease-token}
               (mq/msg-settle! q "mid" "" (mq/outcome:ack))))
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option, :option :lease-token}
               (mq/msg-release! q "mid" 42)))
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option, :option :lease-token}
               (mq/msg-extend-lease! q "mid" nil)))
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option, :option :outcome}
               (mq/msg-settle! q "mid" "token" :ack)))
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option, :option :outcome}
               (mq/msg-settle! q "mid" "token" {:action :ack})))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-mid}
               (mq/msg-settle! q "" "token" (mq/outcome:ack))))
         (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
               (mq/msg-claim! q {:max-msgs 2}))
           "Batch options remain strictly rejected, reserving the name")
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option
                :option :maintenance-batch-size}
               (mq/msg-claim! q {:maintenance-batch-size 0})))
         (is (throws? :ex-info
               {:eid :carmine.mq/invalid-option
                :option :maintenance-batch-size}
               (mq/msg-claim! q {:maintenance-batch-size 1001})))])
      (finally (mq/queue-clear!! q)))))

(def ^:private worker-event-envelope-keys
  #{:event :queue :worker-id :client-time-ms})

(deftest _worker-event-and-settle-result-key-contract
  ;; Worker event maps are additive API. Pin the documented keys without
  ;; rejecting useful additions.
  (testing "Required worker event keys per event kind"
    (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
          events_ (atom [])
          log-ids_ (atom [])
          delivery-keys #{:runner :mid :attempt :priority :enqueued-at-ms
                          :age-ms :server-time-ms :handler-ms}
          worker
          (mq/worker-create q
            (fn [{:keys [mid attempt]}]
              (case mid
                "ack"  (mq/outcome:ack)
                "boom" (if (= attempt 1)
                         (throw (ex-info "boom" {}))
                         (mq/outcome:ack))
                "bad"  (if (= attempt 1) :not-an-outcome (mq/outcome:ack))))
            {:idle-max-ms 20, :on-event #(swap! events_ conj %)})]
      (try
        (with-redefs [trove/*log-fn*
                      (fn [_ns _coords _level id _data_]
                        (swap! log-ids_ conj id))]
          (is (mq/worker-start! worker))
          (doseq [mid ["ack" "boom" "bad"]]
            (is (= (:action (mq/msg-enqueue! q {:msg mid} {:mid mid})) :added)))
          (is (wait-for 5000
                #(every? nil? (map (partial mq/msg-status q)
                                ["ack" "boom" "bad"]))))
          (is (mq/worker-stop! worker))
          (is (mq/worker-await-stopped! worker 2000))
          (let [events @events_
                event-of (fn [kind pred]
                           (some #(when (and (= (:event %) kind) (pred %)) %)
                             events))
                expected
                {:worker-started  #{}
                 :worker-stopping #{}
                 :worker-stopped  #{}
                 :handler-error   (conj delivery-keys :error)
                 :invalid-handler-return (conj delivery-keys :return-type)
                 ;; Plain acked settle; `:include-msg?` workers add `:msg`
                 :settled (into delivery-keys [:intent :action :result])}]
            (doseq [[kind data-keys] expected]
              (let [event (case kind
                            :settled (event-of :settled #(= (:mid %) "ack"))
                            (event-of kind (fn [_] true)))]
                [(is (some? event) (str kind))
                 (is (set/subset?
                       (into worker-event-envelope-keys data-keys)
                       (set (keys event)))
                   (str kind))]))
            ;; Retried settles additionally carry the outcome reason and the
            ;; effective retry delay
            (let [retried (event-of :settled
                            #(and (= (:mid %) "boom") (= (:action %) :retried)))]
              [(is (some? retried))
               (is (set/subset?
                     (into worker-event-envelope-keys
                       (into delivery-keys
                         [:intent :action :result :reason :retry-delay-ms]))
                     (set (keys retried))))])
            (is (empty?
                  (set/intersection
                    #{:carmine.mq/handler-error
                      :carmine.mq/invalid-handler-return}
                    (set @log-ids_)))
              "User handler outcomes are events, not automatic logs")))
        (finally
          (.close ^java.io.Closeable worker)
          (mq/queue-clear!! q)))))

  (testing "Required worker error/failure event keys"
    (let [events_ (atom [])
          logs_ (atom [])
          claims_ (atom 0)
          worker (mq/worker-create (inert-queue) (constantly (mq/outcome:ack))
                   {:idle-min-ms 1, :idle-max-ms 1
                    :on-event #(swap! events_ conj %)})]
      (try
        (with-redefs [mq/claim!
                      (fn [& _]
                        (case (long (swap! claims_ inc))
                          1 (throw (Exception. "ordinary loop error"))
                          2 (throw (LinkageError. "fatal loop error"))
                          {:action :skip, :reason :unexpected-extra-claim}))
                      trove/*log-fn*
                      (fn [_ns _coords level id data_]
                        (swap! logs_ conj
                          {:level level, :id id, :data (force data_)}))]
          (is (mq/worker-start! worker))
          ;; Nb await only after the fatal claim: `worker-await-stopped!`
          ;; itself initiates an ordinary stop
          (is (wait-for 2000
                (fn [] (boolean (some #(= (:event %) :worker-failed)
                                  @events_)))))
          (is (mq/worker-await-stopped! worker 2000))
          (is (= (:state @worker) :failed))
          (let [event-of (fn [kind]
                           (some #(when (= (:event %) kind) %) @events_))
                worker-error (event-of :worker-error)]
            (doseq [kind [:worker-error :worker-failed]]
              (let [event (event-of kind)]
                [(is (some? event) (str kind))
                 (is (set/subset?
                       (into worker-event-envelope-keys
                         #{:runner :phase :error})
                       (set (keys event)))
                   (str kind))]))
            (let [{:keys [level data] :as log}
                  (some #(when (= (:id %) :carmine.mq/worker-error) %) @logs_)]
              [(is (some? log))
               (is (= level :error))
               (is (= (select-keys (:data data)
                        [:qname :worker-id :runner :phase])
                     {:qname "option-validation"
                      :worker-id (:worker-id worker-error)
                      :runner 0, :phase :loop}))])))
        (finally (.close ^java.io.Closeable worker)))))

  (testing "Public settle-result key sets per action"
    (let [q (mq/queue mgr_ (qname)
              {:lease-ms 60000, :max-attempts 3
               :retry-base-ms 0, :retry-max-ms 0})
          claim-and-settle!
          (fn [mid outcome]
            (is (= (:action (mq/msg-enqueue! q {:msg mid} {:mid mid})) :added))
            (let [claim (mq/msg-claim! q)]
              (is (= (:mid claim) mid))
              [(mq/msg-settle! q mid (:lease-token claim) outcome)
               (:lease-token claim)]))]
      (try
        (let [[acked token] (claim-and-settle! "ack" (mq/outcome:ack))]
          [(is (= acked {:success? true, :action :acked
                         :successor-promoted? false}))
           (is (= (mq/msg-settle! q "ack" token (mq/outcome:ack))
                 {:success? false, :action :stale}))])
        (let [[retried _] (claim-and-settle! "retry"
                            (mq/outcome:retry {:delay-ms 60000}))]
          [(is (= (:action retried) :retried))
           (is (= (set (keys retried))
                 #{:success? :action :successor-promoted?
                   :retry-at-ms :retry-delay-ms}))])
        (let [[dead _] (claim-and-settle! "dead" (mq/outcome:dead))]
          [(is (= (:action dead) :dead))
           (is (= (set (keys dead))
                 #{:success? :action :successor-promoted? :server-time-ms}))])
        (let [[discarded _] (claim-and-settle! "discard" (mq/outcome:discard))]
          [(is (= (:action discarded) :discarded))
           (is (= (set (keys discarded))
                 #{:success? :action :successor-promoted?}))])
        (finally (mq/queue-clear!! q))))))

(deftest _queue-state-machine
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 500, :max-attempts 2
             :retry-base-ms 0, :retry-max-ms 0})]
    (try
      (testing "Idempotent enqueue, conflict detection, and status"
        [(is (= (:action (mq/msg-enqueue! q {:value 1} {:mid "m1"})) :added))
         (is (= (:action (mq/msg-enqueue! q {:value 1} {:mid "m1"})) :existing))
         (is (= (:error  (mq/msg-enqueue! q {:value 2} {:mid "m1"})) :mid-conflict))
         (is (= (mq/msg-status q "m1") :ready))])

      (testing "Message MIDs have one stable textual representation"
        (is (= (:action (mq/msg-enqueue! q :named {:mid :named/mid})) :added))
        (is (= (mq/msg-status q 'named/mid) :ready))
        (is (mq/msg-remove! q "named/mid")))

      (testing "Priority and FIFO within a priority"
        (mq/msg-enqueue! q :low  {:mid "low",  :priority :low})
        (mq/msg-enqueue! q :high {:mid "high", :priority :high})
        (mq/msg-enqueue! q :normal-1 {:mid "normal-1"})
        (mq/msg-enqueue! q :normal-2 {:mid "normal-2"})
        (let [claims (repeatedly 5 #(deref (future (#'mq/claim! q 64)) 5000 :timeout))]
          [(is (= (mapv :mid claims) ["high" "m1" "normal-1" "normal-2" "low"]))
           (doseq [{:keys [mid token]} claims]
             (is (= (:action (#'mq/settle! q mid token (mq/outcome:ack))) :acked)))]))

      (testing "Scheduling uses Redis server time"
        (let [enqueued (mq/msg-enqueue! q :later {:mid "later", :delay-ms 100})]
          [(is (pos-int? (:enqueued-at-ms enqueued)))
           (is (>= (:available-at-ms enqueued) (:enqueued-at-ms enqueued)))
           (is (= (mq/msg-status q "later") :scheduled))
           (is (pos-int? (:available-at-ms (mq/msg-info q "later"))))
           (is (= (:action (#'mq/claim! q 64)) :idle))
           (wait-for-server-time! q (:available-at-ms enqueued))
           (let [{:keys [mid token]} (#'mq/claim! q 64)]
             (is (= mid "later"))
             (#'mq/settle! q mid token (mq/outcome:ack)))]))

      (testing "Explicit retry delays schedule another attempt"
        (mq/msg-enqueue! q :retry-later {:mid "retry-later"})
        (let [{:keys [mid token]} (#'mq/claim! q 64)
              settled (#'mq/settle! q mid token
                        (mq/outcome:retry {:delay-ms 1000, :reason "later"}))
              info (mq/msg-info q mid)]
          [(is (= (:action settled) :retried))
           (is (= (:retry-delay-ms settled) 1000))
           (is (= (:status info) :scheduled))
           (is (pos-int? (:available-at-ms info)))
           (is (= (:retry-at-ms settled) (:available-at-ms info)))
           (mq/msg-remove! q mid)]))

      (testing "Stale settlement and renewal are fenced"
        (mq/msg-enqueue! q :lease {:mid "lease"})
        (let [lease-a (#'mq/claim! q 64)]
          (wait-for-server-time! q (:lease-expiry-ms lease-a))
          (let [lease-b (#'mq/claim! q 64)]
            [(is (= (:mid lease-b) "lease"))
             (is (nil? (#'mq/extend-lease! q "lease" (:token lease-a))))
             (is (= (:action (#'mq/settle! q "lease" (:token lease-a) (mq/outcome:ack))) :stale))
             (is (= (mq/msg-status q "lease") :leased))
             (is (= (:action (#'mq/settle! q "lease" (:token lease-b) (mq/outcome:ack))) :acked))])))

      (testing "Lease extension survives the original expiry"
        (mq/msg-enqueue! q :extended {:mid "extended"})
        (let [{:keys [token] :as claim} (#'mq/claim! q 64)]
          (wait-for-server-time! q (+ (:server-time-ms claim) 300))
          (is (> (#'mq/extend-lease! q "extended" token) (:lease-expiry-ms claim)))
          (wait-for-server-time! q (:lease-expiry-ms claim))
          (is (= (:action (#'mq/claim! q 64)) :idle))
          (is (= (:action (#'mq/settle! q "extended" token (mq/outcome:ack))) :acked))))

      (testing "Explicit terminal outcomes"
        (mq/msg-enqueue! q :discarded {:mid "discarded"})
        (let [claim (#'mq/claim! q 64)]
          (is (= (:action (#'mq/settle! q "discarded" (:token claim)
                           (mq/outcome:discard))) :discarded))
          (is (nil? (mq/msg-status q "discarded"))))
        (mq/msg-enqueue! q :terminal {:mid "terminal"})
        (let [claim (#'mq/claim! q 64)]
          (is (= (:action (#'mq/settle! q "terminal" (:token claim)
                           (mq/outcome:dead {:reason "requested"})))
                :dead))
          (is (= (:reason (mq/dead-info q "terminal")) "requested"))
          (mq/msg-remove! q "terminal"))
        (mq/msg-enqueue! q :terminal-default {:mid "terminal-default"})
        (let [claim (#'mq/claim! q 64)]
          (#'mq/settle! q "terminal-default" (:token claim)
            (mq/outcome:dead {:reason nil}))
          (is (= (:reason (mq/dead-info q "terminal-default"))
                "handler-requested"))
          (mq/msg-remove! q "terminal-default")))

      (testing "Retry exhaustion, dead-letter inspection, and redrive"
        (mq/msg-enqueue! q {:job :fragile} {:mid "dead"})
        (let [a (#'mq/claim! q 64)]
          (is (= (:action (#'mq/settle! q "dead" (:token a)
                           (mq/outcome:retry {:reason "first"})))
                :retried)))
        (let [b (#'mq/claim! q 64)]
          (is (= (:action (#'mq/settle! q "dead" (:token b)
                           (mq/outcome:retry {:reason "second"})))
                :dead)))
        [(is (= (mq/msg-status q "dead") :dead))
         (is (= (mq/dead-mids q) ["dead"]))
         (is (truss/submap? (mq/dead-info q "dead")
               {:msg {:job :fragile}, :attempt 2, :reason "second"}))
         (is (pos-int? (:failed-at-ms (mq/dead-info q "dead"))))
         (is (pos-int? (:enqueued-at-ms (mq/dead-info q "dead"))))
         (is (= (mq/dead-redrive! q "dead") {:success? true, :action :redriven}))
         (is (= (mq/msg-status q "dead") :ready))
         (is (mq/msg-remove! q "dead"))
         (is (nil? (mq/msg-status q "dead")))])

      (testing "Dead-letter corruption is distinct from absence"
        (doseq [[mid mutation expected-reason]
                [["corrupt-dead-meta"
                  #(car/hset (get (queue-keys q) :failures) % "invalid-msgpack")
                  :invalid-failure-meta]
                 ["corrupt-dead-fields"
                  #(car/lua
                     "redis.call('hset', KEYS[1], ARGV[1], cmsgpack.pack({1})); return 1"
                     [(get (queue-keys q) :failures)] [%])
                  :invalid-failure-meta]
                 ["corrupt-dead-fractional"
                  #(car/lua
                     "local f=cmsgpack.unpack(redis.call('hget', KEYS[1], ARGV[1])); f[4]=1.5; redis.call('hset', KEYS[1], ARGV[1], cmsgpack.pack(f)); return 1"
                     [(get (queue-keys q) :failures)] [%])
                  :invalid-failure-meta]
                 ["corrupt-dead-priority"
                  #(car/lua
                     "local f=cmsgpack.unpack(redis.call('hget', KEYS[1], ARGV[1])); f[5]=99; redis.call('hset', KEYS[1], ARGV[1], cmsgpack.pack(f)); return 1"
                     [(get (queue-keys q) :failures)] [%])
                  :invalid-failure-meta]
                 ["corrupt-dead-partial"
                  #(car/hdel (get (queue-keys q) :dead-payloads) %)
                  :incomplete-record]]]
          (mq/msg-enqueue! q :failed {:mid mid})
          (let [claim (#'mq/claim! q 64)]
            (#'mq/settle! q mid (:token claim) (mq/outcome:dead)))
          (car/wcar @mgr_ (mutation mid))
          (is (= (mq/dead-info q mid)
                {:mid mid, :status :corrupt, :corruption expected-reason}))
          (mq/msg-remove! q mid))
        (is (nil? (mq/dead-info q "absent-dead"))))

      (testing "Dead-letter purging is bounded"
        (doseq [mid ["purge-1" "purge-2"]]
          (mq/msg-enqueue! q :failed {:mid mid, :max-attempts 1})
          (let [claim (#'mq/claim! q 64)]
            (#'mq/settle! q mid (:token claim)
              (mq/outcome:dead {:reason "purge"}))))
        (is (= (:removed (mq/dead-purge! q {:older-than-ms 100000})) 0))
        (is (truss/submap? (mq/dead-purge! q {:older-than-ms 0, :limit 1})
              {:removed 1, :more? true}))
        (is (truss/submap? (mq/dead-purge! q {:older-than-ms 0, :limit 1})
              {:removed 1, :more? false}))
        (is (throws? :ex-info {:eid :carmine.mq/purge-age-required}
              (mq/dead-purge! q {}))))

      (testing "Lease expiry consumes attempts and removal fences handlers"
        (mq/msg-enqueue! q :expires {:mid "expires", :max-attempts 1})
        (let [claim (#'mq/claim! q 64)]
          (wait-for-server-time! q (:lease-expiry-ms claim))
          (#'mq/claim! q 64) ; Bounded maintenance moves exhausted lease to dead.
          [(is (= (mq/msg-status q "expires") :dead))
           (is (= (:reason (mq/dead-info q "expires")) "lease-expired"))
           (is (mq/msg-remove! q "expires"))
           (is (= (:action (#'mq/settle! q "expires" (:token claim) (mq/outcome:ack)))
                 :stale))]))

      (testing "Releasing a claim does not consume an attempt"
        (mq/msg-enqueue! q :release {:mid "release", :max-attempts 1})
        (let [claim-a (#'mq/claim! q 64)]
          (is (= (:attempt claim-a) 1))
          (is (= (#'mq/release! q "release" (:token claim-a)) :released))
          (is (= (mq/msg-status q "release") :ready))
          (let [claim-b (#'mq/claim! q 64)]
            (is (= (:attempt claim-b) 1))
            (is (= (:action (#'mq/settle! q "release" (:token claim-b) (mq/outcome:ack)))
                  :acked)))))

      (testing "Corrupt settlement metadata is reported without reply coercion"
        (mq/msg-enqueue! q :corrupt {:mid "corrupt"})
        (let [claim (#'mq/claim! q 64)]
          (car/wcar @mgr_ (car/hset (get (queue-keys q) :meta) "corrupt" "invalid-msgpack"))
          (is (throws? :ex-info {:eid :carmine.mq/settlement-failed}
                (#'mq/settle! q "corrupt" (:token claim) (mq/outcome:ack))))
          (mq/msg-remove! q "corrupt")))

      (testing "Queue status reflects indexed positions"
        (let [ready (mq/msg-enqueue! q :ready {:mid "status-ready"})
              scheduled (mq/msg-enqueue! q :scheduled
                          {:mid "status-scheduled" :delay-ms 10000})
              status (mq/queue-status q)]
          [(is (= (:counts status)
                 {:ready 1, :overdue 0, :scheduled 1, :leased 0
                  :lease-expired 0, :dead 0, :successors 0, :active 2}))
           (is (= (:heads status)
                 {:ready-enqueued-at-ms
                  {:high nil, :normal (:enqueued-at-ms ready), :low nil}
                  :scheduled-available-at-ms (:available-at-ms scheduled)
                  :leased-expiry-at-ms nil
                  :dead-failed-at-ms nil}))
           (is (truss/submap? (:lags-ms status)
                 {:scheduled-overdue nil, :lease-expired nil
                  :next-lease-expiry-in nil, :dead-age nil}))
           (is (pos? (get-in status [:lags-ms :next-scheduled-in])))
           (is (nat-int? (get-in status [:lags-ms :ready-age :normal])))
           (is (pos-int? (:server-time-ms status)))])
        (mq/msg-remove! q "status-ready")
        (mq/msg-remove! q "status-scheduled"))

      (testing "Queue lags distinguish current conditions from future deadlines"
        (let [now (:server-time-ms (mq/queue-status q))
              overdue-at (- now 1000)
              scheduled-at (+ now 60000)
              lease-expired-at (- now 2000)
              lease-live-at (+ now 90000)]
          (mq/msg-enqueue! q :overdue {:mid "status-overdue", :delay-ms 60000})
          (mq/msg-enqueue! q :scheduled {:mid "status-future", :delay-ms 60000})

          (mq/msg-enqueue! q :expired {:mid "status-expired"})
          (is (= (:mid (#'mq/claim! q 64)) "status-expired"))
          (mq/msg-enqueue! q :leased {:mid "status-leased"})
          (is (= (:mid (#'mq/claim! q 64)) "status-leased"))

          (mq/msg-enqueue! q :dead {:mid "status-dead"})
          (let [{:keys [mid token]} (#'mq/claim! q 64)]
            (is (= mid "status-dead"))
            (#'mq/settle! q mid token
              (mq/outcome:dead {:reason "status-gauge"})))

          (car/wcar @mgr_
            (car/zadd (get (queue-keys q) :scheduled) overdue-at "status-overdue")
            (car/zadd (get (queue-keys q) :scheduled) scheduled-at "status-future")
            (car/zadd (get (queue-keys q) :leased) lease-expired-at "status-expired")
            (car/zadd (get (queue-keys q) :leased) lease-live-at "status-leased"))

          (let [status (mq/queue-status q)]
            [(is (= (:counts status)
                   {:ready 0, :overdue 1, :scheduled 1, :leased 1
                    :lease-expired 1, :dead 1, :successors 0, :active 4}))
             (is (truss/submap? (:heads status)
                   {:scheduled-available-at-ms overdue-at
                    :leased-expiry-at-ms lease-expired-at}))
             (is (<= 1000 (get-in status [:lags-ms :scheduled-overdue])))
             (is (pos? (get-in status [:lags-ms :next-scheduled-in])))
             (is (<= 2000 (get-in status [:lags-ms :lease-expired])))
             (is (pos? (get-in status [:lags-ms :next-lease-expiry-in])))
             (is (nat-int? (get-in status [:lags-ms :dead-age])))])

          (doseq [mid ["status-overdue" "status-future" "status-expired"
                      "status-leased" "status-dead"]]
            (mq/msg-remove! q mid))))

      (testing "Durable configuration rejects mismatched handles"
        [(is (throws? :ex-info {:eid :carmine.mq/config-mismatch}
               (mq/queue mgr_ (queue-name q) {:lease-ms 999})))
         (is (throws? :ex-info {:eid :carmine.mq/config-mismatch}
               (mq/queue mgr_ (queue-name q) {:on-exhaustion :discard})))
         (is (throws? :ex-info {:eid :carmine.mq/config-mismatch}
               (mq/queue mgr_ (queue-name q)
                 {:on-duplicate :coalesce, :revision-mode :required})))])

      (finally (mq/queue-clear!! q)))))

(deftest _exhaustion-and-coalescing
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 150, :max-attempts 3
             :retry-base-ms 0, :retry-max-ms 0
             :on-duplicate :coalesce})]
    (try
      (testing "Automatic exhaustion can discard while explicit dead-letter retains"
        (mq/msg-enqueue! q :automatic {:mid "discard", :max-attempts 1
                                    :on-exhaustion :discard})
        (let [claim (#'mq/claim! q 64)]
          (is (truss/submap?
                (#'mq/settle! q "discard" (:token claim)
                  (mq/outcome:retry {:reason "exhausted"}))
                {:action :discarded, :successor-promoted? false})))
        (is (nil? (mq/msg-info q "discard")))

        (mq/msg-enqueue! q :explicit {:mid "explicit", :max-attempts 1
                                   :on-exhaustion :discard})
        (let [claim (#'mq/claim! q 64)]
          (is (= (:action (#'mq/settle! q "explicit" (:token claim)
                            (mq/outcome:dead {:reason "operator"})))
                :dead)))
        (is (= (:reason (mq/dead-info q "explicit")) "operator"))
        (mq/msg-remove! q "explicit"))

      (testing "Lease-expiry exhaustion honors the per-message policy"
        (mq/msg-enqueue! q :lease-expiry {:mid "lease-discard", :max-attempts 1
                                      :on-exhaustion :discard})
        (let [claim (#'mq/claim! q 64)]
          (wait-for-server-time! q (:lease-expiry-ms claim)))
        (#'mq/claim! q 64)
        (is (nil? (mq/msg-status q "lease-discard"))))

      (testing "Never-claimed work coalesces in place"
        (is (= (:action (mq/msg-enqueue! q :v1 {:mid "in-place", :delay-ms 10000})) :added))
        (is (= (:action (mq/msg-enqueue! q :v2
                          {:mid "in-place", :delay-ms 0, :priority :high})) :coalesced))
        (is (truss/submap? (mq/msg-info q "in-place")
              {:status :ready, :attempt 0, :successor? false}))
        (let [claim (#'mq/claim! q 64)]
          (is (truss/submap? claim
                {:mid "in-place", :msg :v2, :priority :high}))
          (#'mq/settle! q "in-place" (:token claim) (mq/outcome:ack))))

      (testing "Running updates collapse to one latest successor"
        (mq/msg-enqueue! q :v1 {:mid "successor"})
        (let [active (#'mq/claim! q 64)]
          [(is (= (:action (mq/msg-enqueue! q :v2 {:mid "successor"}))
                  :coalesced-successor))
           (is (= (:action (mq/msg-enqueue! q :v3 {:mid "successor"}))
                  :coalesced-successor))
           (is (truss/submap? (mq/msg-info q "successor")
                 {:status :leased, :successor? true}))
           (is (truss/submap?
                 (#'mq/settle! q "successor" (:token active) (mq/outcome:ack))
                 {:action :acked, :successor-promoted? true}))])
        (let [successor (#'mq/claim! q 64)]
          [(is (= (:msg successor) :v3))
           (is (= (:attempt successor) 1))
           (#'mq/settle! q "successor" (:token successor) (mq/outcome:ack))]))

      (testing "Releasing an unhandled claim cannot resurrect an older successor"
        (mq/msg-enqueue! q :v1 {:mid "release-successor"})
        (let [active (#'mq/claim! q 64)]
          (is (= (:action (mq/msg-enqueue! q :v2 {:mid "release-successor"}))
                :coalesced-successor))
          (is (= (#'mq/release! q "release-successor" (:token active))
                :released-successor)))
        [(is (truss/submap? (mq/msg-info q "release-successor")
               {:status :ready, :attempt 0, :successor? false}))
         (is (= (:action (mq/msg-enqueue! q :v3 {:mid "release-successor"})) :coalesced))]
        (let [latest (#'mq/claim! q 64)]
          [(is (= (:msg latest) :v3))
           (#'mq/settle! q "release-successor" (:token latest) (mq/outcome:ack))])
        (is (= (:action (#'mq/claim! q 64)) :idle))
        (is (nil? (mq/msg-status q "release-successor"))))

      (testing "An attempted retry never receives newer content ahead of its successor"
        (mq/msg-enqueue! q :v1 {:mid "retry-order"})
        (let [first-claim (#'mq/claim! q 64)]
          (mq/msg-enqueue! q :v2 {:mid "retry-order"})
          (is (= (:action (#'mq/settle! q "retry-order" (:token first-claim)
                           (mq/outcome:retry))) :retried)))
        (is (= (:action (mq/msg-enqueue! q :v3 {:mid "retry-order"}))
              :coalesced-successor))
        (let [retry-claim (#'mq/claim! q 64)]
          [(is (= (:msg retry-claim) :v1))
           (is (:successor? (mq/msg-info q "retry-order")))
           (is (:successor-promoted?
                 (#'mq/settle! q "retry-order" (:token retry-claim) (mq/outcome:ack))))])
        (let [latest (#'mq/claim! q 64)]
          (is (= (:msg latest) :v3))
          (#'mq/settle! q "retry-order" (:token latest) (mq/outcome:ack))))

      (testing "Discard exhaustion still promotes a producer-requested successor"
        (mq/msg-enqueue! q :old {:mid "discard-promote", :max-attempts 1
                             :on-exhaustion :discard})
        (let [active (#'mq/claim! q 64)]
          (mq/msg-enqueue! q :latest {:mid "discard-promote"})
          (is (truss/submap?
                (#'mq/settle! q "discard-promote" (:token active)
                  (mq/outcome:retry))
                {:action :discarded, :successor-promoted? true})))
        (let [latest (#'mq/claim! q 64)]
          (is (= (:msg latest) :latest))
          (#'mq/settle! q "discard-promote" (:token latest) (mq/outcome:ack))))

      (testing "A retained dead generation can coexist with newer active work"
        (mq/msg-enqueue! q :failed {:mid "coexist"})
        (let [active (#'mq/claim! q 64)]
          (mq/msg-enqueue! q :successor {:mid "coexist"})
          (#'mq/settle! q "coexist" (:token active)
            (mq/outcome:dead {:reason "old"})))
        [(is (truss/submap? (mq/msg-info q "coexist")
               {:status :ready, :prior-dead? true, :successor? false}))
         (is (= (:msg (mq/dead-info q "coexist")) :failed))
         (is (= (mq/dead-redrive! q "coexist")
               {:success? false, :action :active-exists}))]
        (let [active (#'mq/claim! q 64)]
          (is (= (:msg active) :successor))
          (#'mq/settle! q "coexist" (:token active) (mq/outcome:ack)))
        (is (= (mq/msg-status q "coexist") :dead))
        (let [result (mq/msg-enqueue! q :fresh {:mid "coexist"})]
          [(is (= (:action result) :added))
           (is (:prior-dead? result))])
        (is (= (:removed (mq/dead-purge! q {:older-than-ms 0})) 1))
        (is (= (mq/msg-status q "coexist") :ready))
        (mq/msg-remove! q "coexist"))

      (is (= (mq/dead-redrive! q "missing") {:success? false, :action :not-dead}))

      (testing "Schema-1 active metadata without additive fields remains readable"
        (mq/msg-enqueue! q :legacy {:mid "legacy"})
        (car/wcar @mgr_
          (car/lua
            "local m=cmsgpack.unpack(redis.call('hget', KEYS[1], ARGV[1])); m[6]=nil; m[7]=nil; redis.call('hset', KEYS[1], ARGV[1], cmsgpack.pack(m)); return 1"
            [(get (queue-keys q) :meta)] ["legacy"]))
        (let [claim (#'mq/claim! q 64)]
          [(is (= (:msg claim) :legacy))
           (is (= (:action (#'mq/settle! q "legacy" (:token claim) (mq/outcome:ack)))
                  :acked))]))

      (finally (mq/queue-clear!! q)))))

(deftest _coalescing-revisions
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :revision-mode :required
             :retry-base-ms 0, :retry-max-ms 0})]
    (try
      [(is (throws? :ex-info {:eid :carmine.mq/revision-required}
             (mq/msg-enqueue! q :missing {:mid "revision"})))
       (is (= (:action (mq/msg-enqueue! q :v1 {:mid "revision", :revision 1})) :added))
       (is (= (:action (mq/msg-enqueue! q :old {:mid "revision", :revision 0}))
             :stale-revision))
       (is (= (:error (mq/msg-enqueue! q :different {:mid "revision", :revision 1}))
             :revision-conflict))
       (is (= (:action (mq/msg-enqueue! q :v2 {:mid "revision", :revision 2}))
             :coalesced))]
      (let [active (#'mq/claim! q 64)]
        [(is (= (:msg active) :v2))
         (is (= (:action (mq/msg-enqueue! q :v3 {:mid "revision", :revision 3}))
               :coalesced-successor))
         (is (= (:action (mq/msg-enqueue! q :stale {:mid "revision", :revision 2}))
               :stale-revision))
         (#'mq/settle! q "revision" (:token active) (mq/outcome:ack))])
      (let [successor (#'mq/claim! q 64)]
        (is (= (:msg successor) :v3))
        (#'mq/settle! q "revision" (:token successor) (mq/outcome:ack)))
      (finally (mq/queue-clear!! q)))))

(deftest _revision-replay-idempotency
  ;; An identical (revision + payload) enqueue replay must remain idempotent
  ;; (:existing) even after the active generation has been claimed.
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :revision-mode :required
             :retry-base-ms 0, :retry-max-ms 0})]
    (try
      [(is (= (:action (mq/msg-enqueue! q :v1 {:mid "replay", :revision 1})) :added))
       (is (= (:action (mq/msg-enqueue! q :v1 {:mid "replay", :revision 1})) :existing)
         "Pre-claim replay")]
      (let [active (#'mq/claim! q 64)]
        [(is (= (:msg active) :v1))
         (is (= (:action (mq/msg-enqueue! q :v1 {:mid "replay", :revision 1})) :existing)
           "Post-claim replay of the active generation")
         (is (= (:error (mq/msg-enqueue! q :other {:mid "replay", :revision 1}))
               :revision-conflict)
           "Same revision with a different payload still conflicts")
         (is (= (:action (mq/msg-enqueue! q :v2 {:mid "replay", :revision 2}))
               :coalesced-successor))
         (is (= (:action (mq/msg-enqueue! q :v2 {:mid "replay", :revision 2})) :existing)
           "Replay against an existing successor record")
         (#'mq/settle! q "replay" (:token active) (mq/outcome:ack))])
      (finally (mq/queue-clear!! q)))))

(deftest _legacy-revision-successor-upgrade
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :revision-mode :required
             :retry-base-ms 0, :retry-max-ms 0})]
    (try
      (mq/msg-enqueue! q :v1 {:mid "legacy-revision", :revision 1})
      ;; Simulate an active record written before the additive revision field
      ;; existed, then claimed after an upgraded handle enabled revisions.
      (car/wcar @mgr_
        (car/lua
          "local m=cmsgpack.unpack(redis.call('hget', KEYS[1], ARGV[1])); m[7]=nil; redis.call('hset', KEYS[1], ARGV[1], cmsgpack.pack(m)); return 1"
          [(get (queue-keys q) :meta)] ["legacy-revision"]))
      (let [active (#'mq/claim! q 64)
            result (mq/msg-enqueue! q :v2 {:mid "legacy-revision", :revision 2})]
        [(is (= (:msg active) :v1))
         (is (= (:action result) :coalesced-successor))
         (is (= (:action (#'mq/settle! q "legacy-revision" (:token active)
                           (mq/outcome:ack)))
               :acked))])
      (let [successor (#'mq/claim! q 64)]
        [(is (= (:msg successor) :v2))
         (is (= (:action (#'mq/settle! q "legacy-revision" (:token successor)
                           (mq/outcome:ack)))
               :acked))])
      (finally (mq/queue-clear!! q)))))

(deftest _lua-safe-numeric-bounds
  (let [q (mq/queue mgr_ (qname)
            {:on-duplicate :coalesce, :revision-mode :required})]
    (try
      [(is (throws? :ex-info {:eid :carmine.mq/invalid-option}
             (mq/msg-enqueue! q :v {:mid "bounds", :revision 1, :delay-ms (inc 10000000000000)}))
         "Durations are bounded (Lua number exactness)")
       (is (throws? :ex-info {:eid :carmine.mq/invalid-option}
             (mq/msg-enqueue! q :v {:mid "bounds", :revision (inc 9007199254740991)}))
         "Revisions are bounded to 2^53-1")
       (is (nil? (mq/msg-status q "bounds"))
         "Rejected enqueues perform no writes")
       (is (= (:action (mq/msg-enqueue! q :v {:mid "bounds", :revision 9007199254740991})) :added)
         "Max Lua-safe revision accepted")
       (is (throws? :ex-info {:eid :carmine.mq/invalid-outcome-options}
             (mq/outcome:retry {:delay-ms (inc 10000000000000)}))
         "Outcome delays enforce the duration maximum at the helper boundary")]
      (finally (mq/queue-clear!! q)))))

(deftest _queue-default-exhaustion
  (let [q (mq/queue mgr_ (qname)
            {:on-exhaustion :discard, :max-attempts 1
             :retry-base-ms 0, :retry-max-ms 0})]
    (try
      (mq/msg-enqueue! q :default-discard {:mid "default-discard"})
      (let [claim (#'mq/claim! q 64)]
        (is (= (:action (#'mq/settle! q "default-discard" (:token claim)
                          (mq/outcome:retry))) :discarded)))
      (is (nil? (mq/msg-status q "default-discard")))

      (mq/msg-enqueue! q :override {:mid "override", :on-exhaustion :dead})
      (let [claim (#'mq/claim! q 64)]
        (is (= (:action (#'mq/settle! q "override" (:token claim)
                          (mq/outcome:retry {:reason "retained"})))
              :dead)))
      (is (= (:reason (mq/dead-info q "override")) "retained"))
      (finally (mq/queue-clear!! q)))))

(deftest _worker-handler-outcomes
  (let [q (mq/queue mgr_ (qname)
            {:max-attempts 3, :retry-base-ms 0, :retry-max-ms 0})
        handler-keys_ (atom [])
        events_ (atom [])
        worker
        (mq/worker-create q
          (fn [{:keys [msg attempt] :as input}]
            (swap! handler-keys_ conj (set (keys input)))
            (case msg
              :ack     (mq/outcome:ack)
              :retry   (if (= attempt 1)
                         (mq/outcome:retry {:reason "temporary"})
                         (mq/outcome:ack))
              :dead    (mq/outcome:dead)
              :discard (mq/outcome:discard)))
          {:idle-max-ms 20, :on-event #(swap! events_ conj %)})]
    (try
      (is (mq/worker-start! worker))
      (doseq [message [:ack :retry :dead :discard]]
        (let [result (mq/msg-enqueue! q message {:mid (name message)})]
          [(is (= (:action result) :added))
           (is (pos-int? (:enqueued-at-ms result)))
           (is (pos-int? (:available-at-ms result)))]))
      (is (wait-for 3000
            #(and (nil? (mq/msg-status q "ack"))
               (nil? (mq/msg-status q "retry"))
               (= (mq/msg-status q "dead") :dead)
               (nil? (mq/msg-status q "discard")))))
      (is (wait-for 2000
            #(= (reduce + (vals (get-in (mq/worker-stats worker)
                                  [:counts :settlements])))
               5)))
      (let [expected-keys
            #{:queue :mid :msg :attempt :priority :enqueued-at-ms :age-ms
              :lease-expiry-ms :server-time-ms :extend-lease!}
            settled (filterv #(= (:event %) :settled) @events_)
            stats (mq/worker-stats worker)]
        [(is (= @handler-keys_ (repeat 5 expected-keys)))
         (is (= (frequencies (map :action settled))
               {:acked 2, :retried 1, :dead 1, :discarded 1}))
         (is (= (frequencies (map :intent settled))
               {:ack 2, :retry 1, :dead 1, :discard 1}))
         (is (every? #(pos-int? (:client-time-ms %)) settled))
         (is (every? #(truss/submap? % {:outcome :submap/nx}) settled))
         (is (= (:reason (mq/dead-info q "dead")) "handler-requested"))
         (is (truss/submap? (:counts stats)
               {:claims 5, :handler-calls 5}))
         (is (= (get-in stats [:counts :handler-intents])
               {:ack 2, :retry 1, :dead 1, :discard 1}))
         (is (= (get-in stats [:counts :settlements])
               {:acked 2, :retried 1, :dead 1, :discarded 1, :stale 0}))
         (is (= (get-in stats [:timings :settlement-round-trip :count]) 5))
         (is (<= 5 (get-in stats [:timings :claim-round-trip :count])))
         (is (= (get-in stats [:timings :claim-age :count]) 5))
         (is (= (get-in stats [:timings :first-claim-age :count]) 4))])
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _invalid-handler-returns-retry
  (doseq [[label invalid]
          [["nil" nil]
           ["keyword" :ack]
           ["map" {:action :ack}]
           ["number" 42]]]
    (let [q (mq/queue mgr_ (qname)
              {:max-attempts 2, :retry-base-ms 10000, :retry-max-ms 10000})
          events_ (atom [])
          worker (mq/worker-create q (constantly invalid)
                   {:idle-max-ms 20, :on-event #(swap! events_ conj %)})]
      (try
        (is (mq/worker-start! worker))
        (mq/msg-enqueue! q label {:mid label})
        (is (wait-for 2000
              #(some (fn [event]
                       (when (and (= (:event event) :settled)
                               (= (:mid event) label))
                         event))
                 @events_)))
        (let [invalid-event (some #(when (= (:event %) :invalid-handler-return) %) @events_)
              settled-event (some #(when (= (:event %) :settled) %) @events_)]
          [(is (= (mq/msg-status q label) :scheduled)
             "Invalid returns must never acknowledge")
           (is (= (:intent settled-event) :retry))
           (is (= (:action settled-event) :retried))
           (is (= (:reason settled-event) "invalid-handler-return"))
           (is (= (:mid invalid-event) label))
           (is (string? (:return-type invalid-event)))
           (is (= (get-in (mq/worker-stats worker)
                    [:counts :invalid-handler-returns]) 1))])
        (finally
          (.close ^java.io.Closeable worker)
          (mq/queue-clear!! q))))))

(deftest _worker
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 1000, :max-attempts 3
             :retry-base-ms 0, :retry-max-ms 0})
        attempts_ (atom [])
        events_ (atom [])
        done (promise)
        worker
        (mq/worker-create q
          (fn [{:keys [attempt msg extend-lease!]}]
            (swap! attempts_ conj attempt)
            (is (= msg :work))
            (is (pos-int? (extend-lease!)))
            (if (= attempt 1)
              (throw (Exception. "retry me"))
              (do (deliver done true) (mq/outcome:ack))))
          {:concurrency 2, :idle-max-ms 20
           :on-event #(swap! events_ conj %)})]
    (try
      [(is (mq/worker-start! worker))
       (is (false? (mq/worker-start! worker)))
       (is (= (:action (mq/msg-enqueue! q :work {:mid "work"})) :added))
       (is (= (deref done 3000 :timeout) true))
       (is (wait-for 2000 #(nil? (mq/msg-status q "work"))))
       (is (= @attempts_ [1 2]))
       (is (wait-for 1000
             #(= (count (filter (fn [event] (= (:event event) :handler-error)) @events_)) 1)))
       (let [event (some #(when (= (:event %) :handler-error) %) @events_)]
         [(is (truss/submap? event
                {:mid "work", :attempt 1, :priority :normal}))
          (is (pos-int? (:enqueued-at-ms event)))
          (is (nat-int? (:age-ms event)))
          (is (pos-int? (:server-time-ms event)))
          (is (number? (:handler-ms event)))])
       (let [stats (mq/worker-stats worker)]
         [(is (= (get-in stats [:counts :handler-errors]) 1))
          (is (= (get-in stats [:counts :lease-extensions :extended]) 2))
          (is (= (get-in stats [:counts :handler-intents])
                {:ack 1, :retry 1, :dead 0, :discard 0}))
          (is (= (get-in stats [:counts :settlements :retried]) 1))
          (is (= (get-in stats [:counts :settlements :acked]) 1))])
       (is (mq/worker-stop! worker))
       (is (mq/worker-await-stopped! worker 2000))
       (is (= (:state @worker) :stopped))]
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _worker-lease-heartbeat
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 500, :max-attempts 3
             :retry-base-ms 0, :retry-max-ms 0})
        started_ (promise)
        release_ (promise)
        worker
        (mq/worker-create q
          (fn [_]
            (deliver started_ true)
            (deref release_ 5000 :timeout)
            (mq/outcome:ack))
          {:idle-max-ms 20, :lease-extend-every-ms 100})]
    (try
      [(is (mq/worker-start! worker))
       (is (= (:action (mq/msg-enqueue! q :work {:mid "hb"})) :added))
       (is (= (deref started_ 3000 :timeout) true))
       ;; Wait past the un-extended lease deadline, then let a competing
       ;; claimant attempt token-fenced reaping.
       (do (Thread/sleep 700) :slept)
       (is (= (:action (mq/msg-claim! q)) :idle)
         "The heartbeat kept the lease alive past its original expiry")
       (is (>= (get-in (mq/worker-stats worker)
                 [:counts :lease-heartbeats :extended]) 1))
       (do (deliver release_ true) :released)
       (is (wait-for 3000
             #(= (get-in (mq/worker-stats worker)
                   [:counts :settlements :acked]) 1))
         "A heartbeat-extended delivery still settles normally")
       (is (nil? (mq/msg-status q "hb")))
       (let [extended (get-in (mq/worker-stats worker)
                        [:counts :lease-heartbeats :extended])]
         (do (Thread/sleep 350) :slept)
         [(is (= (get-in (mq/worker-stats worker)
                   [:counts :lease-heartbeats :extended]) extended)
            "The heartbeat is cancelled before the settlement attempt")
          (is (nil? (mq/msg-status q "hb"))
            "A cancelled heartbeat cannot resurrect a settled lease")])
       (is (= (get-in (mq/worker-stats worker)
                [:counts :lease-heartbeats :stale]) 0))
       (is (mq/worker-stop! worker))
       (is (mq/worker-await-stopped! worker 2000))]
      (finally
        (.close ^java.io.Closeable worker)
        (remove-v4-queue!! q)))))

(deftest _worker-lease-heartbeat-stale-stops
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 1000, :retry-base-ms 0, :retry-max-ms 0})
        started_ (promise)
        release_ (promise)
        worker
        (mq/worker-create q
          (fn [_]
            (deliver started_ true)
            (deref release_ 5000 :timeout)
            (mq/outcome:ack))
          {:idle-max-ms 20, :lease-extend-every-ms 100})]
    (try
      [(is (mq/worker-start! worker))
       (is (= (:action (mq/msg-enqueue! q :work {:mid "hb-stale"})) :added))
       (is (= (deref started_ 3000 :timeout) true))
       (is (:success? (mq/msg-remove! q "hb-stale"))
         "A competing transition fences the running delivery")
       (is (wait-for 2000
             #(= (get-in (mq/worker-stats worker)
                   [:counts :lease-heartbeats :stale]) 1))
         "A fenced beat stops the heartbeat")
       (do (Thread/sleep 350) :slept)
       (let [stats (mq/worker-stats worker)]
         [(is (= (get-in stats [:counts :lease-heartbeats :stale]) 1)
            "A stopped heartbeat beats no further")
          (is (= (get-in stats [:counts :lease-heartbeats :errors]) 0))
          (is (= (get-in stats [:counts :worker-errors]) 0)
            "A stale stop is silent, not an error")])
       (do (deliver release_ true) :released)
       (is (wait-for 3000
             #(= (get-in (mq/worker-stats worker)
                   [:counts :settlements :stale]) 1))
         "The fenced delivery's settlement attempt reports :stale")
       (is (mq/worker-stop! worker))
       (is (mq/worker-await-stopped! worker 2000))]
      (finally
        (.close ^java.io.Closeable worker)
        (remove-v4-queue!! q)))))

(deftest _worker-lease-heartbeat-stops-with-worker
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 1000, :retry-base-ms 0, :retry-max-ms 0})
        started_ (promise)
        release_ (promise)
        events_ (atom [])
        worker
        (mq/worker-create q
          (fn [_]
            (deliver started_ true)
            (deref release_ 5000 :timeout)
            (mq/outcome:ack))
          {:idle-max-ms 20, :lease-extend-every-ms 50
           :on-event #(swap! events_ conj %)})
        worker-id (:worker-id @worker)
        heartbeat-thread-alive?
        (fn []
          (boolean
            (some
              (fn [^Thread thread]
                (and (.isAlive thread)
                  (.contains (.getName thread)
                    (str (subs worker-id 0 8) "-heartbeat"))))
              (keys (Thread/getAllStackTraces)))))]
    (try
      [(is (mq/worker-start! worker))
       (is (= (:action (mq/msg-enqueue! q :work {:mid "hb-stop"})) :added))
       (is (= (deref started_ 3000 :timeout) true))
       (is (wait-for 2000 heartbeat-thread-alive?)
         "An enabled heartbeat runs on a worker-local daemon thread")
       (do (deliver release_ true) :released)
       (is (mq/worker-stop! worker))
       (is (mq/worker-await-stopped! worker 3000))
       (is (wait-for 2000 #(not (heartbeat-thread-alive?)))
         "The heartbeat scheduler retires with its worker")
       (let [counts (get-in (mq/worker-stats worker)
                      [:counts :lease-heartbeats])
             event-count (count @events_)]
         (do (Thread/sleep 250) :slept)
         [(is (= (get-in (mq/worker-stats worker)
                   [:counts :lease-heartbeats]) counts)
            "A terminal worker records no further heartbeat stats")
          (is (= (count @events_) event-count)
            "A terminal worker publishes no further events")])]
      (finally
        (.close ^java.io.Closeable worker)
        (remove-v4-queue!! q)))))

(deftest _worker-lease-heartbeat-error-once
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 1000, :retry-base-ms 0, :retry-max-ms 0})
        started_ (promise)
        release_ (promise)
        events_ (atom [])
        logs_ (atom [])
        worker
        (mq/worker-create q
          (fn [_]
            (deliver started_ true)
            (deref release_ 5000 :timeout)
            (mq/outcome:ack))
          {:idle-max-ms 20, :lease-extend-every-ms 50
           :on-event #(swap! events_ conj %)})
        heartbeat-errors
        (fn []
          (filterv #(and (= (:event %) :worker-error)
                      (= (:phase %) :lease-heartbeat))
            @events_))]
    (try
      (with-redefs [mq/extend-lease!
                    (fn [& _] (throw (Exception. "Expected beat failure")))
                    trove/*log-fn*
                    (fn [_ns _coords level id data_]
                      (swap! logs_ conj
                        {:level level, :id id, :data (force data_)}))]
        [(is (mq/worker-start! worker))
         (is (= (:action (mq/msg-enqueue! q :work {:mid "hb-error"})) :added))
         (is (= (deref started_ 3000 :timeout) true))
         (is (wait-for 2000
               #(= (get-in (mq/worker-stats worker)
                     [:counts :lease-heartbeats :errors]) 1))
           "A failed beat is counted")
         (do (Thread/sleep 300) :slept)
         (is (= (get-in (mq/worker-stats worker)
                  [:counts :lease-heartbeats :errors]) 1)
           "A failed heartbeat stops; failures surface at most once per delivery")
         (is (= (count (heartbeat-errors)) 1))
         (let [event (first (heartbeat-errors))
               {:keys [level data] :as log}
               (some #(when (= (:id %) :carmine.mq/lease-heartbeat-error) %)
                 @logs_)]
           [(is (some? log))
            (is (= level :error))
            (is (= (select-keys (:data data)
                     [:worker-id :runner :phase :mid :attempt :worker-failed?])
                  {:worker-id (:worker-id event)
                   :runner 0, :phase :lease-heartbeat
                   :mid "hb-error", :attempt 1, :worker-failed? false}))])
         (do (deliver release_ true) :released)
         (is (wait-for 3000
               #(= (get-in (mq/worker-stats worker)
                     [:counts :settlements :acked]) 1))
           "Beat failure does not affect the delivery's settlement")
         (is (mq/worker-stop! worker))
         (is (mq/worker-await-stopped! worker 3000))])
      (finally
        (.close ^java.io.Closeable worker)
        (remove-v4-queue!! q)))))

(deftest _worker-blocking-wake
  (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
        handled (promise)
        worker (mq/worker-create q (fn [{:keys [msg]}]
                              (deliver handled msg)
                              (mq/outcome:ack))
                 {:idle-min-ms 10, :idle-max-ms 60000})]
    (try
      (is (mq/worker-start! worker))
      (is (wait-for 5000
            #(pos? (long (get-in @worker [:stats :counts :wake-waits] 0)))))
      (mq/msg-enqueue! q :awake {:mid "awake"})
      (is (= (deref handled 5000 :timeout) :awake)
        "The wake signal beats the worker's long fallback poll")
      (is (wait-for 5000 #(nil? (mq/msg-status q "awake"))))
      (is (pos? (long (get-in @worker [:stats :counts :wake-signals] 0))))
      (is (wait-for 5000 #(zero? (get-in @@mgr_ [:stats :connections :active]))))
      (is (wait-for 5000
            #(>= (long (get-in @worker [:stats :counts :wake-waits] 0)) 2))
        "Worker is back in a NEW blocking wake before the stop below")
      (is (mq/worker-stop! worker))
      (is (mq/worker-await-stopped! worker 5000)
        "Stop interrupts the worker's long blocking wake")
      (is (zero? (long (get-in (mq/worker-stats worker) [:counts :wake-errors] 0)))
        "Gracefully stopping an actively blocked wake conn is not a wake error")
      (is (= (car/wcar @mgr_ (car/ping)) "PONG"))
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _blocked-peer-recomputes-a-new-lease-deadline
  (let [lease-ms 250
        idle-max-ms 60000
        q (mq/queue mgr_ (qname)
            {:lease-ms lease-ms, :max-attempts 2
             :retry-base-ms 0, :retry-max-ms 0})
        first (promise)
        second (promise)
        release-first (promise)
        handler
        (fn [{:keys [attempt] :as delivery}]
          (if (= attempt 1)
            (do (deliver first delivery)
              @release-first
              (mq/outcome:ack))
            (do (deliver second delivery) (mq/outcome:ack))))
        worker-a (mq/worker-create q handler
                   {:idle-min-ms 10, :idle-max-ms idle-max-ms})
        worker-b (mq/worker-create q handler
                   {:idle-min-ms 10, :idle-max-ms idle-max-ms})]
    (try
      [(is (mq/worker-start! worker-a))
       (is (mq/worker-start! worker-b))
       (is (wait-for 5000
             #(and (pos? (get-in (mq/worker-stats worker-a)
                           [:counts :wake-waits]))
                (pos? (get-in (mq/worker-stats worker-b)
                        [:counts :wake-waits]))))
         "Both independent workers are already blocked before the sole enqueue")]
      (mq/msg-enqueue! q :lease-baton {:mid "lease-baton"})
      (let [first-delivery (deref first 5000 :timeout)
            second-delivery (deref second 5000 :timeout)]
        [(is (map? first-delivery))
         (is (map? second-delivery)
           "The retry occurs well before the 60s idle poll; without the lease baton this times out")
         (when (and (map? first-delivery) (map? second-delivery))
           [(is (>= (:server-time-ms second-delivery)
                  (:lease-expiry-ms first-delivery))
              "The peer does not reap before the lease expires")
            (is (< (- (:server-time-ms second-delivery)
                     (:server-time-ms first-delivery))
                  5000)
              "The retry is deadline-driven rather than idle-max-driven")])])
      (deliver release-first true)
      (is (wait-for 5000 #(nil? (mq/msg-status q "lease-baton"))))
      (is (wait-for 5000
            #(and (zero? (signal-count q))
               (>= (get-in (mq/worker-stats worker-a) [:counts :wake-waits]) 2)
               (>= (get-in (mq/worker-stats worker-b) [:counts :wake-waits]) 2)))
        "The coalesced baton is consumed and both workers return to blocking waits")
      (let [before (+ (get-in (mq/worker-stats worker-a) [:counts :wake-waits])
                     (get-in (mq/worker-stats worker-b) [:counts :wake-waits]))]
        (Thread/sleep 150)
        (is (<= (+ (get-in (mq/worker-stats worker-a) [:counts :wake-waits])
                    (get-in (mq/worker-stats worker-b) [:counts :wake-waits]))
                 (+ before 2))
          "At most one in-flight blocking transition per worker remains; there is no wake storm"))
      (finally
        (deliver release-first true)
        (.close ^java.io.Closeable worker-a)
        (.close ^java.io.Closeable worker-b)
        (mq/queue-clear!! q)))))

(deftest _worker-wake-stall-safety
  (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
        handled (promise)
        waiting (promise)
        worker (mq/worker-create q (fn [{:keys [msg]}]
                              (deliver handled msg)
                              (mq/outcome:ack))
                 {:idle-min-ms 100, :idle-max-ms 100})]
    (try
      ;; Simulate a completely missing advisory signal. The finite local poll
      ;; remains the correctness path and must still discover new work.
      (with-redefs [mq/await-wake!
                    (fn [_queue state_ sleeping-threads_
                         _wake-lock _wake-conn_ _wake-blocker_ _stats_ timeout-ms]
                      (deliver waiting true)
                      (#'mq/idle-sleep! state_ sleeping-threads_ timeout-ms))]
        (is (mq/worker-start! worker))
        (is (= (deref waiting 5000 :timeout) true))
        (mq/msg-enqueue! q :fallback {:mid "fallback"})
        (is (= (deref handled 5000 :timeout) :fallback)))
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _worker-wake-burst-fanout
  (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
        entered_ (atom #{})
        all-entered (promise)
        release (promise)
        worker
        (mq/worker-create q
          (fn [{:keys [mid]}]
            (when (= (count (swap! entered_ conj mid)) 4) (deliver all-entered true))
            @release
            (mq/outcome:ack))
          {:concurrency 4, :idle-min-ms 10, :idle-max-ms 60000})]
    (try
      (is (mq/worker-start! worker))
      (is (wait-for 5000
            #(pos? (long (get-in @worker [:stats :counts :wake-waits] 0)))))
      (doseq [idx (range 4)]
        (mq/msg-enqueue! q idx {:mid (str idx)}))
      [(is (= (deref all-entered 5000 :timeout) true)
         "Wake fanout beats every runner's long fallback poll")
       (is (= @entered_ #{"0" "1" "2" "3"}))]
      (deliver release true)
      (is (wait-for 5000
            #(= (get-in (mq/worker-stats worker)
                  [:counts :settlements :acked])
               4)))
      (is (wait-for 5000 #(zero? (get-in (mq/queue-status q) [:counts :active]))))
      (let [counts (:counts (mq/worker-stats worker))]
        [(is (= (:claims counts) 4))
         (is (= (:handler-calls counts) 4))
         (is (= (get-in counts [:settlements :acked]) 4))])
      (finally
        (deliver release true)
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _worker-releases-unhandled-claim-on-stop
  (let [q (mq/queue mgr_ (qname)
            {:lease-ms 1000, :max-attempts 1
             :retry-base-ms 0, :retry-max-ms 0})
        _ (mq/msg-enqueue! q :work {:mid "stop-window"})
        real-claim (#'mq/claim! q 64)
        entered (promise)
        continue (promise)
        worker (mq/worker-create q (fn [_] (throw (AssertionError. "handler must not run")))
                 {:idle-max-ms 20})]
    (try
      (with-redefs [mq/claim!
                    (fn [& _]
                      (deliver entered true)
                      @continue
                      real-claim)]
        (is (mq/worker-start! worker))
        (is (= (deref entered 2000 :timeout) true))
        (is (mq/worker-stop! worker))
        (deliver continue true)
        (is (mq/worker-await-stopped! worker 2000)))
      [(is (= (mq/msg-status q "stop-window") :ready))
       (is (truss/submap? (:counts (mq/worker-stats worker))
             {:claims 1, :handler-calls 0}))
       (is (= (get-in (mq/worker-stats worker) [:counts :releases :released]) 1))
       (is (= (get-in (mq/worker-stats worker) [:timings :claim-age :count]) 1))
       (is (zero? (get-in (mq/worker-stats worker)
                    [:timings :first-claim-age :count]))
         "A released (unhandled) claim records no first-claim age: the
          refunded attempt will be delivered as attempt 1 again")]
      (let [claim (#'mq/claim! q 64)]
        (is (= (:attempt claim) 1))
        (#'mq/settle! q "stop-window" (:token claim) (mq/outcome:ack)))
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _handler-can-stop
  (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
        worker_ (atom nil)
        stopped (promise)
        worker (mq/worker-create q
                 (fn [_]
                   (deliver stopped (mq/worker-stop! @worker_))
                   (mq/outcome:ack))
                 {:idle-max-ms 20})]
    (reset! worker_ worker)
    (try
      (mq/msg-enqueue! q :work {:mid "self-stop"})
      (mq/worker-start! worker)
      [(is (= (deref stopped 2000 :timeout) true))
       (is (mq/worker-await-stopped! worker 2000))
       (is (nil? (mq/msg-status q "self-stop")))]
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _worker-executor-events-and-stats
  (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
        entered (promise)
        continue (promise)
        events_ (atom [])
        worker (mq/worker-create q
                 (fn [_]
                   (deliver entered true)
                   @continue
                   (mq/outcome:ack))
                 {:concurrency 2, :idle-max-ms 20
                  :on-event #(swap! events_ conj %)})]
    (try
      (is (mq/worker-start! worker))
      (is (= (:action (mq/msg-enqueue! q :work {:mid "diagnostics"})) :added))
      (is (= (deref entered 2000 :timeout) true))
      (let [{:keys [worker-id threads]} @worker]
        [(is (string? worker-id))
         (is (= (:busy threads) 1))
         (is (= (:concurrency threads) 2))
         (is (seq (:names threads)))
         (is (every? #(re-find #"^carmine-mq-v4-carmine-v4-mq-test-.*-[0-9a-f]{8}-\d+$" %)
               (:names threads)))])
      (deliver continue true)
      (is (wait-for 2000 #(nil? (mq/msg-status q "diagnostics"))))
      (is (mq/worker-stop! worker))
      (is (mq/worker-await-stopped! worker 2000))
      (let [events @events_
            settled (some #(when (= (:event %) :settled) %) events)
            stats (mq/worker-stats worker)]
        [(is (= (mapv :event events)
               [:worker-started :settled :worker-stopping :worker-stopped]))
         (is (= (:worker-id settled) (:worker-id @worker)))
         (is (truss/submap? settled
               {:mid "diagnostics", :attempt 1, :action :acked
                :msg :submap/nx}))
         (is (= (:intent settled) :ack))
         (is (pos-int? (:client-time-ms settled)))
         (is (pos-int? (:server-time-ms settled)))
         (is (pos-int? (:enqueued-at-ms settled)))
         (is (nat-int? (:age-ms settled)))
         (is (= (:priority settled) :normal))
         (is (number? (:handler-ms settled)))
         (is (truss/submap? (:counts stats)
               {:claims 1, :handler-calls 1}))
         (is (= (get-in stats [:counts :handler-intents :ack]) 1))
         (is (= (get-in stats [:counts :settlements :acked]) 1))
         (is (pos? (get-in stats [:timings :claim-round-trip :count])))
         (is (= (get-in stats [:timings :settlement-round-trip :count]) 1))
         (is (every? number?
               (map #(get-in stats [:timings :settlement-round-trip %])
                 [:sum-ms :min-ms :max-ms :last-ms])))
         (is (= (get-in stats [:timings :handler :count]) 1))
         (is (pos? (get-in stats [:timings :handler :sum-ms])))
         (let [{:keys [sum-ms min-ms max-ms last-ms]}
               (get-in stats [:timings :handler])]
           (is (= sum-ms min-ms max-ms last-ms)))
         (is (= (get-in stats [:timings :claim-age :count]) 1))
         (is (= (get-in stats [:timings :first-claim-age :count]) 1))
         (let [prior (mq/worker-clear-stats! worker)
               current (mq/worker-stats worker)]
           [(is (= (dissoc prior :snapshot-client-time-ms)
                  (dissoc stats :snapshot-client-time-ms)))
            (is (= (:snapshot-client-time-ms prior)
                  (:since-client-time-ms current)))
            (is (= (get-in current [:counts :claims]) 0))])])
      (finally
        (deliver continue true)
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _worker-start-stop-notification-handshake
  (let [started-notify-entered (promise)
        started-notify-continue (promise)
        events_ (atom [])
        executor-creations_ (atom 0)
        real-notify-worker! (var-get #'mq/notify-worker!)
        worker
        (mq/worker-create (inert-queue) (constantly (mq/outcome:ack))
          {:concurrency 3
           :on-event #(swap! events_ conj (:event %))})]
    (try
      (with-redefs [mq/notify-worker!
                    (fn [queue worker-id opts stats_ event data]
                      (when (= event :worker-started)
                        (deliver started-notify-entered true)
                        @started-notify-continue)
                      (real-notify-worker!
                        queue worker-id opts stats_ event data))
                    mq/new-worker-executor
                    (fn [& _]
                      (swap! executor-creations_ inc)
                      (throw (AssertionError.
                               "A stopped startup must not create an executor")))]
        (let [start-result (future (mq/worker-start! worker))]
          (try
            [(is (= (deref started-notify-entered 2000 :timeout) true)
               "Start is paused before its event is recorded or invoked")
             (is (mq/worker-stop! worker)
               "External stop records a deferred stop-phase owner")
             (is (= (:state @worker) :stopping))
             (is (empty? @events_)
               "Stopping cannot publish before the pending started event")
             (is (= (deref start-result 50 :pending) :pending))]
            (deliver started-notify-continue true)
            [(is (= (deref start-result 2000 :timeout) true))
             (is (mq/worker-await-stopped! worker 2000))
             (is (= (:state @worker) :stopped))
             (is (zero? @executor-creations_))
             (is (= @events_
                   [:worker-started :worker-stopping :worker-stopped]))]
            (finally
              (deliver started-notify-continue true)
              (future-cancel start-result)))))
      (finally
        (deliver started-notify-continue true)
        (.close ^java.io.Closeable worker)))))

(deftest _worker-event-callback-await-context
  (let [events_ (atom [])
        executor-creations_ (atom 0)
        self-await (promise)
        async-await-future (promise)
        async-await-go (promise)
        worker_ (atom nil)
        worker
        (mq/worker-create (inert-queue) (constantly (mq/outcome:ack))
          {:concurrency 2
           :on-event
           (fn [{:keys [event]}]
             (swap! events_ conj event)
             (when (= event :worker-started)
               (deliver self-await
                 (mq/worker-await-stopped! @worker_ nil))
               ;; Clojure futures convey dynamic bindings. The callback marker
               ;; must be a non-inheritable ThreadLocal so this later task is
               ;; not misclassified as a synchronous self-await.
               (deliver async-await-future
                 (future
                   @async-await-go
                   (mq/worker-await-stopped! @worker_ 2000)))))})]
    (reset! worker_ worker)
    (try
      (with-redefs [mq/new-worker-executor
                    (fn [& _]
                      (swap! executor-creations_ inc)
                      (throw (AssertionError.
                               "A callback-stopped startup must not create an executor")))]
        (is (mq/worker-start! worker)))
      (let [async-result (deref async-await-future 2000 :timeout)]
        (try
          [(is (false? (deref self-await 2000 :timeout))
             "A synchronous started callback cannot await its own handoff")
           (is (zero? @executor-creations_))
           (is (= (:state @worker) :stopped))]
          (deliver async-await-go true)
          [(is (true? (deref async-result 2000 :timeout))
             "Callback context is not inherited by asynchronous work")
           (is (= @events_
                 [:worker-started :worker-stopping :worker-stopped]))]
          (finally
            (deliver async-await-go true)
            (when (future? async-result) (future-cancel async-result)))))
      (finally
        (deliver async-await-go true)
        (.close ^java.io.Closeable worker)))))

(deftest _worker-stop-during-successful-executor-creation
  (let [pool-create-entered (promise)
        pool-create-continue (promise)
        uncaught (promise)
        submissions_ (atom 0)
        shutdown?_ (atom false)
        executor (uncaught-capturing-executor uncaught submissions_ shutdown?_)
        events_ (atom [])
        worker
        (mq/worker-create (inert-queue) (constantly (mq/outcome:ack))
          {:concurrency 3
           :on-event #(swap! events_ conj (:event %))})]
    (try
      (with-redefs [mq/new-worker-executor
                    (fn [& _]
                      (deliver pool-create-entered true)
                      @pool-create-continue
                      executor)]
        (let [start-result (future (mq/worker-start! worker))]
          (try
            [(is (= (deref pool-create-entered 2000 :timeout) true))
             (is (mq/worker-stop! worker))
             (is (= (:state @worker) :stopping))
             (is (= @events_ [:worker-started :worker-stopping]))]
            (deliver pool-create-continue true)
            [(is (= (deref start-result 2000 :timeout) true))
             (is @shutdown?_)
             (is (zero? @submissions_)
               "A stop won before pool publication, so no slot is submitted")
             (is (mq/worker-await-stopped! worker 2000))
             (is (= (:state @worker) :stopped))
             (is (= @events_
                   [:worker-started :worker-stopping :worker-stopped]))]
            (finally
              (deliver pool-create-continue true)
              (future-cancel start-result)))))
      (finally
        (deliver pool-create-continue true)
        (.close ^java.io.Closeable worker)))))

(deftest _worker-stop-during-failed-executor-creation
  (let [pool-create-entered (promise)
        pool-create-continue (promise)
        pool-failure (java.util.concurrent.RejectedExecutionException.
                       "injected pool creation failure")
        events_ (atom [])
        worker
        (mq/worker-create (inert-queue) (constantly (mq/outcome:ack))
          {:concurrency 3
           :on-event #(swap! events_ conj (:event %))})]
    (try
      (with-redefs [mq/new-worker-executor
                    (fn [& _]
                      (deliver pool-create-entered true)
                      @pool-create-continue
                      (throw pool-failure))]
        (let [start-result
              (future
                (try
                  {:value (mq/worker-start! worker)}
                  (catch Throwable t {:error t})))]
          (try
            [(is (= (deref pool-create-entered 2000 :timeout) true))
             (is (mq/worker-stop! worker))
             (is (= (:state @worker) :stopping))
             (is (= @events_ [:worker-started :worker-stopping]))]
            (deliver pool-create-continue true)
            (let [{:keys [error] :as result}
                  (deref start-result 2000 {:timeout true})]
              [(is (not (:timeout result)))
               (is (identical? error pool-failure))])
            [(is (mq/worker-await-stopped! worker 2000))
             (is (= (:state @worker) :stopped))
             (is (= @events_
                   [:worker-started :worker-stopping
                    :worker-error :worker-stopped]))
             (is (= (get-in (mq/worker-stats worker)
                      [:counts :worker-errors]) 1))
             (is (= (get-in (mq/worker-stats worker)
                      [:counts :worker-failures]) 0))]
            (finally
              (deliver pool-create-continue true)
              (future-cancel start-result)))))
      (finally
        (deliver pool-create-continue true)
        (.close ^java.io.Closeable worker)))))

(deftest _worker-stop-notification-handshake
  (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
        handler-entered (promise)
        handler-continue (promise)
        close-entered (promise)
        close-continue (promise)
        stopping-entered (promise)
        stopping-continue (promise)
        reentrant-await (promise)
        events_ (atom [])
        worker_ (atom nil)
        worker
        (mq/worker-create q
          (fn [_]
            (deliver handler-entered true)
            @handler-continue
            (mq/outcome:ack))
          {:idle-max-ms 20
           :on-event
           (fn [{:keys [event]}]
             (swap! events_ conj event)
             (case event
               :worker-stopping
               (do
                 (deliver reentrant-await
                   (mq/worker-await-stopped! @worker_ nil))
                 (deliver stopping-entered true)
                 @stopping-continue)

               nil))})]
    (reset! worker_ worker)
    (try
      [(is (mq/worker-start! worker))
       (is (= (:action (mq/msg-enqueue! q :work {:mid "stop-handshake"}))
             :added))
       (is (= (deref handler-entered 2000 :timeout) true))]
      (with-redefs [mq/close-wake-conn!
                    (fn [& _]
                      (deliver close-entered true)
                      @close-continue)]
        (let [stop-result
              (future
                (try
                  {:value (mq/worker-stop! worker)}
                  (catch Throwable t {:error t})))]
          (try
            (is (= (deref close-entered 2000 :timeout) true)
              "The stop winner is paused after its lifecycle CAS")
            (deliver handler-continue true)
            [(is (wait-for 2000
                   #(empty? (get-in @worker [:threads :names])))
               "The sole runner retires inside the CAS-to-notification window")
             (is (= (:state @worker) :stopping))
             (is (= @events_ [:worker-started :settled])
               "A retired last runner cannot publish stopped before stopping")]

            (deliver close-continue true)
            [(is (= (deref stopping-entered 2000 :timeout) true))
             (is (false? (deref reentrant-await 2000 :timeout))
               "A synchronous stopping callback cannot await itself")
             (is (= (:state @worker) :stopping))
             (is (= @events_
                   [:worker-started :settled :worker-stopping])
               "Stopped remains deferred while the stopping callback is active")]

            (deliver stopping-continue true)
            (let [{:keys [value] :as result}
                  (deref stop-result 2000 {:timeout true})]
              [(is (not (:timeout result)))
               (is (true? value))])
            [(is (mq/worker-await-stopped! worker 2000))
             (is (= (:state @worker) :stopped))
             (is (= @events_
                   [:worker-started :settled
                    :worker-stopping :worker-stopped]))
             (is (= (count (filter #{:worker-stopped} @events_)) 1)
               "Stopped is published exactly once")
             (is (false? (mq/worker-stop! worker)))]
            (finally
              (deliver handler-continue true)
              (deliver close-continue true)
              (deliver stopping-continue true)
              (future-cancel stop-result)))))
      (finally
        (deliver handler-continue true)
        (deliver close-continue true)
        (deliver stopping-continue true)
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest ^{:mq-soak true, :v4-canary true}
  _worker-concurrent-stop-event-order-stress
  (with-redefs [mq/claim!
                (fn [& _]
                  {:action :idle, :next-at-ms nil
                   :server-time-ms (System/currentTimeMillis)})
                mq/await-wake! (fn [& _] (Thread/yield))
                mq/close-wake-conn! (fn [& _] nil)]
    (let [reports
          (mapv
            (fn [iteration]
              (let [events_ (atom [])
                    worker
                    (mq/worker-create (inert-queue)
                      (constantly (mq/outcome:ack))
                      {:concurrency 2, :idle-min-ms 1, :idle-max-ms 1
                       :on-event #(swap! events_ conj (:event %))})]
                (try
                  (let [started? (mq/worker-start! worker)
                        gate (promise)
                        stop-a (future @gate (mq/worker-stop! worker))
                        stop-b (future @gate (mq/worker-stop! worker))]
                    (try
                      (deliver gate true)
                      (let [stop-results
                            [(deref stop-a 2000 :timeout)
                             (deref stop-b 2000 :timeout)]
                            awaited? (mq/worker-await-stopped! worker 2000)]
                        {:iteration iteration, :started? started?
                         :stop-results stop-results, :awaited? awaited?
                         :state (:state @worker), :events @events_})
                      (finally
                        (deliver gate true)
                        (future-cancel stop-a)
                        (future-cancel stop-b))))
                  (finally (.close ^java.io.Closeable worker)))))
            (range 100))
          unexpected
          (fn [pred] (filterv (complement pred) reports))]
      [(is (empty? (unexpected :started?)))
       (is (empty?
             (unexpected
               #(= (frequencies (:stop-results %)) {true 1, false 1})))
         (pr-str (unexpected
                   #(= (frequencies (:stop-results %)) {true 1, false 1}))))
       (is (empty? (unexpected :awaited?)))
       (is (empty? (unexpected #(= (:state %) :stopped))))
       (is (empty?
             (unexpected
               #(= (:events %)
                  [:worker-started :worker-stopping :worker-stopped])))
         (pr-str (unexpected
                   #(= (:events %)
                      [:worker-started :worker-stopping :worker-stopped]))))])))

(deftest _worker-event-msg-opt-in
  (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
        settled (promise)
        worker (mq/worker-create q (constantly (mq/outcome:ack))
                 {:idle-max-ms 20, :include-msg? true
                  :on-event #(when (= (:event %) :settled) (deliver settled %))})]
    (try
      (mq/worker-start! worker)
      (mq/msg-enqueue! q {:private :payload} {:mid "included"})
      (is (= (:msg (deref settled 2000 nil)) {:private :payload}))
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _worker-event-callback-errors-are-isolated
  (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
        throw?_ (atom true)
        logs_ (atom [])
        worker
        (mq/worker-create q (constantly (mq/outcome:ack))
          {:idle-max-ms 20
           :on-event
           (fn [{:keys [event]}]
             (when (and (= event :settled) (compare-and-set! throw?_ true false))
               (throw (Exception. "Expected callback failure"))))})]
    (try
      (with-redefs [trove/*log-fn*
                    (fn [_ns _coords level id data_]
                      (swap! logs_ conj
                        {:level level, :id id, :data (force data_)}))]
        (mq/worker-start! worker)
        (mq/msg-enqueue! q :work {:mid "callback-error"})
        (is (wait-for 2000
              #(and
                 (= (get-in (mq/worker-stats worker)
                      [:counts :event-callback-errors]) 1)
                 (some (fn [log]
                         (= (:id log) :carmine.mq/event-callback-error))
                   @logs_))))
        (let [stats (mq/worker-stats worker)
              {:keys [level data] :as log}
              (some #(when (= (:id %) :carmine.mq/event-callback-error) %)
                @logs_)
              log-data (:data data)]
          [(is (nil? (mq/msg-status q "callback-error")))
           (is (= (get-in stats [:counts :settlements :acked]) 1))
           (is (= (get-in stats [:counts :event-callback-errors]) 1))
           (is (= (:state @worker) :running))
           (is (some? log))
           (is (= level :error))
           (is (string? (:qname log-data)))
           (is (string? (:worker-id log-data)))
           (is (= (select-keys log-data [:event :runner :mid :attempt])
                 {:event :settled, :runner 0
                  :mid "callback-error", :attempt 1}))]))
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _fatal-worker-event-callbacks-propagate-after-cleanup
  (testing "A fatal started callback cannot strand a running worker"
    (let [fatal (LinkageError. "fatal worker-started callback")
          worker (mq/worker-create (inert-queue) (constantly (mq/outcome:ack))
                   {:on-event #(when (= (:event %) :worker-started)
                                 (throw fatal))})]
      (try
        [(is (identical? fatal (truss/throws (mq/worker-start! worker))))
         (is (= (:state @worker) :failed))
         (is (mq/worker-await-stopped! worker 100))
         (is (= (get-in (mq/worker-stats worker)
                  [:counts :worker-failures]) 1))]
        (finally (.close ^java.io.Closeable worker)))))

  (testing "A fatal stopping callback runs wake cleanup first"
    (let [fatal (LinkageError. "fatal worker-stopping callback")
          closed_ (atom 0)
          worker (mq/worker-create (inert-queue) (constantly (mq/outcome:ack))
                   {:idle-min-ms 1, :idle-max-ms 1
                    :on-event #(when (= (:event %) :worker-stopping)
                                 (throw fatal))})]
      (try
        (with-redefs [mq/claim!
                      (fn [& _]
                        {:action :idle, :next-at-ms nil
                         :server-time-ms (System/currentTimeMillis)})
                      mq/await-wake! (fn [& _] (Thread/yield))
                      mq/close-wake-conn! (fn [& _] (swap! closed_ inc))]
          (is (mq/worker-start! worker))
          (is (wait-for 1000 #(seq (get-in @worker [:threads :names]))))
          [(is (identical? fatal (truss/throws (mq/worker-stop! worker))))
           (is (= @closed_ 1))
           (is (mq/worker-await-stopped! worker 2000))
           (is (= (:state @worker) :stopped))])
        (finally (.close ^java.io.Closeable worker))))))

(deftest _worker-terminal-failure-is-counted
  (let [q (mq/queue mgr_ (qname))
        worker (mq/worker-create q (constantly (mq/outcome:ack)))]
    (try
      (with-redefs [mq/new-worker-executor
                    (fn [& _] (throw (Exception. "Expected executor failure")))]
        (is (thrown? Exception (mq/worker-start! worker))))
      [(is (= (:state @worker) :failed))
       (is (= (get-in (mq/worker-stats worker) [:counts :worker-failures]) 1))
       (is (= (get-in (mq/worker-stats worker) [:counts :worker-errors]) 0))]
      (finally
        (.close ^java.io.Closeable worker)
        (mq/queue-clear!! q)))))

(deftest _worker-failure-waits-for-all-runners-to-quiesce
  (let [next-claim* (atom 0)
        blocked-entered (promise)
        fatal-entered   (promise)
        release-blocked (promise)
        now (System/currentTimeMillis)
        claim
        (fn [mid]
          {:action :handle, :mid mid, :msg mid, :attempt 1
           :token (str "token-" mid), :lease-expiry-ms (+ now 1000)
           :enqueued-at-ms now, :priority :normal, :server-time-ms now})
        worker
        (mq/worker-create (inert-queue)
          (fn [{:keys [mid]}]
            (case mid
              "blocked"
              (do
                (deliver blocked-entered true)
                @release-blocked
                (mq/outcome:ack))

              "fatal"
              (do
                (deliver fatal-entered true)
                @blocked-entered
                (throw (LinkageError. "Expected fatal handler failure")))))
          {:concurrency 2, :idle-min-ms 1, :idle-max-ms 1})]
    (try
      (with-redefs [mq/claim!
                    (fn [& _]
                      (case (long (swap! next-claim* inc))
                        1 (claim "blocked")
                        2 (claim "fatal")
                        {:action :idle, :next-at-ms nil, :server-time-ms now}))
                    mq/settle! (fn [& _] {:action :acked})]
        (is (mq/worker-start! worker))
        (is (= (deref fatal-entered 2000 :timeout) true))
        (is (wait-for 2000 #(= (:state @worker) :failed)))
        (is (false? (mq/worker-await-stopped! worker 50))
          "A failed state alone is not quiescence while another handler runs")
        (deliver release-blocked true)
        (is (mq/worker-await-stopped! worker 2000))
        (is (empty? (get-in @worker [:threads :names]))))
      (finally
        (deliver release-blocked true)
        (.close ^java.io.Closeable worker)))))

(deftest _worker-partial-start-reaches-terminal-state
  (letfn [(rejecting-executor [shutdown?_ failure]
            (proxy [java.util.concurrent.AbstractExecutorService] []
              (execute [_] (throw failure))
              (shutdown [] (reset! shutdown?_ true))
              (shutdownNow [] (reset! shutdown?_ true) [])
              (isShutdown [] @shutdown?_)
              (isTerminated [] @shutdown?_)
              (awaitTermination [_ _] @shutdown?_)))]
    (let [q (mq/queue mgr_ (qname) {:retry-base-ms 0, :retry-max-ms 0})
          shutdown?_ (atom false)
          failure (java.util.concurrent.RejectedExecutionException. "injected")
          executor (rejecting-executor shutdown?_ failure)
          worker (mq/worker-create q (constantly (mq/outcome:ack)) {:concurrency 3})]
      (try
        (with-redefs [mq/new-worker-executor (fn [_ _ _] executor)]
          (is (identical? failure (truss/throws (mq/worker-start! worker)))))
        [(is @shutdown?_)
         (is (mq/worker-await-stopped! worker 100))
         (is (= (:state @worker) :stopped))
         (is (= (get-in (mq/worker-stats worker) [:counts :worker-errors]) 1))
         (is (= (get-in (mq/worker-stats worker) [:counts :worker-failures]) 0))]
        (finally
          (.close ^java.io.Closeable worker)
          (mq/queue-clear!! q))))))
