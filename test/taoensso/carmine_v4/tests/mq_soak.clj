(ns taoensso.carmine-v4.tests.mq-soak
  "Reusable mixed coalescing workload for standalone and topology failover tests."
  (:require
   [taoensso.carmine-v4.message-queue :as mq]))

(defn- wait-for! [timeout-ms description pred]
  (let [deadline (+ (System/nanoTime) (* (long timeout-ms) 1000000))]
    (loop []
      (if-let [result (pred)]
        result
        (if (< (System/nanoTime) deadline)
          (do (Thread/sleep 10) (recur))
          (throw (ex-info (str "Timed out waiting for " description)
                   {:timeout-ms timeout-ms})))))))

(defn- remaining-ms [deadline]
  (max 0 (- deadline (System/currentTimeMillis))))

(defrecord Soak
  [queue worker mid-count run-id running_ producers revisions_ handled_
   enqueue-locks max-successors_ producer-error-count_ finished_])

(defn- accepted-action? [action]
  (contains? #{:added :coalesced :coalesced-successor :existing} action))

(defn- proof [run-id mid revision] (str run-id ":" mid ":" revision))

(defn- sample-successors! [queue max-successors_]
  (let [n (get-in (mq/queue-status queue) [:counts :successors])]
    (swap! max-successors_ max n)))

(defn start!
  "Starts a bounded mixed coalescing workload. Call `finish!`, then `close!`.

  Producer errors are expected during injected failover and are retried by
  subsequent updates. Revision allocation and enqueue are serialized per MID,
  matching revision ordering's active-lifecycle scope while retaining
  concurrency across MIDs. Correctness is judged from attempted revisions and
  the final accepted revision for every fixed MID. Memory use is bounded by MID
  count."
  ([manager prefix] (start! manager prefix nil))
  ([manager prefix opts]
   (let [opts (merge {:mid-count 8, :producer-count 2, :concurrency 4} opts)
         mid-count (long (:mid-count opts))
         queue (mq/queue manager
                 (or (:queue-name opts)
                   (str "carmine-v4-mq-soak-" prefix "-" (java.util.UUID/randomUUID)))
                 {:lease-ms 10000, :retry-base-ms 0, :retry-max-ms 0
                  :on-duplicate :coalesce, :revision-mode :required})
         running_ (atom true)
         run-id (str (java.util.UUID/randomUUID))
         revisions_ (atom (zipmap (range mid-count) (repeat 0)))
         enqueue-locks (mapv (fn [_] (Object.)) (range mid-count))
         enqueue-next!
         (fn [mid extra-msg]
           (locking (nth enqueue-locks mid)
             (let [revision (get (swap! revisions_ update mid inc) mid)
                   msg (merge
                         {:logical-mid mid, :revision revision
                          :proof (proof run-id mid revision)}
                         extra-msg)]
               [revision
                (mq/msg-enqueue! queue msg
                  {:mid (str mid), :revision revision})])))
         handled_ (atom {})
         max-successors_ (atom 0)
         producer-error-count_ (atom 0)
         forced-claim_ (promise)
         release-forced-claim_ (promise)
         worker
         (mq/worker-create queue
           (fn [{:keys [msg]}]
             (when (:hold-for-successor? msg)
               (deliver forced-claim_ true)
               @release-forced-claim_)
             (let [{:keys [logical-mid revision] msg-proof :proof} msg
                   valid? (and (integer? logical-mid) (integer? revision)
                            (= msg-proof (proof run-id logical-mid revision))
                            (<= 1 (long revision)
                              (long (get @revisions_ logical-mid 0))))]
               (swap! handled_ update logical-mid
                 (fn [{:keys [last max count ordered? no-phantoms?]}]
                   {:last revision, :max (clojure.core/max (long (or max 0)) (long revision))
                    :count (inc (long (or count 0)))
                    :ordered? (and (not= ordered? false)
                                (or (nil? last) (<= (long last) (long revision))))
                    :no-phantoms? (and (not= no-phantoms? false) valid?)})))
             (mq/outcome:ack))
           {:concurrency (:concurrency opts), :idle-max-ms 250})
         _ (mq/worker-start! worker)
         ;; Force a lease to remain in flight while several newer revisions
         ;; coalesce. The free-running soak can otherwise complete handlers so
         ;; quickly that its successor bound passes vacuously at zero.
         _
         (try
           (let [[revision result]
                 (enqueue-next! 0 {:hold-for-successor? true})]
             (when-not (= (:action result) :added)
               (throw (ex-info "MQ soak could not seed forced successor contention"
                        {:result result})))
             (wait-for! 5000 "the forced successor seed to be claimed"
               #(deref forced-claim_ 10 nil))
             (try
               (dotimes [_ 4]
                 (let [[revision result] (enqueue-next! 0 nil)]
                   (when-not (= (:action result) :coalesced-successor)
                     (throw
                       (ex-info "MQ soak did not exercise successor coalescing"
                         {:revision revision, :result result})))))
               (sample-successors! queue max-successors_)
               (when-not (pos? @max-successors_)
                 (throw (ex-info "MQ soak successor count remained zero" {})))
               (finally (deliver release-forced-claim_ true))))
           (catch Throwable t
             (deliver release-forced-claim_ true)
             (.close ^java.io.Closeable worker)
             (throw t)))
         producers
         (mapv
           (fn [producer]
             (future
               (loop [idx producer]
                 (when @running_
                   (let [mid (mod idx mid-count)]
                     (try
                       (let [[_revision result]
                             ;; `enqueue-next!` locks allocation plus the whole
                             ;; enqueue round trip for this MID. The local
                             ;; binding documents that producers intentionally
                             ;; retain parallelism only across distinct MIDs.
                             (enqueue-next! mid nil)]
                         (when (zero? (mod idx 64))
                           (sample-successors! queue max-successors_)))
                     (catch Throwable t
                         (swap! producer-error-count_ inc)
                         (Thread/sleep 10)))
                     (Thread/sleep 1)
                     ;; Every producer cycles through every MID. Per-MID locks
                     ;; therefore stay exercised instead of passing vacuously
                     ;; when MID count is divisible by producer count.
                     (recur (inc idx)))))))
           (range (:producer-count opts)))]
     (->Soak queue worker mid-count run-id running_ producers revisions_ handled_
       enqueue-locks max-successors_ producer-error-count_ (atom false)))))

(defn- enqueue-final! [soak mid timeout-ms]
  (locking (nth (:enqueue-locks soak) mid)
    (let [revision (get (swap! (:revisions_ soak) update mid inc) mid)]
      ;; Every retry reuses the exact final revision and payload. If an earlier
      ;; response was lost after Redis committed, its idempotent :existing
      ;; replay is therefore completion rather than a reason to time out.
      (wait-for! timeout-ms "a final coalescing update after failover"
        (fn []
          (try
            (let [result (mq/msg-enqueue! (:queue soak)
                           {:logical-mid mid, :revision revision, :final? true
                            :proof (proof (:run-id soak) mid revision)}
                           {:mid (str mid), :revision revision})]
              (when (accepted-action? (:action result))
                revision))
            (catch Throwable _ nil)))))))

(defn finish!
  "Stops producers, sends one final revision per MID, drains, and returns an
  invariant report. Duplicate deliveries are allowed; revision regression and
  phantom delivery are not."
  ([soak] (finish! soak 30000))
  ([soak timeout-ms]
   (when (compare-and-set! (:finished_ soak) false true)
     (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
       (reset! (:running_ soak) false)
       (doseq [producer (:producers soak)]
         (when (= (deref producer (remaining-ms deadline) ::timeout) ::timeout)
           (future-cancel producer)
           (throw (ex-info "MQ soak producer did not stop before finalization"
                    {:timeout-ms timeout-ms}))))
       (let [finals (into {}
                      (map (fn [mid]
                             [mid (enqueue-final! soak mid (remaining-ms deadline))]))
                    (range (:mid-count soak)))]
         (wait-for! (remaining-ms deadline) "all final coalesced revisions to settle"
           (fn []
             (let [handled @(:handled_ soak)]
               (when (and
                       (every? (fn [[mid revision]]
                                 (= (get-in handled [mid :max]) revision))
                         finals)
                       (zero? (get-in (mq/queue-status (:queue soak)) [:counts :active])))
                 true))))
         (sample-successors! (:queue soak) (:max-successors_ soak))
         (let [handled @(:handled_ soak)
               status (mq/queue-status (:queue soak))
               ordered? (every? :ordered? (vals handled))
               no-phantoms? (every? :no-phantoms? (vals handled))]
           {:finals finals
            :handled-max (into {} (map (fn [[mid summary]] [mid (:max summary)])) handled)
            :ordered? ordered?, :no-phantoms? no-phantoms?
            :drained? (and (zero? (get-in status [:counts :active]))
                        (zero? (get-in status [:counts :successors]))
                        (zero? (get-in status [:counts :leased])))
            :successors-bounded? (<= @(:max-successors_ soak) (:mid-count soak))
            :successors-exercised? (pos? @(:max-successors_ soak))
            :max-successors @(:max-successors_ soak)
            :producer-errors @(:producer-error-count_ soak)
            :worker-stats (mq/worker-stats (:worker soak))}))))))

(defn close! [soak]
  (reset! (:running_ soak) false)
  (let [deadline (+ (System/currentTimeMillis) 1000)]
    (doseq [producer (:producers soak)]
      (when (= (deref producer (remaining-ms deadline) ::timeout) ::timeout)
        (future-cancel producer))))
  (.close ^java.io.Closeable (:worker soak))
  (mq/queue-clear!! (:queue soak))
  true)
