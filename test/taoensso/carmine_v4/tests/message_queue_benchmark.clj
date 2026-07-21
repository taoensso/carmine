(ns taoensso.carmine-v4.tests.message-queue-benchmark
  "Opt-in Carmine v4 MQ load/soak benchmarks (`lein bench-v4-mq`).

  All sizes are environment-configurable. Defaults exercise useful populations
  without making this an endurance suite:

  CARMINE_MQ_BENCH_MESSAGES=10000
  CARMINE_MQ_BENCH_CONCURRENCY=8
  CARMINE_MQ_BENCH_SOAK_SECONDS=10
  CARMINE_MQ_BENCH_SOAK_PRODUCERS=4
  CARMINE_MQ_BENCH_MAX_BACKLOG=5000
  CARMINE_MQ_BENCH_MAINTENANCE_MESSAGES=1000
  CARMINE_MQ_BENCH_MAINTENANCE_BATCH_SIZES=1,16,64,256,1000
  CARMINE_MQ_BENCH_DEAD_MESSAGES=2000
  CARMINE_MQ_BENCH_PURGE_LIMIT=500
  CARMINE_MQ_BENCH_WAKE_MESSAGES=25
  CARMINE_MQ_BENCH_COALESCE_UPDATES=10000
  CARMINE_MQ_BENCH_TIMEOUT_SECONDS=300"
  (:require
   [clojure.string                    :as str]
   [clojure.test                      :refer [deftest is]]
   [taoensso.carmine-v4               :as car]
   [taoensso.carmine-v4.conns         :as conns]
   [taoensso.carmine-v4.message-queue :as mq]))

(defn- queue-keys [queue] (#'mq/queue-keys queue))

(defonce mgr_
  (delay
    (conns/conn-manager-pooled
      {:conn-opts
       {:server ["127.0.0.1"
                 (Long/parseLong (or (System/getenv "CARMINE_TEST_REDIS_PORT") "6379"))]}})))

(defn- env-long [k default]
  (let [value (Long/parseLong (or (not-empty (System/getenv k)) (str default)))]
    (when-not (pos? value)
      (throw (ex-info "MQ benchmark setting must be positive" {:key k, :value value})))
    value))

(defn- env-longs [k default]
  (mapv
    (fn [s]
      (let [value (Long/parseLong (str/trim s))]
        (when-not (pos? value)
          (throw (ex-info "MQ benchmark setting must be positive" {:key k, :value value})))
        value))
    (str/split (or (not-empty (System/getenv k)) default) #",")))

(defn- round3 [n] (/ (Math/round (* (double n) 1000.0)) 1000.0))
(defn- seconds [started-ns] (/ (double (- (System/nanoTime) started-ns)) 1.0e9))
(defn- rate [n elapsed-seconds] (round3 (/ (double n) elapsed-seconds)))
(defn- mean [xs] (/ (reduce + 0.0 xs) (double (count xs))))

(defn- measure [f]
  (let [started (System/nanoTime)
        result (f)]
    {:result result, :seconds (round3 (seconds started))}))

(defn- wait-for! [timeout-ms description pred]
  (let [deadline (+ (System/nanoTime) (* (long timeout-ms) 1000000))]
    (loop []
      (if (pred)
        true
        (if (< (System/nanoTime) deadline)
          (do (Thread/sleep 10) (recur))
          (throw (ex-info (str "Timed out waiting for " description)
                   {:timeout-ms timeout-ms})))))))

(defn- with-queue [suffix opts f]
  (let [queue (mq/queue mgr_
                (str "carmine-v4-mq-benchmark-" suffix "-" (java.util.UUID/randomUUID))
                opts)]
    (try (f queue) (finally (mq/queue-clear!! queue)))))

(defn- enqueue-n! [queue n options-fn]
  (dotimes [idx n]
    (let [result (mq/msg-enqueue! queue idx (assoc (options-fn idx) :mid (str idx)))]
      (when-not (= (:action result) :added)
        (throw (ex-info "Benchmark enqueue failed" {:index idx, :result result}))))))

(defn- claim-and-settle-n! [queue n maintenance-batch-size outcome]
  (dotimes [_ n]
    (let [{:keys [action mid token] :as claim} (#'mq/claim! queue maintenance-batch-size)]
      (when-not (= action :handle)
        (throw (ex-info "Benchmark expected an MQ claim" {:claim claim})))
      (let [result (#'mq/settle! queue mid token outcome)]
        (when-not (contains? #{:acked :dead} (:action result))
          (throw (ex-info "Benchmark settlement failed" {:claim claim, :result result})))))))

(deftest ^:mq-benchmark _throughput
  (let [n (env-long "CARMINE_MQ_BENCH_MESSAGES" 10000)
        concurrency (env-long "CARMINE_MQ_BENCH_CONCURRENCY" 8)
        timeout-ms (* 1000 (env-long "CARMINE_MQ_BENCH_TIMEOUT_SECONDS" 300))]
    (with-queue "throughput" {:retry-base-ms 0, :retry-max-ms 0}
      (fn [queue]
        (let [{enqueue-seconds :seconds}
              (measure #(enqueue-n! queue n (constantly {})))
              acknowledged_ (atom 0)
              done_ (promise)
              unexpected_ (atom [])
              worker
              (mq/worker-create queue (constantly (mq/outcome:ack))
                {:concurrency concurrency
                 :on-event
                 (fn [{:keys [event result]}]
                   (when (= event :settled)
                     (if (= (:action result) :acked)
                       (when (>= (swap! acknowledged_ inc) n) (deliver done_ true))
                       (swap! unexpected_ conj result))))})]
          (try
            (let [started (System/nanoTime)]
              (is (mq/worker-start! worker))
              (is (= (deref done_ timeout-ms ::timeout) true))
              (let [drain-seconds (seconds started)
                    report
                    {:messages n, :concurrency concurrency
                     :enqueue-seconds (round3 enqueue-seconds)
                     :enqueue-messages-per-second (rate n enqueue-seconds)
                     :drain-seconds (round3 drain-seconds)
                     :drain-messages-per-second (rate n drain-seconds)}]
                (println "\nCarmine v4 MQ enqueue/drain throughput:")
                (prn report)
                [(is (empty? @unexpected_))
                 (is (= @acknowledged_ n))
                 (is (zero? (get-in (mq/queue-status queue) [:counts :active])))
                 (is (pos? (:enqueue-messages-per-second report)))
                 (is (pos? (:drain-messages-per-second report)))]))
            (finally
              (.close ^java.io.Closeable worker))))))))

(deftest ^:mq-benchmark _concurrent-soak
  (let [duration-seconds (env-long "CARMINE_MQ_BENCH_SOAK_SECONDS" 10)
        nproducers (env-long "CARMINE_MQ_BENCH_SOAK_PRODUCERS" 4)
        concurrency (env-long "CARMINE_MQ_BENCH_CONCURRENCY" 8)
        max-backlog (env-long "CARMINE_MQ_BENCH_MAX_BACKLOG" 5000)
        timeout-ms (* 1000 (env-long "CARMINE_MQ_BENCH_TIMEOUT_SECONDS" 300))]
    (with-queue "soak" {:retry-base-ms 0, :retry-max-ms 0}
      (fn [queue]
        (let [produced_ (atom 0)
              acknowledged_ (atom 0)
              duplicates_ (atom 0)
              unexpected_ (atom [])
              handled-mids (java.util.concurrent.ConcurrentHashMap/newKeySet)
              worker
              (mq/worker-create queue
                (fn [{:keys [mid]}]
                  (when-not (.add handled-mids mid) (swap! duplicates_ inc))
                  (mq/outcome:ack))
                {:concurrency concurrency
                 :on-event
                 (fn [{:keys [event result]}]
                   (when (= event :settled)
                     (if (= (:action result) :acked)
                       (swap! acknowledged_ inc)
                       (swap! unexpected_ conj result))))})
              started (System/nanoTime)
              deadline (+ started (* duration-seconds 1000000000))]
          (try
            (is (mq/worker-start! worker))
            (let [producers
                  (mapv
                    (fn [producer-idx]
                      (future
                        (try
                          (loop [idx 0]
                            (when (< (System/nanoTime) deadline)
                              (if (< (- @produced_ @acknowledged_) max-backlog)
                                (let [mid (str producer-idx "-" idx)
                                      result (mq/msg-enqueue! queue mid {:mid mid})]
                                  (when-not (= (:action result) :added)
                                    (throw (ex-info "Soak enqueue failed" {:result result})))
                                  (swap! produced_ inc)
                                  (recur (inc idx)))
                                (do (Thread/sleep 1) (recur idx)))))
                          :done
                          (catch Throwable t t))))
                    (range nproducers))
                  producer-results (mapv #(deref % timeout-ms ::timeout) producers)
                  produced @produced_]
              (is (every? #{:done} producer-results))
              (wait-for! timeout-ms "the soak backlog to drain"
                #(>= @acknowledged_ produced))
              (let [elapsed (seconds started)
                    report
                    {:duration-seconds duration-seconds
                     :elapsed-through-drain-seconds (round3 elapsed)
                     :producers nproducers, :concurrency concurrency
                     :max-backlog max-backlog, :messages produced
                     :messages-per-second (rate produced elapsed)
                     :duplicate-deliveries @duplicates_}]
                (println "\nCarmine v4 MQ concurrent producer/consumer soak:")
                (prn report)
                [(is (pos? produced))
                 (is (= @acknowledged_ produced))
                 (is (= (.size handled-mids) produced))
                 ;; With no failover or lease expiry, this baseline intentionally
                 ;; has exactly-once-shaped delivery. Topology soaks do not assert it.
                 (is (zero? @duplicates_))
                 (is (empty? @unexpected_))
                 (is (zero? (get-in (mq/queue-status queue) [:counts :active])))
                 (is (pos? (:messages-per-second report)))]))
            (finally
              (.close ^java.io.Closeable worker))))))))

(defn- maintenance-run! [scenario n maintenance-batch-size]
  (with-queue (str "maintenance-" (name scenario) "-" maintenance-batch-size)
    {:lease-ms 3600000, :max-attempts 3, :retry-base-ms 0, :retry-max-ms 0}
    (fn [queue]
      (enqueue-n! queue n (fn [_] (if (= scenario :scheduled) {:delay-ms 3600000} {})))

      (case scenario
        :scheduled
        (car/wcar @mgr_ :as-vec
          (dotimes [idx n]
            (car/zadd (get (queue-keys queue) :scheduled) "XX" 0 (str idx))))

        :expired-leases
        (do
          (dotimes [_ n]
            (let [claim (#'mq/claim! queue 1)]
              (when-not (= (:action claim) :handle)
                (throw (ex-info "Failed to seed benchmark lease" {:claim claim})))))
          (car/wcar @mgr_ :as-vec
            (dotimes [idx n]
              (car/zadd (get (queue-keys queue) :leased) "XX" 0 (str idx))))))

      (let [status-before (mq/queue-status queue)
            started (System/nanoTime)]
        (claim-and-settle-n! queue n maintenance-batch-size (mq/outcome:ack))
        (let [elapsed (seconds started)]
          {:scenario scenario, :messages n
           :maintenance-batch-size maintenance-batch-size
           :seconds (round3 elapsed), :messages-per-second (rate n elapsed)
           :status-before status-before, :status-after (mq/queue-status queue)})))))

(deftest ^:mq-benchmark _maintenance-batch-sizing
  (let [n (env-long "CARMINE_MQ_BENCH_MAINTENANCE_MESSAGES" 1000)
        batch-sizes (env-longs "CARMINE_MQ_BENCH_MAINTENANCE_BATCH_SIZES"
                      "1,16,64,256,1000")
        results
        (mapv
          (fn [scenario]
            [scenario (mapv #(maintenance-run! scenario n %) batch-sizes)])
          [:scheduled :expired-leases])]
    (println "\nCarmine v4 MQ maintenance batch sizing:")
    (prn (into (array-map) results))
    (doseq [[scenario runs] results
            {:keys [messages-per-second status-before status-after]} runs]
      [(is (pos? messages-per-second))
       (is (= (get-in status-before
                [:counts (if (= scenario :scheduled) :overdue :lease-expired)]) n))
       (is (zero? (get-in status-after [:counts :active])))])))

(deftest ^:mq-benchmark _large-dead-letter-population
  (let [n (env-long "CARMINE_MQ_BENCH_DEAD_MESSAGES" 2000)
        purge-limit (env-long "CARMINE_MQ_BENCH_PURGE_LIMIT" 500)]
    (with-queue "dead-letters"
      {:max-attempts 1, :retry-base-ms 0, :retry-max-ms 0}
      (fn [queue]
        (let [seed-started (System/nanoTime)]
          (dotimes [idx n]
            (let [mid (str idx)
                  result (mq/msg-enqueue! queue idx {:mid mid})
                  claim (#'mq/claim! queue 1)
                  settled (#'mq/settle! queue mid (:token claim)
                            (mq/outcome:dead {:reason "benchmark"}))]
              (when-not (and (= (:action result) :added)
                          (= (:mid claim) mid)
                          (= (:action settled) :dead))
                (throw (ex-info "Failed to seed benchmark dead letter"
                         {:index idx, :enqueue result, :claim claim, :settled settled})))))

          (let [seed-seconds (seconds seed-started)
                status-measure (measure #(mq/queue-status queue))
                page-measure (measure #(mq/dead-mids queue 0 (dec (min n 100))))
                all-measure (measure #(mq/dead-mids queue))
                info-measure (measure #(mq/dead-info queue (str (quot n 2))))
                purge-started (System/nanoTime)
                purge-result
                (loop [removed (long 0), batches (long 0)]
                  (let [{batch-removed :removed, more? :more?}
                        (mq/dead-purge! queue {:older-than-ms 0, :limit purge-limit})
                        removed (unchecked-add (long removed) (long batch-removed))
                        batches (unchecked-inc (long batches))]
                    (if more?
                      (recur removed batches)
                      {:removed removed, :batches batches})))
                purge-seconds (seconds purge-started)
                report
                {:messages n
                 :seed-seconds (round3 seed-seconds)
                 :seed-messages-per-second (rate n seed-seconds)
                 :queue-status-milliseconds (round3 (* 1000 (:seconds status-measure)))
                 :page-100-milliseconds (round3 (* 1000 (:seconds page-measure)))
                 :list-all-milliseconds (round3 (* 1000 (:seconds all-measure)))
                 :single-info-milliseconds (round3 (* 1000 (:seconds info-measure)))
                 :purge-limit purge-limit, :purge-batches (:batches purge-result)
                 :purge-seconds (round3 purge-seconds)
                 :purge-messages-per-second (rate n purge-seconds)}]
            (println "\nCarmine v4 MQ retained dead-letter population:")
            (prn report)
            [(is (= (get-in (:result status-measure) [:counts :dead]) n))
             (is (= (count (:result all-measure)) n))
             (is (= (:mid (:result info-measure)) (str (quot n 2))))
             (is (= (:removed purge-result) n))
             (is (zero? (get-in (mq/queue-status queue) [:counts :active])))
             (is (pos? (:seed-messages-per-second report)))
             (is (pos? (:purge-messages-per-second report)))]))))))

(defn- wake-latencies! [queue n poll-only?]
  (let [current_ (atom nil)
        worker (mq/worker-create queue
                 (fn [_]
                   (when-let [result_ @current_]
                     (deliver result_ (System/nanoTime)))
                   (mq/outcome:ack))
                 {:idle-min-ms 250, :idle-max-ms 250})
        run!
        (fn []
          (try
            (mq/worker-start! worker)
            (mapv
              (fn [idx]
                (Thread/sleep 275)
                (let [handled_ (promise)
                      started (System/nanoTime)
                      mid (str (if poll-only? "poll-" "wake-") idx)]
                  (reset! current_ handled_)
                  (mq/msg-enqueue! queue idx {:mid mid})
                  (let [handled-at (deref handled_ 5000 ::timeout)]
                    (when (= handled-at ::timeout)
                      (throw (ex-info "Wake benchmark timed out" {:mid mid})))
                    (wait-for! 5000 "wake benchmark settlement"
                      #(nil? (mq/msg-status queue mid)))
                    (/ (- (long handled-at) started) 1e6))))
              (range n))
            (finally (.close ^java.io.Closeable worker))))]
    (if poll-only?
      (with-redefs-fn
        {#'mq/await-wake!
         (fn [_queue state_ sleeping-threads_
              _wake-lock _wake-conn_ _wake-blocker_ _stats_ timeout-ms]
           (#'mq/idle-sleep! state_ sleeping-threads_ timeout-ms))}
        run!)
      (run!))))

(deftest ^:mq-benchmark _blocking-wake-latency
  (let [n (env-long "CARMINE_MQ_BENCH_WAKE_MESSAGES" 25)]
    (with-queue "wake-latency" {:retry-base-ms 0, :retry-max-ms 0}
      (fn [queue]
        (let [wake-ms (wake-latencies! queue n false)
              poll-ms (wake-latencies! queue n true)
              report {:messages-per-mode n
                      :wake-mean-ms (round3 (mean wake-ms))
                      :wake-max-ms (round3 (apply max wake-ms))
                      :poll-mean-ms (round3 (mean poll-ms))
                      :poll-max-ms (round3 (apply max poll-ms))}]
          (println "\nCarmine v4 MQ blocking wake versus polling latency:")
          (prn report)
          [(is (= (count wake-ms) n))
           (is (= (count poll-ms) n))
           (is (zero? (get-in (mq/queue-status queue) [:counts :active])))])))))

(deftest ^:mq-benchmark _coalescing-throughput
  (let [n (env-long "CARMINE_MQ_BENCH_COALESCE_UPDATES" 10000)]
    (with-queue "coalescing"
      {:on-duplicate :coalesce, :retry-base-ms 0, :retry-max-ms 0}
      (fn [queue]
        (mq/msg-enqueue! queue 0 {:mid "coalesced"})
        (let [active (#'mq/claim! queue 1)
              started (System/nanoTime)]
          (dotimes [idx n]
            (let [result (mq/msg-enqueue! queue (inc idx) {:mid "coalesced"})]
              (when-not (= (:action result) :coalesced-successor)
                (throw (ex-info "Coalescing benchmark update failed"
                         {:index idx, :result result})))))
          (let [elapsed (seconds started)
                status (mq/queue-status queue)]
            (is (= (get-in status [:counts :successors]) 1))
            (#'mq/settle! queue "coalesced" (:token active) (mq/outcome:ack))
            (let [latest (#'mq/claim! queue 1)
                  report {:updates n, :seconds (round3 elapsed)
                          :updates-per-second (rate n elapsed)}]
              (println "\nCarmine v4 MQ coalescing throughput:")
              (prn report)
              [(is (= (:msg latest) n))
               (is (= (:attempt latest) 1))
               (is (pos? (:updates-per-second report)))
               (#'mq/settle! queue "coalesced" (:token latest) (mq/outcome:ack))])))))))
