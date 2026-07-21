(ns taoensso.carmine-v4.tests.benchmark
  "Side-by-side Carmine v3/v4 microbenchmark, excluded from normal tests."
  (:require
   [clojure.test        :refer [deftest is]]
   [taoensso.carmine    :as v3]
   [taoensso.carmine-v4 :as v4]
   [taoensso.carmine-v4.tests.test-support :as support]))

(defn- env-long [k default]
  (if-let [value (System/getenv k)] (Long/parseLong value) default))

(defn- median [xs]
  (let [sorted (vec (sort xs))]
    (nth sorted (quot (count sorted) 2))))

(defn- measure-ns-per-op
  "Returns {<implementation> <median-ns-per-op>}. Samples are interleaved and
  their order rotated, so that drift (JIT, server, machine load) affects all
  implementations alike."
  [warmup-laps timed-laps samples implementations]
  (doseq [[_ f] implementations] (dotimes [_ warmup-laps] (f)))
  (let [ids (vec (keys implementations))
        measure
        (fn [f]
          (let [t0 (System/nanoTime)]
            (dotimes [_ timed-laps] (f))
            (/ (double (- (System/nanoTime) t0)) timed-laps)))]
    (reduce-kv (fn [m id xs] (assoc m id (median xs))) {}
      (reduce
        (fn [acc sample-idx]
          (reduce
            (fn [acc id] (update acc id (fnil conj []) (measure (get implementations id))))
            acc
            (take (count ids) (drop (mod sample-idx (count ids)) (cycle ids)))))
        {} (range samples)))))

(defn- round1 [n] (/ (Math/round (* (double n) 10.0)) 10.0))
(defn- round3 [n] (/ (Math/round (* (double n) 1000.0)) 1000.0))

(defn- benchmark!
  [{:keys [warmup-laps timed-laps samples]}]
  (let [v3-pool (v3/connection-pool {})
        v3-opts (assoc support/v3-test-conn-opts :pool v3-pool)
        key     (str "carmine:benchmark:" (java.util.UUID/randomUUID))
        key-list (str key ":list")
        key-hash (str key ":hash")]
    (with-open [^java.io.Closeable v3-pool v3-pool
                v4-default (v4/conn-manager {})
                v4-comparable
                (v4/conn-manager
                  {:pool-opts {:test-on-create? false
                               :test-on-borrow? false
                               :test-on-return? false
                               :test-while-idle? false}})]
      (v4/wcar v4-default
        (v4/rcmd* (into ["RPUSH" key-list] (map #(str "item-" %)) (range 1000)))
        (v4/rcmd* (into ["HSET"  key-hash]
                    (mapcat (fn [n] [(str "field-" n) (str "value-" n)]))
                    (range 500))))

      (let [;; Slower operations run proportionally fewer laps
            laps-divisors {:pipeline-100 10, :pipeline-1000 50, :lrange-1000 10, :hgetall-500 10}
            operations
            {:empty
             {:v3 #(v3/wcar v3-opts)
              :v4-default #(v4/wcar v4-default)
              :v4-comparable #(v4/wcar v4-comparable)}

             :ping
             {:v3 #(v3/wcar v3-opts (v3/ping))
              :v4-default #(v4/wcar v4-default (v4/ping))
              :v4-comparable #(v4/wcar v4-comparable (v4/ping))}

             :set-get
             {:v3 #(v3/wcar v3-opts (v3/set key "value") (v3/get key))
              :v4-default #(v4/wcar v4-default (v4/set key "value") (v4/get key))
              :v4-comparable #(v4/wcar v4-comparable
                                (v4/set key "value") (v4/get key))}

             :pipeline-100
             {:v3 #(v3/wcar v3-opts (dotimes [_ 100] (v3/ping)))
              :v4-default #(v4/wcar v4-default (dotimes [_ 100] (v4/ping)))
              :v4-comparable #(v4/wcar v4-comparable
                                (dotimes [_ 100] (v4/ping)))}

             :pipeline-1000
             {:v3 #(v3/wcar v3-opts (dotimes [_ 1000] (v3/ping)))
              :v4-default #(v4/wcar v4-default (dotimes [_ 1000] (v4/ping)))
              :v4-comparable #(v4/wcar v4-comparable
                                (dotimes [_ 1000] (v4/ping)))}

             ;; Large replies, dominated by reply reading
             :lrange-1000
             {:v3 #(v3/wcar v3-opts (v3/lrange key-list 0 -1))
              :v4-default #(v4/wcar v4-default (v4/lrange key-list 0 -1))
              :v4-comparable #(v4/wcar v4-comparable (v4/lrange key-list 0 -1))}

             ;; NB v3 returns a flat vector, v4 with RESP3 returns a map
             :hgetall-500
             {:v3 #(v3/wcar v3-opts (v3/hgetall key-hash))
              :v4-default #(v4/wcar v4-default (v4/hgetall key-hash))
              :v4-comparable #(v4/wcar v4-comparable (v4/hgetall key-hash))}}

            results
            (into (sorted-map)
              (map
                (fn [[operation implementations]]
                  (let [divisor (long (get laps-divisors operation 1))
                        timings
                        (into (sorted-map)
                          (map (fn [[implementation ns-per-op]] [implementation (round1 ns-per-op)]))
                          (measure-ns-per-op
                            (max 1 (quot (long warmup-laps) divisor))
                            (max 1 (quot (long timed-laps)  divisor))
                            samples implementations))]
                    [operation
                     (assoc timings
                       :v4-comparable/v3-ratio
                       (round3 (/ (:v4-comparable timings) (:v3 timings))))]))
                operations))]
        (v4/wcar v4-default (v4/del key key-list key-hash))
        {:config {:warmup-laps warmup-laps
                  :timed-laps timed-laps
                  :samples samples}
         :units :nanoseconds-per-operation
         :results results}))))

(deftest ^:benchmark _v3-v4-baseline
  (let [config {:warmup-laps (env-long "CARMINE_BENCH_WARMUP_LAPS" 1000)
                :timed-laps  (env-long "CARMINE_BENCH_TIMED_LAPS" 5000)
                :samples     (env-long "CARMINE_BENCH_SAMPLES" 5)}
        result (benchmark! config)]
    (println "\nCarmine v3/v4 benchmark baseline:")
    (prn result)
    (is (every? pos?
          (for [[_ timings] (:results result)
                [_ value] (dissoc timings :v4-comparable/v3-ratio)]
            value)))))
