(ns taoensso.carmine-v4.tests.benchmark
  "Side-by-side Carmine v3/v4 microbenchmark, excluded from normal tests."
  (:require
   [clojure.test        :refer [deftest is]]
   [taoensso.carmine    :as v3]
   [taoensso.carmine-v4 :as v4]))

(defn- env-long [k default]
  (if-let [value (System/getenv k)] (Long/parseLong value) default))

(defn- median [xs]
  (let [sorted (vec (sort xs))]
    (nth sorted (quot (count sorted) 2))))

(defn- measure-ns-per-op [warmup-laps timed-laps samples f]
  (dotimes [_ warmup-laps] (f))
  (let [measure
        (fn []
          (let [t0 (System/nanoTime)]
            (dotimes [_ timed-laps] (f))
            (/ (double (- (System/nanoTime) t0)) timed-laps)))]
    (median (repeatedly samples measure))))

(defn- round1 [n] (/ (Math/round (* (double n) 10.0)) 10.0))
(defn- round3 [n] (/ (Math/round (* (double n) 1000.0)) 1000.0))

(defn- benchmark!
  [{:keys [warmup-laps timed-laps samples]}]
  (let [v3-pool (v3/connection-pool {})
        v3-opts {:pool v3-pool}
        key     (str "carmine:benchmark:" (java.util.UUID/randomUUID))]
    (with-open [^java.io.Closeable v3-pool v3-pool
                v4-default (v4/conn-manager {})
                v4-comparable
                (v4/conn-manager
                  {:pool-opts {:test-on-create? false
                               :test-on-borrow? false
                               :test-on-return? false
                               :test-while-idle? false}})]
      (let [operations
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
                                (dotimes [_ 100] (v4/ping)))}}

            results
            (into (sorted-map)
              (map
                (fn [[operation implementations]]
                  (let [timings
                        (into (sorted-map)
                          (map
                            (fn [[implementation f]]
                              [implementation
                               (round1
                                 (measure-ns-per-op
                                   warmup-laps timed-laps samples f))])
                            implementations))]
                    [operation
                     (assoc timings
                       :v4-comparable/v3-ratio
                       (round3 (/ (:v4-comparable timings) (:v3 timings))))]))
                operations))]
        (v4/wcar v4-default (v4/del key))
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
