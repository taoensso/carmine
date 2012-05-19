(ns carmine.benchmarks
  "Tools for comparing Carmine performance to other clients."
  (:require [redis.core       :as redis-clojure]
            [clj-redis.client :as clj-redis]
            [accession.core   :as accession]
            [carmine.core     :as carmine]))

(defn make-benching-options
  [{:keys [requests clients data-size carmine-pool carmine-spec
           clj-redis-pool accession-spec] :as opts}]
  (let [merged-opts (merge {:requests  10000
                            :clients   5
                            :data-size 32
                            :data-key  "carmine-benchmark:data-key"} opts)]
    (assoc merged-opts
      :data (apply str (repeat (:data-size merged-opts) "x")))))

(defmacro time-requests
  "Executes threaded requests and returns total execution time in msecs or
  \"DNF\" if there was a problem."
  [opts & body]
  `(let [start-time#          (System/nanoTime)
         requests-per-client# (int (/ (:requests ~opts)
                                      (:clients  ~opts)))]
     (try
       (->> (fn [] (future (dotimes [_# requests-per-client#] ~@body)))
            (repeatedly (:clients ~opts))
            (doall) ; Make sure all the threads have started
            (map deref)
            (dorun) ; Wait for all the threads to complete
            )
       (/ (double (- (System/nanoTime) start-time#)) 1000000.0)
       (catch Exception e# (println "Exception: " e#) "DNF")
       (finally (Thread/sleep 500)) ; Let server rest
       )))

(comment (time-requests (make-benching-options {:requests 9
                                                :clients  3})
                        (Thread/sleep 1000)))

(defn bench-redis-clojure
  [{:keys [data-key data] :as opts}]
  (println "Benching redis-clojure...")
  {:ping (time-requests opts (redis-clojure/with-server {}
                               (redis-clojure/ping)))
   :set  (time-requests opts (redis-clojure/with-server {}
                               (redis-clojure/set data-key data)))
   :get  (time-requests opts (redis-clojure/with-server {}
                               (redis-clojure/get data-key)))})

(defn bench-clj-redis
  [{:keys [data-key data] db :clj-redis-pool :as opts}]
  (println "Benching clj-redis...")
  {:ping (time-requests opts (clj-redis/ping db))
   :set  (time-requests opts (clj-redis/set db data-key data))
   :get  (time-requests opts (clj-redis/get db data-key))})

(defn bench-accession
  [{:keys [data-key data] spec :accession-spec :as opts}]
  (println "Benching Accession...")
  {:ping (time-requests opts (accession/with-connection spec
                               (accession/ping)))
   :set  (time-requests opts (accession/with-connection spec
                               (accession/set data-key data)))
   :get  (time-requests opts (accession/with-connection spec
                               (accession/get data-key)))})

(defn bench-carmine
  [{:keys [data-key data] pool :carmine-pool spec :carmine-spec :as opts}]
  (println "Benching Carmine...")
  {:ping (time-requests opts (carmine/with-conn pool spec
                               (carmine/ping)))
   :set  (time-requests opts (carmine/with-conn pool spec
                               (carmine/set data-key data)))
   :get  (time-requests opts (carmine/with-conn pool spec
                               (carmine/get data-key)))})

(defn- sort-relative-times
  "{:a 447.38 :b \"DNF\" :c 112.77 :d 374.47 :e 374.47} =>
  '([:c 1.0] [:d 3.3] [:e 3.3] [:a 4.0] [:b \"DNF\"])"
  [m]
  (let [round #(float (/ (Math/round (double (* % 100))) 100)) ; 2 places

        ;; Like standard compare, but allows comparison of strings and numbers
        ;; (numbers always sort first)
        mixed-compare (fn [x y] (cond (and (number? x) (string? y)) -1
                                     (and (string? x) (number? y)) 1
                                     :else (compare x y)))

        min-time (apply min (filter number? (vals m))) ; 112.77

        relative-val (fn [t] (if-not (number? t) t
                                    (round (/ t min-time))))]

    (sort-by second mixed-compare
             (map (fn [k] [k (relative-val (get m k))]) (keys m)))))

(comment
  (sort-relative-times {:a 447.38 :b "DNF" :c 112.77 :d 374.47 :e 374.47}))

(defn bench-and-compare-clients
  "(bench-and-compare-clients (make-benching-options) :carmine bench-carmine ...)"
  [opts & client-benchmark-fn-pairs]
  (println "---")
  (println "Starting benchmarks with options:" opts)
  (let [times (let [m (apply hash-map client-benchmark-fn-pairs)]
                (zipmap (keys m)
                        (map (fn [bench-fn] (bench-fn opts)) (vals m))))]
    (println "Done!\n")
    (println "RAW TIMES:\n" times "\n")

    ;; Analysis
    (let [clients  (keys times)
          subtests (keys (get times (first clients)))
          times-by-test
          (apply merge-with (partial merge-with merge)
                 (for [s subtests c clients] {s {c (get-in times [c s])}}))]

      (println "SORTED RELATIVE TIMES (smaller is better):\n"
               (zipmap (keys times-by-test)
                       (map sort-relative-times (vals times-by-test)))))))

(comment
  ;; Define pools and stuff only ONCE
  (defonce shared-opts {:carmine-spec   (carmine/make-conn-spec)
                        :carmine-pool   (carmine/make-conn-pool)
                        :accession-spec (accession/connection-map)
                        :clj-redis-pool (clj-redis/init)})

  (defn opts [& opts] (make-benching-options
                       (merge shared-opts (apply hash-map opts))))

  ;; Ad hoc
  (bench-carmine       (opts :requests 10000 :clients 1 :data-size 100))
  ;; {:ping 829.496, :set 933.46, :get 882.737}
  (bench-accession     (opts :requests 10000 :clients 1 :data-size 100))
  ;; {:ping 2418.782, :set "DNF", :get "DNF"}
  (bench-clj-redis     (opts :requests 10000 :clients 1 :data-size 100))
  ;; {:ping 613.685, :set 631.608, :get 620.953}
  (bench-redis-clojure (opts :requests 10000 :clients 1 :data-size 100))
  ;; {:ping 1585.879, :set 1833.997, :get 1882.774}

  ;; ./redis-benchmark -n 10000 -d 100 -c 1
  ;; PING-INLINE 1000 requests completed in 0.43 seconds
  ;; SET         1000 requests completed in 0.46 seconds
  ;; GET         1000 requests completed in 0.46 seconds

  ;; Comparisons
  (bench-and-compare-clients
   (assoc shared-opts :requests 1000 :clients 1 :data-size 100)
   :carmine       bench-carmine
   :redis-clojure bench-redis-clojure
   :clj-redis     bench-clj-redis
   :accession     bench-accession ; WARNING: Doesn't seem to close sockets!

   ;; Reference benchmark
   :redis-benchmark
   (constantly {:ping 430 :get 460 :set 460})))