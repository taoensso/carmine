(ns carmine.benchmarks
  "Tools for comparing Carmine performance to other Clojure clients."
  (:require [redis
             (core            :as redis-clojure)
             (pipeline        :as redis-clojure-pipeline)]
            [clj-redis.client :as clj-redis]
            [accession.core   :as accession]
            [carmine.core     :as carmine]))

(defn make-benching-options
  [& {:keys [num-laps num-threads val-length]
      :or   {num-laps    10000
             num-threads 10
             val-length  10}}]
  {:num-laps num-laps :num-threads num-threads
   :test-key "this-is-a-test-key"
   :test-val (->> (str "2 7182818284 5904523536 0287471352 6624977572"
                       "  4709369995 9574966967 6277240766 3035354759"
                       "  4571382178 5251664274")
                  (cycle)
                  (take val-length)
                  (apply str))})

(defmacro time-threaded-laps
  "Excutue body (/ 'num-laps' 'num-threads') times in 'num-threads'
  threads. Returns how long execution took in msecs."
  [opts & body]
  `(let [laps-per-thread# (int (/ (:num-laps ~opts) (:num-threads ~opts)))
         start-time# (System/nanoTime)]

     (try

       (dotimes [_# laps-per-thread#]
         (->> (fn [] (future ~@body))
              (repeatedly (:num-threads ~opts))
              (doall)
              (map deref)
              (dorun)))

       (/ (double (- (System/nanoTime) start-time#)) 1000000.0)
       (catch Exception e# (println "Exception: " e#) "DNF")

       ;; Give Redis server a breather
       (finally (Thread/sleep 1000)))))

(comment
  ;; Should be about equal to 'num-laps' plus some threading overhead
  (let [opts (make-benching-options :num-threads 10 :num-laps 1000)]
    (time-threaded-laps opts (Thread/sleep (:num-threads opts)))))

(defn bench-redis-clojure
  [{:keys [test-key test-val] :as opts}]
  (println "Benching redis-clojure...")
  (time-threaded-laps
   opts
   (redis-clojure/with-server {}
     ;; SET pipeline
     (redis-clojure-pipeline/pipeline
      (redis-clojure/ping)
      (redis-clojure/set test-key test-val)
      (redis-clojure/ping))

     ;; GET pipeline
     (redis-clojure-pipeline/pipeline
      (redis-clojure/ping)
      (redis-clojure/get test-key)
      (redis-clojure/ping)))))

(defn bench-clj-redis
  "NOTE: as of 0.0.12, clj-redis has no pipeline facility."
  [{:keys [test-key test-val] :as opts}]
  (println "Benching clj-redis...")
  (time-threaded-laps
   opts
   (let [db (clj-redis/init)]
     ;; SET (unpipelined)
     (clj-redis/ping db)
     (clj-redis/set  db test-key test-val)
     (clj-redis/ping db)

     ;; GET (unpipelined)
     (clj-redis/ping db)
     (clj-redis/get  db test-key)
     (clj-redis/ping db))))

(defn bench-accession
  [{:keys [test-key test-val] :as opts}]
  (println "Benching Accession...")
  (let [spec (accession/connection-map)]
    (time-threaded-laps
     opts
     ;; SET pipeline
     (accession/with-connection spec
       (accession/ping)
       (accession/set test-key test-val)
       (accession/ping))

     ;; GET pipeline
     (accession/with-connection spec
       (accession/ping)
       (accession/get test-key)
       (accession/ping)))))

(defn bench-carmine
  [{:keys [test-key test-val] :as opts}]
  (println "Benching Carmine...")
  (let [pool     (carmine/make-conn-pool)
        spec     (carmine/make-conn-spec)]
    (time-threaded-laps
     opts
     ;; SET pipeline
     (carmine/with-conn pool spec
       (carmine/ping)
       (carmine/set test-key test-val)
       (carmine/ping))

     ;; GET pipeline
     (carmine/with-conn pool spec
       (carmine/ping)
       (carmine/get test-key)
       (carmine/ping)))))

(defn- sorted-map-by-vals
  "{:a 447.38 :b \"DNF\" :c 112.77 :d 374.47} =>
  {:c 1.0 :d 3.3 :a 4.0 :b \"DNF\"}"
  [m]
  (let [round-to-one-place
        (fn [x] (float (/ (Math/round (* (double x) 10)) 10)))

        ;; Like 'compare' but can handle "DNF"/number comparison
        safe-compare (fn [x y]
                       (cond (and (number? x) (string? y)) -1
                             (and (string? x) (number? y)) 1
                             :else (compare x y)))

        min-time (apply min (filter number? (vals m))) ; 112.77

        ;; {:a 3.9671898 :b "DNF" :c 1.0 :d 3.3206527}
        relative-times
        (zipmap (keys m)
                (map (fn [t] (if-not (number? t) t
                                    (round-to-one-place (/ t min-time))))
                     (vals m)))]

    ;; {:c 1.0, :d 3.3206527, :a 3.9671898, :b "DNF"}
    (into (sorted-map-by #(safe-compare (get relative-times %1)
                                        (get relative-times %2)))
          relative-times)))

(comment (sorted-map-by-vals {:a 447.38 :b "DNF" :c 112.77 :d 374.47}))

(defn bench-and-compare-clients
  [opts]
  (println "---")
  (println "Starting benchmarks with"
           (:num-threads opts) "threads and" (:num-laps opts) "laps.")
  (println "Each lap consists of 4 PINGS, 1 SET, 1 GET.")
  (let [times {:redis-clojure (bench-redis-clojure opts)
               :clj-redis     (bench-clj-redis opts)
               :accession     (bench-accession opts)
               :carmine       (bench-carmine opts)}]
    (println "Done!" "\n")
    (println "Raw times:" times "\n")
    (println "Sorted relative times (smaller is better):"
             (sorted-map-by-vals times))))

(comment

  ;; Easy test
  (bench-and-compare-clients (make-benching-options :num-threads 1
                                                    :num-laps 10))

  ;; Standard test
  (bench-and-compare-clients (make-benching-options))

  ;; High threading
  (bench-and-compare-clients (make-benching-options :num-threads 100))

  ;; No threading
  (bench-and-compare-clients (make-benching-options :num-threads 1))

  ;; Long values
  (bench-and-compare-clients (make-benching-options :val-length 1000)))