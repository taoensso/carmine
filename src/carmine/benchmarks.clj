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

     (dotimes [_# laps-per-thread#]
       (->> (fn [] (future ~@body))
            (repeatedly (:num-threads ~opts))
            (doall)
            (map deref)
            (doall)))

     (/ (double (- (System/nanoTime) start-time#)) 1000000.0)))

(comment
  ;; Should be about equal to 'num-laps' plus some threading overhead
  (let [opts (make-benching-options :num-threads 10 :num-laps 100)]
    (time-threaded-laps opts (Thread/sleep (:num-threads opts)))))

(defn bench-redis-clojure
  [{:keys [test-key test-val] :as opts}]
  (println "Benching redis-clojure...")
  (redis-clojure/with-server {}
    (time-threaded-laps
     opts
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
  (let [db (clj-redis/init)]
    (time-threaded-laps
     opts
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

(defn- sorted-map-by-val
  [m]
  (into (sorted-map-by #(compare (get m %1) (get m %2))) m))

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
             (let [min-time (max 0.1 (apply min (vals times)))]
               (sorted-map-by-val (zipmap (keys times)
                                          (map #(float (/ % min-time))
                                               (vals times))))))))

(comment

  (bench-and-compare-clients (make-benching-options :num-laps 10
                                                    :num-threads 1))

  ;; Standard test
  (bench-and-compare-clients (make-benching-options))

  ;; High threading
  (bench-and-compare-clients (make-benching-options :num-threads 100))

  ;; No threading
  (bench-and-compare-clients (make-benching-options :num-threads 1))

  ;; Long values
  (bench-and-compare-clients (make-benching-options :val-length 1000)))