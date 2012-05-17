(ns carmine.benchmarks
  (:require [redis
             (core            :as redis-clojure)
             (pipeline        :as redis-clojure-pipeline)]
            [clj-redis.client :as clj-redis]
            [accession.core   :as accession]
            [carmine.core     :as carmine]))

(defn make-benching-options
  [& {:keys [num-laps num-threads val-length]
      :or   {num-laps    1000
             num-threads 10
             val-length  10}}]
  {:num-laps num-laps :num-threads num-threads
   :test-key "this-is-a-test-key"
   :test-val (->> (str "2 7182818284 5904523536 0287471352 6624977572"
                       "  4709369995 9574966967 6277240766 3035354759"
                       "  4571382178 5251664274")
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
  [opts]
  (println "Benching redis-clojure...")
  (redis-clojure/with-server {}
    (time-threaded-laps
     opts
     ;; SET pipeline
     (redis-clojure-pipeline/pipeline
      (redis-clojure/ping)
      (redis-clojure/set test-key (:test-val opts))
      (redis-clojure/ping))

     ;; GET pipeline
     (redis-clojure-pipeline/pipeline
      (redis-clojure/ping)
      (redis-clojure/get test-key)
      (redis-clojure/ping)))))

(defn bench-clj-redis
  "NOTE: as of 0.0.12, clj-redis has no pipeline facility."
  [opts]
  (println "Benching clj-redis...")
  (let [db (clj-redis/init)]
    (time-threaded-laps
     opts
     ;; SET (unpipelined)
     (clj-redis/ping db)
     (clj-redis/set  db test-key (:test-val opts))
     (clj-redis/ping db)

     ;; GET (unpipelined)
     (clj-redis/ping db)
     (clj-redis/get  db test-key)
     (clj-redis/ping db))))

(defn bench-accession
  [opts]
  (println "Benching Accession...")
  (let [spec (accession/connection-map)]
    (time-threaded-laps
     opts
     ;; SET pipeline
     (accession/with-connection spec
       (accession/ping)
       (accession/set test-key (:test-val opts))
       (accession/ping))

     ;; GET pipeline
     (accession/with-connection spec
       (accession/ping)
       (accession/get test-key)
       (accession/ping)))))

(defn bench-carmine
  [opts]
  (println "Benching Carmine...")
  (let [pool     (carmine/make-conn-pool)
        spec     (carmine/make-conn-spec)]
    (time-threaded-laps
     opts
     ;; SET pipeline
     (carmine/with-conn pool spec
       (carmine/ping)
       (carmine/set test-key (:test-val opts))
       (carmine/ping))

     ;; GET pipeline
     (carmine/with-conn pool spec
       (carmine/ping)
       (carmine/get test-key)
       (carmine/ping)))))

(defn bench-and-compare-clients
  [opts]
  (println "---")
  (println "Starting benchmarks with"
           num-threads "threads and" num-laps "laps.")
  (println "Each lap consists of 4 PINGS, 1 SET, 1 GET.")
  (let [times {:redis-clojure (bench-redis-clojure opts)
               :clj-redis     (bench-clj-redis opts)
               :accession     (bench-accession opts)
               :carmine       (bench-carmine opts)}]
    (println "DONE! Results: " times)))

(comment
  (bench-and-compare-clients (make-benching-options)))