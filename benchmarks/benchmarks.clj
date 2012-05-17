(ns carmine.benchmarks
  "Tools for comparing Carmine performance to other Clojure clients."
  (:require [redis
             (core            :as redis-clojure)
             (pipeline        :as redis-clojure-pipeline)]
            [clj-redis.client :as clj-redis]
            [accession.core   :as accession]
            [carmine.core     :as carmine]))

(defn make-benching-options
  [& {:keys [num-laps val-length]
      :or   {num-laps    100000
             val-length  10}}]
  {:num-laps num-laps
   :test-key "this-is-a-test-key"
   :test-val (->> (str "2 7182818284 5904523536 0287471352 6624977572"
                       "  4709369995 9574966967 6277240766 3035354759"
                       "  4571382178 5251664274")
                  (cycle)
                  (take val-length)
                  (apply str))})

(defmacro time-laps
  "Execute body 'num-laps' times and return how long execution took in msecs
  or \"DNF\" if laps couldn't complete."
  [opts & body]
  `(let [start-time# (System/nanoTime)]
     (try
       (dorun (repeatedly (:num-laps ~opts) (fn [] ~@body)))

       (/ (double (- (System/nanoTime) start-time#)) 1000000.0)
       (catch Exception e# (println "Exception: " e#) "DNF")

       ;; Give Redis server a breather
       (finally (Thread/sleep 2000)))))

(defn bench-redis-clojure
  [{:keys [test-key test-val] :as opts}]
  (println "Benching redis-clojure...")
  (time-laps
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
  (let [db (clj-redis/init)]
    (time-laps
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
    (time-laps
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
    (time-laps
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

(defn- sort-times
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

(comment (sort-times {:a 447.38 :b "DNF" :c 112.77 :d 374.47 :e 374.47}))

(defn bench-and-compare-clients
  [opts]
  (println "---")
  (println "Starting benchmarks with" (:num-laps opts) "laps.")
  (println "Each lap consists of 4 PINGS, 1 SET, 1 GET.")
  (let [times {:redis-clojure (bench-redis-clojure opts)
               :clj-redis     (bench-clj-redis     opts)
               ;; Doesn't seem to close its sockets!
               ;; :Accession  (bench-accession     opts)
               :Carmine       (bench-carmine       opts)}]
    (println "Done!" "\n")
    (println "Raw times:" times "\n")
    (println "Sorted relative times (smaller is better):" (sort-times times))))

(comment

  ;; Easy (required to get Accession to bench)
  (bench-and-compare-clients (make-benching-options :num-laps 100))
  ;; '([:Carmine 1.0] [:redis-clojure 1.06] [:clj-redis 1.12] [:Accession 1.84])

  ;; Standard
  (bench-and-compare-clients (make-benching-options :num-laps 10000))
  ;; '([:Carmine 1.0] [:redis-clojure 1.25] [:clj-redis 1.36] [:Accession "DNF")

  ;; Big values
  (bench-and-compare-clients (make-benching-options :num-laps 10000
                                                    :val-length 1000))
  ;; '([:Carmine 1.0] [:redis-clojure 1.36] [:clj-redis 1.37] [:Accession "DNF")

  (bench-carmine (make-benching-options :num-laps    10000
                                        :val-length  100))
  ;; Snapshot: +/- 1300ms
  )