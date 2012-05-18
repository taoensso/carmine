(ns carmine.benchmarks
  "Tools for comparing Carmine performance to other Clojure clients."
  (:require [redis
             (core            :as redis-clojure)
             (pipeline        :as redis-clojure-pipeline)]
            [clj-redis.client :as clj-redis]
            [accession.core   :as accession]
            [carmine.core     :as carmine]))

;;; Setup connection pools only ONCE
(defonce clj-redis-pool (clj-redis/init))
(defonce carmine-pool   (carmine/make-conn-pool))

(defn make-benching-options
  [& {:keys [num-laps data-size]
      :or   {num-laps  100000
             data-size 32}}]
  {:num-laps  num-laps
   :data  (apply str (repeat (int (/ data-size 2)) "x"))
   :k1    "carmine-benchmark:key1"
   :k2    "carmine-benchmark:key2"})

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
  [{:keys [k1 k2 data] :as opts}]
  (println "Benching redis-clojure...")
  (time-laps
   opts
   (redis-clojure/with-server {}
     ;; SET pipeline
     (redis-clojure-pipeline/pipeline
      (redis-clojure/ping)
      (redis-clojure/mset k1 data k2 data)
      (redis-clojure/ping))

     ;; GET pipeline
     (redis-clojure-pipeline/pipeline
      (redis-clojure/ping)
      (redis-clojure/mget k1 k2)
      (redis-clojure/ping)))))

(defn bench-clj-redis
  "NOTE: as of 0.0.12, clj-redis has no pipeline facility. This fact will
  dominate timings."
  [{:keys [k1 k2 data] :as opts}]
  (println "Benching clj-redis...")
  (let [db clj-redis-pool]
    (time-laps
     opts
     ;; SET (unpipelined)
     (clj-redis/ping db)
     (clj-redis/mset db k1 data k2 data)
     (clj-redis/ping db)

     ;; GET (unpipelined)
     (clj-redis/ping db)
     (clj-redis/mget db k1 k2)
     (clj-redis/ping db))))

(defn bench-accession
  [{:keys [k1 k2 data] :as opts}]
  (println "Benching Accession...")
  (let [spec (accession/connection-map)]
    (time-laps
     opts
     ;; SET pipeline
     (accession/with-connection spec
       (accession/ping)
       (accession/mset k1 data k2 data)
       (accession/ping))

     ;; GET pipeline
     (accession/with-connection spec
       (accession/ping)
       (accession/mget k1 k2)
       (accession/ping)))))

(defn bench-carmine
  [{:keys [k1 k2 data] :as opts}]
  (println "Benching Carmine...")
  (let [pool carmine-pool
        spec (carmine/make-conn-spec)]
    (time-laps
     opts
     ;; SET pipeline
     (carmine/with-conn pool spec
       (carmine/ping)
       (carmine/mset k1 data k2 data)
       (carmine/ping))

     ;; GET pipeline
     (carmine/with-conn pool spec
       (carmine/ping)
       (carmine/mget k1 k2)
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
  (println "Each lap consists of 4 PINGS, 1 MSET (2 keys), 1 MGET (2 keys).")
  (let [times {:redis-clojure (bench-redis-clojure opts)
               :clj-redis     (bench-clj-redis     opts)
               ;; Doesn't seem to close its sockets!
               ;; :Accession  (bench-accession     opts)
               :Carmine       (bench-carmine       opts)}]
    (println "Done!" "\n")
    (println "Raw times:" times "\n")
    (println "Sorted relative times (smaller is better):" (sort-times times))))

(comment
  (bench-and-compare-clients (make-benching-options :num-laps 1000))
  ;; ([:Carmine 1.0] [:clj-r  edis 1.51] [:redis-clojure 1.72] [:Accession 4.19])

  (bench-and-compare-clients (make-benching-options :num-laps 10000))
  ;; ([:Carmine 1.0] [:clj-redis 1.58] [:redis-clojure 1.7] [:Accession "DNF"])

  (bench-and-compare-clients (make-benching-options :num-laps  1000
                                                    :data-size 1000))
  ;; ([:Carmine 1.0] [:clj-redis 1.36] [:redis-clojure 1.43] [:Accession 2.47])

  (bench-carmine (make-benching-options :num-laps  10000
                                        :data-size 100)))