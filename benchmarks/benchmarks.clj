(ns taoensso.carmine.benchmarks
  {:author "Peter Taoussanis"}
  (:require [redis.core             :as redis-clojure]
            [clj-redis.client       :as clj-redis]
            [accession.core         :as accession]
            [taoensso.carmine       :as carmine]
            [taoensso.carmine.utils :as utils]))

;;;; Bench params

(def data     (apply str (repeat 32 "x")))
(def data-key "carmine:temp:benchmark:data-key")

(defmacro bench [& body] `(utils/bench 10000 (do ~@body)
                                       ;; :num-threads 5
                                       :warmup-laps 10000))

;;;; Client rigs

;;; redis-clojure

(defmacro wrc [& body] `(bench (redis-clojure/with-server {} ~@body)))

(defn bench-redis-clojure
  []
  (println "Benching redis-clojure...")
  {:ping (wrc (redis-clojure/ping))
   :set  (wrc (redis-clojure/set data-key data))
   :get  (wrc (redis-clojure/get data-key))})

;;; clj-redis

(defonce clj-redis-db (clj-redis/init))
(defmacro wcr [& body] `(bench (-> clj-redis-db ~@body)))

(defn bench-clj-redis
  []
  (println "Benching clj-redis...")
  {:ping (wcr (clj-redis/ping))
   :set  (wcr (clj-redis/set data-key data))
   :get  (wcr (clj-redis/get data-key))})

;;; Accession

(def accession-spec (accession/connection-map))
(defmacro wa [& body]
  `(bench (accession/with-connection accession-spec ~@body)))

(defn bench-accession
  []
  (println "Benching Accession...")
  {:ping (wa (accession/ping))
   :set  (wa (accession/set data-key data))
   :get  (wa (accession/get data-key))})

;;; Carmine

(def carmine-spec     (carmine/make-conn-spec))
(defonce carmine-pool (carmine/make-conn-pool))

(defmacro wc [& body]
  `(bench (carmine/with-conn carmine-pool carmine-spec ~@body)))

(defn bench-carmine
  []
  (println "Benching Carmine...")
  {:ping (wc (carmine/ping))
   :set  (wc (carmine/set data-key data))
   :get  (wc (carmine/get data-key))})

(comment

  (bench-redis-clojure)
  ;; {:ping 1548, :set 1742, :get 1732}
  (bench-clj-redis)
  ;; {:ping 498, :set 580, :get 557}
  (bench-carmine)
  ;; {:ping 642, :set 717, :get 718} ; Carmine 0.9.0
  (bench-accession)
  ;; {:ping 2214, :set "DNF", :get "DNF"} ; Doesn't close sockets!!

  ;; ./redis-benchmark -n 10000 -d 100 -c 1
  ;; PING-INLINE 1000 requests completed in 0.43 seconds
  ;; SET         1000 requests completed in 0.46 seconds
  ;; GET         1000 requests completed in 0.46 seconds
  ;; {:ping 430, :get 460, :set 460}

  )