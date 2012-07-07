(ns taoensso.carmine.benchmarks
  {:author "Peter Taoussanis"}
  (:require [redis.core       :as redis-clojure]
            [clj-redis.client :as clj-redis]
            [accession.core   :as accession]
            [taoensso.carmine :as carmine]))

(defn make-benching-options
  [{:keys [requests clients data-size carmine-pool carmine-spec
           clj-redis-pool accession-spec] :as opts}]
  (let [merged-opts (merge {:requests  10000
                            :clients   5
                            :data-size 32
                            :data-key  "carmine:temp:benchmark:data-key"} opts)]
    (assoc merged-opts
      :data (apply str (repeat (:data-size merged-opts) "x")))))

(defmacro time-requests
  "Executes threaded requests and returns total execution time in msecs or
  \"DNF\" if there was a problem."
  [opts & body]
  `(do (dotimes [_# (int (/ (:requests ~opts) 4))] ~@body) ; Warmup

       (let [start-time#          (System/nanoTime)
             requests-per-client# (int (/ (:requests ~opts)
                                          (:clients  ~opts)))]
         (try (->> (fn [] (future (dotimes [_# requests-per-client#] ~@body)))
                   (repeatedly (:clients ~opts))
                   doall
                   (map deref)
                   dorun)
              (Math/round (/ (- (System/nanoTime) start-time#) 1000000.0))
              (catch Exception e# (println "Exception: " e#) "DNF")
              (finally (Thread/sleep 500))))))

(defmacro wrc [& body]
  `(time-requests ~'opts (redis-clojure/with-server {} ~@body)))

(defn bench-redis-clojure
  [{:keys [data-key data] :as opts}]
  (println "Benching redis-clojure...")
  {:ping (wrc (redis-clojure/ping))
   :set  (wrc (redis-clojure/set data-key data))
   :get  (wrc (redis-clojure/get data-key))})

(defmacro wcr [& body]
  `(time-requests ~'opts (-> ~'db ~@body)))

(defn bench-clj-redis
  [{:keys [data-key data] db :clj-redis-pool :as opts}]
  (println "Benching clj-redis...")
  {:ping (wcr (clj-redis/ping))
   :set  (wcr (clj-redis/set data-key data))
   :get  (wcr (clj-redis/get data-key))})

(defmacro wa [& body]
  `(time-requests ~'opts (accession/with-connection ~'spec ~@body)))

(defn bench-accession
  [{:keys [data-key data] spec :accession-spec :as opts}]
  (println "Benching Accession...")
  {:ping (wa (accession/ping))
   :set  (wa (accession/set data-key data))
   :get  (wa (accession/get data-key))})

(defmacro wc [& body]
  `(time-requests ~'opts (carmine/with-conn ~'pool ~'spec ~@body)))

(defn bench-carmine
  [{:keys [data-key data] pool :carmine-pool spec :carmine-spec :as opts}]
  (println "Benching Carmine...")
  {:ping (wc (carmine/ping))
   :set  (wc (carmine/set data-key data))
   :get  (wc (carmine/get data-key))})

(comment
  ;; Define pools and stuff only ONCE
  (def shared-opts {:carmine-spec   (carmine/make-conn-spec)
                    :carmine-pool   (carmine/make-conn-pool)
                    :accession-spec (accession/connection-map)
                    :clj-redis-pool (clj-redis/init)})

  (defn opts [& opts] (make-benching-options
                       (merge shared-opts (apply hash-map opts))))

  ;; ./redis-benchmark -n 10000 -d 100 -c 1
  ;; PING-INLINE 1000 requests completed in 0.43 seconds
  ;; SET         1000 requests completed in 0.46 seconds
  ;; GET         1000 requests completed in 0.46 seconds
  ;; {:ping 430, :get 460, :set 460}

  (bench-redis-clojure (opts :requests 10000 :clients 1 :data-size 100))
  ;; {:ping 1548, :set 1742, :get 1732}
  (bench-clj-redis     (opts :requests 10000 :clients 1 :data-size 100))
  ;; {:ping 498, :set 580, :get 557}
  (bench-carmine       (opts :requests 10000 :clients 1 :data-size 100))
  ;; {:ping 642, :set 717, :get 718} ; Carmine 0.9.0
  (bench-accession     (opts :requests 10000 :clients 1 :data-size 100))
  ;; {:ping 2214, :set "DNF", :get "DNF"} ; Doesn't close sockets!!

  )