(ns taoensso.carmine.benchmarks
  {:author "Peter Taoussanis"}
  (:require [taoensso.encore  :as encore]
            [taoensso.carmine :as car :refer (wcar)]))

(def bench-data  (apply str (repeat 32 "x")))
(def bench-key   "carmine:temp:benchmark:data-key")
(defmacro bench* [& body] `(encore/bench 10000 {:warmup-laps 5000} ~@body))

(defn bench [{:keys [laps unpooled?] :or {laps 1}}]
  (println)
  (println "Benching (this can take some time)")
  (println "----------------------------------")
  (dotimes [l laps]
    (println)
    (println (str "Lap " (inc l) "/" laps "..."))

    (when unpooled?
      (println
       {:wcar-unpooled  (bench* (wcar {:pool :none} "Do nothing"))
        :ping-unpooled  (bench* (wcar {:pool :none} (car/ping)))}))

    (println
     {:wcar (bench* (wcar {} "Do nothing"))
      :ping (bench* (wcar {} (car/ping)))
      :set  (bench* (wcar {} (car/set bench-key bench-data)))
      :get  (bench* (wcar {} (car/get bench-key)))
      :roundtrip (bench* (wcar {} (car/ping)
                                  (car/set bench-key bench-data)
                                  (car/get bench-key)))
      :ping-pipelined (bench* (wcar {} (dorun (repeatedly 100 car/ping))))}))

  (println)
  (println "Done! (Time for cake?)")
  true)

(comment (bench {:unpooled? true})
         (bench {:laps 3}))

(comment

  ;;; 2014 Feb 13, cleaned up request-planned design --server
  ;; {:wcar 47, :ping 728, :set 765, :get 763, :roundtrip 946, :ping-pipelined 3741}

  ;;; 2014 Feb 12, completely new request-planning design
  ;;; (motivated by desire for Cluster pipeline support):
  ;; {:wcar 100, :ping 860, :set 902, :get 916, :roundtrip 1119, :ping-pipelined 4129}

  ;;; 2013 Oct 12, Carmine 2.2.3
  ;; {:wcar 73, :ping 741, :set 830, :get 785, :roundtrip 1105, :ping-pipelined 14171}

  ;;; 24 June 2013, + Perf. refactoring
  ;; {:wcar-unpooled 53188, :ping-unpooled 33540}
  ;; {:wcar 42, :ping 576, :set 608, :get 596, :roundtrip 875, :ping-pipelined 13939}
  ;; (/ (* 10000 100) 14.0) => ~ 71k pings/sec

  ;;; 24 June 2013: Clojure 1.5.1, JVM 7, Carmine 2.0.0-alpha4
  ;; {:wcar-pooled 191, :wcar-unpooled 29991, :ping 1532, :ping-unpooled 29589,
  ;;  :ping-pipelined 18361, :set 1017, :get 1054, :roundtrip 1437}

  ;; {:ping 704, :set 755, :get 778} ; Carmine 1.9.1
  ;; {:ping 642, :set 717, :get 718} ; Carmine 0.9.0

  ;; ./redis-benchmark -n 10000 -d 100 -c 1
  ;; PING-INLINE 1000 requests completed in 0.43 seconds
  ;; SET         1000 requests completed in 0.46 seconds
  ;; GET         1000 requests completed in 0.46 seconds
  ;; {:ping 430, :get 460, :set 460}

  )
