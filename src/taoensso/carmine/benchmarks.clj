(ns taoensso.carmine.benchmarks
  {:author "Peter Taoussanis"}
  (:require [taoensso.encore  :as enc]
            [taoensso.carmine :as car :refer (wcar)]))

(def bench-data  (apply str (repeat 100 "x")))
(def bench-key   "carmine:temp:benchmark:data-key")
(defmacro bench* [& body] `(enc/bench 10000 {:warmup-laps 5000} ~@body))

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
     {:wcar      (bench* (wcar {} "Do nothing"))
      :ping      (bench* (wcar {} (car/ping)))
      :set       (bench* (wcar {} (car/set bench-key bench-data)))
      :get       (bench* (wcar {} (car/get bench-key)))
      :roundtrip (bench* (wcar {} (car/ping)
                                  (car/set bench-key bench-data)
                                  (car/get bench-key)))
      :ping-pipelined (bench* (wcar {} (dorun (repeatedly 100 car/ping))))}))

  (println)
  (println "Done! (Time for cake?)")
  true)

(comment
  (bench {:unpooled? true})
  (bench {:laps 3})

  ;;; 2016 Apr 28, new hardware
  ;; {:wcar 20, :ping 372, :set 392, :get 391, :roundtrip 464, :ping-pipelined 2361}

  ;; ./redis-benchmark -n 10000 -d 100 -c 1
  ;; PING-INLINE 10k requests completed in 0.36 seconds
  ;; SET         10k requests completed in 0.37 seconds
  ;; GET         10k requests completed in 0.35 seconds
  )
