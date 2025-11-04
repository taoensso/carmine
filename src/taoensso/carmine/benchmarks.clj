(ns ^:no-doc taoensso.carmine.benchmarks
  {:author "Peter Taoussanis"}
  (:require [taoensso.encore  :as enc]
            [taoensso.carmine :as car :refer (wcar)]))

(def bench-data  (apply str (repeat 100 "x")))
(def bench-key   "carmine:temp:benchmark:data-key")

(defn bench [{:keys [laps unpooled? conn-opts n-commands warmup-laps]
              :or {laps 1 conn-opts {} n-commands 10000 warmup-laps 5000}}]
  (println)
  (println "Benching (this can take some time)")
  (println "----------------------------------")
  (dotimes [l laps]
    (println)
    (println (str "Lap " (inc l) "/" laps "..."))

    (when unpooled?
      (println
       {:wcar-unpooled  (enc/bench n-commands {:warmup-laps warmup-laps} (wcar (assoc conn-opts :pool :none) "Do nothing"))
        :ping-unpooled  (enc/bench n-commands {:warmup-laps warmup-laps} (wcar (assoc conn-opts :pool :none) (car/ping)))}))

    (println
     {:wcar      (enc/bench n-commands {:warmup-laps warmup-laps} (wcar conn-opts "Do nothing"))
      :ping      (enc/bench n-commands {:warmup-laps warmup-laps} (wcar conn-opts (car/ping)))
      :set       (enc/bench n-commands {:warmup-laps warmup-laps} (wcar conn-opts (car/set bench-key bench-data)))
      :get       (enc/bench n-commands {:warmup-laps warmup-laps} (wcar conn-opts (car/get bench-key)))
      :roundtrip (enc/bench n-commands {:warmup-laps warmup-laps} (wcar conn-opts (car/ping)
                                                                         (car/set bench-key bench-data)
                                                                         (car/get bench-key)))
      :ping-pipelined (enc/bench n-commands {:warmup-laps warmup-laps} (wcar conn-opts (dorun (repeatedly 100 car/ping))))}))

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
