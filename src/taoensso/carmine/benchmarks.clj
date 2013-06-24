(ns taoensso.carmine.benchmarks
  {:author "Peter Taoussanis"}
  (:require [taoensso.carmine       :as car :refer (wcar)]
            [taoensso.carmine.utils :as utils]))

(def bench-data  (apply str (repeat 32 "x")))
(def bench-key   "carmine:temp:benchmark:data-key")
(defmacro bench* [& body] `(utils/bench 10000 (do ~@body) :warmup-laps 5000))

(defn bench [{:keys [laps unpooled?] :or {laps 1}}]
  (println)
  (println "Benching (this can take some time)")
  (println "----------------------------------")
  (dotimes [l laps]
    (println)
    (println (str "Lap " (inc l) "/" laps "..."))

    (println
     {:wcar-pooled    (bench* (wcar {}            "Do nothing"))
      :wcar-unpooled  (when unpooled?
                        (bench* (wcar {:pool :none} "Do nothing")))

      :ping           (bench* (wcar {} (car/ping)))
      :ping-unpooled  (when unpooled?
                        (bench* (wcar {:pool :none} (car/ping))))
      :ping-pipelined (bench* (wcar {} (dorun (repeatedly 100 car/ping))))

      :set            (bench* (wcar {} (car/set bench-key bench-data)))
      :get            (bench* (wcar {} (car/get bench-key)))
      :roundtrip      (bench* (wcar {} (car/ping)
                                       (car/set bench-key bench-data)
                                       (car/get bench-key)))}))

  (println)
  (println "Done! (Time for cake?)")
  true)

(comment (bench {}))

(comment

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