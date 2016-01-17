(ns taoensso.carmine.tests.cluster
  (:require [taoensso.carmine :as car]
            [taoensso.carmine.tests.cluster :refer :all]
            [clojure.test :refer :all]))



(def conn1 {:pool {} :spec {:host "localhost" :port 8002 :cluster true}})
(def conn2 {:pool {} :spec {:host "localhost" :port 6379}})

(defmacro wcar1 [& form]
  `(car/wcar conn1 ~@form))

(defmacro wcar2 [& form]
  `(car/wcar conn2 ~@form))

(println  (wcar1 (car/get "D")
                 (car/incr "K")
                 (car/incr "A")))
(wcar2 (car/incr "I"))
