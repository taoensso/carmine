(ns taoensso.carmine.tests.main
  (:require [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.benchmarks :as benchmarks]))

(expect (benchmarks/bench {}))