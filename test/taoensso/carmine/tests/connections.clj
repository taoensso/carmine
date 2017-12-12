(ns taoensso.carmine.tests.connections
  (:require  [clojure.test :refer :all]
             [taoensso.carmine.connections :as conn]))

(deftest ssl-socket-factory
  (testing "socket factory should be cached"
    (is (= (#'conn/ssl-socket-factory) (#'conn/ssl-socket-factory))))
  )
