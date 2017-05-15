(ns taoensso.carmine.tests.connections
  (:require [clojure.test :refer :all]
            [taoensso.carmine.connections :as conn])
  (:import (javax.net.ssl SSLSocketFactory)))

(deftest ssl-socket-factory
  (testing "socket factory"
    (is (= (#'conn/ssl-socket-factory) (#'conn/ssl-socket-factory))
        "returns the same instance every time")
    (is (instance? SSLSocketFactory (#'conn/ssl-socket-factory))
        "is instance of type SSLSocketFactory")))
