(ns taoensso.graal-tests
  (:require
   [taoensso.carmine    :as car]
   [taoensso.carmine-v4 :as car-v4])
  (:gen-class))

(defn -main [& args]
  (assert (car-v4/sentinel-spec?
            (car-v4/sentinel-spec {:graal-smoke [["localhost" 26379]]})))
  (assert (car-v4/cluster-spec?
            (car-v4/cluster-spec [["localhost" 6379]])))
  (println "Carmine v3 and v4 namespaces loaded successfully"))
