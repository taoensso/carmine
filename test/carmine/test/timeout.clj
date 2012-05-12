(ns carmine.test.timeout
  (:use     [clojure.test])
  (:require [carmine (core :as redis) (connections :as conns)])
  (:import  [java.net ServerSocket]
            [java.io InputStreamReader BufferedReader]))

(defn handle-connection [conn]
  (let [rdr (BufferedReader. (InputStreamReader. (.getInputStream conn)))]
    (if-let [request (.readLine rdr)]
      (do
        (with-open [os (.getOutputStream conn)]
          (do (Thread/sleep 1000)
              (.write os (.getBytes "*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n"))
              (.flush os)
              (.close conn)))))))

(defn server-loop [server-socket]
  (let [conn (.accept server-socket)]
    (do (.setKeepAlive conn true)
        (future (handle-connection conn))
        (recur server-socket))))

(defn start-server []
  (do (let [ss (ServerSocket. 9000)]
        (future (server-loop ss))
        {:socket ss})))

(defn stop-server [server]
  (.close (:socket server)))

(deftest test-timeout
  (let [server (start-server)
        p (conns/make-conn-pool)
        s (conns/make-conn-spec :port 9000)]
    (do (is (= (conns/with-conn p s (redis/get "foo")) ["one" "two" "three"]))
        (stop-server server)))
  (let [server (start-server)
        p (conns/make-conn-pool)
        s (conns/make-conn-spec :port 9000 :timeout 500)]
    (do (is (thrown? Exception
                     (conns/with-conn p s (redis/get "foo"))))
        (stop-server server))))