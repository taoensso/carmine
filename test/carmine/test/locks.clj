(ns carmine.test.locks
  "Locks test assumes a redis instance on localhost"
  (:require 
    [taoensso.carmine :as car])
  (:use clojure.test 
    [taoensso.timbre :only  (set-level!)] 
    [taoensso.carmine.locks :only (acquire release get- with-lock clear-all set-connection)]))


;enable if you want to see locks work (set-level! :trace)

(set-connection 
  (car/make-conn-pool) (car/make-conn-spec :host "localhost"))

(use-fixtures :once (fn [f] (f) (clear-all)))

(deftest ^:locks basic-locking 
  (is (acquire 2)) ; for 2 sec
  (is (not (acquire 2 {:wait-time 200}))) ; too early
  (Thread/sleep 1000)
  (is (acquire 2)))

(deftest ^:locks releasing  
  (let [uuid (acquire 3)]
    (is (not (acquire 3 {:wait-time 200}))) ; too early
    (is (release 3 uuid)) 
    (is (acquire 3))))

(deftest ^:locks aleady-released
  (let [uuid (acquire 4 {:wait-time 2000}) f1 (future (release 4 uuid))]
     (Thread/sleep 200); letting future run 
     (is (not (release 4 uuid)))))

(deftest ^:locks locking-scope
  (try 
    (with-lock 5 #(throw (Exception.)))
    (catch Exception e nil))
  (is (not (get- 5))))

(deftest ^:locks locking-failure 
  (acquire 6 {:expiry 3}) 
  (is (thrown? RuntimeException (with-lock 6 #() {:wait-time 10}))))

(deftest ^:locks with-lock-expiry
 (future (with-lock 9 (fn [] (Thread/sleep 1000)) {:expiry 4}))
 (Thread/sleep 200)
 (is (thrown? RuntimeException
        (with-lock 9 #() {:expiry 3 :wait-time 10}))))

