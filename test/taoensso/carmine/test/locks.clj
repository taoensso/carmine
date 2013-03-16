(ns taoensso.carmine.test.locks
  "Locks test assumes a redis instance on localhost"
  (:require 
    [taoensso.carmine :as car])
  (:use clojure.test 
    [taoensso.timbre :only  (set-level!)] 
    [taoensso.carmine.locks :only (acquire-lock release-lock with-lock set-config! clear-all)]))


(set-config! [:conns :spec] (car/make-conn-spec :host "localhost"))
(set-config! [:conns :pool] (car/make-conn-pool))

(use-fixtures :once (fn [f] (f) (clear-all)))

(deftest ^:locks basic-locking 
  (is (acquire-lock 2 2000 2000)) ; for 2 sec
  (is (not (acquire-lock 2 2000 200))) ; too early
  (Thread/sleep 1000)
  (is (acquire-lock 2 2000 2000)))

(deftest ^:locks releasing  
  (let [uuid (acquire-lock 3 2000 2000)]
    (is (not (acquire-lock 3 2000 200))) ; too early
    (is (release-lock 3 uuid)) 
    (is (acquire-lock 3 2000 2000))))

(deftest ^:locks aleady-released
  (let [uuid (acquire-lock 4 2000 2000) f1 (future (release-lock 4 uuid))]
     (Thread/sleep 200); letting future run 
     (is (not (release-lock 4 uuid)))))

(deftest ^:locks locking-scope
  (try 
    (with-lock 5 2000 2000 (throw (Exception.)))
    (catch Exception e nil))
  (is (not (nil? (acquire-lock 5 2000 2000)))))

(deftest ^:locks locking-failure 
  (acquire-lock 6 3000 2000) 
  (is (= nil (with-lock 6 #() 2000 10))))

(deftest ^:locks with-lock-expiry
 (future (with-lock 9 4000 2000 (Thread/sleep 1000)))
 (Thread/sleep 200)
 (is (= nil (with-lock 9 3000 10 (identity 1)))))

