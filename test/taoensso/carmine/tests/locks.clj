(ns taoensso.carmine.tests.locks
  (:require [clojure.test :refer :all]
            [taoensso.carmine.locks
             :refer (acquire-lock release-lock with-lock)]))

(deftest basic-locking
  (is (acquire-lock {} 2 2000 2000)) ; For 2 secs
  (is (not (acquire-lock {} 2 2000 200))) ; Too early
  (Thread/sleep 1000)
  (is (acquire-lock {} 2 2000 2000)))

(deftest releasing
  (let [uuid (acquire-lock {} 3 2000 2000)]
    (is (not (acquire-lock {} 3 2000 200))) ; Too early
    (is (release-lock {} 3 uuid))
    (is (acquire-lock {} 3 2000 2000))))

(deftest aleady-released
  (let [uuid (acquire-lock {} 4 2000 2000)
        f1   (future (release-lock {} 4 uuid))]
    (Thread/sleep 200) ; Let future run
    (is (not (release-lock {} 4 uuid)))))

(deftest locking-scope
  (try (with-lock {} 5 2000 2000 (throw (Exception.)))
       (catch Exception e nil))
  (is (not (nil? (acquire-lock {} 5 2000 2000)))))

(deftest locking-failure
  (acquire-lock {} 6 3000 2000)
  (is (= nil (with-lock {} 6 #() 2000 10))))

(deftest with-lock-expiry
  (future (with-lock {} 9 4000 2000 (Thread/sleep 1000)))
 (Thread/sleep 200)
 (is (= nil (with-lock {} 9 3000 10 (identity 1)))))
