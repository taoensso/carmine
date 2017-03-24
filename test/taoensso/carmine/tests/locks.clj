(ns taoensso.carmine.tests.locks
  (:require
   [clojure.test     :as test :refer [is deftest]]
   [taoensso.carmine :as car  :refer [wcar]]
   [taoensso.carmine.locks    :refer
    [acquire-lock release-lock with-lock]]))

(comment
  (remove-ns      'taoensso.carmine.tests.locks)
  (test/run-tests 'taoensso.carmine.tests.locks))

(def conn-opts {})
(def timeout-ms 2000)

(deftest basic-locking-tests
  (let [lock-name :2
        act-op1 (acquire-lock {} lock-name timeout-ms 2000)
        act-op2 (acquire-lock {} lock-name timeout-ms 200)
        act-op3
        (do
          (Thread/sleep 1000)
          (acquire-lock {} lock-name timeout-ms 2000))]

    (is (string? act-op1) "Should acquire lock and return UUID owner string")
    (is (nil?    act-op2) "Should not acquire lock and return nil")
    (is (string? act-op3) "Should acquire lock and return new UUID owner string")
    (is (not= act-op1 act-op3) "It should return new owner UUID")))

(deftest releasing-lock-tests
  (let [lock-name :3
        uuid    (acquire-lock conn-opts lock-name timeout-ms 2000)
        act-op1 (acquire-lock conn-opts lock-name timeout-ms 200) ; Too early
        act-op2 (release-lock conn-opts lock-name uuid)
        act-op3 (acquire-lock conn-opts lock-name timeout-ms 10)]

    (is (nil?    act-op1) "Can't get lock as its too early")
    (is (true?   act-op2) "Releasing lock should be successful")
    (is (string? act-op3) "Now that its released, we should be able to acquire a lock")))

(deftest already-released-tests
  (let [lock-name :4
        uuid (acquire-lock conn-opts lock-name timeout-ms 2000)]

    (future (release-lock conn-opts lock-name uuid))
    (is
      (false?
        (do (Thread/sleep 200) ; Wait for future to run
            (release-lock conn-opts lock-name uuid)))
      "Sine we already released the lock we can't release it again")))

(deftest locking-scope-tests
  (let [lock-name :5]
    (try (with-lock {} conn-opts lock-name timeout-ms (throw (Exception.)))
         (catch Exception e nil))
    (is (string? (acquire-lock conn-opts lock-name timeout-ms 2000))
        "Since with-lock threw an exception it came outside the scope and hence we can acquire a lock again.")))

(deftest locking-failure-tests
  (let [lock-name :6]
    (acquire-lock conn-opts lock-name 3000 2000)
    (is (nil? (with-lock conn-opts lock-name 2000 10))
        "There is already a lock, hence with-lock failed.")))

(deftest with-lock-expiry-tests
  (test/testing "Case 1"
    (is (thrown? clojure.lang.ExceptionInfo
                 (with-lock {} :9 500 2000 (Thread/sleep 1000)))
        "Since lock expired before being released, it should throw an exception."))

  (test/testing "Case 2"
    (let [lock-name :10]
      (future (with-lock conn-opts lock-name 500 2000 (Thread/sleep 1000)))
      (Thread/sleep 100) ;; Give future time to acquire lock
      (is (nil? (with-lock conn-opts lock-name 3000 10 :foo))
          "Since Lock was already acquired we should get nil back")))

  (test/testing "Case 3"
    (let [lock-name :11]
      (future (with-lock conn-opts lock-name 500 2000 (Thread/sleep 1000)))
      (Thread/sleep 600) ;; Give future time to acquire + lose lock
      (is (= {:result :foo}
             (with-lock conn-opts lock-name 3000 10 :foo))
          "Since Lock was expired and then we tried to acquire it, we should get a lock"))))
