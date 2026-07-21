(ns taoensso.carmine-v4.tests.mq-version
  "Runs in the Redis 5/6 v4-core CI jobs to verify the MQ fails clearly."
  (:require
   [clojure.test :refer [deftest is]]
   [taoensso.truss :as truss]
   [taoensso.carmine-v4 :as car]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.message-queue :as mq]
   [taoensso.carmine-v4.tests.test-support :as support]))

(defonce mgr_ (delay (conns/conn-manager-pooled {})))

(deftest _minimum-redis-version
  (let [actual-major (support/redis-major-version)
        expected-major
        (some-> (System/getenv "CARMINE_TEST_REDIS_MAJOR") Long/parseLong)
        qname (str "carmine-v4-mq-version-" (java.util.UUID/randomUUID))]
    (when expected-major
      (is (= actual-major expected-major)
        "CI is exercising the requested Redis major version"))
    (if (< actual-major 7)
      (let [error
            (truss/throws :ex-info
              (mq/queue mgr_ qname))
            config-key (get (#'mq/qkeys qname) :config)]
        [(is (= (:eid (ex-data error))
               :carmine.mq/unsupported-redis-version))
         (is (= (:feature (ex-data error)) :message-queue))
         (is (= (:required-version (ex-data error)) "7.0.0"))
         (is (string? (:actual-version (ex-data error))))
         (is (zero? (car/wcar @mgr_ (car/exists config-key)))
           "Version refusal performs no queue writes")])
      (let [q (mq/queue mgr_ qname)]
        (try
          [(is (mq/queue? q))
           (is (<= 7 (quot (long
                            (get (#'mq/queue-opts q)
                              :taoensso.carmine-v4.message-queue/redis-version-num))
                         65536)))]
          (finally
            (car/wcar @mgr_
              (apply car/unlink (vals (#'mq/qkeys qname))))))))))
