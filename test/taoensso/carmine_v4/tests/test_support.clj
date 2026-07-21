(ns taoensso.carmine-v4.tests.test-support
  "Shared support for Carmine v4 tests that use a standalone Redis server."
  (:require
   [clojure.test :as test]
   [taoensso.encore :as enc]
   [taoensso.carmine-v4 :as car :refer [wcar]]
   [taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.opts :as opts]
   [taoensso.carmine-v4.resp :as resp]))

(defn env-bool [k default]
  (if-let [^String value (not-empty (System/getenv k))]
    (contains? #{"1" "true" "yes" "on"} (.toLowerCase value java.util.Locale/ROOT))
    default))

(defn test-key "Returns a namespaced Redis key owned by the test suite." [key]
  (str "__:carmine:test:" (enc/as-qname key)))

(def test-conn-opts
  "Unparsed standalone connection options for the active test environment."
  (if (env-bool "CARMINE_TEST_REDIS_RESP3" true)
    {}
    {:init {:resp3? false}}))

(def parsed-test-conn-opts (opts/parse-conn-opts :redis test-conn-opts))
(defonce manager_ (delay (conns/conn-manager-pooled {:conn-opts test-conn-opts})))

(defn redis-major-version []
  (let [server-info (wcar manager_ (resp/rcmd "INFO" "server"))]
    (some-> (re-find #"(?m)^redis_version:(\d+)" server-info)
      second Long/parseLong)))

(defn supported-resp3-options
  "Returns the RESP protocol options supported by the active Redis server."
  []
  (if (>= (long (redis-major-version)) 6) [false true] [false]))

(defn- delete-test-keys! []
  (when-let [ks (seq (wcar manager_ (resp/rcmd "KEYS" (test-key "*"))))]
    (wcar manager_ (doseq [k ks] (resp/rcmd "DEL" k)))))

(defn clean-redis-fixture []
  (enc/test-fixtures {:before delete-test-keys!, :after delete-test-keys!}))

(defn use-clean-redis-fixture! []
  (test/use-fixtures :once (clean-redis-fixture)))

(defn await-pred!
  "Waits up to `timeout-ms` for `(pred)` to return a truthy value."
  [timeout-ms description pred]
  (let [deadline (+ (System/nanoTime) (* (long timeout-ms) 1000000))]
    (loop []
      (if-let [result (pred)]
        result
        (if (< (System/nanoTime) deadline)
          (do (Thread/sleep 5) (recur))
          (throw
            (ex-info (str "Timed out waiting for " description)
              {:timeout-ms timeout-ms, :description description})))))))

(defn assert-manager-stats-schema! [kind manager]
  (let [{:keys [schema-version snapshot-client-time-ms connections counts
                timings push nodes] :as stats}
        (car/conn-manager-stats manager)]
    [(test/is (= (set (keys stats))
                #{:schema-version :kind :snapshot-client-time-ms :connections
                  :counts :timings :push :nodes}))
     (test/is (= [schema-version (:kind stats)] [1 kind]))
     (test/is (pos-int? snapshot-client-time-ms))
     (test/is (= (set (keys connections)) #{:active :idle :waiting}))
     (test/is (= (set (keys counts))
                #{:created :borrowed :returned :failed :cleared :destroyed}))
     (test/is (= (set (keys (:destroyed counts)))
                #{:total :by-borrow-validation :by-eviction}))
     (test/is (= (set (keys timings))
                #{:mean-borrow-wait-ms :max-borrow-wait-ms
                  :mean-idle-ms :mean-active-ms}))
     (test/is (= (set (keys push))
                #{:enabled? :queue-capacity :queue-depth :active :shutdown? :counts}))
     (test/is (= (set (keys (:counts push)))
                #{:received :completed :rejected :handler-errors
                  :discarded-on-close}))
     (if (= kind :clustered)
       [(test/is (vector? nodes))
        (test/is (every? #(= (set (keys %)) #{:server-addr :active :idle}) nodes))]
       (test/is (nil? nodes)))]))
