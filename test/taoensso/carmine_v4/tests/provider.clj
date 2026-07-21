(ns taoensso.carmine-v4.tests.provider
  "Opt-in, provider-safe acceptance tests for a user-supplied Redis endpoint."
  (:require
   [clojure.test        :refer [deftest testing is]]
   [taoensso.carmine-v4 :as car])
  (:import [java.util Locale]))

(defn- parse-bool-env [k default]
  (if-let [value (System/getenv k)]
    (contains? #{"1" "true" "yes" "on"} (.toLowerCase value Locale/ROOT))
    default))

(defn- exercise-endpoint! [uri resp3? key]
  (with-open [mgr
              (car/conn-manager
                {:conn-opts {:server uri, :init {:resp3? resp3?}}})]
    (try
      [(is (= (car/wcar mgr (car/ping)) "PONG"))
       (is (= (car/wcar mgr (car/set key "provider-safe")) "OK"))
       (is (= (car/wcar mgr (car/get key)) "provider-safe"))
       (is (= (car/wcar mgr :as-vec
                (car/set key "pipeline")
                (car/get key))
              ["OK" "pipeline"]))]
      (finally
        (try (car/wcar mgr (car/del key)) (catch Throwable _))))))

(deftest ^:provider-integration _managed-provider
  (if-let [uri (System/getenv "CARMINE_TEST_REDIS_URI")]
    (let [resp3?     (parse-bool-env "CARMINE_TEST_REDIS_RESP3" true)
          key-prefix (str "carmine:v4:provider-acceptance:"
                       (java.util.UUID/randomUUID))]
      (testing "Non-destructive commands against the supplied managed endpoint"
        (exercise-endpoint! uri false (str key-prefix ":resp2"))
        (when resp3?
          (exercise-endpoint! uri true (str key-prefix ":resp3")))))

    (is true
      "Skipped: set CARMINE_TEST_REDIS_URI to run managed-provider acceptance")))
