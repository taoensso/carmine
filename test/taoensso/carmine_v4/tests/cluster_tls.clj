(ns taoensso.carmine-v4.tests.cluster-tls
  "Live TLS-only Redis Cluster smoke test, selected explicitly by CI."
  (:require
   [clojure.test :refer [deftest is]]
   [taoensso.carmine-v4 :as car]
   [taoensso.carmine-v4.tests.tls :as tls])
  (:import [javax.net.ssl SSLContext]))

(defn- get-env! [k]
  (or (System/getenv k)
    (throw (ex-info "Missing TLS Cluster test environment variable" {:key k}))))

(deftest ^:cluster-tls-integration _live-cluster-tls
  (let [host     (get-env! "CARMINE_TEST_CLUSTER_TLS_HOST")
        addrs    (mapv #(vector host (Long/parseLong (get-env! %)))
                   ["CARMINE_TEST_CLUSTER_TLS_PORT_1"
                    "CARMINE_TEST_CLUSTER_TLS_PORT_2"
                    "CARMINE_TEST_CLUSTER_TLS_PORT_3"])
        ca-cert  (get-env! "CARMINE_TEST_CLUSTER_TLS_CA_CERT")
        original-context (SSLContext/getDefault)
        ssl-context (tls/trusting-ssl-context ca-cert)
        cluster-spec-opts {:conn-opts {:socket-opts {:ssl true}}}
        keys (mapv #(str "{cluster-tls-" % "}:carmine:v4") (range 24))]
    (try
      ;; This selector runs in its own JVM, before Carmine's delayed default
      ;; SSLSocketFactory has been realized.
      (SSLContext/setDefault ssl-context)
      (let [spec (car/cluster-spec addrs cluster-spec-opts)]
        (with-open [mgr
                    (car/conn-manager-clustered
                      {:conn-opts
                       {:server {:cluster-spec spec}
                        :socket-opts {:ssl true}}})]
          (try
            (doseq [key keys]
              (is (= (car/wcar mgr (car/set key "encrypted")) "OK")))
            (doseq [key keys]
              (is (= (car/wcar mgr (car/get key)) "encrypted")))
            (let [topology (:topology @spec)
                  masters (mapv :master (vals (:slot-ranges topology)))]
              (is (:complete? topology))
              (is (= (count (set (map :tls-addr masters))) 3))
              (is (every? :tls-addr masters)))
            (finally
              (doseq [key keys]
                (try (car/wcar mgr (car/del key)) (catch Throwable _)))))))
      (finally
        (SSLContext/setDefault original-context)))))
