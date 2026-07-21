(ns taoensso.carmine-v4.tests.tls
  "Live TLS acceptance tests, selected explicitly by CI."
  (:require
   [clojure.test        :refer [deftest testing is]]
   [taoensso.carmine-v4 :as car])
  (:import
   [java.io FileInputStream]
   [java.security KeyStore SecureRandom]
   [java.security.cert CertificateFactory]
   [javax.net.ssl SSLContext SSLHandshakeException TrustManagerFactory]))

(defn- get-env! [k]
  (or (System/getenv k)
    (throw (ex-info "Missing Redis TLS test environment variable" {:key k}))))

(defn trusting-ssl-context [^String ca-cert-path]
  (let [cert
        (with-open [in (FileInputStream. ca-cert-path)]
          (.generateCertificate (CertificateFactory/getInstance "X.509") in))

        key-store (doto (KeyStore/getInstance (KeyStore/getDefaultType))
                    (.load nil nil)
                    (.setCertificateEntry "carmine-test-ca" cert))

        trust-manager-factory
        (doto (TrustManagerFactory/getInstance
                (TrustManagerFactory/getDefaultAlgorithm))
          (.init key-store))

        ssl-context (SSLContext/getInstance "TLS")]
    (.init ssl-context nil (.getTrustManagers trust-manager-factory) (SecureRandom.))
    ssl-context))

(defn- caused-by? [class t]
  (loop [t t]
    (cond
      (nil? t) false
      (instance? class t) true
      :else (recur (.getCause ^Throwable t)))))

(deftest ^:tls-integration _live-tls
  (let [host       (get-env! "CARMINE_TEST_TLS_HOST")
        port       (Long/parseLong (get-env! "CARMINE_TEST_TLS_PORT"))
        ca-cert    (get-env! "CARMINE_TEST_TLS_CA_CERT")
        mismatch-host (get-env! "CARMINE_TEST_TLS_MISMATCH_HOST")
        ssl-context (trusting-ssl-context ca-cert)
        original-context (SSLContext/getDefault)
        key-prefix (str "carmine:v4:tls-acceptance:" (java.util.UUID/randomUUID))]

    (try
      ;; Carmine fetches the default SSLSocketFactory per connection, so this
      ;; runtime default is honored by all conns created below.
      (SSLContext/setDefault ssl-context)
      (doseq [resp3? [false true]]
        (testing (str "Default TLS trust, hostname verification, and RESP"
                   (if resp3? 3 2) " round trip")
          (let [key (str key-prefix ":" (if resp3? "resp3" "resp2"))]
            (with-open [mgr
                        (car/conn-manager
                          {:conn-opts
                           {:server (str "rediss://" host ":" port "/0")
                            :init {:resp3? resp3?}}})]
              (try
                [(is (= (car/wcar mgr (car/ping)) "PONG"))
                 (is (= (car/wcar mgr (car/set key "encrypted")) "OK"))
                 (is (= (car/wcar mgr (car/get key)) "encrypted"))]
                (finally
                  (try (car/wcar mgr (car/del key)) (catch Throwable _))))))))

      (testing "A trusted certificate for a different hostname is rejected"
        (let [error
              (try
                (with-open [mgr
                            (car/conn-manager-unpooled
                              {:conn-opts
                               {:server [mismatch-host port]
                                :socket-opts {:ssl true}}})]
                  (car/wcar mgr (car/ping))
                  nil)
                (catch Throwable t t))]
          (is (caused-by? SSLHandshakeException error))))

      (finally
        (SSLContext/setDefault original-context)))))
