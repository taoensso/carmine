(ns taoensso.carmine-v4.tests.auth
  "Authenticated Redis compatibility tests, selected explicitly by CI."
  (:require
   [clojure.test             :refer [deftest testing is]]
   [taoensso.carmine-v4      :as car :refer [wcar]]
   [taoensso.carmine-v4.resp :as resp]))

(defn- get-env! [k]
  (or (System/getenv k)
    (throw (ex-info "Missing Redis compatibility test environment variable"
             {:key k}))))

(defn- ping!
  [port resp3? username password]
  (with-open [mgr
              (car/conn-manager-unpooled
                {:conn-opts
                 {:server ["127.0.0.1" port]
                  :init
                  {:resp3? resp3?
                   :auth {:username username, :password password}
                   :client-name nil}}})]
    (wcar mgr (resp/ping))))

(deftest ^:redis-auth _authenticated-connections
  (let [redis-major (Long/parseLong (get-env! "CARMINE_TEST_REDIS_MAJOR"))
        port        (Long/parseLong (or (System/getenv "CARMINE_TEST_REDIS_PORT") "6379"))
        password    (get-env! "CARMINE_TEST_REDIS_PASSWORD")]
    (testing "Password-only authentication"
      (is (= (ping! port false nil password) "PONG")
        "RESP2 uses legacy one-argument AUTH")

      (when (>= redis-major 6)
        (is (= (ping! port true nil password) "PONG")
          "RESP3 supplies the default ACL username required by HELLO")))

    (when (>= redis-major 6)
      (let [username     (get-env! "CARMINE_TEST_REDIS_ACL_USERNAME")
            acl-password (get-env! "CARMINE_TEST_REDIS_ACL_PASSWORD")]
        (testing "Explicit ACL authentication"
          [(is (= (ping! port false username acl-password) "PONG")
             "RESP2 uses two-argument AUTH")
           (is (= (ping! port true username acl-password) "PONG")
             "RESP3 includes the explicit ACL username in HELLO")]))

      (let [username (get-env! "CARMINE_TEST_REDIS_ACL_NOPASS_USERNAME")]
        (testing "Password-free ACL authentication"
          [(is (= (ping! port false username nil) "PONG")
             "RESP2 sends an empty password for a username-only ACL URI")
           (is (= (ping! port true username nil) "PONG")
             "RESP3 includes username-only ACL authentication in HELLO")])))))
