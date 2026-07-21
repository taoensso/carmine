(ns taoensso.carmine-v4.tests.config
  "Carmine v4 utility and connection-option tests."
  (:require
   [clojure.test        :refer [deftest testing is]]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine-v4.utils    :as utils]
   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.cluster  :as cluster]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tc+ "Parsed test conn-opts" support/parsed-test-conn-opts)

(support/use-clean-redis-fixture!)

(defrecord ^:private SecretConfig [password nested])

(deftest _diagnostic-secret-redaction
  (let [secret "never-report-this"
        opts
        {:server (str "redis://alice:" secret "@redis.example:6379/0")
         :init
         {:auth {:username "alice", :password secret}
          :commands [["AUTH" "alice" secret]]}
         :nested [{:password secret}]}
        redacted (utils/redact-secrets opts)]
    [(is (= (:server redacted)
           "redis://<carmine-redacted>@redis.example:6379/0"))
     (is (= (get-in redacted [:init :auth])
           {:username "alice", :password :carmine/redacted}))
     (is (= (get-in redacted [:init :commands]) :carmine/redacted))
     (is (= (get-in redacted [:nested 0 :password]) :carmine/redacted))
     (is (= (utils/redact-secrets {:auth secret})
           {:auth :carmine/redacted}))
     (is (= (utils/redact-secrets
              {:uri (str "redis://alice:" secret "@redis.example")})
           {:uri "redis://<carmine-redacted>@redis.example"}))
     (let [record (utils/redact-secrets
                    (->SecretConfig secret {:password secret}))]
       [(is (instance? SecretConfig record))
        (is (= (:password record) :carmine/redacted))
        (is (= (get-in record [:nested :password]) :carmine/redacted))])
     (is (not (.contains (pr-str redacted) secret)))

     (with-open [mgr
                 (conns/conn-manager-pooled
                   {:conn-opts {:init {:auth {:username "alice", :password secret}}}})]
       (is (= (get-in @mgr [:conn-opts :init :auth :password]) :carmine/redacted)))

     (let [spec
           (sentinel/sentinel-spec
             {:primary [["sentinel.example" 26379]]}
             {:conn-opts {:init {:auth {:password secret}}}})]
       (is (= (get-in @spec [:sentinel-spec-opts :conn-opts :init :auth :password])
             :carmine/redacted)))

     (let [spec
           (cluster/cluster-spec
             [["redis.example" 6379]]
             {:conn-opts {:init {:auth {:password secret}}}})]
       (is (= (get-in @spec [:cluster-spec-opts :conn-opts :init :auth :password])
             :carmine/redacted)))

     (doseq [[label bad-opts]
             [[:malformed-auth     {:init {:auth secret}}]
              [:malformed-commands {:init {:commands ["AUTH" secret]}}]
              [:unknown-init-key   {:init {:auth {:password secret}
                                           :unexpected true}}]
              [:malformed-uri      {:server
                                    (str "redis://alice:" secret
                                      " bad@redis.example")}]
              [:non-map-opts       (str "redis://alice:" secret
                                     "@redis.example")]]]
       (let [error (truss/throws (opts/parse-conn-opts :redis bad-opts))
             sw    (java.io.StringWriter.)]
         (.printStackTrace ^Throwable error (java.io.PrintWriter. sw))
         [(is (= (:eid (ex-data error)) :carmine.conn-opts/invalid)
            (name label))
          (is (string? (:error-class (ex-data (ex-cause error))))
            (str (name label) " sanitized cause identifies its error class"))
          (is (nil? (:error-type (ex-data (ex-cause error))))
            (str (name label) " sanitized cause uses the standard error-class key"))
          (when (= label :non-map-opts)
            (is (= (:eid (ex-data (ex-cause error)))
                  :carmine.conn-opts/validation-failed)
              "A sanitized assertion cause receives a stable fallback eid"))
          (is (not (.contains (str sw) secret))
            (str (name label) " error and cause are credential-safe"))]))]))

(deftest _callback-notification-isolation
  (let [events_    (atom [])
        logs_      (atom [])
        data-calls_ (atom 0)
        data_      (delay (swap! data-calls_ inc)
                     {:event :data, :cbid :test-callback})
        cause      (StackOverflowError. "Expected callback failure")]
    (with-redefs [utils/report-callback-error!
                  (fn [cb cbid t]
                    (swap! logs_ conj
                      {:callback cb, :cbid cbid, :error t}))]
      (utils/cb-notify!
        (fn [_] (throw cause))
        (fn [data] (swap! events_ conj [:second data]))
        (fn [data] (swap! events_ conj [:third data]))
        data_))

    [(is (= @events_
           [[:second {:event :data, :cbid :test-callback}]
            [:third  {:event :data, :cbid :test-callback}]])
       "A critical callback failure does not escape or suppress later callbacks")
     (is (= @data-calls_ 1) "Callback event data is realized once")
     (is (= (count @logs_) 1))
     (is (= (:cbid (first @logs_)) :test-callback))
     (is (identical? (:error (first @logs_)) cause))
     (is
       (true?
         @(future
            (with-redefs [utils/report-callback-error! (fn [_cb _cbid _t])]
              (utils/cb-notify!
                (fn [_] (throw (InterruptedException. "Expected callback interruption")))
                data_))
            (Thread/interrupted)))
       "Interrupted callbacks restore the thread's interrupt status")]))

;;;; Opts

(deftest _sock-addrs
  [(is (= (opts/descr-sock-addr (opts/parse-sock-addr            "ip" "80"))  ["ip" 80                ]))
   (is (= (opts/descr-sock-addr (opts/parse-sock-addr ^:my-meta ["ip" "80"])) ["ip" 80 {:my-meta true}]))
   (is (throws? :ex-info (opts/parse-sock-addr ["ip" 0])))
   (is (throws? :ex-info (opts/parse-sock-addr ["ip" 65536])))
   (is (throws? :ex-info (opts/parse-sock-addr ["ip" 80 :extra])))])

(deftest _parse-string-server
  (let [parse #'opts/parse-string-server]
    [(is (= (parse "redis://user:pass@x.y.com:9475/3")     {:server ["x.y.com"     9475],  :init {:auth {:username "user", :password "pass"}, :select-db 3}}))
     (is (= (parse "redis://:pass@x.y.com.com:9475/3")     {:server ["x.y.com.com" 9475],  :init {:auth {                  :password "pass"}, :select-db 3}} ))
     (is (= (parse "redis://user:@x.y.com:9475/3")         {:server ["x.y.com"     9475],  :init {:auth {:username "user"                  }, :select-db 3}}))
     (is (= (parse "rediss://user:@x.y.com:9475/3")        {:server ["x.y.com"     9475],  :init {:auth {:username "user"                  }, :select-db 3}, :socket-opts {:ssl true}}))
     (is (= (parse "redis://localhost")                    {:server ["localhost"   6379]}) "Redis URIs use the standard port by default")
     (is (= (parse "redis://user:p%3Ass+word@localhost/0") {:server ["localhost"   6379],  :init {:auth {:username "user",  :password "p:ss+word"}, :select-db 0}}) "Credentials are split before percent-decoding and preserve literal plus signs")
     (is (= (parse "redis://us%3Aer:pass@localhost/")      {:server ["localhost"   6379],  :init {:auth {:username "us:er", :password "pass"}}}))
     (is (= (parse "redis://user:p:a:ss@localhost")        {:server ["localhost"   6379],  :init {:auth {:username "user",  :password "p:a:ss"}}}))
     (is (= (parse "redis://user@localhost")               {:server ["localhost"   6379],  :init {:auth {:username "user"}}}))
     (is (= (parse "redis://[::1]/3"),                     {:server ["[::1]"       6379],  :init {:select-db 3}}) "IPv6 literals are accepted")

     (is (throws? :ex-info (parse "http://localhost")))
     (is (throws? :ex-info (parse "redis:///3")))
     (is (throws? :ex-info (parse "redis://localhost:0")))
     (is (throws? :ex-info (parse "redis://localhost:65536")))
     (is (throws? :ex-info {:eid :carmine.conn-opts/invalid-uri-path}
           (parse "redis://localhost/not-a-db")))
     (is (throws? :ex-info (parse "redis://localhost/3?query=unsupported")))
     (is (throws? :ex-info (parse "redis://localhost/3#fragment")))]))

(deftest _parse-conn-opts
  [(is (truss/submap? (opts/parse-conn-opts :redis {:server [      "127.0.0.1"       "80"]}) {:server ["127.0.0.1" 80]}))
   (is (truss/submap? (opts/parse-conn-opts :redis {:server {:host "127.0.0.1" :port "80"}}) {:server ["127.0.0.1" 80]}))
   (is (= (get-in (opts/parse-conn-opts :redis {:init {:auth {:password "pass"}}}) [:init :auth])
         {:username nil, :password "pass"})
     "Default options preserve an absent username")
   (is (truss/submap? (opts/parse-conn-opts :redis {:server "rediss://user:pass@x.y.com:9475/3"})
         {:server ["x.y.com" 9475], :init {:auth {:username "user", :password "pass"}, :select-db 3
                                           :resp3? (get-in tc+ [:init :resp3?])},
          :socket-opts {:ssl true}}))

   (is (->> (opts/parse-conn-opts :redis {:server ^:my-meta ["127.0.0.1" "6379"]}) :server (meta) :my-meta) "Retains metadata")

   (is (->> (opts/parse-conn-opts :redis {:server ["127.0.0.1" "invalid-port"]})                  (throws? :ex-info {:eid :carmine.conn-opts/invalid-server})))
   (is (->> (opts/parse-conn-opts :redis {:server {:host "127.0.0.1" :port "80" :invalid "foo"}}) (throws? :ex-info {:eid :carmine.conn-opts/invalid-server})))
   (is (throws? :ex-info
         (opts/parse-conn-opts :redis {:buffer-opts {:init-size-in 0}})))
   (is (throws? :ex-info
         (opts/parse-conn-opts :redis
           {:buffer-opts {:init-size-out (inc (long Integer/MAX_VALUE))}})))

   (doseq [bad-init
           [{:select-db -1}
            {:select-db "3"}
            {:resp3? "yes"}
            {:auth "user:pass"}
            {:auth {:username :user}}
            {:auth {:password 1234}}
            {:client-name :kw}]]
     (is (->> (opts/parse-conn-opts :redis {:init bad-init})
           (throws? :ex-info {:eid :carmine.conn-opts/invalid-init-option}))
       "Init option values are validated eagerly, at parse time"))

   (is (map? (opts/parse-conn-opts :redis
               {:init {:select-db 3, :resp3? false, :client-name nil
                       :auth {:username "user", :password "pass"}}})))
   (is (map? (opts/parse-conn-opts :redis
               {:init {:client-name (fn [_conn-opts] "my-name")}})))
   (is (map? (opts/parse-conn-opts :redis
               {:init {:commands []}}))
     "An empty command list intentionally disables connection initialization")
   (is (map? (opts/parse-conn-opts :redis
               {:init {:commands [[]]}}))
     "Empty individual commands support programmatic construction")

   (is (truss/submap?
         (opts/parse-conn-opts :redis
           {:server {:sentinel-spec (sentinel/sentinel-spec {:foo/bar [["127.0.0.1" 26379]]})
                     :master-name :foo/bar}})
         {:server {:master-name "foo/bar", :sentinel-opts {:prefer-read-replica? false}}}))

   (is (truss/submap?
         (opts/parse-conn-opts :redis
           {:server {:sentinel-spec (sentinel/sentinel-spec {:foo/bar [["127.0.0.1" 26379]]})
                     :master-name :foo/bar, :sentinel-opts {:prefer-read-replica? true}}})
         {:server {:master-name "foo/bar",  :sentinel-opts {:prefer-read-replica? true}}}))

   (let [spec (sentinel/sentinel-spec {:foo/bar [["127.0.0.1" 26379]]})]
     (doseq [bad-opts
             [{:clear-timeout-ms -1}
              {:prefer-read-replica? nil}]]
       (is (->> (opts/parse-conn-opts :redis
                   {:server {:sentinel-spec spec, :master-name :foo/bar
                             :sentinel-opts bad-opts}})
             (throws? :ex-info {:eid :carmine.conn-opts/invalid}))))
     (doseq [discovery-opt
             [{:retry-delay-ms 100}
              {:resolve-cache-ttl-ms nil}
              {:update-replicas? false}
              {:cbs {}}]]
       (is (->> (opts/parse-conn-opts :redis
                   {:server {:sentinel-spec spec, :master-name :foo/bar
                             :sentinel-opts discovery-opt}})
             (throws? :ex-info {:eid :carmine.conn-opts/invalid})))))

   (doseq [bad-spec-opts
           [{:retry-delay-ms -1}
            {:resolve-timeout-ms "slow"}
            {:resolve-cache-ttl-ms -1}
            {:update-sentinels? :yes}
            {:update-replicas? 1}
            {:prefer-read-replica? true}
            {:clear-timeout-ms 10}]]
     (is (->> (sentinel/sentinel-spec
                {:foo/bar [["127.0.0.1" 26379]]} bad-spec-opts)
           (throws? :ex-info {:eid :carmine.sentinel/invalid-spec-opts}))))

   (is (nil?
         (get-in @(sentinel/sentinel-spec
                    {:foo/bar [["127.0.0.1" 26379]]}
                    {:resolve-cache-ttl-ms nil})
           [:sentinel-spec-opts :resolve-cache-ttl-ms]))
     "Nil explicitly disables proactive Sentinel refresh on the shared spec")

   (let [spec
         (cluster/cluster-spec [["seed.redis" 7000]]
           {:topology-source :cluster-slots})]
     [(is (truss/submap?
            (opts/parse-conn-opts :redis {:server {:cluster-spec spec}})
            {:server
             {:cluster-spec spec
              :cluster-opts
              {:max-retry-rounds 4
               :retry-backoff-ms 25
               :max-concurrent-partitions 1}}})
        "Managers receive only Cluster execution policy")

      (is (truss/submap? (:cluster-spec-opts @spec)
            {:topology-source :cluster-slots
             :conn-opts {:buffer-opts {:init-size-in 4096}
                         :socket-opts {:connect-timeout-ms 200}}})
        "The spec owns parsed discovery policy")

      (is (truss/submap?
            (opts/parse-conn-opts :redis
              {:server
               {:cluster-spec spec
                :cluster-opts {:max-retry-rounds 2
                               :max-concurrent-partitions 3}}})
            {:server {:cluster-opts {:max-retry-rounds 2
                                     :max-concurrent-partitions 3}}})
        "Managers may override only execution policy")

      (is (truss/submap?
            (opts/parse-conn-opts :redis
              {:server {:cluster-spec spec}, :init {:select-db 0}})
            {:init {:select-db 0}})
        "Cluster permits explicit database zero")

      (doseq [bad
              [{:server {:cluster-spec spec :sentinel-spec :mixed}}
               {:server {:cluster-spec :not-a-spec}}
               {:server {:cluster-spec spec :cluster-opts {:unknown true}}}
               {:server {:cluster-spec spec :cluster-opts
                         {:topology-source :cluster-shards}}}
               {:server {:cluster-spec spec :cluster-opts
                         {:max-concurrent-partitions 0}}}
               {:server {:cluster-spec spec :cluster-opts
                         {:max-concurrent-partitions nil}}}
               {:server {:cluster-spec spec}, :init {:select-db 1}}]]
        (is (throws? :ex-info (opts/parse-conn-opts :redis bad))))])

   (testing "RESP resource limits"
     [(is (= (get-in (opts/parse-conn-opts :redis nil)
               [:resp-opts :limits :max-line-bytes])
            1048576))
      (is (= (get-in
               (opts/parse-conn-opts :redis
                 {:resp-opts {:limits {:max-line-bytes 64}}})
               [:resp-opts :limits])
            {:max-line-bytes 64
             :max-nesting-depth 128
             :max-blob-bytes nil
             :max-aggregate-elements nil
             :max-frame-bytes nil})
        "Caller limits deeply merge over defaults")
      (is (nil? (get-in
                  (opts/parse-conn-opts :redis
                    {:resp-opts {:limits {:max-line-bytes nil}}})
                  [:resp-opts :limits :max-line-bytes]))
        "A per-limit nil explicitly disables a default")
      (is (= (get-in
               (opts/parse-conn-opts :sentinel
                 {:resp-opts {:limits {:max-frame-bytes 1024}}})
               [:resp-opts :limits :max-frame-bytes])
            1024)
        "Sentinel connections accept and inherit RESP limits")
      (doseq [bad [{:resp-opts nil}
                   {:resp-opts {:limits nil}}
                   {:resp-opts {:unknown true}}
                   {:resp-opts {:limits {:unknown 1}}}
                   {:resp-opts {:limits {:max-line-bytes -1}}}
                   {:resp-opts {:limits {:max-line-bytes 1.5}}}]]
        (is (throws? :ex-info (opts/parse-conn-opts :redis bad))))])])
