(ns taoensso.carmine-v4.tests.sentinel-unit
  "Deterministic Carmine v4 Sentinel discovery and state tests."
  (:require
   [clojure.string      :as str]
   [clojure.test        :as test :refer [deftest testing is]]
   [taoensso.encore     :as enc]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine    :as v3-core]
   [taoensso.carmine-v4          :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.utils    :as utils]
   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.cluster  :as cluster]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tk  "Test key" support/test-key)
(def tc  "Unparsed test conn-opts" support/test-conn-opts)
(def tc+ "Parsed test conn-opts" support/parsed-test-conn-opts)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

(defn- redis-major-version [] (support/redis-major-version))

;;;; Sentinel

(deftest _addr-utils
  (let [sm (#'sentinel/add-addrs->back nil [["ip1" 1] ["ip2" "2"] ^{:server-name "server3"} ["ip3" 3]])
        sm (#'sentinel/add-addr->front sm  ["ip2" 2])
        sm (#'sentinel/add-addrs->back sm [["ip3" 3] ["ip6" 6]])]

    [(is (= sm [["ip2" 2] ["ip1" 1] ["ip3" 3] ["ip6" 6]]))
     (is (= (mapv opts/descr-sock-addr sm)
           [["ip2" 2] ["ip1" 1] ["ip3" 3 {:server-name "server3"}] ["ip6" 6]]))]))

(deftest _unique-addrs
  [(is (= (#'sentinel/unique-addrs
            {:m0 {              :sentinels [[0 1]]}
             :m1 {:master [1 1] :sentinels [[1 1] [1 2] [2 2]]}
             :m2 {:master [1 1] :sentinels [[3 3]] :replicas #{[1 1] [3 3]}}})

         {:masters   #{[1 1]},
          :replicas  #{[3 3] [1 1]},
          :sentinels #{[0 1] [1 1] [1 2] [2 2] [3 3]}}))

   (is (= (get-in @(sentinel/sentinel-spec {:master [["sentinel" 26379]]})
            [:stats :node-counts])
         {:masters 0, :replicas 0, :sentinels 1})
     "An unresolved master isn't counted as a known node")])

(deftest _parse-nodes-info->addrs
  [(is (= (#'sentinel/parse-nodes-info->addrs
            [{"ip"   "host1" "port" "1" "x1" "y1"}
             {"ip"   "host2" "port" "2"}
             ["host" "host3" "port" "3" "x2" "y2"]
             {"ip" "bad-port-low"  "port" "0"}
             {"ip" "bad-port-high" "port" "65536"}
             {"ip" 17 "port" "4"}
             ["ip" "missing-port"]])

         [["host1" 1] ["host2" 2] ["host3" 3]])
     "Malformed ancillary node reports are skipped without discarding valid peers")

   (is (= (#'sentinel/parse-nodes-info->addrs :replica
            [{"ip" "healthy"      "port" "1" "flags" "slave"                 "master-link-status" "ok"}
             {"ip" "subjective"   "port" "2" "flags" "slave,s_down"          "master-link-status" "ok"}
             {"ip" "objective"    "port" "3" "flags" "slave,o_down"          "master-link-status" "ok"}
             {"ip" "disconnected" "port" "4" "flags" "slave,disconnected"    "master-link-status" "ok"}
             {"ip" "link-down"    "port" "5" "flags" "slave"                 "master-link-status" "err"}])
          [["healthy" 1]])
     "Sentinel down flags and failed replication links are excluded")])

(deftest _parse-node-role
  [(is (= (#'sentinel/parse-node-role "master")  :master))
   (is (= (#'sentinel/parse-node-role "slave")   :replica))
   (is (= (#'sentinel/parse-node-role "replica") :replica))
   (is (= (#'sentinel/parse-node-role "unknown") nil))])

(deftest _sentinel-role-candidate-fallback
  (let [bad-replica  ["bad-replica" 1]
        good-replica ["good-replica" 2]
        master       ["master" 3]
        role-replies
        {bad-replica  ["slave" "master" 3 "sync"]
         good-replica ["slave" "master" 3 "connected"]
         master       ["master"]}]
    [(is (= (:confirmed
              (#'sentinel/confirm-role-candidates
                master [[bad-replica :replica] [good-replica :replica] [master :master]]
                role-replies))
            [good-replica :replica])
       "A disconnected replica is skipped in favor of another healthy replica")
     (is (= (:confirmed
              (#'sentinel/confirm-role-candidates
                master [[bad-replica :replica] [master :master]] role-replies))
            [master :master])
       "The reported master remains the final fallback")]))

(deftest _sentinel-misidentified-reporter-fallback
  (let [calls_ (atom [])
        spec
        (sentinel/sentinel-spec
          {"my-master" [["sentinel-a" 26379] ["sentinel-b" 26380]]}
          {:resolve-timeout-ms 1000, :retry-delay-ms 1})
        replies
        {"sentinel-a"
         [["stale-master" 6379]
          [{"ip" "stale-replica", "port" "6380", "flags" "slave",
            "master-link-status" "ok"}]
          []]
         "stale-replica" ["slave" "stale-master" 6379 "disconnected"]
         "stale-master"  ["slave"]
         "sentinel-b"    [["healthy-master" 6381] [] []]
         "healthy-master" ["master"]}]
    (#'sentinel/update-addrs! spec "my-master" :replicas
      (fn [_] [["cached-replica" 6382]]))
    (with-redefs [conns/with-new-conn
                  (fn [_conn-opts host _port _master-name f]
                    (swap! calls_ conj host)
                    (f nil host nil))
                  resp/rcmd (fn [& _] nil)
                  resp/with-replies
                  (fn [in _out _reply-opts _as-vec? body-fn]
                    (body-fn)
                    (get replies in))]
      [(is (= (#'sentinel/resolve-addr! spec "my-master"
                {:prefer-read-replica? true} false)
              ["healthy-master" 6381]))
       (is (= @calls_
             ["sentinel-a" "stale-replica" "stale-master"
              "sentinel-b" "healthy-master"])
         "A rejected report does not prevent consulting later Sentinels")
       (is (= (get-in @spec [:stats :sentinel-stats ["sentinel-a" 26379]
                             :n-misidentified])
             1))
       (is (= (get-in @spec [:stats :sentinel-stats ["sentinel-b" 26380]
                             :n-successes])
             1))
       (is (= (get-in @spec [:nodes-addrs "my-master" :replicas]) [])
         "Replica topology comes only from the successful reporter")])))

(deftest _sentinel-malformed-reporter-fallback
  (let [calls_ (atom [])
        spec
        (sentinel/sentinel-spec
          {"my-master" [["sentinel-a" 26379] ["sentinel-b" 26380]]}
          {:resolve-timeout-ms 1000, :retry-delay-ms 1})
        replies
        {"sentinel-a" [["bad-master" 0] [] []]
         "sentinel-b"
         [["healthy-master" 6381]
          [{"ip" "healthy-replica", "port" "6382", "flags" "slave",
            "master-link-status" "ok"}
           {"ip" "bad-replica", "port" "0", "flags" "slave",
            "master-link-status" "ok"}]
          [{"ip" "sentinel-c", "port" "26381"}
           {"ip" "bad-sentinel", "port" "not-a-port"}]]
         "healthy-master" ["master"]}]
    (with-redefs [conns/with-new-conn
                  (fn [_conn-opts host _port _master-name f]
                    (swap! calls_ conj host)
                    (f nil host nil))
                  resp/rcmd (fn [& _] nil)
                  resp/with-replies
                  (fn [in _out _reply-opts _as-vec? body-fn]
                    (body-fn)
                    (get replies in))]
      [(is (= (#'sentinel/resolve-addr! spec "my-master"
                {:prefer-read-replica? false} false)
              ["healthy-master" 6381]))
       (is (= @calls_ ["sentinel-a" "sentinel-b" "healthy-master"])
         "A malformed master report does not prevent consulting later Sentinels")
       (is (= (get-in @spec [:stats :sentinel-stats ["sentinel-a" 26379]
                             :n-invalid-replies])
             1))
       (is (= (get-in @spec [:nodes-addrs "my-master" :replicas])
             [["healthy-replica" 6382]])
         "Malformed replicas are skipped while healthy ones are retained")
       (is (= (get-in @spec [:nodes-addrs "my-master" :sentinels])
             [["sentinel-b" 26380] ["sentinel-a" 26379] ["sentinel-c" 26381]])
         "Malformed Sentinel peers are skipped while healthy ones are retained")])))

(deftest _sentinel-malformed-replica-report-preserves-cache
  (let [spec
        (sentinel/sentinel-spec {"my-master" [["sentinel-a" 26379]]}
          {:resolve-timeout-ms 1000, :retry-delay-ms 1})
        replies_
        (atom
          {"sentinel-a"
           [["healthy-master" 6381]
            [{"ip" "bad-port", "port" "0", "flags" "slave",
              "master-link-status" "ok"}
             {"ip" 17, "port" "6382", "flags" "slave",
              "master-link-status" "ok"}]
            []]
           "healthy-master" ["master"]})]
    (#'sentinel/update-addrs! spec "my-master" :replicas
      (fn [_] [["cached-replica" 6380]]))
    (with-redefs [conns/with-new-conn
                  (fn [_conn-opts host _port _master-name f]
                    (f nil host nil))
                  resp/rcmd (fn [& _] nil)
                  resp/with-replies
                  (fn [in _out _reply-opts _as-vec? body-fn]
                    (body-fn)
                    (get @replies_ in))]
      [(is (= (#'sentinel/resolve-addr! spec "my-master"
                {:prefer-read-replica? false} false)
              ["healthy-master" 6381]))
       (is (= (get-in @spec [:nodes-addrs "my-master" :replicas])
             [["cached-replica" 6380]])
         "An all-malformed non-empty report is not an authoritative cache clear")

       (reset! replies_
         {"sentinel-a"
          [["healthy-master" 6381]
           [{"ip" "down-replica", "port" "6382", "flags" "slave,s_down",
             "master-link-status" "ok"}]
           []]
          "healthy-master" ["master"]})
       (is (= (#'sentinel/resolve-addr! spec "my-master"
                {:prefer-read-replica? false} false)
              ["healthy-master" 6381]))
       (is (= (get-in @spec [:nodes-addrs "my-master" :replicas]) [])
         "A well-formed but entirely unhealthy report authoritatively clears the cache")])))

(deftest _sentinel-resolve-single-flight
  (let [resolved-at_ (atom {})
        locks_       (atom {})
        calls_       (atom 0)
        started      (promise)
        release      (promise)
        resolve-fn
        (fn []
          (swap! calls_ inc)
          (deliver started true)
          @release
          (swap! resolved-at_ assoc "master" (System/nanoTime)))
        futures
        (doall
          (repeatedly 8
            #(future
               (#'sentinel/ensure-fresh-resolve!
                 resolved-at_ locks_ "master" 5000 resolve-fn))))]
    (is (true? (deref started 2000 false)))
    (deliver release true)
    (doseq [f futures] (is (not= (deref f 2000 ::timeout) ::timeout)))
    (is (= @calls_ 1)
      "Concurrent stale checks share one successful topology refresh")))

(deftest _sentinel-resolve-coalescing
  ;; A herd of concurrent full resolves (e.g. pool refill after failover)
  ;; coalesces onto ONE resolution attempt; waiters reuse its outcome.
  (testing "Success herd"
    (let [spec (sentinel/sentinel-spec {"my-master" [["sentinel-a" 26379]]}
                 {:resolve-timeout-ms 4000, :retry-delay-ms 1})
          release_     (promise)
          query-count_ (atom 0)
          replies      {"sentinel-a"     [["healthy-master" 6381] [] []]
                        "healthy-master" ["master"]}]
      (with-redefs [conns/with-new-conn
                    (fn [_conn-opts host _port _master-name f]
                      (when (= host "sentinel-a")
                        (swap! query-count_ inc)
                        (deref release_ 4000 nil))
                      (f nil host nil))
                    resp/rcmd (fn [& _] nil)
                    resp/with-replies
                    (fn [in _out _reply-opts _as-vec? body-fn]
                      (body-fn)
                      (get replies in))]
        (let [futures
              (doall
                (repeatedly 8
                  #(future
                     (#'sentinel/resolve-addr! spec "my-master"
                       {:prefer-read-replica? false} false))))]
          (loop [n 0] ; Owner in flight
            (when (and (zero? @query-count_) (< n 400))
              (Thread/sleep 5) (recur (inc n))))
          (Thread/sleep 100) ; Let waiters queue on the resolve lock
          (deliver release_ true)
          [(is (every? #(= (deref % 5000 ::timeout) ["healthy-master" 6381])
                 futures))
           (is (= @query-count_ 1)
             "All concurrent resolves share one Sentinel query sweep")
           (is (= (get-in @spec [:stats :resolve-stats "my-master" :n-coalesced]) 7))]))))

  (testing "Success herd against a WARMED spec"
    ;; Regression: completion records must have fresh identity per resolution
    ;; (a constant map like {:error nil} would make waiters see "no change"
    ;; after the first success and destroy coalescing forever after)
    (let [spec (sentinel/sentinel-spec {"my-master" [["sentinel-a" 26379]]}
                 {:resolve-timeout-ms 4000, :retry-delay-ms 1})
          release_     (promise)
          query-count_ (atom 0)
          replies      {"sentinel-a"     [["healthy-master" 6381] [] []]
                        "healthy-master" ["master"]}]
      (with-redefs [conns/with-new-conn
                    (fn [_conn-opts host _port _master-name f]
                      (when (= host "sentinel-a")
                        (swap! query-count_ inc)
                        (when (> @query-count_ 1) ; Only gate the 2nd (herd) resolve
                          (deref release_ 4000 nil)))
                      (f nil host nil))
                    resp/rcmd (fn [& _] nil)
                    resp/with-replies
                    (fn [in _out _reply-opts _as-vec? body-fn]
                      (body-fn)
                      (get replies in))]
        ;; Warm: one completed successful resolution
        (is (= (#'sentinel/resolve-addr! spec "my-master"
                 {:prefer-read-replica? false} false)
              ["healthy-master" 6381]))
        (let [futures
              (doall
                (repeatedly 8
                  #(future
                     (#'sentinel/resolve-addr! spec "my-master"
                       {:prefer-read-replica? false} false))))]
          (loop [n 0] ; Herd owner in flight
            (when (and (< @query-count_ 2) (< n 400))
              (Thread/sleep 5) (recur (inc n))))
          (Thread/sleep 100) ; Let waiters queue on the resolve lock
          (deliver release_ true)
          [(is (every? #(= (deref % 5000 ::timeout) ["healthy-master" 6381])
                 futures))
           (is (= @query-count_ 2)
             "A second herd still coalesces onto one fresh resolution")]))))

  (testing "Replica waiters reuse the exact ROLE-confirmed address"
    (let [spec (sentinel/sentinel-spec {"my-master" [["sentinel-a" 26379]]}
                 {:resolve-timeout-ms 4000, :retry-delay-ms 1})
          release_     (promise)
          query-count_ (atom 0)
          replies
          {"sentinel-a"
           [["healthy-master" 6381]
            [{"ip" "replica-1", "port" "6382", "flags" "slave",
              "master-link-status" "ok"}
             {"ip" "replica-2", "port" "6383", "flags" "slave",
              "master-link-status" "ok"}]
            []]
           "replica-1" ["slave" "healthy-master" 6381 "connected"]}]
      (with-redefs [shuffle identity
                    rand-int (constantly 1)
                    conns/with-new-conn
                    (fn [_conn-opts host _port _master-name f]
                      (when (= host "sentinel-a")
                        (swap! query-count_ inc)
                        (deref release_ 4000 nil))
                      (f nil host nil))
                    resp/rcmd (fn [& _] nil)
                    resp/with-replies
                    (fn [in _out _reply-opts _as-vec? body-fn]
                      (body-fn)
                      (get replies in))]
        (let [futures
              (doall
                (repeatedly 8
                  #(future
                     (#'sentinel/resolve-addr! spec "my-master"
                       {:prefer-read-replica? true} false))))]
          (support/await-pred! 2000 "the owning Sentinel query"
            #(pos? @query-count_))
          (Thread/sleep 100)
          (deliver release_ true)
          [(is (every? #(= (deref % 5000 ::timeout) ["replica-1" 6382])
                 futures)
             "Waiters cannot randomly select a different, unverified cached replica")
           (is (= @query-count_ 1))]))))

  (testing "Master and replica selection modes do not share outcomes"
    (let [spec (sentinel/sentinel-spec {"my-master" [["sentinel-a" 26379]]}
                 {:resolve-timeout-ms 4000, :retry-delay-ms 1})
          release_     (promise)
          query-count_ (atom 0)
          replies
          {"sentinel-a"
           [["healthy-master" 6381]
            [{"ip" "replica-1", "port" "6382", "flags" "slave",
              "master-link-status" "ok"}]
            []]
           "healthy-master" ["master"]
           "replica-1"      ["slave" "healthy-master" 6381 "connected"]}]
      (with-redefs [shuffle identity
                    conns/with-new-conn
                    (fn [_conn-opts host _port _master-name f]
                      (when (= host "sentinel-a")
                        (let [n (swap! query-count_ inc)]
                          (when (= n 1) (deref release_ 4000 nil))))
                      (f nil host nil))
                    resp/rcmd (fn [& _] nil)
                    resp/with-replies
                    (fn [in _out _reply-opts _as-vec? body-fn]
                      (body-fn)
                      (get replies in))]
        (let [master-future
              (future
                (#'sentinel/resolve-addr! spec "my-master"
                  {:prefer-read-replica? false} false))]
          (support/await-pred! 2000 "the master-preferring Sentinel query"
            #(= @query-count_ 1))
          (let [replica-future
                (future
                  (#'sentinel/resolve-addr! spec "my-master"
                    {:prefer-read-replica? true} false))]
            (Thread/sleep 100)
            (deliver release_ true)
            [(is (= (deref master-future 5000 ::timeout) ["healthy-master" 6381]))
             (is (= (deref replica-future 5000 ::timeout) ["replica-1" 6382]))
             (is (= @query-count_ 2)
               "Each selection mode performs its own ROLE-confirmed resolution")])))))

  (testing "Failure herd"
    (let [spec (sentinel/sentinel-spec {"my-master" [["sentinel-a" 26379]]}
                 {:resolve-timeout-ms 300, :retry-delay-ms 50})
          release_     (promise)
          query-count_ (atom 0)]
      (with-redefs [conns/with-new-conn
                    (fn [& _]
                      (swap! query-count_ inc)
                      (deref release_ 8000 nil)
                      (throw (Exception. "Simulated unreachable")))]
        (let [futures
              (doall
                (repeatedly 4
                  #(future
                     (try
                       (#'sentinel/resolve-addr! spec "my-master"
                         {:prefer-read-replica? false} false)
                       (catch Throwable t t)))))]
          (loop [n 0]
            (when (and (zero? @query-count_) (< n 400))
              (Thread/sleep 5) (recur (inc n))))
          (Thread/sleep 100)
          (deliver release_ true)
          (let [errors (mapv #(deref % 5000 ::timeout) futures)
                {coalesced true, owned nil}
                (group-by #(:coalesced? (ex-data %)) errors)]
            [(is (every? #(= (:eid (ex-data %)) :carmine.sentinel/resolve-timeout)
                   errors)
               "Waiters inherit the owning attempt's error eid")
             (is (= (count owned) 1)
               "Exactly one caller ran (and failed) the actual resolution")
             (is (= (count coalesced) 3))
             (is (every? #(some? (ex-cause %)) coalesced)
               "Coalesced failures keep the original error as cause")]))))))

(deftest _resolve-addr-cache
  (let [spec (sentinel/sentinel-spec {"my-master" [["unreachable" 26379]]})]
    (#'sentinel/update-addrs! spec "my-master" :master
      (fn [_] ["master" 6379]))
    (#'sentinel/update-addrs! spec "my-master" :replicas
      (fn [_] [["replica" 6380]]))

    [(is (= (#'sentinel/resolve-addr! spec "my-master"
              {:prefer-read-replica? false} true)
            ["master" 6379]))

     (is (= (#'sentinel/resolve-addr! spec "my-master"
              {:prefer-read-replica? true} true)
            ["replica" 6380]))

     (is (->> (#'sentinel/resolve-addr!
                (sentinel/sentinel-spec {"my-master" [["unreachable" 26379]]}
                  {:retry-delay-ms 1, :resolve-timeout-ms 0})
                "my-master" {:prefer-read-replica? false} true)
           (throws? :ex-info {:eid :carmine.sentinel/resolve-timeout}))
       "Cache miss with `use-cache?` falls back to full resolution")]))

(deftest _sentinel-error-replies
  ;; An error reply (e.g. "-ERR No such master with that name") from a
  ;; Sentinel must be recorded and resolution must continue/fail normally,
  ;; never crash resolution itself.
  (let [err (truss/throws
              (#'sentinel/resolve-addr!
                (sentinel/sentinel-spec {"my-master" [["error-reply" 26379]]}
                  {:retry-delay-ms 1, :resolve-timeout-ms 100})
                "my-master" {:prefer-read-replica? false} false))
        data (ex-data err)]
    [(is (= (:eid data) :carmine.sentinel/resolve-timeout)
       "All-error-replies resolution fails with the normal timeout error")
     (is (pos? (get-in data [:sentinel-errors ["error-reply" 26379] :error-reply] 0))
       "Error replies are recorded in resolution error counts")
     (is (some #(and (map? %) (= (:error %) :error-reply)) (:attempt-log data))
       "Error replies are recorded in the attempt log")]))

(deftest _sentinel-node-conn-opts
  ;; Mixed-auth deployments: data-node ROLE checks may use different
  ;; conn-opts (e.g. auth) than the Sentinel queries themselves.
  (let [conns_ (atom [])
        spec
        (sentinel/sentinel-spec
          {"my-master" [["sentinel-a" 26379]]}
          {:resolve-timeout-ms 1000, :retry-delay-ms 1
           :node-conn-opts {:init {:auth {:username "node-user"
                                          :password "node-pass"}}}})
        replies
        {"sentinel-a"     [["healthy-master" 6381] [] []]
         "healthy-master" ["master"]}]
    (with-redefs [conns/with-new-conn
                  (fn [conn-opts host _port _master-name f]
                    (swap! conns_ conj
                      [host (get-in conn-opts [:init :auth :username])])
                    (f nil host nil))
                  resp/rcmd (fn [& _] nil)
                  resp/with-replies
                  (fn [in _out _reply-opts _as-vec? body-fn]
                    (body-fn)
                    (get replies in))]
      [(is (= (#'sentinel/resolve-addr! spec "my-master"
                {:prefer-read-replica? false} false)
              ["healthy-master" 6381]))
       (is (= @conns_
             [["sentinel-a" nil] ["healthy-master" "node-user"]])
         "Sentinel queries use `:conn-opts`; ROLE checks use `:node-conn-opts`")]))

  (let [parsed
        (opts/parse-sentinel-spec-opts
          {:conn-opts      {:socket-opts {:connect-timeout-ms 123}}
           :node-conn-opts {:init {:auth {:username "u", :password "p"}}}})]
    [(is (= (get-in parsed [:node-conn-opts :socket-opts :connect-timeout-ms]) 123)
       "`:node-conn-opts` is deeply merged OVER `:conn-opts`")
     (is (= (get-in parsed [:node-conn-opts :init :auth :username]) "u"))
     (is (nil? (get-in parsed [:conn-opts :init :auth :username]))
       "`:node-conn-opts` never leaks back into `:conn-opts`")])

  (doseq [empty-override [nil {}]]
    (is (truss/submap?
          (opts/parse-sentinel-spec-opts
            {:conn-opts      {:socket-opts {:connect-timeout-ms 123}}
             :node-conn-opts empty-override})
          {:node-conn-opts :submap/nx})
      "A nil/empty `:node-conn-opts` is removed so ROLE conns inherit `:conn-opts`")))

(deftest _sentinel-learned-mid-resolve
  ;; A Sentinel address learned during one (failed) resolution round is
  ;; attempted in the next round of the SAME resolution.
  (let [calls_ (atom [])
        spec
        (sentinel/sentinel-spec
          {"my-master" [["sentinel-a" 26379]]}
          {:resolve-timeout-ms 2000, :retry-delay-ms 1})
        replies
        {;; sentinel-a reports a bogus master (fails ROLE check) but ALSO
         ;; reports sentinel-c
         "sentinel-a"
         [["stale-master" 6379] []
          [{"ip" "sentinel-c", "port" "26381"}]]
         "stale-master" ["slave"]
         "sentinel-c"     [["healthy-master" 6381] [] []]
         "healthy-master" ["master"]}]
    (with-redefs [conns/with-new-conn
                  (fn [_conn-opts host _port _master-name f]
                    (swap! calls_ conj host)
                    (f nil host nil))
                  resp/rcmd (fn [& _] nil)
                  resp/with-replies
                  (fn [in _out _reply-opts _as-vec? body-fn]
                    (body-fn)
                    (get replies in))]
      [(is (= (#'sentinel/resolve-addr! spec "my-master"
                {:prefer-read-replica? false} false)
              ["healthy-master" 6381]))
       (is (some #{"sentinel-c"} @calls_)
         "The newly-learned Sentinel is consulted before resolution gives up")])))

(deftest _update-addrs-error-isolation
  ;; An update fn that throws (e.g. on a malformed reported address) must
  ;; leave existing state untouched and must never poison later reads.
  (let [spec (sentinel/sentinel-spec {"my-master" [["sentinel" 26379]]})]
    [(is (throws? (#'sentinel/update-addrs! spec "my-master" :master
                    (fn [_] (throw (Exception. "Malformed address"))))))
     (is (= (get-in @spec [:nodes-addrs "my-master" :sentinels])
           [["sentinel" 26379]])
       "A throwing update leaves state untouched")
     (is (#'sentinel/update-addrs! spec "my-master" :master
           (fn [_] ["master" 6379]))
       "The spec remains fully usable after a throwing update")
     (is (= (#'sentinel/resolve-addr! spec "my-master"
              {:prefer-read-replica? false} true)
           ["master" 6379]))]))

(deftest _sentinel-responder-promotion
  ;; The Sentinel that answered a successful resolution is promoted to the
  ;; front of the shared list for future resolves (pure reorder, no
  ;; `:on-changed-sentinels` cb).
  (let [events_ (atom [])
        spec
        (sentinel/sentinel-spec
          {"my-master" [["ignorant" 26379] ["sentinel-b" 26380]]}
          {:resolve-timeout-ms 1000, :retry-delay-ms 1
           :cbs {:on-changed-sentinels #(swap! events_ conj %)}})
        replies
        {"sentinel-b"     [["healthy-master" 6381] [] []]
         "healthy-master" ["master"]}]
    (with-redefs [conns/with-new-conn
                  (fn [_conn-opts host _port _master-name f]
                    (f nil host nil))
                  resp/rcmd (fn [& _] nil)
                  resp/with-replies
                  (fn [in _out _reply-opts _as-vec? body-fn]
                    (body-fn)
                    (get replies in))]
      [(is (= (#'sentinel/resolve-addr! spec "my-master"
                {:prefer-read-replica? false} false)
              ["healthy-master" 6381]))
       (is (= (get-in @spec [:nodes-addrs "my-master" :sentinels])
             [["sentinel-b" 26380] ["ignorant" 26379]])
         "The responding Sentinel moves to the front of the list")
       (is (= @events_ [])
         "A pure reorder fires no `:on-changed-sentinels` callback")])))

(deftest _sentinel-resolution-deadline
  (let [conn-opts {:socket-opts
                   {:connect-timeout-ms 200
                    :read-timeout-ms    0
                    :ready-timeout-ms   nil}}
        capped
        (with-redefs-fn
          {#'utils/remaining-timeout-ms (constantly 17)}
          #(utils/conn-opts-before-deadline conn-opts ::deadline))]
    (is (truss/submap? (:socket-opts capped)
          {:connect-timeout-ms 17
           :read-timeout-ms    17
           :ready-timeout-ms   17})
      "Configured and unbounded socket timeouts are capped by remaining budget"))

  (let [calls_ (atom 0)
        spec   (sentinel/sentinel-spec {"my-master" [["sentinel" 26379]]}
                 {:resolve-timeout-ms 0})
        error
        (with-redefs-fn
          {#'conns/with-new-conn
           (fn [& _] (swap! calls_ inc) (throw (Exception. "Unexpected I/O")))}
          #(try
             (#'sentinel/resolve-addr! spec "my-master"
               {:prefer-read-replica? false} false)
             (catch Throwable t t)))]
    [(is (= (:eid (ex-data error)) :carmine.sentinel/resolve-timeout))
     (is (zero? @calls_) "A zero aggregate timeout performs no network I/O")
     (is (zero? (:n-attempts (ex-data error))))])

  (let [calls_    (atom [])
        remaining_ (atom [20 20 nil nil])
        spec
        (sentinel/sentinel-spec
          {"my-master" [["sentinel-a" 26379] ["sentinel-b" 26380]]}
          {:resolve-timeout-ms 1000})
        error
        (with-redefs-fn
          {#'utils/remaining-timeout-ms
           (fn [_]
             (let [remaining (first @remaining_)]
               (swap! remaining_ next)
               remaining))
           #'conns/with-new-conn
           (fn [conn-opts host port _master-name _f]
             (swap! calls_ conj [[host port] (:socket-opts conn-opts)])
             (throw (Exception. "Unavailable")))}
          #(try
             (#'sentinel/resolve-addr! spec "my-master"
               {:prefer-read-replica? false} false)
             (catch Throwable t t)))]
    [(is (= (:eid (ex-data error)) :carmine.sentinel/resolve-timeout))
     (is (= (mapv first @calls_) [["sentinel-a" 26379]])
       "An exhausted sweep never advances to the next Sentinel")
     (is (truss/submap? (second (first @calls_))
           {:connect-timeout-ms 20
            :read-timeout-ms    20
            :ready-timeout-ms   20}))]))

(deftest _sentinel-resolution-interruption
  (testing "Interruptions from discovery workers reach the caller"
    (doseq [phase [:sentinel-query :role-check]]
      (let [spec
            (sentinel/sentinel-spec {"my-master" [["sentinel-a" 26379]]}
              {:resolve-timeout-ms 1000, :retry-delay-ms 1})
            replies
            {"sentinel-a"     [["healthy-master" 6381] [] []]
             "healthy-master" ["master"]}
            outcome
            (try
              (with-redefs-fn
                {#'conns/with-new-conn
                 (fn [_conn-opts host _port _master-name f]
                   (if (or (= phase :sentinel-query)
                         (= host "healthy-master"))
                     (throw (InterruptedException. (name phase)))
                     (f nil host nil)))
                 #'resp/rcmd (fn [& _] nil)
                 #'resp/with-replies
                 (fn [in _out _reply-opts _as-vec? body-fn]
                   (body-fn)
                   (get replies in))}
                #(do
                   (#'sentinel/resolve-addr! spec "my-master"
                     {:prefer-read-replica? false} false)
                   {:error nil, :interrupted? false}))
              (catch InterruptedException t
                {:error t
                 :interrupted? (.isInterrupted (Thread/currentThread))})
              (finally
                ;; Never leave the clojure.test runner thread interrupted.
                (Thread/interrupted)))]
        [(is (instance? InterruptedException (:error outcome)) (name phase))
         (is (:interrupted? outcome)
           (str (name phase) " restores the caller's interrupt status"))])))

  (testing "Retry sleep interruption terminates resolution promptly"
    (let [spec
          (sentinel/sentinel-spec {"my-master" [["ignorant" 26379]]}
            {:resolve-timeout-ms 20000, :retry-delay-ms 10000})
          outcome (promise)
          caller
          (doto
            (Thread.
              ^Runnable
              (fn []
                (try
                  (deliver outcome
                    {:result
                     (#'sentinel/resolve-addr! spec "my-master"
                       {:prefer-read-replica? false} false)})
                  (catch Throwable t
                    (deliver outcome
                      {:error t
                       :interrupted? (.isInterrupted (Thread/currentThread))})))))
            (.setDaemon true))]
      (try
        (.start caller)
        (support/await-pred! 2000 "the first Sentinel retry attempt"
          #(pos? (get-in @spec [:stats :resolve-stats "my-master" :n-attempts] 0)))
        (Thread/sleep 25)
        (.interrupt caller)
        (let [{:keys [error interrupted?] :as result}
              (deref outcome 2000 ::timeout)]
          [(is (not= result ::timeout))
           (is (instance? InterruptedException error))
           (is interrupted? "The caller's interrupt status is restored")])
        (finally
          (.interrupt caller)
          (.join caller 2000)
          (is (not (.isAlive caller))
            "The interrupted resolution caller always terminates"))))))

(deftest _sentinel-state-changes
  (let [global-events_ (atom [])
        mgr-events_    (atom [])
        local-events_  (atom [])
        cbids     [:on-changed-master :on-changed-replicas :on-changed-sentinels]
        callbacks (fn [events_] (zipmap cbids (repeat #(swap! events_ conj %))))
        local-cbs (callbacks local-events_)
        spec
        (sentinel/sentinel-spec
          {:my-master [["sentinel-1" 26379]]}
          {:cbs local-cbs})]

    (binding [car/*conn-cbs*     (callbacks global-events_)
              sentinel/*mgr-cbs* (callbacks    mgr-events_)]
      [(is         (#'sentinel/update-addrs! spec "my-master" :master    (fn [_] ["master" 6379])))
       (is (false? (#'sentinel/update-addrs! spec "my-master" :master    (fn [_] ["master" 6379]))))
       (is         (#'sentinel/update-addrs! spec "my-master" :replicas  (fn [_] [["replica-1" 6380] ["replica-2" 6381]])))
       (is (false? (#'sentinel/update-addrs! spec "my-master" :replicas  (fn [_] [["replica-2" 6381] ["replica-1" 6380]]))))
       (is         (#'sentinel/update-addrs! spec "my-master" :sentinels (fn [old] (conj old ["sentinel-2" 26380]))))])

    [(is (= @global-events_ @mgr-events_ @local-events_) "Global, manager, and local callbacks receive identical changes")
     (is (= (mapv :cbid    @local-events_) cbids))
     (is (every? #(= (:via %) 'update-addrs!) @local-events_))
     (is (= (mapv :changed @local-events_)
           [{:old nil, :new ["master" 6379]}
            {:old nil, :new [["replica-1" 6380] ["replica-2" 6381]]}
            {:old [["sentinel-1" 26379]]
             :new [["sentinel-1" 26379] ["sentinel-2" 26380]]}]))

     (is (every? #(= (:master-name %) "my-master")       @local-events_))
     (is (every? #(identical? (:sentinel-spec %) spec)   @local-events_))
     (is (every? #(true? (get-in % [:sentinel-spec-opts :update-replicas?])) @local-events_))
     (is (= (get-in @spec [:stats :node-counts]) {:masters 1, :replicas 2, :sentinels 2}))
     (is (= (get-in @spec [:stats :resolve-stats "my-master"])
           {:n-changes-to-master    1
            :n-changes-to-replicas  1
            :n-changes-to-sentinels 1}))

     (is (=   (#'sentinel/resolved-addr? spec "my-master" {:prefer-read-replica? false} true ["master"    6379]) :master))
     (is (=   (#'sentinel/resolved-addr? spec "my-master" {:prefer-read-replica? false} true ["replica-1" 6380]) nil))
     (is (=   (#'sentinel/resolved-addr? spec "my-master" {:prefer-read-replica?  true} true ["replica-1" 6380]) :replica))
     (is (->> (#'sentinel/resolve-addr!  spec "my-master" {:prefer-read-replica?  true} true) (contains? #{["replica-1" 6380] ["replica-2" 6381]})))]

    (is    (#'sentinel/update-addrs! spec "my-master" :replicas (fn [_] [])))
    (is (= (#'sentinel/resolve-addr! spec "my-master" {:prefer-read-replica? true} true) ["master" 6379])
      "Replica preference falls back to the cached master")))

(deftest _sentinel-discovery-is-a-shared-superset
  (let [spec (sentinel/sentinel-spec {"my-master" [["sentinel" 26379]]})
        commands_ (atom [])
        replies_
        (atom
          [[["master" 6379]
            [{"ip" "replica", "port" "6380", "flags" "slave",
              "master-link-status" "ok"}]
            []]
           ["master"]])]
    (with-redefs [conns/with-new-conn
                  (fn [_conn-opts _host _port _master-name f]
                    (f nil nil nil))
                  resp/rcmd
                  (fn [& args]
                    (swap! commands_ conj args)
                    nil)
                  resp/with-replies
                  (fn [_in _out _reply-opts _as-vec? body-fn]
                    (body-fn)
                    (let [reply (first @replies_)]
                      (swap! replies_ subvec 1)
                      reply))]
      (is (= (#'sentinel/resolve-addr! spec "my-master"
               {:prefer-read-replica? false} false)
            ["master" 6379]))
      (is (some #{["SENTINEL" "replicas" "my-master"]} @commands_)
        "Master selection does not suppress shared replica discovery")
      (is (= (#'sentinel/resolve-addr! spec "my-master"
               {:prefer-read-replica? true} true)
            ["replica" 6380])
        "A second manager can select a replica discovered by the first")

      (reset! replies_ [[["master" 6379] [] []] ["master"]])
      (is (= (#'sentinel/resolve-addr! spec "my-master"
               {:prefer-read-replica? false} false)
            ["master" 6379]))
      (is (= (#'sentinel/resolve-addr! spec "my-master"
               {:prefer-read-replica? true} true)
            ["master" 6379])
        "An authoritative empty replica reply clears stale shared replicas"))))

(deftest _sentinel-manager-repeated-master-clears
  (let [master-name   "my-master"
        spec          (sentinel/sentinel-spec {master-name [["sentinel.redis" 26379]]})
        captured-cbs_ (atom nil)
        real-new-conn @#'conns/new-conn]
    (with-redefs-fn
      {#'conns/new-conn
       (fn
         ([conn-opts]
          (reset! captured-cbs_ sentinel/*mgr-cbs*)
          (real-new-conn (assoc conn-opts :server ["127.0.0.1" 6379])))
         ([conn-opts t0 resolved-master-name host port]
          (real-new-conn conn-opts t0 resolved-master-name host port)))}
      (fn []
        (with-open [mgr
                    (conns/conn-manager-pooled
                      {:conn-opts
                       {:server {:master-name master-name, :sentinel-spec spec}}
                       :pool-opts
                       {:test-on-create? false, :test-on-borrow? false
                        :test-while-idle? false}})]
          (is (= (wcar mgr (resp/ping)) "PONG"))
          (let [on-changed-master (:on-changed-master @captured-cbs_)]
            (is (fn? on-changed-master))
            (let [before (get-in @mgr [:stats :counts :cleared])
                  _ (dotimes [_ 3]
                      (on-changed-master {:master-name master-name}))
                  after-matching (get-in @mgr [:stats :counts :cleared])
                  _ (on-changed-master {:master-name "another-master"})
                  after-other (get-in @mgr [:stats :counts :cleared])]
              [(is (= (- after-matching before) 3)
                 "Every matching master transition clears stale pooled connections")
               (is (= after-other after-matching)
                 "Changes for another Sentinel master do not clear this manager")])))))))

(deftest _sentinel-error-callbacks
  (let [global-events_ (atom [])
        mgr-events_    (atom [])
        local-events_  (atom [])
        callback       (fn [events_] #(swap! events_ conj %))
        spec            (sentinel/sentinel-spec {:my-master []}
                          {:cbs {:on-resolve-error (callback local-events_)}})]

    (binding [car/*conn-cbs*     {:on-resolve-error (callback global-events_)}
              sentinel/*mgr-cbs* {:on-resolve-error (callback mgr-events_)}]
      (is (->> (#'sentinel/resolve-addr! spec "my-master"
                  {:prefer-read-replica? false} false)
            (throws? :ex-info {:eid :carmine.sentinel/no-sentinel-addrs-in-spec}))))

    [(is (= @global-events_ @mgr-events_ @local-events_))
     (is (truss/submap? (first @local-events_)
           {:cbid :on-resolve-error
            :eid :carmine.sentinel/no-sentinel-addrs-in-spec
            :via 'resolve-addr!
            :master-name "my-master"}))
     (is (identical? (:sentinel-spec (first @local-events_)) spec))
     (is (= (get-in @spec [:stats :resolve-stats "my-master" :n-errors]) 1))]))
