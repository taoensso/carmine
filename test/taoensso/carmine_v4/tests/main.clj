(ns taoensso.carmine-v4.tests.main
  "High-level Carmine tests.
  These need a running Redis server."
  (:require
   [clojure.test        :as test :refer [deftest testing is]]
   [taoensso.encore     :as enc  :refer [throws?]]
   [taoensso.carmine    :as v3-core]
   [taoensso.carmine-v4          :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.utils    :as utils]
   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.cluster  :as cluster]))

(comment
  (remove-ns      'taoensso.carmine-v4.tests.main)
  (test/run-tests 'taoensso.carmine-v4.tests.main)
  (test/run-all-tests #"taoensso\.carmine-v4.*"))

;;;; TODO
;; - Interactions between systems (read-opts, parsers, etc.)
;; - Conns
;;   - Conn cbs, closing data, etc.
;;   - Sentinel, resolve changes
;;   - Confirm :mgr works correctly w/in sentinel-opts/conn-opts

;;;; Setup, etc.

(defn tk "Test key" [key] (str "__:carmine:test:" (enc/as-qname key)))
(def  tc "Test conn-opts" {})

(let [delete-test-keys
      (fn []
        (when-let [ks (seq (wcar tc (resp/redis-call "keys" (tk "*"))))]
          (wcar tc (doseq [k ks] (resp/redis-call "del" k)))))]

  (test/use-fixtures :once
    (enc/test-fixtures
      {:before delete-test-keys
       :after  delete-test-keys})))

(def  mgr   "Var used to hold `ConnManager`" nil)
(defn mgrv! "Mutates and returns #'mgr var"  [mgr]
  (alter-var-root #'mgr (fn [_] mgr)) #'mgr)

;;;; Utils

(deftest _merge-opts
  [(is (= (utils/merge-opts {:a 1 :b 1}       {:a      2})  {:a 2,       :b 1}))
   (is (= (utils/merge-opts {:a {:a1 1} :b 1} {:a {:a1 2}}) {:a {:a1 2}, :b 1}))
   (is (= (utils/merge-opts {:a {:a1 1} :b 1} {:a     nil}) {:a nil,     :b 1}))

   (is (= (utils/merge-opts {:a 1} {:a 2} {:a 3}) {:a 3}))

   (is (= (utils/merge-opts {:a 1} {:a 2} {    }) {:a 2}))
   (is (= (utils/merge-opts {:a 1} {    } {:a 3}) {:a 3}))
   (is (= (utils/merge-opts {    } {:a 2} {:a 3}) {:a 3}))])

(deftest _dissoc-utils
  [(is (= (utils/dissoc-k  {:a {:b :B :c :C :d :D}} :a  :b)     {:a {:c :C, :d :D}}))
   (is (= (utils/dissoc-ks {:a {:b :B :c :C :d :D}} :a [:b :d]) {:a {:c :C}}))])

(deftest _get-first-contained
  [(is (= (let [m {:a :A    :b :B}] (utils/get-first-contained m :q :r :a :b)) :A))
   (is (= (let [m {:a false :b :B}] (utils/get-first-contained m :q :r :a :b)) false))])

;;;; Opts

(deftest _sock-addrs
  [(is (= (opts/descr-sock-addr (opts/parse-sock-addr            "ip" "80"))  ["ip" 80                ]))
   (is (= (opts/descr-sock-addr (opts/parse-sock-addr ^:my-meta ["ip" "80"])) ["ip" 80 {:my-meta true}]))])

(deftest _parse-string-server->opts
  [(is (= (#'opts/parse-string-server->opts "redis://redistogo:pass@panga.redistogo.com:9475/0")
          {:server ["panga.redistogo.com" 9475]
           :init   {:auth {:username "redistogo", :password "pass"}
                    :select-db 0}}))])

(deftest _parse-server->opts
  [(is (= (#'opts/parse-server->opts nil ["127.0.0.1" "80"])           {:server ["127.0.0.1" 80]}))
   (is (= (#'opts/parse-server->opts nil {:ip "127.0.0.1" :port "80"}) {:server ["127.0.0.1" 80]}))
   (is (= (#'opts/parse-server->opts nil {:sentinel-spec #'opts/dummy-var, :master-name :foo/bar})
         {:server                        {:sentinel-spec #'opts/dummy-var, :master-name "foo/bar" :sentinel-opts {}}}))

   (is (->> (#'opts/parse-server->opts nil {:ip "127.0.0.1" :port "80" :invalid true})
            (throws? :any {:eid :carmine.conn-opts/invalid-server})))

   (is (= (#'opts/parse-server->opts nil "redis://redistogo:pass@panga.redistogo.com:9475/0")
          {:server ["panga.redistogo.com" 9475],
           :init {:auth {:username "redistogo", :password "pass"},
                  :select-db 0}}))])

(deftest _parse-conn-opts
  [(is (map? (#'opts/-parse-conn-opts false nil     @#'opts/ref-conn-opts)))
   (is (map? (#'opts/-parse-conn-opts false nil @#'opts/default-conn-opts)))

   (is (map? (#'opts/-parse-conn-opts true  nil     @#'opts/ref-sentinel-conn-opts)))
   (is (map? (#'opts/-parse-conn-opts true  nil @#'opts/default-sentinel-conn-opts)))

   (is (=   (#'opts/-parse-conn-opts false nil           {:server ["127.0.0.1" "6379"]}) {:server ["127.0.0.1" 6379]}))
   (is (->> (#'opts/-parse-conn-opts false nil           {:server ["127.0.0.1" "invalid-port"]}) (throws? :any)))
   (is (->> (#'opts/-parse-conn-opts false nil ^:my-meta {:server ["127.0.0.1" "6379"]}) (meta) :my-meta) "Retains metadata")])

(deftest _parse-conn-opts
  [(is (map? (opts/parse-conn-opts true  {})))
   (is (map? (opts/parse-conn-opts false {:server ["127.0.0.1" "6379"]})))
   (is (->>  (opts/parse-conn-opts false {}) (throws? :any)))])

;;;; Conns

(deftest kop-keys
  (let [kc (enc/counter 0)]
    (binding [conns/*kop-counter* kc]
      (let [conn-opts->kop-key @#'conns/conn-opts->kop-key
            kop-key->conn-opts @#'conns/kop-key->conn-opts
            result
            (-> {:server ["127.0.0.1" 6379], :socket-opts {:read-timeout-ms 1000}}
              conn-opts->kop-key kop-key->conn-opts
              conn-opts->kop-key kop-key->conn-opts
              conn-opts->kop-key kop-key->conn-opts)]

        [(is (= result {:server ["127.0.0.1" 6379], :socket-opts {:read-timeout-ms 1000}}))
         (is (= @kc 1))]))))

(defn- test-conn
  "Runs basic connection tests on given open `Conn`."
  [ready-after? conn]
  [(is    (conns/conn?       conn))
   (is    (conns/conn-ready? conn))
   (is (= (conns/with-conn   conn
            (fn [conn in out]
              [(resp/basic-ping!  in out)
               (resp/with-replies in out false false
                 (fn [] (resp/redis-call "echo" "x")))])) ["PONG" "x"]))

   (is (= (conns/conn-ready? conn) ready-after?))])

(deftest _basic-conns
  [(let [mgrv (mgrv! nil)] (test-conn false (conns/get-conn (assoc tc :mgr nil) true true)))
   (let [mgrv (mgrv! (conns/conn-manager-unpooled {}))]
     [(test-conn false (conns/get-conn (assoc tc :mgr mgrv) true true))
      (test-conn false (conns/get-conn (assoc tc :mgr mgrv) true true))
      (enc/submap? (force (get @@mgrv :stats))
        {:counts {:created 2, :active 0}})])

   (let [mgrv (mgrv! (conns/conn-manager-pooled {}))]
     [(test-conn true (conns/get-conn (assoc tc :mgr mgrv) true true))
      (test-conn true (conns/get-conn (assoc tc :mgr mgrv) true true))
      (enc/submap? (force (get @@mgrv :stats))
        {:counts {:sub-pools 0, :created 1, :borrowed 2, :returned 2}})])])

(deftest _conn-manager-hard-shutdown
  (let [mgrv (mgrv! (conns/conn-manager-unpooled {}))
        k1   (tk "tlist")
        f
        (future
          (wcar (assoc tc :mgr mgrv)
            (resp/redis-calls
              ["del"   k1]
              ["lpush" k1 "x"]
              ["lpop"  k1]
              ["blpop" k1 5] ; Block for 5 secs
              )))]

    (Thread/sleep 1000) ; Wait for wcar to start but not complete
    [(is (true? (car/conn-manager-close! @mgrv {} 0))) ; Interrupt pool conns
     (is (instance? java.net.SocketException (enc/ex-cause (enc/throws @f)))
       "Hard pool shutdown interrupts blocking blpop")]))

(deftest _wcar-basics
  [(is (= (wcar tc                 (resp/ping))  "PONG"))
   (is (= (wcar tc {:as-vec? true} (resp/ping)) ["PONG"]))
   (is (= (wcar tc (resp/local-echo "hello")) "hello") "Local echo")

   (let [k1 (tk "k1")
         v1 (str (rand-int 1e6))]
     (is
       (= (wcar tc
            (resp/ping)
            (resp/rset k1 v1)
            (resp/echo (wcar tc (resp/rget k1)))
            (resp/rset k1 "0"))

         ["PONG" "OK" v1 "OK"])

       "Flush triggered by `wcar` in `wcar`"))

   (let [k1 (tk "k1")
         v1 (str (rand-int 1e6))]
     (is
      (= (wcar tc
           (resp/ping)
           (resp/rset k1 v1)
           (resp/echo         (with-replies (resp/rget k1)))
           (resp/echo (str (= (with-replies (resp/rget k1)) v1)))
           (resp/rset k1 "0"))

        ["PONG" "OK" v1 "true" "OK"])

      "Flush triggered by `with-replies` in `wcar`"))

   (is (= (wcar tc (resp/ping) (wcar tc))      "PONG") "Parent replies not swallowed by `wcar`")
   (is (= (wcar tc (resp/ping) (with-replies)) "PONG") "Parent replies not swallowed by `with-replies`")

   (is (= (let [k1 (tk "k1")]
            (wcar tc
              (resp/rset k1 "v1")
              (resp/echo
                (with-replies
                  (car/skip-replies (resp/rset k1 "v2"))
                  (resp/echo
                    (with-replies (resp/rget k1)))))))
         ["OK" "v2"]))

   (is (=
         (wcar tc
           (resp/ping)
           (resp/echo       (first (with-replies {:as-vec? true} (resp/ping))))
           (resp/local-echo (first (with-replies {:as-vec? true} (resp/ping)))))

         ["PONG" "PONG" "PONG"])

     "Nested :as-vec")])

;;;; Sentinel

(deftest _addr-utils
  [(let [sm (#'sentinel/add-addrs->back nil [["ip1" 1] ["ip2" "2"] ^{:server-name "server3"} ["ip3" 3]])
         sm (#'sentinel/add-addr->front sm  ["ip2" 2])
         sm (#'sentinel/add-addrs->back sm [["ip3" 3] ["ip6" 6]])]

     [(is (= sm [["ip2" 2] ["ip1" 1] ["ip3" 3] ["ip6" 6]]))
      (is (= (mapv opts/descr-sock-addr sm)
            [["ip2" 2] ["ip1" 1] ["ip3" 3 {:server-name "server3"}] ["ip6" 6]]))])

   (let [sm (#'sentinel/add-addrs->back nil [["ip4" 4] ["ip5" "5"]])
         sm (#'sentinel/remove-addr     sm   ["ip4" 4])]
     [(is (= sm [["ip5" 5]]))])])

(deftest _unique-addrs
  [(is (= (#'sentinel/unique-addrs
            {:m1 {:master [1 1] :sentinels [[1 1] [1 2] [2 2]]}
             :m2 {:master [1 1] :sentinels [[3 3]] :replicas #{[1 1] [3 3]}}})

         {:masters   #{[1 1]},
          :replicas  #{[3 3] [1 1]},
          :sentinels #{[1 1] [1 2] [2 2] [3 3]}}))])

(deftest _parse-nodes-info->addrs
  [(is (= (#'sentinel/parse-nodes-info->addrs
            [{"ip" "ip1" "port" "port1" "x1" "y1"}
             {"ip" "ip2" "port" "port2"}
             ["ip" "ip3" "port" "port3" "x2" "y2"]])

         [["ip1" "port1"] ["ip2" "port2"] ["ip3" "port3"]]))])

;;;; Cluster

(deftest _cluster
  [(is (=                      @(cluster/cluster-key "foo")          12182))
   (is (=                      @(cluster/cluster-key "ignore{foo}")  12182))
   (is (= @(cluster/cluster-key (cluster/cluster-key "ignore{foo}")) 12182))])

;;;; Benching

(deftest _benching
  (println)
  (println "Benching...")
  (println "  v4 includes robust conn pool testing by default")
  [(is (= (v3-core/wcar tc (v3-core/ping)) "PONG"))
   (is (=         (wcar tc    (resp/ping)) "PONG"))

   (let [[v3-wcar v4-wcar] (enc/qb 1e3 (v3-core/wcar tc)                (wcar tc))
         [v3-pong v4-pong] (enc/qb 1e3 (v3-core/wcar tc (v3-core/ping)) (wcar tc (resp/ping)))]
     (println "  - wcar time (v3 -> v4)" v3-wcar "->" v4-wcar)
     (println "  - pong time (v3 -> v4)" v3-pong "->" v4-pong)
     true)])
