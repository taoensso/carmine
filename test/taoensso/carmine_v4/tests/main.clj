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
;; - Test conns
;;   - Callbacks, closing data, etc.
;;   - Sentinel, resolve changes

;;;; Setup, etc.

(defn tk  "Test key" [key] (str "__:carmine:test:" (enc/as-qname key)))
(def  tc  "Unparsed test conn-opts" {})
(def  tc+ "Parsed   test conn-opts" (opts/parse-conn-opts false tc))

(defonce mgr_ (delay (conns/conn-manager-pooled {:conn-opts tc})))

(let [delete-test-keys
      (fn []
        (when-let [ks (seq (wcar mgr_ (resp/rcall "keys" (tk "*"))))]
          (wcar mgr_ (doseq [k ks] (resp/rcall "del" k)))))]

  (test/use-fixtures :once
    (enc/test-fixtures
      {:before delete-test-keys
       :after  delete-test-keys})))

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

(deftest _parse-string-server
  [(is (= (#'opts/parse-string-server "redis://user:pass@x.y.com:9475/3") {:server ["x.y.com"     9475], :init {:auth {:username "user", :password "pass"}, :select-db 3}}))
   (is (= (#'opts/parse-string-server "redis://:pass@x.y.com.com:9475/3") {:server ["x.y.com.com" 9475], :init {:auth {                  :password "pass"}, :select-db 3}} ))
   (is (= (#'opts/parse-string-server "redis://user:@x.y.com:9475/3")     {:server ["x.y.com"     9475], :init {:auth {:username "user"                  }, :select-db 3}}))
   (is (= (#'opts/parse-string-server "rediss://user:@x.y.com:9475/3")    {:server ["x.y.com"     9475], :init {:auth {:username "user"                  }, :select-db 3},
                                                                           :socket-opts {:ssl true}}))])
(deftest _parse-conn-opts
  [(is (enc/submap? (opts/parse-conn-opts false {:server [      "127.0.0.1"       "80"]}) {:server ["127.0.0.1" 80]}))
   (is (enc/submap? (opts/parse-conn-opts false {:server {:host "127.0.0.1" :port "80"}}) {:server ["127.0.0.1" 80]}))
   (is (enc/submap? (opts/parse-conn-opts false {:server {:host "127.0.0.1" :port "80"}}) {:server ["127.0.0.1" 80]}))
   (is (enc/submap? (opts/parse-conn-opts false {:server "rediss://user:pass@x.y.com:9475/3"})
         {:server ["x.y.com" 9475], :init {:auth {:username "user", :password "pass"}, :select-db 3, :resp3? true},
          :socket-opts {:ssl true}}))

   (is (->> (opts/parse-conn-opts false {:server ^:my-meta ["127.0.0.1" "6379"]}) :server (meta) :my-meta) "Retains metadata")

   (is (->> (opts/parse-conn-opts false {:server ["127.0.0.1" "invalid-port"]})                  (throws? :any {:eid :carmine.conn-opts/invalid-server})))
   (is (->> (opts/parse-conn-opts false {:server {:host "127.0.0.1" :port "80" :invalid "foo"}}) (throws? :any {:eid :carmine.conn-opts/invalid-server})))

   (is (enc/submap?
         (opts/parse-conn-opts false
           {:server {:sentinel-spec (sentinel/sentinel-spec {:foo/bar [["127.0.0.1" 26379]]})
                     :master-name :foo/bar}})
         {:server {:master-name "foo/bar", :sentinel-opts {:retry-delay-ms 250}}}))

   (is (enc/submap?
         (opts/parse-conn-opts false
           {:server {:sentinel-spec (sentinel/sentinel-spec {:foo/bar [["127.0.0.1" 26379]]})
                     :master-name :foo/bar, :sentinel-opts {:retry-delay-ms 100}}})
         {:server {:master-name "foo/bar",  :sentinel-opts {:retry-delay-ms 100}}}))])

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
            [{"host" "host1" "port" "port1" "x1" "y1"}
             {"host" "host2" "port" "port2"}
             ["host" "host3" "port" "port3" "x2" "y2"]])

         [["host1" "port1"] ["host2" "port2"] ["host3" "port3"]]))])

;;;; Cluster

(deftest _cluster
  [(is (=                      @(cluster/cluster-key "foo")          12182))
   (is (=                      @(cluster/cluster-key "ignore{foo}")  12182))
   (is (= @(cluster/cluster-key (cluster/cluster-key "ignore{foo}")) 12182))])


;;;; Conns

(defn- test-manager [mgr_]
  (let [v   (volatile! [])
        v+ #(vswap! v conj %)]

    (with-open [mgr ^java.io.Closeable (force mgr_)]
      (v+
        (car/with-car mgr
          (fn [conn]
            [(v+ (#'conns/conn?       conn))
             (v+ (#'conns/conn-ready? conn))
             (v+ (resp/ping))
             (v+ (car/with-replies (resp/rcall "echo" "x")))])))

      [@v mgr])))

(deftest _basic-conns
  [(is (= (conns/with-new-conn tc+
            (fn [conn in out]
              [(#'conns/conn?       conn)
               (#'conns/conn-ready? conn)
               (resp/basic-ping!  in out)
               (resp/with-replies in out false false
                 (fn [] (resp/rcall "echo" "x")))]))
         [true true "PONG" "x"])
     "Unmanaged conn")

   (let [[v mgr] (test-manager (delay (conns/conn-manager-unpooled {})))]
     [(is (= [true true nil "x" "PONG"]))
      (is (enc/submap? @mgr {:ready? false, :stats {:counts {:active 0, :created 1, :failed 0}}}))])

   (let [[v mgr] (test-manager (delay (conns/conn-manager-pooled {})))]
     [(is (= [true true nil "x" "PONG"]))
      (is (enc/submap? @mgr
            {:ready? false,
             :stats {:counts {:idle 0, :returned 1, :created 1, :waiting 0, :active 0, :cleared 0,
                              :destroyed {:total 1}, :borrowed 1, :failed 0}}}))])])

(deftest _conn-manager-interrupt
  (let [mgr (conns/conn-manager-unpooled {})
        k1  (tk "tlist")
        f
        (future
          (wcar mgr
            (resp/rcalls
              ["del"   k1]
              ["lpush" k1 "x"]
              ["lpop"  k1]
              ["blpop" k1 5] ; Block for 5 secs
              )))]

    (Thread/sleep 1000) ; Wait for wcar to start but not complete
    [(is (true? (car/conn-manager-close! mgr 0 {}))) ; Interrupt pool conns
     (is (instance? java.net.SocketException (enc/ex-cause (enc/throws @f)))
       "Close with zero timeout interrupts blocking blpop")]))

(deftest _wcar-basics
  [(is (= (wcar mgr_                 (resp/ping))  "PONG"))
   (is (= (wcar mgr_ {:as-vec? true} (resp/ping)) ["PONG"]))
   (is (= (wcar mgr_ (resp/local-echo "hello")) "hello") "Local echo")

   (let [k1 (tk "k1")
         v1 (str (rand-int 1e6))]
     (is
       (= (wcar mgr_
            (resp/ping)
            (resp/rset k1 v1)
            (resp/echo (wcar mgr_ (resp/rget k1)))
            (resp/rset k1 "0"))

         ["PONG" "OK" v1 "OK"])

       "Flush triggered by `wcar` in `wcar`"))

   (let [k1 (tk "k1")
         v1 (str (rand-int 1e6))]
     (is
      (= (wcar mgr_
           (resp/ping)
           (resp/rset k1 v1)
           (resp/echo         (with-replies (resp/rget k1)))
           (resp/echo (str (= (with-replies (resp/rget k1)) v1)))
           (resp/rset k1 "0"))

        ["PONG" "OK" v1 "true" "OK"])

      "Flush triggered by `with-replies` in `wcar`"))

   (is (= (wcar mgr_ (resp/ping) (wcar mgr_))    "PONG") "Parent replies not swallowed by `wcar`")
   (is (= (wcar mgr_ (resp/ping) (with-replies)) "PONG") "Parent replies not swallowed by `with-replies`")

   (is (= (let [k1 (tk "k1")]
            (wcar mgr_
              (resp/rset k1 "v1")
              (resp/echo
                (with-replies
                  (car/skip-replies (resp/rset k1 "v2"))
                  (resp/echo
                    (with-replies (resp/rget k1)))))))
         ["OK" "v2"]))

   (is (=
         (wcar mgr_
           (resp/ping)
           (resp/echo       (first (with-replies {:as-vec? true} (resp/ping))))
           (resp/local-echo (first (with-replies {:as-vec? true} (resp/ping)))))

         ["PONG" "PONG" "PONG"])

     "Nested :as-vec")])

;;;; Benching

(deftest _benching
  (do
    (println)
    (println "Benching times (1e4 laps)...")
    (with-open [mgr-unpooled (conns/conn-manager-unpooled {})
                mgr-default  (conns/conn-manager-pooled   {})
                mgr-untested (conns/conn-manager-pooled   {:pool-opts {:test-on-create? false
                                                                       :test-on-borrow? false
                                                                       :test-on-return? false}})]

      (println "  - wcar/unpooled:" (enc/round0 (* (enc/qb 1e3 (wcar mgr-unpooled)) 10)))
      (println "  - wcar/default: " (enc/round0    (enc/qb 1e4 (wcar mgr-default))))
      (println "  - wcar/untested:" (enc/round0    (enc/qb 1e4 (wcar mgr-untested))))
      (println "  - ping/default: " (enc/round0    (enc/qb 1e4 (wcar mgr-default  (resp/ping)))))
      (println "  - ping/untested:" (enc/round0    (enc/qb 1e4 (wcar mgr-untested (resp/ping))))))))
