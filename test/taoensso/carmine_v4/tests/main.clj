(ns taoensso.carmine-v4.tests.main
  "High-level Carmine tests.
  These need an active Redis server."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test        :as test :refer [deftest testing is]]
   [taoensso.encore     :as enc  :refer [throws?]]
   [taoensso.carmine-v4 :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.sentinel :as sentinel]))

(comment
  (remove-ns      'taoensso.carmine-v4.tests.main)
  (test/run-tests 'taoensso.carmine-v4.tests.main)
  (core/run-all-carmine-tests))

;;;; TODO
;; - Interactions between systems (read-opts, parsers, etc.)
;; - Conns
;;   - Conn cbs, closing data, etc.
;;   - Sentinel, resolve changes
;;   - Confirm :mgr works correctly w/in sentinel-opts/conn-opts

;;;; Setup, etc.

(defn- tk "Test key" [key] (str "__:carmine:test:" (enc/as-qname key)))
(def ^:private tc "Test conn-opts" {})

(let [delete-test-keys
      (fn []
        (when-let [ks (seq (wcar tc (resp/redis-call "keys" (tk "*"))))]
          (wcar tc (doseq [k ks] (resp/redis-call "del" k)))))]

  (test/use-fixtures :once
    (enc/test-fixtures
      {:before delete-test-keys
       :after  delete-test-keys})))

(def ^:private mgr "Var used to hold ConnManager" nil)
(defn- mgrv! "Mutates and returns #'mgr var" [mgr]
  (alter-var-root #'mgr (fn [_] mgr)) #'mgr)

;;;;

(defn- test-conn
  "Runs basic connection tests on given open Conn."
  [conn]
  [(is    (conns/conn?       conn))
   (is    (conns/conn-ready? conn))
   (is (= (conns/with-conn   conn
            (fn [conn in out]
              [(resp/basic-ping!  in out)
               (resp/with-replies in out false false
                 (fn [] (resp/redis-call "echo" "x")))])) ["PONG" "x"]))

   (is (not (conns/conn-ready? conn)) "Closed by `with-conn`")])

(deftest _basic-conns
  [(test-conn (conns/get-conn (assoc tc :mgr nil) :parse-opts false))
   (let [mgrv (mgrv! (conns/conn-manager-unpooled {}))]
     [(test-conn (conns/get-conn (assoc tc :mgr mgrv) :parse-opts false))
      (test-conn (conns/get-conn (assoc tc :mgr mgrv) :parse-opts true))
      (enc/submap? @@mgrv {:stats {:n-created 1}})])

   (let [mgrv (mgrv! (conns/conn-manager-pooled {}))]
     [(test-conn (conns/get-conn (assoc tc :mgr mgrv) :parse-opts false))
      (test-conn (conns/get-conn (assoc tc :mgr mgrv) :parse-opts true))])])

(deftest _conn-manager-hard-shutdown
  (let [mgrv (mgrv! (conns/conn-manager-unpooled {}))
        k1   (tk "tlist")
        f    (future
               (wcar (assoc tc :mgr mgrv)
                 (resp/redis-calls
                   ["del"   k1]
                   ["lpush" k1 "x"]
                   ["lpop"  k1]
                   ["blpop" k1 5] ; Block for 5 secs
                   )))]

    (Thread/sleep 100) ; Wait for future to run
    [(is (true? (car/conn-manager-close! @mgrv {} 0))) ; Interrupt pool conns
     (is (instance? java.net.SocketException (ex-cause (enc/throws @f)))
       "Hard pool shutdown interrupts blocking blpop")]))

(deftest ^:private _wcar-basics
  [(is (= (wcar {}                 (resp/ping))  "PONG"))
   (is (= (wcar {} {:as-vec? true} (resp/ping)) ["PONG"]))

   (is (= (wcar {} (resp/local-echo "hello")) "hello") "Local echo")

   (let [k1 (tk "k1")
         v1 (str (rand-int 1e6))]
     (is
       (= (wcar {}
            (resp/ping)
            (resp/rset k1 v1)
            (resp/echo (wcar {} (resp/rget k1)))
            (resp/rset k1 "0"))

         ["PONG" "OK" v1 "OK"])

       "Flush triggered by `wcar` in `wcar`"))

   (let [k1 (tk "k1")
         v1 (str (rand-int 1e6))]
     (is
      (= (wcar {}
           (resp/ping)
           (resp/rset k1 v1)
           (resp/echo         (with-replies (resp/rget k1)))
           (resp/echo (str (= (with-replies (resp/rget k1)) v1)))
           (resp/rset k1 "0"))

        ["PONG" "OK" v1 "true" "OK"])

      "Flush triggered by `with-replies` in `wcar`"))

   (is (= (wcar {} (resp/ping) (wcar {}))      "PONG") "Parent replies not swallowed by `wcar`")
   (is (= (wcar {} (resp/ping) (with-replies)) "PONG") "Parent replies not swallowed by `with-replies`")

   (is (= (let [k1 (tk "k1")]
            (wcar {}
              (resp/rset k1 "v1")
              (resp/echo
                (with-replies
                  (car/skip-replies (resp/rset k1 "v2"))
                  (resp/echo
                    (with-replies (resp/rget k1)))))))
         ["OK" "v2"]))

   (is (=
         (wcar {}
           (resp/ping)
           (resp/echo       (first (with-replies {:as-vec? true} (resp/ping))))
           (resp/local-echo (first (with-replies {:as-vec? true} (resp/ping)))))

         ["PONG" "PONG" "PONG"])

     "Nested :as-vec")])
