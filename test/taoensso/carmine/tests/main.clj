(ns taoensso.carmine.tests.main
  (:require [clojure.string :as str]
            [expectations     :as test :refer :all]
            [taoensso.encore  :as encore]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.commands   :as commands]
            [taoensso.carmine.protocol   :as protocol]
            [taoensso.carmine.benchmarks :as benchmarks]))

(comment (test/run-tests '[taoensso.carmine.tests.main]))

(defmacro wcar* [& body] `(car/wcar {:pool {} :spec {}} ~@body))
(def tkey (partial car/key :carmine :temp :test))
(defn clean-up-tkeys! [] (when-let [ks (seq (wcar* (car/keys (tkey :*))))]
                           (wcar* (apply car/del ks))))

(defn- before-run {:expectations-options :before-run} [] (clean-up-tkeys!))
(defn- after-run  {:expectations-options :after-run}  [] (clean-up-tkeys!))

;;;; Parsers
;; Like middleware, comp order can get confusing

;; Basic parsing
(expect ["PONG" "pong" "pong" "PONG"]
  (wcar {} (car/ping) (car/parse str/lower-case (car/ping) (car/ping)) (car/ping)))

;; Cancel parsing
(expect "YoLo" (wcar {} (->> (car/echo "YoLo")
                             (car/parse nil)
                             (car/parse str/upper-case))))

;; No auto-composition: last-applied (inner) parser wins
(expect "yolo" (wcar {} (->> (car/echo "YoLo")
                             (car/parse str/lower-case)
                             (car/parse str/upper-case))))

;; Dynamic composition: prefer inner
(expect "yolo" (wcar {} (->> (car/echo "YoLo")
                             (car/parse (car/parser-comp str/lower-case
                                                         protocol/*parser*))
                             (car/parse str/upper-case))))

;; Dynamic composition: prefer outer
(expect "YOLO" (wcar {} (->> (car/echo "YoLo")
                             (car/parse (car/parser-comp protocol/*parser*
                                                         str/lower-case))
                             (car/parse str/upper-case))))

;; Dynamic composition with metadata (`return` uses metadata and auto-composes)
(expect "yolo" (wcar {} (->> (car/return "YoLo") (car/parse str/lower-case ))))

;;; Exceptions pass through by default
(expect Exception (wcar {} (->> (car/return (Exception. "boo")) (car/parse keyword))))
(expect vector?   (wcar {} (->> (do (car/return (Exception. "boo"))
                                    (car/return (Exception. "boo")))
                                (car/parse keyword))))

;; Parsers can elect to handle exceptions, even over `return`
(expect "Oh noes!" (wcar {} (->> (car/return (Exception. "boo"))
                                 (car/parse
                                  (-> #(if (instance? Exception %) "Oh noes!" %)
                                      (with-meta {:parse-exceptions? true}))))))

;; Lua (`with-replies`) errors bypass normal parsers
(expect Exception (wcar {} (->> (car/lua "invalid" {:_ :_} {}) (car/parse keyword))))
(expect encore/bytes-class ; But _do_ maintain raw parsing metadata:
  (do (wcar {} (car/set (tkey "binkey") (.getBytes "Foobar" "UTF-8")))
      (wcar {} (-> (car/lua "return redis.call('get', _:k)" {:k (tkey "binkey")} {})
                   (car/parse-raw)))))

;; Parsing passes `with-replies` barrier
(expect [["ONE" "three"] "two"]
  (let [p (promise)]
    [(wcar {} (car/echo "ONE") (car/parse str/lower-case
                                 (deliver p (car/with-replies (car/echo "TWO")))
                                 (car/echo "THREE")))
     @p]))

;;;; Request queues (experimental, for Carmine v3)
;; Pre v3, Redis command fns used to immediately trigger writes to their
;; context's io stream. v3 introduced lazy writing to allow request planning in
;; Cluster mode. This is a trade-off: we now have to be careful to realize
;; queued requests any time a nested connection occurs.

(expect ["OK" "echo"] (wcar {} (car/set (tkey "rq1") (wcar {} (car/echo "echo")))
                              (car/get (tkey "rq1"))))
(expect "rq2-val"
  (let [p (promise)]
    (wcar {} (car/del (tkey "rq2")))
    (wcar {} (car/set (tkey "rq2") "rq2-val")
          ;; Nested `wcar` here should trigger eager sending of queued writes
          ;; above (to simulate immediate-write commands):
          (deliver p (wcar {} (car/get (tkey "rq2")))))
    @p))

;;;; `atomic`

(expect Exception (car/atomic {} 1)) ; Missing multi
(expect Exception (car/atomic {} 1 (car/multi))) ; Empty multi
(expect Exception (car/atomic {} 1 (car/multi) (car/discard))) ; Like missing multi

(expect [["OK" "QUEUED"] "PONG"] (car/atomic {} 1 (car/multi) (car/ping)))
(expect [["OK" "QUEUED" "QUEUED"] ["PONG" "PONG"]]
        (car/atomic {} 1 (car/multi) (car/ping) (car/ping)))
(expect [["echo" "OK" "QUEUED"] "PONG"]
        (car/atomic {} 1 (car/return "echo") (car/multi) (car/ping)))

(expect ArithmeticException ; Nb be specific here to distinguish from other exs!
  (car/atomic {} 1
    (car/multi)
    (car/ping)
    (/ 1 0) ; Throws client-side
    (car/ping)))

(expect Exception (car/atomic {} 1
                    (car/multi)
                    (car/redis-call [:invalid]) ; Server-side error, before exec
                    (car/ping)))

;; Ignores extra multi [error] while queuing:
(expect "PONG" (-> (car/atomic {} 1 (car/multi) (car/multi) (car/ping))
                   second))

(expect "PONG" (-> (car/atomic {} 1
                     (car/multi)
                     (car/parse (constantly "foo") (car/ping)))
                   second)) ; No parsers

(expect Exception (car/atomic {} 5
                    (car/watch (tkey :watched))
                    (car/set (tkey :watched) (rand))
                    (car/multi)
                    (car/ping))) ; Contending changes to watched key

;; As above, with contending write by different connection:
(expect Exception (car/atomic {} 5
                    (car/watch (tkey :watched))
                    (wcar {} (car/set (tkey :watched) (rand)))
                    (car/multi)
                    (car/ping)))

(expect [[["OK" "OK" "QUEUED"] "PONG"] 3]
        (let [idx (atom 1)]
          [(car/atomic {} 3
             (car/watch (tkey :watched))
             (when (< @idx 3)
               (swap! idx inc)
               (car/set (tkey :watched) (rand))) ; Contend only first 2 attempts
             (car/multi)
             (car/ping))
           @idx]))

;;;; Cluster key hashing

(expect [12182 5061 4813] [(commands/keyslot "foo")
                           (commands/keyslot "bar")
                           (commands/keyslot "baz")]) ; Basic hashing

(expect (= (commands/keyslot "{user1000}.following")
           (commands/keyslot "{user1000}.followers"))) ; Both hash on "user1000"

(expect (not= (commands/keyslot "foo{}{bar}")
              (commands/keyslot "bar"))) ; Only first {}'s non-empty content used

(expect (= (commands/keyslot "foo{{bar}}")
           (commands/keyslot "{bar"))) ; Content of first {}

(expect (= (commands/keyslot "foo{bar}{zap}")
           (commands/keyslot "foo{bar}{zip}")
           (commands/keyslot "baz{bar}{zip}"))) ; All hash on "bar"

(expect (not= (commands/keyslot "foo")
              (commands/keyslot "FOO"))) ; Hashing is case sensitive

;;; Hashing works over arbitrary bin keys
(expect (= (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 20)]))
           (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 20)]))))
(expect (not= (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 20)]))
              (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 21)]))))

;;;; Bin args

(expect (seq (.getBytes "Foobar" "UTF-8"))
  (do (wcar {} (car/set (tkey "bin-arg") (.getBytes "Foobar" "UTF-8")))
      (seq (wcar {} (car/get (tkey "bin-arg"))))))

;;;; Benching

(expect (benchmarks/bench {}))
