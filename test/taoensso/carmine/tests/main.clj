(ns taoensso.carmine.tests.main
  (:require [clojure.string :as str]
            [expectations     :as test :refer :all]
            [taoensso.encore  :as encore]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.commands   :as commands]
            [taoensso.carmine.protocol   :as protocol]
            [taoensso.carmine.benchmarks :as benchmarks]))

;; (remove-ns 'taoensso.carmine.tests.main)
(comment (test/run-tests '[taoensso.carmine.tests.main]))

(defmacro wcar* [& body] `(car/wcar {:pool {} :spec {}} ~@body))
(def tkey (partial car/key :carmine :temp :test))
(defn clean-up-tkeys! [] (when-let [ks (seq (wcar* (car/keys (tkey :*))))]
                           (wcar* (apply car/del ks))))

(defn- before-run {:expectations-options :before-run} [] (clean-up-tkeys!))
(defn- after-run  {:expectations-options :after-run}  [] (clean-up-tkeys!))

;;;; Migrated

(expect "Message" (wcar* (car/echo "Message")))
(expect "PONG"    (wcar* (car/ping)))

(let [k (tkey "exists")]
  (expect 0     (wcar* (car/exists k)))
  (expect 1 (do (wcar* (car/set    k "exists!"))
                (wcar* (car/exists k)))))

(let [k (tkey "server:name")]
  (expect "OK"   (wcar* (car/set k "fido")))
  (expect "fido" (wcar* (car/get k)))
  (expect 15     (wcar* (car/append k " [shutdown]")))
  (expect "fido [shutdown]" (wcar* (car/getset   k "fido [running]")))
  (expect "running"         (wcar* (car/getrange k 6 12))))

(let [k (tkey "mykey")]
  (expect 1 (do (wcar* (car/setbit k 7 1))
                (wcar* (car/getbit k 7)))))

(let [k (tkey "multiline")]
  (expect "OK" (wcar* (car/set k "Redis\r\nDemo")))
  (expect "Redis\r\nDemo" (wcar* (car/get k))))

(let [k (tkey "connections")]
  (expect 11 (do (wcar* (car/set k 10))
                 (wcar* (car/incr k))))
  (expect 20 (wcar* (car/incrby k 9)))
  (expect 19 (wcar* (car/decr k)))
  (expect 10 (wcar* (car/decrby k 9))))

(let [k (tkey "something")]
  (expect 1 (do (wcar* (car/set k "foo"))
                (wcar* (car/del k)))))

(let [k (tkey "resource:lock")]
  (expect -1 (do (wcar* (car/set k "Redis Demo"))
                 (wcar* (car/ttl k))))
  (expect 1 (wcar* (car/expire k 120)))
  (expect #(< 0 %) (wcar* (car/ttl k))))

(let [k (tkey "friends")]
  (expect ["Sam" "Tom" "Bob"]
    (do (wcar* (car/rpush k "Tom"))
        (wcar* (car/rpush k "Bob"))
        (wcar* (car/lpush k "Sam"))
        (wcar* (car/lrange k 0 -1))))
  (expect ["Sam" "Tom"] (wcar* (car/lrange k 0 1)))
  (expect ["Sam" "Tom" "Bob"] (wcar* (car/lrange k 0 2)))
  (expect 3 (wcar* (car/llen k)))
  (expect "Sam" (wcar* (car/lpop k)))
  (expect "Bob" (wcar* (car/rpop k)))
  (expect 1 (wcar* (car/llen k)))
  (expect ["Tom"] (wcar* (car/lrange k 0 -1))))

(let [k (tkey "spanish")]
  (expect ["OK" "year->año"] (wcar* (car/set k "year->año")
                                    (car/get k))))

(let [k (tkey "str-field")]
  (expect Exception (do (wcar* (car/set k "str-value"))
                        (wcar* (car/incr k))))
  (expect (some #(instance? Exception %)
            (wcar* (car/ping) (car/incr k) (car/ping)))))

(expect nil (wcar* "This is a malformed request"))
(expect "PONG" (wcar* (car/ping) "This is a malformed request"))
(expect ["PONG" "PONG" "PONG"] (wcar* (doall (repeatedly 3 car/ping))))
(expect ["A" "B" "C"] (wcar* (doall (map car/echo ["A" "B" "C"]))))

(let [out-k (tkey "outside-key")
      in-k  (tkey "inside-key")]
  (expect "inside value"
    (do (wcar* (car/set in-k  "inside value")
               (car/set out-k in-k))
        (wcar* (car/get (last (wcar* (car/ping) (car/get out-k))))))))

(let [k (tkey "parallel-key")]
  (expect "10000"
    (do (wcar* (car/set k 0))
        (->> (fn [] (future (dotimes [n 100] (wcar* (car/incr k)))))
             (repeatedly 100) ; No. of parallel clients
             (doall)
             (map deref)
             (dorun))
        (wcar* (car/get k)))))

(let [k (tkey "myhash")]
  (expect "value1" (do (wcar* (car/hset k "field1" "value1"))
                       (wcar* (car/hget k "field1"))))
  (expect "value1" (do (wcar* (car/hsetnx k "field1" "newvalue"))
                       (wcar* (car/hget k "field1"))))
  (expect 1 (wcar* (car/hexists k "field1")))
  (expect ["field1" "value1"] (wcar* (car/hgetall k)))
  (expect 3 (do (wcar* (car/hset k "field2" 1))
                (wcar* (car/hincrby k "field2" 2))))
  (expect ["field1" "field2"] (wcar* (car/hkeys k)))
  (expect ["value1" "3"] (wcar* (car/hvals k)))
  (expect 2 (wcar* (car/hlen k)))
  (expect 0 (do (wcar* (car/hdel k "field1"))
                (wcar* (car/hexists k "field1")))))

(let [k1 (tkey "superpowers")
      k2 (tkey "birdpowers")]
  (expect 1 (do (wcar* (car/sadd k1 "flight"))
                (wcar* (car/sadd k1 "x-ray vision"))
                (wcar* (car/sadd k1 "reflexes"))
                (wcar* (car/srem k1 "reflexes"))
                (wcar* (car/sismember k1 "flight"))))
  (expect 0 (wcar* (car/sismember k1 "reflexes")))
  (expect (set ["flight" "pecking" "x-ray vision"])
    (do (wcar* (car/sadd k2 "pecking"))
        (wcar* (car/sadd k2 "flight"))
        (set (wcar* (car/sunion k1 k2))))))

(let [k1 (tkey "hackers")
      k2 (tkey "slackers")
      k1-U-k2 (tkey "hackersnslackers")
      k1-I-k2 (tkey "hackerslackers")]
  (expect ["Alan Kay" "Richard Stallman" "Yukihiro Matsumoto"]
    (do
      (wcar*
        (car/zadd k1 "1940" "Alan Kay")
        (car/zadd k1 "1953" "Richard Stallman")
        (car/zadd k1 "1965" "Yukihiro Matsumoto")
        (car/zadd k1 "1916" "Claude Shannon")
        (car/zadd k1 "1969" "Linus Torvalds")
        (car/zadd k1 "1912" "Alan Turing")
        (car/zadd k1 "1972" "Dade Murphy")
        (car/zadd k1 "1970" "Emmanuel Goldstein")
        (car/zadd k2 "1968" "Pauly Shore")
        (car/zadd k2 "1972" "Dade Murphy")
        (car/zadd k2 "1970" "Emmanuel Goldstein")
        (car/zadd k2 "1966" "Adam Sandler")
        (car/zadd k2 "1962" "Ferris Beuler")
        (car/zadd k2 "1871" "Theodore Dreiser")
        (car/zunionstore* k1-U-k2 [k1 k2])
        (car/zinterstore* k1-I-k2 [k1 k2]))
      (wcar* (car/zrange k1 2 4))))
  (expect ["Claude Shannon" "Alan Kay" "Richard Stallman" "Ferris Beuler"]
    (wcar* (car/zrange k1-U-k2 2 5)))
  (expect ["Emmanuel Goldstein" "Dade Murphy"]
    (wcar* (car/zrange k1-I-k2 0 1))))

(let [k1 (tkey "children")
      k2 (tkey "favorite:child")]
  (expect ["B" "B"] (do (wcar* (car/rpush k1 "A")
                               (car/rpush k1 "B")
                               (car/rpush k1 "C")
                               (car/set   k2 "B"))
                        (wcar* (car/get k2)
                               (car/get k2))))
  (expect ["B" ["A" "B" "C"] "B"]
    (wcar* (car/get k2)
           (car/lrange k1 0 3)
           (car/get k2))))

;;; Pub/sub

(expect-let
  [received (atom [])
   listener (car/with-new-pubsub-listener
              {} {"ps-foo" #(swap! received conj %)}
              (car/subscribe "ps-foo"))]
  [["subscribe" "ps-foo" 1]
   ["message"   "ps-foo" "one"]
   ["message"   "ps-foo" "two"]
   ["message"   "ps-foo" "three"]]
  (do
    (wcar* (car/publish "ps-foo" "one")
           (car/publish "ps-foo" "two")
           (car/publish "ps-foo" "three"))
    (Thread/sleep 500)
    (car/close-listener listener)
    @received))

(expect-let
  [received (atom [])
   listener (car/with-new-pubsub-listener
              {} {"ps-foo" #(swap! received conj %)
                  "ps-baz" #(swap! received conj %)}
              (car/subscribe "ps-foo" "ps-baz"))]
  [["subscribe" "ps-foo" 1]
   ["subscribe" "ps-baz" 2]
   ["message"   "ps-foo" "one"]
   ["message"   "ps-baz" "three"]]
  (do
    (wcar* (car/publish "ps-foo" "one")
           (car/publish "ps-bar" "two")
           (car/publish "ps-baz" "three"))
    (Thread/sleep 500)
    (car/close-listener listener)
    @received))

(expect-let
  [received (atom [])
   listener (car/with-new-pubsub-listener
              {} {"ps-*"   #(swap! received conj %)
                  "ps-foo" #(swap! received conj %)})]
  [["psubscribe"  "ps-*"   1]
   ["subscribe"   "ps-foo" 2]
   ["message"     "ps-foo" "one"]
   ["pmessage"    "ps-*"   "ps-foo" "one"]
   ["pmessage"    "ps-*"   "ps-bar" "two"]
   ["pmessage"    "ps-*"   "ps-baz" "three"]
   ["unsubscribe" "ps-foo" 1]
   ["pmessage"    "ps-*"   "ps-foo" "four"]
   ["pmessage"    "ps-*"   "ps-baz" "five"]]
  (do
    (car/with-open-listener listener
      (car/psubscribe "ps-*")
      (car/subscribe  "ps-foo"))
    (wcar* (car/publish "ps-foo" "one")
           (car/publish "ps-bar" "two")
           (car/publish "ps-baz" "three"))
    (Thread/sleep 500)
    (car/with-open-listener listener
      (car/unsubscribe "ps-foo"))
    (Thread/sleep 500)
    (wcar* (car/publish "ps-foo" "four")
           (car/publish "ps-baz" "five"))
    (Thread/sleep 500)
    (car/close-listener listener)
    @received))

;;; Bin safety

(let [k  (tkey "binary-safety")
      ba (byte-array [(byte 3) (byte 1) (byte 4)])]
  (expect #(encore/ba= ba %) (do (wcar* (car/set k ba))
                                 (wcar* (car/get k)))))

;;; Nulls

(let [k (tkey "nulls")]
  (expect nil (do (wcar* (car/set k nil))
                  (wcar* (car/get k))))
  (expect "" (do (wcar* (car/set k ""))
                 (wcar* (car/get k))))
  (expect " " (do (wcar* (car/set k " "))
                  (wcar* (car/get k))))
  (expect 0 (do (wcar* (car/set k (byte-array 0)))
                (count (wcar* (car/get k)))))
  (expect 1 (do (wcar* (car/set k (.getBytes "\u0000" "UTF-8")))
                (count (wcar* (car/get k)))))
  (expect "Foobar\u0000" (do (wcar* (car/set k "Foobar\u0000"))
                             (wcar* (car/get k))))
  (expect Exception (wcar* (car/set k "\u0000Foobar"))))

;;; Big responses

(let [k (tkey "big-list")
      n 250000]
  (expect (repeat n "value")
    (do (wcar* (dorun (repeatedly n (fn [] (car/rpush k "value")))))
        (wcar* (car/lrange k 0 -1)))))

;;; Lua

(let [k (tkey "script")
      script "return redis.call('set',KEYS[1],'script-value')"
      script-hash (car/hash-script script)]
  (expect "script-value" (do (wcar* (car/script-load script))
                             (wcar* (car/evalsha script-hash 1 k))
                             (wcar* (car/get k))))
  (expect ["key1" "key2" "arg1" "arg2"]
    (wcar* (car/eval "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"
             2 "key1" "key2" "arg1" "arg2"))))

;;; Reply parsing

(let [k (tkey "reply-parsing")]
  (expect ["PONG" {:a :A :b :B :c :C :d :D} "pong" "pong" "PONG"]
    (do
      (wcar* (car/set k [:a :A :b :B :c :C :d :D]))
      (wcar* (car/ping)
             (car/with-parser #(apply hash-map %) (car/get k))
             (car/with-parser str/lower-case
               (car/ping)
               (car/ping))
             (car/ping)))))

;;; DEPRECATED `atomically` transactions

(let [k      (tkey "atomic")
      wk     (tkey "watch")
      k-val  "initial-k-value"
      wk-val "initial-wk-value"]

  ;;; This transaction will succeed
  (expect ["PONG" "OK"]
    (do (wcar* (car/set k  k-val)
               (car/set wk wk-val))
        (wcar* (car/atomically []
                 (let [wk-val (wcar* (car/get wk))]
                   (car/ping)
                   (car/set k wk-val))))))

  (expect wk-val (wcar* (car/get k)))

  ;;; This transaction will fail
  (expect nil ; []
    (do (wcar* (car/set k  k-val)
               (car/set wk wk-val))
        (wcar* (car/atomically [wk]
                 (wcar* (car/set wk "CHANGE!")) ; Will break watch
                 (car/ping)))))
  (expect k-val (wcar* (car/get k))))

;;;; with-replies

(expect ["1" "2" ["3" "4"] "5"]
  (wcar {}
    (car/echo 1)
    (car/echo 2)
    (car/return (car/with-replies (car/echo 3) (car/echo 4)))
    (car/echo 5)))

(expect ["1" "2" ["3" "4" ["5" "6"]] "7"] ; Nested `with-replies`
  (wcar {}
    (car/echo 1)
    (car/echo 2)
    (car/return (car/with-replies (car/echo 3) (car/echo 4)
                  (car/return (car/with-replies (car/echo 5) (car/echo 6)))))
    (car/echo 7)))

(expect ["A" "b" ["c" "D"] ["E" "F"] "g"] ; `with-replies` vs parsers
  (wcar {}
    (car/parse str/upper-case (car/echo :a))
    (car/echo :b)
    (car/return (car/with-replies (car/echo :c) (car/parse str/upper-case (car/echo :d))))
    (car/return
      (car/parse str/upper-case
        (car/with-replies (car/echo :e) (car/echo :f))))
    (car/echo :g)))

(expect ["a" "b" ["c" "d" ["e" "f"]] "g"] ; Nested `with-replies` vs parsers
  (wcar {}
    (car/echo :a)
    (car/echo :b)
    (car/return
      (car/parse #(cond (string? %) (str/lower-case %)
                        (vector? %) (mapv str/lower-case %))
        (car/with-replies (car/echo :c) (car/echo :d)
          (car/return (car/parse str/upper-case
                        (car/with-replies (car/echo :e) (car/echo :f)))))))
    (car/echo :g)))

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

(expect Exception    (car/atomic {} 1)) ; Missing multi
(expect [["OK"] nil] (car/atomic {} 1 (car/multi))) ; Empty multi
(expect Exception    (car/atomic {} 1 (car/multi) (car/discard))) ; Like missing multi

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

(expect ArithmeticException ; Correct (unmasked) error
  (car/atomic {} 1 (/ 1 0)))


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

;;;; compare-and-set

(expect [1 1 nil 1 1 "final-val"]
  (let [tk  (tkey "cas-k")
        cas (partial car/compare-and-set tk)]
    (wcar {}
      (cas :redis/nx 0)
      (cas 0         1)
      (cas 22        23)  ; Will be ignored
      (cas 1         nil) ; Serialized nil
      (cas nil       "final-val")
      (car/get tk))))

;;;; Benching

(expect (benchmarks/bench {}))
