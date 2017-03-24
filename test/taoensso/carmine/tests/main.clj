(ns taoensso.carmine.tests.main
  (:require
   [clojure.string   :as str]
   [clojure.test     :as test :refer [is deftest]]
   [taoensso.encore  :as enc]
   [taoensso.carmine :as car  :refer [wcar]]
   [taoensso.carmine.commands   :as commands]
   [taoensso.carmine.protocol   :as protocol]
   [taoensso.carmine.benchmarks :as benchmarks]))

(comment
  (remove-ns      'taoensso.carmine.tests.main)
  (test/run-tests 'taoensso.carmine.tests.main))

(defmacro wcar* [& body] `(car/wcar {:pool {} :spec {}} ~@body))
(def tkey (partial car/key :carmine :temp :test))
(defn clean-up-tkeys! [] (when-let [ks (seq (wcar* (car/keys (tkey :*))))]
                           (wcar* (apply car/del ks))))

(defn cleanup-fixture [f] (clean-up-tkeys!) (f) (clean-up-tkeys!))
(test/use-fixtures :once cleanup-fixture)

(deftest basic-tests
  (is (= "Message" (wcar* (car/echo "Message"))))
  (is (= "PONG" (wcar* (car/ping)))))

(deftest key-exists-test
  (let [k (tkey "exists")]
    (is (= 0 (wcar* (car/exists k)))
        "Since key does not exists should return 0")

    ;; Set key
    (wcar* (car/set k "exists!"))
    (is (= 1 (wcar* (car/exists k)))
        "Now that key is set, it should exists!")))

(deftest getset-test
  (let [k (tkey "server:name")]
    (is (= "OK"              (wcar* (car/set k "fido"))))
    (is (= "fido"            (wcar* (car/get k))))
    (is (= 15                (wcar* (car/append k " [shutdown]"))))
    (is (= "fido [shutdown]" (wcar* (car/getset k "fido [running]"))))
    (is (= "running"         (wcar* (car/getrange k 6 12))))))

(deftest setbit-test
  (let [k (tkey "mykey")]
    (wcar* (car/setbit k 7 1))
    (is (= 1 (wcar* (car/getbit k 7)))
        "7th bit of the key was set to 1")))

(deftest multiline-test
  (let [k (tkey "multiline")]
    (is (= "OK" (wcar* (car/set k "Redis\r\nDemo"))))
    (is (= "Redis\r\nDemo" (wcar* (car/get k))))))

(deftest inc-dec-tests
  (let [k (tkey "connections")]
    (wcar* (car/set k 10))
    (is (= 11 (wcar* (car/incr k))))
    (is (= 20 (wcar* (car/incrby k 9))))
    (is (= 19 (wcar* (car/decr k))))
    (is (= 10 (wcar* (car/decrby k 9))))))

(deftest delete-key-test
  (let [k (tkey "something")]
    (wcar* (car/set k "foo"))
    (is (= 1 (wcar* (car/del k))))))

(deftest expiry-tests
  (let [k (tkey "resource:lock")]
    (wcar* (car/set k "Redis Demo"))
    (is (= -1 (wcar* (car/ttl k))))
    (is (= 1  (wcar* (car/expire k 120))))
    (is (pos? (wcar* (car/ttl k))))))

(deftest array-cmds-tests
  (let [k (tkey "friends")]
    (test/testing "Push command"
      (wcar* (car/rpush k "Tom"))
      (wcar* (car/rpush k "Bob"))
      (wcar* (car/lpush k "Sam"))
      (is (= ["Sam" "Tom" "Bob"]
             (wcar* (car/lrange k 0 -1)))))

    (test/testing "lrange command"
      (is (= ["Sam" "Tom"]       (wcar* (car/lrange k 0 1))))
      (is (= ["Sam" "Tom" "Bob"] (wcar* (car/lrange k 0 2)))))

    (test/testing "len and pop commands"
      (is (= 3       (wcar* (car/llen k))))
      (is (= "Sam"   (wcar* (car/lpop k))))
      (is (= "Bob"   (wcar* (car/rpop k))))
      (is (= 1       (wcar* (car/llen k))))
      (is (= ["Tom"] (wcar* (car/lrange k 0 -1)))))))

(deftest get-set-spanish-test
  (let [k (tkey "spanish")]
    (is (= ["OK" "year->año"]
           (wcar* (car/set k "year->año") (car/get k))))))

(deftest exception-test
  (let [k (tkey "str-field")]

    (wcar* (car/set k "str-value"))
    (is (thrown? clojure.lang.ExceptionInfo
                 (wcar* (car/incr k)))
        "Can't increment a string value")

    (let [[r1 r2 r3] (wcar* (car/ping)
                            (car/incr k)
                            (car/ping))]
      (is (= "PONG" r1))
      (is (instance? clojure.lang.ExceptionInfo r2))
      (is (= "PONG" r3)))))

(deftest malformed-tests
  (is (= nil (wcar* "This is a malformed request")))
  (is (= "PONG" (wcar* (car/ping) "This is a malformed request"))))

(deftest pong-test
  (is (= ["PONG" "PONG" "PONG"]
         (wcar* (doall (repeatedly 3 car/ping))))))

(deftest echo-test
  (is (= ["A" "B" "C"]
         (wcar* (doall (map car/echo ["A" "B" "C"]))))))

(deftest key-inside-key-test
  (let [out-k (tkey "outside-key")
        in-k  (tkey "inside-key")]
    (wcar* (car/set in-k "inside value")
           (car/set out-k in-k))
    (is (= "inside value"
           (wcar* (car/get (last (wcar* (car/ping)
                                        (car/get out-k))))))
        "Should get the inner value")))

(deftest parallel-incr-test
  (let [k (tkey "parallel-key")]
    (wcar* (car/set k 0))
    (->> (fn [] (future (dotimes [n 100] (wcar* (car/incr k)))))
         (repeatedly 100)   ; No. of parallel clients
         (doall)
         (map deref)
         (dorun))
    (is (= "10000"
           (wcar* (car/get k))))))

(deftest hash-set-test
  (let [k (tkey "myhash")]

    (wcar* (car/hset k "field1" "value1"))
    (is (= "value1" (wcar* (car/hget k "field1"))))

    (wcar* (car/hsetnx k "field1" "newvalue"))
    (is (= "value1" (wcar* (car/hget k "field1"))))

    (is (= 1 (wcar* (car/hexists k "field1"))))

    (is (= ["field1" "value1"] (wcar* (car/hgetall k))))

    (wcar* (car/hset k "field2" 1))
    (is (= 3 (wcar* (car/hincrby k "field2" 2))))

    (is (= ["field1" "field2"] (wcar* (car/hkeys k))))

    (is (= ["value1" "3"] (wcar* (car/hvals k))))

    (is (= 2 (wcar* (car/hlen k))))

    (wcar* (car/hdel k "field1"))
    (is (= 0 (wcar* (car/hexists k "field1"))))))

(deftest set-tests
  (let [k1 (tkey "superpowers")
        k2 (tkey "birdpowers")]

    (test/testing "Member in set case"
      (wcar* (car/sadd k1 "flight"))
      (wcar* (car/sadd k1 "x-ray vision"))
      (wcar* (car/sadd k1 "reflexes"))
      (wcar* (car/srem k1 "reflexes"))
      (is 1 (= (wcar* (car/sismember k1 "flight")))))

    (test/testing "Member NOT in set case"
      (is (= 0 (wcar* (car/sismember k1 "reflexes")))))

    (test/testing "Set union case"
      (wcar* (car/sadd k2 "pecking"))
      (wcar* (car/sadd k2 "flight"))
      (is (= (set ["flight" "pecking" "x-ray vision"])
             (set (wcar* (car/sunion k1 k2))))))))

(deftest sorted-set-tests
  (let [k1 (tkey "hackers")
        k2 (tkey "slackers")
        k1-U-k2 (tkey "hackersnslackers")
        k1-I-k2 (tkey "hackerslackers")]

    (test/testing "Sorted Set case 1"
      (wcar* (car/zadd k1 "1940" "Alan Kay")
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
      (is (= ["Alan Kay" "Richard Stallman" "Yukihiro Matsumoto"]
             (wcar* (car/zrange k1 2 4)))))

    (test/testing "Sorted Set Union Case"
      (is (= ["Claude Shannon" "Alan Kay" "Richard Stallman" "Ferris Beuler"]
             (wcar* (car/zrange k1-U-k2 2 5)))))

    (test/testing "Sorted Set Intersect Case"
      (is (= ["Emmanuel Goldstein" "Dade Murphy"]
             (wcar* (car/zrange k1-I-k2 0 1)))))))

(deftest getset-test-2
  (let [k1 (tkey "children")
        k2 (tkey "favorite:child")]

    (test/testing "Case 1"
      (wcar* (car/rpush k1 "A")
             (car/rpush k1 "B")
             (car/rpush k1 "C")
             (car/set   k2 "B"))
      (is (= ["B" "B"] (wcar* (car/get k2)
                              (car/get k2)))))

    (test/testing "Case 2"
      (is (= ["B" ["A" "B" "C"] "B"]
             (wcar* (car/get k2)
                    (car/lrange k1 0 3)
                    (car/get k2)))))))

;;; Pub/sub

(deftest pubsub-single-channel-test
  (let [received (atom [])
        listener (car/with-new-pubsub-listener
                   {} {"ps-foo" #(swap! received conj %)}
                   (car/subscribe "ps-foo"))]
    (wcar* (car/publish "ps-foo" "one")
           (car/publish "ps-foo" "two")
           (car/publish "ps-foo" "three"))
    (Thread/sleep 500)
    (car/close-listener listener)
    (is (= [["subscribe" "ps-foo" 1]
            ["message"   "ps-foo" "one"]
            ["message"   "ps-foo" "two"]
            ["message"   "ps-foo" "three"]]
           @received))))

(deftest pubsub-multi-channels-test
  (let [received (atom [])
        listener (car/with-new-pubsub-listener
                   {} {"ps-foo" #(swap! received conj %)
                       "ps-baz" #(swap! received conj %)}
                   (car/subscribe "ps-foo" "ps-baz"))]
    (wcar* (car/publish "ps-foo" "one")
           (car/publish "ps-bar" "two")
           (car/publish "ps-baz" "three"))
    (Thread/sleep 500)
    (car/close-listener listener)
    (is (= [["subscribe" "ps-foo" 1]
            ["subscribe" "ps-baz" 2]
            ["message"   "ps-foo" "one"]
            ["message"   "ps-baz" "three"]]
           @received))))

(deftest pubsub-unsubscribe-test
  (let [received (atom [])
        listener (car/with-new-pubsub-listener
                   {} {"ps-*"   #(swap! received conj %)
                       "ps-foo" #(swap! received conj %)})]

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

    (is (= [["psubscribe"  "ps-*"   1]
            ["subscribe"   "ps-foo" 2]
            ["message"     "ps-foo" "one"]
            ["pmessage"    "ps-*"   "ps-foo" "one"]
            ["pmessage"    "ps-*"   "ps-bar" "two"]
            ["pmessage"    "ps-*"   "ps-baz" "three"]
            ["unsubscribe" "ps-foo" 1]
            ["pmessage"    "ps-*"   "ps-foo" "four"]
            ["pmessage"    "ps-*"   "ps-baz" "five"]]
           @received))))

(deftest bin-safety-test
  (let [k  (tkey "binary-safety")
        ba (byte-array [(byte 3) (byte 1) (byte 4)])]
    (wcar* (car/set k ba))
    (is (enc/ba= ba (wcar* (car/get k))))))

(deftest null-tests
  (let [k (tkey "nulls")]
    (is (= nil (do (wcar* (car/set k nil))
                   (wcar* (car/get k)))))
    (is (= "" (do (wcar* (car/set k ""))
                  (wcar* (car/get k)))))
    (is (= " " (do (wcar* (car/set k " "))
                   (wcar* (car/get k)))))
    (is (= 0 (do (wcar* (car/set k (byte-array 0)))
                 (count (wcar* (car/get k))))))
    (is (= 1 (do (wcar* (car/set k (.getBytes "\u0000" "UTF-8")))
                 (count (wcar* (car/get k))))))
    (is (= "Foobar\u0000" (do (wcar* (car/set k "Foobar\u0000"))
                              (wcar* (car/get k)))))
    (is (thrown? clojure.lang.ExceptionInfo
                 (wcar* (car/set k "\u0000Foobar"))))))

(deftest big-response-test
  (let [k (tkey "big-list")
        n 250000]
    (wcar* (dorun (repeatedly n #(car/rpush k "value"))))
    (is (= (repeat n "value")
           (wcar* (car/lrange k 0 -1))))))

(deftest lua-test
  (let [k (tkey "script")
        script "return redis.call('set',KEYS[1],'script-value')"
        script-hash (car/script-hash script)]

    (test/testing "Case 1"
      (wcar* (car/script-load script))
      (wcar* (car/evalsha script-hash 1 k))
      (is (= "script-value" (wcar* (car/get k)))))

    (test/testing "Case 2"
      (is (= ["key1" "key2" "arg1" "arg2"]
             (wcar* (car/eval "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"
                              2 "key1" "key2" "arg1" "arg2")))))))

(deftest reply-parsing-test
  (let [k (tkey "reply-parsing")]
    (wcar* (car/set k [:a :A :b :B :c :C :d :D]))
    (is (= ["PONG" {:a :A :b :B :c :C :d :D} "pong" "pong" "PONG"]
           (wcar* (car/ping)
                  (car/parse #(apply hash-map %) (car/get k))
                  (car/parse str/lower-case
                             (car/ping)
                             (car/ping))
                  (car/ping))))))

(deftest deprecated-atomically-transactions-tests
  (enc/deprecated
   (let [k      (tkey "atomic")
         wk     (tkey "watch")
         k-val  "initial-k-value"
         wk-val "initial-wk-value"]

     (test/testing "This transaction will succeed"
       (wcar* (car/set k  k-val)
              (car/set wk wk-val))
       (is (= ["PONG" "OK"]
              (wcar* (car/atomically []
                                     (let [wk-val (wcar* (car/get wk))]
                                       (car/ping)
                                       (car/set k wk-val))))))

       (is (= wk-val (wcar* (car/get k)))))

     (test/testing "This transaction will fail"
       (wcar* (car/set k  k-val)
              (car/set wk wk-val)))
     (is (= nil
            (wcar* (car/atomically [wk]
                                   ;; Will break watch
                                   (wcar* (car/set wk "CHANGE!"))
                                   (car/ping)))))
     (is (= k-val (wcar* (car/get k)))))))

(deftest with-replies-tests
  (test/testing "Basic Case"
    (is (= ["1" "2" ["3" "4"] "5"]
           (wcar {}
                 (car/echo 1)
                 (car/echo 2)
                 (car/return (car/with-replies (car/echo 3) (car/echo 4)))
                 (car/echo 5)))))

  (test/testing "Nested 'with-replies' case"
    (is (= ["1" "2" ["3" "4" ["5" "6"]] "7"]
           (wcar {}
                 (car/echo 1)
                 (car/echo 2)
                 (car/return (car/with-replies (car/echo 3) (car/echo 4)
                               (car/return (car/with-replies (car/echo 5) (car/echo 6)))))
                 (car/echo 7)))))

  (test/testing "'with-replies' vs parsers case"
    (is (= ["A" "b" ["c" "D"] ["E" "F"] "g"]
           (wcar {}
                 (car/parse str/upper-case (car/echo :a))
                 (car/echo :b)
                 (car/return (car/with-replies (car/echo :c) (car/parse str/upper-case (car/echo :d))))
                 (car/return
                  (car/parse str/upper-case
                             (car/with-replies (car/echo :e) (car/echo :f))))
                 (car/echo :g)))))

  (test/testing "Nested 'with-replies' vs parsers case"
    (is (= ["a" "b" ["c" "d" ["e" "f"]] "g"]
           (wcar {}
                 (car/echo :a)
                 (car/echo :b)
                 (car/return
                  (car/parse #(cond (string? %) (str/lower-case %)
                                    (vector? %) (mapv str/lower-case %))
                             (car/with-replies (car/echo :c) (car/echo :d)
                               (car/return (car/parse str/upper-case
                                                      (car/with-replies (car/echo :e) (car/echo :f)))))))
                 (car/echo :g))))))

;;;; Parsers
;; Like middleware, comp order can get confusing

(deftest parsers-tests
  (test/testing "Basic parsing"
    (is (= ["PONG" "pong" "pong" "PONG"]
           (wcar {} (car/ping) (car/parse str/lower-case
                                          (car/ping)
                                          (car/ping))
                 (car/ping)))))

  (test/testing "Cancel parsing"
    (is (= "YoLo" (wcar {} (->> (car/echo "YoLo")
                                (car/parse nil)
                                (car/parse str/upper-case))))))

  (test/testing "No auto-composition: last-applied (inner) parser wins"
    (is (= "yolo" (wcar {} (->> (car/echo "YoLo")
                                (car/parse str/lower-case)
                                (car/parse str/upper-case))))))

  (test/testing "Dynamic composition: prefer inner"
    (is (= "yolo" (wcar {} (->> (car/echo "YoLo")
                                (car/parse (car/parser-comp str/lower-case
                                                            protocol/*parser*))
                                (car/parse str/upper-case))))))

  (test/testing "Dynamic composition: prefer outer"
    (is (= "YOLO" (wcar {} (->> (car/echo "YoLo")
                                (car/parse (car/parser-comp protocol/*parser*
                                                            str/lower-case))
                                (car/parse str/upper-case))))))

  (test/testing "Dynamic composition with metadata ('return' uses metadata and auto-composes)"
    (is (= "yolo" (wcar {} (->> (car/return "YoLo")
                                (car/parse str/lower-case ))))))

  (test/testing "Exceptions pass through by default"
    (is (thrown? Exception (wcar {} (->> (car/return (Exception. "boo")) (car/parse keyword)))))
    (is (vector? (wcar {} (->> (do (car/return (Exception. "boo"))
                                   (car/return (Exception. "boo")))
                               (car/parse keyword))))))

  (test/testing "Parsers can elect to handle exceptions, even over 'return'"
    (is (= "Oh noes!" (wcar {} (->> (car/return (Exception. "boo"))
                                    (car/parse
                                     (-> #(if (instance? Exception %) "Oh noes!" %)
                                         (with-meta {:parse-exceptions? true}))))))))

  (test/testing "Lua ('with-replies') errors bypass normal parsers"
    (is (thrown? Exception (wcar {} (->> (car/lua "invalid" {:_ :_} {})
                                         (car/parse keyword)))))
    (is (instance? enc/bytes-class ;; But _do_ maintain raw parsing metadata:
                   (do (wcar {} (car/set (tkey "binkey")
                                         (.getBytes "Foobar" "UTF-8")))
                       (wcar {} (-> (car/lua "return redis.call('get', _:k)"
                                             {:k (tkey "binkey")} {})
                                    (car/parse-raw)))))))

  (test/testing "Parsing passes 'with-replies' barrier"
    (is (= [["ONE" "three"] "two"]
           (let [p (promise)]
             [(wcar {} (car/echo "ONE") (car/parse str/lower-case
                                                   (deliver p (car/with-replies (car/echo "TWO")))
                                                   (car/echo "THREE")))
              @p])))))

;;;; Request queues (experimental, for Carmine v3)
;; Pre v3, Redis command fns used to immediately trigger writes to their
;; context's io stream. v3 introduced lazy writing to allow request planning in
;; Cluster mode. This is a trade-off: we now have to be careful to realize
;; queued requests any time a nested connection occurs.

(deftest request-queues-tests
  (is (= ["OK" "echo"] (wcar {} (car/set (tkey "rq1")
                                         (wcar {} (car/echo "echo")))
                             (car/get (tkey "rq1")))))
  (is (= "rq2-val"
         (let [p (promise)]
           (wcar {} (car/del (tkey "rq2")))
           (wcar {} (car/set (tkey "rq2") "rq2-val")
                 ;; Nested `wcar` here should trigger eager sending of queued writes
                 ;; above (to simulate immediate-write commands):
                 (deliver p (wcar {} (car/get (tkey "rq2")))))
           @p))))


(deftest atomic-tests
  (test/testing "Bad cases"
    (is (thrown? Exception (car/atomic {} 1))
        "should throw exception because multi command is missing")

    (is (= [["OK"] nil]
           (car/atomic {} 1 (car/multi)))
        "Empty multi case")

    (is (thrown? Exception (car/atomic {} 1 (car/multi) (car/discard)))
        "Like missing multi case"))

  (test/testing "Basic tests"
    (is (= [["OK" "QUEUED"] "PONG"] (car/atomic {} 1 (car/multi) (car/ping))))
    (is (= [["OK" "QUEUED" "QUEUED"] ["PONG" "PONG"]]
           (car/atomic {} 1 (car/multi) (car/ping) (car/ping))))
    (is (= [["echo" "OK" "QUEUED"] "PONG"]
           (car/atomic {} 1 (car/return "echo") (car/multi) (car/ping)))))

  (test/testing "Exception case"
    (is (thrown? ArithmeticException ;; Nb be specific here to distinguish from other exs!
                 (car/atomic {} 1
                             (car/multi)
                             (car/ping)
                             (/ 1 0)    ; Throws client-side
                             (car/ping))))

    (is (thrown? Exception (car/atomic {} 1
                                       (car/multi)
                                       (car/redis-call [:invalid]) ; Server-side error, before exec
                                       (car/ping)))))

  (test/testing "Multi Ping case"
    (is (= "PONG" (-> (car/atomic {} 1 (car/multi) (car/multi) (car/ping))
                      second))
        "Ignores extra multi [error] while queuing:")

    (is (= "PONG" (-> (car/atomic {} 1
                                  (car/multi)
                                  (car/parse (constantly "foo") (car/ping)))
                                        ; No parsers
                      second))))

  (is (thrown? Exception (car/atomic {} 5
                                     (car/watch (tkey :watched))
                                     (car/set (tkey :watched) (rand))
                                     (car/multi)
                                     (car/ping)))) ; Contending changes to watched key

  ;; As above, with contending write by different connection:
  (is (thrown? Exception (car/atomic {} 5
                                     (car/watch (tkey :watched))
                                     (wcar {} (car/set (tkey :watched) (rand)))
                                     (car/multi)
                                     (car/ping))))

  (is (= [[["OK" "OK" "QUEUED"] "PONG"] 3]
         (let [idx (atom 1)]
           [(car/atomic {} 3
                        (car/watch (tkey :watched))
                        (when (< @idx 3)
                          (swap! idx inc)
                          (car/set (tkey :watched) (rand))) ; Contend only first 2 attempts
                        (car/multi)
                        (car/ping))
            @idx])))

  (is (thrown? ArithmeticException ;; Correct (unmasked) error
               (car/atomic {} 1 (/ 1 0)))))

(deftest cluster-key-hashing-tests
  (is (= [12182 5061 4813] [(commands/keyslot "foo")
                            (commands/keyslot "bar")
                            (commands/keyslot "baz")])
      "Basic hashing")

  (is (= (commands/keyslot "{user1000}.following")
         (commands/keyslot "{user1000}.followers"))
      "Both hash on 'user1000'")

  (is (not= (commands/keyslot "foo{}{bar}")
            (commands/keyslot "bar"))
      "Only first {}'s non-empty content used")

  (is (= (commands/keyslot "foo{{bar}}")
         (commands/keyslot "{bar"))
      "Content of first {}")

  (is (= (commands/keyslot "foo{bar}{zap}")
         (commands/keyslot "foo{bar}{zip}")
         (commands/keyslot "baz{bar}{zip}"))
       "All hash on 'bar")

  (is (not= (commands/keyslot "foo")
            (commands/keyslot "FOO"))
      "Hashing is case sensitive"))

(deftest hashing-arbitrary-bin-keys-tests
  (is (= (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 20)]))
         (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 20)]))))

  (is (not= (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 20)]))
            (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 21)])))))

(deftest bin-args-tests
  (wcar {} (car/set (tkey "bin-arg")
                    (.getBytes "Foobar" "UTF-8")))
  (is (= (seq (.getBytes "Foobar" "UTF-8"))
         (seq (wcar {} (car/get (tkey "bin-arg")))))))

(deftest cas-tests
  (is (= ["OK" 1 0 1 1 "final-val"]
         (let [tk  (tkey "cas-k")
               cas (partial car/compare-and-set tk)]
           ;; (wcar {} (car/del tk))
           (wcar {}
                 (car/set tk    0)
                 (cas 0         1)
                 (cas 22        23)        ; Will be ignored
                 (cas 1         nil)       ; Serialized nil
                 (cas nil       "final-val")
                 (car/get tk)))))

  (is (= ["state1" "state2" "state3" "RETURN" "state4" "state4" "OK"
          [:this :is :a :big :value :needs :sha :woo]
          :aborted "tx-value" "OK" :aborted "tx-value"]

         (let [tk      (tkey "swap1")
               big-val [:this :is :a :big :value :needs :sha]]
           ;; (wcar {} (car/del tk)) ; Debug
           (wcar {}
                 (car/swap tk (fn [?old nx?] (if nx? "state1" "_")))
                 (car/swap tk (fn [?old nx?] (if nx? "_" "state2")))
                 (car/swap tk (fn [?old _]   (if (= ?old "state2")  "state3" "_")))
                 (car/parse str/upper-case
                            (car/swap tk (fn [?old _] (enc/swapped "state4" "return"))))
                 (car/get  tk)
                 (car/swap tk (fn [?old _] (enc/swapped "state5" ?old)))

                 (car/set  tk big-val)
                 (car/swap tk (fn [?old nx?] (conj ?old :woo)))

                 (car/swap tk (fn [?old nx?] (wcar {} (car/set tk "non-tx-value")) "tx-value")
                           1 :aborted)
                 (car/swap tk (fn [?old nx?] (wcar {} (car/set tk "non-tx-value")) "tx-value")
                           2 :aborted)

                 (car/set tk big-val)
                 (car/swap tk (fn [?old nx?] (wcar {} (car/set tk "non-tx-value")) "tx-value")
                           1 :aborted)
                 (car/swap tk (fn [?old nx?] (wcar {} (car/set tk "non-tx-value")) "tx-value")
                           2 :aborted)))))

  (is (= ["nx" 1 1 "3" "tx-value1" :aborted "tx-value3"
          0 ["nx" "val2" "tx-value3" "val4"]]
   (let [tk (tkey "swap2")]
     ;; (wcar {} (car/del tk)) ; Debug
     (wcar {}
           (car/hswap tk "field1" (fn [?old-val nx?] (if nx? "nx" "ex")))
           (car/hset  tk "field2" "val2")
           (car/hset  tk "field3" 3)
           (car/hswap tk "field3" (fn [?old-val nx?] ?old-val))

           (car/hswap tk "field3"
                      (fn [?old nx?] (wcar {} (car/hset tk "field4" "non-tx-value1")) "tx-value1")
                      1 :aborted)
           (car/hswap tk "field3"
                      (fn [?old nx?] (wcar {} (car/hset tk "field3" "non-tx-value2")) "tx-value2")
                      1 :aborted)
           (car/hswap tk "field3"
                      (fn [?old nx?] (wcar {} (car/hset tk "field3" "non-tx-value3")) "tx-value3")
                      2 :aborted)

           (car/hset  tk "field4" "val4")
           (car/hvals tk))))))

(deftest benchmarks-tests
  (is (true? (benchmarks/bench {}))))
