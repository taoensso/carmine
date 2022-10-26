(ns taoensso.carmine.tests.main
  (:require
   [clojure.string   :as str]
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc  :refer [throws?]]
   [taoensso.carmine :as car  :refer [wcar]]
   [taoensso.carmine.commands   :as commands]
   [taoensso.carmine.protocol   :as protocol]
   [taoensso.carmine.benchmarks :as benchmarks]))

(comment
  (remove-ns      'taoensso.carmine.tests.main)
  (test/run-tests 'taoensso.carmine.tests.main))

;;;; Config, etc.

(def conn-opts {})
(defmacro wcar* [& body] `(car/wcar conn-opts ~@body))

(def tkey (partial car/key :carmine :temp :test))
(defn clear-tkeys! []
  (when-let [ks (seq (wcar* (car/keys (tkey :*))))]
    (wcar* (doseq [k ks] (car/del k)))))

(defn test-fixture [f] (clear-tkeys!) (f) (clear-tkeys!))
(test/use-fixtures :once test-fixture)

(defn sleep [n] (Thread/sleep n) (str "slept " n "msecs"))

;;;;

(deftest basic-tests
  [(is (= (wcar* (car/echo "Message")) "Message"))
   (is (= (wcar* (car/ping))           "PONG"))])

(deftest key-exists-test
  (let [_ (clear-tkeys!)
        k (tkey "exists")]
    [(is (= (wcar* (car/exists k)) 0) "Since key does not exists should return 0")
     (wcar* (car/set k "exists!"))
     (is (= (wcar* (car/exists k)) 1) "Now that key is set, it should exists!")]))

(deftest getset-test
  (let [_ (clear-tkeys!)
        k (tkey "server:name")]
    [(is (= (wcar* (car/set k "fido")) "OK"))
     (is (= (wcar* (car/get k))        "fido"))
     (is (= (wcar* (car/append k " [shutdown]")) 15))
     (is (= (wcar* (car/getset k "fido [running]")) "fido [shutdown]" ))
     (is (= (wcar* (car/getrange k 6 12)) "running"))]))

(deftest setbit-test
  (let [_ (clear-tkeys!)
        k (tkey "mykey")]
    [(is (= (wcar* (car/setbit k 7 1)) 0))
     (is (= (wcar* (car/getbit k 7)) 1) "7th bit of the key was set to 1")]))

(deftest multiline-test
  (let [_ (clear-tkeys!)
        k (tkey "multiline")]
    [(is (= (wcar* (car/set k   "Redis\r\nDemo")) "OK" ))
     (is (= (wcar* (car/get k)) "Redis\r\nDemo" ))]))

(deftest inc-dec-tests
  (let [_ (clear-tkeys!)
        k (tkey "connections")]
    [(wcar* (car/set k 10))
     (is (= (wcar* (car/incr   k))   11))
     (is (= (wcar* (car/incrby k 9)) 20))
     (is (= (wcar* (car/decr   k))   19))
     (is (= (wcar* (car/decrby k 9)) 10))]))

(deftest delete-key-test
  (let [_ (clear-tkeys!)
        k (tkey "something")]
    [(wcar* (car/set k "foo"))
     (is (= (wcar* (car/del k)) 1))]))

(deftest expiry-tests
  (let [_ (clear-tkeys!)
        k (tkey "resource:lock")]
    [(wcar* (car/set k "Redis Demo"))
     (is (= (wcar* (car/ttl k)) -1 ))
     (is (= (wcar* (car/expire k 120)) 1))
     (is (pos? (wcar* (car/ttl k))))]))

(deftest array-cmds-tests
  (let [_ (clear-tkeys!)k (tkey "friends")]
    [(testing "Push command"
       [(is (= (wcar* (car/rpush k "Tom")) 1))
        (is (= (wcar* (car/rpush k "Bob")) 2))
        (is (= (wcar* (car/lpush k "Sam")) 3))
        (is (= (wcar* (car/lrange k 0 -1)) ["Sam" "Tom" "Bob"]))])

     (testing "lrange command"
       [(is (= (wcar* (car/lrange k 0 1)) ["Sam" "Tom"]))
        (is (= (wcar* (car/lrange k 0 2)) ["Sam" "Tom" "Bob"]))])

     (testing "len and pop commands"
       [(is (= (wcar* (car/llen k)) 3))
        (is (= (wcar* (car/lpop k)) "Sam"))
        (is (= (wcar* (car/rpop k)) "Bob"))
        (is (= (wcar* (car/llen k)) 1))
        (is (= (wcar* (car/lrange k 0 -1)) ["Tom"]))])]))

(deftest get-set-spanish-test
  (let [_ (clear-tkeys!)
        k (tkey "spanish")]
    [(is (= (wcar* (car/set k "year->año") (car/get k)) ["OK" "year->año"]))]))

(deftest exception-test
  (let [_ (clear-tkeys!)
        k (tkey "str-field")]

    [(wcar* (car/set k "str-value"))
     (is (throws? clojure.lang.ExceptionInfo (wcar* (car/incr k)))
       "Can't increment a string value")

     (let [[r1 r2 r3] (wcar* (car/ping) (car/incr k) (car/ping))]
       [(is (= r1 "PONG"))
        (is (instance? clojure.lang.ExceptionInfo r2))
        (is (= r3 "PONG"))])]))

(deftest malformed-tests
  [(is (= (wcar* "This is a malformed request") nil))
   (is (= (wcar* (car/ping) "This is a malformed request") "PONG"))])

(deftest pong-test
  (is (= (wcar* (doall (repeatedly 3 car/ping))) ["PONG" "PONG" "PONG"])))

(deftest echo-test
  (is (= (wcar* (doall (map car/echo ["A" "B" "C"]))) ["A" "B" "C"])))

(deftest key-inside-key-test
  (let [_ (clear-tkeys!)
        out-k (tkey "outside-key")
        in-k  (tkey "inside-key")]
    [(wcar*
       (car/set in-k "inside value")
       (car/set out-k in-k))
     (is (= (wcar* (car/get (last (wcar* (car/ping) (car/get out-k))))) "inside value")
       "Should get the inner value")]))

(deftest parallel-incr-test
  (let [_ (clear-tkeys!)
        k (tkey "parallel-key")]
    (wcar* (car/set k 0))
    (->>
      (repeatedly 100 ; No. of parallel clients
        (fn [] (future (dotimes [n 100] (wcar* (car/incr k))))))
      (doall)
      (map deref)
      (dorun))
    (is (= (wcar* (car/get k)) "10000"))))

(deftest hash-set-test
  (let [_ (clear-tkeys!)
        k (tkey "myhash")]
    [(is (= (wcar* (car/hset    k "field1" "value1")) 1))
     (is (= (wcar* (car/hget    k "field1")) "value1"))
     (is (= (wcar* (car/hsetnx  k "field1" "newvalue")) 0))
     (is (= (wcar* (car/hget    k "field1")) "value1"))
     (is (= (wcar* (car/hexists k "field1")) 1))
     (is (= (wcar* (car/hgetall k)) ["field1" "value1"]))
     (is (= (wcar* (car/hset    k "field2" 1)) 1))
     (is (= (wcar* (car/hincrby k "field2" 2)) 3))
     (is (= (wcar* (car/hkeys   k)) ["field1" "field2"]))
     (is (= (wcar* (car/hvals   k)) ["value1" "3"]))
     (is (= (wcar* (car/hlen    k)) 2))
     (is (= (wcar* (car/hdel    k "field1")) 1))
     (is (= (wcar* (car/hexists k "field1")) 0))]))

(deftest set-tests
  (let [_ (clear-tkeys!)
        k1 (tkey "superpowers")
        k2 (tkey "birdpowers")]

    [(testing "Member in set case"
       [(is (= (wcar* (car/sadd k1 "flight"))       1))
        (is (= (wcar* (car/sadd k1 "x-ray vision")) 1))
        (is (= (wcar* (car/sadd k1 "reflexes"))     1))
        (is (= (wcar* (car/sadd k1 "reflexes"))     0))
        (is (= (wcar* (car/srem k1 "reflexes"))     1))
        (is (= (wcar* (car/sismember k1 "flight")) 1))])

     (testing "Member NOT in set case"
       [(is (= (wcar* (car/sismember k1 "reflexes")) 0))])

     (testing "Set union case"
       [(is (= (wcar* (car/sadd k2 "pecking")) 1))
        (is (= (wcar* (car/sadd k2 "flight"))  1))
        (is (= (set (wcar* (car/sunion k1 k2)))
              #{"flight" "pecking" "x-ray vision"}))])]))

(deftest sorted-set-tests
  (let [_ (clear-tkeys!)
        k1 (tkey "hackers")
        k2 (tkey "slackers")
        k1-U-k2 (tkey "hackersnslackers")
        k1-I-k2 (tkey "hackerslackers")]

    [(testing "Sorted Set case 1"
       [(wcar*
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

        (is (= (wcar* (car/zrange k1 2 4))
              ["Alan Kay" "Richard Stallman" "Yukihiro Matsumoto"]))])

     (testing "Sorted Set Union Case"
       [(is (= (wcar* (car/zrange k1-U-k2 2 5))
              ["Claude Shannon" "Alan Kay" "Richard Stallman" "Ferris Beuler"]))])

     (testing "Sorted Set Intersect Case"
       [(is (= (wcar* (car/zrange k1-I-k2 0 1))
              ["Emmanuel Goldstein" "Dade Murphy"]))])]))

(deftest getset-test-2
  (let [_ (clear-tkeys!)
        k1 (tkey "children")
        k2 (tkey "favorite:child")]

    [(testing "Case 1"
       [(wcar*
          (car/rpush k1 "A")
          (car/rpush k1 "B")
          (car/rpush k1 "C")
          (car/set   k2 "B"))

        (is (= (wcar*
                 (car/get k2)
                 (car/get k2))
              ["B" "B"] ))])

     (testing "Case 2"
       [(is (= (wcar*
                 (car/get k2)
                 (car/lrange k1 0 3)
                 (car/get k2))
              ["B" ["A" "B" "C"] "B"]))])]))

;;; Pub/sub

(deftest pubsub-single-channel-test
  (let [_ (clear-tkeys!)
        received_ (atom [])
        listener
        (car/with-new-pubsub-listener
          {} {"ps-foo" #(swap! received_ conj %)}
          (car/subscribe "ps-foo"))]

    (sleep 200)
    [(is (= (wcar*
              (car/publish "ps-foo" "one")
              (car/publish "ps-foo" "two")
              (car/publish "ps-foo" "three"))
           [1 1 1]))

     (sleep 400)
     (do (car/close-listener listener) :close-listener)

     (is (= [["subscribe" "ps-foo" 1]
             ["message"   "ps-foo" "one"]
             ["message"   "ps-foo" "two"]
             ["message"   "ps-foo" "three"]]
           @received_))]))

(deftest pubsub-multi-channels-test
  (let [_ (clear-tkeys!)
        received_ (atom [])
        listener
        (car/with-new-pubsub-listener
          {} {"ps-foo" #(swap! received_ conj %)
              "ps-baz" #(swap! received_ conj %)}
          (car/subscribe "ps-foo" "ps-baz"))]

    (sleep 200)
    [(is (= (wcar*
              (car/publish "ps-foo" "one")
              (car/publish "ps-bar" "two")
              (car/publish "ps-baz" "three"))
           [1 0 1]))

     (sleep 400)
     (do (car/close-listener listener) :close-listener)
     (is (= [["subscribe" "ps-foo" 1]
             ["subscribe" "ps-baz" 2]
             ["message"   "ps-foo" "one"]
             ["message"   "ps-baz" "three"]]
           @received_))]))

(deftest pubsub-unsubscribe-test
  (let [_ (clear-tkeys!)
        received_ (atom [])
        listener
        (car/with-new-pubsub-listener {}
          {"ps-*"   #(swap! received_ conj %)
           "ps-foo" #(swap! received_ conj %)})]

    (sleep 200)
    [(car/with-open-listener listener
       (car/psubscribe "ps-*")
       (car/subscribe  "ps-foo"))

     (sleep 400)
     (is (= (wcar*
              (car/publish "ps-foo" "one")
              (car/publish "ps-bar" "two")
              (car/publish "ps-baz" "three"))
           [2 1 1]))

     (sleep 400)
     (car/with-open-listener listener
       (car/unsubscribe "ps-foo"))

     (sleep 400)
     (is (= (wcar*
              (car/publish "ps-foo" "four")
              (car/publish "ps-baz" "five"))
           [1 1]))

     (sleep 400)
     (do (car/close-listener listener) :close-listener)

     (is (= [["psubscribe"  "ps-*"   1]
             ["subscribe"   "ps-foo" 2]
             ["message"     "ps-foo" "one"]
             ["pmessage"    "ps-*"   "ps-foo" "one"]
             ["pmessage"    "ps-*"   "ps-bar" "two"]
             ["pmessage"    "ps-*"   "ps-baz" "three"]
             ["unsubscribe" "ps-foo" 1]
             ["pmessage"    "ps-*"   "ps-foo" "four"]
             ["pmessage"    "ps-*"   "ps-baz" "five"]]
           @received_))]))

(deftest pubsub-ping-and-errors-test
  (let [_ (clear-tkeys!)
        received_  (atom [])
        received-pong?_ (atom false)
        listener
        (car/with-new-pubsub-listener
          {:ping-ms 1000} ^:parse
          (fn [msg _]
            (let [{:keys [kind channel pattern payload raw]} msg]
              (cond
                ;; Precisely timing pong difficult in GitHub workflow
                (= kind    "pong")  (reset! received-pong?_ true)
                (= payload "throw") (throw (Exception.))
                :else               (swap!  received_ conj msg))))

          (car/psubscribe "*"))]

    (sleep 200) ; < ping-ms
    [(is (= (wcar*
              (car/publish "a" "1")
              (car/publish "a" "2")
              (car/publish "b" "1")
              (car/publish "b" "throw")
              (car/publish "b" "2"))
           [1 1 1 1 1]))

     (sleep 1200) ; > ping-ms

     (wcar* (car/publish "a" "3"))

     (sleep 400)
     (do (car/close-listener listener) :close-listener)

     (sleep 400)
     (let [received @received_
           clean-received
           (mapv
             (fn [{:keys [kind channel payload]}]
               (cond
                 (= kind    "pmessage")               [kind (str channel "/" payload)]
                 (= channel "carmine:listener:error") [kind (:error          payload)]
                 :else                                [kind channel]))
             received)]

       (is (= clean-received
             [["psubscribe" "*"]
              ["pmessage" "a/1"]
              ["pmessage" "a/2"]
              ["pmessage" "b/1"]
              ["carmine" :handler-ex]
              ["pmessage" "b/2"]
              ["pmessage" "a/3"]
              ["carmine" :conn-closed]])))]))

(deftest bin-safety-test
  (let [_  (clear-tkeys!)
        k  (tkey "binary-safety")
        ba (byte-array [(byte 3) (byte 1) (byte 4)])]
    [(wcar* (car/set k ba))
     (is (enc/ba= (wcar* (car/get k)) ba))]))

(deftest null-tests
  (let [_ (clear-tkeys!)
        k (tkey "nulls")]
    [(is (= (do (wcar* (car/set k nil))
                (wcar* (car/get k))) nil))

     (is (= (do (wcar* (car/set k ""))
                (wcar* (car/get k))) ""))
     (is (= (do (wcar* (car/set k " "))
                (wcar* (car/get k))) " "))

     (is (= (do (wcar* (car/set k (byte-array 0)))
                (count (wcar* (car/get k)))) 0))

     (is (= (do (wcar* (car/set k (.getBytes "\u0000" "UTF-8")))
                (count (wcar* (car/get k)))) 1))

     (is (= (do (wcar* (car/set k "Foobar\u0000"))
                (wcar* (car/get k)))
           "Foobar\u0000"))

     (is (throws? clojure.lang.ExceptionInfo
           (wcar* (car/set k "\u0000Foobar"))))]))

(deftest big-response-test
  (let [_ (clear-tkeys!)
        k (tkey "big-list")
        n 250000]
    (wcar* (dorun (repeatedly n #(car/rpush k "value"))))
    (is (= (wcar* (car/lrange k 0 -1)) (repeat n "value")))))

(deftest lua-test
  (let [_ (clear-tkeys!)
        k (tkey "script")
        script "return redis.call('set',KEYS[1],'script-value')"
        script-hash (car/script-hash script)]

    [(testing "Case 1"
       [(wcar* (car/script-load script))
        (wcar* (car/evalsha script-hash 1 k))
        (is (= (wcar* (car/get k)) "script-value"))])

    (testing "Case 2"
      (is (= (wcar* (car/eval "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"
                      2 "key1" "key2" "arg1" "arg2"))
            ["key1" "key2" "arg1" "arg2"])))]))

(deftest reply-parsing-test
  (let [_ (clear-tkeys!)
        k (tkey "reply-parsing")]
    [(wcar* (car/set k [:a :A :b :B :c :C :d :D]))
     (is (= (wcar*
              (car/ping)
              (car/parse #(apply hash-map %) (car/get k))
              (car/parse str/lower-case
                (car/ping)
                (car/ping))
              (car/ping))

           ["PONG" {:a :A :b :B :c :C :d :D} "pong" "pong" "PONG"]))]))

(enc/deprecated
 (deftest deprecated-atomically-transactions-tests
   (let [_      (clear-tkeys!)
         k      (tkey "atomic")
         wk     (tkey "watch")
         k-val  "initial-k-value"
         wk-val "initial-wk-value"]

     (testing "Successful transaction"
       [(wcar* (car/set k  k-val) (car/set wk wk-val))

        (is (= (wcar*
                 (car/atomically []
                   (let [wk-val (wcar* (car/get wk))]
                     (car/ping)
                     (car/set k wk-val))))
              ["PONG" "OK"]))

        (is (= (wcar* (car/get k)) wk-val))])

     (testing "Unsuccessful transaction"
       [(wcar*
          (car/set k  k-val)
          (car/set wk wk-val))

        (is (= (wcar*
                 (car/atomically [wk]
                   (wcar* (car/set wk "CHANGE!")) ; Breaks watch
                   (car/ping)))
              nil))
        (is (= (wcar* (car/get k)) k-val))]))))

(deftest with-replies-tests
  (let [_ (clear-tkeys!)]
    [(testing "Basic Case"
       (is (= ["1" "2" ["3" "4"] "5"]
             (wcar {}
               (car/echo 1)
               (car/echo 2)
               (car/return (car/with-replies (car/echo 3) (car/echo 4)))
               (car/echo 5)))))

     (testing "Nested 'with-replies' case"
       (is (= ["1" "2" ["3" "4" ["5" "6"]] "7"]
             (wcar {}
               (car/echo 1)
               (car/echo 2)
               (car/return (car/with-replies (car/echo 3) (car/echo 4)
                             (car/return (car/with-replies (car/echo 5) (car/echo 6)))))
               (car/echo 7)))))

     (testing "'with-replies' vs parsers case"
       (is (= ["A" "b" ["c" "D"] ["E" "F"] "g"]
             (wcar {}
               (car/parse str/upper-case (car/echo :a))
               (car/echo :b)
               (car/return (car/with-replies (car/echo :c) (car/parse str/upper-case (car/echo :d))))
               (car/return
                 (car/parse str/upper-case
                   (car/with-replies (car/echo :e) (car/echo :f))))
               (car/echo :g)))))

     (testing "Nested 'with-replies' vs parsers case"
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
               (car/echo :g)))))]))

;;;; Parsers
;; Like middleware, comp order can get confusing

(deftest parsers-tests
  (let [_ (clear-tkeys!)]
    [(testing "Basic parsing"
       (is (= ["PONG" "pong" "pong" "PONG"]
             (wcar {} (car/ping) (car/parse str/lower-case
                                   (car/ping)
                                   (car/ping))
               (car/ping)))))

     (testing "Cancel parsing"
       (is (= "YoLo" (wcar {} (->> (car/echo "YoLo")
                                (car/parse nil)
                                (car/parse str/upper-case))))))

     (testing "No auto-composition: last-applied (inner) parser wins"
       (is (= "yolo" (wcar {} (->> (car/echo "YoLo")
                                (car/parse str/lower-case)
                                (car/parse str/upper-case))))))

     (testing "Dynamic composition: prefer inner"
       (is (= "yolo" (wcar {} (->> (car/echo "YoLo")
                                (car/parse (car/parser-comp str/lower-case
                                             protocol/*parser*))
                                (car/parse str/upper-case))))))

     (testing "Dynamic composition: prefer outer"
       (is (= "YOLO" (wcar {} (->> (car/echo "YoLo")
                                (car/parse (car/parser-comp protocol/*parser*
                                             str/lower-case))
                                (car/parse str/upper-case))))))

     (testing "Dynamic composition with metadata ('return' uses metadata and auto-composes)"
       (is (= "yolo" (wcar {} (->> (car/return "YoLo")
                                (car/parse str/lower-case ))))))

     (testing "Exceptions pass through by default"
       (is (throws? Exception (wcar {} (->> (car/return (Exception. "boo")) (car/parse keyword)))))
       (is (vector? (wcar {} (->> (do (car/return (Exception. "boo"))
                                      (car/return (Exception. "boo")))
                               (car/parse keyword))))))

     (testing "Parsers can elect to handle exceptions, even over 'return'"
       (is (= "Oh noes!" (wcar {} (->> (car/return (Exception. "boo"))
                                    (car/parse
                                      (-> #(if (instance? Exception %) "Oh noes!" %)
                                        (with-meta {:parse-exceptions? true}))))))))

     (testing "Lua ('with-replies') errors bypass normal parsers"
       (is (throws? Exception (wcar {} (->> (car/lua "invalid" {:_ :_} {})
                                         (car/parse keyword)))))

       (is (instance? enc/bytes-class
             ;; But _do_ maintain raw parsing metadata:
             (do (wcar {} (car/set (tkey "binkey")
                            (.getBytes "Foobar" "UTF-8")))
                 (wcar {} (-> (car/lua "return redis.call('get', _:k)"
                                {:k (tkey "binkey")} {})
                            (car/parse-raw)))))))

     (testing "Parsing passes 'with-replies' barrier"
       (is (= [["ONE" "three"] "two"]
             (let [p_ (promise)]
               [(wcar {} (car/echo "ONE")
                  (car/parse str/lower-case
                    (deliver p_ (car/with-replies (car/echo "TWO")))
                    (car/echo "THREE")))
                (deref p_ 0 ::timeout)]))))]))

;;;; Request queues (experimental, for Carmine v3)
;; Pre v3, Redis command fns used to immediately trigger writes to their
;; context's io stream. v3 introduced lazy writing to allow request planning in
;; Cluster mode. This is a trade-off: we now have to be careful to realize
;; queued requests any time a nested connection occurs.

(deftest request-queues-tests
  (let [_ (clear-tkeys!)]
    [(is (= ["OK" "echo"] (wcar {} (car/set (tkey "rq1")
                                     (wcar {} (car/echo "echo")))
                            (car/get (tkey "rq1")))))

     (is (= "rq2-val"
           (let [p_ (promise)]
             (wcar {} (car/del (tkey "rq2")))
             (wcar {} (car/set (tkey "rq2") "rq2-val")
               ;; Nested `wcar` here should trigger eager sending of queued writes
               ;; above (to simulate immediate-write commands):
               (deliver p_ (wcar {} (car/get (tkey "rq2")))))
             (deref p_ 0 ::timeout))))]))

(deftest atomic-tests
  (let [_ (clear-tkeys!)]
    [(testing "Bad cases"
       [(is (throws? Exception (car/atomic {} 1))
          "Multi command is missing")

        (is (= (car/atomic {} 1 (car/multi)) [["OK"] nil])
          "Empty multi case")

        (is (throws? Exception (car/atomic {} 1 (car/multi) (car/discard)))
          "Like missing multi case")])

     (testing "Basic tests"
       [(is (= [["OK" "QUEUED"] "PONG"] (car/atomic {} 1 (car/multi) (car/ping))))
        (is (= [["OK" "QUEUED" "QUEUED"] ["PONG" "PONG"]]
              (car/atomic {} 1 (car/multi) (car/ping) (car/ping))))
        (is (= [["echo" "OK" "QUEUED"] "PONG"]
              (car/atomic {} 1 (car/return "echo") (car/multi) (car/ping))))])

     (testing "Exception case"
       [(is (throws? ArithmeticException ;; Nb be specific here to distinguish from other exs!
              (car/atomic {} 1
                (car/multi)
                (car/ping)
                (/ 1 0)    ; Throws client-side
                (car/ping))))

        (is (throws? Exception (car/atomic {} 1
                                 (car/multi)
                                 (car/redis-call [:invalid]) ; Server-side error, before exec
                                 (car/ping))))])

     (testing "Multi Ping case"
       [(is (= "PONG" (-> (car/atomic {} 1 (car/multi) (car/multi) (car/ping))
                        second))
          "Ignores extra multi [error] while queuing:")

        (is (= "PONG" (-> (car/atomic {} 1
                            (car/multi)
                            (car/parse (constantly "foo") (car/ping)))
                                        ; No parsers
                        second)))])

     (testing "Misc"
       [(is (throws? Exception
              (car/atomic {} 5
                (car/watch (tkey :watched))
                (car/set (tkey :watched) (rand))
                (car/multi)
                (car/ping)))) ; Contending changes to watched key

        ;; As above, with contending write by different connection:
        (is (throws? Exception
              (car/atomic {} 5
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

        (is (throws? ArithmeticException ;; Correct (unmasked) error
              (car/atomic {} 1 (/ 1 0))))])]))

(deftest cluster-key-hashing-tests
  [(is (= [12182 5061 4813] [(commands/keyslot "foo")
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
     "Hashing is case sensitive")])

(deftest hashing-arbitrary-bin-keys-tests
  [(is (= (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 20)]))
          (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 20)]))))

   (is (not= (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 20)]))
             (commands/keyslot (byte-array [(byte 3) (byte 100) (byte 21)]))))])

(deftest bin-args-tests
  (let [_ (clear-tkeys!)]
    [(wcar {} (car/set (tkey "bin-arg") (.getBytes "Foobar" "UTF-8")))
     (is (= (seq (.getBytes "Foobar" "UTF-8"))
            (seq (wcar {} (car/get (tkey "bin-arg"))))))]))

(deftest cas-tests
  (let [_ (clear-tkeys!)]
    [(is (= ["OK" 1 0 1 1 "final-val"]
           (let [tk  (tkey "cas-k")
                 cas (partial car/compare-and-set tk)]
             ;; (wcar {} (car/del tk))
             (wcar {}
               (car/set tk 0)
               (cas 0      1)
               (cas 22     23)  ; Will be ignored
               (cas 1      nil) ; Serialized nil
               (cas nil    "final-val")
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
               (car/hvals tk)))))]))

(deftest benchmarks-tests
  (is (benchmarks/bench {})))
