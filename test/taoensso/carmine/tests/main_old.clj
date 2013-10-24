(ns taoensso.carmine.tests.main-old
  (:require [clojure.test :refer :all]
            [clojure.string         :as str]
            [taoensso.carmine       :as car]
            [taoensso.carmine.utils :as utils]
            [taoensso.nippy         :as nippy]))

(defmacro wcar* [& body] `(car/wcar {:pool {} :spec {}} ~@body))
(defn test-key [key] (str "carmine:temp:test:" key))
(defn clean-up []
  (when-let [test-keys (seq (wcar* (car/keys (test-key "*"))))]
    (wcar* (apply car/del test-keys))))

(clean-up)

(deftest test-echo
  (is (= "Message" (wcar* (car/echo "Message"))))
  (is (= "PONG"    (wcar* (car/ping)))))

(deftest test-exists
  (let [k (test-key "singularity")]
    (is (= 0 (wcar* (car/exists k))))
    (wcar* (car/set k "exists"))
    (is (= 1 (wcar* (car/exists k))))))

(deftest test-set-get
  (let [k (test-key "server:name")]
    (is (= "OK" (wcar* (car/set k "fido"))))
    (is (= "fido" (wcar* (car/get k))))
    (is (= 15 (wcar* (car/append k " [shutdown]"))))
    (is (= "fido [shutdown]" (wcar* (car/getset k "fido [running]"))))
    (is (= "running" (wcar* (car/getrange k 6 12)))))
  (let [k (test-key "mykey")]
    (wcar* (car/setbit k 7 1))
    (is (= 1 (wcar* (car/getbit k 7)))))
  (let [k (test-key "multiline")]
    (is (= "OK" (wcar* (car/set k "Redis\r\nDemo"))))
    (is (= "Redis\r\nDemo" (wcar* (car/get k))))))

(deftest test-incr-decr
  (let [k (test-key "connections")]
    (wcar* (car/set k 10))
    (is (= 11 (wcar* (car/incr k))))
    (is (= 20 (wcar* (car/incrby k 9))))
    (is (= 19 (wcar* (car/decr k))))
    (is (= 10 (wcar* (car/decrby k 9))))))

(deftest test-del
  (let [k (test-key "something")]
    (wcar* (car/set k "foo"))
    (is (= 1 (wcar* (car/del k))))))

(deftest test-expiry
  (let [k (test-key "resource:lock")]
    (wcar* (car/set k "Redis Demo"))
    (is (= -1 (wcar* (car/ttl k))))
    (is (= 1 (wcar* (car/expire k 120))))
    (is (< 0 (wcar* (car/ttl k))))))

(deftest test-lists
  (let [k (test-key "friends")]
    (wcar* (car/rpush k "Tom"))
    (wcar* (car/rpush k "Bob"))
    (wcar* (car/lpush k "Sam"))
    (is (= ["Sam" "Tom" "Bob"]
           (wcar* (car/lrange k 0 -1))))
    (is (= ["Sam" "Tom"]
           (wcar* (car/lrange k 0 1))))
    (is (= ["Sam" "Tom" "Bob"]
           (wcar* (car/lrange k 0 2))))
    (is (= 3 (wcar* (car/llen k))))
    (is (= "Sam" (wcar* (car/lpop k))))
    (is (= "Bob" (wcar* (car/rpop k))))
    (is (= 1 (wcar* (car/llen k))))
    (is (= ["Tom"] (wcar* (car/lrange k 0 -1))))
    (wcar* (car/del k))))

(deftest test-non-ascii-params
  (let [k (test-key "spanish")]
    (is (= "OK" (wcar* (car/set k "year->año"))))
    (is (= "year->año" (wcar* (car/get k))))))

(deftest test-error-handling
  (let [k (test-key "str-field")]
    (wcar* (car/set k "str-value"))
    (is (thrown? Exception (wcar* (car/incr k))))
    (is (some #(instance? Exception %)
              (wcar* (car/ping) (car/incr k) (car/ping))))))

(deftest test-commands-as-real-functions
  (is (= nil (wcar* "This is a malformed request")))
  (is (= "PONG" (wcar* (car/ping) "This is a malformed request")))
  (is (= ["PONG" "PONG" "PONG"] (wcar* (doall (repeatedly 3 car/ping)))))
  (is (= ["A" "B" "C"] (wcar* (doall (map car/echo ["A" "B" "C"]))))))

(deftest test-composition
  (let [out-k (test-key "outside-key")
        in-k  (test-key "inside-key")]
    (wcar* (car/set in-k  "inside value")
          (car/set out-k in-k))
    (is (= "inside value" (wcar* (car/get (last (wcar* (car/ping) (car/get out-k)))))))))

(deftest test-parallelism
  (let [k (test-key "parallel-key")]
    (wcar* (car/set k 0))
    (->> (fn [] (future (dotimes [n 100] (wcar* (car/incr k)))))
         (repeatedly 100) ; No. of parallel clients
         (doall)
         (map deref)
         (dorun))
    (is (= (wcar* (car/get k)) "10000"))))

(deftest test-hashes
  (let [k (test-key "myhash")]
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

(deftest test-sets
  (let [k1 (test-key "superpowers")
        k2 (test-key "birdpowers")]
    (wcar* (car/sadd k1 "flight"))
    (wcar* (car/sadd k1 "x-ray vision"))
    (wcar* (car/sadd k1 "reflexes"))
    (wcar* (car/srem k1 "reflexes"))
    (is (= 1 (wcar* (car/sismember k1 "flight"))))
    (is (= 0 (wcar* (car/sismember k1 "reflexes"))))
    (wcar* (car/sadd k2 "pecking"))
    (wcar* (car/sadd k2 "flight"))
    (is (= (set ["flight" "pecking" "x-ray vision"])
           (set (wcar* (car/sunion k1 k2)))))))

(deftest test-sorted-sets
  (let [k1 (test-key "hackers")
        k2 (test-key "slackers")
        k1-U-k2 (test-key "hackersnslackers")
        k1-I-k2 (test-key "hackerslackers")]
    (wcar* (car/zadd k1 "1940" "Alan Kay"))
    (wcar* (car/zadd k1 "1953" "Richard Stallman"))
    (wcar* (car/zadd k1 "1965" "Yukihiro Matsumoto"))
    (wcar* (car/zadd k1 "1916" "Claude Shannon"))
    (wcar* (car/zadd k1 "1969" "Linus Torvalds"))
    (wcar* (car/zadd k1 "1912" "Alan Turing"))
    (wcar* (car/zadd k1 "1972" "Dade Murphy"))
    (wcar* (car/zadd k1 "1970" "Emmanuel Goldstein"))
    (wcar* (car/zadd k2 "1968" "Pauly Shore"))
    (wcar* (car/zadd k2 "1972" "Dade Murphy"))
    (wcar* (car/zadd k2 "1970" "Emmanuel Goldstein"))
    (wcar* (car/zadd k2 "1966" "Adam Sandler"))
    (wcar* (car/zadd k2 "1962" "Ferris Beuler"))
    (wcar* (car/zadd k2 "1871" "Theodore Dreiser"))
    (wcar* (car/zunionstore* k1-U-k2 [k1 k2]))
    (wcar* (car/zinterstore* k1-I-k2 [k1 k2]))
    (is (= ["Alan Kay" "Richard Stallman" "Yukihiro Matsumoto"]
           (wcar* (car/zrange k1 2 4))))
    (is (= ["Claude Shannon" "Alan Kay" "Richard Stallman" "Ferris Beuler"]
           (wcar* (car/zrange k1-U-k2 2 5))))
    (is (= ["Emmanuel Goldstein" "Dade Murphy"]
           (wcar* (car/zrange k1-I-k2 0 1))))))

(deftest test-pipeline
  (let [k1 (test-key "children")
        k2 (test-key "favorite:child")]
    (wcar* (car/rpush k1 "A")
          (car/rpush k1 "B")
          (car/rpush k1 "C"))
    (wcar* (car/set k2 "B"))
    (is (= ["B" "B"] (wcar* (car/get k2)
                           (car/get k2))))
    (is (= ["B" ["A" "B" "C"] "B"]
           (wcar* (car/get k2)
                 (car/lrange k1 0 3)
                 (car/get k2))))))

(deftest test-pubsub
  (let [received (atom [])
        listener (car/with-new-pubsub-listener
                   {} {"ps-foo" #(swap! received conj %)}
                   (car/subscribe "ps-foo"))]

    (wcar* (car/publish "ps-foo" "one")
          (car/publish "ps-foo" "two")
          (car/publish "ps-foo" "three"))
    (Thread/sleep 500)
    (car/close-listener listener)
    (is (= @received [["subscribe" "ps-foo" 1]
                      ["message"   "ps-foo" "one"]
                      ["message"   "ps-foo" "two"]
                      ["message"   "ps-foo" "three"]])))

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
    (is (= @received [["subscribe" "ps-foo" 1]
                      ["subscribe" "ps-baz" 2]
                      ["message"   "ps-foo" "one"]
                      ["message"   "ps-baz" "three"]])))

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
    (is (= @received [["psubscribe"  "ps-*"   1]
                      ["subscribe"   "ps-foo" 2]
                      ["message"     "ps-foo" "one"]
                      ["pmessage"    "ps-*"   "ps-foo" "one"]
                      ["pmessage"    "ps-*"   "ps-bar" "two"]
                      ["pmessage"    "ps-*"   "ps-baz" "three"]
                      ["unsubscribe" "ps-foo" 1]
                      ["pmessage"    "ps-*"   "ps-foo" "four"]
                      ["pmessage"    "ps-*"   "ps-baz" "five"]]))))

(deftest test-binary-safety
  (let [k (test-key "binary-safety")
        ba (byte-array [(byte 3) (byte 1) (byte 4)])]
    (wcar* (car/set k ba))
    (is (utils/ba= ba (wcar* (car/get k))))))

(deftest test-nulls
  (let [k (test-key "nulls")]
    (wcar* (car/set k nil))
    (is (= nil (wcar* (car/get k))))
    (wcar* (car/set k ""))
    (is (= "" (wcar* (car/get k))))
    (wcar* (car/set k " "))
    (is (= " " (wcar* (car/get k))))
    (wcar* (car/set k (byte-array 0)))
    (is (= 0 (count (wcar* (car/get k)))))
    (wcar* (car/set k (.getBytes "\u0000" "UTF-8")))
    (is (= 1 (count (wcar* (car/get k)))))
    (wcar* (car/set k "Foobar\u0000"))
    (is (= "Foobar\u0000" (wcar* (car/get k))))
    (is (thrown? Exception (wcar* (car/set k "\u0000Foobar"))))))

(deftest test-big-responses
  (let [k (test-key "big-list")
        n 250000]
    (wcar* (dorun (repeatedly n (fn [] (car/rpush k "value")))))
    (is (= (repeat n "value") (wcar* (car/lrange k 0 -1))))))

(deftest test-lua
  (let [k (test-key "script")
        script "return redis.call('set',KEYS[1],'script-value')"
        script-hash (car/hash-script script)]
    (wcar* (car/script-load script))
    (wcar* (car/evalsha script-hash 1 k))
    (is (= "script-value" (wcar* (car/get k))))
    (is (= ["key1" "key2" "arg1" "arg2"]
           (wcar* (car/eval "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"
                            2 "key1" "key2" "arg1" "arg2"))))))

(deftest test-reply-parsing
  (let [k (test-key "reply-parsing")]
    (wcar* (car/set k [:a :A :b :B :c :C :d :D]))
    (is (= ["PONG" {:a :A :b :B :c :C :d :D} "pong" "pong" "PONG"]
           (wcar* (car/ping)
                 (car/with-parser #(apply hash-map %) (car/get k))
                 (car/with-parser str/lower-case
                   (car/ping)
                   (car/ping))
                 (car/ping))))))

(deftest test-transactions ; Uses DEPRECATED `atomically`
  (let [k      (test-key "atomic")
        wk     (test-key "watch")
        k-val  "initial-k-value"
        wk-val "initial-wk-value"]

    ;;; This transaction will succeed
    (wcar* (car/set k  k-val)
          (car/set wk wk-val))
    (is (= ["PONG" "OK"]
           (wcar* (car/atomically
                  []
                  (let [wk-val (wcar* (car/get wk))]
                    (car/ping)
                    (car/set k wk-val))))))
    (is (= wk-val (wcar* (car/get k))))

    ;;; This transaction will fail
    (wcar* (car/set k  k-val)
          (car/set wk wk-val))
    (is (= []
           (wcar* (car/atomically
                  [wk]
                  (wcar* (car/set wk "CHANGE!")) ; Will break watch
                  (car/ping)))))
    (is (= k-val (wcar* (car/get k))))))

(clean-up)
