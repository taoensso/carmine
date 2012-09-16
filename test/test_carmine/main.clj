(ns test-carmine.main
  (:use [clojure.test])
  (:require [clojure.string         :as str]
            [taoensso.carmine       :as r]
            [taoensso.carmine.utils :as utils]
            [taoensso.nippy         :as nippy]))

;;;; TODO
;; * Change weird (= const thing) form to (= thing const).
;; * Bring over tests from redis-clojure, etc.

;;;; Setup

(def p (r/make-conn-pool))
(def s (r/make-conn-spec))
(defmacro wc [& body] `(r/with-conn p s ~@body))

(defn redis-version-sufficient?
  [minimum-version]
  (utils/version-sufficient? (get (wc (r/info*)) "redis_version")
                             minimum-version))

(defn test-key [key] (str "carmine:temp:test:" key))
(defn clean-up!
  []
  (let [test-keys (wc (r/keys (test-key "*")))]
    (when (seq test-keys)
      (wc (apply r/del test-keys)))))

;;;; Tests

(clean-up!) ; Start with a fresh db

(deftest test-echo
  (is (= "Message" (wc (r/echo "Message"))))
  (is (= "PONG"    (wc (r/ping)))))

(deftest test-exists
  (let [k (test-key "singularity")]
    (is (= 0 (wc (r/exists k))))
    (wc (r/set k "exists"))
    (is (= 1 (wc (r/exists k))))))

(deftest test-set-get
  (let [k (test-key "server:name")]
    (is (= "OK" (wc (r/set k "fido"))))
    (is (= "fido" (wc (r/get k))))
    (is (= 15 (wc (r/append k " [shutdown]"))))
    (is (= "fido [shutdown]" (wc (r/getset k "fido [running]"))))
    (is (= "running" (wc (r/getrange k 6 12)))))
  (let [k (test-key "mykey")]
    (wc (r/setbit k 7 1))
    (is (= 1 (wc (r/getbit k 7)))))
  (let [k (test-key "multiline")]
    (is (= "OK" (wc (r/set k "Redis\r\nDemo"))))
    (is (= "Redis\r\nDemo" (wc (r/get k))))))

(deftest test-incr-decr
  (let [k (test-key "connections")]
    (wc (r/set k 10))
    (is (= 11 (wc (r/incr k))))
    (is (= 20 (wc (r/incrby k 9))))
    (is (= 19 (wc (r/decr k))))
    (is (= 10 (wc (r/decrby k 9))))))

(deftest test-del
  (let [k (test-key "something")]
    (wc (r/set k "foo"))
    (is (= 1 (wc (r/del k))))))

(deftest test-expiry
  (let [k (test-key "resource:lock")]
    (wc (r/set k "Redis Demo"))
    (is (= -1 (wc (r/ttl k))))
    (is (= 1 (wc (r/expire k 120))))
    (is (< 0 (wc (r/ttl k))))))

(deftest test-lists
  (let [k (test-key "friends")]
    (wc (r/rpush k "Tom"))
    (wc (r/rpush k "Bob"))
    (wc (r/lpush k "Sam"))
    (is (= ["Sam" "Tom" "Bob"]
           (wc (r/lrange k 0 -1))))
    (is (= ["Sam" "Tom"]
           (wc (r/lrange k 0 1))))
    (is (= ["Sam" "Tom" "Bob"]
           (wc (r/lrange k 0 2))))
    (is (= 3 (wc (r/llen k))))
    (is (= "Sam" (wc (r/lpop k))))
    (is (= "Bob" (wc (r/rpop k))))
    (is (= 1 (wc (r/llen k))))
    (is (= ["Tom"] (wc (r/lrange k 0 -1))))
    (wc (r/del k))))

(deftest test-non-ascii-params
  (let [k (test-key "spanish")]
    (is (= "OK" (wc (r/set k "year->año"))))
    (is (= "year->año" (wc (r/get k))))))

(deftest test-error-handling
  (let [k (test-key "str-field")]
    (wc (r/set k "str-value"))
    (is (thrown? Exception (wc (r/incr k))))
    (is (some #(instance? Exception %)
              (wc (r/ping) (r/incr k) (r/ping))))))

(deftest test-commands-as-real-functions
  (is (= nil (wc "This is a malformed request")))
  (is (= "PONG" (wc (r/ping) "This is a malformed request")))
  (is (= ["PONG" "PONG" "PONG"] (wc (doall (repeatedly 3 r/ping)))))
  (is (= ["A" "B" "C"] (wc (doall (map r/echo ["A" "B" "C"]))))))

(deftest test-composition
  (let [out-k (test-key "outside-key")
        in-k  (test-key "inside-key")]
    (wc (r/set in-k  "inside value")
        (r/set out-k in-k))
    (is (= "inside value" (wc (r/get (last (wc (r/ping) (r/get out-k)))))))))

(deftest test-parallelism
  (let [k (test-key "parallel-key")]
    (wc (r/set k 0))
    (->> (fn [] (future (dotimes [n 100] (wc (r/incr k)))))
         (repeatedly 100) ; No. of parallel clients
         (doall)
         (map deref)
         (dorun))
    (is (= (wc (r/get k)) "10000"))))

(deftest test-hashes
  (let [k (test-key "myhash")]
    (wc (r/hset k "field1" "value1"))
    (is (= "value1" (wc (r/hget k "field1"))))
    (wc (r/hsetnx k "field1" "newvalue"))
    (is (= "value1" (wc (r/hget k "field1"))))
    (is (= 1 (wc (r/hexists k "field1"))))
    (is (= ["field1" "value1"] (wc (r/hgetall k))))
    (wc (r/hset k "field2" 1))
    (is (= 3 (wc (r/hincrby k "field2" 2))))
    (is (= ["field1" "field2"] (wc (r/hkeys k))))
    (is (= ["value1" "3"] (wc (r/hvals k))))
    (is (= 2 (wc (r/hlen k))))
    (wc (r/hdel k "field1"))
    (is (= 0 (wc (r/hexists k "field1"))))))

(deftest test-sets
  (let [k1 (test-key "superpowers")
        k2 (test-key "birdpowers")]
    (wc (r/sadd k1 "flight"))
    (wc (r/sadd k1 "x-ray vision"))
    (wc (r/sadd k1 "reflexes"))
    (wc (r/srem k1 "reflexes"))
    (is (= 1 (wc (r/sismember k1 "flight"))))
    (is (= 0 (wc (r/sismember k1 "reflexes"))))
    (wc (r/sadd k2 "pecking"))
    (wc (r/sadd k2 "flight"))
    (is (= (set ["flight" "pecking" "x-ray vision"])
           (set (wc (r/sunion k1 k2)))))))

(deftest test-sorted-sets
  (let [k1 (test-key "hackers")
        k2 (test-key "slackers")
        k1-U-k2 (test-key "hackersnslackers")
        k1-I-k2 (test-key "hackerslackers")]
    (wc (r/zadd k1 "1940" "Alan Kay"))
    (wc (r/zadd k1 "1953" "Richard Stallman"))
    (wc (r/zadd k1 "1965" "Yukihiro Matsumoto"))
    (wc (r/zadd k1 "1916" "Claude Shannon"))
    (wc (r/zadd k1 "1969" "Linus Torvalds"))
    (wc (r/zadd k1 "1912" "Alan Turing"))
    (wc (r/zadd k1 "1972" "Dade Murphy"))
    (wc (r/zadd k1 "1970" "Emmanuel Goldstein"))
    (wc (r/zadd k2 "1968" "Pauly Shore"))
    (wc (r/zadd k2 "1972" "Dade Murphy"))
    (wc (r/zadd k2 "1970" "Emmanuel Goldstein"))
    (wc (r/zadd k2 "1966" "Adam Sandler"))
    (wc (r/zadd k2 "1962" "Ferris Beuler"))
    (wc (r/zadd k2 "1871" "Theodore Dreiser"))
    (wc (r/zunionstore* k1-U-k2 [k1 k2]))
    (wc (r/zinterstore* k1-I-k2 [k1 k2]))
    (is (= ["Alan Kay" "Richard Stallman" "Yukihiro Matsumoto"]
           (wc (r/zrange k1 2 4))))
    (is (= ["Claude Shannon" "Alan Kay" "Richard Stallman" "Ferris Beuler"]
           (wc (r/zrange k1-U-k2 2 5))))
    (is (= ["Emmanuel Goldstein" "Dade Murphy"]
           (wc (r/zrange k1-I-k2 0 1))))))

(deftest test-pipeline
  (let [k1 (test-key "children")
        k2 (test-key "favorite:child")]
    (wc (r/rpush k1 "A")
        (r/rpush k1 "B")
        (r/rpush k1 "C"))
    (wc (r/set k2 "B"))
    (is (= ["B" "B"] (wc (r/get k2)
                         (r/get k2))))
    (is (= ["B" ["A" "B" "C"] "B"]
           (wc (r/get k2)
               (r/lrange k1 0 3)
               (r/get k2))))))

(deftest test-pubsub
  (let [received (atom [])
        listener (r/with-new-pubsub-listener
                   s {"ps-foo" #(swap! received conj %)}
                   (r/subscribe "ps-foo"))]

    (wc (r/publish "ps-foo" "one")
        (r/publish "ps-foo" "two")
        (r/publish "ps-foo" "three"))
    (Thread/sleep 500)
    (r/close-listener listener)
    (is (= @received [["subscribe" "ps-foo" 1]
                      ["message"   "ps-foo" "one"]
                      ["message"   "ps-foo" "two"]
                      ["message"   "ps-foo" "three"]])))

  (let [received (atom [])
        listener (r/with-new-pubsub-listener
                   s {"ps-foo" #(swap! received conj %)
                      "ps-baz" #(swap! received conj %)}
                   (r/subscribe "ps-foo" "ps-baz"))]
    (wc (r/publish "ps-foo" "one")
        (r/publish "ps-bar" "two")
        (r/publish "ps-baz" "three"))
    (Thread/sleep 500)
    (r/close-listener listener)
    (is (= @received [["subscribe" "ps-foo" 1]
                      ["subscribe" "ps-baz" 2]
                      ["message"   "ps-foo" "one"]
                      ["message"   "ps-baz" "three"]])))

  (let [received (atom [])
        listener (r/with-new-pubsub-listener
                   s {"ps-*"   #(swap! received conj %)
                      "ps-foo" #(swap! received conj %)})]
    (r/with-open-listener listener
      (r/psubscribe "ps-*")
      (r/subscribe  "ps-foo"))
    (wc (r/publish "ps-foo" "one")
        (r/publish "ps-bar" "two")
        (r/publish "ps-baz" "three"))
    (Thread/sleep 500)
    (r/with-open-listener listener
      (r/unsubscribe "ps-foo"))
    (Thread/sleep 500)
    (wc (r/publish "ps-foo" "four")
        (r/publish "ps-baz" "five"))
    (Thread/sleep 500)
    (r/close-listener listener)
    (is (= @received [["psubscribe"  "ps-*"   1]
                      ["subscribe"   "ps-foo" 2]
                      ["message"     "ps-foo" "one"]
                      ["pmessage"    "ps-*"   "ps-foo" "one"]
                      ["pmessage"    "ps-*"   "ps-bar" "two"]
                      ["pmessage"    "ps-*"   "ps-baz" "three"]
                      ["unsubscribe" "ps-foo" 1]
                      ["pmessage"    "ps-*"   "ps-foo" "four"]
                      ["pmessage"    "ps-*"   "ps-baz" "five"]]))))

(deftest test-serialization
  (let [k1   (test-key "stress-data")
        k2   (test-key "stress-data-hash")
        data (dissoc nippy/stress-data :bytes)]
    (wc (r/set k1 data))
    (is (= data (wc (r/get k1))))
    (wc (r/hmset k2 "field1" data "field2" "just a string"))
    (is (= data (wc (r/hget k2 "field1"))))
    (is (= "just a string" (wc (r/hget k2 "field2"))))))

(deftest test-binary-safety
  (let [k (test-key "binary-safety")
        ba (byte-array [(byte 3) (byte 1) (byte 4)])]
    (wc (r/set k ba))
    (is (= [ba 3]) (wc (r/get k)))))

(deftest test-nulls
  (let [k (test-key "nulls")]
    (wc (r/set k nil))
    (is (= nil (wc (r/get k))))
    (wc (r/set k ""))
    (is (= "" (wc (r/get k))))
    (wc (r/set k " "))
    (is (= " " (wc (r/get k))))
    (wc (r/set k (byte-array 0)))
    (is (= 0 (second (wc (r/get k)))))
    (wc (r/set k (.getBytes "\u0000" "UTF-8")))
    (is (= 1 (second (wc (r/get k)))))
    (wc (r/set k "Foobar\u0000"))
    (is (= "Foobar\u0000" (wc (r/get k))))
    (is (thrown? Exception (wc (r/set k "\u0000Foobar"))))))

(deftest test-big-responses
  (let [k (test-key "big-list")
        n 250000]
    (wc (dorun (repeatedly n (fn [] (r/rpush k "value")))))
    (is (= (repeat n "value") (wc (r/lrange k 0 -1))))))

(deftest test-lua
  (when (redis-version-sufficient? "2.6.0")
    (let [k (test-key "script")
          script "return redis.call('set',KEYS[1],'script-value')"
          script-hash (r/hash-script script)]
      (wc (r/script-load script))
      (wc (r/evalsha script-hash 1 k))
      (is (= "script-value" (wc (r/get k))))
      (is (= ["key1" "key2" "arg1" "arg2"]
             (wc (r/eval "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"
                         2 "key1" "key2" "arg1" "arg2")))))))

(deftest test-reply-parsing
  (let [k (test-key "reply-parsing")]
    (wc (r/set k [:a :A :b :B :c :C :d :D]))
    (is (= ["PONG" {:a :A :b :B :c :C :d :D} "PONG" "PONG"]
           (wc (r/ping)
               (r/with-parser #(apply hash-map %) (r/get k))
               (r/ping)
               (r/ping))))))

(deftest test-transactions
  (let [k      (test-key "atomic")
        wk     (test-key "watch")
        k-val  "initial-k-value"
        wk-val "initial-wk-value"]

    ;;; This transaction will succeed
    (wc (r/set k  k-val)
        (r/set wk wk-val))
    (is (= ["PONG" "OK"]
           (wc (r/atomically
                []
                (let [wk-val (wc (r/get wk))]
                  (r/ping)
                  (r/set k wk-val))))))
    (is (= wk-val (wc (r/get k))))

    ;;; This transaction will fail
    (wc (r/set k  k-val)
        (r/set wk wk-val))
    (is (= []
           (wc (r/atomically
                [wk]
                (wc (r/set wk "CHANGE!")) ; Will break watch
                (r/ping)))))
    (is (= k-val (wc (r/get k))))))

(clean-up!) ; Leave with a fresh db