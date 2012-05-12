(ns carmine.test.core
  (:use [clojure.test])
  (:require [carmine (core :as redis) (connections :as conns)]))

(def p (conns/make-conn-pool))
(def s (conns/make-conn-spec))
(defmacro wq [& queries] `(conns/with-conn p s ~@queries))

(wq (redis/flushall)) ; Start with fresh db

(deftest test-command-construction
  (is (= "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
         (#'redis/query "set" "foo" "bar")))
  (is (= "*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n" (#'redis/query "get" "bar"))))

(deftest test-echo
  (is (= "Message" (wq (redis/echo "Message"))))
  (is (= "PONG" (wq (redis/ping)))))

(deftest test-exists
  (is (= 0 (wq (redis/exists "singularity"))))
  (wq (redis/set "singularity" "exists"))
  (is (= 1 (wq (redis/exists "singularity")))))

(deftest test-keys
  (is (= '("resource:lock" "singularity") (wq (redis/keys "*")))))

(deftest test-set-get
  (is (= "OK" (wq (redis/set "server:name" "fido"))))
  (is (= "fido" (wq (redis/get "server:name"))))
  (is (= 15 (wq (redis/append "server:name" " [shutdown]"))))
  (is (= "fido [shutdown]" (wq (redis/getset "server:name" "fido [running]"))))
  (is (= "running" (wq (redis/getrange "server:name" "6" "12"))))
  (wq (redis/setbit "mykey" "7" "1"))
  (is (= 1 (wq (redis/getbit "mykey" "7"))))
  (is (= "OK" (wq (redis/set "multiline" "Redis\r\nDemo"))))
  (is (= "Redis\r\nDemo" (wq (redis/get "multiline")))))

(deftest test-incr-decr
  (wq (redis/set "connections" "10"))
  (is (= 11 (wq (redis/incr "connections"))))
  (is (= 20 (wq (redis/incrby "connections" "9"))))
  (is (= 19 (wq (redis/decr "connections"))))
  (is (= 10 (wq (redis/decrby "connections" "9")))))

(deftest test-del
  (wq (redis/set "something" "foo"))
  (is (= 1 (wq (redis/del "something")))))

(deftest test-expiry
  (wq (redis/set "resource:lock" "Redis Demo"))
  (is (= 1 (wq (redis/expire "resource:lock" "120"))))
  (is (< 0 (wq (redis/ttl "resource:lock"))))
  (is (= -1 (wq (redis/ttl "count")))))

(deftest test-lists
  (wq (redis/rpush "friends" "Tom"))
  (wq (redis/rpush "friends" "Bob"))
  (wq (redis/lpush "friends" "Sam"))
  (is (= '("Sam" "Tom" "Bob")
         (wq (redis/lrange "friends" "0" "-1"))))
  (is (= '("Sam" "Tom")
         (wq (redis/lrange "friends" "0" "1"))))
  (is (= '("Sam" "Tom" "Bob")
         (wq (redis/lrange "friends" "0" "2"))))
  (is (= 3 (wq (redis/llen "friends"))))
  (is (= "Sam" (wq (redis/lpop "friends"))))
  (is (= "Bob" (wq (redis/rpop "friends"))))
  (is (= 1 (wq (redis/llen "friends"))))
  (is (= '("Tom") (wq (redis/lrange "friends" "0" "-1"))))
  (wq (redis/del "friends")))

(deftest test-non-ascii-params
  (is (= "OK" (wq (redis/set "spanish" "year->año"))))
  (is (= "year->año" (wq (redis/get "spanish")))))

(deftest test-non-string-params
  (is (= "OK" (wq (redis/set "statement" "I am doing well"))))
  (is (= "doing well" (wq (redis/getrange "statement" 5 14))))
  (wq (redis/rpush "alist" "A")
      (redis/rpush "alist" "B")
      (redis/lpush "alist" "C"))
  (is (= ["A" "B"]) (redis/lrange "alist" 0 2)))

(deftest test-malformed-requests
  (is (thrown? Exception (wq "This is an invalid request"))))

(deftest test-hashes
  (wq (redis/hset "myhash" "field1" "value1"))
  (is (= "value1" (wq (redis/hget "myhash" "field1"))))
  (wq (redis/hsetnx "myhash" "field1" "newvalue"))
  (is (= "value1" (wq (redis/hget "myhash" "field1"))))
  (is (= 1 (wq (redis/hexists "myhash" "field1"))))
  (is (= '("field1" "value1") (wq (redis/hgetall "myhash"))))
  (wq (redis/hset "myhash" "field2" "1"))
  (is (= 3 (wq (redis/hincrby "myhash" "field2" "2"))))
  (is (= '("field1" "field2") (wq (redis/hkeys "myhash"))))
  (is (= '("value1" "3") (wq (redis/hvals "myhash"))))
  (is (= 2 (wq (redis/hlen "myhash"))))
  (wq (redis/hdel "myhash" "field1"))
  (is (= 0 (wq (redis/hexists "myhash" "field1")))))

(deftest test-sets
  (wq (redis/sadd "superpowers" "flight"))
  (wq (redis/sadd "superpowers" "x-ray vision"))
  (wq (redis/sadd "superpowers" "reflexes"))
  (wq (redis/srem "superpowers" "reflexes"))
  (is (= 1 (wq (redis/sismember "superpowers" "flight"))))
  (is (= 0 (wq (redis/sismember "superpowers" "reflexes"))))
  (wq (redis/sadd "birdpowers" "pecking"))
  (wq (redis/sadd "birdpowers" "flight"))
  (is (= '("pecking" "x-ray vision" "flight")
         (wq (redis/sunion "superpowers" "birdpowers")))))

(deftest test-sorted-sets
  (wq (redis/zadd "hackers" "1940" "Alan Kay"))
  (wq (redis/zadd "hackers" "1953" "Richard Stallman"))
  (wq (redis/zadd "hackers" "1965" "Yukihiro Matsumoto"))
  (wq (redis/zadd "hackers" "1916" "Claude Shannon"))
  (wq (redis/zadd "hackers" "1969" "Linus Torvalds"))
  (wq (redis/zadd "hackers" "1912" "Alan Turing"))
  (wq (redis/zadd "hackers" "1972" "Dade Murphy"))
  (wq (redis/zadd "hackers" "1970" "Emmanuel Goldstein"))
  (wq (redis/zadd "slackers" "1968" "Pauly Shore"))
  (wq (redis/zadd "slackers" "1972" "Dade Murphy"))
  (wq (redis/zadd "slackers" "1970" "Emmanuel Goldstein"))
  (wq (redis/zadd "slackers" "1966" "Adam Sandler"))
  (wq (redis/zadd "slackers" "1962" "Ferris Beuler"))
  (wq (redis/zadd "slackers" "1871" "Theodore Dreiser"))
  (wq (redis/zunionstore* "hackersnslackers" ["hackers" "slackers"]))
  (wq (redis/zinterstore* "hackerslackers" ["hackers" "slackers"]))
  (is (= '("Alan Kay" "Richard Stallman" "Yukihiro Matsumoto")
         (wq (redis/zrange "hackers" "2" "4"))))
  (is (= '("Claude Shannon" "Alan Kay" "Richard Stallman" "Ferris Beuler")
         (wq (redis/zrange "hackersnslackers" "2" "5"))))
  (is (= '("Emmanuel Goldstein" "Dade Murphy")
         (wq (redis/zrange "hackerslackers" "0" "1")))))

(deftest test-dbsize
  (wq (redis/flushdb))
  (wq (redis/set "something" "with a value"))
  (is (= 1 (wq (redis/dbsize))))
  (wq (redis/flushall))
  (is (= 0 (wq (redis/dbsize)))))

(deftest test-pipeline
  (wq (redis/rpush "children" "A")
      (redis/rpush "children" "B")
      (redis/rpush "children" "C"))
  (wq (redis/set "favorite:child" "B"))
  (is (= ["B" "B"] (wq (redis/get "favorite:child")
                       (redis/get "favorite:child"))))
  (is (= ["B" ["A" "B" "C"] "B"]
         (wq (redis/get "favorite:child")
             (redis/lrange "children" "0" "3")
             (redis/get "favorite:child")))))

(deftest test-pubsub
  (let [received (atom [])
        listener (redis/make-listener
                  s {"ps-foo" #(swap! received conj %)}
                  (redis/subscribe "ps-foo"))]
    (wq (redis/publish "ps-foo" "one")
        (redis/publish "ps-foo" "two")
        (redis/publish "ps-foo" "three"))
    (Thread/sleep 500)
    (redis/close-listener listener)
    (is (= @received ['("subscribe" "ps-foo" 1)
                      '("message"   "ps-foo" "one")
                      '("message"   "ps-foo" "two")
                      '("message"   "ps-foo" "three")])))

  (let [received (atom [])
        listener (redis/make-listener
                  s {"ps-foo" #(swap! received conj %)
                     "ps-baz" #(swap! received conj %)}
                  (redis/subscribe "ps-foo" "ps-baz"))]
    (wq (redis/publish "ps-foo" "one")
        (redis/publish "ps-bar" "two")
        (redis/publish "ps-baz" "three"))
    (Thread/sleep 500)
    (redis/close-listener listener)
    (is (= @received ['("subscribe" "ps-foo" 1)
                      '("subscribe" "ps-baz" 2)
                      '("message"   "ps-foo" "one")
                      '("message"   "ps-baz" "three")])))

  (let [received (atom [])
        listener (redis/make-listener
                  s {"ps-*"   #(swap! received conj %)
                     "ps-foo" #(swap! received conj %)})
        _ (redis/with-open-listener listener
            (redis/psubscribe "ps-*")
            (redis/subscribe  "ps-foo"))]
    (wq (redis/publish "ps-foo" "one")
        (redis/publish "ps-bar" "two")
        (redis/publish "ps-baz" "three"))
    (Thread/sleep 500)
    (redis/with-open-listener listener
      (redis/unsubscribe "ps-foo"))
    (Thread/sleep 500)
    (wq (redis/publish "ps-foo" "four")
        (redis/publish "ps-baz" "five"))
    (Thread/sleep 500)
    (redis/close-listener listener)
    (is (= @received ['("psubscribe"  "ps-*"   1)
                      '("subscribe"   "ps-foo" 2)
                      '("message"     "ps-foo" "one")
                      '("pmessage"    "ps-*"   "ps-foo" "one")
                      '("pmessage"    "ps-*"   "ps-bar" "two")
                      '("pmessage"    "ps-*"   "ps-baz" "three")
                      '("unsubscribe" "ps-foo" 1)
                      '("pmessage"    "ps-*"   "ps-foo" "four")
                      '("pmessage"    "ps-*"   "ps-baz" "five")]))))

(wq (redis/flushall)) ; Leave with a fresh db