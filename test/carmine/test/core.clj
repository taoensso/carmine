(ns carmine.test.core
  (:use [clojure.test])
  (:require [carmine (core :as r) (connections :as conns)]))

(def p (conns/make-conn-pool))
(def s (conns/make-conn-spec))
(defmacro wc [& commands] `(conns/with-conn p s ~@commands))

(wc (r/flushall)) ; Start with fresh db

(deftest test-command-construction
  (is (= "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
         (#'r/query "set" "foo" "bar")))
  (is (= "*2\r\n$3\r\nGET\r\n$3\r\nbar\r\n" (#'r/query "get" "bar"))))

(deftest test-echo
  (is (= "Message" (wc (r/echo "Message"))))
  (is (= "PONG" (wc (r/ping)))))

(deftest test-exists
  (is (= 0 (wc (r/exists "singularity"))))
  (wc (r/set "singularity" "exists"))
  (is (= 1 (wc (r/exists "singularity")))))

(deftest test-keys
  (is (= '("resource:lock" "singularity") (wc (r/keys "*")))))

(deftest test-set-get
  (is (= "OK" (wc (r/set "server:name" "fido"))))
  (is (= "fido" (wc (r/get "server:name"))))
  (is (= 15 (wc (r/append "server:name" " [shutdown]"))))
  (is (= "fido [shutdown]" (wc (r/getset "server:name" "fido [running]"))))
  (is (= "running" (wc (r/getrange "server:name" "6" "12"))))
  (wc (r/setbit "mykey" "7" "1"))
  (is (= 1 (wc (r/getbit "mykey" "7"))))
  (is (= "OK" (wc (r/set "multiline" "Redis\r\nDemo"))))
  (is (= "Redis\r\nDemo" (wc (r/get "multiline")))))

(deftest test-incr-decr
  (wc (r/set "connections" "10"))
  (is (= 11 (wc (r/incr "connections"))))
  (is (= 20 (wc (r/incrby "connections" "9"))))
  (is (= 19 (wc (r/decr "connections"))))
  (is (= 10 (wc (r/decrby "connections" "9")))))

(deftest test-del
  (wc (r/set "something" "foo"))
  (is (= 1 (wc (r/del "something")))))

(deftest test-expiry
  (wc (r/set "resource:lock" "Redis Demo"))
  (is (= 1 (wc (r/expire "resource:lock" "120"))))
  (is (< 0 (wc (r/ttl "resource:lock"))))
  (is (= -1 (wc (r/ttl "count")))))

(deftest test-lists
  (wc (r/rpush "friends" "Tom"))
  (wc (r/rpush "friends" "Bob"))
  (wc (r/lpush "friends" "Sam"))
  (is (= '("Sam" "Tom" "Bob")
         (wc (r/lrange "friends" "0" "-1"))))
  (is (= '("Sam" "Tom")
         (wc (r/lrange "friends" "0" "1"))))
  (is (= '("Sam" "Tom" "Bob")
         (wc (r/lrange "friends" "0" "2"))))
  (is (= 3 (wc (r/llen "friends"))))
  (is (= "Sam" (wc (r/lpop "friends"))))
  (is (= "Bob" (wc (r/rpop "friends"))))
  (is (= 1 (wc (r/llen "friends"))))
  (is (= '("Tom") (wc (r/lrange "friends" "0" "-1"))))
  (wc (r/del "friends")))

(deftest test-non-ascii-params
  (is (= "OK" (wc (r/set "spanish" "year->año"))))
  (is (= "year->año" (wc (r/get "spanish")))))

(deftest test-non-string-params
  (is (= "OK" (wc (r/set "statement" "I am doing well"))))
  (is (= "doing well" (wc (r/getrange "statement" 5 14))))
  (wc (r/rpush "alist" "A")
      (r/rpush "alist" "B")
      (r/lpush "alist" "C"))
  (is (= ["A" "B"]) (r/lrange "alist" 0 2)))

(deftest test-error-handling
  (is (thrown? Exception (wc "This is a malformed request")))
  (wc (r/set "str-field" "str-value"))
  (is (thrown? Exception (wc (r/incr "str-field"))))
  (is (some #(instance? Exception %)
            (wc (r/ping) (r/incr "str-field") (r/ping)))))

(deftest test-hashes
  (wc (r/hset "myhash" "field1" "value1"))
  (is (= "value1" (wc (r/hget "myhash" "field1"))))
  (wc (r/hsetnx "myhash" "field1" "newvalue"))
  (is (= "value1" (wc (r/hget "myhash" "field1"))))
  (is (= 1 (wc (r/hexists "myhash" "field1"))))
  (is (= '("field1" "value1") (wc (r/hgetall "myhash"))))
  (wc (r/hset "myhash" "field2" "1"))
  (is (= 3 (wc (r/hincrby "myhash" "field2" "2"))))
  (is (= '("field1" "field2") (wc (r/hkeys "myhash"))))
  (is (= '("value1" "3") (wc (r/hvals "myhash"))))
  (is (= 2 (wc (r/hlen "myhash"))))
  (wc (r/hdel "myhash" "field1"))
  (is (= 0 (wc (r/hexists "myhash" "field1")))))

(deftest test-sets
  (wc (r/sadd "superpowers" "flight"))
  (wc (r/sadd "superpowers" "x-ray vision"))
  (wc (r/sadd "superpowers" "reflexes"))
  (wc (r/srem "superpowers" "reflexes"))
  (is (= 1 (wc (r/sismember "superpowers" "flight"))))
  (is (= 0 (wc (r/sismember "superpowers" "reflexes"))))
  (wc (r/sadd "birdpowers" "pecking"))
  (wc (r/sadd "birdpowers" "flight"))
  (is (= '("pecking" "x-ray vision" "flight")
         (wc (r/sunion "superpowers" "birdpowers")))))

(deftest test-sorted-sets
  (wc (r/zadd "hackers" "1940" "Alan Kay"))
  (wc (r/zadd "hackers" "1953" "Richard Stallman"))
  (wc (r/zadd "hackers" "1965" "Yukihiro Matsumoto"))
  (wc (r/zadd "hackers" "1916" "Claude Shannon"))
  (wc (r/zadd "hackers" "1969" "Linus Torvalds"))
  (wc (r/zadd "hackers" "1912" "Alan Turing"))
  (wc (r/zadd "hackers" "1972" "Dade Murphy"))
  (wc (r/zadd "hackers" "1970" "Emmanuel Goldstein"))
  (wc (r/zadd "slackers" "1968" "Pauly Shore"))
  (wc (r/zadd "slackers" "1972" "Dade Murphy"))
  (wc (r/zadd "slackers" "1970" "Emmanuel Goldstein"))
  (wc (r/zadd "slackers" "1966" "Adam Sandler"))
  (wc (r/zadd "slackers" "1962" "Ferris Beuler"))
  (wc (r/zadd "slackers" "1871" "Theodore Dreiser"))
  (wc (r/zunionstore* "hackersnslackers" ["hackers" "slackers"]))
  (wc (r/zinterstore* "hackerslackers" ["hackers" "slackers"]))
  (is (= '("Alan Kay" "Richard Stallman" "Yukihiro Matsumoto")
         (wc (r/zrange "hackers" "2" "4"))))
  (is (= '("Claude Shannon" "Alan Kay" "Richard Stallman" "Ferris Beuler")
         (wc (r/zrange "hackersnslackers" "2" "5"))))
  (is (= '("Emmanuel Goldstein" "Dade Murphy")
         (wc (r/zrange "hackerslackers" "0" "1")))))

(deftest test-dbsize
  (wc (r/flushdb))
  (wc (r/set "something" "with a value"))
  (is (= 1 (wc (r/dbsize))))
  (wc (r/flushall))
  (is (= 0 (wc (r/dbsize)))))

(deftest test-pipeline
  (wc (r/rpush "children" "A")
      (r/rpush "children" "B")
      (r/rpush "children" "C"))
  (wc (r/set "favorite:child" "B"))
  (is (= ["B" "B"] (wc (r/get "favorite:child")
                       (r/get "favorite:child"))))
  (is (= ["B" ["A" "B" "C"] "B"]
         (wc (r/get "favorite:child")
             (r/lrange "children" "0" "3")
             (r/get "favorite:child")))))

(deftest test-pubsub
  (let [received (atom [])
        listener (r/make-listener
                  s {"ps-foo" #(swap! received conj %)}
                  (r/subscribe "ps-foo"))]
    (wc (r/publish "ps-foo" "one")
        (r/publish "ps-foo" "two")
        (r/publish "ps-foo" "three"))
    (Thread/sleep 500)
    (r/close-listener listener)
    (is (= @received ['("subscribe" "ps-foo" 1)
                      '("message"   "ps-foo" "one")
                      '("message"   "ps-foo" "two")
                      '("message"   "ps-foo" "three")])))

  (let [received (atom [])
        listener (r/make-listener
                  s {"ps-foo" #(swap! received conj %)
                     "ps-baz" #(swap! received conj %)}
                  (r/subscribe "ps-foo" "ps-baz"))]
    (wc (r/publish "ps-foo" "one")
        (r/publish "ps-bar" "two")
        (r/publish "ps-baz" "three"))
    (Thread/sleep 500)
    (r/close-listener listener)
    (is (= @received ['("subscribe" "ps-foo" 1)
                      '("subscribe" "ps-baz" 2)
                      '("message"   "ps-foo" "one")
                      '("message"   "ps-baz" "three")])))

  (let [received (atom [])
        listener (r/make-listener
                  s {"ps-*"   #(swap! received conj %)
                     "ps-foo" #(swap! received conj %)})
        _ (r/with-open-listener listener
            (r/psubscribe "ps-*")
            (r/subscribe  "ps-foo"))]
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
    (is (= @received ['("psubscribe"  "ps-*"   1)
                      '("subscribe"   "ps-foo" 2)
                      '("message"     "ps-foo" "one")
                      '("pmessage"    "ps-*"   "ps-foo" "one")
                      '("pmessage"    "ps-*"   "ps-bar" "two")
                      '("pmessage"    "ps-*"   "ps-baz" "three")
                      '("unsubscribe" "ps-foo" 1)
                      '("pmessage"    "ps-*"   "ps-foo" "four")
                      '("pmessage"    "ps-*"   "ps-baz" "five")]))))

(wc (r/flushall)) ; Leave with a fresh db