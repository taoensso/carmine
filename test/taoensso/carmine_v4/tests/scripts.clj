(ns taoensso.carmine-v4.tests.scripts
  "Carmine v4 Lua preparation, scripting, and atomic CAS tests."
  (:require
   [clojure.test        :refer [deftest testing is]]
   [taoensso.encore     :as enc]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine-v4          :as car  :refer [wcar]]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.scripts  :as scripts]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tk  "Test key" support/test-key)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

(defn- redis-major-version [] (support/redis-major-version))

;;;; Scripts

(deftest _lua-preparation
  (let [script
        (str
          "local a = _:foo\n"
          "local b = _:foo-bar\n"
          "local c = _:ns/value\n"
          "local d = _:foobar\n"
          "local s1 = '_:foo'\n"
          "local s2 = \"_:foo\"\n"
          "local s3 = [==[_:foo]==]\n"
          "-- _:foo\n"
          "--[=[_:foo]=]\n"
          "return _:foo")
        expected
        (str
          "local a = ARGV[4]\n"
          "local b = ARGV[2]\n"
          "local c = ARGV[1]\n"
          "local d = ARGV[3]\n"
          "local s1 = '_:foo'\n"
          "local s2 = \"_:foo\"\n"
          "local s3 = [==[_:foo]==]\n"
          "-- _:foo\n"
          "--[=[_:foo]=]\n"
          "return ARGV[4]")]
    [(is (= (car/prepare-lua script [:route] [:ns/value :foo-bar :foobar :foo])
            expected)
       "Preparation replaces exact code placeholders but not strings, comments, or prefixes")
     (is (= (car/prepare-lua "return {_:key, _:value}" [:key] [:value])
            "return {KEYS[1], ARGV[1]}"))
     (is (= (car/prepare-lua "obj_:key(x)" [:key] [])
            "obj_:key(x)")
       "Preparation does not replace a named variable suffix inside a Lua identifier")
     (is (= (car/prepare-lua "return x-_:n" [] [:n])
            "return x-ARGV[1]")
       "A preceding Lua operator does not suppress recognition")
     (is (= (car/prepare-lua "return 'p'.._:key" [:key] [])
            "return 'p'..KEYS[1]"))
     (is (= (car/prepare-lua "return x/_:d" [] [:d])
            "return x/ARGV[1]"))
     (is (= (car/prepare-lua "return _:foo" [] [:foo])
            (car/prepare-lua "return _:foo" [] ['foo])))
     (is (= (car/prepare-lua
              "-- _:foo\rreturn _:foo\r\n-- _:foo\n\rreturn _:foo"
              [] [:foo])
            "-- _:foo\rreturn ARGV[1]\r\n-- _:foo\n\rreturn ARGV[1]")
       "Lua CR, CRLF, and LFCR line endings terminate line comments")
     (let [supplementary-letter (String. (Character/toChars 0x10400))]
       (is (= (car/prepare-lua (str "return _:" supplementary-letter)
                [] [supplementary-letter])
              "return ARGV[1]")
         "Supplementary Unicode letters are valid variable names"))
     (let [error
           (truss/throws
             (car/prepare-lua
               "return {_:missing, '_:literal', _:other, _:missing}"
               [:routing-only] []))]
       [(is (truss/submap? (ex-data error)
              {:eid :carmine.script/unresolved-variable-names
               :variables ["missing" "other"]}))
        (is (= (car/prepare-lua "return 1" [:routing-only] []) "return 1")
          "Supplied variables may be absent for routing or positional use")])
     (is (throws? :ex-info {:eid :carmine.script/duplicate-variable-names}
           (car/prepare-lua "return 1" [] [:same 'same "same"])))
     (doseq [invalid ["" "has whitespace" "operator+"]]
       (is (throws? :ex-info {:eid :carmine.script/invalid-variable-name}
             (car/prepare-lua "return 1" [] [invalid]))))]))

(deftest _scripting
  (let [script-echo "return ARGV[1]"
        script-null "return nil"
        script-array "return {1, 2, 3}"
        script-error "return redis.error_reply('LUA_TEST')"]
    [(is (= (car/script-hash script-echo)
           (org.apache.commons.codec.digest.DigestUtils/sha1Hex script-echo)))

     (let [script-bytes (enc/str->utf8-ba script-echo)
           wire-script  (car/bytes script-bytes)
           [prepared-script prepared-hash]
           (#'scripts/prepare-script+hash wire-script)
           expected-bytes (vec script-bytes)]
       [(is (= (car/script-hash wire-script) (car/script-hash script-echo))
          "Exact-byte scripts hash the bytes Redis receives")
        (is (= (car/script-hash :foo)
              (org.apache.commons.codec.digest.DigestUtils/sha1Hex "foo"))
          "Native Redis argument encoding, not Clojure `str`, determines the hash")
        (aset-byte script-bytes 0 (byte (inc (aget script-bytes 0))))
        (is (= (vec (write/arg-payload-bytes prepared-script)) expected-bytes)
          "Script preparation snapshots mutable exact-byte inputs")
        (is (= prepared-hash
              (org.apache.commons.codec.digest.DigestUtils/sha1Hex
                ^bytes (byte-array expected-bytes))))])

     (let [script-bytes (enc/str->utf8-ba script-echo)]
       (doseq [auto-freeze? [true false]]
         (binding [car/*auto-freeze?* auto-freeze?]
           (let [prepared (write/prepare-arg script-bytes)]
             (is (= (car/script-hash script-bytes)
                   (org.apache.commons.codec.digest.DigestUtils/sha1Hex
                     ^bytes (write/arg-payload-bytes prepared)))
               "Hashing follows the active exact argument encoding")))))

     (let [prepared (car/prepare-lua "return {_:key, _:value}" [:key] [:value])]
       [(is (= (wcar mgr_ (car/eval prepared 1 "prepared-key" "prepared-value"))
               ["prepared-key" "prepared-value"]))
        (is (= (wcar mgr_ (car/evalsha* prepared 1 "prepared-key" "prepared-value"))
               ["prepared-key" "prepared-value"]))
        (is (= (wcar mgr_ (car/eval* prepared 1 "prepared-key" "prepared-value"))
               ["prepared-key" "prepared-value"]))])

     (is (= (wcar mgr_
              (car/script-flush)
              (car/eval* script-echo 0 "cold"))
            ["OK" "cold"])
       "A cold script cache falls back from EVALSHA to EVAL")

     (let [wire-script (car/bytes (enc/str->utf8-ba script-echo))]
       [(is (= (wcar mgr_
                (car/script-flush)
                (car/eval* wire-script 0 "cold-wire"))
              ["OK" "cold-wire"]))
        (is (= (wcar mgr_ (car/evalsha* wire-script 0 "warm-wire"))
              "warm-wire")
          "The fallback loads the same exact bytes used by subsequent EVALSHA")])

     (is (= (wcar mgr_
              (car/parse nil #(str "parsed-" %)
                (car/eval* script-echo 0 "once")))
            "parsed-once")
       "Fn parsers apply exactly once on the cached path")

     (is (= (wcar mgr_
              (car/script-flush)
              (car/parse-aggregates {} (map inc) (com/parsing-rf conj)
                (car/eval* script-array 0)))
            ["OK" [2 3 4]])
       "Reducing parsers apply on the cold fallback path")

     (is (= (wcar mgr_
              (car/parse-aggregates {} (map inc) (com/parsing-rf conj)
                (car/eval* script-array 0)))
            [2 3 4])
       "Reducing parsers apply on the cached path")

     (is (= (let [[flush-reply bytes-reply]
                  (wcar mgr_
                    (car/script-flush)
                    (car/as-bytes (car/eval* script-echo 0 "cold-bytes")))]
              [flush-reply (enc/utf8-ba->str bytes-reply)])
            ["OK" "cold-bytes"]))

     (is (= (enc/utf8-ba->str
              (wcar mgr_ (car/as-bytes (car/eval* script-echo 0 "warm-bytes"))))
            "warm-bytes"))

     (is (= (wcar mgr_
              (car/script-flush)
              (car/parse {:parse-null-replies? true}
                #(if (nil? %) :null :not-null)
                (car/eval* script-null 0)))
            ["OK" :null])
       "Null parsing is preserved on the fallback path")

     (is (= (wcar mgr_
              (car/parse {:parse-null-replies? true}
                #(if (nil? %) :null :not-null)
                (car/eval* script-null 0)))
            :null)
       "Null parsing is preserved on the cached path")

     (is (= (wcar mgr_
              (car/script-flush)
              (car/skip-replies (car/eval* script-echo 0 "ignored"))
              (car/ping))
            ["OK" "PONG"])
       "Skip mode still observes NOSCRIPT internally and omits success")

     (let [key (tk :lua-reply-parity)
           read-script "return redis.call('get', KEYS[1])"]
       (wcar mgr_ (car/set key {:value 1}) (car/script-flush))
       (let [cold (wcar mgr_ {:natural-replies? true}
                    (car/eval* read-script 1 key))
             warm (wcar mgr_ {:natural-replies? true}
                    (car/eval* read-script 1 key))]
         [(is (string? cold))
          (is (= cold warm)
            "Hard natural-reply boundaries are cache-independent")])

       (wcar mgr_ (car/set key nil) (car/script-flush))
       (let [call #(wcar mgr_
                    (car/parse nil (fn [x] (if (nil? x) :stored-nil :unexpected))
                      (car/eval* read-script 1 key)))]
         (is (= [(call) (call)] [:stored-nil :stored-nil])
           "Decoded Carmine nil remains distinct from RESP null")))

     (wcar mgr_ (car/script-flush))
     (let [call #(wcar mgr_
                  (car/as-bytes
                    (car/as-long (car/eval* "return '42'" 0))))]
       (is (= [(call) (call)] [42 42])
         "Function-parser read options are cache-independent"))

     (let [key (tk :lua-immediate-fallback)
           script "redis.call('set', KEYS[1], ARGV[1]); return 'OK'"]
       (wcar mgr_ (car/del key) (car/script-flush))
       [(is (throws? Exception "After eval*"
              (wcar mgr_
                (car/eval* script 1 key "ran")
                (throw (Exception. "After eval*")))))
        (is (= (wcar mgr_ (car/get key)) "ran")
          "A cold fallback executes before eval* returns")])

     (is (= (wcar mgr_
              (car/lua
                "return {redis.call('set', _:key, _:value), _:value}"
                (array-map :key (tk :lua))
                (array-map :value "lua-value")))
            ["OK" "lua-value"]))

     (is (= (wcar mgr_
              (car/lua "return KEYS[1]" [(tk :lua-sequential)] []))
            (tk :lua-sequential))
       "Sequential keys and arguments remain positional")

     (is (throws? :ex-info {:eid :carmine.script/duplicate-variable-names}
           (car/lua "return _:same"
             {:same (tk :lua-duplicate)} {:same "value"})))

     (is (throws? :ex-info {:eid :carmine.script/invalid-variable-name}
           (car/lua "return 1" {42 (tk :lua-invalid)} {})))

     (is (= (wcar mgr_
              (car/lua "return {'_:literal', _:ns/value, _:foobar, _:foo}"
                {:route (tk :lua-routing-only)}
                (array-map :ns/value "qualified" :foobar "long" :foo "short")))
            ["_:literal" "qualified" "long" "short"])
       "Substitution preserves literals, qualified names, prefixes, and routing-only keys")

     (let [[flush-reply cold-error]
           (wcar mgr_ {:error-mode :return}
             (car/script-flush)
             (car/eval* script-error 0))
           warm-error
           (wcar mgr_ {:error-mode :return}
             (car/eval* script-error 0))]
       [(is (= flush-reply "OK"))
        (is (com/reply-error? cold-error))
        (is (com/reply-error? warm-error))
        (is (= (:code (ex-data cold-error)) (:code (ex-data warm-error))))])

     (doseq [cold? [true false]]
       (when cold? (wcar mgr_ (car/script-flush)))
       (let [error
             (truss/throws
               (wcar mgr_
                 (car/parse {}
                   (fn [_] (throw (ex-info "Expected Lua parser failure" {:cold? cold?})))
                   (car/eval* script-echo 0 "parser-error"))))]
         [(is (= (:eid (ex-data (ex-cause error))) :carmine.read/parser-error))
          (is (= (wcar mgr_ (car/ping)) "PONG")
            "A drained parser error leaves the connection reusable")]))

     (when (>= (redis-major-version) 7)
       (let [key (tk :lua-ro)
             read-script "return redis.call('get', KEYS[1])"
             write-script "return redis.call('set', KEYS[1], ARGV[1])"]
         [(is (= (wcar mgr_
                  (car/set key "read-only-value")
                  (car/script-flush)
                  (car/eval-ro* read-script 1 key))
                ["OK" "OK" "read-only-value"])
            "Read-only fallback works on a cold script cache")
          (is (= (wcar mgr_ (car/eval-ro* read-script 1 key)) "read-only-value"))
          (is (= (wcar mgr_
                   (car/lua-ro "return redis.call('get', _:key)" {:key key} {}))
                 "read-only-value"))
          (is (com/reply-error?
                (wcar mgr_ {:error-mode :return}
                  (car/lua-ro write-script [key] ["forbidden"])))
            "Redis rejects writes through the read-only helper")]))

     (doseq [[called f] [['eval*    car/eval*]
                         ['eval-ro* car/eval-ro*]]]
       (let [error (truss/throws (f script-echo 0))]
         (is (truss/submap? (ex-data error)
               {:eid :carmine/no-context, :called called}))))]))

(deftest _compare-and-set
  (let [string-key (tk :cas/string)
        ttl-key    (tk :cas/string-ttl)
        bytes-key  (tk :cas/bytes)
        hash-key   (tk :cas/hash)
        wrong-string-key (tk :cas/wrong-string)
        wrong-hash-key   (tk :cas/wrong-hash)
        keys [string-key ttl-key bytes-key hash-key wrong-string-key wrong-hash-key]]
    (wcar mgr_ :as-vec (doseq [key keys] (car/del key)))
    (try
      [(testing "String CAS is pipelineable and distinguishes missing from stored nil"
         (is (= (wcar mgr_ :as-vec
                  (car/compare-and-set string-key nil :unexpected)
                  (car/set string-key nil)
                  (car/compare-and-set string-key nil {:version 1})
                  (car/compare-and-set string-key nil :stale)
                  (resp/local-echo (resp/ctx-pending-request-count))
                  (car/get string-key))
                [false "OK" true false 4 {:version 1}])
           "All four Redis operations remain queued until the enclosing flush")

         (is (= (wcar mgr_ :as-vec
                  (car/compare-and-set string-key {:version 1} :cas/delete)
                  (car/get string-key)
                  (car/compare-and-delete string-key :cas/delete)
                  (car/compare-and-delete string-key :cas/delete))
                [true "cas/delete" true false])
           "The v3 delete sentinel is an ordinary storable v4 value")

         (is (false?
               (wcar mgr_
                 (car/compare-and-set string-key
                   (apply str (repeat 1000 "missing")) "replacement")))
           "A large expected value against a missing key is a clean mismatch"))

       (testing "String replacement clears TTL and deletion is conditional"
         (let [[set-reply mismatch ttl-before replaced ttl-after stale deleted
                repeated exists]
               (wcar mgr_ :as-vec
                 (car/psetex ttl-key 60000 "before")
                 (car/compare-and-set ttl-key "wrong" "ignored")
                 (car/pttl ttl-key)
                 (car/compare-and-set ttl-key "before" "after")
                 (car/pttl ttl-key)
                 (car/compare-and-delete ttl-key "wrong")
                 (car/compare-and-delete ttl-key "after")
                 (car/compare-and-delete ttl-key "after")
                 (car/exists ttl-key))]
           [(is (= [set-reply mismatch replaced ttl-after stale deleted repeated exists]
                  ["OK" false true -1 false true false 0]))
            (is (pos? ttl-before)
              "A mismatch leaves the existing string TTL unchanged")]))

       (testing "Explicit byte encoding compares by content"
         (let [expected    (byte-array [(byte 0) (byte 1) (byte 2) (byte 127)])
               expected-2 (byte-array [(byte 0) (byte 1) (byte 2) (byte 127)])
               replacement (byte-array [(byte 9) (byte 8) (byte 7)])]
           (is (= (wcar mgr_ :as-vec
                    (car/set bytes-key (car/bytes expected))
                    (car/compare-and-set bytes-key
                      (car/bytes expected-2) (car/bytes replacement)))
                  ["OK" true]))
           (is (= (seq (wcar mgr_ (car/as-bytes (car/get bytes-key))))
                  (seq replacement)))))

       (testing "Hash CAS preserves TTL and distinguishes missing from stored nil"
         (let [[hset-keep expiry missing hset-nil replaced ttl-before stale deleted
                field-exists ttl-after keep-value]
               (wcar mgr_ :as-vec
                 (car/hset hash-key "keep" "yes")
                 (car/pexpire hash-key 60000)
                 (car/compare-and-hset hash-key "field" nil :unexpected)
                 (car/hset hash-key "field" nil)
                 (car/compare-and-hset hash-key "field" nil [:version 1])
                 (car/pttl hash-key)
                 (car/compare-and-hdel hash-key "field" nil)
                 (car/compare-and-hdel hash-key "field" [:version 1])
                 (car/hexists hash-key "field")
                 (car/pttl hash-key)
                 (car/hget hash-key "keep"))]
           [(is (= [hset-keep expiry missing hset-nil replaced stale deleted field-exists keep-value]
                  [1 1 false 1 true false true 0 "yes"]))
            (is (pos? ttl-before))
            (is (pos? ttl-after))]))

       (testing "Wrong Redis types retain ordinary Redis errors"
         (wcar mgr_
           (car/set wrong-string-key "string")
           (car/hset wrong-hash-key "field" "hash"))
         (let [[hash-error string-error]
               (wcar mgr_ {:as-vec? true, :error-mode :return}
                 (car/compare-and-hset wrong-string-key "field" "string" "new")
                 (car/compare-and-set wrong-hash-key "hash" "new"))]
           [(is (com/reply-error? hash-error))
            (is (com/reply-error? string-error))]))

       (testing "Natural replies deliberately bypass the boolean parser"
         (is (= (wcar mgr_ {:as-vec? true, :natural-replies? true}
                  (car/set string-key "natural")
                  (car/compare-and-set string-key "natural" "updated")
                  (car/compare-and-delete string-key "updated"))
                ["OK" 1 1])))

       (testing "Skip mode discards the CAS reply without skipping its mutation"
         (is (= (wcar mgr_ :as-vec
                  (car/set string-key "skip")
                  (car/skip-replies
                    (car/compare-and-set string-key "skip" "updated"))
                  (car/get string-key))
                ["OK" "updated"])))

       (testing "Invalid values fail before an earlier queued command is sent"
         (let [error
               (binding [car/*auto-freeze?* false]
                 (truss/throws
                   (wcar mgr_ :as-vec
                     (car/set string-key "must-not-be-sent")
                     (car/compare-and-set string-key (Object.) "new"))))]
           [(is (truss/submap? (ex-data error)
                  {:eid :carmine.write/non-native-arg-type}))
            (is (= (wcar mgr_ (car/get string-key)) "updated")
              "The pre-existing value proves the earlier SET was not sent")]))

       (testing "RESP2 and RESP3 both return booleans"
         (doseq [resp3? (support/supported-resp3-options)]
           (with-open [mgr (car/conn-manager-unpooled
                             {:conn-opts {:init {:resp3? resp3?}}})]
             (wcar mgr (car/del string-key))
             (is (= (wcar mgr :as-vec
                      (car/set string-key "old")
                      (car/compare-and-set string-key "old" "new")
                      (car/compare-and-delete string-key "new"))
                    ["OK" true true])))))

       (doseq [[called f]
               [['compare-and-set    #(car/compare-and-set    string-key "old" "new")]
                ['compare-and-delete #(car/compare-and-delete string-key "old")]
                ['compare-and-hset   #(car/compare-and-hset   hash-key "field" "old" "new")]
                ['compare-and-hdel   #(car/compare-and-hdel   hash-key "field" "old")]]]
         (is (throws? :ex-info {:eid :carmine/no-context, :called called} (f))))]
      (finally
        (wcar mgr_ :as-vec (doseq [key keys] (car/del key)))))))
