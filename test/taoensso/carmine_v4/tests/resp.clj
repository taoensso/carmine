(ns taoensso.carmine-v4.tests.resp
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc  :refer [throws?]]

   [taoensso.carmine-v4.resp.common :as com
    :refer [str->bytes bytes->str xs->in+ throw!]]

   [taoensso.carmine-v4.resp.read :as read]
   [taoensso.carmine-v4 :as core]))

(comment
  (remove-ns      'taoensso.carmine-v4.tests.resp)
  (test/run-tests 'taoensso.carmine-v4.tests.resp)
  (core/run-all-carmine-tests))

;;;;

(enc/defalias ^:private rr read/read-reply)

(deftest ^:private _read-reply
  [(testing "Basics"
     [(is (= (rr (xs->in+ "*10" "+simple string"
                   ":1" ",1" ",1.5" ",inf" ",-inf" "(1" "#t" "#f" "_"))
            ["simple string" 1 1.0 1.5 ##Inf ##-Inf 1N true false nil]))

      (is (= (rr (xs->in+ "$7" "hello\r\n")) "hello\r\n") "Binary safe")
      (is (= (rr (xs->in+ "$?" ";5" "hello" ";9" " world!\r\n" ";0")) "hello world!\r\n") "Streaming")])

   (testing "Basic aggregates"
     [(is (= (rr (xs->in+ "*3" ":1" ":2" "+3")) [1 2 "3"]))
      (is (= (binding [core/*keywordize-maps?* true]  (rr (xs->in+ "%2" "+k1" "+v1" ":2" "+v2"))) {:k1  "v1", 2 "v2"}))
      (is (= (binding [core/*keywordize-maps?* false] (rr (xs->in+ "%2" "+k1" "+v1" ":2" "+v2"))) {"k1" "v1", 2 "v2"}))

      (is (= (rr (xs->in+ "*3" ":1" "$?" ";4" "bulk" ";6" "string" ";0" ",1.5")) [1 "bulkstring" 1.5]))

      (is (=                             (rr (xs->in+ "*2" ":1" "$3" [\a \b \c])) [1 "abc"]) "Baseline...")
      (is (let [[x y] (com/as-bytes (rr (xs->in+ "*2" ":1" "$3" [\a \b \c])))]
            [(is (= x 1))
             (is (= (bytes->str y) "abc"))])
        "`as-bytes` penetrates aggregates")])

   (testing "Errors"
     [(testing "Simple errors"
        [(let [r1 (rr (xs->in+ "-ERR Foo bar baz"))]
           (is (com/reply-error?
                 {:eid :carmine.read/error-reply
                  :message "ERR Foo bar baz"
                  :code    "ERR"}
                 r1)))

         (let [[r1 r2 r3 r4] (rr (xs->in+ "*4" ":1" "-CODE1 a" ":2" "-CODE2 b"))]
           [(is (= r1 1))
            (is (= r3 2))
            (com/reply-error? {:eid :carmine.read/error-reply :code "CODE1" :message "CODE1 a"} r2)
            (com/reply-error? {:eid :carmine.read/error-reply :code "CODE2" :message "CODE2 b"} r4)])])

      (testing "Bulk errors"
        [(let [r1 (rr (xs->in+ "!10" "CODE Foo\r\n"))]
           (is (com/reply-error?
                 {:eid :carmine.read/error-reply
                  :message "CODE Foo\r\n"
                  :code    "CODE"}
                 r1)
             "Binary safe"))

         (let [[r1 r2 r3 r4] (rr (xs->in+ "*4" ":1" "!9" "CODE1 a\r\n" ":2" "!9" "CODE2 b\r\n"))]
           [(is (= r1 1))
            (is (= r3 2))
            (com/reply-error? {:eid :carmine.read/error-reply :code "CODE1" :message "CODE1 a\r\n"} r2)
            (com/reply-error? {:eid :carmine.read/error-reply :code "CODE2" :message "CODE2 b\r\n"} r4)])])])

   (testing "Nested aggregates"
     [(is (= [[1 "2" 3] ["a" "b"] []]
             (rr (xs->in+
                          "*3"
                          "*3" ":1" "+2" ":3"
                          "*2" "+a" "+b"
                          "*0"))))

      (is (= [#{1 3 "2"} {:k1 "v1", 2 "v2"} [["a" "b"] [] #{} {}]]
             (rr
               (xs->in+
                 "*3"
                 "~3" ":1" "+2" ":3"
                 "%2" "+k1" "+v1" ":2" "+v2"
                 "*4"
                 "*2" "+a" "+b"
                 "*0"
                 "~0"
                 "%0"))))

      (is (= {[1 "2" 3] #{1 3 "2"},
              {:k1 "v1"} {:k1 "v1", 2 2},
              #{"a" "b"} #{1 2}}

            (rr
              (xs->in+
                "%3"
                "*3" ":1" "+2" ":3"            ; Array key
                "~?" ":1" "+2" ":3" "."        ; Set val
                "%1" "+k1" "+v1"               ; Map key
                "%?" "+k1" "+v1" ":2" ":2" "." ; Map val
                "~2" "+a" "+b"                 ; Set key
                "~?" ":1" ":2" "."             ; Set val
                ))))])

   (testing "Misc types"
     [(is (= (rr (xs->in+ "=11" "txt:hello\r\n")) [:carmine/verbatim-string "txt" "hello\r\n"])
        "Verbatim string")

      (is (enc/submap?
            {:carmine/attributes {:key-popularity {:a 0.1923 :b 0.0012}}}
            (meta
              (rr (xs->in+ "|1" "+key-popularity"
                           "%2" "$1" "a" ",0.1923" "$1" "b" ",0.0012"
                           "*2" ":2039123" ":9543892"))))
        "Attributes")])

   (testing "Pushes"
     ;; Push replies can be received at any time, but only at the top level
     ;; (e.g. not within the middle of a map reply)
     [(let [p_ (promise)
            pf (fn [dv] (deliver p_ dv))

            reply
            (binding [core/*push-fn* pf]
              (rr
                (xs->in+
                  ">4" "+pubsub" "+message" "+channel" "+message content"
                  "$9" "get reply")))]

        [(is (= reply "get reply"))
         (is (= (deref p_ 0 nil) ["pubsub" "message" "channel" "message content"]))])])])

(defn- parser-error?
  ([        x] (parser-error? nil x))
  ([subdata x]
   (com/reply-error?
     (assoc subdata :eid :carmine.read/parser-error)
     x)))

(deftest ^:private _read-reply-with-parsing
  [(testing "fn parsers"
     [(testing "Against non-aggregates"
        [(is (=                  (rr (xs->in+  "+1")) "1"))
         (is (=   (com/as-long   (rr (xs->in+  "+1"))) 1))
         (is (=   (com/as-double (rr (xs->in+  "+1"))) 1.0))
         (is (->> (com/as-long   (rr (xs->in+  "+s"))) parser-error?))
         (is (=   (com/as-?long  (rr (xs->in+  "+s"))) nil))
         (is (=                  (rr (xs->in+ "+kw"))  "kw"))
         (is (=   (com/as-kw     (rr (xs->in+ "+kw"))) :kw))

         (is (=   (com/parse {} (fn [x] (str x "!")) (rr (xs->in+ "+1"))) "1!"))
         (is (->> (com/parse {} throw!               (rr (xs->in+ "+1"))) parser-error?))

         (testing "With parser opts"
           [(testing ":parse-null-replies?"
              [(is (= (com/parse {}                          (fn [_] :parsed) (rr (xs->in+ "_"))) nil))
               (is (= (com/parse {:parse-null-replies? true} (fn [_] :parsed) (rr (xs->in+ "_"))) :parsed))])

            (testing ":parse-error-replies?"
              [(is (-> (com/parse {}                           (fn [_] :parsed) (rr (xs->in+ "-err"))) com/reply-error?))
               (is (=  (com/parse {:parse-error-replies? true} (fn [_] :parsed) (rr (xs->in+ "-err"))) :parsed))])

            (testing ":read-mode"
              [(is (= (com/parse {:read-mode :bytes} bytes->str                 (rr (xs->in+ "$5" "hello")))  "hello")  "Parser  read mode (:bytes)")
               (is (= (com/parse {}                  bytes->str   (com/as-bytes (rr (xs->in+ "$5" "hello")))) "hello")  "Dynamic read mode (:bytes)")
               (is (= (com/parse {:read-mode nil}    #(str % "!") (com/as-bytes (rr (xs->in+ "$5" "hello")))) "hello!") "Parser  read mode (nil)")])])])

      (testing "Against aggregates"
        [(is (=                        (rr (xs->in+ "*2" ":1" ":2"))              [1 2])    "Baseline...")
         (is (=   (com/parse {} set    (rr (xs->in+ "*2" ":1" ":2")))            #{1 2})    "Acts as (f <aggr>)")
         (is (=                        (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))   [[1 2] 3]) "Baseline...")
         (is (=   (com/parse {} set    (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) #{[1 2] 3}) "No nesting")
         (is (->> (com/parse {} throw! (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) parser-error?))])])

   (testing "rf parsers"
     [(testing "Against aggregates"
        [(is (=                                                                (rr (xs->in+ "*4" ":1" ":2" ":3" ":4"))    [1 2 3 4])    "Baseline...")
         (is (=   (com/parse-aggregates {} nil            (com/crf conj   #{}) (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  #{1 2 3 4})    "Parsed (without xform)")
         (is (=   (com/parse-aggregates {} (filter even?) (com/crf conj   #{}) (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  #{  2   4})    "Parsed (with    xform)")
         (is (->> (com/parse-aggregates {} (map throw!)   (com/crf conj   #{}) (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  parser-error?) "Trap xform errors")
         (is (->> (com/parse-aggregates {} (map identity) (com/crf throw! #{}) (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  parser-error?) "Trap rf    errors")
         (is (=   (com/parse-aggregates {} (map identity) (com/crf conj   #{}) (rr (xs->in+ "*4" ":1" "_"  ":2"  "_")))  #{nil 1 2})    "Nulls in aggregate")


         (is (=                                                                          (rr (xs->in+ "*4" ":1" ":2" ":3" ":4"))   [1 2 3 4]) "Baseline...")
         (is (= (com/parse-aggregates {} nil (com/crf conj! (transient #{}) persistent!) (rr (xs->in+ "*4" ":1" ":2" ":3" ":4"))) #{1 2 3 4}) "Using transients")

         (is (=                                                                                    (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2")) {:k1  1, :k2  2})  "Baseline...")
         (is (= (com/parse-aggregates {}             nil (com/crf (fn [m [k v]] (assoc m k v)) {}) (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2")) {"k1" 1, "k2" 2})) "Ignore *keywordize-maps?*")
         (is (= (com/parse-aggregates {:kv-rf? true} nil (com/crf (fn [m  k v]  (assoc m k v)) {}) (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2")) {"k1" 1, "k2" 2})) "With kv-rf")

         (is (= (com/parse-aggregates {}
                  (filter (fn [[k v]] (even? v)))
                  (com/crf (fn [m [k v]] (assoc m k v)) {})
                  (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2"))) {"k2" 2}) "Aggregate map, with xform")

         (is (=                                                            (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))   [[1 2] 3]) "Baseline...")
         (is (= (com/parse-aggregates {} nil            (com/crf conj #{}) (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) #{[1 2] 3}) "No nesting (without xform)")
         (is (= (com/parse-aggregates {} (map identity) (com/crf conj #{}) (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) #{[1 2] 3}) "No nesting (with    xform)")])

      (testing "Against non-aggregates"
        [(is (= (com/parse-aggregates {} (map throw!) throw! (rr (xs->in+ "_")))         nil)  "No effect")
         (is (= (com/parse-aggregates {} (map throw!) throw! (rr (xs->in+ "+hello"))) "hello") "No effect")])])])
