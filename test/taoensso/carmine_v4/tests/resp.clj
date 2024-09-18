(ns taoensso.carmine-v4.tests.resp
  "Low-level RESP protocol tests.
  These don't need a running Redis server."
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc  :refer [throws?]]
   [taoensso.nippy   :as nippy]

   [taoensso.carmine-v4.resp.common :as com
    :refer [xs->in+ throw!]]

   [taoensso.carmine-v4.resp.read  :as read]
   [taoensso.carmine-v4.resp.write :as write]
   [taoensso.carmine-v4 :as core])

  (:import
   [taoensso.carmine_v4.resp.common #_ReadOpts ReadThawed #_Parser]
   [taoensso.carmine_v4.resp.write WriteFrozen]
   [taoensso.carmine_v4.resp.read VerbatimString]))

(comment
  (remove-ns      'taoensso.carmine-v4.tests.resp)
  (test/run-tests 'taoensso.carmine-v4.tests.resp))

;;;; Common

(deftest _byte-strings  [(is (= (enc/utf8-ba->str (enc/str->utf8-ba enc/a-utf8-str)) enc/a-utf8-str) "UTF_8 charset for byte strings")])
(deftest _with-out->str [(is (= (com/with-out->str (.write out (enc/str->utf8-ba enc/a-utf8-str))) enc/a-utf8-str))])
(deftest _with-out->in  [(is (= (.readLine (com/with-out->in (.write out (enc/str->utf8-ba "hello\r\n")))) "hello"))])
(deftest _skip1         [(is (= (.readLine (com/skip1 (com/with-out->in (.write out (enc/str->utf8-ba "+hello\r\n"))))) "hello"))])

(deftest _xs->ba
  [(is (= (enc/utf8-ba->str (com/xs->ba  "a" "b" 1 (byte-array [(int \A) (int \B)]) \C [\d \e])) "ab1ABCde"))
   (is (= (enc/utf8-ba->str (com/xs->ba+ "a" "b" 1 (byte-array [(int \A) (int \B)]) \C [\d \e])) "a\r\nb\r\n1\r\nAB\r\nC\r\nde\r\n"))])

(defn- test-blob-?marker [s]
  (let [^bytes ba (enc/str->utf8-ba s)
        in (com/ba->in ba)]
    [(com/read-blob-?marker in (alength ba))
     (.readLine             in)]))

(deftest _read-blob-?marker
  [(is (= (test-blob-?marker "foo")            [nil "foo"]))
   (is (= (test-blob-?marker "\u0000more")     [nil "\u0000more"]))
   (is (= (test-blob-?marker "\u0000_more")    [:nil "more"]))
   (is (= (test-blob-?marker "\u0000>more")    [:npy "more"]))
   (is (= (test-blob-?marker "\u0000<more")    [:bin "more"]))
   (is (= (binding [core/*issue-83-workaround?* true ] (test-blob-?marker "\u0000<NPYmore")) [:npy "NPYmore"]))
   (is (= (binding [core/*issue-83-workaround?* false] (test-blob-?marker "\u0000<NPYmore")) [:bin "NPYmore"]))
   (is (= (test-blob-?marker "\u0000<NPmore")  [:bin "NPmore"]))
   (is (= (test-blob-?marker "\u0000<Nmore")   [:bin "Nmore"]))])

(deftest _stream-discards
  [(is (->>   (com/discard-stream-separator (com/xs->in+ ""))  (throws? :common {:eid :carmine.read/missing-stream-separator})))
   (is (->>   (com/discard-crlf             (com/xs->in+ "_")) (throws? :common {:eid :carmine.read/missing-crlf})))
   (is (true? (com/discard-crlf             (com/xs->in+ ""))))])

(defn- test-rf-parser [kvs? ?xform rf init coll]
  (let [rf* ((.-rfc (com/rf-parser {} ?xform rf)))]
    (identity ; As (rf* completing [acc] acc)
      (if kvs?
        (reduce-kv rf* init coll)
        (reduce    rf* init coll)))))

(deftest _rf-parser
  [(testing "Basics"
     [(is (=   (test-rf-parser false nil (fn [acc in] (conj acc in)) [] [:a :b]) [:a :b]))
      (is (->> (test-rf-parser false nil (fn [acc in] (throw!   in)) [] [:a :b])
            (com/reply-error? {:thrown-by :rf})) "Identifies rf error")

      (is (=   (test-rf-parser false (map identity) (fn [acc in] (conj acc in)) [] [:a :b]) [:a :b]))
      (is (->> (test-rf-parser false (map throw!)   (fn [acc in] (conj acc in)) [] [:a :b])
            (com/reply-error? {:thrown-by :xform})) "Identifies xform error")

      (is (=   (test-rf-parser true nil (fn [acc k v] (assoc acc k v))  {} {:a :A}) {:a :A}))
      (is (->> (test-rf-parser true nil (fn [acc k v] (throw!   [k v])) {} {:a :A})
            (com/reply-error? {:thrown-by :rf}))
        "kv-rf supported when no user-supplied xform")])

   (testing "Stateful short-circuiting"
     (let [xform (map        (fn [    in] (if (and (int? in) (neg? ^long in)) (throw! in) in)))
           rf    (completing (fn [acc in] (if (and (int? in) (odd? ^long in)) (throw! in) in)))]

       [(testing "Permanently short-circuit on rf error"
          (let [rf* ((.-rfc (com/rf-parser {} xform rf)))]
            [(is (=   (rf* :acc   ) :acc))
             (is (=   (rf* :acc  2) 2))
             (is (->> (rf* :acc  3) (com/reply-error? {:thrown-by :rf :args {:in {:value 3}}})))
             (is (->> (rf* :acc  2) (com/reply-error? {:thrown-by :rf :args {:in {:value 3}}})))
             (is (->> (rf* :acc -2) (com/reply-error? {:thrown-by :rf :args {:in {:value 3}}})))]))

        (testing "Permanently short-circuit on xform error"
          (let [rf* ((.-rfc (com/rf-parser {} xform rf)))]
            [(is (=   (rf* :acc   ) :acc))
             (is (=   (rf* :acc  2) 2))
             (is (->> (rf* :acc -2) (com/reply-error? {:thrown-by :xform :args {:in {:value -2}}})))
             (is (->> (rf* :acc  2) (com/reply-error? {:thrown-by :xform :args {:in {:value -2}}})))
             (is (->> (rf* :acc  3) (com/reply-error? {:thrown-by :xform :args {:in {:value -2}}})))]))]))])

;;;; Read

(enc/defalias rr read/read-reply)

(deftest _read-reply
  [(testing "Basics"
     [(is (= (rr (xs->in+ "*10" "+simple string" ":1" ",1" ",1.5" ",inf" ",-inf" "(1" "#t" "#f" "_"))
             ["simple string" 1 1.0 1.5 ##Inf ##-Inf 1N true false nil]))

      (is (= (rr (xs->in+ "$7" "hello\r\n")) "hello\r\n") "Binary safe")
      (is (= (rr (xs->in+ "$?" ";5" "hello" ";9" " world!\r\n" ";0")) "hello world!\r\n") "Streaming")])

   (testing "Basic aggregates"
     [(is (=                                          (rr (xs->in+ "*3" ":1" ":2" "+3"))          [1 2 "3"]))
      (is (= (binding [core/*keywordize-maps?* true]  (rr (xs->in+ "%2" "+k1" "+v1" ":2" "+v2"))) {:k1  "v1", 2 "v2"}))
      (is (= (binding [core/*keywordize-maps?* false] (rr (xs->in+ "%2" "+k1" "+v1" ":2" "+v2"))) {"k1" "v1", 2 "v2"}))
      (is (= (rr (xs->in+ "*3" ":1" "$?" ";4" "bulk" ";6" "string" ";0" ",1.5")) [1 "bulkstring" 1.5]))

      (is (=                        (rr (xs->in+ "*2" ":1" "$3" [\a \b \c])) [1 "abc"]) "Baseline...")
      (is (let [[x y] (com/as-bytes (rr (xs->in+ "*2" ":1" "$3" [\a \b \c])))]
            [(is (= x 1))
             (is (= (enc/utf8-ba->str y) "abc"))])
        "`bytes` penetrates aggregates")])

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

   (testing "Verbatim string type"
     [(is (= (binding [core/*raw-verbatim-strings?* true ] (rr (xs->in+ "=11" "txt:hello\r\n"))) (VerbatimString. "txt", "hello\r\n")))
      (is (= (binding [core/*raw-verbatim-strings?* false] (rr (xs->in+ "=11" "txt:hello\r\n")))                         "hello\r\n"))])

   (testing "Attribute map type"
     [(is (enc/submap?
            {:carmine/attributes {:key-popularity {:a 0.1923 :b 0.0012}}}
            (meta
              (rr (xs->in+ "|1" "+key-popularity"
                    "%2" "$1" "a" ",0.1923" "$1" "b" ",0.0012"
                    "*2" ":2039123" ":9543892")))))])

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

(defn parser-error?
  ([        x] (parser-error? nil x))
  ([subdata x]
   (com/reply-error?
     (assoc subdata :eid :carmine.read/parser-error)
     x)))

(deftest _read-reply-with-parsing
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
              [(is (= (com/parse {:read-mode :bytes} enc/utf8-ba->str               (rr (xs->in+ "$5" "hello")))  "hello")  "Parser  read mode (:bytes)")
               (is (= (com/parse {}                  enc/utf8-ba->str (com/as-bytes (rr (xs->in+ "$5" "hello")))) "hello")  "Dynamic read mode (:bytes)")
               (is (= (com/parse {:read-mode nil}    #(str % "!")     (com/as-bytes (rr (xs->in+ "$5" "hello")))) "hello!") "Parser  read mode (nil)")])])])

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

(defn- empty-bytes? [ba] (enc/ba= ba (byte-array 0)))

(deftest _read-blob
  [(testing "Basics"
     [(is (= ""                      (#'read/read-blob nil              nil (xs->in+  0))) "As default: empty blob")
      (is (empty-bytes?              (#'read/read-blob :bytes           nil (xs->in+  0))) "As bytes:   empty blob")
      (is (= com/sentinel-null-reply (#'read/read-blob nil              nil (xs->in+ -1))) "As default: RESP2 null")
      (is (= com/sentinel-null-reply (#'read/read-blob :bytes           nil (xs->in+ -1))) "As bytes:   RESP2 null")
      (is (= com/sentinel-null-reply (#'read/read-blob (ReadThawed. {}) nil (xs->in+ -1))) "As thawed:  RESP2 null")

      (is (=                   (#'read/read-blob nil    nil (xs->in+ 5 "hello"))  "hello"))
      (is (= (enc/utf8-ba->str (#'read/read-blob :bytes nil (xs->in+ 5 "hello"))) "hello"))

      (is (=                   (#'read/read-blob nil    nil (xs->in+ 7 "hello\r\n"))  "hello\r\n") "Binary safe")
      (is (= (enc/utf8-ba->str (#'read/read-blob :bytes nil (xs->in+ 7 "hello\r\n"))) "hello\r\n") "Binary safe")

      (let [pattern {:eid :carmine.read/missing-crlf}]
        [(is (throws? :common pattern (#'read/read-blob nil    nil (com/str->in "5\r\nhello"))))
         (is (throws? :common pattern (#'read/read-blob :bytes nil (com/str->in "5\r\nhello"))))
         (is (throws? :common pattern (#'read/read-blob nil    nil (com/str->in "5\r\nhello__"))))
         (is (throws? :common pattern (#'read/read-blob :bytes nil (com/str->in "5\r\nhello__"))))])])

   (testing "Streaming"
     [(is (=                   (#'read/read-blob nil    nil (xs->in+ "?" ";5" "hello" ";1" " " ";6" "world!" ";0"))  "hello world!"))
      (is (= (enc/utf8-ba->str (#'read/read-blob :bytes nil (xs->in+ "?" ";5" "hello" ";1" " " ";6" "world!" ";0"))) "hello world!"))

      (let [pattern {:eid :carmine.read/missing-stream-separator}]
        [(is (throws? :common pattern (#'read/read-blob nil    nil (xs->in+ "?" ";5" "hello" "1" " " ";6" "world!" ";0"))))
         (is (throws? :common pattern (#'read/read-blob :bytes nil (xs->in+ "?" ";5" "hello" "1" " " ";6" "world!" ";0"))))])])

   (testing "Marked blobs"
     ;; See also `common/_read-blob-?marker` tests
     [(is (=                   (#'read/read-blob nil true  (xs->in+ 5 "\u0000more"))   "\u0000more"))
      (is (=                   (#'read/read-blob nil true  (xs->in+ 2 "\u0000_"))               nil))
      (is (=                   (#'read/read-blob nil false (xs->in+ 2 "\u0000_"))         "\u0000_"))
      (is (= (enc/utf8-ba->str (#'read/read-blob nil true  (xs->in+ 6 "\u0000<more")))       "more"))
      (is (=                   (#'read/read-blob nil false (xs->in+ 6 "\u0000<more")) "\u0000<more"))

      (testing "Mark ba still removed from returned bytes, even with :bytes read mode"
        [(is (= (enc/utf8-ba->str (#'read/read-blob nil    true (xs->in+ "5" (com/xs->ba com/ba-bin [\a \b \c])))) "abc"))
         (is (= (enc/utf8-ba->str (#'read/read-blob :bytes true (xs->in+ "5" (com/xs->ba com/ba-bin [\a \b \c])))) "abc"))])

      (let [data       (nippy/stress-data {:comparable? true})
            ba         (nippy/freeze data)
            marked-ba  (com/xs->ba com/ba-npy ba)
            marked-len (alength ^bytes marked-ba)]

        (is (= (#'read/read-blob nil true (xs->in+ marked-len marked-ba)) data) "Simple Nippy data"))

      (let [data       (nippy/stress-data {:comparable? true})
            pwd        [:salted "secret"]
            ba         (nippy/freeze data {:password pwd})
            marked-ba  (com/xs->ba com/ba-npy ba)
            marked-len (alength ^bytes marked-ba)]

        [(is (= (#'read/read-blob (ReadThawed. {:password pwd}) true (xs->in+ marked-len marked-ba)) data)
           "Encrypted Nippy data (good password)")

         (let [r (#'read/read-blob nil true (xs->in+ marked-len marked-ba))]
           [(is (com/reply-error? {:eid :carmine.read.blob/nippy-thaw-error} r) "Encrypted Nippy data (bad password)")
            (is (enc/ba= (-> r ex-data :bytes :content) ba) "Unthawed Nippy data still provided")])])])])

(deftest _read-aggregate-by-ones-bootstrap
  ;; Very basic bootstrap tests using only `read-basic-reply`
  [(is (= (#'read/read-aggregate-by-ones [] com/read-opts-default nil (xs->in+  0))                      []) "Empty blob")
   (is (= (#'read/read-aggregate-by-ones [] com/read-opts-default nil (xs->in+ -1)) com/sentinel-null-reply) "RESP2 null")

   (is (= (#'read/read-aggregate-by-ones [] com/read-opts-default #'read/read-basic-reply (xs->in+ 2   ":1" ":2"))     [1 2]))
   (is (= (#'read/read-aggregate-by-ones [] com/read-opts-default #'read/read-basic-reply (xs->in+ "?" ":1" ":2" ".")) [1 2]) "Streaming")])

(deftest _read-aggregate-by-pairs-bootstrap
  ;; Very basic bootstrap tests using only `read-basic-reply`
  [(testing "Basics"
     [(is (= (#'read/read-aggregate-by-pairs com/read-opts-default nil (xs->in+  0))                      {}) "Empty blob")
      (is (= (#'read/read-aggregate-by-pairs com/read-opts-default nil (xs->in+ -1)) com/sentinel-null-reply) "RESP2 null")

      (is (= (#'read/read-aggregate-by-pairs com/read-opts-default #'read/read-basic-reply (xs->in+ 2 "+k1" "+v1" "+k2" "+v2")) {:k1  "v1" :k2 "v2"}) "With keywordize")
      (is (= (#'read/read-aggregate-by-pairs com/read-opts-natural #'read/read-basic-reply (xs->in+ 2 "+k1" "+v1"  ":2" "+v2")) {"k1" "v1",  2 "v2"}) "W/o  keywordize")

      (is (= (#'read/read-aggregate-by-pairs com/read-opts-default #'read/read-basic-reply (xs->in+ "?" "+k1" "+v1" ":2" "+v2" ".")) {:k1  "v1"  2 "v2"}) "Streaming, with keywordize")
      (is (= (#'read/read-aggregate-by-pairs com/read-opts-natural #'read/read-basic-reply (xs->in+ "?" "+k1" "+v1" ":2" "+v2" ".")) {"k1" "v1", 2 "v2"}) "Streaming, w/o  keywordize")])])

;;;; Write

(def ^:const an-uncached-num (inc write/max-num-to-cache))

(deftest _write-nums
  [(is (= (com/with-out->str (write/write-array-len     out              12))    "*12\r\n"))
   (is (= (com/with-out->str (write/write-array-len     out an-uncached-num)) "*32768\r\n"))

   (is (= (com/with-out->str (#'write/write-bulk-len    out              12))    "$12\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-len    out an-uncached-num)) "$32768\r\n"))

   (is (= (com/with-out->str (#'write/write-simple-long out              12))    ":12\r\n"))
   (is (= (com/with-out->str (#'write/write-simple-long out an-uncached-num)) ":32768\r\n"))

   (is (= (com/with-out->str (#'write/write-bulk-long   out              12)) "$2\r\n12\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-long   out an-uncached-num)) "$5\r\n32768\r\n"))

   (is (= (com/with-out->str (#'write/write-bulk-double out              12)) "$4\r\n12.0\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-double out an-uncached-num)) "$7\r\n32768.0\r\n"))])

(deftest _write-bulk-str
  [(is (= (com/with-out->str (#'write/write-bulk-str out "hello\r\n"))    "$7\r\nhello\r\n\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-str out enc/a-utf8-str)) "$47\r\nHi ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ 10\r\n"))

   (testing "reserve-null!"
     [(is (nil?                                                (#'write/reserve-null! "")))
      (is (throws? :common {:eid :carmine.write/null-reserved} (#'write/reserve-null! "\u0000<")))])

   (testing "Bulk num/str equivalence"
     [(is (=
            (com/with-out->str (#'write/write-bulk-double out  12.5))
            (com/with-out->str (#'write/write-bulk-str    out "12.5"))))
      (is (=
            (com/with-out->str (#'write/write-bulk-double out      (double an-uncached-num)))
            (com/with-out->str (#'write/write-bulk-str    out (str (double an-uncached-num))))))])])

(deftest _wrappers
  [(is (= (enc/utf8-ba->str (.-ba (write/bytes (write/bytes (com/xs->ba [\a \b \c]))))) "abc"))
   (is (= (.-freeze-opts (write/freeze {:a :A} (write/freeze {:b :B} "x"))) {:a :A}))

   (is (= (binding [core/*freeze-opts* {:o :O}]
            (let [[c1 c2 c3] (write/freeze :dynamic "x" "y" "z")]
              (mapv #(.-freeze-opts ^WriteFrozen %) [c1 c2 c3])))
         [{:o :O} {:o :O} {:o :O}])
     "Multiple frozen arguments sharing dynamic config")])

(deftest _write-requests
  [(testing "Basics"
     [(is (= (enc/utf8-ba->str  @#'write/bulk-nil)                               "$2\r\n\u0000_\r\n"))
      (is (= (com/with-out->str (#'write/write-requests out [["hello\r\n"]]))    "*1\r\n$7\r\nhello\r\n\r\n"))
      (is (= (com/with-out->str (#'write/write-requests out [[enc/a-utf8-str]])) "*1\r\n$47\r\nHi ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ 10\r\n"))
      (is (=
            (com/with-out->str
              (#'write/write-requests out [["a1" "a2" "a3"] ["b1"] ["c1" "c2"]]))
            "*3\r\n$2\r\na1\r\n$2\r\na2\r\n$2\r\na3\r\n*1\r\n$2\r\nb1\r\n*2\r\n$2\r\nc1\r\n$2\r\nc2\r\n")

        "Multiple reqs, with multiple args each")

      (is (= (com/with-out->str (#'write/write-requests out [["str" 1 2 3 4.0 :kw \x]]))
            #_"*7\r\n$3\r\nstr\r\n:1\r\n:2\r\n:3\r\n$3\r\n4.0\r\n$2\r\nkw\r\n$1\r\nx\r\n" ; Simple nums
            "*7\r\n$3\r\nstr\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n$3\r\n4.0\r\n$2\r\nkw\r\n$1\r\nx\r\n"))

      (is (=
            (com/with-out->str (#'write/write-requests out [["-1" "0" "1" (str (dec write/min-num-to-cache)) (str (inc write/max-num-to-cache))]]))
            (com/with-out->str (#'write/write-requests out [[ -1   0   1       (dec write/min-num-to-cache)       (inc write/max-num-to-cache)]])))
        "Simple longs produce same output as longs or strings")])

   (testing "Blob markers"
     [(testing "Auto freeze enabled"
        (binding [core/*auto-freeze?* true]
          [(is (= (com/with-out->str (#'write/write-requests out [[nil]])) "*1\r\n$2\r\n\u0000_\r\n")            "nil arg => ba-nil marker")
           (is (= (com/with-out->str (#'write/write-requests out [[{}]]))  "*1\r\n$7\r\n\u0000>NPY\u0000\r\n") "clj arg => ba-npy marker")

           (let [ba (byte-array [(int \a) (int \b) (int \c)])]
             [(is (= (com/with-out->str (#'write/write-requests out [[             ba]]))  "*1\r\n$5\r\n\u0000<abc\r\n") "ba-bin marker")
              (is (= (com/with-out->str (#'write/write-requests out [[(write/bytes ba)]])) "*1\r\n$3\r\nabc\r\n") "Unmarked bin")])]))

      (testing "Auto freeze disabled"
        (binding [core/*auto-freeze?* false]
          (let [pattern {:eid :carmine.write/non-native-arg-type}]
           [(is (throws? :common pattern (com/with-out->str (#'write/write-requests out [[nil]]))) "nil arg => throw")
            (is (throws? :common pattern (com/with-out->str (#'write/write-requests out [[{}]])))  "clj arg => throw")

            (let [ba (byte-array [(int \a) (int \b) (int \c)])]
              [(is (= (com/with-out->str (#'write/write-requests out [[             ba]]))  "*1\r\n$3\r\nabc\r\n") "Unmarked bin")
               (is (= (com/with-out->str (#'write/write-requests out [[(write/bytes ba)]])) "*1\r\n$3\r\nabc\r\n") "Same unmarked bin with `bytes`")])])))])])
