(ns taoensso.carmine-v4.tests.resp
  "Low-level RESP protocol tests.
  These don't need a running Redis server."
  (:require
   [clojure.test     :as test  :refer [deftest testing is]]
   [clojure.test.check.clojure-test :refer [defspec]]
   [clojure.test.check.generators   :as gen]
   [clojure.test.check.properties   :as prop]
   [taoensso.encore  :as enc]
   [taoensso.truss   :as truss :refer [throws?]]
   [taoensso.nippy   :as nippy]

   [taoensso.carmine-v4.resp.common :as com
    :refer [xs->in+]]

   [taoensso.carmine-v4.resp       :as resp]
   [taoensso.carmine-v4.resp.read  :as read]
   [taoensso.carmine-v4.resp.write :as write]
   [taoensso.carmine-v4 :as core])

  (:import
   [java.io BufferedOutputStream ByteArrayOutputStream DataInputStream InputStream]
   [java.util LinkedList]
   [taoensso.carmine_v4.resp Ctx]
   [taoensso.carmine_v4.resp.common ReadOpts ReadThawed #_Parser]
   [taoensso.carmine_v4.resp.write WriteFrozen]
   [taoensso.carmine_v4.resp.read VerbatimString WithAttributes Push]))

(comment
  (remove-ns      'taoensso.carmine-v4.tests.resp)
  (test/run-tests 'taoensso.carmine-v4.tests.resp))

(defn- throw! [x]
  (truss/ex-info! "Simulated throw" {:arg (enc/typed-val x)}))

;;;; Common

(deftest _xs-utils
  [(is (= (com/xs->str  "a" "b" 1 (byte-array [(int \A) (int \B)]) \C [\d \e]) "ab1ABCde"))
   (is (= (com/xs->str+ "a" "b" 1 (byte-array [(int \A) (int \B)]) \C [\d \e]) "a\r\nb\r\n1\r\nAB\r\nC\r\nde\r\n"))
   (is (= (com/xs->str+ "$5" "hello")  "$5\r\nhello\r\n"))
   (is (= (com/xs->str+ "$5\r\nhello") "$5\r\nhello\r\n"))
   (is (= (com/xs->str+ "$0\r\n")      "$0\r\n\r\n"))])

(defn- test-blob-?marker
  ([s] (test-blob-?marker s false))
  ([s issue-83-workaround?]
   (let [^bytes ba (enc/str->utf8-ba s)
         in (com/ba->in ba)]
     [(com/read-blob-?marker in (alength ba) issue-83-workaround?)
      (.readLine             in)])))

(deftest _read-blob-?marker
  [(is (= (test-blob-?marker "foo")            [nil "foo"]))
   (is (= (test-blob-?marker "\u0000more")     [nil "\u0000more"]))
   (is (= (test-blob-?marker "\u0000_more")    [:nil "more"]))
   (is (= (test-blob-?marker "\u0000>more")    [:npy "more"]))
   (is (= (test-blob-?marker "\u0000<more")    [:bin "more"]))
   (is (= (test-blob-?marker "\u0000<NPYmore" true ) [:npy "NPYmore"]))
   (is (= (test-blob-?marker "\u0000<NPYmore" false) [:bin "NPYmore"]))
   (is (= (test-blob-?marker "\u0000<NPmore")  [:bin "NPmore"]))
   (is (= (test-blob-?marker "\u0000<Nmore")   [:bin "Nmore"]))])

(deftest _stream-discards
  [(is (->>   (com/discard-stream-separator (com/xs->in+ ""))  (throws? :ex-info {:eid :carmine.read/missing-stream-separator})))
   (is (->>   (com/discard-crlf             (com/xs->in+ "_")) (throws? :ex-info {:eid :carmine.read/missing-crlf})))
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
(defn- rr-with-push [push-fn in]
  (read/read-reply
    (com/with-push-fn (com/get-read-opts) push-fn) in))

(deftest _raw-command-validation
  (let [pending-reqs   (LinkedList.)
        pending-replies (LinkedList.)
        ctx (Ctx. false false pending-reqs pending-replies nil nil nil nil)]
    (binding [resp/*ctx* ctx]
      [(doseq [call [(fn [] (resp/rcmd))
                     (fn [] (resp/rcmd* []))
                     (fn [] (resp/rcmds ["PING"] []))
                     (fn [] (resp/rcmds* [["PING"] [] ["ECHO" "x"]]))]]
         (is (throws? :ex-info {:eid :carmine.write/empty-command} (call))))
       (is (zero? (.size pending-reqs))
         "An invalid raw-command batch is rejected before queue mutation")
       (is (nil? (resp/rcmds* [])) "An empty outer batch remains a no-op")
       (is (zero? (.size pending-reqs)))
       (is (nil? (resp/rcmds* [["PING"] ["ECHO" "x"]])))
       (is (= (.size pending-reqs) 2))])))

(deftest _request-command-compatibility
  (let [pending-reqs    (LinkedList.)
        pending-replies (LinkedList.)
        ctx (Ctx. false false pending-reqs pending-replies nil nil nil nil)]
    (binding [resp/*ctx* ctx]
      [(doseq [command [["subscribe" "channel"]
                        ["PSUBSCRIBE" "pattern"]
                        ["SSUBSCRIBE" "shard"]
                        ["UNSUBSCRIBE"]
                        ["PUNSUBSCRIBE"]
                        ["SUNSUBSCRIBE"]
                        ["CLIENT" "REPLY" "OFF"]
                        ["CLIENT" "REPLY" "SKIP"]]]
         [(is (throws? :ex-info {:eid :carmine/invalid-request-command}
                (resp/rcmd* command)))
          (is (zero? (.size pending-reqs)))])

       (is (throws? :ex-info
             {:eid :carmine/invalid-request-command, :command "SUBSCRIBE"}
             (core/subscribe "channel"))
         "Generated commands use the same guard")
       (is (zero? (.size pending-reqs)))

       (is (throws? :ex-info
             {:eid :carmine/invalid-request-command, :command "CLIENT REPLY SKIP"}
             (resp/rcmds* [["PING"] ["CLIENT" "REPLY" "SKIP"] ["PING"]])))
       (is (zero? (.size pending-reqs))
         "An incompatible raw batch is rejected before queue mutation")

       (is (nil? (resp/rcmd* ["CLIENT" "REPLY" "ON"])))
       (is (= (.size pending-reqs) 1)
         "CLIENT REPLY ON still has one ordinary reply")])))

(defn- limited-in [limits wire] (com/str->limited-in wire limits))

(defn- resource-error [limits wire]
  (truss/throws (rr (limited-in limits wire))))

(deftest _read-resource-limits
  [(testing "CRLF lines"
     [(is (= (rr (limited-in {:max-line-bytes 3} "+abc\r\n")) "abc"))
      (is (truss/submap?
            (ex-data (resource-error {:max-line-bytes 2} "+abc\r\n"))
            {:eid :carmine.read/resource-limit
             :limit :max-line-bytes, :max 2, :actual 3}))])

   (testing "Fixed and streamed blobs"
     [(is (= (rr (limited-in {:max-blob-bytes 3} "$3\r\nabc\r\n")) "abc"))
      (is (= (:limit (ex-data (resource-error {:max-blob-bytes 2} "$3\r\nabc\r\n")))
            :max-blob-bytes))
      (is (= (rr (limited-in {:max-blob-bytes 4}
                    "$?\r\n;2\r\nab\r\n;2\r\ncd\r\n;0\r\n"))
            "abcd"))
      (is (truss/submap?
            (ex-data
              (resource-error {:max-blob-bytes 3}
                "$?\r\n;2\r\nab\r\n;2\r\ncd\r\n;0\r\n"))
            {:limit :max-blob-bytes, :max 3, :actual 4
             :kind :streaming-blob}))])

   (testing "Fixed and streamed aggregates"
     [(is (= (rr (limited-in {:max-aggregate-elements 2}
                    "*2\r\n:1\r\n:2\r\n")) [1 2]))
      (is (= (:limit
               (ex-data
                 (resource-error {:max-aggregate-elements 1}
                   "*2\r\n:1\r\n:2\r\n")))
            :max-aggregate-elements))
      (is (= (rr (limited-in {:max-aggregate-elements 2}
                    "*?\r\n:1\r\n:2\r\n.\r\n")) [1 2]))
      (is (= (:actual
               (ex-data
                 (resource-error {:max-aggregate-elements 1}
                   "*?\r\n:1\r\n:2\r\n.\r\n"))) 2))
      (is (= (rr (limited-in {:max-aggregate-elements 1}
                    "%1\r\n+k\r\n+v\r\n")) {"k" "v"}))
      (is (= (:actual
               (ex-data
                 (resource-error {:max-aggregate-elements 1}
                   "%?\r\n+k1\r\n+v1\r\n+k2\r\n+v2\r\n.\r\n"))) 2))])

   (testing "Nesting, attributes, and pushes"
     [(is (= (rr (limited-in {:max-nesting-depth 2}
                    "*1\r\n*1\r\n:1\r\n")) [[1]]))
      (is (= (:actual
               (ex-data
                 (resource-error {:max-nesting-depth 1}
                   "*1\r\n*1\r\n:1\r\n"))) 2))
      ;; Attribute scopes close before their content is read, so chained
      ;; attributes don't add phantom nesting depth to the content:
      (is (instance? WithAttributes
            (rr (limited-in {:max-nesting-depth 1}
                  "|0\r\n|0\r\n+ok\r\n"))))
      (is (= (:kind
               (ex-data
                 (resource-error {:max-nesting-depth 1}
                   "*1\r\n|0\r\n+ok\r\n"))) :attributes))

      (let [n 2000
            calls_ (atom 0)
            wire (str (apply str (repeat n ">1\r\n+x\r\n")) "+ok\r\n")]
        (is (= (rr-with-push (fn [_] (swap! calls_ inc))
                 (limited-in {:max-nesting-depth 1} wire))
              "ok"))
        (is (= @calls_ n)
          "Consecutive pushes don't consume aggregate nesting budget"))

      (let [reply (rr (limited-in {:max-frame-bytes 12}
                        "*1\r\n>1\r\n+x\r\n+ok\r\n"))]
        (is (truss/submap? (ex-data (ex-cause reply))
              {:eid :carmine.read/invalid-scalar
               :kind :push, :value :nested})
          "Pushes are valid only at the top level"))])

   (testing "Complete frame accounting and poisoning"
     [(is (= (rr (limited-in {:max-frame-bytes 6} "+abc\r\n")) "abc"))
      (is (= (:actual
               (ex-data (resource-error {:max-frame-bytes 5} "+abc\r\n"))) 6))
      (is (= (rr (limited-in {:max-frame-bytes 9} "$3\r\nabc\r\n")) "abc")
        "Blob marker lookahead is not double-counted")
      (is (= (rr-with-push (fn [_])
               (limited-in {:max-frame-bytes 7}
                 ">1\r\n+\r\n>1\r\n+\r\n+ok\r\n")) "ok")
        "Each top-level push and following reply has an independent budget")

      (let [in (limited-in {:max-frame-bytes 6} "+abc\r\n+def\r\n")]
        [(is (= (rr in) "abc"))
         (is (= (rr in) "def")
           "Frame accounting resets between sequential replies")])

      (let [calls_ (atom 0)
            failure
            (truss/throws
              (rr-with-push (fn [_] (swap! calls_ inc))
                (limited-in {:max-frame-bytes 6} ">1\r\n+x\r\n+ok\r\n")))]
        [(is (= (:limit (ex-data failure)) :max-frame-bytes))
         (is (zero? @calls_) "A breached push is never delivered")])

      (let [^java.io.DataInputStream in
            (limited-in {:max-line-bytes 1} "+ab\r\n+ok\r\n")
            first-error (truss/throws (rr in))
            remaining-before (.available in)
            second-error (truss/throws (rr in))]
        [(is (= (:eid (ex-data first-error)) :carmine.read/resource-limit))
         (is (= (:eid (ex-data second-error)) :carmine.read/poisoned))
         (is (= (.available in) remaining-before)
           "Poisoned reads consume no more input")])])

   (testing "Skip mode remains limited"
     [(is (= (read/read-reply com/read-opts-skip
               (limited-in {:max-blob-bytes 3} "$3\r\nabc\r\n"))
            com/sentinel-skipped-reply))
      (is (= (read/read-reply com/read-opts-skip+errors
               (limited-in {:max-blob-bytes 3} "$3\r\nabc\r\n"))
            com/sentinel-skipped-reply))
      (is (com/reply-error? {:eid :carmine.read/redis-error-reply}
            (read/read-reply com/read-opts-skip+errors
              (com/str->in "-MOVED 1 host:7000\r\n"))))
      (is (com/reply-error? {:eid :carmine.read/redis-error-reply}
            (read/read-reply com/read-opts-skip+errors
              (com/str->in "!17\r\nMOVED 1 host:7000\r\n"))))
      (is (= (read/read-reply com/read-opts-skip+errors
               (com/str->in "*2\r\n+ok\r\n-TRYAGAIN nested\r\n"))
            com/sentinel-skipped-reply)
        "Nested aggregate errors cannot trigger top-level Cluster retries")
      (is (= (read/read-reply com/read-opts-skip+errors
               (com/str->in "%1\r\n+k\r\n-TRYAGAIN nested\r\n"))
            com/sentinel-skipped-reply))
      (is (= (:limit
               (ex-data
                 (truss/throws
                   (read/read-reply com/read-opts-skip
                     (limited-in {:max-frame-bytes 8} "$3\r\nabc\r\n")))))
            :max-frame-bytes))
      (is (= (:limit
               (ex-data
                 (truss/throws
                   (read/read-reply com/read-opts-skip
                     (limited-in {:max-blob-bytes 2}
                       "$?\r\n;2\r\nab\r\n;1\r\nc\r\n;0\r\n")))))
            :max-blob-bytes))])

   (testing "Reducing parsers cannot capture breaches"
     (let [read-opts
           (com/get-read-opts
             {:parser (com/rf-parser {} nil (com/parsing-rf [] conj))})
           failure
           (truss/throws
             (read/read-reply read-opts
               (limited-in {:max-aggregate-elements 1}
                 "*2\r\n:1\r\n:2\r\n")))]
       [(is (= (:eid (ex-data failure)) :carmine.read/resource-limit))
        (is (not (com/reply-error? failure)))]))])

(defn- malformed-reply-in? [in-fn]
  (let [malformed-with?
        (fn [read-opts]
          (try
            (com/reply-error? (read/read-reply read-opts (in-fn)))
            (catch Throwable _ false)))]
    (and
      (malformed-with? com/read-opts-default)
      (malformed-with? com/read-opts-skip))))

(defn- malformed-reply? [wire]
  (malformed-reply-in? #(com/str->in wire)))

(defn- malformed-reply-bytes? [^bytes wire]
  (malformed-reply-in? #(com/ba->in wire)))

(def ^:private truncated-aggregate-gen
  (gen/bind
    (gen/vector (gen/choose -100000 100000) 0 20)
    (fn [xs]
      (let [wire
            (apply com/xs->str+
              (concat
                [(str "*" (inc (count xs))) "*2" ":1" ":2"]
                (map #(str ":" %) xs)))]
        (gen/fmap #(subs wire 0 %)
          (gen/choose 0 (dec (count wire))))))))

(defspec _truncated-aggregates-are-rejected 100
  (prop/for-all [wire truncated-aggregate-gen]
    (malformed-reply? wire)))

(def ^:private generated-text-gen
  (gen/frequency
    [[8 (gen/fmap #(apply str %)
          (gen/vector
            (gen/elements
              (vec "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_:/ಬಾไทย中文"))
            0 32))]
     [1 (gen/elements ["" "🙂" "line without terminators" "é" "東京"])]]))

(def ^:private generated-simple-text-gen
  ;; Incl. non-ASCII: simple strings/errors are UTF-8 decoded like blobs
  (gen/fmap #(apply str %)
    (gen/vector
      (gen/elements
        ;; NB BMP-only chars here (`vec` splits surrogate pairs)
        (vec "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 -_:/é✓東ü"))
      0 32)))

(def ^:private generated-bytes-gen
  (gen/fmap
    #(byte-array (map unchecked-byte %))
    (gen/vector (gen/choose 0 255) 0 48)))

(def ^:private generated-long-gen
  (gen/frequency
    [[8 (gen/choose -1000000 1000000)]
     [1 (gen/elements [Long/MIN_VALUE Long/MAX_VALUE])]]))

(def ^:private generated-double-gen
  (gen/fmap #(/ (double %) 10.0) (gen/choose -1000000 1000000)))

(def ^:private generated-verbatim-gen
  ;; Verbatim strings always decode as text (all read modes)
  (gen/fmap (fn [[format content]] [:verbatim format content])
    (gen/tuple (gen/elements ["txt" "mkd"]) generated-simple-text-gen)))

(def ^:private resp-scalar-tree-gen
  (gen/one-of
    [(gen/fmap #(vector :simple %) generated-simple-text-gen)
     (gen/fmap (fn [[ba streaming?]] [:blob ba streaming?])
       (gen/tuple generated-bytes-gen gen/boolean))
     (gen/fmap #(vector :long %) generated-long-gen)
     (gen/fmap #(vector :double %) generated-double-gen)
     (gen/fmap #(vector :boolean %) gen/boolean)
     generated-verbatim-gen
     (gen/elements [[:null] [:bigint 18446744073709551616N]
                    [:bigint -18446744073709551616N]])]))

(def ^:private resp-tree-gen
  (gen/recursive-gen
    (fn [inner]
      (gen/fmap
        (fn [[streaming? children]] [:array streaming? children])
        (gen/tuple gen/boolean (gen/vector inner 0 5))))
    resp-scalar-tree-gen))

(def ^:private resp-error-node-gen
  ;; Ordinary Redis error replies (simple `-` and blob `!` frames) are legal
  ;; both top-level and as aggregate elements
  (gen/fmap
    (fn [[code message blob?]]
      [:error (str code (when-not (empty? message) " ") message) blob?])
    (gen/tuple
      (gen/elements ["ERR" "WRONGTYPE" "TRYAGAIN"])
      generated-simple-text-gen
      gen/boolean)))

(def ^:private resp-attr-pair-gen
  ;; Attr values restricted to read-mode-independent scalars so that expected
  ;; attribute maps stay comparable under all read modes
  (gen/tuple generated-simple-text-gen
    (gen/one-of
      [(gen/fmap #(vector :long %) generated-long-gen)
       (gen/fmap #(vector :boolean %) gen/boolean)])))

(def ^:private resp-extended-tree-gen
  ;; `resp-tree-gen`, extended with error frames and RESP3 attribute frames
  (gen/recursive-gen
    (fn [inner]
      (gen/frequency
        [[4 (gen/fmap
              (fn [[streaming? children]] [:array streaming? children])
              (gen/tuple gen/boolean (gen/vector inner 0 5)))]
         [1 (gen/fmap
              (fn [[pairs inner-tree]] [:attrs pairs inner-tree])
              (gen/tuple (gen/vector resp-attr-pair-gen 0 2) inner))]]))
    (gen/frequency
      [[8 resp-scalar-tree-gen]
       [1 resp-error-node-gen]])))

(defn- render-streaming-blob [^bytes ba]
  (let [n (alength ba)
        split (quot n 2)
        first-ba  (java.util.Arrays/copyOfRange ba 0 split)
        second-ba (java.util.Arrays/copyOfRange ba split n)
        chunks (if (zero? n) []
                 (filterv #(pos? (alength ^bytes %)) [first-ba second-ba]))]
    (apply com/xs->ba
      (concat
        [(com/xs->ba+ "$?")]
        (mapcat
          (fn [^bytes chunk]
            [(com/xs->ba+ (str ";" (alength chunk)))
             (com/xs->ba+ chunk)])
          chunks)
        [(com/xs->ba+ ";0")]))))

(defn- render-resp-tree ^bytes [[kind & args :as tree]]
  (case kind
    :simple  (com/xs->ba+ (str "+" (first args)))
    :blob    (let [[^bytes ba streaming?] args]
               (if streaming?
                 (render-streaming-blob ba)
                 (com/xs->ba+ (str "$" (alength ba)) ba)))
    :long    (com/xs->ba+ (str ":" (first args)))
    :double  (com/xs->ba+ (str "," (first args)))
    :boolean (com/xs->ba+ (if (first args) "#t" "#f"))
    :null    (com/xs->ba+ "_")
    :bigint  (com/xs->ba+ (str "(" (first args)))
    :verbatim
    (let [[format content] args
          ^bytes ba (enc/str->utf8-ba (str format ":" content))]
      (com/xs->ba+ (str "=" (alength ba)) ba))
    :error
    (let [[message blob?] args]
      (if blob?
        (let [^bytes ba (enc/str->utf8-ba message)]
          (com/xs->ba+ (str "!" (alength ba)) ba))
        (com/xs->ba+ (str "-" message))))
    :attrs
    (let [[pairs inner] args]
      (apply com/xs->ba
        (concat
          [(com/xs->ba+ (str "|" (count pairs)))]
          (mapcat
            (fn [[k v]] [(com/xs->ba+ (str "+" k)) (render-resp-tree v)])
            pairs)
          [(render-resp-tree inner)])))
    :array
    (let [[streaming? children] args]
      (apply com/xs->ba
        (concat
          [(com/xs->ba+ (if streaming? "*?" (str "*" (count children))))]
          (map render-resp-tree children)
          (when streaming? [(com/xs->ba+ ".")]))))
    (throw (ex-info "Unknown generated RESP tree" {:tree tree}))))

(defn- expected-resp-tree [[kind & args :as tree] bytes?]
  (case kind
    :simple  (first args)
    :blob    (let [^bytes ba (first args)] (if bytes? ba (enc/utf8-ba->str ba)))
    :long    (long (first args))
    :double  (double (first args))
    :boolean (boolean (first args))
    :null    nil
    :bigint  (bigint (first args))
    :verbatim (second args)
    :error    [::error (first args)]
    :attrs    (expected-resp-tree (second args) bytes?)
    :array   (mapv #(expected-resp-tree % bytes?) (second args))
    (throw (ex-info "Unknown generated RESP tree" {:tree tree}))))

(defn- same-generated-reply? [expected actual]
  (let [actual (com/reply-content actual)] ; Strip any attribute wrapper
    (cond
      (and (vector? expected) (identical? (first expected) ::error))
      (com/reply-error?
        {:eid :carmine.read/redis-error-reply, :message (second expected)} actual)

      (and (enc/bytes? expected) (enc/bytes? actual)) (enc/ba= expected actual)
      (and (vector? expected) (vector? actual))
      (and (= (count expected) (count actual))
        (every? true? (map same-generated-reply? expected actual)))
      :else (= expected actual))))

(defn- tree-top-level-error? [[kind & args]]
  (case kind
    :error true
    :attrs (tree-top-level-error? (second args))
    false))

(defn- expected-top-attrs
  "Returns [<check?> <expected-attrs-map>] for the outermost tree node."
  [[kind & args]]
  (if (and (= kind :attrs)
        (not (contains? #{:attrs :error} (first (second args)))))
    [true (into {}
            (map (fn [[k v]] [k (expected-resp-tree v false)]))
            (first args))]
    [false nil]))

(defn- chunked-in ^DataInputStream [^bytes ba chunk-size]
  (let [position_ (volatile! 0)
        nbytes (alength ba)]
    (DataInputStream.
      (proxy [InputStream] []
        (read
          ([]
           (let [position (long @position_)]
             (if (< position nbytes)
               (let [result (bit-and (aget ba (int position)) 0xff)]
                 (vreset! position_ (inc position))
                 result)
               -1)))
          ([dst off len]
           (let [^bytes dst dst
                 position (long @position_)]
             (if (< position nbytes)
               (let [n (int (min (long len) (long chunk-size) (- nbytes position)))]
                 (System/arraycopy ba (int position) dst (int off) n)
                 (vreset! position_ (+ position n))
                 n)
               -1))))
        (skip [_] 0)))))

(def ^:private read-opts-plain (ReadOpts. nil    nil false false false false nil))
(def ^:private read-opts-bytes (ReadOpts. :bytes nil false false false false nil))

(defspec _generated-resp-trees-survive-fragmentation 250
  (prop/for-all [tree resp-extended-tree-gen
                 pushes (gen/vector (gen/vector generated-simple-text-gen 1 3) 0 2)
                 chunk-size (gen/choose 1 16)]
    (let [push-frames ; Interleaved top-level pushes before the reply frame
          (mapv
            (fn [items]
              (apply com/xs->ba
                (com/xs->ba+ (str ">" (count items)))
                (map #(com/xs->ba+ (str "+" %)) items)))
            pushes)
          wire
          (apply com/xs->ba
            (conj push-frames (render-resp-tree tree) (com/xs->ba+ "+tail")))
          top-error? (tree-top-level-error? tree)
          [check-attrs? expected-attrs] (expected-top-attrs tree)
          check-mode
          (fn [read-opts bytes?]
            (let [n-pushes_ (atom 0)
                  read-opts (com/with-push-fn read-opts
                              (fn [_] (swap! n-pushes_ inc)))
                  in    (chunked-in wire chunk-size)
                  reply (read/read-reply read-opts in)]
              (and
                (same-generated-reply? (expected-resp-tree tree bytes?) reply)
                (or (not check-attrs?)
                  (= (com/reply-attributes reply) expected-attrs))
                (= (read/read-reply read-opts-plain in) "tail")
                (= @n-pushes_ (count pushes)))))
          check-skip
          (fn [read-opts]
            (let [in    (chunked-in wire chunk-size)
                  reply (read/read-reply read-opts in)]
              (and
                (if (and top-error?
                      (identical? read-opts com/read-opts-skip+errors))
                  (com/reply-error? {:eid :carmine.read/redis-error-reply} reply)
                  (identical? reply com/sentinel-skipped-reply))
                (= (read/read-reply read-opts-plain in) "tail"))))]
      (and
        (check-mode read-opts-plain false)
        (check-mode read-opts-bytes true)
        (check-skip com/read-opts-skip)
        (check-skip com/read-opts-skip+errors)))))

(def ^:private resp-map-gen
  (gen/fmap
    (fn [[streaming? entries]]
      {:streaming? streaming?, :entries (vec (into {} entries))})
    (gen/tuple gen/boolean
      (gen/vector (gen/tuple generated-simple-text-gen resp-tree-gen) 0 10))))

(def ^:private resp-set-gen
  (gen/fmap
    (fn [[streaming? values]]
      {:streaming? streaming?, :values values})
    (gen/tuple gen/boolean (gen/vector resp-tree-gen 0 10))))

(defn- render-generated-map ^bytes [{:keys [streaming? entries]}]
  (apply com/xs->ba
    (concat
      [(com/xs->ba+ (if streaming? "%?" (str "%" (count entries))))]
      (mapcat
        (fn [[k v]]
          [(render-resp-tree [:blob (enc/str->utf8-ba k) false])
           (render-resp-tree v)])
        entries)
      (when streaming? [(com/xs->ba+ ".")]))))

(defn- render-generated-set ^bytes [{:keys [streaming? values]}]
  (apply com/xs->ba
    (concat
      [(com/xs->ba+ (if streaming? "~?" (str "~" (count values))))]
      (map render-resp-tree values)
      (when streaming? [(com/xs->ba+ ".")]))))

(defspec _generated-resp-maps-and-sets-survive-fragmentation 200
  (prop/for-all [generated-map resp-map-gen
                 generated-set resp-set-gen
                 chunk-size (gen/choose 1 16)]
    (let [map-wire (com/xs->ba (render-generated-map generated-map)
                     (com/xs->ba+ "+map-tail"))
          set-wire (com/xs->ba (render-generated-set generated-set)
                     (com/xs->ba+ "+set-tail"))
          map-in (chunked-in map-wire chunk-size)
          set-in (chunked-in set-wire chunk-size)
          expected-map
          (into {}
            (map (fn [[k v]] [k (expected-resp-tree v false)]))
            (:entries generated-map))
          expected-set
          (into #{} (map #(expected-resp-tree % false)) (:values generated-set))]
      (and
        (= (read/read-reply read-opts-plain map-in) expected-map)
        (= (read/read-reply read-opts-plain map-in) "map-tail")
        (= (read/read-reply read-opts-plain set-in) expected-set)
        (= (read/read-reply read-opts-plain set-in) "set-tail")

        (every?
          true?
          (for [read-opts [com/read-opts-skip com/read-opts-skip+errors]
                [wire tail] [[map-wire "map-tail"] [set-wire "set-tail"]]]
            (let [in (chunked-in wire chunk-size)]
              (and
                (identical? (read/read-reply read-opts in)
                  com/sentinel-skipped-reply)
                (= (read/read-reply read-opts-plain in) tail)))))))))

(deftest _aggregate-branch-framing
  (testing "Streamed blob terminators preserve the following frame"
    (doseq [[read-opts expected]
            [[read-opts-plain "abc"]
             [com/read-opts-skip com/sentinel-skipped-reply]
             [com/read-opts-skip+errors com/sentinel-skipped-reply]]]
      (let [in (com/str->in "$?\r\n;3\r\nabc\r\n;0\r\n+tail\r\n")]
        [(is (= (read/read-reply read-opts in) expected))
         (is (= (read/read-reply read-opts-plain in) "tail"))])))

  (testing "Skipped streaming maps consume complete key/value pairs"
    (doseq [read-opts [com/read-opts-skip com/read-opts-skip+errors]]
      (let [in (xs->in+ "%?" "+k1" ":1" "+k2" "*2" ":2" ":3" "." "+tail")]
        [(is (identical? (read/read-reply read-opts in)
               com/sentinel-skipped-reply))
         (is (= (read/read-reply read-opts-plain in) "tail"))])))

  (testing "Large fixed maps preserve all entries and following frames"
    (let [entries (mapcat (fn [n] [(str "+k" n) (str ":" n)]) (range 12))
          in      (apply xs->in+ "%12" (concat entries ["+tail"]))]
      [(is (= (read/read-reply read-opts-plain in)
             (into {} (map (fn [n] [(str "k" n) n])) (range 12))))
       (is (= (read/read-reply read-opts-plain in) "tail"))])))

(def ^:private resp-error-gen
  (gen/fmap
    (fn [[code message]]
      [code (str code (when-not (empty? message) " ") message)])
    (gen/tuple
      (gen/elements ["ERR" "MOVED" "TRYAGAIN" "WRONGTYPE"])
      generated-simple-text-gen)))

(defspec _generated-error-replies-preserve-framing 100
  (prop/for-all [[code message] resp-error-gen
                 chunk-size (gen/choose 1 16)]
    (let [wire (com/xs->ba (com/xs->ba+ (str "-" message))
                 (com/xs->ba+ "+tail"))
          check
          (fn [read-opts]
            (let [in (chunked-in wire chunk-size)
                  error (read/read-reply read-opts in)]
              (and
                (com/reply-error? {:eid :carmine.read/redis-error-reply
                                   :code code, :message message}
                  error)
                (= (read/read-reply read-opts-plain in) "tail"))))]
      (and (check read-opts-plain) (check com/read-opts-skip+errors)))))

(def ^:private truncated-generated-resp-gen
  (gen/bind resp-extended-tree-gen
    (fn [tree]
      (let [wire (render-resp-tree tree)]
        (gen/fmap
          (fn [cut] (java.util.Arrays/copyOf wire (int cut)))
          (gen/choose 0 (dec (alength wire))))))))

(defspec _generated-truncated-resp-is-rejected 250
  (prop/for-all [wire truncated-generated-resp-gen]
    (malformed-reply-bytes? wire)))

(def ^:private generated-native-arg-gen
  (gen/one-of
    [(gen/fmap #(vector % (enc/str->utf8-ba %)) generated-text-gen)
     (gen/fmap #(vector % (enc/str->utf8-ba (str %))) generated-long-gen)
     (gen/fmap #(vector % (enc/str->utf8-ba (str %))) generated-double-gen)
     (gen/fmap #(vector (keyword %) (enc/str->utf8-ba %)) generated-text-gen)
     (gen/fmap (fn [^bytes ba] [(write/bytes ba) ba]) generated-bytes-gen)]))

(defspec _generated-command-writes-round-trip-through-reader 250
  (prop/for-all [args (gen/vector generated-native-arg-gen 1 20)
                 chunk-size (gen/choose 1 16)]
    (let [inputs   (mapv first args)
          expected (mapv second args)
          prepared (write/prepare-command inputs)
          routed   (mapv write/arg-payload-bytes prepared)
          wire
          (let [baos (ByteArrayOutputStream.)
                out  (BufferedOutputStream. baos)]
            (write/write-command out prepared)
            (.flush out)
            (.toByteArray baos))
          actual (read/read-reply read-opts-bytes (chunked-in wire chunk-size))]
      (and
        (same-generated-reply? expected actual)
        (same-generated-reply? routed   actual)))))

(deftest _malformed-reply-lengths
  (doseq [[wire kind length]
          [["$-2\r\n"          :blob                  "-2"]
           ["$x\r\n"           :blob                   "x"]
           ["$+1\r\nx\r\n"     :blob                  "+1"]
           ["$١\r\nx\r\n"     :blob                   "١"]
           ["$2147483648\r\n"  :blob          "2147483648"]
           ["$?\r\n;-1\r\n"   :streaming-blob-chunk    "-1"]
           ["!-1\r\n"          :blob-error             "-1"]
           ["!?\r\n"           :blob-error              "?"]
           ["=-1\r\n"          :verbatim-string         "-1"]
           ["=?\r\n"           :verbatim-string          "?"]
           ["*-2\r\n"          :array                  "-2"]
           ["~-1\r\n"          :set                    "-1"]
           ["%-2\r\n"          :map                    "-2"]
           ["%-1\r\n"          :map                    "-1"]
           ["|-1\r\n"          :attributes            "-1"]
           ["|?\r\n"           :attributes             "?"]
           [">?\r\n"           :push                   "?"]
           [">-1\r\n"          :push                  "-1"]
           [">0\r\n"           :push                   "0"]]]
    (let [reply (rr (com/str->in wire))]
      [(is (com/reply-error? reply))
       (is (truss/submap? (ex-data (ex-cause reply))
             {:eid :carmine.read/invalid-length, :kind kind, :length length}))]))

  [(is (nil? (rr (com/str->in "$-1\r\n"))) "RESP2 null blob remains valid")
   (is (nil? (rr (com/str->in "*-1\r\n"))) "RESP2 null aggregate remains valid")

   (doseq [wire ["!-1\r\n" "!?\r\n" "=-1\r\n" "=?\r\n"
                 "~-1\r\n" "%-1\r\n" "|-1\r\n" "|?\r\n"
                 ">-1\r\n" ">?\r\n" ">0\r\n"]]
     (is (malformed-reply? wire)
       (str "Type-specific lengths are enforced in all read modes: " (pr-str wire))))

   (is (com/reply-error?
         (rr (com/ba->in (byte-array [(unchecked-byte 0x80)]))))
     "Non-ASCII reply kinds produce protocol errors")

   (doseq [wire ["*1\r\n.\r\n" "%?\r\n+k\r\n.\r\n"]]
     (let [reply (rr (com/str->in wire))]
       (is (= (:eid (ex-data (ex-cause reply)))
             :carmine.read/unexpected-aggregate-end))))])

(deftest _malformed-reply-scalars
  (doseq [wire [":١\r\n" ": 1\r\n"
                "(١\r\n" "(1.0\r\n"
                ",Infinity\r\n" ",+inf\r\n" ",.5\r\n" ",1.\r\n"
                ",0x1p4\r\n" ",1f\r\n" ",1e999\r\n" ", 1\r\n"
                ".\r\n"
                ">1\r\n:1\r\n"
                "*1\r\n>1\r\n+x\r\n+next\r\n"
                "*?\r\n>0\r\n.\r\n"
                "*1\r\n|0\r\n.\r\n"
                "*?\r\n|0\r\n.\r\n.\r\n"]]
    (is (malformed-reply? wire)
      (str "Reject malformed scalar in normal and skip modes: " (pr-str wire))))

  [(is (= (rr (com/str->in ":+1\r\n")) 1))
   (is (= (rr (com/str->in "(+123456789012345678901234567890\r\n"))
         123456789012345678901234567890N))
   (is (= (rr (com/str->in ",+1.5e+2\r\n")) 150.0))])

(deftest _request-context-framing-errors-are-fatal
  (let [in (com/str->in "?bad\r\n")
        out (java.io.BufferedOutputStream. (java.io.ByteArrayOutputStream.))
        failure
        (truss/throws
          (resp/with-replies in out
            {:natural-replies? true, :error-mode :throw} nil
            resp/ping))]
    [(is (com/reply-error? {:eid :carmine.read/unexpected-read-error} failure))
     (is (not (instance? taoensso.carmine_v4.classes.ReusableConnError failure))
       "Unknown protocol position must invalidate rather than re-pool a connection")]))

(deftest _read-reply
  [(testing "Basics"
     [(is (= "" (rr (xs->in+ "$0\r\n"))) "Empty blob")
      (is (= [] (rr (xs->in+ "*0")))     "Empty array")
      (is (=    (rr (xs->in+ "*10" "+simple string" ":1" ",1" ",1.5" ",inf" ",-inf" "(1" "#t" "#f" "_"))
            ["simple string" 1 1.0 1.5 ##Inf ##-Inf 1N true false nil]))
      (doseq [wire [",nan\r\n" ",-nan\r\n" ",NAN\r\n" ",nan(payload)\r\n"]]
        (is (Double/isNaN (double (rr (com/str->in wire))))
          (str "Accept canonical and pre-Redis-7.2 libc NaN spelling " (pr-str wire))))

      (is (= (rr (xs->in+ "$5" "hello"))     "hello"))
      (is (= (rr (xs->in+ "$7" "hello\r\n")) "hello\r\n") "Binary safe")
      (is (= (rr (xs->in+ "$?" ";5" "hello" ";9" " world!\r\n" ";0")) "hello world!\r\n") "Streaming")])

   (testing "Basic aggregates"
     [(is (=                                          (rr (xs->in+ "*3" ":1" ":2" "+3"))          [1 2 "3"]))
      (is (=                                          (rr (xs->in+ "%1" "+k" "+v"))                {"k" "v"}) "Preserve string map keys by default")
      (is (= (binding [core/*keywordize-maps?* true]  (rr (xs->in+ "%2" "+k1" "+v1" ":2" "+v2"))) {:k1  "v1", 2 "v2"}))
      (is (= (binding [core/*keywordize-maps?* false] (rr (xs->in+ "%2" "+k1" "+v1" ":2" "+v2"))) {"k1" "v1", 2 "v2"}))
      (is (= (rr (xs->in+ "*3" ":1" "$?" ";4" "bulk" ";6" "string" ";0" ",1.5")) [1 "bulkstring" 1.5]))
      (is (= (rr (xs->in+ "*3" "$0\r\n" "*0" "+simple string")) ["" [] "simple string"]) "Empties in aggregates")

      (is (=                        (rr (xs->in+ "*2" ":1" "$3" [\a \b \c])) [1 "abc"]) "Baseline...")
      (is (let [[x y] (com/as-bytes (rr (xs->in+ "*2" ":1" "$3" [\a \b \c])))]
            [(is (= x 1))
             (is (= (enc/utf8-ba->str y) "abc"))])
        "`bytes` penetrates aggregates")])

   (testing "Errors"
     [(testing "Simple errors"
        [(let [r1 (rr (xs->in+ "-ERR Foo bar baz"))]
           (is (com/reply-error?
                 {:eid :carmine.read/redis-error-reply
                  :message "ERR Foo bar baz"
                  :code    "ERR"}
                 r1)))

         (let [[r1 r2 r3 r4] (rr (xs->in+ "*4" ":1" "-CODE1 a" ":2" "-CODE2 b"))]
           [(is (= r1 1))
            (is (= r3 2))
            (is (com/reply-error? {:eid :carmine.read/redis-error-reply :code "CODE1" :message "CODE1 a"} r2))
            (is (com/reply-error? {:eid :carmine.read/redis-error-reply :code "CODE2" :message "CODE2 b"} r4))])])

      (testing "Bulk errors"
        [(let [r1 (rr (xs->in+ "!10" "CODE Foo\r\n"))]
           (is (com/reply-error?
                 {:eid :carmine.read/redis-error-reply
                  :message "CODE Foo\r\n"
                  :code    "CODE"}
                 r1)
             "Binary safe"))

         (let [[r1 r2 r3 r4] (rr (xs->in+ "*4" ":1" "!9" "CODE1 a\r\n" ":2" "!9" "CODE2 b\r\n"))]
           [(is (= r1 1))
            (is (= r3 2))
            (is (com/reply-error? {:eid :carmine.read/redis-error-reply :code "CODE1" :message "CODE1 a\r\n"} r2))
            (is (com/reply-error? {:eid :carmine.read/redis-error-reply :code "CODE2" :message "CODE2 b\r\n"} r4))])])])

   (testing "Nested aggregates"
     [(is (= [[1 "2" 3] ["a" "b"] []]
             (rr (xs->in+
                          "*3"
                          "*3" ":1" "+2" ":3"
                          "*2" "+a" "+b"
                          "*0"))))

      (is (= [#{1 3 "2"} {"k1" "v1", 2 "v2"} [["a" "b"] [] #{} {}]]
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
              {"k1" "v1"} {"k1" "v1", 2 2},
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
      (is (= (binding [core/*raw-verbatim-strings?* false] (rr (xs->in+ "=11" "txt:hello\r\n")))                         "hello\r\n"))
      (let [raw-opts (binding [core/*raw-verbatim-strings?* true] (com/get-read-opts))
            plain-opts (binding [core/*raw-verbatim-strings?* false] (com/get-read-opts))]
        [(is (= (binding [core/*raw-verbatim-strings?* false]
                  (read/read-reply raw-opts (xs->in+ "=9" "txt:hello")))
                (VerbatimString. "txt" "hello"))
           "Raw mode is captured before lazy reply reading")
         (is (= (binding [core/*raw-verbatim-strings?* true]
                  (read/read-reply plain-opts (xs->in+ "=9" "txt:hello")))
                "hello")
           "Captured false mode is likewise stable")])
      (is (malformed-reply? "=3\r\ntxt\r\n"))
      (is (malformed-reply? "=7\r\ntxt!bad\r\n"))
      (is (= (binding [core/*raw-verbatim-strings?* true]
               (rr (com/str->in "=8\r\néa:body\r\n")))
            (VerbatimString. "éa" "body"))
        "The format occupies three bytes, not three decoded characters")
      (is (malformed-reply? "=9\r\néab:body\r\n")
        "The fourth payload byte must be a colon")

      ;; Verbatim strings are display-oriented by definition and always
      ;; decode as text: special blob read modes do not apply to them
      (is (= (com/as-bytes  (rr (xs->in+ "=9" "txt:hello"))) "hello")
        "`as-bytes` does not apply to verbatim strings")
      (is (= (com/thaw nil  (rr (xs->in+ "=9" "txt:hello"))) "hello")
        "`thaw` does not apply to verbatim strings")])

   (testing "Boolean grammar"
     [(is (true?  (rr (xs->in+ "#t"))))
      (is (false? (rr (xs->in+ "#f"))))
      (is (malformed-reply? "#x\r\n"))])

   (testing "Attribute map type"
     [(let [reply
            (rr (xs->in+ "|1" "+key-popularity"
                  "%2" "$1" "a" ",0.1923" "$1" "b" ",0.0012"
                  "*2" ":2039123" ":9543892"))
            attrs {"key-popularity" {"a" 0.1923 "b" 0.0012}}]
        [(is (= (meta reply) {:carmine/reply-attributes attrs})
           "Attrs stored under dedicated meta key")
         (is (= (core/reply-content reply) reply))
         (is (= (core/reply-attributes reply) attrs))])

      (let [reply
            (rr (xs->in+ "|1" "+key-popularity"
                  "%2" "$1" "a" ",0.1923" "$1" "b" ",0.0012"
                  "$7" "hello\r\n"))
            attrs {"key-popularity" {"a" 0.1923 "b" 0.0012}}]
        [(is (= reply (WithAttributes. "hello\r\n" attrs)))
         (is (= (core/reply-content reply) "hello\r\n"))
         (is (= (core/reply-attributes reply) attrs))])

      (let [reply (rr (xs->in+ "|1" "+ttl" ":3" "-ERR bad"))]
        [(is (core/reply-error? reply)
           "Attributed Redis errors retain their error identity")
         (is (core/reply-error? {:eid :carmine.read/redis-error-reply, :code "ERR"}
               reply))
         (is (= (core/reply-attributes reply) {"ttl" 3}))
         (is (core/reply-error? (core/reply-content reply)))])])

   (testing "Pushes"
     ;; Push replies can be received at any time, but only at the top level
     ;; (e.g. not within the middle of a map reply)
     [(let [push (read/read-reply-or-push
                   (xs->in+ ">2" "+notice" "+payload"))]
        [(is (instance? Push push))
         (is (= (:data push) ["notice" "payload"]))])

      (let [push (read/read-reply-or-push
                   (com/get-read-opts {:auto-thaw? true})
                   (xs->in+ ">3" "+message" "+channel" "+payload"))]
        [(is (instance? Push push))
         (is (= (:data push) ["message" "channel" "payload"]))])

      (let [push (read/read-reply-or-push
                   (com/get-read-opts {:read-mode :bytes})
                   (xs->in+ ">1" "$6" "notice"))]
        (is (enc/ba= (first (:data push)) (enc/str->utf8-ba "notice"))
          "Listener push kinds respect byte-oriented reads"))

      (let [push (read/read-reply-or-push
                   (com/get-read-opts
                     {:parser (com/rf-parser {} nil (com/parsing-rf #{} conj))})
                   (xs->in+ ">2" "+notice" "+payload"))]
        (is (= (:data push) ["notice" "payload"])
          "Listener push data is never reduced by reply parsers"))

      (let [push (read/read-reply-or-push
                   (xs->in+ ">1" "|1" "+meta" "+value" "+notice"))
            push-kind (first (:data push))]
        [(is (= (core/reply-content push-kind) "notice"))
         (is (= (core/reply-attributes push-kind) {"meta" "value"}))])

      (let [p_ (promise)
            pf (fn [dv] (deliver p_ dv))

            reply
            (rr-with-push pf
              (xs->in+
                ">4" "+pubsub" "+message" "+channel" "+message content"
                "$9" "get reply"))]

        [(is (= reply "get reply"))
         (is (= (deref p_ 0 nil) ["pubsub" "message" "channel" "message content"]))])

      (let [calls_ (atom [])
            reply
            (rr-with-push
              (fn [data]
                (swap! calls_ conj data)
                (throw (Exception. "Expected dispatch failure")))
              (xs->in+ ">2" "+notice" "+payload" "+actual reply"))]

        [(is (= reply "actual reply")
           "Push dispatch failures don't consume the actual reply")
         (is (= @calls_ [["notice" "payload"]]))])])])

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

            (testing ":read-mode (INTERNAL, via `fn-parser` only)"
              [(is (= (binding [com/*parser* (com/fn-parser {:read-mode :bytes} enc/utf8-ba->str)]
                                                                     (rr (xs->in+ "$5" "hello")))   "hello")  "Parser  read mode (:bytes)")
               (is (= (com/parse {}          enc/utf8-ba->str (com/as-bytes (rr (xs->in+ "$5" "hello")))) "hello")  "Dynamic read mode (:bytes)")
               (is (= (binding [com/*parser* (com/fn-parser {:read-mode nil} #(str % "!"))]
                        (com/as-bytes                                (rr (xs->in+ "$5" "hello")))) "hello!") "Parser  read mode (nil)")])])])

      (testing "Against aggregates"
        [(is (=                        (rr (xs->in+ "*2" ":1" ":2"))              [1 2])    "Baseline...")
         (is (=   (com/parse {} set    (rr (xs->in+ "*2" ":1" ":2")))            #{1 2})    "Acts as (f <aggr>)")
         (is (=                        (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))   [[1 2] 3]) "Baseline...")
         (is (=   (com/parse {} set    (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) #{[1 2] 3}) "No nesting")
         (is (->> (com/parse {} throw! (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) parser-error?))])])

   (testing "rf parsers"
     [(testing "Against aggregates"
        [(is (=                                                                 (rr (xs->in+ "*4" ":1" ":2" ":3" ":4"))      [1 2 3 4])    "Baseline...")
         (is (=   (com/parse-aggregates {} nil            (com/parsing-rf #{0} conj)   (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  #{0 1 2 3 4})    "Parsed (without xform)")
         (is (=   (com/parse-aggregates {} nil            (com/parsing-rf #'conj)      (rr (xs->in+ "*2" ":1" ":2")))               [1 2])          "RF may be callable without satisfying fn?")
         (is (=   (com/parse-aggregates {} (filter even?) (com/parsing-rf #{0} conj)   (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  #{0   2   4})    "Parsed (with    xform)")
         (is (->> (com/parse-aggregates {} (map throw!)   (com/parsing-rf #{0} conj)   (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  parser-error?) "Trap xform errors")
         (is (->> (com/parse-aggregates {} (map identity) (com/parsing-rf #{0} throw!) (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  parser-error?) "Trap rf    errors")
         (is (com/parse-aggregates {} nil
               (com/parsing-rf #{} (fn [acc in] (if (= in 1) (throw! in) (conj acc in))))
               (let [r1 (rr (xs->in+ "*2" ":1" ":2"))
                     r2 (rr (xs->in+ "*2" ":3" ":4"))]
                 (and (parser-error? r1) (= r2 #{3 4}))))
           "Caught state is per reduction: a parser error never poisons later replies")
         (is (=   (com/parse-aggregates {} (map identity) (com/parsing-rf #{0} conj)   (rr (xs->in+ "*4" ":1" "_"  ":2"  "_")))  #{nil 0 1 2})  "Nulls in aggregate")
         (is (=   (com/parse-aggregates {} nil            (com/parsing-rf #{0} nil)    (rr (xs->in+ "*0")))                      #{0})          "Empty    aggregate")

         (is (=                                                                           (rr (xs->in+ "*4" ":1" ":2" ":3" ":4"))     [1 2 3 4]) "Baseline...")
         (is (= (com/parse-aggregates {} nil (com/parsing-rf #(transient #{0}) persistent! conj!) (rr (xs->in+ "*4" ":1" ":2" ":3" ":4"))) #{0 1 2 3 4}) "Using transients")
         (let [in (xs->in+ "*1" ":1" "*1" ":2")]
           (is (= (com/parse-aggregates {} nil
                    (com/parsing-rf #(transient []) persistent! conj!)
                    [(rr in) (rr in)])
                 [[1] [2]])
             "Mutable parser init is fresh for every aggregate reply"))

         (is (=                                                                                         (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2"))  {"k1" 1, "k2" 2}) "Baseline...")
         (is (= (com/parse-aggregates {}             nil (com/parsing-rf {:k0 0} (fn [m [k v]] (assoc m k v))) (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2"))) {:k0  0, "k1" 1, "k2" 2}) "Ignore *keywordize-maps?*")
         (is (= (binding [com/*parser* (com/rf-parser {:kv-rf? true} nil (com/parsing-rf {:k0 0} (fn [m  k v]  (assoc m k v))))]
                                                                          (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2"))) {:k0  0, "k1" 1, "k2" 2}) "With kv-rf (INTERNAL, via `rf-parser` only)")
         (is (= (com/parse-aggregates {}
                  (filter (fn [[k v]] (even? v)))
                  (com/parsing-rf {:k0 0} (fn [m [k v]] (assoc m k v)))
                  (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2"))) {:k0 0, "k2" 2}) "Aggregate map, with xform")

         (is (=                                                             (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))     [[1 2] 3]) "Baseline...")
         (is (= (com/parse-aggregates {} nil            (com/parsing-rf #{0} conj) (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) #{0 [1 2] 3}) "No nesting (without xform)")
         (is (= (com/parse-aggregates {} (map identity) (com/parsing-rf #{0} conj) (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) #{0 [1 2] 3}) "No nesting (with    xform)")])

      (testing "Against non-aggregates"
        [(is (= (com/parse-aggregates {} (map throw!) throw! (rr (xs->in+ "_")))         nil)  "No effect")
         (is (= (com/parse-aggregates {} (map throw!) throw! (rr (xs->in+ "+hello"))) "hello") "No effect")])])])

(deftest _xform-construction-error-preserves-framing
  (let [bad-xform (fn [_] (throw! :construction))]
    (doseq [wire [(com/xs->str+ "*2" ":1" ":2" "+tail")
                  (com/xs->str+ "*?" ":1" ":2" "." "+tail")]]
      (let [in    (com/str->in wire)
            reply
            (com/parse-aggregates {} bad-xform
              (com/parsing-rf [] conj)
              (rr in))]
        [(is (parser-error? {:thrown-by :xform} reply))
         (is (= (rr in) "tail")
           "A parser construction error still drains the aggregate frame")]))))

(deftest _attributes-composition
  ;; Attributes are decoration: a parsed attributed reply must equal the
  ;; parsed bare reply, plus attributes.
  [(testing "Consecutive attributes accumulate iteratively"
     [(doseq [content ["+ok\r\n" "*1\r\n+ok\r\n"]]
        (let [reply (rr (com/str->in
                          (str "|1\r\n+k\r\n+far\r\n"
                            "|2\r\n+k\r\n+near\r\n+n\r\n+v\r\n"
                            content)))]
          (is (= (core/reply-attributes reply) {"k" "near", "n" "v"})
            "Nearer attributes win consistently for wrapped and metadata-backed content")))

      (let [n 10000
            reply (rr (com/str->in
                        (str (apply str (repeat n "|0\r\n")) "+ok\r\n")))]
        [(is (= (core/reply-content reply) "ok"))
         (is (= (core/reply-attributes reply) {})
           "Long attribute chains use constant stack space")])])

   (testing "fn parsers apply exactly once, to content only"
     [(let [calls_ (atom 0)
            reply  (com/parse {} (fn [x] (swap! calls_ inc) (str "<" x ">"))
                     (rr (xs->in+ "|1" "+k" "+v" "+hello")))]
        [(is (= (core/reply-content    reply) "<hello>"))
         (is (= (core/reply-attributes reply) {"k" "v"}))
         (is (= @calls_ 1) "Parser applied exactly once")])

      (let [reply (com/parse {} set (rr (xs->in+ "|1" "+k" "+v" "*2" ":1" ":2")))]
        [(is (= reply #{1 2})           "Parsed value equals parsed bare reply")
         (is (= (core/reply-attributes reply) {"k" "v"}) "Attributes preserved")])])

   (testing "rf parsers apply to content, never to the attribute map"
     (let [reply (com/parse-aggregates {} nil (com/parsing-rf #{0} conj)
                   (rr (xs->in+ "|1" "+k" "+v" "*2" ":1" ":2")))]
       [(is (= reply #{0 1 2}))
        (is (= (core/reply-attributes reply) {"k" "v"}))]))

   (testing "Ordinary metadata is never misreported as RESP attributes"
     [(is (nil? (core/reply-attributes
                  (com/parse {} (fn [x] (with-meta [x] {:user :meta}))
                    (rr (xs->in+ "+1"))))))
      (let [reply (com/parse {} (fn [x] (with-meta [x] {:user :meta}))
                    (rr (xs->in+ "|1" "+k" "+v" "+1")))]
        [(is (= (core/reply-attributes reply) {"k" "v"}))
         (is (= (:user (meta reply)) :meta)
           "User metadata preserved alongside attrs")])])

   (testing "Null content parsing"
     [(is (= (core/reply-content
               (com/parse {} (fn [_] :parsed)
                 (rr (xs->in+ "|1" "+k" "+v" "_")))) nil))
      (is (= (core/reply-content
               (com/parse {:parse-null-replies? true} (fn [_] :parsed)
                 (rr (xs->in+ "|1" "+k" "+v" "_")))) :parsed))])

   (testing "Skip modes"
     [(is (= (read/read-reply com/read-opts-skip
               (xs->in+ "|1" "+k" "+v" "+hello"))
            com/sentinel-skipped-reply))

      (doseq [read-opts [com/read-opts-skip com/read-opts-skip+errors]]
        (let [delivered_ (promise)
              read-opts (com/with-push-fn read-opts #(deliver delivered_ %))]
          [(is (= (read/read-reply read-opts
                    (xs->in+ "|1" "+k" "+v"
                      ">2" "+notice" "+payload" "+ignored"))
                 com/sentinel-skipped-reply))
           (is (= (deref delivered_ 0 ::nx) ["notice" "payload"]))
           (is (= (core/reply-attributes (deref delivered_ 0 ::nx)) {"k" "v"})
             "Skipped ordinary replies don't discard push attributes")]))

      (is (com/reply-error? {:eid :carmine.read/redis-error-reply}
            (read/read-reply com/read-opts-skip+errors
              (xs->in+ "|1" "+k" "+v" "-MOVED 1 host:7000")))
        "Attributed Cluster redirections remain observable under :skip+errors")
      (is (com/reply-error? {:eid :carmine.read/unexpected-read-error}
            (read/read-reply com/read-opts-skip
              (xs->in+ "|1" "+k" "+v" "$5" "ab")))
        "Attributed framing errors remain observable under plain :skip")])

   (testing "Framing errors are never decorated"
     (let [reply (rr (xs->in+ "|1" "+k" "+v" "$5" "ab"))]
       [(is (com/reply-error? {:eid :carmine.read/unexpected-read-error} reply))
        (is (instance? Throwable reply)
          "Framing errors stay bare so the invalidation path can throw them")]))

   (testing "Attributed pushes"
     ;; Attributes attach to the push (the next protocol frame), not to
     ;; the following ordinary reply.
     [(let [push (read/read-reply-or-push
                   (xs->in+ "|1" "+k" "+v" ">2" "+notice" "+payload"))]
        [(is (instance? Push push))
         (is (= (:data push) ["notice" "payload"]))
         (is (= (core/reply-attributes (:data push)) {"k" "v"}))])

      (let [p_ (promise)
            reply (rr-with-push (fn [dv] (deliver p_ dv))
                    (xs->in+ "|1" "+k" "+v" ">2" "+notice" "+payload" "+ok"))
            dv (deref p_ 0 ::nx)]
        [(is (= reply "ok") "Following ordinary reply is NOT attributed")
         (is (= dv ["notice" "payload"]))
         (is (= (core/reply-attributes dv) {"k" "v"}))])

      (let [p_ (promise)
            reply (com/as-bytes
                    (rr-with-push (fn [dv] (deliver p_ dv))
                      (xs->in+ "|1" "+k" "+v" ">2" "+notice" "+payload" "$2" "ok")))
            dv (deref p_ 0 ::nx)]
        [(is (= (enc/utf8-ba->str reply) "ok"))
         (is (= dv ["notice" "payload"])
           "Dispatched push data stays naturally decoded under read modes")
         (is (= (core/reply-attributes dv) {"k" "v"}))])])])

(deftest _framing-errors-bypass-parsers
  [(is (com/reply-error? {:eid :carmine.read/unexpected-read-error}
         (com/parse {:parse-error-replies? true} (fn [_] :masked)
           (rr (xs->in+ "$5" "ab"))))
     "Parsers must never mask connection-fatal framing errors")
   (is (= (com/parse {:parse-error-replies? true} (fn [_] :masked)
            (rr (xs->in+ "-ERR bad")))
         :masked)
     "Ordinary Redis error replies remain parseable")])

(deftest _reply-processing-pins
  [(is (= (com/as-bytes (com/as-long (rr (xs->in+ "$1" "1")))) 1)
     "Coercing parsers read the textual reply, unaffected by `as-bytes`")

   (testing "`natural-replies` bypasses default reply decoding"
     (let [data   {:a 1}
           marked (com/xs->ba com/ba-npy (nippy/freeze data))
           len    (alength ^bytes marked)]
       [(is (= (binding [core/*auto-thaw?* true]
                 (rr (xs->in+ (str "$" len) marked)))
              data)
          "Baseline...")
        (is (= (binding [core/*auto-thaw?* true]
                 (com/natural-replies (rr (xs->in+ (str "$" len) marked))))
              (enc/utf8-ba->str marked))
          "Auto-thaw bypassed: raw marker+payload decoded as string")
        (is (= (binding [core/*keywordize-maps?* true]
                 (com/natural-replies (rr (xs->in+ "%1" "+k" "+v"))))
              {"k" "v"})
          "Keywordization bypassed")
        (is (= (binding [core/*raw-verbatim-strings?* true]
                 (com/natural-replies (rr (xs->in+ "=9" "txt:hello"))))
              "hello")
          "Raw verbatim strings bypassed")
        (is (= (com/natural-replies (com/as-bytes (rr (xs->in+ "+1")))) "1")
          "INNER read modes dominated")
        (is (= (com/natural-replies (com/as-long  (rr (xs->in+ "+1")))) "1")
          "INNER parsers dominated")]))

   (testing "Strict public parser opts"
     [(is (throws? :ex-info {:eid :carmine/invalid-parser-opts}
            (com/parse {:read-mode :bytes} identity (rr (xs->in+ "+x"))))
        "Internal `:read-mode` is publicly unreachable")
      (is (throws? :ex-info {:eid :carmine/invalid-parser-opts}
            (com/parse {:parse-nulls? true} identity (rr (xs->in+ "+x")))))
      (is (throws? :ex-info {:eid :carmine/invalid-parser-opts}
            (com/parse-aggregates {:parse-null-replies? true} nil
              (com/parsing-rf [] conj) (rr (xs->in+ "*0"))))
        "`parse-aggregates` never sees nulls: the opt would be inert")
      (is (throws? :ex-info {:eid :carmine/invalid-parser-opts}
            (com/parse-aggregates {:kv-rf? true} nil
              (com/parsing-rf [] conj) (rr (xs->in+ "*0")))))])])

(deftest _utf8-simple-strings
  [(is (= (rr (xs->in+ "+héllo wörld ✓")) "héllo wörld ✓"))
   (is (com/reply-error? {:eid :carmine.read/redis-error-reply, :message "ERR héllo"}
         (rr (xs->in+ "-ERR héllo"))))])

(def ^:private parser-composition-gen
  (gen/tuple
    (gen/vector (gen/choose -1000 1000) 0 30)
    (gen/choose 0 20)
    gen/boolean))

(defspec _aggregate-parser-composition-preserves-framing 100
  (prop/for-all [[xs limit streaming?] parser-composition-gen]
    (let [header (if streaming? "*?" (str "*" (count xs)))
          wire
          (apply com/xs->str+
            (concat [header]
              (map #(str ":" %) xs)
              (when streaming? ["."])
              ["+tail"]))
          in    (com/str->in wire)
          xform (comp (map inc) (filter odd?) (take limit))
          parsed
          (com/parse-aggregates {} xform (com/parsing-rf [] conj)
            (rr in))]
      (= [parsed (rr in)]
        [(into [] xform xs) "tail"]))))

(deftest _map-parser-early-termination-preserves-framing
  (doseq [wire
          [(com/xs->str+ "%3" "+k1" ":1" "+k2" ":2" "+k3" ":3" "+tail")
           (com/xs->str+ "%?" "+k1" ":1" "+k2" ":2" "+k3" ":3" "." "+tail")]]
    (let [in     (com/str->in wire)
          parsed
          (com/parse-aggregates {} (take 1) (com/parsing-rf [] conj)
            (rr in))]
      [(is (= (mapv vec parsed) [["k1" 1]]))
       (is (= (rr in) "tail"))])))

(defn- blob-in [streaming? ^bytes ba]
  (com/ba->in
    (if streaming?
      (render-streaming-blob ba)
      (com/xs->ba+ (str "$" (alength ba)) ba))))

(deftest _blob-decoding
  (let [decode
        (fn [read-opts streaming? ba]
          (read/read-reply read-opts (blob-in streaming? ba)))
        plain-opts (com/get-read-opts
                     {:auto-thaw? true, :issue-83-workaround? false})
        bytes-opts (com/get-read-opts
                     {:read-mode :bytes, :auto-thaw? true
                      :issue-83-workaround? false})
        raw-opts (com/get-read-opts
                   {:read-mode :bytes, :auto-thaw? false
                    :issue-83-workaround? false})]

    (testing "Fixed and streamed blobs share marker semantics"
      (let [data    {:marked "nippy", :values [1 2 3]}
            payload (nippy/freeze data)
            marked  (com/xs->ba com/ba-npy payload)
            binary  (com/xs->ba com/ba-bin (enc/str->utf8-ba "abc"))]
        (doseq [streaming? [false true]]
          [(is (= (decode plain-opts streaming? (enc/str->utf8-ba "\u0000more"))
                 "\u0000more")
             "An unrecognized null prefix remains ordinary content")
           (is (nil? (decode plain-opts streaming? com/ba-nil)))
           (is (enc/ba= (decode plain-opts streaming? binary)
                 (enc/str->utf8-ba "abc")))
           (is (= (decode plain-opts streaming? marked) data))
           (is (enc/ba= (decode bytes-opts streaming? marked) payload)
             "Bytes mode removes a recognized marker without thawing")
           (is (enc/ba= (decode raw-opts streaming? marked) marked)
             "Disabling auto-thaw preserves exact bytes")])))

    (testing "Explicit thawing"
      (let [data {:unmarked "nippy", :values [1 2 3]}
            ba   (nippy/freeze data)
            thaw-opts (com/get-read-opts
                        {:read-mode (ReadThawed. {}), :auto-thaw? false})]
        (doseq [streaming? [false true]]
          [(is (= (decode thaw-opts streaming? ba) data)
             "Explicit thaw supports legacy unmarked Nippy blobs")
           (is (com/reply-error? {:eid :carmine.read/nippy-thaw-error}
                 (decode thaw-opts streaming? (byte-array 0)))
             "Empty fixed and streamed blobs behave consistently")])))

    (testing "Encrypted Nippy markers"
      (let [data    (nippy/stress-data {:comparable? true})
            pwd     [:salted "secret"]
            payload (nippy/freeze data {:password pwd})
            marked  (com/xs->ba com/ba-npy payload)
            good-opts (com/get-read-opts
                        {:read-mode (ReadThawed. {:password pwd})
                         :auto-thaw? true, :issue-83-workaround? false})]
        (doseq [streaming? [false true]]
          [(is (= (decode good-opts streaming? marked) data))
           (let [reply (decode plain-opts streaming? marked)]
             [(is (com/reply-error?
                    {:eid :carmine.read/nippy-thaw-error} reply))
              (is (enc/ba= (-> reply ex-data :bytes :content) payload)
                "A thaw error retains the unmarked payload")])])))))

(deftest _skip-mode-marked-blobs
  ;; Skipped blobs are discarded without marker decoding or Nippy thawing
  (let [marked-ba (com/xs->ba com/ba-npy (nippy/freeze {:a 1}))
        len       (alength ^bytes marked-ba)
        n-thaws_  (atom 0)]
    [(with-redefs [nippy/thaw (fn [& _] (swap! n-thaws_ inc) ::thawed)]
       (doseq [read-mode [:skip :skip+errors]]
         (let [read-opts
               (com/get-read-opts
                 {:read-mode read-mode, :auto-thaw? true
                  :issue-83-workaround? false})
               in (xs->in+ (str "$" len) marked-ba "+tail")]
           [(is (identical? (read/read-reply read-opts in)
                  com/sentinel-skipped-reply))
            (is (= (read/read-reply read-opts-plain in) "tail")
              "Skipped marked blobs are fully consumed")])))
     (is (zero? @n-thaws_) "Skip mode never thaws discarded blobs")

     (let [in (chunked-in (com/xs->ba+ "$3" "abc" "+tail") 1)]
       [(is (identical? (read/read-reply com/read-opts-skip in)
              com/sentinel-skipped-reply))
        (is (= (read/read-reply read-opts-plain in) "tail")
          "Skipping completes even when the underlying stream cannot skip")])]))

(deftest _issue-83-capture
  ;; Like all decode options, the issue-83 workaround is captured to
  ;; `ReadOpts` at enqueue time, never consulted during lazy reply reading
  (let [data    {:a 1}
        payload (nippy/freeze data) ; Starts with Nippy header
        marked  (com/xs->ba com/ba-bin payload)
        len     (alength ^bytes marked)
        opts-with
        (binding [core/*auto-thaw?* true, core/*issue-83-workaround?* true]
          (com/get-read-opts))
        opts-sans
        (binding [core/*auto-thaw?* true, core/*issue-83-workaround?* false]
          (com/get-read-opts))]
    [(is (= (binding [core/*issue-83-workaround?* false]
              (read/read-reply opts-with (xs->in+ (str "$" len) marked)))
           data)
       "Workaround is captured before lazy reply reading")
     (is (enc/ba=
           (binding [core/*issue-83-workaround?* true]
             (read/read-reply opts-sans (xs->in+ (str "$" len) marked)))
           payload)
       "Captured false mode is likewise stable")]))

;;;; Write

(def ^:const an-uncached-num (inc write/max-num-to-cache))

(deftest _write-nums
  [(is (= (com/with-out->str (write/write-array-len     out              12))    "*12\r\n"))
   (is (= (com/with-out->str (write/write-array-len     out an-uncached-num)) "*1025\r\n"))

   (is (= (com/with-out->str (#'write/write-bulk-len    out              12))    "$12\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-len    out an-uncached-num)) "$1025\r\n"))

   (is (= (com/with-out->str (#'write/write-bulk-long   out              12)) "$2\r\n12\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-long   out an-uncached-num)) "$4\r\n1025\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-long   out write/min-num-to-cache)) "$5\r\n-1024\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-long   out (dec write/min-num-to-cache))) "$5\r\n-1025\r\n"))

   (is (= (com/with-out->str (#'write/write-bulk-double out              12)) "$4\r\n12.0\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-double out an-uncached-num)) "$6\r\n1025.0\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-double out            12.5)) "$4\r\n12.5\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-double out            -0.0)) "$4\r\n-0.0\r\n")
     "Negative zero misses the whole-double cache")
   (is (= (com/with-out->str (#'write/write-bulk-float out (float 1.2)))
         "$3\r\n1.2\r\n")
     "Floats use their shortest round-tripping representation")
   (is (= (enc/utf8-ba->str (write/arg-payload-bytes (float 1.2))) "1.2")
     "Routing and wire representations agree")])

(deftest _write-bulk-str
  [(is (= (com/with-out->str (#'write/write-bulk-str out "hello\r\n"))    "$7\r\nhello\r\n\r\n"))
   (is (= (com/with-out->str (#'write/write-bulk-str out enc/a-utf8-str)) "$47\r\nHi ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ 10\r\n"))

   (testing "reserve-null!"
     [(is (nil?                                                (#'write/reserve-null! "")))
      (is (throws? :ex-info {:eid :carmine.write/null-reserved} (#'write/reserve-null! "\u0000<")))])

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
     "Multiple frozen arguments sharing dynamic config")

   (testing "Outer freeze composes with bytes wrappers"
     (let [source (byte-array [0 60 78 80 89 0 13 -1 127 -128])
           expected (aclone source)
           freeze-opts {:compressor nil}
           frozen-payload
           (fn [wrapped]
             (binding [core/*auto-freeze?* false]
               (write/arg-payload-bytes (write/prepare-arg wrapped))))
           direct (write/freeze freeze-opts source)
           wrapped (write/freeze freeze-opts (write/bytes source))
           [multi-1 multi-2]
           (write/freeze freeze-opts
             (write/bytes source) (write/bytes (aclone source)))]
       [(is (enc/ba= (frozen-payload wrapped) (frozen-payload direct))
          "freeze(bytes ba) is wire-equivalent to freeze(ba)")
        (is (enc/ba= (frozen-payload wrapped)
              (nippy/freeze expected freeze-opts))
          "The explicit outer options govern byte-array serialization")
        (is (identical? wrapped (write/freeze freeze-opts wrapped))
          "Repeated freeze with equal options retains the eager wrapper")
        (is (every? true?
              (map #(enc/ba= (frozen-payload %) (frozen-payload direct))
                [multi-1 multi-2]))
          "Multi-argument freeze normalizes every bytes wrapper")
        (aset-byte source 0 (byte 42))
        (is (enc/ba= (nippy/thaw (frozen-payload wrapped) freeze-opts) expected)
          "The outer freeze eagerly snapshots mutable bytes")]))])

(deftest _prepared-arguments
  (let [ba (enc/str->utf8-ba "abc")
        command (binding [core/*auto-freeze?* true]
                  (write/prepare-command ["SET" (write/bytes ba) ba]))]
    (aset-byte ba 0 (byte (int \z)))
    (is (= (binding [core/*auto-freeze?* false]
             (com/with-out->str (write/write-command out command)))
          "*3\r\n$3\r\nSET\r\n$3\r\nabc\r\n$5\r\n\u0000<abc\r\n")
      "Enqueue snapshots mutable bytes and captures marker policy"))

  (let [baos (java.io.ByteArrayOutputStream.)
        out  (java.io.BufferedOutputStream. baos)]
    (is (throws? :ex-info {:eid :carmine.write/empty-command}
          (#'write/write-requests out [["PING"] [] ["PING"]])))
    (.flush out)
    (is (zero? (.size baos))
      "A later invalid request cannot leave an earlier command on the wire"))

  (doseq [invalid [(char 0) (keyword (str (char 0) "bad"))]]
    (let [baos (java.io.ByteArrayOutputStream.)
          out  (java.io.BufferedOutputStream. baos)]
      (is (throws? :ex-info {:eid :carmine.write/null-reserved}
            (#'write/write-requests out [["PING"] ["ECHO" invalid]])))
      (.flush out)
      (is (zero? (.size baos))
        "All text arguments are validated before a batch writes bytes"))))

(deftest _fallback-freeze-options
  (let [value [:non-native (apply str (repeat 1024 "x"))]
        freeze-opts {:compressor nippy/lz4-compressor}
        [expected wire]
        (binding [core/*auto-freeze?* true, core/*freeze-opts* freeze-opts]
          [(write/arg-payload-bytes (write/prepare-arg value))
           (com/with-out (write/write-command out ["ECHO" value]))])
        [_ actual] (read/read-reply read-opts-bytes (com/ba->in wire))]
    (is (enc/ba= actual expected)
      "Direct writing and prepared routing honor the same Nippy options")))

(deftest _connection-state-marking
  ;; The prefilter fast path must agree with the definitive string path
  (let [marked?
        (fn [cmd]
          (let [reusable?_ (volatile! true)]
            (binding [write/*conn-reusable?_ reusable?_]
              (com/with-out (#'write/write-requests out [cmd])))
            (not @reusable?_)))]
    [(is (false? (marked? ["GET" "k"])))
     (is (false? (marked? ["SET" "k" "v"])))
     (is (false? (marked? ["DEL" "k"])))
     (is (false? (marked? ["LPUSH" "k" "v"])))
     (is (false? (marked? ["SCAN" "0"]))
       "Prefilter collision (SCAN vs SYNC) resolves via the definitive path")
     (is (false? (marked? ["CLIENT" "LIST"])))
     (is (false? (marked? [:get "k"])) "Uncommon first-arg types still work")
     (is (true?  (marked? ["MULTI"])))
     (is (true?  (marked? ["multi"])) "Case-insensitive")
     (is (true?  (marked? ["SUBSCRIBE" "chan"])))
     (is (true?  (marked? ["watch" "k"])))
     (is (true?  (marked? ["CLIENT" "SETNAME" "x"])))
     (is (true?  (marked? [:multi])) "Uncommon first-arg types still work")]))

(deftest _write-requests
  [(testing "Basics"
     [(let [args   ["CONFIG" "SET" "timeout" 10]
            prefix (write/encode-command-prefix ["CONFIG" "SET"])
            generic
            (com/with-out->str
              (write/write-command out args))
            encoded
            (com/with-out->str
              (write/write-command out args prefix 2))]
        [(is (= (enc/utf8-ba->str prefix)
               "$6\r\nCONFIG\r\n$3\r\nSET\r\n")
           "Static command tokens include complete bulk frames but no array header")
         (is (= encoded generic
                "*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$7\r\ntimeout\r\n$2\r\n10\r\n")
           "Pre-encoding preserves exact mixed static/dynamic command framing")])

      (is (= (enc/utf8-ba->str  @#'write/bulk-nil)                               "$2\r\n\u0000_\r\n"))
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
           (let [wire (com/with-out (#'write/write-requests out [[{}]]))]
             (is (= (binding [core/*auto-thaw?* true]
                      (rr (com/ba->in wire)))
                    [{}])
               "Frozen arguments carry a readable Nippy marker without pinning encoder bytes"))

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
