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
   [taoensso.nippy.tools :as nippy-tools]

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
   [taoensso.carmine_v4.resp.common ReadOpts ReadThawed RespInputState #_Parser]
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

(deftest _stream-discards
  (let [state (fn [s] (com/input-state (com/xs->in+ s)))]
    [(is (->>   (com/discard-stream-separator (state ""))  (throws? :ex-info {:eid :carmine.read/missing-stream-separator})))
     (is (->>   (com/discard-crlf             (state "_")) (throws? :ex-info {:eid :carmine.read/missing-crlf})))
     (is (true? (com/discard-crlf             (state ""))))]))

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
        ctx (Ctx. false false pending-reqs pending-replies nil nil nil nil (object-array 1))]
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

;; Read opts are cached per binding frame, so check that each request still
;; captures the opts in effect when it was queued
(deftest _request-read-opts
  (let [pending-reqs (LinkedList.)
        ctx (Ctx. false false pending-reqs (LinkedList.) nil nil nil nil (object-array 1))
        ro  (fn []
              (let [req (.pollFirst pending-reqs)]
                (if (instance? taoensso.carmine_v4.resp.LocalEchoReq req)
                  (.-read-opts ^taoensso.carmine_v4.resp.LocalEchoReq req)
                  (.-read-opts ^taoensso.carmine_v4.resp.Req          req))))
        ro1 (fn [] (resp/ping) (ro))]
    (binding [resp/*ctx* ctx]
      (let [d1 (ro1), d2 (ro1)
            p1 (com/as-long  (ro1))
            d3 (ro1)
            b1 (com/as-bytes (ro1))
            p2 (com/as-long  [(ro1) (ro1)])
            k1 (binding [core/*keywordize-maps?* (not core/*keywordize-maps?*)] (ro1))
            d4 (ro1)
            n1 (com/as-long (com/as-bytes (ro1)))
            e1 (do (resp/local-echo :x) (ro))
            e2 (com/as-long (resp/local-echo :x) (ro))
            s1 (com/skip-replies (ro1))

            ;; Non-default opts, since default opts are a shared constant
            ;; that'd be identical with or without caching
            [k2 k3 k4 k5 k6]
            (binding [core/*keywordize-maps?* (not core/*keywordize-maps?*)]
              [(ro1) (ro1) (com/as-long (ro1)) (ro1) (ro1)])

            ;; Conveyed bindings get a new frame (futures clone their parent's,
            ;; bound fns push one), so must see the same opts without the cache
            [f1 f2 f3]
            (com/as-bytes
              [(ro1) @(future (ro1)) ((bound-fn [] (ro1)))])
            f4 @(future (com/as-bytes (ro1)))
            f5 @(future (ro1))
            d5 (ro1)]
        [(is (identical? d1 d2))
         (is (identical? d1 d3))
         (is (identical? d1 d4))
         (is (identical? d1 e1) "Shared with local echos")
         (is (nil?  (.-parser ^ReadOpts d1)))
         (is (some? (.-parser ^ReadOpts p1)))
         (is (identical? (.-parser ^ReadOpts p1) (.-parser ^ReadOpts (first p2))))
         (is (identical? (first p2) (second p2)) "Cached within binding")
         (is (identical? (.-parser ^ReadOpts p1) (.-parser ^ReadOpts e2)))
         (is (= (.-read-mode ^ReadOpts b1) :bytes))
         (is (nil? (.-read-mode ^ReadOpts d3)))
         (is (= (.-keywordize-maps? ^ReadOpts k1) (not core/*keywordize-maps?*)))
         (is (= (.-keywordize-maps? ^ReadOpts d4)      core/*keywordize-maps?*))
         (is (nil?  (.-read-mode ^ReadOpts n1)) "Coercers force default read mode")
         (is (some? (.-parser    ^ReadOpts n1)))
         (is (= (.-read-mode ^ReadOpts s1) :skip))

         (is (= (.-keywordize-maps? ^ReadOpts k2) (not core/*keywordize-maps?*)))
         (is (identical? k2 k3) "Cached within binding")
         (is (identical? k5 k6) "Cached within binding")
         (is (not (identical? k2 k4)))
         (is (some? (.-parser ^ReadOpts k4)))
         (is (= (.-keywordize-maps? ^ReadOpts k4) (not core/*keywordize-maps?*))
           "Nested bindings keep outer opts")
         (is (nil? (.-parser ^ReadOpts k5)) "Opts restored after nested binding")
         (is (= (.-keywordize-maps? ^ReadOpts k5) (not core/*keywordize-maps?*)))

         (is (= (.-read-mode ^ReadOpts f1) :bytes))
         (is (= (.-read-mode ^ReadOpts f2) :bytes) "Conveyed to future")
         (is (= (.-read-mode ^ReadOpts f3) :bytes) "Conveyed to bound fn")
         (is (= (.-read-mode ^ReadOpts f4) :bytes) "Bound within future")
         (is (nil? (.-read-mode ^ReadOpts f5)))
         (is (nil? (.-read-mode ^ReadOpts d5)))]))))

(deftest _request-command-compatibility
  (let [pending-reqs    (LinkedList.)
        pending-replies (LinkedList.)
        ctx (Ctx. false false pending-reqs pending-replies nil nil nil nil (object-array 1))]
    (binding [resp/*ctx* ctx]
      [(doseq [command [["subscribe" "channel"]
                        ["PSUBSCRIBE" "pattern"]
                        ["SSUBSCRIBE" "shard"]
                        ["UNSUBSCRIBE"]
                        ["PUNSUBSCRIBE"]
                        ["SUNSUBSCRIBE"]
                        ["CLIENT" "REPLY" "OFF"]
                        ["CLIENT" "REPLY" "SKIP"]
                        ["monitor"]
                        [(write/bytes (enc/str->utf8-ba "MoNiToR"))]
                        ["PSYNC" "?" -1]
                        ["SYNC"]
                        ["script" "debug" "yes"]
                        ["SCRIPT" "DEBUG" "SYNC"]]]
         [(is (throws? :ex-info {:eid :carmine/invalid-request-command}
                (resp/rcmd* command)))
          (is (zero? (.size pending-reqs)))])

       (is (throws? :ex-info
             {:eid :carmine/invalid-request-command, :command "SUBSCRIBE"}
             (core/subscribe "channel"))
         "Generated commands use the same guard")
       (is (zero? (.size pending-reqs)))

       (doseq [call [core/monitor core/sync #(core/psync "?" -1)
                    #(core/script-debug "YES") #(core/script-debug "SYNC")]]
         [(is (throws? :ex-info
                {:eid :carmine/invalid-request-command, :reason :streaming-command}
                (call)))
          (is (zero? (.size pending-reqs)))])

       (is (throws? :ex-info
             {:eid :carmine/invalid-request-command, :command "MONITOR"}
             (resp/rcmds* [["PING"] ["MONITOR"] ["PING"]])))
       (is (zero? (.size pending-reqs))
         "Streaming commands are rejected before any batch request is queued")

       (is (throws? :ex-info
             {:eid :carmine/invalid-request-command, :command "CLIENT REPLY SKIP"}
             (resp/rcmds* [["PING"] ["CLIENT" "REPLY" "SKIP"] ["PING"]])))
       (is (zero? (.size pending-reqs))
         "An incompatible raw batch is rejected before queue mutation")

       (is (nil? (resp/rcmd* ["CLIENT" "REPLY" "ON"])))
       (is (nil? (resp/rcmd* ["SCRIPT" "DEBUG" "NO"])))
       (is (= (.size pending-reqs) 2)
         "Disabling no-reply or debugger modes still has an ordinary reply")])))

(def ^:private ^:dynamic *resp-input-buffer-size*
  "When non-nil, test inputs are `resp-input` streams with this buffer size."
  nil)

(defn- limited-in [limits ^String wire]
  (if-let [buffer-size *resp-input-buffer-size*]
    (com/resp-input (java.io.ByteArrayInputStream. (enc/str->utf8-ba wire))
      buffer-size limits)
    (com/str->limited-in wire limits)))

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
      (is (= (:actual
               (ex-data (resource-error {:max-frame-bytes 5} "+abcdef\r\n"))) 6)
        "Reports the first byte over the limit, however the line was buffered")
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
      (is (com/reply-error? {:eid :carmine.read/redis-error-reply}
            (read/read-reply com/read-opts-skip
              (com/str->in "-MOVED 1 host:7000\r\n"))))
      (is (com/reply-error? {:eid :carmine.read/redis-error-reply}
            (read/read-reply com/read-opts-skip
              (com/str->in "!17\r\nMOVED 1 host:7000\r\n"))))
      (is (= (read/read-reply com/read-opts-skip
               (com/str->in "*2\r\n+ok\r\n-TRYAGAIN nested\r\n"))
            com/sentinel-skipped-reply)
        "Nested aggregate errors cannot trigger top-level Cluster retries")
      (is (= (read/read-reply com/read-opts-skip
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
  (malformed-reply-in?
    (if-let [buffer-size *resp-input-buffer-size*]
      #(com/resp-input (java.io.ByteArrayInputStream. wire) buffer-size nil)
      #(com/ba->in wire))))

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
        nbytes (alength ba)
        wrap
        (fn [^InputStream raw]
          (com/resp-input raw (or *resp-input-buffer-size* 8192) nil))]
    (wrap
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

(def ^:private read-opts-plain (ReadOpts. nil    nil false false false false nil nil))
(def ^:private read-opts-bytes (ReadOpts. :bytes nil false false false false nil nil))

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
                (if top-error?
                  (com/reply-error? {:eid :carmine.read/redis-error-reply} reply)
                  (identical? reply com/sentinel-skipped-reply))
                (= (read/read-reply read-opts-plain in) "tail"))))]
      (and
        (check-mode read-opts-plain false)
        (check-mode read-opts-bytes true)
        (check-skip com/read-opts-skip)))))

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
          (for [read-opts [com/read-opts-skip]
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
             [com/read-opts-skip com/sentinel-skipped-reply]]]
      (let [in (com/str->in "$?\r\n;3\r\nabc\r\n;0\r\n+tail\r\n")]
        [(is (= (read/read-reply read-opts in) expected))
         (is (= (read/read-reply read-opts-plain in) "tail"))])))

  (testing "Skipped streaming maps consume complete key/value pairs"
    (doseq [read-opts [com/read-opts-skip]]
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

(deftest _blob-size-boundaries
  ;; Blobs are read in place when they and their CRLF are already buffered.
  ;; With the blob header that's at 57 and 8183 bytes for the first reply
  ;; here with 64 and 8192 byte buffers, see `_buffered-resp-input`.
  (testing "Blobs around the buffer sizes of `resp-input` streams"
    (doseq [n          (concat [0 1 2 3] (range 54 68) [2000] (range 8182 8196))
            chunk-size [1 7 4096 16384]]
      (let [ba   (byte-array n (map #(unchecked-byte (+ 97 (mod % 26))) (range n)))
            wire (com/xs->ba+ (str "$" n) ba (str "$" n) ba "+tail")]
        [(let [in (chunked-in wire chunk-size)]
           [(is (= (read/read-reply read-opts-plain in) (enc/utf8-ba->str ba)))
            (is (= (read/read-reply read-opts-plain in) (enc/utf8-ba->str ba)))
            (is (= (read/read-reply read-opts-plain in) "tail"))])

         (let [in  (chunked-in wire chunk-size)
               ba1 (read/read-reply read-opts-bytes in)
               ba2 (read/read-reply read-opts-bytes in)]
           [(is (enc/ba= ba1 ba) "Earlier reply unaffected by later reads")
            (is (enc/ba= ba2 ba))
            (is (= (read/read-reply read-opts-plain in) "tail"))])])))

  (testing "Small blobs with a missing or invalid CRLF"
    (doseq [[suffix-bytes read]
            [[[]       [-1  -1]]
             [[13]     [13  -1]]
             [[13 120] [13 120]]
             [[255 10] [255 10]]]]
      (let [wire  (byte-array (concat (map int "$1\r\nx") suffix-bytes))
            reply (read/read-reply read-opts-plain (com/ba->in (bytes wire)))]
        [(is (com/reply-error? {:eid :carmine.read/unexpected-read-error} reply))
         (is (= (select-keys (ex-data (ex-cause reply)) [:eid :read])
               {:eid :carmine.read/missing-crlf, :read read}))]))))

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
      (and (check read-opts-plain) (check com/read-opts-skip)))))

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

(declare _blob-markers _blob-decoding)

(deftest _buffer-refill-boundaries
  ;; Every buffer size from 1 up has each refill boundary fall at each
  ;; position of these replies in turn: within lengths, payloads, and CRLFs
  (let [marked (com/xs->ba com/ba-bin (enc/str->utf8-ba "abc"))
        wire   (com/xs->ba+ "$3" "abc" "$5" marked "*1" ":42" "+tail")
        in     (fn [buffer-size limits]
                 (com/resp-input (java.io.ByteArrayInputStream. wire)
                   buffer-size limits))
        auto-opts (com/get-read-opts {:auto-thaw? true})]

    (doseq [buffer-size (range 1 (+ (alength ^bytes wire) 2))]
      (testing (str "Buffer size " buffer-size)
        [(let [in (in buffer-size nil)]
           [(is (= (read/read-reply read-opts-plain in) "abc"))
            (is (enc/ba= (read/read-reply auto-opts in) (enc/str->utf8-ba "abc")))
            (is (= (read/read-reply read-opts-plain in) [42]))
            (is (= (read/read-reply read-opts-plain in) "tail"))])

         (let [in (in buffer-size nil)]
           [(is (identical? (read/read-reply com/read-opts-skip in) com/sentinel-skipped-reply))
            (is (identical? (read/read-reply com/read-opts-skip in) com/sentinel-skipped-reply))
            (is (identical? (read/read-reply com/read-opts-skip in) com/sentinel-skipped-reply))
            (is (= (read/read-reply read-opts-plain in) "tail"))])

         (testing "Frame limits count bytes consumed, not bytes buffered"
           (doseq [read-opts [read-opts-plain com/read-opts-skip]]
             [(let [in (in buffer-size {:max-frame-bytes 11})] ; Largest reply
                [(is (not (com/reply-error? (read/read-reply read-opts in))))
                 (is (not (com/reply-error? (read/read-reply read-opts in))))
                 (is (not (com/reply-error? (read/read-reply read-opts in))))
                 (is (= (read/read-reply read-opts-plain in) "tail"))])

              (let [in (in buffer-size {:max-frame-bytes 10})]
                [(is (not (com/reply-error? (read/read-reply read-opts in))))
                 (is (= (:limit (ex-data (truss/throws (read/read-reply read-opts in))))
                       :max-frame-bytes))])]))]))))

(deftest _large-blob-resource-limits
  ;; Blobs larger than the input's buffer are read and skipped without it,
  ;; and must still count toward the limit for the rest of their frame
  (let [n     1000
        ba    (byte-array n (byte 97))
        frame (com/xs->ba+ "*2" (str "$" n) ba "+abc")
        wire  (com/xs->ba frame (com/xs->ba+ "+tail"))
        size  (alength ^bytes frame)
        in   (fn [buffer-size limits]
               (com/resp-input (java.io.ByteArrayInputStream. wire)
                 buffer-size limits))]

    (doseq [buffer-size [1 3 64 512]
            read-opts   [read-opts-plain com/read-opts-skip]]
      [(let [in (in buffer-size {:max-frame-bytes size, :max-blob-bytes n})]
         [(is (not (com/reply-error? (read/read-reply read-opts in))))
          (is (= (read/read-reply read-opts-plain in) "tail"))])

       (is (= (:limit
                (ex-data
                  (truss/throws
                    (read/read-reply read-opts
                      (in buffer-size {:max-frame-bytes (dec size)})))))
             :max-frame-bytes))

       (is (= (:limit
                (ex-data
                  (truss/throws
                    (read/read-reply read-opts
                      (in buffer-size {:max-blob-bytes (dec n)})))))
             :max-blob-bytes))])))

(deftest _long-lines
  ;; Lines are read in place from the input's buffer, which grows as needed
  ;; for lines longer than it
  (let [n    5000
        line (apply str (repeat n \a))
        in   (fn [buffer-size limits ^String wire]
               (com/resp-input (java.io.ByteArrayInputStream. (enc/str->utf8-ba wire))
                 buffer-size limits))]

    [(doseq [buffer-size [1 3 64 4096 8192]]
       [(let [in (in buffer-size {:max-line-bytes n} (str "+" line "\r\n+tail\r\n"))]
          [(is (= (rr in) line))
           (is (= (rr in) "tail"))])

        (let [in (in buffer-size nil (str "+" line "\r\n+tail\r\n"))]
          [(is (= (read/read-reply com/read-opts-skip in) com/sentinel-skipped-reply))
           (is (= (rr in) "tail"))])

        (is (truss/submap?
              (ex-data (truss/throws (rr (in buffer-size {:max-line-bytes (dec n)} (str "+" line "\r\n")))))
              {:eid :carmine.read/resource-limit
               :limit :max-line-bytes, :max (dec n), :actual n}))

        (doseq [[wire read] [["+abc" "abc"] ["+abc\r" "abc\r"] ["+abc\rx" "abc\rx"] ["+abc\n" "abc\n"]
                             [(str "+" line) line] [(str "+" line "\r") (str line "\r")]]]
          (is (= (select-keys (ex-data (ex-cause (rr (in buffer-size nil wire)))) [:eid :read])
                {:eid :carmine.read/missing-crlf, :read read})))])

     (testing "Long lines arriving in small chunks"
       ;; Each read then adds little to the buffer, so the line must be
       ;; scanned incrementally rather than afresh after each read
       (doseq [[n chunk-size] [[5000 1] [5000 7] [300000 1]]
               buffer-size    [1 64]]
         (let [line (apply str (repeat n \a))
               wire (enc/str->utf8-ba (str "+" line "\r\n+tail\r\n"))
               t0   (System/currentTimeMillis)
               in   (binding [*resp-input-buffer-size* buffer-size]
                      (chunked-in wire chunk-size))]
           [(is (= (rr in) line))
            (is (= (rr in) "tail"))
            (is (< (- (System/currentTimeMillis) t0) 10000) "Linear time")])))

     (testing "Buffer growth is bounded by the line limit"
       (doseq [buffer-size [1 64]]
         (let [max-line 1000
               in  (in buffer-size {:max-line-bytes max-line} (str "+" line "\r\n"))
               _   (is (= (:limit (ex-data (truss/throws (rr in)))) :max-line-bytes))
               ^RespInputState state (com/input-state in)]
           (is (<= (alength (.buf state)) (* 2 (+ max-line 2)))))))

     (testing "Blobs after the buffer has grown for a long line"
       (let [small (apply str (repeat 100   \b))
             large (apply str (repeat 20000 \c))
             wire  (str "+" line "\r\n$100\r\n" small "\r\n$20000\r\n" large "\r\n+tail\r\n")
             size  (count wire)]
         [(let [in (in 64 nil wire)
                ^RespInputState state (com/input-state in)]
            [(is (= (rr in) line))
             (is (> (alength (.buf state)) n) "Buffer grew for long line")
             (is (= [(rr in) (rr in) (rr in)] [small large "tail"]))
             (is (= (alength (.buf state)) 64) "Buffer back to its initial size")])

          (let [in (in 64 {:max-frame-bytes 20010} wire)]
            (is (= [(rr in) (rr in) (rr in) (rr in)] [line small large "tail"])))

          (let [in (in 64 {:max-frame-bytes 20009} wire)]
            [(is (= [(rr in) (rr in)] [line small]))
             (is (= (:limit (ex-data (truss/throws (rr in)))) :max-frame-bytes))])]))

     (is (= (:eid
              (ex-data
                (truss/throws
                  (rr (java.io.DataInputStream.
                        (java.io.ByteArrayInputStream. (enc/str->utf8-ba "+ok\r\n")))))))
           :carmine.read/unexpected-input)
       "Reply reading needs a `resp-input`")

     (testing "Frame limit bounds an unterminated line with no line limit"
       (doseq [buffer-size [1 64 8192]]
         (let [total 1000000
               raw   (java.io.ByteArrayInputStream.
                       (enc/str->utf8-ba (str "+" (apply str (repeat total \a)))))
               in    (com/resp-input raw buffer-size {:max-frame-bytes 100})]
           [(is (truss/submap? (ex-data (truss/throws (rr in)))
                  {:eid :carmine.read/resource-limit
                   :limit :max-frame-bytes, :max 100, :actual 101}))
            (is (> (.available raw) (- total 20000)) "Stopped reading early")])))]))

(deftest _buffered-resp-input
  ;; `resp-input` streams read lines and blobs directly from their buffer
  ;; when possible, so repeat the stream tests above with small buffers, which
  ;; have lines and blobs cross buffer boundaries.
  (doseq [buffer-size [1 3 64]]
    (binding [*resp-input-buffer-size* buffer-size]
      (testing (str "Buffer size " buffer-size)
        [(doseq [spec [_generated-resp-trees-survive-fragmentation
                       _generated-resp-maps-and-sets-survive-fragmentation
                       _generated-error-replies-preserve-framing
                       _generated-truncated-resp-is-rejected
                       _generated-command-writes-round-trip-through-reader]]
           (let [result (spec 100)]
             (is (true? (:result result)) (pr-str (dissoc result :result)))))
         (_aggregate-branch-framing)
         (_blob-size-boundaries)
         (_read-resource-limits)
         (_blob-markers)
         (_blob-decoding)]))))

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
   (let [in (com/str->in "_\r\n*3\r\n_\r\n$1\r\na\r\n_\r\n+next\r\n")]
     (is (= [(rr in) (rr in) (rr in)] [nil [nil "a" nil] "next"])
       "RESP3 nulls keep the stream aligned at top level and when nested"))
   (is (malformed-reply? "_x\r\n")         "RESP3 null with payload")
   (is (malformed-reply? "*1\r\n_x\r\n") "Nested RESP3 null with payload")

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
  (doseq [wire [":١\r\n" ": 1\r\n" ":\r\n" ":?\r\n" ":-\r\n" ":1x\r\n"
                ":9223372036854775808\r\n"
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
   (is (= (let [in (com/str->in "+OK\r\n+Ok\r\n+OX\r\n+O\r\n+OKK\r\n+OK\r\n:1\r\n")]
            (vec (repeatedly 7 #(rr in))))
         ["OK" "Ok" "OX" "O" "OKK" "OK" 1])
     "Simple strings on and off the allocation-free path")
   (is (= (mapv #(rr (com/str->in (str ":" % "\r\n")))
            ["0" "7" "007" "999999999" "1000000000" "-1" "-0" "-999999999"
             "9223372036854775807" "-9223372036854775808"])
         [0 7 7 999999999 1000000000 -1 0 -999999999
          Long/MAX_VALUE Long/MIN_VALUE])
     "Integers on and off the allocation-free path")
   (is (= (rr (com/str->in "(+123456789012345678901234567890\r\n"))
         123456789012345678901234567890N))
   (is (= (rr (com/str->in ",+1.5e+2\r\n")) 150.0))])

(deftest _end-of-stream
  (let [reply (read/read-reply read-opts-plain (com/str->in ""))]
    [(is (com/reply-error? {:eid :carmine.read/unexpected-read-error} reply))
     (is (instance? java.io.EOFException (ex-cause reply))
       "A connection closed before a reply reads as end of stream")
     (is (= (ex-message reply) "[Carmine] Connection error while reading reply"))
     (is (not (contains? (ex-data reply) :kind)) "There's no reply kind to report")]))

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

         (is (=   (com/parse (fn [x] (str x "!")) (rr (xs->in+ "+1"))) "1!"))
         (is (->> (com/parse throw!               (rr (xs->in+ "+1"))) parser-error?))

         (testing "Null and error replies"
           [(is (=  (com/parse (fn [x] [:parsed x]) (rr (xs->in+ "_")))   [:parsed nil]) "Parsers receive nil for null replies")
            (is (=  (com/parse (fn [x] [:parsed x]) (rr (xs->in+ "$-1"))) [:parsed nil]))
            (is (=  (com/parse nil                  (rr (xs->in+ "+1")))  "1") "A nil fn removes the parser")
            (is (=  (let [f nil] (com/parse f       (rr (xs->in+ "+1")))) "1"))
            (is (-> (com/as-double (rr (xs->in+ "+x"))) parser-error?))
            (is (=  (com/parse (fn [_] :outer) (com/parse nil (rr (xs->in+ "+1")))) "1"))
            (is (-> (com/parse (fn [_] :parsed) (rr (xs->in+ "-err"))) com/reply-error?) "Parsers never receive error replies")
            (is (-> (com/as-long    (rr (xs->in+ "_")))  parser-error?) "Strict coercers reject nil")
            (is (-> (com/as-double  (rr (xs->in+ "_")))  parser-error?))
            (is (-> (com/as-kw      (rr (xs->in+ "_")))  parser-error?))
            (is (=  (com/as-?long   (rr (xs->in+ "_")))  nil) "Lenient coercers return nil")
            (is (=  (com/as-?double (rr (xs->in+ "_")))  nil))
            (is (=  (com/as-?kw     (rr (xs->in+ "_")))  nil))
            (is (=  (com/as-long    (rr (xs->in+ "+12"))) 12))
            (is (=  (com/as-?long   (rr (xs->in+ "+x")))  nil))
            (is (-> (com/as-long    (rr (xs->in+ "+x")))  parser-error?))

            (testing ":read-mode (INTERNAL, via `fn-parser` only)"
              [(is (= (binding [com/*parser* (com/fn-parser {:read-mode :bytes} enc/utf8-ba->str)]
                                                                     (rr (xs->in+ "$5" "hello")))   "hello")  "Parser  read mode (:bytes)")
               (is (= (com/parse          enc/utf8-ba->str (com/as-bytes (rr (xs->in+ "$5" "hello")))) "hello")  "Dynamic read mode (:bytes)")
               (is (= (binding [com/*parser* (com/fn-parser {:read-mode nil} #(str % "!"))]
                        (com/as-bytes                                (rr (xs->in+ "$5" "hello")))) "hello!") "Parser  read mode (nil)")])])])

      (testing "Against aggregates"
        [(is (=                        (rr (xs->in+ "*2" ":1" ":2"))              [1 2])    "Baseline...")
         (is (=   (com/parse set    (rr (xs->in+ "*2" ":1" ":2")))            #{1 2})    "Acts as (f <aggr>)")
         (is (=                        (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))   [[1 2] 3]) "Baseline...")
         (is (=   (com/parse set    (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) #{[1 2] 3}) "No nesting")
         (is (->> (com/parse throw! (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) parser-error?))])])

   (testing "rf parsers"
     [(testing "Against aggregates"
        [(is (=                                                                 (rr (xs->in+ "*4" ":1" ":2" ":3" ":4"))      [1 2 3 4])    "Baseline...")
         (is (=   (com/parse-aggregates nil            (com/parsing-rf #{0} conj)   (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  #{0 1 2 3 4})    "Parsed (without xform)")
         (is (=   (com/parse-aggregates nil            (com/parsing-rf #'conj)      (rr (xs->in+ "*2" ":1" ":2")))               [1 2])          "RF may be callable without satisfying fn?")
         (is (=   (com/parse-aggregates (filter even?) (com/parsing-rf #{0} conj)   (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  #{0   2   4})    "Parsed (with    xform)")
         (is (->> (com/parse-aggregates (map throw!)   (com/parsing-rf #{0} conj)   (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  parser-error?) "Trap xform errors")
         (is (->> (com/parse-aggregates (map identity) (com/parsing-rf #{0} throw!) (rr (xs->in+ "*4" ":1" ":2" ":3" ":4")))  parser-error?) "Trap rf    errors")
         (is (com/parse-aggregates nil
               (com/parsing-rf #{} (fn [acc in] (if (= in 1) (throw! in) (conj acc in))))
               (let [r1 (rr (xs->in+ "*2" ":1" ":2"))
                     r2 (rr (xs->in+ "*2" ":3" ":4"))]
                 (and (parser-error? r1) (= r2 #{3 4}))))
           "Caught state is per reduction: a parser error never poisons later replies")
         (is (=   (com/parse-aggregates (map identity) (com/parsing-rf #{0} conj)   (rr (xs->in+ "*4" ":1" "_"  ":2"  "_")))  #{nil 0 1 2})  "Nulls in aggregate")
         (is (=   (com/parse-aggregates nil            (com/parsing-rf #{0} nil)    (rr (xs->in+ "*0")))                      #{0})          "Empty    aggregate")

         (is (=                                                                           (rr (xs->in+ "*4" ":1" ":2" ":3" ":4"))     [1 2 3 4]) "Baseline...")
         (is (= (com/parse-aggregates nil (com/parsing-rf #(transient #{0}) persistent! conj!) (rr (xs->in+ "*4" ":1" ":2" ":3" ":4"))) #{0 1 2 3 4}) "Using transients")
         (let [in (xs->in+ "*1" ":1" "*1" ":2")]
           (is (= (com/parse-aggregates nil
                    (com/parsing-rf #(transient []) persistent! conj!)
                    [(rr in) (rr in)])
                 [[1] [2]])
             "Mutable parser init is fresh for every aggregate reply"))

         (is (=                                                                                         (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2"))  {"k1" 1, "k2" 2}) "Baseline...")
         (is (= (com/parse-aggregates nil (com/parsing-rf {:k0 0} (fn [m [k v]] (assoc m k v))) (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2"))) {:k0  0, "k1" 1, "k2" 2}) "Ignore *keywordize-maps?*")
         (is (= (binding [com/*parser* (com/rf-parser {:kv-rf? true} nil (com/parsing-rf {:k0 0} (fn [m  k v]  (assoc m k v))))]
                                                                          (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2"))) {:k0  0, "k1" 1, "k2" 2}) "With kv-rf (INTERNAL, via `rf-parser` only)")
         (is (= (com/parse-aggregates                   (filter (fn [[k v]] (even? v)))
                  (com/parsing-rf {:k0 0} (fn [m [k v]] (assoc m k v)))
                  (rr (xs->in+ "%2" "+k1" ":1" "+k2" ":2"))) {:k0 0, "k2" 2}) "Aggregate map, with xform")

         (is (=                                                             (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))     [[1 2] 3]) "Baseline...")
         (is (= (com/parse-aggregates nil            (com/parsing-rf #{0} conj) (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) #{0 [1 2] 3}) "No nesting (without xform)")
         (is (= (com/parse-aggregates (map identity) (com/parsing-rf #{0} conj) (rr (xs->in+ "*2" "*2" ":1" ":2" ":3"))) #{0 [1 2] 3}) "No nesting (with    xform)")])

      (testing "Against non-aggregates"
        [(is (= (com/parse-aggregates (map throw!) throw! (rr (xs->in+ "_")))         nil)  "No effect")
         (is (= (com/parse-aggregates (map throw!) throw! (rr (xs->in+ "+hello"))) "hello") "No effect")])])])

(deftest _xform-construction-error-preserves-framing
  (let [bad-xform (fn [_] (throw! :construction))]
    (doseq [wire [(com/xs->str+ "*2" ":1" ":2" "+tail")
                  (com/xs->str+ "*?" ":1" ":2" "." "+tail")]]
      (let [in    (com/str->in wire)
            reply
            (com/parse-aggregates bad-xform
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
            reply  (com/parse (fn [x] (swap! calls_ inc) (str "<" x ">"))
                     (rr (xs->in+ "|1" "+k" "+v" "+hello")))]
        [(is (= (core/reply-content    reply) "<hello>"))
         (is (= (core/reply-attributes reply) {"k" "v"}))
         (is (= @calls_ 1) "Parser applied exactly once")])

      (let [reply (com/parse set (rr (xs->in+ "|1" "+k" "+v" "*2" ":1" ":2")))]
        [(is (= reply #{1 2})           "Parsed value equals parsed bare reply")
         (is (= (core/reply-attributes reply) {"k" "v"}) "Attributes preserved")])])

   (testing "rf parsers apply to content, never to the attribute map"
     (let [reply (com/parse-aggregates nil (com/parsing-rf #{0} conj)
                   (rr (xs->in+ "|1" "+k" "+v" "*2" ":1" ":2")))]
       [(is (= reply #{0 1 2}))
        (is (= (core/reply-attributes reply) {"k" "v"}))]))

   (testing "Ordinary metadata is never misreported as RESP attributes"
     [(is (nil? (core/reply-attributes
                  (com/parse (fn [x] (with-meta [x] {:user :meta}))
                    (rr (xs->in+ "+1"))))))
      (let [reply (com/parse (fn [x] (with-meta [x] {:user :meta}))
                    (rr (xs->in+ "|1" "+k" "+v" "+1")))]
        [(is (= (core/reply-attributes reply) {"k" "v"}))
         (is (= (:user (meta reply)) :meta)
           "User metadata preserved alongside attrs")])])

   (testing "Null content parsing"
     [(is (= (core/reply-content
               (com/parse (fn [_] :parsed)
                 (rr (xs->in+ "|1" "+k" "+v" "_")))) :parsed))])

   (testing "Skip modes"
     [(is (= (read/read-reply com/read-opts-skip
               (xs->in+ "|1" "+k" "+v" "+hello"))
            com/sentinel-skipped-reply))

      (doseq [read-opts [com/read-opts-skip]]
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
            (read/read-reply com/read-opts-skip
              (xs->in+ "|1" "+k" "+v" "-MOVED 1 host:7000")))
        "Attributed Cluster redirections remain observable when skipping")
      (is (com/reply-error? {:eid :carmine.read/unexpected-read-error}
            (read/read-reply com/read-opts-skip
              (xs->in+ "|1" "+k" "+v" "$5" "ab")))
        "Attributed framing errors remain observable when skipping")])

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

(deftest _error-replies-bypass-parsers
  [(is (com/reply-error? {:eid :carmine.read/unexpected-read-error}
         (com/parse (fn [_] :masked) (rr (xs->in+ "$5" "ab"))))
     "Parsers must never mask connection-fatal framing errors")
   (is (com/reply-error? {:eid :carmine.read/redis-error-reply}
         (com/parse (fn [_] :masked) (rr (xs->in+ "-ERR bad")))))])

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
          "INNER parsers dominated")]))])

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
          (com/parse-aggregates xform (com/parsing-rf [] conj)
            (rr in))]
      (= [parsed (rr in)]
        [(into [] xform xs) "tail"]))))

(deftest _map-parser-early-termination-preserves-framing
  (doseq [wire
          [(com/xs->str+ "%3" "+k1" ":1" "+k2" ":2" "+k3" ":3" "+tail")
           (com/xs->str+ "%?" "+k1" ":1" "+k2" ":2" "+k3" ":3" "." "+tail")]]
    (let [in     (com/str->in wire)
          parsed
          (com/parse-aggregates (take 1) (com/parsing-rf [] conj)
            (rr in))]
      [(is (= (mapv vec parsed) [["k1" 1]]))
       (is (= (rr in) "tail"))])))

(defn- blob-in [streaming? ^bytes ba]
  (let [wire
        (if streaming?
          (render-streaming-blob ba)
          (com/xs->ba+ (str "$" (alength ba)) ba))]
    (if-let [buffer-size *resp-input-buffer-size*]
      (com/resp-input (java.io.ByteArrayInputStream. wire) buffer-size nil)
      (com/ba->in wire))))

(deftest _blob-markers
  (let [decode
        (fn [issue-83-workaround? streaming? s]
          (let [reply
                (read/read-reply
                  (com/get-read-opts
                    {:auto-thaw? true, :issue-83-workaround? issue-83-workaround?})
                  (blob-in streaming? (enc/str->utf8-ba s)))]
            (cond
              (enc/bytes?       reply) [:bin (enc/utf8-ba->str reply)]
              (com/reply-error? reply) [:npy (-> reply ex-data :bytes :content enc/utf8-ba->str)]
              :else                    reply)))]

    ;; Non-Nippy `:npy` payloads here fail to thaw, giving an error reply
    (doseq [streaming? [false true]
            i83?       [false true]]
      [(is (= (decode i83? streaming? "foo")          "foo"))
       (is (= (decode i83? streaming? "\u0000")       "\u0000"))
       (is (= (decode i83? streaming? "\u0000more")   "\u0000more"))
       (is (= (decode i83? streaming? "\u0000_")      nil))
       (is (= (decode i83? streaming? "\u0000_more")  nil))
       (is (= (decode i83? streaming? "\u0000>more")  [:npy "more"]))
       (is (= (decode i83? streaming? "\u0000<")      [:bin ""]))
       (is (= (decode i83? streaming? "\u0000<more")  [:bin "more"]))
       (is (= (decode i83? streaming? "\u0000<NPmore") [:bin "NPmore"]))
       (is (= (decode i83? streaming? "\u0000<NPY")   [:bin "NPY"]) "Too short for Nippy")
       (is (= (decode i83? streaming? "\u0000<NPYmore")
             (if i83? [:npy "NPYmore"] [:bin "NPYmore"])))
       (let [reply
             (read/read-reply
               (com/get-read-opts {:auto-thaw? true, :issue-83-workaround? i83?})
               (blob-in streaming? com/ba-npy))]
         [(is (com/reply-error? {:eid :carmine.read/nippy-thaw-error} reply))
          (is (= (-> reply ex-data :bytes) {:length 0, :content nil})
            "Empty Nippy payloads are consistent for all blob read paths")])])))

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
                "A thaw error retains the unmarked payload")])])))

    (testing "Automatic thawing with custom options"
      (let [pwd    [:salted "secret"]
            marked (com/xs->ba com/ba-npy (nippy/freeze :data {:password pwd}))
            opts   (com/get-read-opts
                     {:thaw-opts {:password pwd}
                      :auto-thaw? true, :issue-83-workaround? false})]
        (doseq [streaming? [false true]]
          [(is (= (decode opts streaming? marked) :data))
           (is (= (decode opts streaming? (enc/str->utf8-ba "text")) "text")
             "Unmarked blobs decode as strings")
           (is (= (decode opts streaming? (byte-array 0)) ""))])))))

(deftest _stored-blob-decoding
  ;; As used for swaps, which keep the stored bytes as a CAS token
  (let [opts  (fn [m] (com/get-read-opts (merge {:auto-thaw? true, :issue-83-workaround? false} m)))
        npy   (com/xs->ba com/ba-npy (nippy/freeze {:a 1}))
        bin   (com/xs->ba com/ba-bin (enc/str->utf8-ba "abc"))
        plain (enc/str->utf8-ba "abc")
        null+ (enc/str->utf8-ba "\u0000abc")]

    [(is (= (read/complete-stored-blob (opts {}) npy) {:a 1}))
     (is (= (read/complete-stored-blob (opts {}) plain) "abc"))
     (is (= (read/complete-stored-blob (opts {}) null+) "\u0000abc"))
     (is (nil? (read/complete-stored-blob (opts {}) com/ba-nil)))
     (is (enc/ba= (read/complete-stored-blob (opts {}) bin) plain))
     (is (enc/ba= (read/complete-stored-blob (opts {:read-mode :bytes}) npy)
           (nippy/freeze {:a 1})))
     (is (enc/ba= (read/complete-stored-blob (opts {:read-mode :bytes, :auto-thaw? false}) npy) npy))
     (is (identical? (read/complete-stored-blob com/read-opts-skip npy)
           com/sentinel-skipped-reply))

     (testing "Never returns the stored bytes themselves"
       (doseq [^bytes ba [npy bin plain null+ (byte-array 0)]
               m [{:read-mode :bytes} {:read-mode :bytes, :auto-thaw? false}]]
         (let [before (aclone ba)
               reply  (read/complete-stored-blob (opts m) ba)]
           [(is (enc/bytes? reply))
            (is (not (identical? reply ba)))
            (when (pos? (alength ^bytes reply)) (aset ^bytes reply 0 (byte 120)))
            (is (enc/ba= ba before))])))]))

(deftest _skip-mode-marked-blobs
  ;; Skipped blobs are discarded without marker decoding or Nippy thawing
  (let [marked-ba (com/xs->ba com/ba-npy (nippy/freeze {:a 1}))
        len       (alength ^bytes marked-ba)
        n-thaws_  (atom 0)]
    [(with-redefs [nippy/thaw (fn [& _] (swap! n-thaws_ inc) ::thawed)]
       (doseq [read-mode [:skip]]
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

;; Ambient Nippy thaw opts (`taoensso.nippy.tools/*thaw-opts*`) are captured
;; to `ReadOpts` independently of read mode, never consulted during reading
(deftest _thaw-opts-capture
  (let [pwd       [:cached "secret"]
        data      {:secret [1 2 3]}
        payload   (nippy/freeze data {:password pwd})
        marked    (com/xs->ba com/ba-npy payload)
        bulk      (fn [^bytes ba] [(str "$" (alength ba)) ba])
        mixed-in
        (fn []
          (apply xs->in+
            (concat
              ["*4"] (bulk marked) ["$5" "plain"]
              ["*2"] (bulk marked) ["$4" "text"]
              ["%1" "$1" "k"] (bulk marked))))
        captured  (core/with-thaw-opts {:password pwd} (com/get-read-opts))
        thaw-opts (fn [read-opts] (.-thaw-opts ^ReadOpts read-opts))
        thaw-err? (fn [x] (com/reply-error? {:eid :carmine.read/nippy-thaw-error} x))]

    [(testing "Ambient opts thaw marked payloads, leave ordinary strings"
       [(is (= (thaw-opts captured) {:password pwd}))
        (is (= (read/read-reply captured (mixed-in))
              [data "plain" [data "text"] {"k" data}])
          "Mixed aggregates, decoded after the binding exits")
        (is (= (thaw-opts (core/with-thaw-opts {:password pwd}
                            (com/get-read-opts {:auto-thaw? true})))
              {:password pwd})
          "The REPL/test arity also captures ambient opts")
        (is (thaw-err? (read/read-reply (com/get-read-opts {:auto-thaw? true})
                         (apply xs->in+ (bulk marked))))
          "Without opts, the encrypted payload can't be thawed")
        (is (identical? (com/get-read-opts) com/read-opts-default)
          "Default fast path when no opts are bound")])

     (testing "Decoding never rereads ambient opts"
       (is (thaw-err?
             (core/with-thaw-opts {:password [:cached "other"]}
               (read/read-reply
                 (com/get-read-opts {:auto-thaw? true, :thaw-opts nil})
                 (apply xs->in+ (bulk marked)))))))

     (testing "Read modes and internal copies keep captured opts"
       (core/with-thaw-opts {:password pwd}
         [(is (= (thaw-opts (core/as-bytes (com/get-read-opts))) {:password pwd}))
          (is (= (thaw-opts (com/with-push-fn captured identity)) {:password pwd}))
          (is (= (thaw-opts (com/in-aggregate-read-opts
                              (com/parse identity (com/get-read-opts))))
                {:password pwd}))
          (is (= (thaw-opts (com/parse identity (com/get-read-opts)))
                {:password pwd})
            "Parser path")
          (is (identical? (com/skip-replies (com/get-read-opts)) com/read-opts-skip)
            "Skip modes never thaw")
          (is (identical? (com/natural-replies (com/get-read-opts)) com/read-opts-natural)
            "Natural replies never thaw")]))

     (testing "Opts affect only actual thawing"
       (core/with-thaw-opts {:password pwd}
         [(is (enc/ba= (read/read-reply (core/as-bytes (com/get-read-opts))
                         (apply xs->in+ (bulk marked)))
                payload)
            "Bytes mode removes markers without thawing")
          (is (enc/ba= (binding [core/*auto-thaw?* false]
                         (read/read-reply (core/as-bytes (com/get-read-opts))
                           (apply xs->in+ (bulk marked))))
                marked)
            "Auto-thaw false keeps exact bytes")
          (is (= (binding [core/*auto-thaw?* false]
                   (read/read-reply (com/get-read-opts) (xs->in+ "$5" "plain")))
                "plain"))]))

     (testing "`thaw` mode: forced thaw, explicit opts merge over ambient"
       (let [unmarked-in #(apply xs->in+ (bulk payload))]
         [(is (= (read/read-reply
                   (core/with-thaw-opts {:password pwd}
                     (core/thaw nil (com/get-read-opts)))
                   (unmarked-in))
                data)
            "Nil explicit opts use ambient opts")
          (is (= (thaw-opts
                   (core/with-thaw-opts {:password pwd, :v1-compatibility? true}
                     (core/thaw {:v1-compatibility? false} (com/get-read-opts))))
                {:password pwd, :v1-compatibility? false})
            "Explicit keys win, other ambient keys remain")
          (is (= (read/read-reply
                   (core/with-thaw-opts {:password pwd}
                     (core/thaw {:v1-compatibility? false} (com/get-read-opts)))
                   (unmarked-in))
                data)
            "Explicit opts preserve the ambient password")
          (is (thaw-err?
                (read/read-reply
                  (core/with-thaw-opts {:password pwd}
                    (core/thaw {:password [:cached "other"]} (com/get-read-opts)))
                  (unmarked-in)))
            "Explicit password overrides ambient")
          (is (thaw-err?
                (read/read-reply (core/thaw nil (com/get-read-opts)) (xs->in+ "$5" "plain")))
            "Forced thaw of ordinary strings still fails")]))

     (testing "Thaw error data redacts passwords"
       (let [err (read/read-reply
                   (core/with-thaw-opts {:password [:cached "wrong"]} (com/get-read-opts))
                   (apply xs->in+ (bulk marked)))]
         [(is (thaw-err? err))
          (is (= (-> err ex-data :thaw-opts) {:password :redacted}))
          (is (not (re-find #"wrong" (pr-str (ex-data err)))))]))]))

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

   (testing "Keyword args"
     [(is (= (mapv #'write/kw->str
               [:k :ns/k :a.b/c-d (keyword "a/b" "c") (keyword "" "k") (keyword nil "a/b") (keyword "ಬಾ" "ಇ")])
            ["k" "ns/k" "a.b/c-d" "a/b/c" "/k" "a/b" "ಬಾ/ಇ"]))
      (is (= (com/with-out->str (write/write-bulk-arg :k    out)) "$1\r\nk\r\n"))
      (is (= (com/with-out->str (write/write-bulk-arg :ns/k out)) "$4\r\nns/k\r\n"))
      (is (= (vec (write/arg-payload-bytes :ns/k)) (vec (enc/str->utf8-ba "ns/k"))) "Routing and write paths agree")])

   (testing "Bulk num/str equivalence"
     [(is (=
            (com/with-out->str (#'write/write-bulk-double out  12.5))
            (com/with-out->str (#'write/write-bulk-str    out "12.5"))))
      (is (=
            (com/with-out->str (#'write/write-bulk-double out      (double an-uncached-num)))
            (com/with-out->str (#'write/write-bulk-str    out (str (double an-uncached-num))))))])])

(deftest _wrappers
  [(is (= (enc/utf8-ba->str (.-ba (write/bytes (write/bytes (com/xs->ba [\a \b \c]))))) "abc"))

   (testing "Outer freeze composes with bytes wrappers"
     (let [source (byte-array [0 60 78 80 89 0 13 -1 127 -128])
           expected (aclone source)
           freeze-opts {:compressor nil}
           frozen-payload
           (fn [wrapped]
             (binding [core/*auto-freeze?* false]
               (write/arg-payload-bytes (write/prepare-arg wrapped))))
           direct  (write/freeze source               freeze-opts)
           wrapped (write/freeze (write/bytes source) freeze-opts)]
       [(is (enc/ba= (frozen-payload wrapped) (frozen-payload direct))
          "freeze(bytes ba) is wire-equivalent to freeze(ba)")
        (is (enc/ba= (frozen-payload wrapped)
              (nippy/freeze expected freeze-opts))
          "The explicit options govern byte-array serialization")
        (is (identical? wrapped (write/freeze wrapped freeze-opts))
          "Repeated freeze with equal options retains the eager wrapper")
        (aset-byte source 0 (byte 42))
        (is (enc/ba= (nippy/thaw (frozen-payload wrapped) freeze-opts) expected)
          "The freeze eagerly snapshots mutable bytes")]))])

(deftest _freeze-options
  (let [pwd     [:cached "secret"]
        opts-of (fn [x] (.-freeze-opts ^WriteFrozen x))
        payload (fn [x] (.-?frozen-ba  ^WriteFrozen x))]

    [(testing "Signature: (freeze value), (freeze value opts)"
       [(is (= (opts-of (write/freeze "x")) nil))
        (is (= (opts-of (write/freeze "x" {:a :A})) {:a :A}))
        (is (= (opts-of (write/freeze {:a :A})) nil)
          "A single map argument is a value, never options")
        (is (= (opts-of (write/freeze "x" nil)) nil) "Nil opts add no overrides")
        (is (throws? (write/freeze "x" :dynamic)) "Opts must be nil or a map")
        (is (throws? clojure.lang.ArityException (write/freeze "x" {} "y"))
          "No variadic form")])

     (testing "Ambient opts are captured eagerly at wrap time"
       (let [w (core/with-freeze-opts {:password pwd} (write/freeze :value))]
         [(is (= (opts-of w) {:password pwd}))
          (is (= (nippy/thaw (payload w) {:password pwd}) :value)
            "Frozen at wrap time, after the binding exits")
          (is (throws? (nippy/thaw (payload w))) "Payload really is encrypted")]))

     (testing "Explicit opts merge over ambient opts"
       [(is (= (core/with-freeze-opts {:password pwd, :a :ambient}
                 (opts-of (write/freeze "x" {:a :explicit, :b :B})))
              {:password pwd, :a :explicit, :b :B}))
        (is (= (core/with-freeze-opts {:password pwd}
                 (opts-of (write/freeze "x" nil)))
              {:password pwd}))])

     (testing "Rewrapping"
       (let [w1 (write/freeze :value {:a :A})]
         [(is (identical? w1 (write/freeze w1)) "Single-arg rewrap is idempotent")
          (is (identical? w1 (core/with-freeze-opts {:password pwd} (write/freeze w1)))
            "Single-arg rewrap ignores ambient opts")
          (is (identical? w1 (write/freeze w1 {:a :A})) "Equal effective opts")
          (is (identical? w1 (write/freeze w1 nil)))
          (let [w2 (core/with-freeze-opts {:c :ambient}
                     (write/freeze w1 {:password pwd}))]
            [(is (= (opts-of w2) {:a :A, :password pwd})
               "Explicit rewrap keeps captured opts, applies overrides, ignores ambient")
             (is (= (nippy/thaw (payload w2) {:password pwd}) :value)
               "Explicit rewrap refreezes eagerly")])]))

     (testing "Carmine v3 (Nippy tools) wrappers"
       (let [v3w (nippy-tools/with-freeze-opts {:password pwd}
                   (nippy-tools/wrap-for-freezing :value {:b :B}))
             w   (write/freeze v3w)]
         [(is (= (opts-of w) {:password pwd, :b :B}) "Uses v3 captured opts")
          (is (= (.-unfrozen-val ^WriteFrozen w) :value))
          (is (= (opts-of (write/freeze v3w {:b :explicit})) {:password pwd, :b :explicit}))
          (let [^bytes wire (binding [core/*auto-freeze?* true]
                              (write/arg-payload-bytes (write/prepare-arg v3w)))]
            [(is (enc/ba= (java.util.Arrays/copyOfRange wire 0 2) com/ba-npy))
             (is (= (nippy/thaw (java.util.Arrays/copyOfRange wire 2 (alength wire))
                      {:password pwd})
                   :value)
               "Unwrapped v3 wrapper args are written as marked frozen values")])
          (let [wire (binding [core/*auto-freeze?* true]
                       (com/with-out (write/write-command out (write/prepare-command ["ECHO" v3w]))))
                [_ reply] (read/read-reply
                            (core/with-thaw-opts {:password pwd}
                              (com/get-read-opts {:auto-thaw? true}))
                            (com/ba->in wire))]
            (is (= reply :value) "Direct writes use the v3 captured opts"))]))

     (testing "Lazy v3 wrappers retain ambient supplementation at first freeze"
       (let [v3w (nippy-tools/wrap-for-freezing :value {:compressor nil})
             ambient {:password pwd, :compressor nippy/lz4-compressor}
             explicit {:password [:cached "override"]}
             w (core/with-freeze-opts ambient (write/freeze v3w))
             w-explicit (core/with-freeze-opts ambient (write/freeze v3w explicit))
             ^bytes wire (binding [core/*auto-freeze?* true]
                           (core/with-freeze-opts ambient
                             (write/arg-payload-bytes (write/prepare-arg v3w))))]
         [(is (= (opts-of w) {:password pwd, :compressor nil})
            "Captured legacy options override ambient options")
          (is (= (nippy/thaw (payload w) {:password pwd}) :value))
          (is (throws? (nippy/thaw (payload w))) "Later ambient password is honored")
          (is (= (opts-of w-explicit) (assoc explicit :compressor nil))
            "Explicit overrides win over legacy and ambient options")
          (is (= (nippy/thaw (payload w-explicit) explicit) :value))
          (is (= (nippy/thaw (java.util.Arrays/copyOfRange wire 2 (alength wire))
                   {:password pwd})
                 :value)
            "Passing a lazy v3 wrapper directly to a command also honors ambient opts")]))

     (testing "Auto-frozen args capture ambient opts at preparation"
       (let [prepared (binding [core/*auto-freeze?* true]
                        (core/with-freeze-opts {:password pwd}
                          (write/prepare-arg {:a 1})))
             ^bytes ba (write/arg-payload-bytes prepared)]
         (is (= (nippy/thaw (java.util.Arrays/copyOfRange ba 2 (alength ba))
                  {:password pwd})
               {:a 1}))))]))

(deftest _prepared-arguments
  (let [command ["SET" "key" 1]]
    (is (identical? command (write/prepare-command command))
      "Already-safe command vectors are retained without copying"))

  (let [command  ["MSET" "k1" (write/bytes (enc/str->utf8-ba "v1")) "k2" "v2" 3 {:a 1}]
        prepared (write/prepare-command command)]
    [(is (= (mapv identical? command prepared)
           [true true false true true true false])
       "Only args that need it are replaced")
     (is (= (count prepared) (count command)))])

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
        (binding [core/*auto-freeze?* true, nippy-tools/*freeze-opts* freeze-opts]
          [(write/arg-payload-bytes (write/prepare-arg value))
           (com/with-out (write/write-command out (write/prepare-command ["ECHO" value])))])
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
     (is (false? (marked? ["SETBIT" "k" 0 1]))
       "Prefilter collision (SETBIT vs SELECT) resolves via the definitive path")
     (is (false? (marked? ["WAIT" 0 0])) "Unsupported by Cluster, but not stateful")
     (is (false? (marked? ["HINCRBY" "k" "f" 1])))
     (is (false? (marked? ["CLIENT" "LIST"])))
     (is (false? (marked? [:get "k"])) "Uncommon first-arg types still work")
     (is (true?  (marked? ["MULTI"])))
     (is (true?  (marked? ["multi"])) "Case-insensitive")
     (is (true?  (marked? ["SUBSCRIBE" "chan"])))
     (is (true?  (marked? ["watch" "k"])))
     (is (true?  (marked? ["CLIENT" "SETNAME" "x"])))
     (is (true?  (marked? [:multi])) "Uncommon first-arg types still work")
     (is (true?  (marked? ["himport" "prepare" "fs" "f"])))
     (is (= (mapv #(write/cluster-incompatible-command? (write/prepare-command %))
              [["wait" 0 0] ["WAITAOF" 1 0 0] [:HIMPORT "SET" "k" "fs" "v"] ["MULTI"]
               ["SCRIPT" "DEBUG" "NO"] ["HINCRBY" "k" "f" 1] ["GET" "k"]])
           [true true true true false false false]))]))

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
