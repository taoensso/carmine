(ns taoensso.carmine.impl.resp.read
  "Read part of RESP3 implementation."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string  :as str]
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]
   [taoensso.nippy  :as nippy]
   [taoensso.carmine.impl.resp.common :as com
    :refer [str->bytes bytes->str xs->in+]]

   [taoensso.carmine.impl.resp.read.blobs :as blobs])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInputStream]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.read)
  (test/run-tests 'taoensso.carmine.impl.resp.read))

;;;; Blobs

(enc/defalias blobs/read-blob)

;;;; Aggregates

(defn- read-basic-reply
  "Basic version of `read-reply`, useful for testing"
  [^DataInputStream in]
  (let [kind-b (.readByte in)]
    (enc/case-eval kind-b
      (byte \+)                 (.readLine in)  ; Simple string
      (byte \:) (Long/parseLong (.readLine in)) ; Int64
      (byte \.) (do (com/discard-crlf in) ::end-of-aggregate-stream)
      )))

(defn- read-aggregate-into
  [to read-reply ^DataInputStream in]
  (let [size-str (.readLine in)]
    (if-let [stream? (= size-str "?")]

      (loop [acc (transient (empty to))]
        (let [item (read-reply in)]
          (if (identical? item ::end-of-aggregate-stream)
            (persistent!  acc)
            (recur (conj! acc item)))))

      (let [n (Integer/parseInt size-str)]
        (if (<= n 0)
          (if (zero? n) to nil) ; -1 for RESP2 support
          (enc/repeatedly-into to n #(read-reply in)))))))

(deftest ^:private _read-aggregate-into
  [(is (= (read-aggregate-into [] nil (xs->in+  0))) "Empty")
   (is (= (read-aggregate-into [] nil (xs->in+ -1))) "RESP2 null")

   (is (= (read-aggregate-into [] read-basic-reply (xs->in+ 2   ":1" ":2"))     [1 2]))
   (is (= (read-aggregate-into [] read-basic-reply (xs->in+ "?" ":1" ":2" ".")) [1 2]) "Streaming")])

(let [strings->keywords (fn [x] (if (string? x) (keyword x) x))
      get-fn
      (fn [?opt]
        (if (identical? ?opt :strings->keywords)
          strings->keywords
          (if (nil? ?opt)
            identity
            ?opt)))

      default-pre  {:key-fn :strings->keywords :val-fn nil}
      default-post {:key-fn  strings->keywords :val-fn identity}]

  (def ^:dynamic *kv-fns*
    ;; TODO Document
    default-pre)

  (defn- get-kv-fns [kv-fns]
    (if (identical? kv-fns default-pre)
      default-post ; Optimized common case
      (let [{:keys [key-fn val-fn]} kv-fns]
        {:key-fn (get-fn key-fn)
         :val-fn (get-fn val-fn)}))))

(comment (enc/qb 1e6 (get-kv-fns *kv-fns*)))

(defn- read-aggregate-into-map
  [read-reply ^DataInputStream in]
  (let [size-str (.readLine in)]
    (if-let [stream? (= size-str "?")]

      (let [{:keys [key-fn val-fn]} (get-kv-fns *kv-fns*)]
        (loop [acc (transient {})]
          (let [item (read-reply in)]
            (if (identical? item ::end-of-aggregate-stream)
              (persistent! acc)
              (let [k item
                    v (read-reply in)]
                (recur (assoc! acc (key-fn k) (val-fn v))))))))

      (let [n (Integer/parseInt size-str)]
        (if (<= n 0)
          (if (zero? n) {} nil) ; -1 for RESP2 support
          (let [{:keys [key-fn val-fn]} (get-kv-fns *kv-fns*)]
            (if (> n 10)
              (persistent! (enc/reduce-n (fn [m _] (let [k (read-reply in), v (read-reply in)] (assoc! m (key-fn k) (val-fn v)))) (transient {}) n))
              (do          (enc/reduce-n (fn [m _] (let [k (read-reply in), v (read-reply in)] (assoc  m (key-fn k) (val-fn v))))            {}  n)))))))))

(deftest ^:private _read-aggregate-into-map
  [(testing "Basics"
     [(is (= (read-aggregate-into-map nil (xs->in+  0))  {}) "Empty")
      (is (= (read-aggregate-into-map nil (xs->in+ -1)) nil) "RESP2 null")

      (is (= (read-aggregate-into-map read-basic-reply (xs->in+ 2   "+k1" "+v1" "+k2" "+v2"))     {:k1 "v1" :k2 "v2"}))
      (is (= (read-aggregate-into-map read-basic-reply (xs->in+ "?" "+k1" "+v1" "+k2" "+v2" ".")) {:k1 "v1" :k2 "v2"}) "Streaming")
      (is (= (read-aggregate-into-map read-basic-reply (xs->in+ "?" ":1"  "+v1" "+k2" "+v2" ".")) {1   "v1" :k2 "v2"}) "Streaming")])

   (testing "*kv-fns*"
     (binding [*kv-fns* nil]
       [(is (= (read-aggregate-into-map read-basic-reply (xs->in+ 2   "+k1" "+v1" "+k2" "+v2"))     {"k1" "v1" "k2" "v2"}))
        (is (= (read-aggregate-into-map read-basic-reply (xs->in+ "?" "+k1" "+v1" "+k2" "+v2" ".")) {"k1" "v1" "k2" "v2"}))])

     (binding [*kv-fns* {:val-fn (fn [x] (if (string? x) (str/upper-case x) x))}]
       [(is (= (read-aggregate-into-map read-basic-reply (xs->in+ 2   "+k1" "+v1" "+k2" ":2"))     {"k1" "V1" "k2" 2}))
        (is (= (read-aggregate-into-map read-basic-reply (xs->in+ "?" "+k1" "+v1" "+k2" ":2" ".")) {"k1" "V1" "k2" 2}))]))])

(defn- redis-reply-error [?message]
  (let [^String message (if (nil? ?message) "" ?message)
        code (re-find #"^\S+" message)] ; "ERR", "WRONGTYPE", etc.

    (com/carmine-reply-error
      (ex-info "[Carmine] Redis replied with an error"
        {:eid :carmine.resp.read/error-reply
         :message message
         :code    code}))))

(comment (redis-reply-error "ERR Foo bar"))

(defmulti  push-handler (fn [state [data-type :as data-vec]] data-type))
(defmethod push-handler :default [state data-vec] #_(println data-vec) nil)

(enc/defonce push-agent_
  (delay (agent nil :error-mode :continue)))

(def ^:dynamic *push-fn*
  "?(fn [data-vec]) -> ?effects.
  If provided (non-nil), this fn should never throw."
  ;; TODO Proper docstring for this & push-handler, etc.
  (fn [data-vec]
    (send-off @push-agent_
      (fn [state]
        (try
          (push-handler state data-vec)
          (catch Throwable t
            ;; TODO Try publish error message?
            ))))))

(comment
  (read-reply
    (xs->in+
      ">4" "+pubsub" "+message" "+channel" "+message content"
      "$9" "get reply")))

;;;;

(defn read-reply
  "Blocks to read reply from given DataInputStream."
  [^DataInputStream in]
  (let [kind-b (.readByte in)]
    (try
      (enc/case-eval kind-b
        ;; RESP3 ⊃ RESP2

        ;; --- RESP2 ---------------------------------------------------------------
        (byte \+)                    (.readLine in)  ; Simple string ✓
        (byte \:) (Long/parseLong    (.readLine in)) ; Int64 ✓
        (byte \-) (redis-reply-error (.readLine in)) ; Simple error ✓
        (byte \$) (read-blob true               in)  ; Blob string/other ✓

        (byte \*) ; Aggregate array ✓
        (blobs/as-default
          (read-aggregate-into []
            read-reply in))

        ;; --- RESP3 ---------------------------------------------------------------
        (byte \.) (do (com/discard-crlf in) ::end-of-aggregate-stream) ; End of aggregate stream ✓
        (byte \_) ; Null ✓
        (do
          (com/discard-crlf in)
          ;; We apply *bxf* to both user-blobs and nulls
          (blobs/complete-blob blobs/*bxf* nil true))

        (byte \#) ; Bool ✓
        (let [b (.readByte in)]
          (com/discard-crlf in)
          (== b #=(byte \t)))

        (byte \!) (redis-reply-error (read-blob false in)) ; Blob error ✓

        (byte \=) ; Verbatim string ; ✓
        (let [^String s (read-blob false in)
              format  (subs s 0 3) ; "txt", "mkd", etc.
              payload (subs s 4)]
          [:carmine/verbatim-string format payload]) ; TODO API okay?

        (byte \,) ; Double ✓
        (let [s (.readLine in)]
          (enc/cond
            (= s  "inf") Double/POSITIVE_INFINITY
            (= s "-inf") Double/NEGATIVE_INFINITY
            :else       (Double/parseDouble s)))

        (byte \() ; Big integer ✓
        (let [s (.readLine in)]
          (bigint (BigInteger. s)))

        (byte \~) (read-aggregate-into #{} read-reply in) ; Aggregate set ✓
        (byte \%) (read-aggregate-into-map read-reply in) ; Aggregate map ✓

        (byte \|) ; Attribute map ✓
        (blobs/as-default
          (let [attrs (read-aggregate-into-map read-reply in)
                target (read-reply in)]

            ;; TODO API okay?
            (if (instance? clojure.lang.IObj target)
              (with-meta target {:carmine/attributes attrs})
              [:carmine/with-attributes target attrs]
              #_
              (throw
                (ex-info "[Carmine] Attributes reply for unexpected (non-IObj) type"
                  {:eid :carmine.resp.read/attributes-for-unexpected-type
                   :target {:type (type target) :value target}
                   :attributes attrs})))))

        (byte \>) ; Push ✓
        (blobs/as-default
          (let [v (read-aggregate-into [] read-reply in)]
            (when-let [push-fn *push-fn*]
              (try ; Silently swallow errors (fn should have own error handling)
                (push-fn v)
                (catch Throwable _)))

            ;; Continue to actual reply
            (read-reply in)))

        (throw
          (ex-info "[Carmine] Unexpected reply kind"
            {:eid :carmine.resp.read/unexpected-reply-kind
             :kind
             (enc/assoc-when
               {:as-byte kind-b :as-char (byte kind-b)}
               :end-of-stream? (== kind-b -1))})))

      (catch Throwable t
        (throw
          (ex-info "[Carmine] Unexpected reply error"
            {:eid :carmine.resp.read/reply-error
             :kind {:as-byte kind-b :as-char (char kind-b)}}
            t))))))

(deftest ^:private _read-reply
  [(testing "Basics"
     [(is (= (read-reply
               (xs->in+ "*10" "+simple string"
                 ":1" ",1" ",1.5" ",inf" ",-inf" "(1" "#t" "#f" "_"))
            ["simple string" 1 1.0 1.5 ##Inf ##-Inf 1N true false nil]))

      (is (= (read-reply (xs->in+ "$7" "hello\r\n")) "hello\r\n") "Binary safe")
      (is (= (read-reply (xs->in+ "$?" ";5" "hello" ";9" " world!\r\n" ";0")) "hello world!\r\n") "Streaming")
      (is (= (read-reply (xs->in+ "*3" ":1" "$?" ";4" "bulk" ";6" "string" ";0" ",1.5"))
            [1 "bulkstring" 1.5]))])

   (testing "Errors"
     [(testing "Simple errors"
        [(let [r1 (read-reply (xs->in+ "-ERR Foo bar baz"))]
           (is (com/crex-match? r1
                 {:eid :carmine.resp.read/error-reply
                  :message "ERR Foo bar baz"
                  :code    "ERR"})))

         (let [[r1 r2 r3 r4] (read-reply (xs->in+ "*4" ":1" "-CODE1 a" ":2" "-CODE2 b"))]
           [(is (= r1 1))
            (is (= r3 2))
            (com/crex-match? r2 {:eid :carmine.resp.read/error-reply :code "CODE1" :message "CODE1 a"})
            (com/crex-match? r4 {:eid :carmine.resp.read/error-reply :code "CODE2" :message "CODE2 b"})])])

      (testing "Bulk errors"
        [(let [r1 (read-reply (xs->in+ "!10" "CODE Foo\r\n"))]
           (is (com/crex-match? r1
                 {:eid :carmine.resp.read/error-reply
                  :message "CODE Foo\r\n"
                  :code    "CODE"})
             "Binary safe"))

         (let [[r1 r2 r3 r4] (read-reply (xs->in+ "*4" ":1" "!9" "CODE1 a\r\n" ":2" "!9" "CODE2 b\r\n"))]
           [(is (= r1 1))
            (is (= r3 2))
            (com/crex-match? r2 {:eid :carmine.resp.read/error-reply :code "CODE1" :message "CODE1 a\r\n"})
            (com/crex-match? r4 {:eid :carmine.resp.read/error-reply :code "CODE2" :message "CODE2 b\r\n"})])])])

   (testing "Nested aggregates"
     [(is (= [[1 "2" 3] ["a" "b"] []]
             (read-reply (xs->in+
                          "*3"
                          "*3" ":1" "+2" ":3"
                          "*2" "+a" "+b"
                          "*0"))))

      (is (= [#{1 3 "2"} {:k1 "v1", 2 "v2"} [["a" "b"] [] #{} {}]]
             (read-reply
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

            (read-reply
              (xs->in+
                "%3"
                "*3" ":1" "+2" ":3"            ; Array key
                "~?" ":1" "+2" ":3" "."        ; Set val
                "%1" "+k1" "+v1"               ; Map key
                "%?" "+k1" "+v1" ":2" ":2" "." ; Map val
                "~2" "+a" "+b"                 ; Set key
                "~?" ":1" ":2" "."             ; Set val
                ))))

      ])

   (testing "Misc types"
     [(is (= (read-reply (xs->in+ "=11" "txt:hello\r\n")) [:carmine/verbatim-string "txt" "hello\r\n"])
        "Verbatim string")

      (is (enc/submap?
            {:carmine/attributes {:key-popularity {:a 0.1923 :b 0.0012}}}
            (meta
              (read-reply (xs->in+ "|1" "+key-popularity"
                           "%2" "$1" "a" ",0.1923" "$1" "b" ",0.0012"
                           "*2" ":2039123" ":9543892"))))
        "Attributes")])

   (testing "Pushes"
     ;; Push replies can be received at any time, but only at the top level
     ;; (e.g. not within the middle of a map reply)
     [(let [p_ (promise)
            pf (fn [dv] (deliver p_ dv))

            reply
            (binding [*push-fn* pf]
              (read-reply
                (xs->in+
                  ">4" "+pubsub" "+message" "+channel" "+message content"
                  "$9" "get reply")))]

        [(is (= reply "get reply"))
         (is (= (deref p_ 0 nil) ["pubsub" "message" "channel" "message content"]))])])])
