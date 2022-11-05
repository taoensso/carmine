(ns taoensso.carmine.impl.resp.read
  "Read-side implementation of the Redis RESP3 protocol,
  Ref. https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]
   [taoensso.nippy  :as nippy]
   [taoensso.carmine.impl.resp.common :as resp-com
    :refer [str->bytes bytes->str xs->in+]]

   [taoensso.carmine.impl.resp.read.common  :as read-com]
   [taoensso.carmine.impl.resp.read.blobs   :as blobs]
   [taoensso.carmine.impl.resp.read.parsing :as parsing])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInputStream]
   [taoensso.carmine.impl.resp.read.common ReadOpts Request]
   [taoensso.carmine.impl.resp.read.parsing Parser]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.read)
  (test/run-tests 'taoensso.carmine.impl.resp.read))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*)

(alias 'core 'taoensso.carmine-v4)

;;;; Aggregates

(defn- read-basic-reply
  "Basic version of `read-reply`, useful for testing"
  [_read-opts ^DataInputStream in]
  (let [kind-b (.readByte in)]
    (enc/case-eval kind-b
      (byte \+)                 (.readLine in)  ; Simple string
      (byte \:) (Long/parseLong (.readLine in)) ; Simple long
      (byte \.) (do (resp-com/discard-crlf in) ::end-of-aggregate-stream)
      )))

(defn- read-aggregate-by-ones
  [to ^ReadOpts read-opts read-reply ^DataInputStream in]
  (let [size-str (.readLine in)
        inner-read-opts (read-com/inner-read-opts read-opts)
        skip? (identical? (.-read-mode read-opts) :skip)]

    (if-let [stream? (= size-str "?")]

      ;; Streaming
      (enc/cond

        skip?
        (loop []
          (let [x (read-reply inner-read-opts in)]
            (if (identical? x ::end-of-aggregate-stream)
              :carmine/_skipped
              (recur))))

        ;; Reducing parser
        :if-let [^Parser p (parsing/when-rf-parser (.-parser read-opts))]
        (let [rf ((.rfc p))
              init-acc (rf)]
          (loop [acc init-acc]
            (let [x (read-reply inner-read-opts in)]
              (if (identical? x ::end-of-aggregate-stream)
                (do    (rf acc)) ; Complete acc
                (recur (rf acc x))))))

        :default
        (loop [acc (transient (empty to))]
          (let [x (read-reply inner-read-opts in)]
            (if (identical? x ::end-of-aggregate-stream)
              (persistent!  acc)
              (recur (conj! acc x))))))

      ;; Not streaming
      (let [n (Integer/parseInt size-str)]
        (if (<= n 0) ; Empty or RESP2 null
          (if (== n 0) to nil)

          (enc/cond

            skip?
            (enc/reduce-n (fn [_ _] (read-reply inner-read-opts in))
              0 n)

            ;; Reducing parser
            :if-let [^Parser p (parsing/when-rf-parser (.-parser read-opts))]
            (let [rf ((.-rfc p))
                  init-acc (rf)]
              (rf ; Complete acc
                (enc/reduce-n
                  (fn [acc _n]
                    (rf acc (read-reply inner-read-opts in)))
                  init-acc
                  n)))

            :default
            (enc/repeatedly-into to n
              #(read-reply inner-read-opts in))))))))

(deftest ^:private _read-aggregate-by-ones-bootstrap
  ;; Very basic bootstrap tests using only `read-basic-reply`
  [(is (= (read-aggregate-by-ones [] read-com/default-read-opts nil (xs->in+  0))  []) "Empty blob")
   (is (= (read-aggregate-by-ones [] read-com/default-read-opts nil (xs->in+ -1)) nil) "RESP2 null")

   (is (= (read-aggregate-by-ones [] read-com/default-read-opts read-basic-reply (xs->in+ 2   ":1" ":2"))     [1 2]))
   (is (= (read-aggregate-by-ones [] read-com/default-read-opts read-basic-reply (xs->in+ "?" ":1" ":2" ".")) [1 2]) "Streaming")])

(let [keywordize (fn [x] (if (string? x) (keyword x) x))]
  (defn- read-aggregate-by-pairs
    "Like `read-aggregate-by-ones` but optimized for read-pair
    cases (notably maps)."
    [^ReadOpts read-opts read-reply ^DataInputStream in]
    (let [size-str (.readLine in)
          inner-read-opts (read-com/inner-read-opts read-opts)
          skip? (identical? (.-read-mode read-opts) :skip)]

      (if-let [stream? (= size-str "?")]

        ;; Streaming
        (enc/cond

          skip?
          (loop []
            (let [x (read-reply inner-read-opts in)]
              (if (identical? x ::end-of-aggregate-stream)
                :carmine/_skipped
                (let [_k x
                      _v (read-reply inner-read-opts in)]
                  (recur)))))

          ;; Reducing parser
          :if-let [^Parser p (parsing/when-rf-parser (.-parser read-opts))]
          (let [rf    ((.-rfc    p))
                kv-rf? (.-kv-rf? p)
                init-acc (rf)]

            (loop [acc init-acc]
              (let [x (read-reply inner-read-opts in)]
                (if (identical? x ::end-of-aggregate-stream)
                  (rf acc) ; Complete acc
                  (let [k x ; Without kfn!
                        v (read-reply inner-read-opts in)]
                    (recur
                      (if kv-rf?
                        (rf acc  k v)
                        (rf acc [k v]))))))))

          :let [kfn (if (.-keywordize-maps? read-opts) keywordize identity)]
          :default
          (loop [acc (transient {})]
            (let [x (read-reply inner-read-opts in)]
              (if (identical? x ::end-of-aggregate-stream)
                (persistent! acc)
                (let [k (kfn x)
                      v (read-reply inner-read-opts in)]
                  (recur (assoc! acc k v)))))))

        ;; Not streaming
        (let [n (Integer/parseInt size-str)]
          (if (<= n 0) ; Empty or RESP2 null
            (if (== n 0) {} nil)

            (enc/cond

              skip?
              (enc/reduce-n
                (fn [_ _]
                  (let [_k (read-reply inner-read-opts in)
                        _v (read-reply inner-read-opts in)]
                    nil))
                0 n)

              ;; Reducing parser
              :if-let [^Parser p (parsing/when-rf-parser (.-parser read-opts))]
              (let [rf    ((.-rfc    p))
                    kv-rf? (.-kv-rf? p)
                    init-acc (rf)]
                (rf ; Complete
                  (enc/reduce-n
                    (fn [acc _n]
                      (let [k (read-reply inner-read-opts in) ; Without kfn!
                            v (read-reply inner-read-opts in)]
                        (if kv-rf?
                          (rf acc  k v)
                          (rf acc [k v]))))
                    init-acc
                    n)))

              :let [kfn (if (.-keywordize-maps? read-opts) keywordize identity)]
              :default
              (if (> n 10)
                (persistent!
                  (enc/reduce-n
                    (fn [m _]
                      (let [k (kfn (read-reply inner-read-opts in))
                            v      (read-reply inner-read-opts in)]
                        (assoc! m k v)))
                    (transient {})
                    n))

                (enc/reduce-n
                  (fn [m _]
                    (let [k (kfn (read-reply inner-read-opts in))
                          v      (read-reply inner-read-opts in)]
                      (assoc m k v)))
                  {}
                  n)))))))))

(deftest ^:private _read-aggregate-by-pairs-bootstrap
  ;; Very basic bootstrap tests using only `read-basic-reply`
  [(testing "Basics"
     [(is (= (read-aggregate-by-pairs read-com/default-read-opts nil (xs->in+  0))  {}) "Empty blob")
      (is (= (read-aggregate-by-pairs read-com/default-read-opts nil (xs->in+ -1)) nil) "RESP2 null")

      (is (= (read-aggregate-by-pairs read-com/default-read-opts read-basic-reply (xs->in+ 2 "+k1" "+v1" "+k2" "+v2")) {:k1  "v1" :k2 "v2"}) "With keywordize")
      (is (= (read-aggregate-by-pairs read-com/nil-read-opts     read-basic-reply (xs->in+ 2 "+k1" "+v1"  ":2" "+v2")) {"k1" "v1",  2 "v2"}) "W/o  keywordize")

      (is (= (read-aggregate-by-pairs read-com/default-read-opts read-basic-reply (xs->in+ "?" "+k1" "+v1" ":2" "+v2" ".")) {:k1  "v1"  2 "v2"}) "Streaming, with keywordize")
      (is (= (read-aggregate-by-pairs read-com/nil-read-opts     read-basic-reply (xs->in+ "?" "+k1" "+v1" ":2" "+v2" ".")) {"k1" "v1", 2 "v2"}) "Streaming, w/o  keywordize")])])

(defn- redis-reply-error [?message]
  (let [^String message (if (nil? ?message) "" ?message)
        code (re-find #"^\S+" message)] ; "ERR", "WRONGTYPE", etc.

    (resp-com/reply-error
      (ex-info "[Carmine] Redis replied with an error"
        {:eid :carmine.resp.read/error-reply
         :message message
         :code    code}))))

(comment (redis-reply-error "ERR Foo bar"))

(defmulti  push-handler (fn [state [data-type :as data-vec]] data-type))
(defmethod push-handler :default [state data-vec] #_(println data-vec) nil)

(enc/defonce push-agent_
  (delay (agent nil :error-mode :continue)))

(def ^:dynamic *push-fn* ; TODO move to core
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

  ;; For REPL/testing
  ([in] (read-reply (read-com/new-read-opts) in))

  ([^ReadOpts read-opts ^DataInputStream in]
   ;; Since dynamic vars are ephemeral and reply reading is lazy, neither this
   ;; fn nor any of its children should access dynamic vars. Instead, we'll capture
   ;; dynamic config to `com/ReadOpts` at the appropriate time.
   (let [kind-b (.readByte in)
         skip?  (identical? (.-read-mode read-opts) :skip)

         reply
         (try
           (enc/case-eval kind-b
             ;; --- RESP2 ⊂ RESP3 -------------------------------------------------------
             (byte \+) (.readLine in) ; Simple string ✓
             (byte \:) ; Simple long ✓
             (let [s (.readLine in)]
               (when-not skip?
                 (Long/parseLong s)))

             (byte \-) ; Simple error ✓
             (let [s (.readLine in)]
               (when-not skip?
                 (redis-reply-error s)))

             (byte \$) ; Blob (nil/string/bytes/thawed) ✓
             (blobs/read-blob
               ;; User blob => obey read-opts
               (.-read-mode         read-opts)
               (.-auto-deserialize? read-opts)
               in)

             (byte \*) ; Aggregate array ✓
             (read-aggregate-by-ones [] read-opts
               read-reply in)

             ;; --- RESP3 ∖ RESP2 -------------------------------------------------------
             (byte \.) (do (resp-com/discard-crlf in) ::end-of-aggregate-stream) ; End of aggregate stream ✓
             (byte \_) (do (resp-com/discard-crlf in) nil) ; Null ✓

             (byte \#) ; Bool ✓
             (let [b (.readByte in)]
               (resp-com/discard-crlf in)
               (== b #=(byte \t)))

             (byte \!) ; Blob error ✓
             (let [;; Nb cancel read-mode, markers
                   blob-reply (blobs/read-blob nil false in)]
               (when-not skip?
                 (redis-reply-error blob-reply) ))

             (byte \=) ; Verbatim string ; ✓
             (let [;; Nb cancel read-mode, markers
                   ^String s (blobs/read-blob nil false in)]
               (when-not skip?
                 (let [format  (subs s 0 3) ; "txt", "mkd", etc.
                       payload (subs s 4)]
                   ;; TODO API okay?
                   [:carmine/verbatim-string format payload])))

             (byte \,) ; Double ✓
             (let [s (.readLine in)]
               (when-not skip?
                 (enc/cond
                   (= s  "inf") Double/POSITIVE_INFINITY
                   (= s "-inf") Double/NEGATIVE_INFINITY
                   :else       (Double/parseDouble s))))

             (byte \() ; Big integer ✓
             (let [s (.readLine in)]
               (when-not skip?
                 (bigint (BigInteger. s))))

             (byte \~) (read-aggregate-by-ones #{} read-opts read-reply in) ; Aggregate set ✓
             (byte \%) (read-aggregate-by-pairs    read-opts read-reply in) ; Aggregate map ✓

             (byte \|) ; Attribute map ✓
             (let [attrs  (read-aggregate-by-pairs read-opts read-reply in)
                   target (read-reply              read-opts            in)]

               (when-not skip?
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
             (let [;; Completely neutral read-opts
                   v (read-aggregate-by-ones [] read-com/nil-read-opts read-reply in)]
               (when-let [push-fn *push-fn*] ; Not part of read-opts, reasonable?
                 (try ; Silently swallow errors (fn should have own error handling)
                   (push-fn v)
                   (catch Throwable _)))

               ;; Continue to actual reply
               (read-reply read-opts in))

             (throw
               (ex-info "[Carmine] Unexpected reply kind"
                 {:eid :carmine.resp.read/unexpected-reply-kind
                  :read-opts (read-com/describe-read-opts read-opts)
                  :kind
                  (enc/assoc-when
                    {:as-byte kind-b :as-char (byte kind-b)}
                    :end-of-stream? (== kind-b -1))})))

           (catch Throwable t
             (throw
               (ex-info "[Carmine] Unexpected reply error"
                 {:eid :carmine.resp.read/reply-error
                  :read-opts (read-com/describe-read-opts read-opts)
                  :kind {:as-byte kind-b :as-char (char kind-b)}}
                 t))))]

     (enc/cond

       skip?
       (if (identical? reply ::end-of-aggregate-stream)
         reply ; Always pass through
         :carmine/skipped)

       :if-let [^Parser p (parsing/when-fn-parser (.-parser read-opts))]
       (if (resp-com/reply-error? reply)
         (if (get (.-opts p) :parse-errors?)
           ((.-f p) reply)
           (do      reply))
         ((.-f p) reply))

       :default
       reply))))

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
           (is (resp-com/reply-error?
                 {:eid :carmine.resp.read/error-reply
                  :message "ERR Foo bar baz"
                  :code    "ERR"}
                 r1)))

         (let [[r1 r2 r3 r4] (read-reply (xs->in+ "*4" ":1" "-CODE1 a" ":2" "-CODE2 b"))]
           [(is (= r1 1))
            (is (= r3 2))
            (resp-com/reply-error? {:eid :carmine.resp.read/error-reply :code "CODE1" :message "CODE1 a"} r2)
            (resp-com/reply-error? {:eid :carmine.resp.read/error-reply :code "CODE2" :message "CODE2 b"} r4)])])

      (testing "Bulk errors"
        [(let [r1 (read-reply (xs->in+ "!10" "CODE Foo\r\n"))]
           (is (resp-com/reply-error?
                 {:eid :carmine.resp.read/error-reply
                  :message "CODE Foo\r\n"
                  :code    "CODE"}
                 r1)
             "Binary safe"))

         (let [[r1 r2 r3 r4] (read-reply (xs->in+ "*4" ":1" "!9" "CODE1 a\r\n" ":2" "!9" "CODE2 b\r\n"))]
           [(is (= r1 1))
            (is (= r3 2))
            (resp-com/reply-error? {:eid :carmine.resp.read/error-reply :code "CODE1" :message "CODE1 a\r\n"} r2)
            (resp-com/reply-error? {:eid :carmine.resp.read/error-reply :code "CODE2" :message "CODE2 b\r\n"} r4)])])])

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
                ))))])

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

;;;;

(let [read-reply      read-reply
      get-reply-error resp-com/get-reply-error]

  (defn read-replies
    ;; TODO Update
    ;; TODO Support dummy (local?) replies
    [in as-pipeline? reqs]
    (let [n-reqs   (count reqs)
          big-n?   (> n-reqs 10)
          complete (if big-n? persistent! identity)
          conj*    (if big-n? conj!       conj)
          error_   (volatile! nil)

          replies
          (complete
            (reduce
              (fn [acc req]
                ;; TODO read-mode, etc.
                (let [reply (read-reply in)]
                  (enc/cond
                    (identical? reply :carmine/skipped) acc

                    :if-let [reply-error (get-reply-error reply)]
                    (do
                      (vreset! error_ reply-error)
                      (conj*   acc    reply-error))

                    :else (conj* acc reply))))

              (if big-n? (transient []) [])
              reqs))]

      (if (or as-pipeline? (> (count replies) 1))
        replies ; Return replies as vector

        ;; Return single value, throw if reply error
        (if-let [error @error_]
          (throw error)
          (nth replies 0))))))
