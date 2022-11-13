(ns taoensso.carmine-v4.resp.read
  "Private ns, implementation detail."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]
   [taoensso.nippy  :as nippy]
   [taoensso.carmine-v4.resp.common :as com
    :refer [str->bytes bytes->str xs->in+ throw!]])

  (:import
   [java.io DataInputStream]
   [taoensso.carmine_v4.resp.common ReadOpts AsThawed Parser]))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*
  ^:dynamic taoensso.carmine-v4/*push-fn*
            taoensso.carmine-v4/issue-83-workaround?)

(alias 'core 'taoensso.carmine-v4)

(comment
  (remove-ns      'taoensso.carmine-v4.resp.read)
  (test/run-tests 'taoensso.carmine-v4.resp.read))

;;;;

(declare
  ^:private read-streaming-blob
  ^:private read-marked-blob

  ^:private blob->thawed
  ^:private blob->parsed-as-?bytes
  ^:private blob->parsed-as-?str
  ^:private complete-blob)

(defn- read-blob
  "$<length>\r\n<bytes>\r\n -> ?<binary safe String or other>"
  [read-mode read-markers? ^DataInputStream in]
  (let [size-str (.readLine in)]

    (if-let [stream? (= size-str "?")]
      ;; Streaming
      (read-streaming-blob read-mode in)

      ;; Not streaming
      (let [n (Integer/parseInt size-str)]
        (if (<= n 0) ; Empty or RESP2 null
          (if (== n 0)
            (if (identical? read-mode :bytes) (byte-array 0) "") ; Empty
            com/sentinel-null-reply)

          ;; Not empty
          (if-let [marker (and read-markers? (com/read-blob-?marker in n))]

            ;; Marked
            (read-marked-blob read-mode marker n in)

            ;; Unmarked
            (if (identical? read-mode :skip)

              ;; Skip
              (do
                (.skipBytes       in n)
                (com/discard-crlf in)
                com/sentinel-skipped-reply)

              ;; Don't skip
              (let [ba (byte-array n)]
                (.readFully       in ba 0 n)
                (com/discard-crlf in)
                (complete-blob read-mode ba)))))))))

(comment :see-tests-below)

(let [discard-stream-separator com/discard-stream-separator
      discard-crlf             com/discard-crlf]

  (defn- read-streaming-blob
    [read-mode ^DataInputStream in]

    (if (identical? read-mode :skip)

      ;; Skip
      (loop []
        (discard-stream-separator in)
        (let [n (Integer/parseInt (.readLine in))]
          (if (== n 0)
            com/sentinel-skipped-reply ; Stream complete

            ;; Stream continues
            (do
              (.skipBytes   in n)
              (discard-crlf in)
              (recur)))))

      ;; Don't skip
      ;; Note: even if the final output is a String, it's faster
      ;; to accumulate to BAOS then transform to a String at the
      ;; end rather than use a StringBuffer.
      (let [baos (java.io.ByteArrayOutputStream. 128)]
        (loop []
          (discard-stream-separator in)
          (let [n (Integer/parseInt (.readLine in))]
            (if (== n 0)

              ;; Stream complete
              (complete-blob read-mode (.toByteArray baos))

              ;; Stream continues
              (let [ba (byte-array n)]
                (.readFully   in ba 0 n)
                (discard-crlf in)
                (.write baos ba 0 (alength ba))
                (recur)))))))))

(defn- read-marked-blob
  [read-mode marker marked-size ^DataInputStream in]
  (let [n (- ^int marked-size 2)
        ?ba
        (when (pos? n)
          (let [ba (byte-array  n)]
            (.readFully in ba 0 n)
            (do            ba)))]

    (com/discard-crlf in)
    (case marker
      :nil nil
      :bin (or ?ba (byte-array 0))
      :npy
      (let [?thaw-opts (com/read-mode->?thaw-opts read-mode)]
        ;; ?ba should be nnil when marked
        (blob->thawed ?thaw-opts ?ba)))))

;;;; Read-mode handling

(defn- blob->thawed [?thaw-opts ba]
  (try
    (nippy/thaw ba ?thaw-opts)
    (catch Throwable t
      (com/reply-error
        "[Carmine] Nippy threw an error while thawing blob reply"
        (enc/assoc-when
          {:eid :carmine.read.blob/nippy-thaw-error
           :thaw-opts ?thaw-opts
           :bytes {:length (count ba) :content ba}}
          :possible-non-nippy-bytes? core/issue-83-workaround?)
        t))))

(defn- complete-blob [read-mode ba]
  (enc/cond!
    (identical? read-mode    nil) (com/bytes->str ba) ; Common case
    (identical? read-mode :bytes)                 ba

    ;; Shouldn't be here at all in this case
    ;; (identical? read-mode :skip) read-com/sentinel-skipped-reply

    :if-let [thaw-opts (com/read-mode->?thaw-opts read-mode)]
    (blob->thawed thaw-opts ba)))

;;;; Tests

(defn- empty-bytes? [ba] (enc/ba= ba (byte-array 0)))

(deftest ^:private _read-blob
  [(testing "Basics"
     [(is (= ""                      (read-blob nil            nil (xs->in+  0))) "As default: empty blob")
      (is (empty-bytes?              (read-blob :bytes         nil (xs->in+  0))) "As bytes:   empty blob")
      (is (= com/sentinel-null-reply (read-blob nil            nil (xs->in+ -1))) "As default: RESP2 null")
      (is (= com/sentinel-null-reply (read-blob :bytes         nil (xs->in+ -1))) "As bytes:   RESP2 null")
      (is (= com/sentinel-null-reply (read-blob (AsThawed. {}) nil (xs->in+ -1))) "As thawed:  RESP2 null")

      (is (=             (read-blob nil    nil (xs->in+ 5 "hello"))  "hello"))
      (is (= (bytes->str (read-blob :bytes nil (xs->in+ 5 "hello"))) "hello"))

      (is (=             (read-blob nil    nil (xs->in+ 7 "hello\r\n"))  "hello\r\n") "Binary safe")
      (is (= (bytes->str (read-blob :bytes nil (xs->in+ 7 "hello\r\n"))) "hello\r\n") "Binary safe")

      (let [pattern {:eid :carmine.read/missing-crlf}]
        [(is (throws? :common pattern (read-blob nil    nil (com/str->in "5\r\nhello"))))
         (is (throws? :common pattern (read-blob :bytes nil (com/str->in "5\r\nhello"))))
         (is (throws? :common pattern (read-blob nil    nil (com/str->in "5\r\nhello__"))))
         (is (throws? :common pattern (read-blob :bytes nil (com/str->in "5\r\nhello__"))))])])

   (testing "Streaming"
     [(is (=             (read-blob nil    nil (xs->in+ "?" ";5" "hello" ";1" " " ";6" "world!" ";0"))  "hello world!"))
      (is (= (bytes->str (read-blob :bytes nil (xs->in+ "?" ";5" "hello" ";1" " " ";6" "world!" ";0"))) "hello world!"))

      (let [pattern {:eid :carmine.read/missing-stream-separator}]
        [(is (throws? :common pattern (read-blob nil    nil (xs->in+ "?" ";5" "hello" "1" " " ";6" "world!" ";0"))))
         (is (throws? :common pattern (read-blob :bytes nil (xs->in+ "?" ";5" "hello" "1" " " ";6" "world!" ";0"))))])])

   (testing "Marked blobs"
     ;; See also `common/_read-blob-?marker` tests
     [(is (=             (read-blob nil true  (xs->in+ 5 "\u0000more"))   "\u0000more"))
      (is (=             (read-blob nil true  (xs->in+ 2 "\u0000_"))               nil))
      (is (=             (read-blob nil false (xs->in+ 2 "\u0000_"))         "\u0000_"))
      (is (= (bytes->str (read-blob nil true  (xs->in+ 6 "\u0000<more")))       "more"))
      (is (=             (read-blob nil false (xs->in+ 6 "\u0000<more")) "\u0000<more"))

      (testing "Mark ba still removed from returned bytes, even with :bytes read mode"
        [(is (= (bytes->str (read-blob nil    true (xs->in+ "5" (com/xs->ba com/ba-bin [\a \b \c])))) "abc"))
         (is (= (bytes->str (read-blob :bytes true (xs->in+ "5" (com/xs->ba com/ba-bin [\a \b \c])))) "abc"))])

      (let [data       nippy/stress-data-comparable
            ba         (nippy/freeze data)
            marked-ba  (com/xs->ba com/ba-npy ba)
            marked-len (alength ^bytes marked-ba)]

        (is (= (read-blob nil true (xs->in+ marked-len marked-ba)) data) "Simple Nippy data"))

      (let [data       nippy/stress-data-comparable
            pwd        [:salted "secret"]
            ba         (nippy/freeze data {:password pwd})
            marked-ba  (com/xs->ba com/ba-npy ba)
            marked-len (alength ^bytes marked-ba)]

        [(is (= (read-blob (AsThawed. {:password pwd}) true (xs->in+ marked-len marked-ba)) data)
           "Encrypted Nippy data (good password)")

         (let [r (read-blob nil true (xs->in+ marked-len marked-ba))]

           [(is (com/reply-error? {:eid :carmine.read.blob/nippy-thaw-error} r)
              "Encrypted Nippy data (bad password)")

            (is (enc/ba= (-> r ex-data :bytes :content) ba)
              "Unthawed Nippy data still provided")])])])])

;;;; Aggregates

(defn- read-basic-reply
  "Basic version of `read-reply`, useful for testing"
  [_read-opts ^DataInputStream in]
  (let [kind-b (.readByte in)]
    (enc/case-eval kind-b
      (byte \+)                 (.readLine in)  ; Simple string
      (byte \:) (Long/parseLong (.readLine in)) ; Simple long
      (byte \.)
      (do
        (com/discard-crlf in)
        com/sentinel-end-of-aggregate-stream))))

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream]
  (defn- read-aggregate-by-ones
    [to ^ReadOpts read-opts read-reply ^DataInputStream in]
    (let [size-str (.readLine in)
          inner-read-opts (com/in-aggregate-read-opts read-opts)
          skip? (identical? (.-read-mode read-opts) :skip)]

      (if-let [stream? (= size-str "?")]

        ;; Streaming
        (enc/cond

          skip?
          (loop []
            (let [x (read-reply inner-read-opts in)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                com/sentinel-skipped-reply
                (recur))))

          ;; Reducing parser
          :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
          (let [rf ((.rfc p))
                init-acc (rf)]
            (loop [acc init-acc]
              (let [x (read-reply inner-read-opts in)]
                (if (identical? x sentinel-end-of-aggregate-stream)
                  (do    (rf acc)) ; Complete acc
                  (recur (rf acc x))))))

          :default
          (loop [acc (transient (empty to))]
            (let [x (read-reply inner-read-opts in)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                (persistent!  acc)
                (recur (conj! acc x))))))

        ;; Not streaming
        (let [n (Integer/parseInt size-str)]
          (if (<= n 0) ; Empty or RESP2 null
            (if (== n 0) to com/sentinel-null-reply)

            (enc/cond

              skip?
              (enc/reduce-n (fn [_ _] (read-reply inner-read-opts in))
                0 n)

              ;; Reducing parser
              :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
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
                #(read-reply inner-read-opts in)))))))))

(deftest ^:private _read-aggregate-by-ones-bootstrap
  ;; Very basic bootstrap tests using only `read-basic-reply`
  [(is (= (read-aggregate-by-ones [] com/read-opts-default nil (xs->in+  0))                      []) "Empty blob")
   (is (= (read-aggregate-by-ones [] com/read-opts-default nil (xs->in+ -1)) com/sentinel-null-reply) "RESP2 null")

   (is (= (read-aggregate-by-ones [] com/read-opts-default read-basic-reply (xs->in+ 2   ":1" ":2"))     [1 2]))
   (is (= (read-aggregate-by-ones [] com/read-opts-default read-basic-reply (xs->in+ "?" ":1" ":2" ".")) [1 2]) "Streaming")])

(let [keywordize (fn [x] (if (string? x) (keyword x) x))
      sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream]

  (defn- read-aggregate-by-pairs
    "Like `read-aggregate-by-ones` but optimized for read-pair
    cases (notably maps)."
    [^ReadOpts read-opts read-reply ^DataInputStream in]
    (let [size-str (.readLine in)
          inner-read-opts (com/in-aggregate-read-opts read-opts)
          skip? (identical? (.-read-mode read-opts) :skip)]

      (if-let [stream? (= size-str "?")]

        ;; Streaming
        (enc/cond

          skip?
          (loop []
            (let [x (read-reply inner-read-opts in)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                com/sentinel-skipped-reply
                (let [_k x
                      _v (read-reply inner-read-opts in)]
                  (recur)))))

          ;; Reducing parser
          :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
          (let [rf    ((.-rfc    p))
                kv-rf? (.-kv-rf? p)
                init-acc (rf)]

            (loop [acc init-acc]
              (let [x (read-reply inner-read-opts in)]
                (if (identical? x sentinel-end-of-aggregate-stream)
                  (rf acc) ; Complete acc
                  (let [k x ; Without kfn!
                        v (read-reply inner-read-opts in)]
                    (recur
                      (if kv-rf?
                        (rf acc                               k v)
                        (rf acc (clojure.lang.MapEntry/create k v)))))))))

          :let [kfn (if (.-keywordize-maps? read-opts) keywordize identity)]
          :default
          (loop [acc (transient {})]
            (let [x (read-reply inner-read-opts in)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                (persistent! acc)
                (let [k (kfn x)
                      v (read-reply inner-read-opts in)]
                  (recur (assoc! acc k v)))))))

        ;; Not streaming
        (let [n (Integer/parseInt size-str)]
          (if (<= n 0) ; Empty or RESP2 null
            (if (== n 0) {} com/sentinel-null-reply)

            (enc/cond

              skip?
              (enc/reduce-n
                (fn [_ _]
                  (let [_k (read-reply inner-read-opts in)
                        _v (read-reply inner-read-opts in)]
                    nil))
                0 n)

              ;; Reducing parser
              :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
              (let [rf    ((.-rfc    p))
                    kv-rf? (.-kv-rf? p)
                    init-acc (rf)]
                (rf ; Complete
                  (enc/reduce-n
                    (fn [acc _n]
                      (let [k (read-reply inner-read-opts in) ; Without kfn!
                            v (read-reply inner-read-opts in)]
                        (if kv-rf?
                          (rf acc                               k v)
                          (rf acc (clojure.lang.MapEntry/create k v)))))
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
     [(is (= (read-aggregate-by-pairs com/read-opts-default nil (xs->in+  0))                      {}) "Empty blob")
      (is (= (read-aggregate-by-pairs com/read-opts-default nil (xs->in+ -1)) com/sentinel-null-reply) "RESP2 null")

      (is (= (read-aggregate-by-pairs com/read-opts-default read-basic-reply (xs->in+ 2 "+k1" "+v1" "+k2" "+v2")) {:k1  "v1" :k2 "v2"}) "With keywordize")
      (is (= (read-aggregate-by-pairs com/read-opts-natural read-basic-reply (xs->in+ 2 "+k1" "+v1"  ":2" "+v2")) {"k1" "v1",  2 "v2"}) "W/o  keywordize")

      (is (= (read-aggregate-by-pairs com/read-opts-default read-basic-reply (xs->in+ "?" "+k1" "+v1" ":2" "+v2" ".")) {:k1  "v1"  2 "v2"}) "Streaming, with keywordize")
      (is (= (read-aggregate-by-pairs com/read-opts-natural read-basic-reply (xs->in+ "?" "+k1" "+v1" ":2" "+v2" ".")) {"k1" "v1", 2 "v2"}) "Streaming, w/o  keywordize")])])

(defn- redis-reply-error [?message]
  (let [^String message (if (nil? ?message) "" ?message)
        code (re-find #"^\S+" message)] ; "ERR", "WRONGTYPE", etc.

    (com/reply-error "[Carmine] Redis replied with an error"
      {:eid :carmine.read/error-reply
       :message message
       :code    code})))

(comment (redis-reply-error "ERR Foo bar"))

(declare complete-reply)

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream
      sentinel-null-reply              com/sentinel-null-reply]

  (defn read-reply
    "Blocks to read reply from given DataInputStream.
    Returns completed reply."

    ;; For REPL/testing
    ([in] (read-reply (com/get-read-opts) in))

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
               (read-blob
                 ;; User blob => obey read-opts
                 (.-read-mode         read-opts)
                 (.-auto-deserialize? read-opts)
                 in)

               (byte \*) ; Aggregate array ✓
               (read-aggregate-by-ones [] read-opts
                 read-reply in)

               ;; --- RESP3 ∖ RESP2 -------------------------------------------------------
               (byte \.) (do (com/discard-crlf in) sentinel-end-of-aggregate-stream) ; ✓
               (byte \_) (do (com/discard-crlf in) sentinel-null-reply) ; ✓

               (byte \#) ; Bool ✓
               (let [b  (.readByte in)]
                 (com/discard-crlf in)
                 (== b #=(byte \t)))

               (byte \!) ; Blob error ✓
               (let [;; Nb cancel read-mode, markers
                     blob-reply (read-blob nil false in)]
                 (when-not skip?
                   (redis-reply-error blob-reply) ))

               (byte \=) ; Verbatim string ; ✓
               (let [;; Nb cancel read-mode, markers
                     ^String s (read-blob nil false in)]
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
                         {:eid :carmine.read/attributes-for-unexpected-type
                          :target {:type (type target) :value target}
                          :attributes attrs})))))

               (byte \>) ; Push ✓
               (let [v (read-aggregate-by-ones [] com/read-opts-natural read-reply in)]
                 (when-let [push-fn core/*push-fn*] ; Not part of read-opts, reasonable?
                   (try ; Silently swallow errors (fn should have own error handling)
                     (push-fn v)
                     (catch Throwable _)))

                 ;; Continue to actual reply
                 (read-reply read-opts in))

               (throw
                 (ex-info "[Carmine] Unexpected reply kind"
                   {:eid :carmine.read/unexpected-reply-kind
                    :read-opts (com/describe-read-opts read-opts)
                    :kind
                    (enc/assoc-when
                      {:as-byte kind-b :as-char (byte kind-b)}
                      :end-of-stream? (== kind-b -1))})))

             (catch Throwable t
               (com/reply-error "[Carmine] Unexpected reply error"
                 {:eid :carmine.read/reply-error
                  :read-opts (com/describe-read-opts read-opts)
                  :kind {:as-byte kind-b :as-char (char kind-b)}}
                 t)))]

       (complete-reply read-opts reply)))))

(comment :see-tests)

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream
      sentinel-null-reply              com/sentinel-null-reply]

  (defn complete-reply [^ReadOpts read-opts reply]
    (let [skip? (identical? (.-read-mode read-opts) :skip)]
      (enc/cond

        skip?
        (if (identical? reply sentinel-end-of-aggregate-stream)
          reply ; Always pass through
          com/sentinel-skipped-reply)

        :if-let [^Parser p (com/when-fn-parser (.-parser read-opts))]
        (enc/cond

          (com/reply-error? reply)
          (if (get (.-opts p) :parse-error-replies?)
            ((.-f p) reply)
            (do      reply))

          (identical? reply sentinel-null-reply)
          (if (get (.-opts p) :parse-null-replies?)
            ((.-f p) nil)
            (do      nil))

          :default
          ((.-f p) reply))

        :default
        (if (identical? reply sentinel-null-reply)
          nil
          reply)))))
