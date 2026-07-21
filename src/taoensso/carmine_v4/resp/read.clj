(ns ^:no-doc taoensso.carmine-v4.resp.read
  "Private RESP read implementation."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.nippy  :as nippy]
   [taoensso.carmine-v4.resp.common :as com])

  (:import
   [java.io ByteArrayInputStream DataInputStream]
   [java.nio.charset StandardCharsets]
   [java.util Arrays]
   [taoensso.carmine_v4.classes AttributedReply]
   [taoensso.carmine_v4.resp.common ReadOpts #_ReadThawed Parser]))

(comment (remove-ns 'taoensso.carmine-v4.resp.read))

;;;;

(declare
  ^:private read-streaming-blob
  ^:private read-marked-blob

  ^:private blob->thawed
  ^:private complete-blob
  ^:private complete-blob-bytes)

(defn- invalid-length! [kind length]
  (truss/ex-info! "[Carmine] Invalid RESP length"
    {:eid :carmine.read/invalid-length
     :kind kind
     :length length}))

(defn- parse-length
  [kind null-allowed? size-str]
  (if (and null-allowed? (= size-str "-1"))
    -1
    (if-not (re-matches #"[0-9]+" size-str)
      (invalid-length! kind size-str)
      (try
        (Integer/parseInt size-str)
        (catch NumberFormatException _
          (invalid-length! kind size-str))))))

(defn- invalid-scalar! [kind value]
  (truss/ex-info! "[Carmine] Invalid RESP scalar value"
    {:eid   :carmine.read/invalid-scalar
     :kind  kind
     :value value}))

(defn- parse-integer-scalar [s]
  (when-not (re-matches #"[+-]?[0-9]+" s)
    (invalid-scalar! :integer s))
  (try
    (Long/parseLong s)
    (catch NumberFormatException _
      (invalid-scalar! :integer s))))

(defn- validate-big-integer-scalar! [s]
  (when-not (re-matches #"[+-]?[0-9]+" s)
    (invalid-scalar! :big-integer s)))

(defn- parse-double-scalar [s]
  (enc/cond
    (= s  "inf") Double/POSITIVE_INFINITY
    (= s "-inf") Double/NEGATIVE_INFINITY

    ;; RESP3 v1.4 normalized NaN to `nan` and forbade `-nan`, but Redis
    ;; <7.2 could emit libc variants and clients are expected to accept them.
    (re-matches #"(?i)-?nan(?:\([a-z0-9_]*\))?" s) Double/NaN

    :else
    (do
      (when-not (re-matches #"[+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?" s)
        (invalid-scalar! :double s))
      (let [n
            (try
              (Double/parseDouble s)
              (catch NumberFormatException _
                (invalid-scalar! :double s)))]
        (if (Double/isInfinite n)
          (invalid-scalar! :double s)
          n)))))

(defn- read-blob
  "$<length>\r\n<bytes>\r\n -> ?<binary safe String or other>"
  ([read-mode read-markers? issue-83-workaround? in]
   (read-blob :blob read-mode read-markers? issue-83-workaround? in))
  ([kind read-mode read-markers? issue-83-workaround? ^DataInputStream in]
   (enc/cond
     :let [size-str (com/read-crlf-line in)]

     :if-let [streaming? (= size-str "?")]
     (if (= kind :blob)
       (read-streaming-blob read-mode read-markers? issue-83-workaround? in)
       (invalid-length! kind size-str))

     :let [n (parse-length kind (= kind :blob) size-str)]

     :do
     (when-not (neg? n)
       (com/check-blob-size!        in kind n)
       (com/check-frame-additional! in kind (+ n 2)))

     (<= n 0) ; Empty or RESP2 null (-1)
     (if (== n 0)
       (do
         (com/discard-crlf in)
         (if (com/skip-read-mode? read-mode)
           com/sentinel-skipped-reply
           (complete-blob read-mode issue-83-workaround? (byte-array 0))))
       com/sentinel-null-reply)

     (com/skip-read-mode? read-mode) ; Skip
     ;; NB before marker handling: skipped blobs are discarded without
     ;; marker decoding or Nippy thawing
     (do
       (com/discard-bytes in n)
       (com/discard-crlf in)
       com/sentinel-skipped-reply)

     :if-let [marker (and read-markers?
                       (com/read-blob-?marker in n issue-83-workaround?))]
     (read-marked-blob read-mode marker n issue-83-workaround? in) ; Marked

     :else
     (let [ba (byte-array n)]
       (.readFully       in ba 0 n)
       (com/discard-crlf in)
       (complete-blob read-mode issue-83-workaround? ba)))))

(let [discard-stream-separator com/discard-stream-separator
      discard-crlf             com/discard-crlf]

  (defn- read-streaming-blob
    [read-mode read-markers? issue-83-workaround? ^DataInputStream in]

    (if (com/skip-read-mode? read-mode)

      ;; Skip
      (loop [total 0]
        (discard-stream-separator in)
        (let [n (parse-length :streaming-blob-chunk false (com/read-crlf-line in))]
          (if (== n 0)
            ;; Stream complete
            com/sentinel-skipped-reply

            ;; Stream continues
            (let [total (long (+ total n))]
              (com/check-blob-size!        in :streaming-blob total)
              (com/check-frame-additional! in :streaming-blob-chunk (+ n 2))
              (com/discard-bytes in n)
              (discard-crlf in)
              (recur total)))))

      ;; Don't skip
      ;; Even if the final output is a String, it's faster
      ;; to accumulate to BAOS then transform to a String at the
      ;; end rather than use a StringBuffer.
      (let [baos (java.io.ByteArrayOutputStream. 128)]
        (loop [total 0]
          (discard-stream-separator in)
          (let [n (parse-length :streaming-blob-chunk false (com/read-crlf-line in))]
            (if (== n 0)

              ;; Stream complete
              (complete-blob-bytes read-mode read-markers?
                issue-83-workaround? (.toByteArray baos))

              ;; Stream continues
              (let [total (long (+ total n))
                    _ (com/check-blob-size!        in :streaming-blob total)
                    _ (com/check-frame-additional! in :streaming-blob-chunk (+ n 2))
                    ba (byte-array n)]
                (.readFully   in ba 0 n)
                (discard-crlf in)
                (.write baos ba 0 (alength ba))
                (recur total)))))))))

(defn- read-marked-blob
  [read-mode marker marked-size issue-83-workaround? ^DataInputStream in]
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
      (if (identical? read-mode :bytes)
        (or ?ba (byte-array 0))
        (let [?thaw-opts (com/read-mode->?thaw-opts read-mode)]
          ;; ?ba should be nnil when marked
          (blob->thawed ?thaw-opts issue-83-workaround? ?ba))))))

;;;; Read-mode handling

(defn- blob->thawed [?thaw-opts issue-83-workaround? ba]
  (try
    (nippy/thaw ba ?thaw-opts)
    (catch Throwable t
      (com/reply-error
        "[Carmine] Nippy threw an error while thawing blob reply"
        (enc/assoc-when
          {:eid :carmine.read/nippy-thaw-error
           :thaw-opts ?thaw-opts
           :bytes {:length (count ba) :content ba}}
          :possible-non-nippy-bytes? issue-83-workaround?)
        t))))

(defn- complete-blob [read-mode issue-83-workaround? ba]
  (enc/cond!
    (identical? read-mode    nil) (enc/utf8-ba->str ba) ; Common case
    (identical? read-mode :bytes)                   ba

    ;; Shouldn't be here at all in this case
    ;; (identical? read-mode :skip) com/sentinel-skipped-reply

    :if-let [thaw-opts (com/read-mode->?thaw-opts read-mode)]
    (blob->thawed thaw-opts issue-83-workaround? ba)))

(defn- complete-blob-bytes
  [read-mode read-markers? issue-83-workaround? ^bytes ba]
  (let [n (alength ba)
        marker
        (when (and read-markers? (>= n 2))
          (with-open [in (DataInputStream. (ByteArrayInputStream. ba))]
            (com/read-blob-?marker in n issue-83-workaround?)))]
    (if marker
      (case marker
        :nil nil
        :bin (Arrays/copyOfRange ba 2 n)
        :npy
        (let [payload (Arrays/copyOfRange ba 2 n)]
          (if (identical? read-mode :bytes)
            payload
            (blob->thawed (com/read-mode->?thaw-opts read-mode)
              issue-83-workaround? payload))))
      (complete-blob read-mode issue-83-workaround? ba))))

(defn ^:no-doc complete-stored-blob
  "Decodes stored blob bytes with captured read options. The caller can retain
  the original bytes as a CAS token while using the decoded logical value.
  Internal."
  [^ReadOpts read-opts ^bytes ba]
  (let [read-mode  (.-read-mode read-opts)
        auto-thaw? (.-auto-thaw? read-opts)
        issue-83-workaround? (.-issue-83-workaround? read-opts)
        decoded
        (if (com/skip-read-mode? read-mode)
          com/sentinel-skipped-reply
          (complete-blob-bytes read-mode auto-thaw?
            issue-83-workaround? ba))]
    ;; Byte mode may otherwise expose the mutable CAS token to the caller.
    (if (identical? decoded ba)
      (Arrays/copyOf ba (alength ba))
      decoded)))

;;;; Aggregates

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream]
  (defn- read-aggregate-element
    [read-reply read-opts ^DataInputStream in]
    (let [x (read-reply read-opts in)]
      (if (com/reply-error? {:eid :carmine.read/unexpected-read-error} x)
        (throw x)
        x)))

  (defn- read-required-aggregate-element
    [read-reply read-opts ^DataInputStream in]
    (let [x (read-aggregate-element read-reply read-opts in)]
      (if (identical? x sentinel-end-of-aggregate-stream)
        (truss/ex-info! "[Carmine] Unexpected end of RESP aggregate stream"
          {:eid :carmine.read/unexpected-aggregate-end})
        x))))

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream]
  (defn- read-aggregate-by-ones*
    [kind to ^ReadOpts read-opts read-reply ^DataInputStream in]
    (let [size-str (com/read-crlf-line in)
          inner-read-opts (com/in-aggregate-read-opts read-opts)
          skip? (com/skip-read-mode? (.-read-mode read-opts))]

      (when (and (= size-str "?") (= kind :push))
        (invalid-length! kind size-str))

      (if (= size-str "?")

        ;; Streaming
        (enc/cond
          skip?
          (loop [n 0]
            (let [x (read-aggregate-element read-reply inner-read-opts in)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                com/sentinel-skipped-reply
                (let [n (inc n)]
                  (com/check-aggregate-size! in kind n)
                  (recur n)))))

          ;; Reducing parser
          :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
          (let [rf ((.rfc p))
                init-acc (rf)]
            (loop [n 0, acc init-acc]
              (let [x (read-aggregate-element read-reply inner-read-opts in)]
                (if (identical? x sentinel-end-of-aggregate-stream)
                  (rf (unreduced acc)) ; Complete acc
                  (let [n (inc n)]
                    (com/check-aggregate-size! in kind n)
                    (recur n (if (reduced? acc) acc (rf acc x))))))))

          :default
          (loop [n 0, acc (transient (empty to))]
            (let [x (read-aggregate-element read-reply inner-read-opts in)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                (persistent!  acc)
                (let [n (inc n)]
                  (com/check-aggregate-size! in kind n)
                  (recur n (conj! acc x)))))))

        ;; Not streaming
        (let [n (parse-length kind (= kind :array) size-str)]
          (when (and (= kind :push) (zero? n))
            (invalid-length! kind size-str))
          (when-not (neg? n) (com/check-aggregate-size! in kind n))
          (enc/cond
            (< n 0) com/sentinel-null-reply
            skip?
            (do
              (enc/reduce-n
                (fn [_ _] (read-required-aggregate-element read-reply inner-read-opts in))
                0 n)
              com/sentinel-skipped-reply)

            ;; Reducing parser
            :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
            (let [rf ((.-rfc p))
                  init-acc (rf)]
              (loop [remaining n, acc init-acc]
                (if (pos? remaining)
                  (let [x (read-required-aggregate-element read-reply inner-read-opts in)]
                    (recur (dec remaining) (if (reduced? acc) acc (rf acc x))))
                  (rf (unreduced acc))))) ; Complete acc

            (== n 0) to
            :default
            (enc/repeatedly-into to n
              #(read-required-aggregate-element read-reply inner-read-opts in)))))))

  (defn- read-aggregate-by-ones
    [kind to read-opts read-reply in]
    (com/with-aggregate in kind
      #(read-aggregate-by-ones* kind to read-opts read-reply in))))

(let [keywordize (fn [x] (if (string? x) (keyword x) x))
      sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream]

  (defn- read-aggregate-by-pairs*
    "Reads aggregates as pairs. Optimized for maps."
    [kind ^ReadOpts read-opts read-reply ^DataInputStream in]
    (let [size-str (com/read-crlf-line in)
          inner-read-opts (com/in-aggregate-read-opts read-opts)
          skip? (com/skip-read-mode? (.-read-mode read-opts))]

      (when (and (= size-str "?") (not= kind :map))
        (invalid-length! kind size-str))

      (if (= size-str "?")

        ;; Streaming
        (enc/cond
          skip?
          (loop [n 0]
            (let [x (read-aggregate-element read-reply inner-read-opts in)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                com/sentinel-skipped-reply
                (let [n  (inc n)
                      _  (com/check-aggregate-size! in kind n)
                      _k x
                      _v (read-required-aggregate-element read-reply inner-read-opts in)]
                  (recur n)))))

          ;; Reducing parser
          :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
          (let [rf    ((.-rfc    p))
                kv-rf? (.-kv-rf? p)
                init-acc (rf)]

            (loop [n 0, acc init-acc]
              (let [x (read-aggregate-element read-reply inner-read-opts in)]
                (if (identical? x sentinel-end-of-aggregate-stream)
                  (rf (unreduced acc)) ; Complete acc
                  (let [n (inc n)
                        _ (com/check-aggregate-size! in kind n)
                        k x ; Without kfn!
                        v (read-required-aggregate-element read-reply inner-read-opts in)]
                    (recur n
                      (if (reduced? acc)
                        acc
                        (if kv-rf?
                          (rf acc                               k v)
                          (rf acc (clojure.lang.MapEntry/create k v))))))))))

          :let [kfn (if (.-keywordize-maps? read-opts) keywordize identity)]
          :default
          (loop [n 0, acc (transient {})]
            (let [x (read-aggregate-element read-reply inner-read-opts in)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                (persistent! acc)
                (let [n (inc n)
                      _ (com/check-aggregate-size! in kind n)
                      k (kfn x)
                      v (read-required-aggregate-element read-reply inner-read-opts in)]
                  (recur n (assoc! acc k v)))))))

        ;; Not streaming
        (let [n (parse-length kind false size-str)]
          (when-not (neg? n) (com/check-aggregate-size! in kind n))
          (enc/cond
            (< n 0) com/sentinel-null-reply
            skip?
            (do
              (enc/reduce-n
                (fn [_ _]
                  (let [_k (read-required-aggregate-element read-reply inner-read-opts in)
                        _v (read-required-aggregate-element read-reply inner-read-opts in)]
                    nil))
                0 n)
              com/sentinel-skipped-reply)

            ;; Reducing parser
            :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
            (let [rf    ((.-rfc    p))
                  kv-rf? (.-kv-rf? p)
                  init-acc (rf)]
              (loop [remaining n, acc init-acc]
                (if (pos? remaining)
                  (let [k (read-required-aggregate-element read-reply inner-read-opts in) ; Without kfn!
                        v (read-required-aggregate-element read-reply inner-read-opts in)]
                    (recur (dec remaining)
                      (if (reduced? acc)
                        acc
                        (if kv-rf?
                          (rf acc                               k v)
                          (rf acc (clojure.lang.MapEntry/create k v))))))
                  (rf (unreduced acc))))) ; Complete acc

            (== n 0) {}
            :let [kfn (if (.-keywordize-maps? read-opts) keywordize identity)]
            :default
            (if (> n 10)
              (persistent!
                (enc/reduce-n
                  (fn [m _]
                    (let [k (kfn (read-required-aggregate-element read-reply inner-read-opts in))
                          v      (read-required-aggregate-element read-reply inner-read-opts in)]
                      (assoc! m k v)))
                  (transient {})
                  n))

              (enc/reduce-n
                (fn [m _]
                  (let [k (kfn (read-required-aggregate-element read-reply inner-read-opts in))
                        v      (read-required-aggregate-element read-reply inner-read-opts in)]
                    (assoc m k v)))
                {}
                n)))))))

  (defn- read-aggregate-by-pairs
    [kind read-opts read-reply in]
    (com/with-aggregate in kind
      #(read-aggregate-by-pairs* kind read-opts read-reply in))))

(defn- redis-reply-error [?message]
  (let [^String message (if (nil? ?message) "" ?message)
        code (re-find #"^\S+" message)] ; "ERR", "WRONGTYPE", etc.

    (com/reply-error "[Carmine] Redis replied with an error"
      {:eid :carmine.read/redis-error-reply
       :message message
       :code    code})))

(comment (redis-reply-error "ERR Foo bar"))

(declare complete-reply read-reply-impl)

(defrecord VerbatimString [format content])
(defrecord WithAttributes [content attributes]
  AttributedReply
  (replyContent     [_] content)
  (replyAttributes [_] attributes))
(defrecord Push [data])
(deftype ^:private PushReply [data])
(deftype ^:private AttributeReply [attributes])

(defn- read-nested-reply [read-opts in]
  (read-reply-impl read-opts in false false))

(defn- attach-attributes [reply attributes]
  (if (nil? attributes)
    reply
    (let [existing (com/reply-attributes reply)
          attributes
          (if (map? existing)
            (merge attributes existing) ; Content-nearer attributes win
            attributes)
          content (com/reply-content reply)]
      (if (and (enc/can-meta? content) (map? attributes))
        (vary-meta content assoc :carmine/reply-attributes attributes)
        (WithAttributes. content attributes)))))

(defn- merge-attributes! [pending attributes]
  (reduce-kv assoc! (or pending (transient {})) attributes))

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream
      sentinel-null-reply              com/sentinel-null-reply]

  (defn- read-reply-impl
    ([read-opts in return-top-level-pushes?]
     (read-reply-impl read-opts in return-top-level-pushes? true))
    ([^ReadOpts read-opts ^DataInputStream in return-top-level-pushes? top-level?]
     ;; Since dynamic vars are ephemeral and reply reading is lazy, neither this
     ;; fn nor any of its children should use dynamic vars. Instead, we'll capture
     ;; dynamic config to `com/ReadOpts` at the appropriate time.
     (when top-level? (com/begin-read! in))
     (try
       (loop [pending-attributes nil]
         (let [kind-b   (.read in)
               read-mode (.-read-mode read-opts)
               skip?   (com/skip-read-mode? read-mode)
               errors? (identical? read-mode :skip+errors)

               reply
               (try
                 (enc/case-eval kind-b
                   ;; --- RESP2 ⊂ RESP3 ---------------------------------------------------
                   (int \+) (com/read-crlf-line in) ; Simple string ✓
                   (int \:) ; Simple long ✓
                   (let [s (com/read-crlf-line in)]
                     (let [n (parse-integer-scalar s)]
                       (when-not skip? n)))

                   (int \-) ; Simple error ✓
                   (let [s (com/read-crlf-line in)]
                     (when (or errors? (not skip?)) (redis-reply-error s)))

                   (int \$) ; Blob (nil/string/bytes/thawed) ✓
                   (read-blob (.-read-mode read-opts) (.-auto-thaw? read-opts)
                     (.-issue-83-workaround? read-opts) in)

                   (int \*) ; Aggregate array ✓
                   (read-aggregate-by-ones :array [] read-opts read-nested-reply in)

                   ;; --- RESP3 ∖ RESP2 ---------------------------------------------------
                   (int \.)
                   (do
                     (com/discard-crlf in)
                     (if (or top-level? pending-attributes)
                       (invalid-scalar! :aggregate-end ".")
                       sentinel-end-of-aggregate-stream))
                   (int \_) (do (com/discard-crlf in) sentinel-null-reply) ; ✓

                   (int \#) ; Bool ✓
                   (let [b (.readByte in)]
                     (com/discard-crlf in)
                     (case b
                       #=(int \t) (when-not skip? true)
                       #=(int \f) (when-not skip? false)
                       (invalid-scalar! :boolean b)))

                   (int \!) ; Blob error ✓
                   (let [blob-reply
                         (read-blob :blob-error
                           (when (and skip? (not errors?)) :skip)
                           false false in)]
                     (when (or errors? (not skip?)) (redis-reply-error blob-reply)))

                   (int \=) ; Verbatim string ✓
                   (let [^bytes ba (read-blob :verbatim-string :bytes false false in)
                         n (alength ba)]
                     (when-not (and (<= 4 n) (== (aget ba 3) (int \:)))
                       (invalid-scalar! :verbatim-string (enc/utf8-ba->str ba)))
                     (when-not skip?
                       (let [format  (String. ba 0 3 StandardCharsets/UTF_8)
                             content (String. ba 4 (- n 4) StandardCharsets/UTF_8)]
                         (if (.-raw-verbatim-strings? read-opts)
                           (VerbatimString. format content)
                           content))))

                   (int \,) ; Double ✓
                   (let [s (com/read-crlf-line in)]
                     (let [n (parse-double-scalar s)]
                       (when-not skip? n)))

                   (int \() ; Big integer ✓
                   (let [s (com/read-crlf-line in)]
                     (validate-big-integer-scalar! s)
                     (when-not skip? (bigint (BigInteger. ^String s))))

                   (int \~) (read-aggregate-by-ones  :set #{} read-opts read-nested-reply in) ; ✓
                   (int \%) (read-aggregate-by-pairs :map     read-opts read-nested-reply in) ; ✓

                   (int \|) ; Attribute map ✓
                   (AttributeReply.
                     ;; Attrs are read parser-free. The loop accumulates
                     ;; consecutive maps before completing their content once.
                     (com/with-aggregate in :attributes
                       (fn []
                         (read-aggregate-by-pairs* :attributes
                           (if skip?
                             com/read-opts-natural
                             (com/in-aggregate-read-opts read-opts))
                           read-nested-reply in))))

                   (int \>) ; Push ✓
                   (do
                     (when-not top-level?
                       (invalid-scalar! :push :nested))
                     (let [v
                           (com/with-aggregate in :push
                             #(read-aggregate-by-ones*
                                :push []
                                (if (and return-top-level-pushes? (not skip?))
                                  (com/in-aggregate-read-opts read-opts)
                                  com/read-opts-natural)
                                read-nested-reply in))
                           push-kind (com/reply-content (first v))]
                       (when-not (or (string? push-kind) (enc/bytes? push-kind))
                         (invalid-scalar! :push-kind push-kind))
                       (PushReply. v)))

                   (truss/ex-info! "[Carmine] Unexpected reply kind"
                     {:eid :carmine.read/unexpected-reply-kind
                      :read-opts (com/describe-read-opts read-opts)
                      :kind
                      (enc/assoc-when
                        {:as-byte kind-b :as-char (char (bit-and (int kind-b) 0xff))}
                        :end-of-stream? (== kind-b -1))}))

                 (catch Throwable t
                   (cond
                     (com/resource-error? t) (throw t)
                     (com/reply-error? {:eid :carmine.read/unexpected-read-error} t) t
                     :else
                     (com/reply-error "[Carmine] Unexpected reply error"
                       {:eid :carmine.read/unexpected-read-error
                        :read-opts (com/describe-read-opts read-opts)
                        :kind {:as-byte kind-b :as-char (char (bit-and (int kind-b) 0xff))}}
                       t))))]

           (cond
             (instance? AttributeReply reply)
             (let [attributes (.-attributes ^AttributeReply reply)]
               (recur (merge-attributes! pending-attributes attributes)))

             (instance? PushReply reply)
             (let [data (attach-attributes
                          (.-data ^PushReply reply)
                          (when pending-attributes
                            (persistent! pending-attributes)))]
               (if return-top-level-pushes?
                 (Push. data)
                 (do
                   (when-let [push-fn (.-push-fn read-opts)]
                     (try
                       (push-fn data)
                       ;; Manager dispatchers report their own failures. This
                       ;; defensive catch keeps dispatch from corrupting reads.
                       (catch Throwable _ nil)))
                   (com/next-frame! in)
                   (recur nil))))

             :else
             (let [reply (complete-reply read-opts reply)]
               ;; There's no trustworthy content to decorate after a protocol
               ;; or framing error, and the invalidation path may throw it.
               (if (or
                     (identical? reply com/sentinel-skipped-reply)
                     (com/reply-error? {:eid :carmine.read/unexpected-read-error} reply))
                 reply
                 (attach-attributes reply
                   (when pending-attributes
                     (persistent! pending-attributes))))))))
       (finally (when top-level? (com/end-read! in))))))

  (defn read-reply
    "Blocks to read and complete one non-push reply from `in`. Dispatches and
    skips top-level RESP3 pushes."
    ([in] (read-reply (com/get-read-opts) in))
    ([read-opts in] (read-reply-impl read-opts in false)))

  (defn read-reply-or-push
    "Blocks to read one reply for an internal listener. Returns a top-level
    RESP3 push as `Push` without dispatching it."
    ([in] (read-reply-or-push (com/get-read-opts) in))
    ([read-opts in] (read-reply-impl read-opts in true))))

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream
      sentinel-null-reply              com/sentinel-null-reply]

  (defn complete-reply [^ReadOpts read-opts reply]
    (let [read-mode    (.-read-mode read-opts)
          skip?        (com/skip-read-mode? read-mode)
          skip-errors? (identical? read-mode :skip+errors)]
      (enc/cond
        skip?
        (cond
          (identical? reply sentinel-end-of-aggregate-stream)
          reply ; Always pass through

          (and skip-errors? (com/reply-error? reply))
          reply ; Cluster executor must observe Redis redirections/errors

          (com/reply-error? {:eid :carmine.read/unexpected-read-error} reply)
          reply ; Protocol/framing errors must invalidate the connection

          :else com/sentinel-skipped-reply)

        (and (identical? reply sentinel-null-reply)
          (com/preserve-null-reply-parser? (.-parser read-opts)))
        reply

        :if-let [^Parser p (com/when-fn-parser (.-parser read-opts))]
        (enc/cond
          (com/reply-error? reply)
          (if (and (get (.-opts p) :parse-error-replies?)
                ;; Protocol/framing errors must remain observable so that
                ;; the pipeline can invalidate the connection
                (not (com/reply-error? {:eid :carmine.read/unexpected-read-error} reply)))
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
