(ns ^:no-doc taoensso.carmine-v4.resp.read
  "Private RESP read implementation."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.nippy  :as nippy]
   [taoensso.carmine-v4.resp.common :as com])

  (:import
   [java.nio.charset StandardCharsets]
   [java.util Arrays]
   [taoensso.carmine_v4.classes AttributedReply]
   [taoensso.carmine_v4.resp.common ReadOpts ReadThawed Parser RespInputState]))

(comment (remove-ns 'taoensso.carmine-v4.resp.read))

;;;;

(declare
  ^:private read-streaming-blob

  ^:private blob->thawed
  ^:private complete-blob
  ^:private blob-marker
  ^:private complete-marked-blob
  ^:private complete-buffered-blob
  ^:private complete-blob-bytes)

(defn- invalid-length! [kind length]
  (truss/ex-info! "[Carmine] Invalid RESP length"
    {:eid :carmine.read/invalid-length
     :kind kind
     :length length}))

(defn- ascii-integer-syntax?
  "Allocation-free strict ASCII integer syntax check. RESP lengths are
  unsigned; scalar integers may have one leading sign."
  [^String s signed?]
  (let [n (.length s)
        start
        (if (and signed? (pos? n))
          (let [c (int (.charAt s 0))]
            (if (or (== c (int \+)) (== c (int \-))) 1 0))
          0)]
    (and (< start n)
      (loop [idx start]
        (if (< idx n)
          (let [c (int (.charAt s idx))]
            (if (and (<= (int \0) c) (<= c (int \9)))
              (recur (inc idx))
              false))
          true)))))

(defn- parse-length
  [kind null-allowed? size-str]
  (if (and null-allowed? (= size-str "-1"))
    -1
    (if-not (ascii-integer-syntax? size-str false)
      (invalid-length! kind size-str)
      (try
        (Integer/parseInt size-str)
        (catch NumberFormatException _
          (invalid-length! kind size-str))))))

(defn- parse-unusual-length [kind size-str] (parse-length kind false size-str))
(defn- read-length
  "Reads a RESP length line and returns the length. Returns -1 for a RESP2
  null iff `null-allowed?`, and -2 for a streamed length iff `stream-allowed?`."
  ^long [kind null-allowed? stream-allowed? state]
  (let [n (com/read-crlf-length state parse-unusual-length kind)]
    (when (< n 0)
      (if (== n -1)
        (when-not   null-allowed? (invalid-length! kind "-1"))
        (when-not stream-allowed? (invalid-length! kind "?"))))
    n))

(defn- invalid-scalar! [kind value]
  (truss/ex-info! "[Carmine] Invalid RESP scalar value"
    {:eid   :carmine.read/invalid-scalar
     :kind  kind
     :value value}))

(defn- parse-integer-scalar [s]
  (when-not (ascii-integer-syntax? s true)
    (invalid-scalar! :integer s))
  (try
    (Long/parseLong s)
    (catch NumberFormatException _
      (invalid-scalar! :integer s))))

(defn- validate-big-integer-scalar! [s]
  (when-not (ascii-integer-syntax? s true)
    (invalid-scalar! :big-integer s)))

(defn- parse-double-scalar [s]
  (enc/cond
    (= s  "inf") Double/POSITIVE_INFINITY
    (= s "-inf") Double/NEGATIVE_INFINITY

    ;; Common case, so checked before NaN
    (re-matches #"[+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?" s)
    (let [n
          (try
            (Double/parseDouble s)
            (catch NumberFormatException _
              (invalid-scalar! :double s)))]
      (if (Double/isInfinite n)
        (invalid-scalar! :double s)
        n))

    ;; RESP3 v1.4 normalized NaN to `nan` and forbade `-nan`, but Redis
    ;; <7.2 could emit libc variants and clients are expected to accept them.
    (re-matches #"(?i)-?nan(?:\([a-z0-9_]*\))?" s) Double/NaN

    :else (invalid-scalar! :double s)))

(defn- read-blob
  "$<length>\r\n<bytes>\r\n -> ?<binary safe String or other>"
  ([read-mode read-markers? issue-83-workaround? thaw-opts state]
   (read-blob :blob read-mode read-markers? issue-83-workaround? thaw-opts state))
  ([kind read-mode read-markers? issue-83-workaround? thaw-opts state]
   (enc/cond
     :let [blob? (identical? kind :blob)
           n     (read-length kind blob? blob? state)]

     (== n -2) ; Streaming
     (read-streaming-blob read-mode read-markers? issue-83-workaround? thaw-opts state)

     :do
     (when-not (neg? n)
       (com/check-blob+frame-size! state kind n kind (+ n 2)))

     (<= n 0) ; Empty or RESP2 null (-1)
     (if (== n 0)
       (do
         (com/discard-crlf state)
         (if (com/skip-read-mode? read-mode)
           com/sentinel-skipped-reply
           (complete-blob read-mode issue-83-workaround? thaw-opts (byte-array 0))))
       com/sentinel-null-reply)

     (com/skip-read-mode? read-mode) ; Skip
     ;; NB before marker handling: skipped blobs are discarded without
     ;; marker decoding or Nippy thawing
     (do
       (com/discard-bytes state n)
       (com/discard-crlf state)
       com/sentinel-skipped-reply)

     ;; Common case: blob is already in the input's buffer, use it in place
     :let [n (int n)]
     (com/buffered-blob? state n)
     (let [^RespInputState state state
           buf (.buf state)
           off (int (.pos state))
           b1  (bit-and (aget buf (+ off n))       0xff)
           b2  (bit-and (aget buf (+ off n (int 1))) 0xff)]
       (.consume state (+ n 2))
       (when-not (and (== b1 (int \return)) (== b2 (int \newline)))
         (truss/ex-info! "[Carmine] Missing CRLF"
           {:eid :carmine.read/missing-crlf
            :read [b1 b2]}))
       (if (and (identical? read-mode nil) (not (== (aget buf off) 0)))
         (String. buf off n StandardCharsets/UTF_8) ; Common case: unmarked string
         (complete-buffered-blob read-mode read-markers? issue-83-workaround?
           thaw-opts buf off n)))

     ;; Blob isn't (fully) buffered, read it into its own array. Checks for
     ;; a marker first so that large marked payloads needn't be copied.
     :let [b0 (unchecked-byte (com/read-byte! state))]

     (not (and read-markers? (>= n 2) (== b0 0))) ; Possible marker iff 1st byte null
     (let [ba (byte-array n)]
       (aset             ba 0 b0)
       (com/read-fully! state ba 1 (dec n))
       (com/discard-crlf state)
       (complete-blob read-mode issue-83-workaround? thaw-opts ba))

     :let [b1 (unchecked-byte (com/read-byte! state))] ; 2nd byte would identify marker kind
     :if-let [marker (blob-marker b1)]
     (let [payload (byte-array (- n 2))]
       (com/read-fully! state payload 0 (- n 2))
       (com/discard-crlf state)
       (complete-marked-blob read-mode marker issue-83-workaround?
         thaw-opts payload))

     :else ; Unmarked after all
     (let [ba (byte-array n)]
       (aset             ba 0 b0)
       (aset             ba 1 b1)
       (com/read-fully! state ba 2 (- n 2))
       (com/discard-crlf state)
       (complete-blob read-mode issue-83-workaround? thaw-opts ba)))))

(let [discard-stream-separator com/discard-stream-separator
      discard-crlf             com/discard-crlf]

  (defn- read-streaming-blob
    [read-mode read-markers? issue-83-workaround? thaw-opts state]

    (if (com/skip-read-mode? read-mode)

      ;; Skip
      (loop [total 0]
        (discard-stream-separator state)
        (let [n (read-length :streaming-blob-chunk false false state)]
          (if (== n 0)
            ;; Stream complete
            com/sentinel-skipped-reply

            ;; Stream continues
            (let [total (long (+ total n))]
              (com/check-blob+frame-size!
                state :streaming-blob total :streaming-blob-chunk (+ n 2))
              (com/discard-bytes state n)
              (discard-crlf state)
              (recur total)))))

      ;; Don't skip
      ;; Even if the final output is a String, it's faster
      ;; to accumulate to BAOS then transform to a String at the
      ;; end rather than use a StringBuffer.
      (let [baos (java.io.ByteArrayOutputStream. 128)]
        (loop [total 0]
          (discard-stream-separator state)
          (let [n (read-length :streaming-blob-chunk false false state)]
            (if (== n 0)

              ;; Stream complete
              (complete-blob-bytes read-mode read-markers?
                issue-83-workaround? thaw-opts (.toByteArray baos))

              ;; Stream continues
              (let [total (long (+ total n))
                    _ (com/check-blob+frame-size!
                        state :streaming-blob total :streaming-blob-chunk (+ n 2))
                    ba (byte-array n)]
                (com/read-fully! state ba 0 n)
                (discard-crlf state)
                (.write baos ba 0 (alength ba))
                (recur total)))))))))

(defn- possible-nippy-payload?
  "Returns true iff a `ba-bin` marked payload looks like Nippy output: Nippy's
  3 byte header + 1 byte metadata + data."
  [^bytes ?ba]
  (and ?ba
    (>= (alength ?ba) 5)
    (== (aget ?ba 0) #=(int \N))
    (== (aget ?ba 1) #=(int \P))
    (== (aget ?ba 2) #=(int \Y))))

;;;; Read-mode handling

(defn- redact-thaw-opts
  "Returns `thaw-opts` without secret values, for error data."
  [thaw-opts]
  (if (and (map? thaw-opts) (contains? thaw-opts :password))
    (assoc thaw-opts :password :redacted)
    thaw-opts))

(defn- blob->thawed
  "Thaws `ba` with the captured effective `thaw-opts`. Calls `nippy/thaw`
  directly so reply decoding never reads `taoensso.nippy.tools/*thaw-opts*`."
  [thaw-opts issue-83-workaround? ba]
  (try
    (nippy/thaw ba thaw-opts)
    (catch Throwable t
      ;; NB the Nippy cause `t` may still include the thaw opts in its data
      (com/reply-error
        "[Carmine] Nippy threw an error while thawing blob reply"
        (enc/assoc-when
          {:eid :carmine.read/nippy-thaw-error
           :thaw-opts (redact-thaw-opts thaw-opts)
           :bytes {:length (count ba) :content ba}}
          :possible-non-nippy-bytes? issue-83-workaround?)
        t))))

(defn- complete-blob [read-mode issue-83-workaround? thaw-opts ba]
  (enc/cond!
    (identical? read-mode    nil) (enc/utf8-ba->str ba) ; Common case
    (identical? read-mode :bytes)                   ba

    ;; Shouldn't be here at all in this case
    ;; (identical? read-mode :skip) com/sentinel-skipped-reply

    ;; Forced thaw of unmarked data
    (instance? ReadThawed read-mode)
    (blob->thawed thaw-opts issue-83-workaround? ba)))

(defn- blob-marker
  "Returns the kind of marker identified by the 2nd byte of a blob that
  starts with a null byte: `:nil`, `:npy`, `:bin`, or nil."
  [b1]
  (enc/case-eval (int b1)
    (int \_) :nil ; ba-nil
    (int \>) :npy ; ba-npy
    (int \<) :bin ; ba-bin
    nil))

(defn- complete-marked-blob
  "Completes a marked blob, given its marker kind and its `payload` bytes
  (without marker). May return `payload` itself."
  [read-mode marker issue-83-workaround? thaw-opts ^bytes payload]
  (enc/cond
    (identical? marker :nil) nil
    (or
      (identical? read-mode :bytes)
      (and
        (identical? marker :bin)
        (not (and issue-83-workaround? (possible-nippy-payload? payload)))))
    payload

    :else
    (blob->thawed thaw-opts issue-83-workaround?
      (when (pos? (alength payload)) payload)))) ; Nil payload when empty

(defn- complete-buffered-blob
  "Completes the blob in the `n` bytes at `off` of `buf`, copying whatever
  it keeps. So `buf` may be an input buffer that'll be reused."
  [read-mode read-markers? issue-83-workaround? thaw-opts ^bytes buf off n]
  (enc/cond
    :let [off (int off)
          n   (int n)
          marker
          (when (and read-markers? (>= n 2) (== (aget buf off) 0))
            (blob-marker (aget buf (inc off))))]

    (nil? marker)
    (if (identical? read-mode nil) ; Common case
      (String. buf off n StandardCharsets/UTF_8)
      (complete-blob read-mode issue-83-workaround? thaw-opts
        (Arrays/copyOfRange buf off (+ off n))))

    (identical? marker :nil) nil
    :else
    (complete-marked-blob read-mode marker issue-83-workaround? thaw-opts
      (Arrays/copyOfRange buf (+ off 2) (+ off n)))))

(defn- complete-blob-bytes
  "Completes the blob that fills given byte array, which won't be modified
  later. May return the array itself."
  [read-mode read-markers? issue-83-workaround? thaw-opts ^bytes ba]
  (let [n (alength ba)]
    (if (and read-markers? (>= n 2) (== (aget ba 0) 0)) ; Possible marker
      (complete-buffered-blob read-mode true issue-83-workaround? thaw-opts ba 0 n)
      (complete-blob          read-mode      issue-83-workaround? thaw-opts ba))))

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
            issue-83-workaround? (.-thaw-opts read-opts) ba))]
    ;; Byte mode may otherwise expose the mutable CAS token to the caller.
    (if (identical? decoded ba)
      (Arrays/copyOf ba (alength ba))
      decoded)))

;;;; Aggregates

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream]
  (defn- read-aggregate-element
    [read-reply read-opts state]
    (let [x (read-reply read-opts state)]
      (if (com/reply-error? {:eid :carmine.read/unexpected-read-error} x)
        (throw x)
        x)))

  (defn- read-required-aggregate-element
    [read-reply read-opts state]
    (let [x (read-aggregate-element read-reply read-opts state)]
      (if (identical? x sentinel-end-of-aggregate-stream)
        (truss/ex-info! "[Carmine] Unexpected end of RESP aggregate stream"
          {:eid :carmine.read/unexpected-aggregate-end})
        x))))

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream]
  (defn- read-aggregate-by-ones*
    [kind to ^ReadOpts read-opts read-reply state]
    (let [n (read-length kind (= kind :array) (not= kind :push) state)
          inner-read-opts (com/in-aggregate-read-opts read-opts)
          skip? (com/skip-read-mode? (.-read-mode read-opts))]

      (if (== n -2)

        ;; Streaming
        (enc/cond
          skip?
          (loop [n 0]
            (let [x (read-aggregate-element read-reply inner-read-opts state)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                com/sentinel-skipped-reply
                (let [n (inc n)]
                  (com/check-aggregate-size! state kind n)
                  (recur n)))))

          ;; Reducing parser
          :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
          (let [rf ((.rfc p))
                init-acc (rf)]
            (loop [n 0, acc init-acc]
              (let [x (read-aggregate-element read-reply inner-read-opts state)]
                (if (identical? x sentinel-end-of-aggregate-stream)
                  (rf (unreduced acc)) ; Complete acc
                  (let [n (inc n)]
                    (com/check-aggregate-size! state kind n)
                    (recur n (if (reduced? acc) acc (rf acc x))))))))

          :default
          (loop [n 0, acc (transient (empty to))]
            (let [x (read-aggregate-element read-reply inner-read-opts state)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                (persistent!  acc)
                (let [n (inc n)]
                  (com/check-aggregate-size! state kind n)
                  (recur n (conj! acc x)))))))

        ;; Not streaming
        (do
          (when (and (= kind :push) (zero? n))
            (invalid-length! kind "0"))
          (when-not (neg? n) (com/check-aggregate-size! state kind n))
          (enc/cond
            (< n 0) com/sentinel-null-reply
            skip?
            (do
              (enc/reduce-n
                (fn [_ _] (read-required-aggregate-element read-reply inner-read-opts state))
                0 n)
              com/sentinel-skipped-reply)

            ;; Reducing parser
            :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
            (let [rf ((.-rfc p))
                  init-acc (rf)]
              (loop [remaining n, acc init-acc]
                (if (pos? remaining)
                  (let [x (read-required-aggregate-element read-reply inner-read-opts state)]
                    (recur (dec remaining) (if (reduced? acc) acc (rf acc x))))
                  (rf (unreduced acc))))) ; Complete acc

            (== n 0) to
            :default
            (enc/repeatedly-into to n
              #(read-required-aggregate-element read-reply inner-read-opts state)))))))

  (defn- read-aggregate-by-ones
    [kind to read-opts read-reply state]
    (com/with-aggregate state kind
      #(read-aggregate-by-ones* kind to read-opts read-reply state))))

(let [keywordize (fn [x] (if (string? x) (keyword x) x))
      sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream]

  (defn- read-aggregate-by-pairs*
    "Reads aggregates as pairs. Optimized for maps."
    [kind ^ReadOpts read-opts read-reply state]
    (let [n (read-length kind false (= kind :map) state)
          inner-read-opts (com/in-aggregate-read-opts read-opts)
          skip? (com/skip-read-mode? (.-read-mode read-opts))]

      (if (== n -2)

        ;; Streaming
        (enc/cond
          skip?
          (loop [n 0]
            (let [x (read-aggregate-element read-reply inner-read-opts state)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                com/sentinel-skipped-reply
                (let [n  (inc n)
                      _  (com/check-aggregate-size! state kind n)
                      _k x
                      _v (read-required-aggregate-element read-reply inner-read-opts state)]
                  (recur n)))))

          ;; Reducing parser
          :if-let [^Parser p (com/when-rf-parser (.-parser read-opts))]
          (let [rf    ((.-rfc    p))
                kv-rf? (.-kv-rf? p)
                init-acc (rf)]

            (loop [n 0, acc init-acc]
              (let [x (read-aggregate-element read-reply inner-read-opts state)]
                (if (identical? x sentinel-end-of-aggregate-stream)
                  (rf (unreduced acc)) ; Complete acc
                  (let [n (inc n)
                        _ (com/check-aggregate-size! state kind n)
                        k x ; Without kfn!
                        v (read-required-aggregate-element read-reply inner-read-opts state)]
                    (recur n
                      (if (reduced? acc)
                        acc
                        (if kv-rf?
                          (rf acc                               k v)
                          (rf acc (clojure.lang.MapEntry/create k v))))))))))

          :let [kfn (if (.-keywordize-maps? read-opts) keywordize identity)]
          :default
          (loop [n 0, acc (transient {})]
            (let [x (read-aggregate-element read-reply inner-read-opts state)]
              (if (identical? x sentinel-end-of-aggregate-stream)
                (persistent! acc)
                (let [n (inc n)
                      _ (com/check-aggregate-size! state kind n)
                      k (kfn x)
                      v (read-required-aggregate-element read-reply inner-read-opts state)]
                  (recur n (assoc! acc k v)))))))

        ;; Not streaming
        (do
          (when-not (neg? n) (com/check-aggregate-size! state kind n))
          (enc/cond
            (< n 0) com/sentinel-null-reply
            skip?
            (do
              (enc/reduce-n
                (fn [_ _]
                  (let [_k (read-required-aggregate-element read-reply inner-read-opts state)
                        _v (read-required-aggregate-element read-reply inner-read-opts state)]
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
                  (let [k (read-required-aggregate-element read-reply inner-read-opts state) ; Without kfn!
                        v (read-required-aggregate-element read-reply inner-read-opts state)]
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
                    (let [k (kfn (read-required-aggregate-element read-reply inner-read-opts state))
                          v      (read-required-aggregate-element read-reply inner-read-opts state)]
                      (assoc! m k v)))
                  (transient {})
                  n))

              (enc/reduce-n
                (fn [m _]
                  (let [k (kfn (read-required-aggregate-element read-reply inner-read-opts state))
                        v      (read-required-aggregate-element read-reply inner-read-opts state)]
                    (assoc m k v)))
                {}
                n)))))))

  (defn- read-aggregate-by-pairs
    [kind read-opts read-reply state]
    (com/with-aggregate state kind
      #(read-aggregate-by-pairs* kind read-opts read-reply state))))

(defn- redis-reply-error [?message]
  (let [^String message (if (nil? ?message) "" ?message)
        code (re-find #"^\S+" message)] ; "ERR", "WRONGTYPE", etc.

    (com/reply-error (str "[Carmine] Redis replied with an error: " message)
      {:eid :carmine.read/redis-error-reply
       :message message
       :code    code})))

(comment (redis-reply-error "ERR Foo bar"))

(declare complete-reply read-reply-impl read-common-reply)

(defrecord VerbatimString [format content])
(defrecord WithAttributes [content attributes]
  AttributedReply
  (replyContent     [_] content)
  (replyAttributes [_] attributes))
(defrecord Push [data])
(deftype ^:private PushReply [data])
(deftype ^:private AttributeReply [attributes])

(defn- read-nested-reply
  "Reads and completes one reply nested in an aggregate."
  [^ReadOpts read-opts state]
  (let [kind-b (com/read-byte state)]
    (if (and
          (or (== kind-b (int \$)) (== kind-b (int \+)) (== kind-b (int \:)) (== kind-b (int \_)))
          (nil? (.-parser read-opts))
          (not (com/skip-read-mode? (.-read-mode read-opts))))

      ;; Fast path for the most common case, equivalent to `read-reply-impl`
      (let [reply (read-common-reply read-opts state kind-b)]
        (if (identical? reply com/sentinel-null-reply)
          nil
          reply))

      (read-reply-impl read-opts false false state kind-b))))

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

  (defn- read-error
    "Returns or throws an appropriate error for `t` thrown while reading a
    reply of the given kind."
    [read-opts kind-b t]
    (cond
      (instance? InterruptedException t)
      (do
        (.interrupt (Thread/currentThread))
        (throw t))

      (com/resource-error? t) (throw t)
      (com/reply-error? {:eid :carmine.read/unexpected-read-error} t) t
      :else
      (com/reply-error
        (if (instance? java.io.IOException t)
          "[Carmine] Connection error while reading reply"
          "[Carmine] Unexpected reply error")
        (enc/assoc-some
          {:eid :carmine.read/unexpected-read-error
           :read-opts (com/describe-read-opts read-opts)}
          :kind ; Nil at end of stream
          (when-not (== (long kind-b) -1)
            {:as-byte kind-b :as-char (char (bit-and (int kind-b) 0xff))}))
        t)))

  (defn- read-reply-by-kind
    "Reads one reply of the given kind (first byte), without completing it.
    A separate fn so that the `try` is in tail position: otherwise it compiles
    to a closure that's allocated and called for every (nested) reply."
    [^ReadOpts read-opts state kind-b skip? top-level?
     return-top-level-pushes? pending-attributes]
    (truss/try*
      (enc/case-eval (long kind-b)
        ;; --- RESP2 ⊂ RESP3 ---------------------------------------------------
        ;; Blobs, simple strings and simple longs: see `read-common-reply`

        (int \-) ; Simple error ✓
        (let [s (com/read-crlf-line state)]
          (redis-reply-error s))

        (int \*) ; Aggregate array ✓
        (read-aggregate-by-ones :array [] read-opts read-nested-reply state)

        ;; --- RESP3 ∖ RESP2 ---------------------------------------------------
        (int \.)
        (do
          (com/discard-crlf state)
          (if (or top-level? pending-attributes)
            (invalid-scalar! :aggregate-end ".")
            sentinel-end-of-aggregate-stream))

        (int \#) ; Bool ✓
        (let [b (unchecked-byte (com/read-byte! state))]
          (com/discard-crlf state)
          (case b
            #=(int \t) (when-not skip? true)
            #=(int \f) (when-not skip? false)
            (invalid-scalar! :boolean b)))

        (int \!) ; Blob error ✓
        (redis-reply-error (read-blob :blob-error nil false false nil state))

        (int \=) ; Verbatim string ✓
        (let [^bytes ba (read-blob :verbatim-string :bytes false false nil state)
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
        (let [s (com/read-crlf-line state)]
          (let [n (parse-double-scalar s)]
            (when-not skip? n)))

        (int \() ; Big integer ✓
        (let [s (com/read-crlf-line state)]
          (validate-big-integer-scalar! s)
          (when-not skip? (bigint (BigInteger. ^String s))))

        (int \~) (read-aggregate-by-ones  :set #{} read-opts read-nested-reply state) ; ✓
        (int \%) (read-aggregate-by-pairs :map     read-opts read-nested-reply state) ; ✓

        (int \|) ; Attribute map ✓
        (AttributeReply.
          ;; Attrs are read parser-free. The loop accumulates
          ;; consecutive maps before completing their content once.
          (com/with-aggregate state :attributes
            (fn []
              (read-aggregate-by-pairs* :attributes
                (if skip?
                  com/read-opts-natural
                  (com/in-aggregate-read-opts read-opts))
                read-nested-reply state))))

        (int \>) ; Push ✓
        (do
          (when-not top-level?
            (invalid-scalar! :push :nested))
          (let [v
                (com/with-aggregate state :push
                  #(read-aggregate-by-ones*
                     :push []
                     (if (and return-top-level-pushes? (not skip?))
                       (com/in-aggregate-read-opts read-opts)
                       com/read-opts-natural)
                     read-nested-reply state))
                push-kind (com/reply-content (first v))]
            (when-not (or (string? push-kind) (enc/bytes? push-kind))
              (invalid-scalar! :push-kind push-kind))
            (PushReply. v)))

        (if (== kind-b -1)
          (throw (java.io.EOFException. "[Carmine] Connection closed by server"))
          (truss/ex-info! "[Carmine] Unexpected reply kind"
            {:eid :carmine.read/unexpected-reply-kind
             :read-opts (com/describe-read-opts read-opts)
             :kind {:as-byte kind-b :as-char (char (bit-and (int kind-b) 0xff))}})))

      (catch InterruptedException t (read-error read-opts kind-b t))
      (catch :default             t (read-error read-opts kind-b t))))

  (defn- read-common-reply
    "Like `read-reply-by-kind` for the most common kinds. A small fn with a
    primitive kind is much faster here than dispatch through the large
    general one."
    [^ReadOpts read-opts state ^long kind-b]
    (truss/try*
      (enc/case-eval kind-b
        (int \$) ; Blob (nil/string/bytes/thawed) ✓
        (read-blob (.-read-mode read-opts) (.-auto-thaw? read-opts)
          (.-issue-83-workaround? read-opts) (.-thaw-opts read-opts) state)

        (int \_) (do (com/discard-crlf state) sentinel-null-reply)
        (int \+) (com/read-crlf-line state) ; Simple string ✓
        (int \:) ; Simple long ✓
        (let [n (com/read-crlf-integer state parse-integer-scalar)]
          (when-not (com/skip-read-mode? (.-read-mode read-opts)) n)))

      (catch InterruptedException t (read-error read-opts kind-b t))
      (catch :default             t (read-error read-opts kind-b t))))

  (defn- read-reply-impl
    ([read-opts in return-top-level-pushes?]
     (read-reply-impl read-opts return-top-level-pushes? true (com/input-state in) nil))
    ([^ReadOpts read-opts return-top-level-pushes? top-level? state ?kind-b]
     ;; `?kind-b` is the reply's first byte when the caller has already read it
     ;; Since dynamic vars are ephemeral and reply reading is lazy, neither this
     ;; fn nor any of its children should use dynamic vars. Instead, we'll capture
     ;; dynamic config to `com/ReadOpts` at the appropriate time.
     (when top-level? (com/begin-read! state))
     (try
       (loop [kind-b (if ?kind-b (long ?kind-b) (com/read-byte state))
              pending-attributes nil]
         (let [read-mode (.-read-mode read-opts)
               skip?   (com/skip-read-mode? read-mode)

               reply
               (if (or (== kind-b (int \$)) (== kind-b (int \+)) (== kind-b (int \:)) (== kind-b (int \_)))
                 (read-common-reply  read-opts state kind-b)
                 (read-reply-by-kind read-opts state kind-b skip? top-level?
                   return-top-level-pushes? pending-attributes))]

           (cond
             ;; Fast path for the most common case, equivalent to `:else`
             (and (string? reply) (nil? pending-attributes) (not skip?)
               (nil? (.-parser read-opts)))
             reply

             (instance? AttributeReply reply)
             (let [attributes (.-attributes ^AttributeReply reply)]
               (recur (com/read-byte state) (merge-attributes! pending-attributes attributes)))

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
                   (com/next-frame! state)
                   (recur (com/read-byte state) nil))))

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
       (finally (when top-level? (com/end-read! state))))))

  (defn read-reply
    "Blocks to read and complete one non-push reply from `in`, a `resp-input`.
    Dispatches and skips top-level RESP3 pushes."
    ([in] (read-reply (com/get-read-opts) in))
    ([read-opts in] (read-reply-impl read-opts in false)))

  (defn read-reply-from-state
    "Like `read-reply` but takes `(com/input-state in)`, which pipelines can
    look up once."
    [read-opts input-state]
    (read-reply-impl read-opts false true input-state nil))

  (defn read-reply-or-push
    "Blocks to read one reply for an internal listener. Returns a top-level
    RESP3 push as `Push` without dispatching it."
    ([in] (read-reply-or-push (com/get-read-opts) in))
    ([read-opts in] (read-reply-impl read-opts in true))))

(let [sentinel-end-of-aggregate-stream com/sentinel-end-of-aggregate-stream
      sentinel-null-reply              com/sentinel-null-reply]

  (defn complete-reply [^ReadOpts read-opts reply]
    (let [skip? (com/skip-read-mode? (.-read-mode read-opts))]
      (enc/cond
        skip?
        (cond
          (identical? reply sentinel-end-of-aggregate-stream)
          reply ; Always pass through

          (com/reply-error? reply)
          reply ; Incl. protocol/framing errors and Cluster redirections

          :else com/sentinel-skipped-reply)

        :if-let [^Parser p (com/when-fn-parser (.-parser read-opts))]
        (enc/cond
          (com/reply-error? reply) reply
          (identical? reply sentinel-null-reply) ((.-f p) nil)
          :default
          ((.-f p) reply))

        :default
        (if (identical? reply sentinel-null-reply)
          nil
          reply)))))
