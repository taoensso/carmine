(ns ^:no-doc taoensso.carmine-v4.resp.common
  "Private implementation namespace."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.carmine-v4.classes])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInputStream InputStream]
   [clojure.lang ExceptionInfo]

   [taoensso.carmine_v4.classes
    AttributedReply ReplyError ReusableConnError RespResourceError RespInput]))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-thaw?*
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*
  ^:dynamic taoensso.carmine-v4/*raw-verbatim-strings?*
  ^:dynamic taoensso.carmine-v4/*issue-83-workaround?*)

(alias 'core 'taoensso.carmine-v4)

(comment (remove-ns 'taoensso.carmine-v4.resp.common))

;;;; Utils

(def ba-crlf (enc/str->utf8-ba "\r\n"))

;; `counters`: frame bytes, read depth, aggregate depth, marked frame bytes,
;; mark active?; `objects[0]`: the first resource-limit error, when poisoned.
(deftype RespLimitState [limits ^longs counters ^objects objects])

(defn- resource-error
  [msg data cause]
  (if cause
    (proxy [ExceptionInfo RespResourceError] [msg data cause])
    (proxy [ExceptionInfo RespResourceError] [msg data])))

(defn resource-error? [x] (instance? RespResourceError x))

(defn- state-resource-limit!
  [^RespLimitState state limit max actual data]
  (let [error
        (resource-error "[Carmine] RESP resource limit exceeded"
          (merge
            {:eid    :carmine.read/resource-limit
             :limit  limit
             :max    max
             :actual actual}
            data)
          nil)]
    (aset ^objects (.-objects state) 0 error)
    (throw error)))

(defn- count-frame-bytes!
  ;; `max` hoisted to a primitive at stream construction: this runs on the
  ;; per-read hot path and must not re-fetch the limit from the options map.
  [^RespLimitState state ^long max n]
  (let [^longs counters (.-counters state)
        actual (+ (aget counters 0) ^long n)]
    (aset counters 0 actual)
    (when (> actual max)
      (state-resource-limit! state :max-frame-bytes max actual nil))))

(defn resp-input
  "Returns a buffered `DataInputStream` with connection-level RESP limits."
  ^DataInputStream [^InputStream raw-in init-buffer-size limits]
  (let [buffered (java.io.BufferedInputStream. raw-in (int init-buffer-size))
        state    (RespLimitState. limits (long-array 5) (object-array 1))
        frame-limited?
        (some? (get limits :max-frame-bytes))

        counted
        (if-not frame-limited?
          buffered
          (let [max-frame-bytes (long (get limits :max-frame-bytes))]
            (proxy [InputStream] []
            (read
              ([]
               (let [b (.read buffered)]
                 (when-not (== b -1) (count-frame-bytes! state max-frame-bytes 1))
                 b))
              ([ba]
               (let [n (.read buffered ^bytes ba)]
                 (when (pos? n) (count-frame-bytes! state max-frame-bytes n))
                 n))
              ([ba off len]
               (let [n (.read buffered ^bytes ba (int off) (int len))]
                 (when (pos? n) (count-frame-bytes! state max-frame-bytes n))
                 n)))
            (skip [n]
              (let [n-skipped (.skip buffered (long n))]
                (when (pos? n-skipped) (count-frame-bytes! state max-frame-bytes n-skipped))
                n-skipped))
            (available [] (.available buffered))
            (close     [] (.close buffered))
            (mark      [read-limit]
              (.mark buffered (int read-limit))
              (let [^longs counters (.-counters state)]
                (aset counters 3 (aget counters 0))
                (aset counters 4 1)))
            (reset []
              (.reset buffered)
              (let [^longs counters (.-counters state)]
                (when (== (aget counters 4) 1)
                  (aset counters 0 (aget counters 3)))))
            (markSupported [] true))))]

    (proxy [DataInputStream RespInput] [counted]
      (respLimitState [] state))))

(defn ba->in
  (^DataInputStream [^bytes ba]
   (-> ba
     java.io.ByteArrayInputStream.
     java.io.BufferedInputStream.
     DataInputStream.))
  (^DataInputStream [^bytes ba limits]
   (resp-input (java.io.ByteArrayInputStream. ba) 8192 limits)))

(defn str->in ^DataInputStream [^String s] (ba->in (.getBytes s StandardCharsets/UTF_8)))

(defn str->limited-in ^DataInputStream [^String s limits]
  (ba->in (.getBytes s StandardCharsets/UTF_8) limits))

(defmacro with-out
  "Returns the bytes that `body` writes to `out`."
  [& body]
  `(let [baos# (java.io.ByteArrayOutputStream.)
         ~'out (java.io.BufferedOutputStream. baos#)]
     (do ~@body)
     (.flush       ~'out)
     (.toByteArray baos#)))

(defmacro with-out->str [& body] `(enc/utf8-ba->str (with-out ~@body)))

(defn xseq->ba ^bytes [with-crlfs? xseq]
  (with-out
    (doseq [x xseq]
      (enc/cond!
        (enc/bytes? x) (.write out                 ^bytes x)
        (string?    x) (.write out (enc/str->utf8-ba      x))
        (int?       x) (.write out (enc/str->utf8-ba (str x)))
        (char?      x) (.write out                   (int x))
        (vector?    x) (.write out (byte-array (mapv byte x))))

      (when with-crlfs?
        (.write out ^bytes ba-crlf)))))

(do ; Variations useful for tests, etc.
  (defn xs->in+ ^DataInputStream [& xs] (ba->in (xseq->ba true  xs)))
  (defn xs->in  ^DataInputStream [& xs] (ba->in (xseq->ba false xs)))
  (defn xs->ba+           ^bytes [& xs]         (xseq->ba true  xs))
  (defn xs->ba            ^bytes [& xs]         (xseq->ba false xs))
  (defn xs->str          ^String [& xs] (enc/utf8-ba->str (xseq->ba false xs)))
  (defn xs->str+         ^String [& xs] (enc/utf8-ba->str (xseq->ba true  xs))))

(defn read-crlf-line
  "Reads and returns a CRLF-terminated line without its terminator.
  Decodes bytes as UTF-8 to match blob reads and all writes. Measures limits in
  bytes."
  [^DataInputStream in]
  (let [^RespLimitState state
        (when (instance? RespInput in)
          (.respLimitState ^RespInput in))
        max (when state (get (.-limits state) :max-line-bytes))
        utf8-str (fn [^bytes ba n] (String. ba 0 (int n) StandardCharsets/UTF_8))]
    (loop [^bytes ba (byte-array 32), n 0]
      (let [b (.read in)]
        (cond
          (== b -1)
          (truss/ex-info! "[Carmine] Missing CRLF"
            {:eid :carmine.read/missing-crlf
             :read (utf8-str ba n)})

          (== b (int \return))
          (let [next-b (.read in)]
            (if (== next-b (int \newline))
              (utf8-str ba n)
              (truss/ex-info! "[Carmine] Missing CRLF"
                {:eid :carmine.read/missing-crlf
                 :read (str (utf8-str ba n) \return
                         (when-not (== next-b -1) (char next-b)))})))

          (== b (int \newline))
          (truss/ex-info! "[Carmine] Missing CRLF"
            {:eid :carmine.read/missing-crlf
             :read (str (utf8-str ba n) \newline)})

          :else
          (let [actual (inc n)]
            (when (and max (> actual ^long max))
              (state-resource-limit! state :max-line-bytes max actual nil))
            (let [^bytes ba
                  (if (< n (alength ba))
                    ba
                    (java.util.Arrays/copyOf ba (* 2 (alength ba))))]
              (aset ba n (unchecked-byte b))
              (recur ba actual))))))))

;;;; Resource limits

(defn- get-limit-state ^RespLimitState [in]
  (when (instance? RespInput in)
    (.respLimitState ^RespInput in)))

(defn begin-read!
  "Starts a possibly recursive read. Resets state for a top-level frame."
  [in]
  (when-let [^RespLimitState state (get-limit-state in)]
    (let [^longs counters (.-counters state)
          ^objects objects (.-objects state)]
      (when-let [cause (aget objects 0)]
        (throw
          (resource-error "[Carmine] Cannot read from poisoned RESP input"
            {:eid :carmine.read/poisoned
             :cause-data (ex-data cause)}
            cause)))
      (let [top-level? (zero? (aget counters 1))]
        (when top-level?
          (aset counters 0 0) ; Frame bytes
          (aset counters 2 0) ; Aggregate nesting
          (aset counters 4 0))
        (aset counters 1 (inc (aget counters 1)))))))

(defn end-read! [in]
  (when-let [^RespLimitState state (get-limit-state in)]
    (let [^longs counters (.-counters state)]
      (aset counters 1 (dec (aget counters 1))))))

(defn next-frame! [in]
  (when-let [^RespLimitState state (get-limit-state in)]
    (let [^longs counters (.-counters state)]
      (aset counters 0 0)
      (aset counters 4 0))))

(defn with-aggregate
  [in kind f]
  (if-let [^RespLimitState state (get-limit-state in)]
    (let [^longs counters (.-counters state)
          actual (inc (aget counters 2))
          max    (get (.-limits state) :max-nesting-depth)]
      (when (and max (> actual ^long max))
        (state-resource-limit! state :max-nesting-depth max actual {:kind kind}))
      (aset counters 2 actual)
      (try (f) (finally (aset counters 2 (dec (aget counters 2))))))
    (f)))

(defn check-blob-size! [in kind actual]
  (when-let [^RespLimitState state (get-limit-state in)]
    (when-let [max (get (.-limits state) :max-blob-bytes)]
      (when (> ^long actual ^long max)
        (state-resource-limit! state :max-blob-bytes max actual {:kind kind})))))

(defn check-aggregate-size! [in kind actual]
  (when-let [^RespLimitState state (get-limit-state in)]
    (when-let [max (get (.-limits state) :max-aggregate-elements)]
      (when (> ^long actual ^long max)
        (state-resource-limit! state :max-aggregate-elements max actual {:kind kind})))))

(defn check-frame-additional! [in kind additional]
  (when-let [^RespLimitState state (get-limit-state in)]
    (when-let [max (get (.-limits state) :max-frame-bytes)]
      (let [actual (+ (aget ^longs (.-counters state) 0) ^long additional)]
        (when (> actual ^long max)
          (state-resource-limit! state :max-frame-bytes max actual {:kind kind}))))))

;;;; Blob markers

(do
  (def ba-npy (enc/str->utf8-ba "\u0000>"))
  (def ba-bin (enc/str->utf8-ba "\u0000<"))
  (def ba-nil (enc/str->utf8-ba "\u0000_")))

(defn read-blob-?marker
  "Returns nil, `:nil`, `:bin`, or `:npy`. May consume a blob marker such as
  [[ba-npy]] from the stream. Call only when the captured auto-thaw read option
  from [[taoensso.carmine-v4/*auto-thaw?*]] is true. `issue-83-workaround?` is
  a captured value, not a dynamic lookup. See
  [[taoensso.carmine-v4/*issue-83-workaround?*]]."
  [^DataInputStream in ^long n issue-83-workaround?]
  (when (>= n 2) ; >= 2 for marker+?payload
    (.mark in 2)
    (if-not (== (.readByte in) 0) ; Possible marker iff 1st byte null
      (do (.reset in) nil)
      (enc/case-eval (.readByte in) ; 2nd byte would identify marker kind
        (int \_) :nil ; ba-nil
        (int \>) :npy ; ba-npy
        (int \<)      ; ba-bin
        (enc/cond
          (not issue-83-workaround?) :bin
          (< n 7)                           :bin ; >= +5 for Nippy payload (4 header + data)
          :do (.mark in 3)
          (not (== (.readByte in) #=(int \N))) (do (.reset in) :bin)
          (not (== (.readByte in) #=(int \P))) (do (.reset in) :bin)
          (not (== (.readByte in) #=(int \Y))) (do (.reset in) :bin)
          :else                                (do (.reset in) :npy))

        ;; :else
        (do (.reset in) nil)))))

;; TODO Add `parse-?marked-ba` -> [<kind> <payload>] user util

;;;; Errors

(defn ^:public reply-content
  "Returns the content of an attributed RESP3 reply. Returns `reply` unchanged
  if it has no wrapper. A metadata-backed reply is its own content."
  [reply]
  (if (instance? AttributedReply reply)
    (.replyContent ^AttributedReply reply)
    reply))

(defn ^:public reply-attributes
  "Returns the RESP3 attribute map associated with `reply`, or nil.

  Carmine stores attributes under a dedicated metadata key
  (`:carmine/reply-attributes`) when the reply value supports metadata, and
  otherwise uses an attributed-reply wrapper. This function reports only that
  key, not ordinary reply metadata (e.g. metadata added by a reply parser)."
  [reply]
  (if (instance? AttributedReply reply)
    (.replyAttributes ^AttributedReply reply)
    (get (meta reply) :carmine/reply-attributes)))

(defn reply-error
  "Returns an `ExceptionInfo` instance that implements `ReplyError`. Use this
  type to distinguish reply errors generated by Carmine or Redis from serialized
  user errors."
  ([msg data cause] (proxy [ExceptionInfo ReplyError] [msg data cause]))
  ([msg data      ] (proxy [ExceptionInfo ReplyError] [msg data]))
  ([ex]
   (if-let [cause (ex-cause ex)]
     (proxy [ExceptionInfo ReplyError] [(ex-message ex) (or (ex-data ex) {}) cause])
     (proxy [ExceptionInfo ReplyError] [(ex-message ex) (or (ex-data ex) {})]))))

(defn drained-reply-errors
  "Returns a reply error for a fully consumed pipeline. After this error, a
  manager may safely reuse the connection if it is still open."
  [replies error-indexes]
  (let [cause (reply-content (nth replies (first error-indexes)))
        cause (when (instance? Throwable cause) cause)]
    (proxy [ExceptionInfo ReplyError ReusableConnError]
      ["[Carmine] Redis replied with one or more errors"
       {:eid :carmine.read/drained-reply-errors
        :replies replies
        :error-indexes error-indexes}
       cause])))

(comment
  (instance? ExceptionInfo (reply-error "msg" {}))
  (instance? ReplyError    (reply-error "msg" {})))

(defn ^:public reply-error?
  "Returns true iff the given `x` is a Carmine `ExceptionInfo` for a reply
  error, or an attributed RESP3 reply that contains one.

  Use this function to distinguish reply errors generated by Carmine or Redis
  from errors serialized as user data.

  If you provide `ex-data-submap`, it must be a submap of the exception data:
    (reply-error? {:eid :carmine.read/parser-error} my-error)"
  ([               x]
   (loop [x x]
     (if (instance? AttributedReply x)
       (recur (.replyContent ^AttributedReply x))
       (instance? ReplyError x))))
  ([ex-data-submap x]
   (loop [x x]
     (if (instance? AttributedReply x)
       (recur (.replyContent ^AttributedReply x))
       (and
         (instance? ReplyError x)
         (enc/submap? (ex-data x) ex-data-submap))))))

;;;; Stream discards

(defn discard-bytes
  "Discards exactly `n` bytes, even when the stream's `skip` makes no progress."
  [^DataInputStream in n]
  (loop [remaining (long n)]
    (when (pos? remaining)
      (let [skipped (.skip in remaining)]
        (if (pos? skipped)
          (recur (- remaining skipped))
          (if (== (.read in) -1)
            (truss/ex-info! "[Carmine] Unexpected end of RESP blob"
              {:eid :carmine.read/unexpected-blob-end
               :remaining remaining})
            (recur (dec remaining)))))))
  true)

(let [ref-b (int \;)]
  (defn discard-stream-separator
    [^DataInputStream in]
    ;; (.skip 1)
    (let [read-b (.readByte in)] ; Throws on EOF
      (if (== ref-b read-b)
        true
        (truss/ex-info! "[Carmine] Missing stream separator"
          {:eid :carmine.read/missing-stream-separator
           :read {:as-byte read-b :as-char (char (bit-and (int read-b) 0xff))}})))))

(defn discard-crlf
  [^DataInputStream in]
  (let [b1 (.read in)
        b2 (.read in)]
    (if (and (== b1 (int \return)) (== b2 (int \newline)))
      true
      (truss/ex-info! "[Carmine] Missing CRLF"
        {:eid :carmine.read/missing-crlf
         :read [b1 b2]}))))

;;;; Sentinels
;; We avoid keywords for flow control due to risk of malicious user data

(do
  (defonce sentinel-null-reply              (Object.))
  (defonce sentinel-skipped-reply           (Object.))
  (defonce sentinel-end-of-aggregate-stream (Object.)))

;;;; Read mode

(def ^:dynamic *read-mode*
  "Special read mode: nil, `:skip`, `:bytes`, or `<ReadThawed>`. Most modes
  apply only to blobs. `:skip` applies to all replies. `:skip+errors` is an
  unsupported internal value and may change without notice."
  nil)

(defmacro ^:public skip-replies
  "Discards Redis replies to requests in `body`.

  Each reply frame is fully consumed. A discarded reply occupies no position in
  a `wcar` or `with-car` result. Ordinary Redis errors are also discarded;
  protocol, framing, and resource-limit failures still surface."
  [& body] `(enc/binding* [*read-mode* :skip] ~@body))

(defmacro ^:public normal-replies
  "Cancels [[skip-replies]], [[as-bytes]], or [[thaw]] for `body`. It does not
  change reply parsers or Carmine's default reply decoding.
  See also [[unparsed]], [[natural-replies]]."
  [& body]
  `(let [body-fn# (fn [] ~@body)]
     (enc/if-not *read-mode*
       (do                        (body-fn#)) ; Common case optimization
       (enc/binding* [*read-mode* nil] (body-fn#)))))

(def ^:dynamic *tx-effects?*
  "True iff an effects function is queueing commands in an open MULTI. Carmine
  uses this value to reject incompatible operations, such as the
  [[taoensso.carmine-v4/eval*]] script fallback. These operations cannot work
  when each reply is `QUEUED`."
  false)

(def ^:dynamic    *natural-replies?* false)
(defmacro ^:public natural-replies
  "Disables all reply transformations for `body`. These transformations include
  special read modes, reply parsers, and the default decode bindings
  [[taoensso.carmine-v4/*auto-thaw?*]],
  [[taoensso.carmine-v4/*keywordize-maps?*]], and
  [[taoensso.carmine-v4/*raw-verbatim-strings?*]].

  Unlike [[normal-replies]] and [[unparsed]], this setting controls its complete
  dynamic scope. Inner read modes, parsers, and decode bindings are ignored for
  requests that `body` queues.

  Intended for wire-level protocol use (e.g. by library authors). It disables
  automatic thawing but retains UTF-8 blob decoding, so a serialized Carmine
  value returns as a string containing its marker and decoded payload. For exact
  stored bytes, instead use [[as-bytes]] with
  [[taoensso.carmine-v4/*auto-thaw?*]] false.

  See also [[normal-replies]], [[unparsed]]."
  [& body]
  `(let [body-fn# (fn [] ~@body)]
     (if *natural-replies?*
       (do                                 (body-fn#)) ; Common case optimization
       (enc/binding* [*natural-replies?* true] (body-fn#)))))

(defmacro ^:public as-bytes
  "Returns Redis blob-string replies to requests in `body` as byte arrays
  instead of UTF-8 strings. When [[taoensso.carmine-v4/*auto-thaw?*]] is true,
  Carmine markers are still interpreted: binary and Nippy markers are removed,
  and a marked nil returns nil. Bind it false to preserve exact stored bytes.

  This mode does not affect RESP3 verbatim strings. See
  [[taoensso.carmine-v4/*raw-verbatim-strings?*]]."
  [& body] `(enc/binding* [*read-mode* :bytes] ~@body))

(defmacro ^:public thaw
  "Tries to thaw blob-type Redis replies to requests in `body` with Nippy.
  This mode does not affect RESP3 verbatim strings. See
  [[taoensso.carmine-v4/*raw-verbatim-strings?*]]."
  [thaw-opts & body]
  `(enc/binding*
     [*read-mode* (taoensso.carmine_v4.resp.common.ReadThawed. ~thaw-opts)]
     ~@body))

(deftype ReadThawed [thaw-opts])
(defn read-mode->?thaw-opts [read-mode]
  (when (instance?    ReadThawed read-mode)
    (or (.-thaw-opts ^ReadThawed read-mode) {})))

;;;; ReadOpts, etc.

(deftype ReadOpts
  [read-mode parser auto-thaw? issue-83-workaround? keywordize-maps?
   raw-verbatim-strings? push-fn])

(defn with-push-fn
  "Returns `read-opts` with the specified top-level RESP3 push function. Keeps
  all reply decoding options. Internal."
  ^ReadOpts [^ReadOpts read-opts push-fn]
  (if (identical? (.-push-fn read-opts) push-fn)
    read-opts
    (ReadOpts.
      (.-read-mode read-opts)
      (.-parser read-opts)
      (.-auto-thaw? read-opts)
      (.-issue-83-workaround? read-opts)
      (.-keywordize-maps? read-opts)
      (.-raw-verbatim-strings? read-opts)
      push-fn)))

(defn skip-read-mode?
  "Returns true for read modes that discard successful reply values."
  [read-mode]
  (or (identical? read-mode :skip)
      (identical? read-mode :skip+errors)))

(do
  (enc/defonce read-opts-natural "For \"natural\" reads" (ReadOpts. nil   nil nil  nil nil   nil nil))
  (enc/defonce read-opts-skip    "For `:skip` read mode" (ReadOpts. :skip nil nil  nil nil   nil nil))
  (enc/defonce read-opts-skip+errors
    "Internal: skips successful replies but preserves Redis error replies"
    (ReadOpts. :skip+errors nil nil nil nil nil nil))
  (enc/defonce read-opts-default "For REPL/tests/etc."   (ReadOpts. nil nil true false false false nil)))

(defn in-aggregate-read-opts
  "Returns internal aggregate `ReadOpts`. Keeps all nested options except the
  parser."
  ^ReadOpts [^ReadOpts read-opts]
  (if (nil? (.-parser read-opts))
    read-opts
    (ReadOpts.
      (.-read-mode        read-opts)
      nil
      (.-auto-thaw?       read-opts)
      (.-issue-83-workaround? read-opts)
      (.-keywordize-maps? read-opts)
      (.-raw-verbatim-strings? read-opts)
      (.-push-fn read-opts))))

(declare ^:dynamic *parser* get-parser-opts)

(let [read-opts-natural           read-opts-natural
      read-opts-skip              read-opts-skip
      ^ReadOpts read-opts-default read-opts-default]

  (defn get-read-opts
    "Returns an appropriate `ReadOpts`."
    (^ReadOpts []
     (if *natural-replies?*
       read-opts-natural

       (let [read-mode *read-mode*]
         (if (identical? read-mode :skip)
           read-opts-skip ; Optimization, all else irrelevant

           (let [parser *parser*]
             ;; INTERNAL mechanism (unsupported, may change without notice):
             ;; allow parser-opts to influence dynamic ReadOpts. This is
             ;; exactly equivalent to
             ;; (parse <...> (establish-bindings <...>)).
             (if-let [p-opts (get-parser-opts parser)]
               (ReadOpts.
                 (get p-opts :read-mode read-mode)
                 parser
                 (if (contains? p-opts :auto-thaw?)        (get p-opts :auto-thaw?)       core/*auto-thaw?*)
                 core/*issue-83-workaround?*
                 (if (contains? p-opts :keywordize-maps?)  (get p-opts :keywordize-maps?) core/*keywordize-maps?*)
                 (if (contains? p-opts :raw-verbatim-strings?)
                   (get p-opts :raw-verbatim-strings?)
                   core/*raw-verbatim-strings?*)
                 nil)

               ;; Common case (no parser-opts present)
               (let [auto-thaw?              core/*auto-thaw?*
                     issue-83-workaround?     core/*issue-83-workaround?*
                     keywordize-maps?         core/*keywordize-maps?*
                     raw-verbatim-strings?    core/*raw-verbatim-strings?*]
                 (if (and (nil? read-mode) (nil? parser)
                       (identical? auto-thaw?           (.-auto-thaw? read-opts-default))
                       (identical? issue-83-workaround?  (.-issue-83-workaround? read-opts-default))
                       (identical? keywordize-maps?      (.-keywordize-maps? read-opts-default))
                       (identical? raw-verbatim-strings? (.-raw-verbatim-strings? read-opts-default)))
                   read-opts-default
                   (ReadOpts. read-mode parser auto-thaw? issue-83-workaround?
                     keywordize-maps? raw-verbatim-strings? nil)))))))))

    (^ReadOpts [opts] ; For REPL/tests
     (if (empty? opts)
       read-opts-natural
       (let [{:keys [read-mode parser auto-thaw? issue-83-workaround?
                     keywordize-maps? raw-verbatim-strings?]} opts]
         (ReadOpts. read-mode parser auto-thaw? issue-83-workaround?
           keywordize-maps? raw-verbatim-strings? nil))))))

(comment (enc/qb 1e6 (get-read-opts))) ; 43.72

(declare describe-parser)

(defn describe-read-opts
  "For error messages, etc."
  [read-opts]
  (when-let [^ReadOpts read-opts read-opts]
    {:read-mode (.-read-mode read-opts)
     :parser (-> (.-parser read-opts) describe-parser)
     :auto-thaw? (.-auto-thaw? read-opts)
     :issue-83-workaround? (.-issue-83-workaround? read-opts)
     :keywordize-maps? (.-keywordize-maps? read-opts)
     :raw-verbatim-strings? (.-raw-verbatim-strings? read-opts)}))

;;;; Reply parsing
;; We choose to keep parsing pretty simple:
;; no nesting, no auto composition, and no concurrent fn+rf parsers.
;; Note that *read-mode* and *parser* are distinct, and may interact.

(def ^:dynamic *parser* "?<Parser>" nil)

(deftype Parser [kind opts f rfc kv-rf?])
;; rfc: auto-generated (fn rf-constructor []) => <possibly-stateful-rf*>
;; parser-opts:
;;   read-mode            ; nx    ; INTERNAL: unsupported, may change without notice
;;   auto-thaw?           ; nx    ; ''
;;   keywordize-maps?     ; nx    ; ''
;;   kv-rf?               ; false ; ''
;;   catch-errors?        ; true  ; ''
;;   parse-error-replies? ; false
;;   parse-null-replies?  ; false

(defn   when-fn-parser  [x] (when (and (instance? Parser x) (.-f    ^Parser x)) x))
(defn   when-rf-parser  [x] (when (and (instance? Parser x) (.-rfc  ^Parser x)) x))
(defn- get-parser-opts  [x] (when      (instance? Parser x) (.-opts ^Parser x)))

(defn defer-fn-parser
  "Returns an internal parser that preserves parser options and aggregate
  reduction, but defers function parsing and RESP null completion."
  [x]
  (if (instance? Parser x)
    (let [^Parser p x]
      (Parser. (.-kind p) (assoc (.-opts p) ::preserve-null-reply? true)
        nil (.-rfc p) (.-kv-rf? p)))
    (Parser. :internal {::preserve-null-reply? true} nil nil nil)))

(defn preserve-null-reply-parser? [x]
  (boolean
    (when (instance? Parser x)
      (get (.-opts ^Parser x) ::preserve-null-reply?))))

(defn- public-parser-opts
  "Returns the public-policy parser options. Parser options may contain internal
  keys, but exception data must contain only the public policy."
  [parser-opts]
  (not-empty
    (select-keys parser-opts [:parse-error-replies? :parse-null-replies?])))

(defn- describe-parser
  "For error messages, etc."
  [parser]
  (when-let [p ^Parser parser]
    {:kind (.-kind p)
     :opts (public-parser-opts (.-opts p))}))

(comment
  [(describe-parser (fn-parser {:o :O}     (fn [])))
   (describe-parser (rf-parser {:o :O} nil (fn [])))])

(defn- parser-error
  [cause data]
  (reply-error
    "[Carmine] Reply parser threw an error"
    (enc/assoc-nx data :eid :carmine.read/parser-error)
    cause))

(defn- safe-parser-fn [parser-opts f]
  (fn  safe-parser-fn [x]
    (try
      (f x)
      (catch Throwable t
        (parser-error  t
          {:kind :fn
           :parser-opts (public-parser-opts parser-opts)
           :arg  (enc/typed-val x)})))))

(defn fn-parser ^Parser [parser-opts f]
  (let [parser-opts (not-empty parser-opts)
        f*
        (if (get parser-opts :catch-errors? true)
          (safe-parser-fn parser-opts f)
          (do                         f))]
    (Parser. :fn parser-opts f* nil nil)))

(defn- safe-parser-xrf
  "Returns a stateful transducer that catches errors from `rf`. After an error,
  each call returns that error without calling `rf`. Parser errors do not
  interrupt the reduction."
  ([caught_ error-data]
   (fn [rf]
     (truss/catching-rf
       (fn error-fn [extra-data cause] (vreset! caught_ (parser-error cause (conj error-data extra-data))))
       (fn
         ([]        (or @caught_ (rf)))
         ([acc]     (or @caught_ (rf acc)))
         ([acc in]  (or @caught_ (rf acc in)))
         ([acc k v] (or @caught_ (rf acc k v))))))))

(defn- parser-error-rf [error]
  (fn
    ([]        error)
    ([_]       error)
    ([_ _]     error)
    ([_ _ _]   error)))

(defn rf-parser
  "Creates a parser. `rf` should be a reducing function with these operations:
    (rf)        => Init     acc
    (rf acc in) => Next     acc (accumulation step)
    (rf acc)    => Complete acc"
  ^Parser [parser-opts ?xform rf]
  (let [parser-opts (not-empty parser-opts)
        kv-rf? (if ?xform false (get parser-opts :kv-rf? false))

        error-data
        (fn [thrown-by]
          {:parser-opts (public-parser-opts parser-opts)
           :xform       ?xform
           :rf          rf
           :thrown-by   thrown-by})

        rf-constructor
        (if (get parser-opts :catch-errors? true)

          ;; Catch errors. Nb caught state is per-reduction (fresh for every
          ;; aggregate) so that a parser error can never poison later replies,
          ;; or race across concurrent reductions sharing this `Parser`:
          (if-let [xform ?xform]
            (fn rfc []
              (let [caught_ (volatile! nil)]
                ;; Currently do double wrapping to distinguish
                ;; between :rf and :xform errors
                (try
                  ((comp
                     (safe-parser-xrf caught_ (error-data :xform))
                     xform
                     (safe-parser-xrf caught_ (error-data :rf)))
                   rf)
                  (catch Throwable t
                    ;; A transducer may throw while it is being applied to the
                    ;; reducing function, before any aggregate element is read.
                    ;; Return an error RF so the caller still drains the frame.
                    (parser-error-rf
                      (parser-error t (error-data :xform)))))))

            (fn rfc []
              (let [caught_ (volatile! nil)]
                ((safe-parser-xrf caught_ (error-data :rf)) rf))))

          ;; Don't catch errors
          (if-let [xform ?xform]
            (fn rfc [] (xform rf)) ; Possibly stateful
            (fn rfc []        rf)))]

    (Parser. :rf parser-opts nil
      rf-constructor kv-rf?)))

(comment (enc/qb 1e6 (rf-parser {} nil (fn [])))) ; 72.61

(defn without-error-parsing
  "Returns `read-opts` with `:parse-error-replies?` disabled for a function
  parser. This lets the Cluster executor classify raw redirect errors. Internal.
  See [[parse-deferred-error-reply]]."
  ^ReadOpts [^ReadOpts read-opts]
  (let [p (.-parser read-opts)]
    (if (and (instance? Parser p) (.-f ^Parser p)
          (get (.-opts ^Parser p) :parse-error-replies?))
      (ReadOpts.
        (.-read-mode read-opts)
        (Parser.
          (.-kind ^Parser p)
          (dissoc (.-opts ^Parser p) :parse-error-replies?)
          (.-f    ^Parser p)
          (.-rfc  ^Parser p)
          (.-kv-rf? ^Parser p))
        (.-auto-thaw? read-opts)
        (.-issue-83-workaround? read-opts)
        (.-keywordize-maps? read-opts)
        (.-raw-verbatim-strings? read-opts)
        (.-push-fn read-opts))
      read-opts)))

(defn parse-deferred-error-reply
  "Applies the `:parse-error-replies?` function parser in `read-opts` to an
  ordinary Redis error reply. Callers must pass only ordinary error replies.
  They must first unwrap an attributed reply and then attach its attributes to
  the parsed content. Internal. See [[without-error-parsing]]."
  [^ReadOpts read-opts reply]
  (if-let [^Parser p (when-fn-parser (.-parser read-opts))]
    (if (get (.-opts p) :parse-error-replies?)
      ((.-f p) reply)
      reply)
    reply))

(defn parsing-rf
  "Internal reducing-function helper for [[parse-aggregates]]. It is an
  optimized alternative to [[clojure.core/completing]]. `init` may be a
  reusable value or a zero-argument function that creates a fresh value. Use
  the latter for mutable accumulators such as transients."
  ([        rf] (parsing-rf (fn [] (rf)) identity rf))
  ([init    rf] (parsing-rf init         identity rf))
  ([init cf rf]
   (let [init-fn (if (fn? init) init (constantly init))]
     (fn parsing-rf
       ([]         (init-fn))
       ([acc]      (cf acc))
       ([acc in]   (rf acc in))
       ([acc k v]  (rf acc k v))))))

(comment
  [((parsing-rf conj))
   ((parsing-rf #(transient []) persistent! conj!))])

;;;; Reply parsing public API

(defn parse-parser-opts
  "Returns strict, canonical opts for the public [[parse]] macro."
  [opts]
  (when-not (or (nil? opts) (map? opts))
    (truss/ex-info! "[Carmine] Expected parser options map"
      {:eid :carmine/invalid-parser-opts
       :opts (enc/typed-val opts)}))
  (let [allowed-keys #{:parse-error-replies? :parse-null-replies?}]
    (when-let [unknown-keys (not-empty (set (remove allowed-keys (keys opts))))]
      (truss/ex-info! "[Carmine] Unexpected parser option keys"
        {:eid :carmine/invalid-parser-opts
         :unknown-keys unknown-keys
         :allowed-keys allowed-keys})))
  opts)

(defn parse-aggregates-opts
  "Returns strict, canonical opts for the public [[parse-aggregates]] macro."
  [opts]
  (when-not (or (nil? opts) (and (map? opts) (empty? opts)))
    (truss/ex-info! "[Carmine] `parse-aggregates` takes no documented options (expected nil or empty map)"
      {:eid :carmine/invalid-parser-opts
       :opts (enc/typed-val opts)}))
  opts)

(defmacro ^:public unparsed
  "Cancels the active reply parser for `body`. Special read modes and Carmine's
  default reply decoding are unaffected.
  See also [[parse]], [[parse-aggregates]]."
  [& body] `(enc/binding* [*parser* nil] ~@body))

(defmacro ^:public parse
  "Sets the given `f` as the reply parser for `body`:
    (fn parse-reply [reply]) => <parsed-reply>.

  The parser applies to each complete reply for requests in `body`:

    - Only one parser can be active. A nested parser replaces the outer parser.
    - The parser receives an aggregate as one complete vector, set, or map. It
      does not parse nested elements recursively.
    - Special read modes such as [[as-bytes]] may change the parser argument.

  `opts` is strictly validated and accepts:

    `:parse-error-replies?` (default false)
      Also parses ordinary Redis reply errors. The parser output replaces the
      error before `wcar` or `with-car` applies `:error-mode`. In a Cluster
      context, redirections such as MOVED and ASK are handled internally.
      The parser sees only the final reply for a request.

    `:parse-null-replies?` (default false)
      Also parses RESP null replies. The parser receives nil. Here, \"null\"
      means a RESP-level null. A Carmine value that thaws to nil is an ordinary
      reply and is always parsed.

  See also [[unparsed]], [[parse-aggregates]]."
  [opts f & body]
  `(enc/binding* [*parser* (fn-parser (parse-parser-opts ~opts) ~f)]
     ~@body))

(defmacro ^:public parse-aggregates
  "Sets the given `rf` as an advanced aggregate reply parser for `body`. `rf`
  must have these reducing-function operations:
    (rf)        => Init acc     ; e.g. (transient [])
    (rf acc in) => Next acc     ; e.g. conj!
    (rf acc)    => Complete acc ; e.g. persistent!

  `rf` is used while each aggregate reply is decoded:

    - Only the outer aggregate is reduced. Nested aggregates decode
      normally and pass to `rf` as complete values.
    - Non-aggregate replies, including RESP nulls and errors, are unchanged.
    - RESP3 attribute maps and push data are never reduced.
    - An early `reduced` value stops accumulation. The complete aggregate reply
      is still read from the wire.

  When a reducing function already has a zero-argument initializer,
  [[clojure.core/completing]] can add the required completion arity.

  `xform` may be a transducer or nil. Only one parser can be active. A nested
  parser replaces the outer parser.

  Special read modes such as [[as-bytes]] may change the elements passed to
  `rf`. There are no documented `opts`: `opts` is strictly validated and must
  be nil or an empty map.

  See also [[unparsed]] and [[parse]]."
  [opts ?xform rf & body]
  `(enc/binding* [*parser* (rf-parser (parse-aggregates-opts ~opts) ~?xform ~rf)]
     ~@body))

(let [opts {:read-mode nil}] ; Coercers force default textual reply decoding
  (def as-?long-parser   (fn-parser opts enc/as-?int))
  (def as-?double-parser (fn-parser opts enc/as-?float))
  (def as-?kw-parser     (fn-parser opts enc/as-?kw))

  (def as-long-parser    (fn-parser opts enc/as-int))
  (def as-double-parser  (fn-parser opts enc/as-float))
  (def as-kw-parser      (fn-parser opts enc/as-kw)))

(do
  (defmacro ^:public as-?long
    "Coerces replies in `body` from text to long or nil. [[as-bytes]] and
    [[thaw]] do not affect this parser."
    [& body] `(enc/binding* [*parser* as-?long-parser] ~@body))

  (defmacro ^:public as-?double
    "Coerces replies in `body` from text to double or nil. [[as-bytes]] and
    [[thaw]] do not affect this parser."
    [& body] `(enc/binding* [*parser* as-?double-parser] ~@body))

  (defmacro ^:public as-?kw
    "Coerces replies in `body` from text to keyword or nil. [[as-bytes]] and
    [[thaw]] do not affect this parser."
    [& body] `(enc/binding* [*parser* as-?kw-parser] ~@body))

  (defmacro ^:public as-long
    "Coerces replies in `body` from text to long, or throws. [[as-bytes]] and
    [[thaw]] do not affect this parser."
    [& body] `(enc/binding* [*parser* as-long-parser] ~@body))

  (defmacro ^:public as-double
    "Coerces replies in `body` from text to double, or throws. [[as-bytes]] and
    [[thaw]] do not affect this parser."
    [& body] `(enc/binding* [*parser* as-double-parser] ~@body))

  (defmacro ^:public as-kw
    "Coerces replies in `body` from text to keyword, or throws. [[as-bytes]] and
    [[thaw]] do not affect this parser."
    [& body] `(enc/binding* [*parser* as-kw-parser] ~@body)))
