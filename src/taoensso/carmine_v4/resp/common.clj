(ns ^:no-doc taoensso.carmine-v4.resp.common
  "Private implementation namespace."
  (:require
   [clojure.string  :as str]
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.nippy.tools :as tools]
   [taoensso.carmine-v4.classes])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInputStream InputStream]
   [clojure.lang ExceptionInfo]

   [taoensso.carmine_v4.classes
    AttributedReply ReplyError ReusableConnError RespResourceError RespInput
    RespBuffer]))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-thaw?*
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*
  ^:dynamic taoensso.carmine-v4/*raw-verbatim-strings?*
  ^:dynamic taoensso.carmine-v4/*issue-83-workaround?*)

(alias 'core 'taoensso.carmine-v4)

(comment (remove-ns 'taoensso.carmine-v4.resp.common))

;; Define proxy classes before their `proxy` forms are compiled. Otherwise
;; AOT compilation loads them from disk with their own copies of our
;; interfaces, so `instance?` checks fail for calls made while compiling.
;; Stale class files in the compile path can still cause this (clean to fix).
(get-proxy-class DataInputStream RespInput)
(get-proxy-class ExceptionInfo   RespResourceError)
(get-proxy-class ExceptionInfo   ReplyError)
(get-proxy-class ExceptionInfo   ReplyError ReusableConnError)

;;;; Utils

(def ba-crlf (enc/str->utf8-ba "\r\n"))

(defn- resource-error
  [msg data cause]
  (if cause
    (proxy [ExceptionInfo RespResourceError] [msg data cause])
    (proxy [ExceptionInfo RespResourceError] [msg data])))

(defn resource-error? [x] (instance? RespResourceError x))

(defn- resource-limit!
  "Records and throws a resource-limit error, poisoning the input."
  [^objects error-slot limit max actual data]
  (let [error
        (resource-error "[Carmine] RESP resource limit exceeded"
          (merge
            {:eid    :carmine.read/resource-limit
             :limit  limit
             :max    max
             :actual actual}
            data)
          nil)]
    (aset error-slot 0 error)
    (throw error)))

(defn- count-frame-bytes!
  [^longs counters error-slot ^long max ^long n]
  (when (< max Long/MAX_VALUE)
    (let [actual (+ (aget counters 0) n)]
      (aset counters 0 actual)
      (when (> actual max)
        ;; Lines are consumed whole when buffered, so report the first byte
        ;; over the limit rather than a count that'd vary with buffering
        (resource-limit! error-slot :max-frame-bytes max (inc max) nil)))))

(defn- incomplete-crlf-line!
  "Handles a buffer that doesn't hold a complete valid line of at most `max`
  bytes at its read position: reads more bytes if that might help, otherwise
  throws. `end` is the index of the line's first CR or LF, or `lim`.
  `frame-room` is the number of bytes left before the frame limit.

  Consumes the offending bytes before throwing, so that frame limits apply
  just as if the line had been read byte by byte."
  [^RespBuffer rb error-slot end max frame-room]
  (let [buf (.buf rb)
        pos (.pos rb)
        lim (.lim rb)
        end (long end)
        max (long max)
        n   (- end pos)
        missing-crlf!
        (fn [^long n-read ?next-b]
          ;; NB `more` may have moved the unread bytes, so locate them afresh
          (let [read (String. (.buf rb) (int (.pos rb)) (int n-read) StandardCharsets/UTF_8)
                read (if ?next-b (str read (char (bit-and (long ?next-b) 0xff))) read)]
            (.consume rb (if ?next-b (inc n-read) n-read))
            (truss/ex-info! "[Carmine] Missing CRLF"
              {:eid :carmine.read/missing-crlf, :read read})))

        more-or-missing-crlf!
        (fn [] ; Line so far is all of the unread bytes
          (let [n-unread (- lim pos)]
            (when (> n-unread (long frame-room))
              (.consume rb n-unread)) ; Over frame limit, so this throws
            (when (neg? (.more rb)) (missing-crlf! n-unread nil))))]

    (cond
      (> n max)
      (do
        (.consume rb (inc max))
        (resource-limit! error-slot :max-line-bytes max (inc max) nil))

      (== end lim)                       (more-or-missing-crlf!)
      (== (aget buf end) (int \newline)) (missing-crlf! (inc n) nil)
      (== (inc end) lim) #_"CR is last"  (more-or-missing-crlf!)
      :else #_"CR, then a byte that isn't LF"
      (missing-crlf! (inc n) (aget buf (inc end))))))

(deftype RespInputState
  ;; Reading state of a `resp-input`: its read buffer (with direct access for
  ;; reply reading), limits, and counters. Not thread-safe: like the stream
  ;; it backs, expects a single reader at a time.
  ;;
  ;; Unread bytes are `buf[pos..lim)`. NB any read may move them or replace
  ;; `buf` with a larger array (see `more`), so re-read `buf`/`pos`/`lim`
  ;; after one. Consumed bytes are counted against the frame limit.
  ;;
  ;; A missing limit is `Long/MAX_VALUE`, making disabled checks a primitive
  ;; comparison rather than a repeated map lookup.
  ;;
  ;; `counters`:   frame bytes, read depth, aggregate depth, flush state
  ;; `error-slot`: 1-element array holding the first resource-limit error
  [^InputStream raw
   ^:unsynchronized-mutable ^bytes buf
   ^:unsynchronized-mutable ^long pos
   ^:unsynchronized-mutable ^long lim
   ^long init-size ^longs counters ^objects error-slot
   ^long max-line-bytes ^long max-nesting-depth ^long max-blob-bytes
   ^long max-aggregate-elements ^long max-frame-bytes]

  RespBuffer
  (buf [_] buf)
  (pos [_] pos)
  (lim [_] lim)
  (consume [_ n]
    (set! pos (+ pos n))
    (count-frame-bytes! counters error-slot max-frame-bytes n))

  (readByte [this] ; Unsigned byte, or -1 at end of stream
    (if (or (< pos lim) (pos? (.more this)))
      (let [b (bit-and (aget buf (int pos)) 0xff)]
        (.consume this 1)
        b)
      -1))

  (read [this ba off len]
    (cond
      (== len 0) 0
      (< pos lim)
      (let [n (min len (- lim pos))]
        (System/arraycopy buf (int pos) ba (int off) (int n))
        (.consume this n)
        n)

      (>= len (alength buf)) ; Large read with empty buffer, bypass buffer
      (let [n (.read raw ba (int off) (int len))]
        (when (pos? n) (count-frame-bytes! counters error-slot max-frame-bytes n))
        n)

      (pos? (.more this)) (.read this ba off len)
      :else -1))

  (skip [this n]
    (cond
      (<= n 0) 0
      (< pos lim) (let [n (min n (- lim pos))] (.consume this n) n)
      :else
      (let [n (.skip raw n)]
        (when (pos? n) (count-frame-bytes! counters error-slot max-frame-bytes n))
        n)))

  (available [_] (+ (- lim pos) (.available raw)))

  (bufferLine [this max]
    ;; Ensures that the buffer holds a complete CRLF-terminated line of at
    ;; most `max` bytes at its read position, reading more bytes as needed.
    ;; Returns the line's length in bytes without its terminator, doesn't
    ;; consume it. Throws for a line that's too long or badly terminated.
    (loop [n-scanned 0] ; Unread bytes already known to hold no CR or LF
      (let [end ; Index of first CR or LF, or `lim`
            (long
              (loop [idx (+ pos n-scanned)]
                (if (< idx lim)
                  (let [b (aget buf idx)]
                    (if (or (== b (int \return)) (== b (int \newline)))
                      idx
                      (recur (inc idx))))
                  lim)))
            n (- end pos)]

        (if (and
              (<= n max)
              (< (inc end) lim)
              (== (aget buf      end)  (int \return))
              (== (aget buf (inc end)) (int \newline)))
          n
          (do
            (incomplete-crlf-line! this error-slot end max
              (- max-frame-bytes (aget counters 0)))
            ;; More bytes were read. They may have moved, but not relative
            ;; to `pos`, so resume scanning where we left off.
            (recur n))))))

  (more [_]
    ;; Reads more bytes into the buffer, keeping any unread bytes (moved to
    ;; the buffer's start). Doubles a buffer that's already full of unread
    ;; bytes, as needed for a line longer than it. Returns a grown buffer to
    ;; its initial size once it's empty. Returns the number of bytes added,
    ;; or -1 at end of stream.
    (let [n-unread (- lim pos)]
      (when (pos? pos)
        (when (pos? n-unread) (System/arraycopy buf (int pos) buf 0 (int n-unread)))
        (set! pos 0)
        (set! lim n-unread))
      (cond
        (== n-unread (alength buf))
        (set! buf (java.util.Arrays/copyOf buf (* 2 (alength buf))))

        (and (== n-unread 0) (> (alength buf) init-size))
        (set! buf (byte-array init-size)))
      (let [n (.read raw buf (int lim) (- (alength buf) (int lim)))]
        (if (pos? n)
          (do (set! lim (+ lim n)) n)
          -1)))))

(defn- state-resource-limit!
  [^RespInputState state limit max actual data]
  (resource-limit! (.-error-slot state) limit max actual data))

(defn resp-input
  "Returns a buffered `DataInputStream` with connection-level RESP limits.

  Reply reading doesn't use the stream's own methods: it reads directly from
  the stream's buffer, via its `input-state`."
  ^DataInputStream [^InputStream raw-in init-buffer-size limits]
  (let [max-line-bytes         (long (or (get limits :max-line-bytes)         Long/MAX_VALUE))
        max-nesting-depth      (long (or (get limits :max-nesting-depth)      Long/MAX_VALUE))
        max-blob-bytes         (long (or (get limits :max-blob-bytes)         Long/MAX_VALUE))
        max-aggregate-elements (long (or (get limits :max-aggregate-elements) Long/MAX_VALUE))
        max-frame-bytes        (long (or (get limits :max-frame-bytes)        Long/MAX_VALUE))
        state
        (RespInputState. raw-in (byte-array (int init-buffer-size)) 0 0
          (long init-buffer-size) (long-array 4) (object-array 1)
          max-line-bytes max-nesting-depth max-blob-bytes
          max-aggregate-elements max-frame-bytes)

        buffered ; Stream view of buffer, for any other readers
        (proxy [InputStream] []
          (read
            ([] (.readByte state))
            ([ba] (.read state ba 0 (alength ^bytes ba)))
            ([ba off len] (.read state ba off len)))
          (skip      [n] (.skip      state n))
          (available []  (.available state))
          (close     []  (.close raw-in)))]

    (proxy [DataInputStream RespInput] [buffered]
      (respInputState [] state))))

(defn ba->in
  (^DataInputStream [^bytes ba] (ba->in ba nil))
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

(defn input-state
  "Returns the `RespInputState` of given `resp-input`: its read buffer,
  limits, and counters. All reply reading goes through this.

  Performance: `resp-input` streams are Clojure proxies, and every call to a
  non-final method of a proxy (incl. this lookup and `.read`) costs a method
  map lookup. So read code looks up the state once per top-level reply and
  passes it down."
  ^RespInputState [in]
  (if (instance? RespInput in)
    (.respInputState ^RespInput in)
    (truss/ex-info! "[Carmine] Expected a RESP input stream"
      {:eid :carmine.read/unexpected-input
       :given {:type (type in)}})))

(defn read-byte
  "Returns the next unsigned byte, or -1 at end of stream."
  ^long [^RespInputState state] (.readByte state))

(defn read-byte!
  "Like `read-byte` but throws at end of stream."
  ^long [^RespInputState state]
  (let [b (.readByte state)]
    (if (== b -1) (throw (java.io.EOFException.)) b)))

(defn read-fully!
  "Reads exactly `n` bytes into given array, or throws at end of stream."
  [^RespInputState state ^bytes ba ^long off ^long n]
  (loop [off off, n n]
    (when (pos? n)
      (let [n-read (.read state ba off n)]
        (if (== n-read -1)
          (throw (java.io.EOFException.))
          (recur (+ off n-read) (- n n-read)))))))

(defn buffered-blob?
  "Returns true iff the input's buffer already holds the next `n` bytes and
  their CRLF."
  [^RespInputState state ^long n]
  (<= (+ n 2) (- (.lim state) (.pos state))))

(defn read-crlf-line
  "Reads and returns a CRLF-terminated line without its terminator.
  Decodes bytes as UTF-8 to match blob reads and all writes. Measures limits in
  bytes."
  [^RespInputState state]
  (let [n   (.bufferLine state (.-max-line-bytes state))
        buf (.buf state)
        pos (int (.pos state))
        s   (if (and (== n 2)
                  (== (aget buf pos)       (int \O))
                  (== (aget buf (inc pos)) (int \K)))
              "OK" ; Common reply, needn't allocate
              (String. buf pos (int n) StandardCharsets/UTF_8))]
    (.consume state (+ n 2))
    s))

(defn- line->simple-length
  "Returns the length for a line of 1-9 ASCII digits, -1 for \"-1\", -2 for
  \"?\", or -3 for anything else."
  ^long [^bytes ba ^long off ^long n]
  (if (or (< n 1) (> n 9))
    -3
    (let [end (+ off n)]
      (loop [idx off, acc 0]
        (if (< idx end)
          (let [b (long (aget ba idx))]
            (if (and (>= b 48) (<= b 57))
              (recur (inc idx) (+ (* acc 10) (- b 48)))
              (cond
                (and (== n 1) (== b (int \?)))                                     -2
                (and (== n 2) (== b (int \-)) (== (aget ba (inc off)) (int \1))) -1
                :else                                                               -3)))
          acc)))))

(defn read-crlf-length
  "Like `read-crlf-line` for a RESP length line, but avoids allocating for
  common lengths. Returns the length as a long, -1 for \"-1\" (RESP2 null), or
  -2 for \"?\" (streamed). Otherwise returns `(parse-fn kind <line>)`, which
  should throw for an invalid length."
  ^long [^RespInputState state parse-fn kind]
  (let [n   (.bufferLine state (.-max-line-bytes state))
        buf (.buf state)
        pos (.pos state)
        len (line->simple-length buf pos n)
        ?s  (when (== len -3) (String. buf (int pos) (int n) StandardCharsets/UTF_8))]
    (.consume state (+ n 2))
    (if ?s (long (parse-fn kind ?s)) len)))

(defn read-crlf-integer
  "Like `read-crlf-line` for a RESP integer line, but avoids allocating a
  string for common integers (1-9 digits, unsigned). Otherwise returns
  `(parse-fn <line>)`, which should throw for an invalid integer."
  [^RespInputState state parse-fn]
  (let [n   (.bufferLine state (.-max-line-bytes state))
        buf (.buf state)
        pos (.pos state)
        v   (line->simple-length buf pos n)
        ?s  (when (< v 0) (String. buf (int pos) (int n) StandardCharsets/UTF_8))]
    (.consume state (+ n 2))
    (if ?s (parse-fn ?s) v)))

;;;; Resource limits

;; These take an `input-state` result rather than an input stream, see
;; `input-state` for why.

(defn begin-read!
  "Starts a possibly recursive read. Resets state for a top-level frame."
  [^RespInputState state]
  (let [^longs counters (.-counters state)
        ^objects error-slot (.-error-slot state)]
    (when-let [cause (aget error-slot 0)]
      (throw
        (resource-error "[Carmine] Cannot read from poisoned RESP input"
          {:eid :carmine.read/poisoned
           :cause-data (ex-data cause)}
          cause)))
    (let [top-level? (zero? (aget counters 1))]
      (when top-level?
        (aset counters 0 0)  ; Frame bytes
        (aset counters 2 0)) ; Aggregate nesting
      (aset counters 1 (inc (aget counters 1))))))

(defn end-read! [^RespInputState state]
  (let [^longs counters (.-counters state)]
    (aset counters 1 (dec (aget counters 1)))))

(defn next-frame! [^RespInputState state]
  (let [^longs counters (.-counters state)]
    (aset counters 0 0)))

;; A connection's requests and replies are matched by position alone, so its
;; flush state (0 => idle, 1 => flushing, 2 => broken) is kept with the
;; connection, where every request context that shares it will see it.

(defn begin-flush!
  "Starts a pipeline flush on the connection of given input state. Throws
  before anything is written if the connection is mid-flush (its replies would
  be read out of order), or if an earlier flush on it failed (it may hold
  partial requests or unread replies)."
  [^RespInputState state]
  (let [^longs counters (.-counters state)
        flush-state (aget counters 3)]
    (case flush-state
      0 (aset counters 3 1)
      1 (truss/ex-info! "[Carmine] Cannot send requests on a connection while reading its replies (e.g. from a reply parser)"
          {:eid :carmine/conn-busy})
      (truss/ex-info! "[Carmine] Cannot send requests on a connection after an earlier pipeline on it failed (e.g. a read timeout)"
        {:eid :carmine/conn-broken}))))

(defn end-flush!
  "Ends a flush started by `begin-flush!`. An unsuccessful flush leaves the
  connection permanently broken."
  [^RespInputState state success?]
  (aset ^longs (.-counters state) 3 (if success? 0 2)))

(defn conn-broken?
  "Returns true iff a flush failed on the connection of given `resp-input`."
  [in]
  (== (aget ^longs (.-counters (input-state in)) 3) 2))

(defn with-aggregate
  [^RespInputState state kind f]
  (let [^longs counters (.-counters state)
        actual (inc (aget counters 2))
        max    (.-max-nesting-depth state)]
    (when (> actual max)
      (state-resource-limit! state :max-nesting-depth max actual {:kind kind}))
    (aset counters 2 actual)
    (try (f) (finally (aset counters 2 (dec (aget counters 2)))))))

(defn check-aggregate-size! [^RespInputState state kind actual]
  (let [max (.-max-aggregate-elements state)]
    (when (> ^long actual max)
      (state-resource-limit! state :max-aggregate-elements max actual {:kind kind}))))

(defn check-blob+frame-size!
  "Checks the two size limits needed before allocating or consuming a fixed
  blob/chunk."
  [^RespInputState state blob-kind blob-actual frame-kind frame-additional]
  (let [max-blob (.-max-blob-bytes state)]
    (when (> ^long blob-actual max-blob)
      (state-resource-limit! state :max-blob-bytes max-blob blob-actual {:kind blob-kind})))
  (let [max-frame (.-max-frame-bytes state)
        frame-actual (+ (aget ^longs (.-counters state) 0) ^long frame-additional)]
    (when (> frame-actual max-frame)
      (state-resource-limit! state :max-frame-bytes max-frame frame-actual {:kind frame-kind}))))

;;;; Blob markers

(do
  (def ba-npy (enc/str->utf8-ba "\u0000>"))
  (def ba-bin (enc/str->utf8-ba "\u0000<"))
  (def ba-nil (enc/str->utf8-ba "\u0000_")))

;; TODO Add `parse-?marked-ba` -> [<kind> <payload>] user util

;;;; Errors

(defn ^:public reply-content
  "Returns `reply` without its RESP3 attributes wrapper, if it has one.
  See also `reply-attributes`."
  [reply]
  (if (instance? AttributedReply reply)
    (.replyContent ^AttributedReply reply)
    reply))

(defn ^:public reply-attributes
  "Returns the RESP3 attributes map that Redis sent with `reply`, or nil.

  A reply with attributes that can't carry metadata (e.g. a string) is
  returned in a wrapper. Use `reply-content` to unwrap it."
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
  ([replies error-indexes] (drained-reply-errors replies error-indexes nil))
  ([replies error-indexes extra-data]
  (let [cause (reply-content (nth replies (first error-indexes)))
        cause (when (instance? Throwable cause) cause)
        n     (count error-indexes)]
    (proxy [ExceptionInfo ReplyError ReusableConnError]
      [(if-let [msg (get (ex-data cause) :message)] ; Error from Redis
         (str "[Carmine] Redis replied with "
           (if (== n 1) (str "an error: " msg) (str n " errors, first: " msg)))
         (str "[Carmine] "
           (if (== n 1) "Reply error" (str n " reply errors"))
           (when-let [msg (some-> cause ex-message)]
             (str (if (== n 1) ": " ", first: ")
               (str/replace-first msg "[Carmine] " "")))))
       (merge extra-data
         {:eid :carmine/reply-errors
          :replies replies
          :error-indexes error-indexes})
       cause]))))

(comment
  (instance? ExceptionInfo (reply-error "msg" {}))
  (instance? ReplyError    (reply-error "msg" {})))

(defn ^:public reply-error?
  "Returns true iff `x` is a reply error: an error that Redis replied with, or
  that occurred while reading or parsing a reply.

  With `ex-data-submap`, the data of the error or of one of its causes must
  also contain that map:
    (reply-error? {:code \"WRONGTYPE\"} x)

  So this also matches an exception thrown by `wcar`, whose cause is its
  first reply error."

  ([x]
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
         (or
           (enc/submap? (ex-data x) ex-data-submap)
           (recur (ex-cause x))))))))

;;;; Discards

(defn discard-bytes
  "Discards exactly `n` bytes."
  [^RespInputState state n]
  (loop [remaining (long n)]
    (when (pos? remaining)
      (let [skipped (.skip state remaining)]
        (if (pos? skipped)
          (recur (- remaining skipped))
          ;; Stream skips may make no progress, so try a read
          (if (== (.readByte state) -1)
            (truss/ex-info! "[Carmine] Unexpected end of RESP blob"
              {:eid :carmine.read/unexpected-blob-end
               :remaining remaining})
            (recur (dec remaining)))))))
  true)

(defn discard-stream-separator
  [^RespInputState state]
  (let [b (read-byte! state)]
    (if (== b (int \;))
      true
      (truss/ex-info! "[Carmine] Missing stream separator"
        {:eid :carmine.read/missing-stream-separator
         :read {:as-byte (unchecked-byte b) :as-char (char b)}}))))

(defn discard-crlf
  [^RespInputState state]
  (let [b1 (read-byte state)
        b2 (read-byte state)]
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
  apply only to blobs. `:skip` discards successful replies but keeps Redis
  error replies."
  nil)

(defmacro ^:public skip-replies
  "Discards the replies to requests in `body`, e.g.:
    (wcar mgr (skip-replies (set \"k1\" \"v1\") (set \"k2\" \"v2\")) (get \"k1\")) => \"v1\"

  Reply errors are kept, and throw or return as usual (see `wcar`)."
  [& body] `(enc/binding* [*read-mode* :skip] ~@body))

(def ^:dynamic *tx-effects?*
  "True iff an effects function is queueing commands in an open MULTI. Carmine
  uses this value to reject incompatible operations, such as the
  `taoensso.carmine-v4/eval*` script fallback. These operations cannot work
  when each reply is `QUEUED`."
  false)

(def ^:dynamic    *natural-replies?* false)
(defmacro ^:public natural-replies
  "Returns replies to requests in `body` with only basic decoding. Ignores
  read modes (`as-bytes`, etc.), reply parsers, and the `*auto-thaw?*`,
  `*keywordize-maps?*` and `*raw-verbatim-strings?*` bindings, including any
  set within `body`.

  Intended for library authors. Blob replies are still decoded as UTF-8
  strings: for exact bytes use `as-bytes` with `*auto-thaw?*` false.

  See also `unparsed`."
  [& body]
  `(let [body-fn# (fn [] ~@body)]
     (if *natural-replies?*
       (do                                 (body-fn#)) ; Common case optimization
       (enc/binding* [*natural-replies?* true] (body-fn#)))))

(defmacro ^:public as-bytes
  "Returns blob replies to requests in `body` as byte arrays instead of
  strings:
    (wcar mgr (as-bytes (get \"k\"))) => <byte[]>

  When `*auto-thaw?*` is true, Carmine's markers for binary and Nippy data are
  still removed. Bind it false for the exact stored bytes."
  [& body] `(enc/binding* [*read-mode* :bytes] ~@body))

(defmacro ^:public thaw
  "Thaws every blob reply to requests in `body` with Nippy, including data
  that wasn't written by Carmine:
    (wcar mgr (thaw {:password [:salted \"my-password\"]} (get \"k\")))

  `thaw-opts` are Nippy thaw options or nil, merged over any options set
  with `with-thaw-opts`. A reply that isn't valid Nippy data is a reply error.

  To only set options for automatic thawing, use `with-thaw-opts` instead."
  [thaw-opts & body]
  `(enc/binding*
     [*read-mode* (taoensso.carmine_v4.resp.common.ReadThawed. ~thaw-opts)]
     ~@body))

(deftype ReadThawed [thaw-opts]) ; Explicit opts only, merged at capture time

(defn effective-thaw-opts
  "Returns the Nippy thaw opts to capture for a request with `read-mode`:
  ambient `taoensso.nippy.tools/*thaw-opts*` merged with any explicit read-mode
  opts (explicit keys win)."
  [read-mode]
  (let [ambient tools/*thaw-opts*]
    (if (instance? ReadThawed read-mode)
      (if-let [explicit (.-thaw-opts ^ReadThawed read-mode)]
        (enc/merge ambient explicit)
        ambient)
      ambient)))

;;;; ReadOpts, etc.

;; `thaw-opts` are the effective Nippy thaw opts captured at request
;; preparation. They apply only when a blob is actually thawed: marked Nippy
;; blobs with auto-thaw, issue-83 candidates, and all blobs in `thaw` mode.
(deftype ReadOpts
  [read-mode parser auto-thaw? issue-83-workaround? keywordize-maps?
   raw-verbatim-strings? thaw-opts push-fn])

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
      (.-thaw-opts read-opts)
      push-fn)))

(defn skip-read-mode?
  "Returns true for read modes that discard successful reply values."
  [read-mode]
  (identical? read-mode :skip))

(do
  ;; Natural and skip reads never thaw, so they capture no thaw opts
  (enc/defonce read-opts-natural "For \"natural\" reads" (ReadOpts. nil   nil nil  nil nil   nil nil nil))
  (enc/defonce read-opts-skip    "For `:skip` read mode" (ReadOpts. :skip nil nil  nil nil   nil nil nil))
  (enc/defonce read-opts-default "For REPL/tests/etc."   (ReadOpts. nil nil true false false false nil nil)))

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
      (.-thaw-opts read-opts)
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
         (if (skip-read-mode? read-mode)
           read-opts-skip ; Optimization, all else irrelevant

           (let [parser *parser*]
             ;; INTERNAL mechanism (unsupported, may change without notice):
             ;; allow parser-opts to influence dynamic ReadOpts. This is
             ;; exactly equivalent to
             ;; (parse <...> (establish-bindings <...>)).
             (if-let [p-opts (get-parser-opts parser)]
               (let [read-mode (get p-opts :read-mode read-mode)]
                 (ReadOpts.
                   read-mode
                   parser
                   (if (contains? p-opts :auto-thaw?)        (get p-opts :auto-thaw?)       core/*auto-thaw?*)
                   core/*issue-83-workaround?*
                   (if (contains? p-opts :keywordize-maps?)  (get p-opts :keywordize-maps?) core/*keywordize-maps?*)
                   (if (contains? p-opts :raw-verbatim-strings?)
                     (get p-opts :raw-verbatim-strings?)
                     core/*raw-verbatim-strings?*)
                   (effective-thaw-opts read-mode)
                   nil))

               ;; Common case (no parser-opts present)
               (let [auto-thaw?              core/*auto-thaw?*
                     issue-83-workaround?     core/*issue-83-workaround?*
                     keywordize-maps?         core/*keywordize-maps?*
                     raw-verbatim-strings?    core/*raw-verbatim-strings?*
                     thaw-opts               (effective-thaw-opts read-mode)]
                 (if (and (nil? read-mode) (nil? parser) (nil? thaw-opts)
                       (identical? auto-thaw?           (.-auto-thaw? read-opts-default))
                       (identical? issue-83-workaround?  (.-issue-83-workaround? read-opts-default))
                       (identical? keywordize-maps?      (.-keywordize-maps? read-opts-default))
                       (identical? raw-verbatim-strings? (.-raw-verbatim-strings? read-opts-default)))
                   read-opts-default
                   (ReadOpts. read-mode parser auto-thaw? issue-83-workaround?
                     keywordize-maps? raw-verbatim-strings? thaw-opts nil)))))))))

    (^ReadOpts [opts] ; For REPL/tests
     (if (empty? opts)
       read-opts-natural
       (let [{:keys [read-mode parser auto-thaw? issue-83-workaround?
                     keywordize-maps? raw-verbatim-strings?]} opts
             thaw-opts
             (if (contains? opts :thaw-opts)
               (get opts :thaw-opts)
               (effective-thaw-opts read-mode))]
         (ReadOpts. read-mode parser auto-thaw? issue-83-workaround?
           keywordize-maps? raw-verbatim-strings? thaw-opts nil))))))

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

(defn   when-fn-parser  [x] (when (and (instance? Parser x) (.-f    ^Parser x)) x))
(defn   when-rf-parser  [x] (when (and (instance? Parser x) (.-rfc  ^Parser x)) x))
(defn- get-parser-opts  [x] (when      (instance? Parser x) (.-opts ^Parser x)))

(defn defer-fn-parser
  "Returns an internal parser that preserves parser options and aggregate
  reduction, but defers function parsing."
  [x]
  (when (instance? Parser x)
    (let [^Parser p x]
      (Parser. (.-kind p) (.-opts p) nil (.-rfc p) (.-kv-rf? p)))))

(defn- describe-parser
  "For error messages, etc."
  [parser]
  (when-let [p ^Parser parser]
    {:kind (.-kind p)}))

(comment
  [(describe-parser (fn-parser {:o :O}     (fn [])))
   (describe-parser (rf-parser {:o :O} nil (fn [])))])

(defn- parser-error
  [cause data]
  ;; `catching-rf` also calls here: normalize ordinary exceptions and assertion
  ;; failures, but never turn cancellation or critical Errors into reply data.
  (truss/try*
    (throw cause)
    (catch InterruptedException t
      (.interrupt (Thread/currentThread))
      (throw t))
    (catch :default t
      (reply-error
        "[Carmine] Reply parser threw an error"
        (enc/assoc-nx data :eid :carmine.read/parser-error)
        t))))

(defn- safe-parser-fn [f]
  (fn  safe-parser-fn [x]
    (try
      (f x)
      (catch Throwable t
        (parser-error  t
          {:kind :fn
           :arg  (enc/typed-val x)})))))

(defn fn-parser ^Parser [parser-opts f]
  (let [parser-opts (not-empty parser-opts)
        f*
        (if (get parser-opts :catch-errors? true)
          (safe-parser-fn f)
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
          {:xform       ?xform
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

(defn parsing-rf
  "Internal reducing-function helper for `parse-aggregates`. It is an
  optimized alternative to `clojure.core/completing`. `init` may be a
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

(defmacro ^:public unparsed
  "Removes any reply parser for requests in `body`.
  See also `parse`, `parse-aggregates`."
  [& body] `(enc/binding* [*parser* nil] ~@body))

(defmacro ^:public parse
  "Calls `(f reply)` on each reply to requests in `body`, and returns the
  result in place of that reply:
    (wcar mgr (parse count (lrange \"k\" 0 -1))) => 3

    - `f` is called for every reply except reply errors. That includes nil
      replies, e.g. `GET` of a missing key.
    - An aggregate reply is passed whole, as a vector, set, or map.
    - `f` sees replies after any read mode (`as-bytes`, etc.) is applied.
    - If `f` throws, the result is a reply error.
    - One parser is active at a time: an inner `parse` replaces an outer one,
      and a nil `f` removes the parser.

  See also `parse-aggregates`, `unparsed`, `as-long`."
  [f & body]
  `(enc/binding* [*parser* (when-let [f# ~f] (fn-parser nil f#))]
     ~@body))

(defmacro ^:public parse-aggregates
  "Advanced. Reduces each aggregate reply to requests in `body` as its elements
  are read, using reducing function `rf` and optional transducer `?xform`:
    (wcar mgr (parse-aggregates (map parse-long) conj (lrange \"k\" 0 -1)))

  `rf` must support all three arities (see `clojure.core/completing`):
    (rf)        => Initial acc ; e.g. (transient [])
    (rf acc in) => Next acc    ; e.g. conj!
    (rf acc)    => Final acc   ; e.g. persistent!

  Only the outer aggregate is reduced: nested aggregates are passed to `rf`
  as complete values. Other replies (nil, reply errors, etc.) are returned as
  usual. Replaces any active reply parser.

  See also `parse`, `unparsed`."
  [?xform rf & body]
  `(enc/binding* [*parser* (rf-parser nil ~?xform ~rf)]
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
    "Coerces each reply to requests in `body` to a long, or to nil if that's
    not possible (e.g. for a missing key):
      (wcar mgr (as-?long (get \"counter\"))) => 42

    See also `as-long`."
    [& body] `(enc/binding* [*parser* as-?long-parser] ~@body))

  (defmacro ^:public as-?double
    "Coerces each reply to requests in `body` to a double, or to nil if that's
    not possible (e.g. for a missing key):
      (wcar mgr (as-?double (get \"score\"))) => 1.5

    See also `as-double`."
    [& body] `(enc/binding* [*parser* as-?double-parser] ~@body))

  (defmacro ^:public as-?kw
    "Coerces each reply to requests in `body` to a keyword, or to nil if that's
    not possible (e.g. for a missing key):
      (wcar mgr (as-?kw (get \"status\"))) => :ok

    See also `as-kw`."
    [& body] `(enc/binding* [*parser* as-?kw-parser] ~@body))

  (defmacro ^:public as-long
    "Coerces each reply to requests in `body` to a long:
      (wcar mgr (as-long (get \"counter\"))) => 42

    A reply that can't be coerced (e.g. for a missing key) is a reply error.
    See also `as-?long`."
    [& body] `(enc/binding* [*parser* as-long-parser] ~@body))

  (defmacro ^:public as-double
    "Coerces each reply to requests in `body` to a double:
      (wcar mgr (as-double (get \"score\"))) => 1.5

    A reply that can't be coerced (e.g. for a missing key) is a reply error.
    See also `as-?double`."
    [& body] `(enc/binding* [*parser* as-double-parser] ~@body))

  (defmacro ^:public as-kw
    "Coerces each reply to requests in `body` to a keyword:
      (wcar mgr (as-kw (get \"status\"))) => :ok

    A reply that can't be coerced (e.g. for a missing key) is a reply error.
    See also `as-?kw`."
    [& body] `(enc/binding* [*parser* as-kw-parser] ~@body)))
