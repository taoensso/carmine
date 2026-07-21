(ns ^:no-doc taoensso.carmine-v4.resp.write
  "Private RESP write implementation."
  (:refer-clojure :exclude [bytes])
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.nippy  :as nippy]
   [taoensso.nippy.tools :as tools]
   [taoensso.carmine-v4.resp.common :as com
    :refer [with-out]])

  (:import
   [java.util Arrays]
   [java.util Locale]
   [java.nio.charset StandardCharsets]
   [java.io BufferedOutputStream]
   [taoensso.nippy.tools WrappedForFreezing]
   [taoensso.carmine_v4.classes RawRedisArg]))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-freeze?*)

(alias 'core 'taoensso.carmine-v4)

(comment (remove-ns 'taoensso.carmine-v4.resp.write))

;; Bound by pooled managers while a connection is borrowed. Any command that
;; can leave connection-local Redis state behind flips this shared marker so the
;; connection is invalidated instead of returned to the pool.
(def ^:dynamic *conn-reusable?_ nil)

;;;; Bulk byte strings

(do
  (def ^:const min-num-to-cache -1024)
  (def ^:const max-num-to-cache  1024))

;; Cache ba representation of common number bulks, etc.
;; Caches are eagerly-built `Object[]`s indexed by (- n from-n); misses fall
;; through to direct encoding.
(let [long->bytes (fn [n] (.getBytes (Long/toString n) StandardCharsets/UTF_8))
      create-cache ; [from-n, to-n) -> ((fn [n])->ba)
      (fn [n-cast ^long from-n ^long to-n f]
        (let [len (- to-n from-n)
              arr (object-array len)]
          (dotimes [i len] (aset arr i (f (n-cast (+ from-n i)))))
          arr))

      b* (int \*)
      b$ (int \$)
      ba-crlf com/ba-crlf]

  (let [;; <n> -> *<n><CRLF> for common lengths
        ^objects cache
        (create-cache long 0 256
          (fn [n]
            (let [n-as-ba (long->bytes n)]
              (com/xs->ba \* n-as-ba "\r\n"))))]

    (defn write-array-len
      [^BufferedOutputStream out n]
      (let [n (long n)]
        (if (and (>= n 0) (< n 256))
          (let [^bytes cached-ba (aget cache n)]
            (.write out cached-ba 0 (alength cached-ba)))

          (let [^bytes n-as-ba (long->bytes n)]
            (.write out b*)
            (.write out n-as-ba 0 (alength n-as-ba))
            (.write out ba-crlf 0 2))))))

  (let [;; <n> -> $<n><CRLF> for common lengths
        ^objects cache
        (create-cache long 0 256
          (fn [n]
            (let [n-as-ba (long->bytes n)]
              (com/xs->ba \$ n-as-ba "\r\n"))))]

    (defn- write-bulk-len
      [^BufferedOutputStream out n]
      (let [n (long n)]
        (if (and (>= n 0) (< n 256))
          (let [^bytes cached-ba (aget cache n)]
            (.write out cached-ba 0 (alength cached-ba)))

          (let [^bytes n-as-ba (long->bytes n)]
            (.write out b$)
            (.write out n-as-ba 0 (alength n-as-ba))
            (.write out ba-crlf 0 2))))))

  (let [;; <n> -> $<len><CRLF><n><CRLF> for common longs
        ^objects cache
        (create-cache long min-num-to-cache (inc max-num-to-cache)
          (fn [n]
            (let [^bytes       n-as-ba (long->bytes n)
                  len (alength n-as-ba)
                  ^bytes     len-as-ba (long->bytes len)]

              (com/xs->ba \$ len-as-ba "\r\n" n-as-ba "\r\n"))))]

    (defn- write-bulk-long
      [^BufferedOutputStream out n]
      (let [n (long n)]
        (if (and (>= n min-num-to-cache) (<= n max-num-to-cache))
          (let [^bytes cached-ba (aget cache (- n min-num-to-cache))]
            (.write out cached-ba 0 (alength cached-ba)))

          (let [^bytes n-as-ba (long->bytes n)
                len (alength n-as-ba)]
            (write-bulk-len out len) ; Always cached
            (.write out n-as-ba 0 len)
            (.write out ba-crlf 0 2))))))

  (let [double->bytes (fn [n] (.getBytes (Double/toString n) StandardCharsets/UTF_8))

        ;; <n> -> $<len><CRLF><n><CRLF> for common whole doubles
        ^objects cache
        (create-cache double min-num-to-cache (inc max-num-to-cache)
          (fn [n]
            (let [^bytes       n-as-ba (double->bytes n)
                  len (alength n-as-ba)
                  ^bytes     len-as-ba (long->bytes len)]

              (com/xs->ba \$ len-as-ba "\r\n" n-as-ba "\r\n"))))]

    (defn- write-bulk-double
      [^BufferedOutputStream out n]
      (let [n (double n)
            l (long n)]
        (if (and (>= l min-num-to-cache) (<= l max-num-to-cache)
              (== n (double l))
              ;; NB -0.0 must miss (encodes as "-0.0", not "0.0")
              (or (not (zero? l)) (zero? (Double/compare n 0.0))))
          (let [^bytes cached-ba (aget cache (- l min-num-to-cache))]
            (.write out cached-ba 0 (alength cached-ba)))

          (let [^bytes n-as-ba (double->bytes n)
                len (alength n-as-ba)]
            (write-bulk-len out len) ; Always cached
            (.write out n-as-ba 0 len)
            (.write out ba-crlf 0 2)))))))

(let [write-bulk-len write-bulk-len
      ba-crlf        com/ba-crlf]

  (defn- write-bulk-ba
    "$<len><CRLF><payload><CRLF>"
    [^BufferedOutputStream out ^bytes ba]
    (let [len (alength ba)]
      (write-bulk-len   out           len)
      (.write           out ba      0 len)
      (.write           out ba-crlf 0   2))))

(defn- write-bulk-float [^BufferedOutputStream out n]
  (write-bulk-ba out
    (.getBytes (Float/toString (float n)) StandardCharsets/UTF_8)))

(defn- reserve-null!
  "Throws if `s` begins with null (char 0). Carmine reserves the null prefix for
  blob markers with special semantics, such as `com/ba-npy`. This is a Carmine
  limit, not a Redis limit."
  [^String s]
  (when (and (not (.isEmpty s)) (== ^int (.charAt s 0) 0))
    (truss/ex-info! "[Carmine] Text args can't begin with null (char 0)"
      {:eid :carmine.write/null-reserved
       :arg s})))

(defn- kw->str
  "Returns \"ns/name\" or \"name\". Uses the keyword's symbol string, which
  Clojure caches, so qualified keywords don't rebuild their string per call."
  ^String [^clojure.lang.Keyword kw]
  (.toString (.-sym kw)))

(defn- non-native-type! [arg]
  (truss/ex-info! "[Carmine] Trying to send argument of non-native type to Redis while `*auto-freeze?` is false"
    {:eid :carmine.write/non-native-arg-type
     :arg (enc/typed-val arg)}))

(defn- marker+payload
  ^bytes [^bytes marker ^bytes payload]
  (if marker
    (let [marker-len  (alength marker)
          payload-len (alength payload)
          result      (byte-array (+ marker-len payload-len))]
      (System/arraycopy marker  0 result 0          marker-len)
      (System/arraycopy payload 0 result marker-len payload-len)
      result)
    payload))

(defn- write-bulk-str [^BufferedOutputStream out s]
  (reserve-null!                       s)
  (write-bulk-ba out (enc/str->utf8-ba s)))

;;;; Wrapper types
;; Influence `IRedisArg` behaviour by wrapping arguments.
;; Wrapping must capture any relevant dynamic config at wrap time.
;;
;; Implementation detail:
;; We try to avoid lazily converting arguments to Redis byte strings
;; (i.e. while writing to out) if there's a chance the conversion
;; could fail (e.g. Nippy freeze).

(deftype  WriteBytes [ba])
(defn ^:public bytes
  "Wraps the given byte array so that it's sent to Redis exactly as is.

  Unwrapped byte arrays are sent with a marker, so that Carmine can return
  them as byte arrays. Wrapped ones are returned as strings unless read with
  `as-bytes`."
  (^WriteBytes [ba]
   (if (instance? WriteBytes ba)
     ba
     (if (enc/bytes? ba)
       (WriteBytes.  ba)
       (truss/ex-info! "[Carmine] `bytes` expects a byte-array argument"
         {:eid :carmine.write/invalid-bytes-arg
          :arg (enc/typed-val ba)}))))

  ;; => Vector for destructuring (undocumented)
  ([ba & more] (mapv bytes (cons ba more))))

(deftype WriteFrozen [unfrozen-val freeze-opts ?frozen-ba])

(defn- new-write-frozen
  ^WriteFrozen [clj-val freeze-opts]
  ;; We do eager freezing here since we can, and we'd prefer to
  ;; catch freezing errors early (rather than while writing to out).
  (WriteFrozen. clj-val freeze-opts (nippy/freeze clj-val freeze-opts)))

(defn ^:public freeze
  "Wraps `clj-val` so that it's serialized with Nippy when sent to Redis:
    (wcar mgr (set \"k\" (freeze \"v\")))
    (wcar mgr (set \"k\" (freeze \"v\" {:password [:salted \"my-password\"]})))

  Useful for values that wouldn't be serialized automatically (e.g. strings,
  or anything when `*auto-freeze?*` is false), or to use specific options.

  `freeze-opts` are Nippy freeze options, merged over any options set with
  `with-freeze-opts`. Serialization happens when wrapping, so errors throw
  here.

  See also `thaw`, `with-thaw-opts`, <https://www.taoensso.com/nippy>."
  (^WriteFrozen [clj-val]
   (cond
     (instance? WriteFrozen        clj-val) clj-val
     (instance? WrappedForFreezing clj-val)
     (let [^WrappedForFreezing w clj-val]
       (new-write-frozen (.-val w) (enc/merge tools/*freeze-opts* (.-opts w))))

     (instance? WriteBytes clj-val)
     (new-write-frozen (.-ba ^WriteBytes clj-val) tools/*freeze-opts*)

     :else
     (new-write-frozen clj-val tools/*freeze-opts*)))

  (^WriteFrozen [clj-val freeze-opts]
   (truss/have [:or nil? map?] freeze-opts)
   (if (nil? freeze-opts)
     (freeze clj-val)
     (cond
       (instance? WriteFrozen clj-val)
       (let [^WriteFrozen wrapper clj-val
             old-opts (.-freeze-opts wrapper)
             new-opts (enc/merge old-opts freeze-opts)]
         (if (= new-opts old-opts)
           wrapper
           (new-write-frozen (.-unfrozen-val wrapper) new-opts))) ; Expensive

       (instance? WrappedForFreezing clj-val)
       (let [^WrappedForFreezing w clj-val]
         (new-write-frozen (.-val w)
           (enc/merge tools/*freeze-opts* (.-opts w) freeze-opts)))

       (instance? WriteBytes clj-val)
       (new-write-frozen (.-ba ^WriteBytes clj-val)
         (enc/merge tools/*freeze-opts* freeze-opts))

       :else
       (new-write-frozen clj-val
         (enc/merge tools/*freeze-opts* freeze-opts))))))

;; Immutable enqueue-time representation for arguments whose later conversion
;; could observe mutation, dynamic bindings, or throw after earlier pipeline
;; bytes have reached the socket. Native immutable arguments deliberately stay
;; unwrapped so their cached write paths remain allocation-free.
(deftype PreparedArg [logical-val ^bytes payload-ba])

(defn ^:no-doc prepared-logical-value [x]
  (if (instance? PreparedArg x) (.-logical-val ^PreparedArg x) x))

(defn ^:no-doc prepare-arg
  "Returns an enqueue-safe Redis argument. Performs fallible conversion and all
  mutable byte access before callers add the request to its context."
  [x]
  (cond
    (instance? PreparedArg x) x

    (string? x)
    (do (reserve-null! x) x)

    (char? x)
    (do (reserve-null! (.toString ^Character x)) x)

    (keyword? x)
    (do (reserve-null! (kw->str x)) x)

    ;; Immutable native values retain the existing cached writers.
    (or (instance? Long x) (instance? Integer x)
        (instance? Short x) (instance? Byte x)
        (instance? Double x) (instance? Float x))
    x

    (instance? WriteBytes x)
    (let [^bytes ba (.-ba ^WriteBytes x)]
      (PreparedArg. x (Arrays/copyOf ba (alength ba))))

    (instance? WriteFrozen x)
    (let [^WriteFrozen w x
          ^bytes ba (or (.-?frozen-ba w)
                      (nippy/freeze (.-unfrozen-val w) (.-freeze-opts w)))]
      (PreparedArg. x
        (marker+payload (when core/*auto-freeze?* com/ba-npy) ba)))

    ;; Lazy v3 wrapper: captured opts override ambient opts at preparation
    (instance? WrappedForFreezing x) (prepare-arg (freeze x))

    (instance? RawRedisArg x)
    (let [^bytes ba (.redisBytes ^RawRedisArg x)]
      (PreparedArg. x (Arrays/copyOf ba (alength ba))))

    (enc/bytes? x)
    (let [^bytes ba x
          marker (when core/*auto-freeze?* com/ba-bin)]
      (PreparedArg. x
        (if marker
          (marker+payload marker ba)
          (Arrays/copyOf ba (alength ba)))))

    (nil? x)
    (if core/*auto-freeze?*
      (PreparedArg. nil com/ba-nil)
      (non-native-type! nil))

    :else
    (if core/*auto-freeze?*
      (PreparedArg. x
        (marker+payload com/ba-npy (nippy/freeze x tools/*freeze-opts*)))
      (non-native-type! x))))

(defn ^:no-doc arg-payload-bytes
  "Returns the exact bytes Redis will receive for one prepared/native bulk
  argument. Cluster routing uses this same representation as socket writes."
  ^bytes [x]
  (cond
    (instance? PreparedArg x) (.-payload-ba ^PreparedArg x)
    (string? x)               (do (reserve-null! x) (enc/str->utf8-ba x))
    (char? x)
    (let [s (.toString ^Character x)]
      (reserve-null! s)
      (enc/str->utf8-ba s))
    (keyword? x)
    (let [s (kw->str x)]
      (reserve-null! s)
      (enc/str->utf8-ba s))
    (or (instance? Long x) (instance? Integer x)
        (instance? Short x) (instance? Byte x))
    (.getBytes (Long/toString (long x)) StandardCharsets/UTF_8)
    (instance? Double x)
    (.getBytes (Double/toString (double x)) StandardCharsets/UTF_8)
    (instance? Float x)
    (.getBytes (Float/toString (float x)) StandardCharsets/UTF_8)
    :else
    (arg-payload-bytes (prepare-arg x))))

(def ^:private pubsub-command-names
  #{"SUBSCRIBE" "PSUBSCRIBE" "SSUBSCRIBE"
    "UNSUBSCRIBE" "PUNSUBSCRIBE" "SUNSUBSCRIBE"})

(def ^:private connection-stateful-command-names
  (into
    #{"ASKING" "AUTH" "DISCARD" "EXEC" "HELLO" "MONITOR" "MULTI" "PSYNC"
      "QUIT" "READONLY" "READWRITE" "REPLCONF" "RESET" "SELECT" "SYNC"
      "UNWATCH" "WATCH"
      "CLIENT CACHING" "CLIENT NO-EVICT" "CLIENT NO-TOUCH" "CLIENT REPLY"
      "CLIENT SETINFO" "CLIENT SETNAME" "CLIENT TRACKING"
      "SCRIPT DEBUG"
      "HIMPORT"} ; Fieldsets, any subcommand
    pubsub-command-names))

(def ^:private cluster-incompatible-command-names
  "Commands that per-command Cluster routing cannot support: those that depend
  on connection-local state, or on a connection's preceding writes."
  (-> connection-stateful-command-names
    (disj "SCRIPT DEBUG")
    (conj "WAIT" "WAITAOF")))

(def ^:private client-reply-no-reply-modes #{"OFF" "SKIP"})
(def ^:private streaming-command-names #{"MONITOR" "PSYNC" "SYNC"})
(def ^:private script-debug-modes #{"YES" "SYNC"})

(defn- command-token [args idx]
  (when (< idx (count args))
    (-> (String. ^bytes (arg-payload-bytes (nth args idx)) StandardCharsets/UTF_8)
      (.toUpperCase Locale/ROOT))))

(defn- command-name [args]
  (let [first-token (command-token args 0)]
    (if (or (= first-token "CLIENT") (= first-token "SCRIPT"))
      (str first-token " " (command-token args 1))
      first-token)))

(defn ^:no-doc connection-stateful-command?
  "Returns true iff a prepared command can change connection-local Redis state."
  [args]
  (contains? connection-stateful-command-names (command-name args)))

(def ^:private ^:const stateful-prefilter-max-token-len 12) ; "PUNSUBSCRIBE"

(def ^:private ^"[Z" stateful-command-prefilter
  ;; boolean[(len * 512) | (first? 0 256) | byte] marking the (length, first
  ;; byte) and (length, last byte), in either case, of every stateful first
  ;; token. A fast conservative filter:
  ;; false => definitely not stateful (the per-command hot path for GET/SET
  ;; etc., no allocation); true => confirm via the definitive string path.
  (let [table (boolean-array (* (inc stateful-prefilter-max-token-len) 512))]
    (doseq [^String command-name
            (into connection-stateful-command-names cluster-incompatible-command-names)]
      (let [^String token (first (.split command-name " "))
            len (.length token)]
        (doseq [[offset ch] [[0 (.charAt token 0)] [256 (.charAt token (dec len))]]
                ch          [ch (Character/toLowerCase (char ch))]]
          (aset table (+ (* len 512) (long offset) (int ch)) true))))
    table))

(defn- possibly-stateful-token? [^long len ^long first-byte ^long last-byte]
  (and (<= len stateful-prefilter-max-token-len)
    (let [base (* len 512)]
      (and
        (aget stateful-command-prefilter (+ base     (bit-and first-byte 0xff)))
        (aget stateful-command-prefilter (+ base 256 (bit-and last-byte  0xff)))))))

(defn- possibly-stateful-command? [args]
  (let [x (nth args 0)
        x (if (and (keyword? x) (nil? (namespace x))) (name x) x)] ; e.g. (rcmd :GET "k")
    (cond
      (instance? PreparedArg x)
      (let [^bytes ba (.-payload-ba ^PreparedArg x)
            len (alength ba)]
        (and (pos? len)
          (possibly-stateful-token? len (aget ba 0) (aget ba (dec len)))))

      (string? x)
      (let [^String s x, len (.length s)]
        (and (pos? len)
          (let [c1 (int (.charAt s 0))
                c2 (int (.charAt s (dec len)))]
            (and (< c1 256) (< c2 256) (possibly-stateful-token? len c1 c2)))))

      ;; Uncommon first-arg type: use the definitive path
      :else true)))

(defn ^:no-doc cluster-incompatible-command?
  "Returns true iff a prepared command depends on connection-local Redis state
  (e.g. MULTI) or on a connection's preceding writes (e.g. WAIT), which
  per-command Cluster routing cannot preserve."
  [args]
  (and (possibly-stateful-command? args)
    (contains? cluster-incompatible-command-names (command-name args))))

(defn ^:no-doc request-incompatible-command
  "Returns details when a prepared command cannot safely participate in an
  ordinary request/reply context, otherwise nil."
  [args]
  (when (possibly-stateful-command? args)
    (let [command-name (command-name args)]
      (cond
        (contains? pubsub-command-names command-name)
        {:command command-name, :reason :pubsub-command}

        ;; These replace ordinary replies with monitor/replication streams or
        ;; debugger exchanges. Closing the connection after use cannot prevent
        ;; those frames from being mistaken for subsequent pipeline replies.
        (contains? streaming-command-names command-name)
        {:command command-name, :reason :streaming-command}

        (= command-name "SCRIPT DEBUG")
        (let [mode (command-token args 2)]
          (when (contains? script-debug-modes mode)
            {:command (str command-name " " mode), :reason :streaming-command}))

        (= command-name "CLIENT REPLY")
        (let [mode (command-token args 2)]
          (when (contains? client-reply-no-reply-modes mode)
            {:command (str command-name " " mode), :reason :no-reply}))))))

(defn- mark-connection-state! [reusable?_ args]
  (when reusable?_
    (when (and (possibly-stateful-command? args)
            (connection-stateful-command? args))
      (vreset! reusable?_ false))))

(defn ^:no-doc prepare-command
  "Validates and prepares one logical Redis command atomically."
  ([args] (prepare-command args 0))
  ([args n-prepared-prefix-args]
   (when-not (sequential? args)
     (truss/ex-info! "[Carmine] Redis command must be sequential"
       {:eid :carmine.write/invalid-command
        :command (enc/typed-val args)}))
   (let [args (if (vector? args) args (vec args))
         n    (count args)
         start (int n-prepared-prefix-args)]
     (when (zero? n)
       (truss/ex-info! "[Carmine] Redis command must contain at least one argument"
         {:eid :carmine.write/empty-command}))
     ;; Generated command prefixes are immutable compile-time strings whose
     ;; exact wire encoding is cached separately. Callers identify that trusted
     ;; prefix so enqueue preparation only visits dynamic arguments.
     (loop [idx start]
       (if (< idx n)
         (let [arg      (nth args idx)
               prepared (prepare-arg arg)]
           (if (identical? arg prepared)
             (recur (inc idx))
             ;; Copy only after the first argument that actually needs an
             ;; enqueue-safe wrapper. Most ordinary command vectors can be
             ;; returned unchanged.
             (let [changed-idx idx]
               (loop [idx (inc changed-idx), result (assoc args changed-idx prepared)]
                 (if (< idx n)
                   (let [arg      (nth args idx)
                         prepared (prepare-arg arg)]
                     (recur (inc idx)
                       (if (identical? arg prepared)
                         result
                         (assoc result idx prepared))))
                   result)))))
         args)))))

;;;; IRedisArg

(defprotocol ^:private IRedisArg
  "Internal protocol, not for public use or extension."
  (write-bulk-arg [x ^BufferedOutputStream out]
    "Writes given prepared argument to `out` as a Redis byte string.
    See `prepare-arg`."))

(let [write-bulk-str write-bulk-str
      kw->str kw->str]

  (extend-protocol IRedisArg
    String               (write-bulk-arg [s  out] (write-bulk-str out            s))
    Character            (write-bulk-arg [c  out] (write-bulk-str out (.toString c)))
    clojure.lang.Keyword (write-bulk-arg [kw out] (write-bulk-str out (kw->str   kw)))

    ;; NB client->server commands are always arrays of bulk strings: number
    ;; args must be written as bulks, never as RESP number frames
    Long        (write-bulk-arg [n out] (write-bulk-long   out n))
    Integer     (write-bulk-arg [n out] (write-bulk-long   out n))
    Short       (write-bulk-arg [n out] (write-bulk-long   out n))
    Byte        (write-bulk-arg [n out] (write-bulk-long   out n))
    Double      (write-bulk-arg [n out] (write-bulk-double out n))
    Float       (write-bulk-arg [n out] (write-bulk-float  out n))
    PreparedArg (write-bulk-arg [w out] (write-bulk-ba     out (.-payload-ba w)))

    Object
    (write-bulk-arg [x out]
      (truss/ex-info! "[Carmine] Unprepared Redis argument"
        {:eid :carmine.write/unprepared-arg
         :type (type x)}))))

(defn- write-arg
  "Like `write-bulk-arg`, but skips protocol dispatch for the most common
  argument types. Dispatch is otherwise relatively expensive here since the
  types written tend to vary from one call to the next."
  [x out]
  (cond
    (instance? String      x) (write-bulk-str out x)
    (instance? PreparedArg x) (write-bulk-ba  out (.-payload-ba ^PreparedArg x))
    :else                     (write-bulk-arg x out)))

;;;;

(defn ^:no-doc encode-command-prefix
  "Returns the RESP bulk encoding for one or more static command tokens.
  Excludes the array header because a variadic command determines its argument
  count at call time."
  ^bytes [static-args]
  (with-out
    (enc/run! (fn [arg] (write-bulk-arg arg out)) static-args)))

(defn ^:no-doc write-command
  "Writes one logical Redis command without flushing `out`.

  A generated command may supply an encoded prefix with `n-prefix-args` complete
  bulk arguments. Those arguments still appear logically in `args` for Cluster
  routing and diagnostics; only their re-encoding is skipped.

  Callers writing many commands may supply the `*conn-reusable?_` value to
  avoid a dynamic var lookup per command."
  ([out args] (write-command out args nil 0))
  ([out args ?encoded-prefix n-prefix-args]
   (write-command out args ?encoded-prefix n-prefix-args *conn-reusable?_))

  ([^BufferedOutputStream out args ?encoded-prefix n-prefix-args conn-reusable?_]
   (let [n-args (count args)]
     (when-not (== n-args 0)
       (mark-connection-state! conn-reusable?_ args)
       (write-array-len out n-args)
       (if ?encoded-prefix
         (let [^bytes encoded-prefix ?encoded-prefix
               n-prefix-args (int n-prefix-args)]
           (.write out encoded-prefix 0 (alength encoded-prefix))
           ;; Generated request vectors are always vectors. Keep a generic
           ;; fallback for internal callers and defensive REPL use.
           (if (vector? args)
             (loop [idx n-prefix-args]
               (when (< idx n-args)
                 (write-arg (nth args idx) out)
                 (recur (inc idx))))
             (enc/run!
               (fn [arg] (write-arg arg out))
               (drop n-prefix-args args))))
         (enc/run! (fn [arg] (write-arg arg out)) args))))))

(defn write-requests ; Internal; also used by dedicated listeners
  "Sends pipelined requests with the Redis byte-string protocol:
      *<num of args> crlf
        [$<size of arg> crlf
          <arg payload> crlf ...]"
  [^BufferedOutputStream out reqs]
  ;; Prepare the complete batch before any write so one invalid later request
  ;; cannot follow already-sent commands.
  (let [reqs (mapv prepare-command reqs)]
    (enc/run! (fn [req-args] (write-command out req-args)) reqs))
  (.flush out))
