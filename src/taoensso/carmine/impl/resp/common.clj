(ns taoensso.carmine.impl.resp.common
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string  :as str]
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInputStream]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.common)
  (test/run-tests 'taoensso.carmine.impl.resp.common))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-deserialize?*
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*
  taoensso.carmine-v4/issue-83-workaround?)

(alias 'core 'taoensso.carmine-v4)

;;;;

(def unicode-string "ಬಾ ಇಲ್ಲಿ ಸಂಭವಿಸ\r\n")
(defn bytes->str ^String [^bytes ba] (String.   ba StandardCharsets/UTF_8))
(defn str->bytes  ^bytes [^String s] (.getBytes s  StandardCharsets/UTF_8))

(deftest ^:private _byte-strings
  [(is (= (bytes->str (str->bytes unicode-string)) unicode-string)
     "UTF_8 charset for byte strings")])

(def ba-crlf (str->bytes "\r\n"))

;;;;

(defn ba->in ^DataInputStream [^bytes ba]
  (-> ba
    java.io.ByteArrayInputStream.
    java.io.BufferedInputStream.
    DataInputStream.))

(defn str->in ^DataInputStream [^String s]
  (ba->in (.getBytes s StandardCharsets/UTF_8)))

(defmacro with-out
  "Body -> bytes"
  [& body]
  `(let [baos# (java.io.ByteArrayOutputStream.)
         ~'out (java.io.BufferedOutputStream. baos#)]
     (do ~@body)
     (.flush       ~'out)
     (.toByteArray baos#)))

(defmacro           with-out->str [& body] `(bytes->str (with-out ~@body)))
(deftest ^:private _with-out->str
  [(is (= (with-out->str (.write out (str->bytes unicode-string))) unicode-string))])

(defmacro           with-out->in [& body] `(ba->in (with-out ~@body)))
(deftest ^:private _with-out->in
  [(is (= (.readLine (with-out->in (.write out (str->bytes "hello\r\n")))) "hello"))])

(defn xseq->ba ^bytes [with-crlfs? xseq]
  (with-out
    (doseq [x xseq]
      (enc/cond!
        (enc/bytes? x) (.write out                 ^bytes x)
        (string?    x) (.write out       (str->bytes      x))
        (int?       x) (.write out       (str->bytes (str x)))
        (char?      x) (.write out                   (int x))
        (vector?    x) (.write out (byte-array (mapv byte x))))

      (when with-crlfs?
        (.write out ^bytes ba-crlf)))))

(do ; Variations useful for tests, etc.
  (defn xs->in+ ^DataInputStream [& xs] (ba->in (xseq->ba true  xs)))
  (defn xs->in  ^DataInputStream [& xs] (ba->in (xseq->ba false xs)))
  (defn xs->ba+           ^bytes [& xs]         (xseq->ba true  xs))
  (defn xs->ba            ^bytes [& xs]         (xseq->ba false xs)))

(deftest ^:private _xs->ba
  [(is (= (bytes->str (xs->ba  "a" "b" 1 (byte-array [(byte \A) (byte \B)]) \C [\d \e])) "ab1ABCde"))
   (is (= (bytes->str (xs->ba+ "a" "b" 1 (byte-array [(byte \A) (byte \B)]) \C [\d \e])) "a\r\nb\r\n1\r\nAB\r\nC\r\nde\r\n"))])

;;;;

(defn skip1 ^DataInputStream [^java.io.DataInput in] (.skipBytes in 1) in)

(deftest ^:private _skip1
  [(is (= (.readLine (skip1 (with-out->in (.write out (str->bytes "+hello\r\n"))))) "hello"))])

;;;; Blob markers

(do
  (def ba-npy (str->bytes "\u0000>"))
  (def ba-bin (str->bytes "\u0000<"))
  (def ba-nil (str->bytes "\u0000_")))

(defn read-blob-?marker
  "Returns e/o {nil :nil :bin :npy}, and possibly advances position
  in stream to skip (consume) any blob markers (`ba-npy`, etc.)."
  [^DataInputStream in ^long n]
  ;; Won't be called if `*auto-deserialize?*` is false
  (when (>= n 2) ; >= 2 for marker+?payload
    (.mark in 2)
    (if-not (== (.readByte in) 0) ; Possible marker iff 1st byte null
      (do (.reset in) nil)
      (enc/case-eval (.readByte in) ; 2nd byte would identify marker kind
        (byte \_) :nil ; ba-nil
        (byte \>) :npy ; ba-npy
        (byte \<)      ; ba-bin
        (enc/cond
          (not core/issue-83-workaround?) :bin
          (< n 7)                         :bin ; >= +5 for Nippy payload (4 header + data)
          :do (.mark in 3)
          (not (== (.readByte in) #=(byte \N))) (do (.reset in) :bin)
          (not (== (.readByte in) #=(byte \P))) (do (.reset in) :bin)
          (not (== (.readByte in) #=(byte \Y))) (do (.reset in) :bin)
          :else                                 (do (.reset in) :npy))

        (do (.reset in) nil)))))

(defn- test-blob-?marker [s]
  (let [^bytes ba (str->bytes s)
        in (ba->in ba)]
    [(read-blob-?marker in (alength ba))
     (.readLine         in)]))

(deftest ^:private _read-blob-?marker
  [(is (= (test-blob-?marker "foo")            [nil "foo"]))
   (is (= (test-blob-?marker "\u0000more")     [nil "\u0000more"]))
   (is (= (test-blob-?marker "\u0000_more")    [:nil "more"]))
   (is (= (test-blob-?marker "\u0000>more")    [:npy "more"]))
   (is (= (test-blob-?marker "\u0000<more")    [:bin "more"]))
   (is (= (test-blob-?marker "\u0000<NPYmore") [:npy "NPYmore"]))
   (is (= (test-blob-?marker "\u0000<NPmore")  [:bin "NPmore"]))
   (is (= (test-blob-?marker "\u0000<Nmore")   [:bin "Nmore"]))])

;; TODO Util for users to parse-?marked-ba -> [<kind> <payload>]

;;;;

;; Wrapper to identify Carmine-generated reply errors for
;; possible throwing or unwrapping
(deftype  CarmineReplyError [ex-info])
(defn     carmine-reply-error  [x] (CarmineReplyError. x))
(defn     carmine-reply-error? [x] (instance? CarmineReplyError x))
(defn get-carmine-reply-error  [x]
  (when (instance? CarmineReplyError x)
    (.-ex-info ^CarmineReplyError x)))

(defn crex-match? [x ex-data-sub]
  (enc/submap? (ex-data (get-carmine-reply-error x))
    ex-data-sub))

;;;;

(let [ref-b (byte \;)]
  (defn discard-stream-separator
    [^DataInputStream in]
    ;; (.skip 1)
    (let [read-b (.readByte in)] ; -1 if nothing to read
      (if (== ref-b read-b)
        true
        (throw
          (ex-info "[Carmine] Missing stream separator"
            {:eid :carmine.resp.read/missing-stream-separator
             :read {:as-byte read-b :as-char (char read-b)}}))))))

(comment :see-tests-below)

(defn discard-crlf
  [^DataInputStream in]
  ;; (.skip 2)
  (let [s (.readLine in)] ; nil if nothing to read
    (if (= s "")
      true
      (throw
        (ex-info "[Carmine] Missing CRLF"
          {:eid :carmine.resp.read/missing-crlf
           :read s})))))

(deftest ^:private _utils
  [(throws? :common {:eid :carmine.resp.read/missing-stream-separator}
     (discard-stream-separator (xs->in+ "")))

   (is (true? (discard-crlf (xs->in+ ""))))
   (is (throws? :common {:eid :carmine.resp.read/missing-crlf}
         (discard-crlf (xs->in+ "_"))))])

;;;; Read mode

(def ^:dynamic *read-mode*
  "Special read mode, e/o {nil :skip :bytes <AsThawed>}.
  Applies mostly to blobs, except notably :skip."
  nil)

(defmacro skip-replies
  "Establishes special read mode that discards any Redis replies
  to requests in body."
  [& body] `(binding [*read-mode* :skip] ~@body))

(defmacro normal-replies
  "Cancels any active special read mode."
  [& body]
  `(if *read-mode*
     (do ~@body) ; Common case optmization
     (binding [*read-mode* nil]
       ~@body)))

(defmacro as-bytes
  "Establishes special read mode that returns raw byte arrays
  for any blob-type Redis replies to requests in body."
  [& body] `(binding [*read-mode* :bytes] ~@body))

(defmacro as-thawed
  "Establishes special read mode that will attempt Nippy thawing
  for any blob-type Redis replies to requests in body."
  [thaw-opts & body]
  `(binding [*read-mode* (AsThawed. ~thaw-opts)]
     ~@body))

(deftype AsThawed [thaw-opts])
(defn read-mode->?thaw-opts [read-mode]
  (when (instance? AsThawed read-mode)
    (or (.-thaw-opts ^AsThawed read-mode) {})))

;;;; Reply parsing
;; We choose to keep parsing capabilities simple:
;; no nesting, no auto composition, and no concurrent fn+rf parsers.
;;
;; Note that *read-mode* and *parser* are distinct, and may interact.

(def ^:private *parser*
  "Reply parser, e/o #{nil <FnParser> <RfParser>}"
  nil)

;; Parser opts are an advanced/undocumented feature for internal use
(deftype FnParser [fn  parser-opts]) ; Applies only to non-aggregates
(deftype RfParser [rfc parser-opts]) ; Applies only to     aggregates

(defn- parser-error
  [cause data]
  (carmine-reply-error
    (ex-info "[Carmine] Reply parser threw an error"
      (conj
        {:eid :carmine.resp.read/parser-error}
        data)
      cause)))

(defn- catching-parser-fn [f parser-opts]
  (fn catching-parser-fn [x]
    (try
      (f x)
      (catch Throwable t
        (parser-error t
          {:kind        :fn-parser
           :parser-opts parser-opts
           :arg         {:value x :type (type x)}})))))

(defn fn-parser [f parser-opts]
  (let [f
        (if (get parser-opts :no-catch?) ; Undocumented
          (do                 f)
          (catching-parser-fn f parser-opts))]
    (FnParser. f (not-empty parser-opts))))

(defn- catching-parser-rfc
  "Returns a stateful parser-rf that'll catch errors. Once the first
  error is caught, all future calls to rf will noop and return that
  first error. Nb: this parser-rf will allow entire stream to be read,
  even in the event of a parsing error."
  [rfc parser-opts]
  (fn catching-rf-constructor []
    (let [;; Once first error is caught, all future calls will noop
          ;; and just return that first error. This'll allow the stream
          ;; to still be read, while retaining the correct (first) error.
          caught_ (volatile! nil)
          catch!  (fn [t data] (vreset! caught_ (parser-error t data)))
          rf
          (try
            (rfc)
            (catch Throwable t
              (catch! t
                {:kind        :rf-parser-constructor
                 :parser-opts parser-opts
                 :arity       '[]})))]

      (fn catching-parser-rf
        ([      ] (try (or @caught_ (rf))        (catch Throwable t (catch! t {:kind :rf-parser :parser-opts parser-opts :arity '[]}))))
        ([acc   ] (try (or @caught_ (rf acc))    (catch Throwable t (catch! t {:kind :rf-parser :parser-opts parser-opts :arity '[acc] :acc {:value acc :type (type acc)}}))))
        ([acc in] (try (or @caught_ (rf acc in)) (catch Throwable t (catch! t {:kind :rf-parser :parser-opts parser-opts :arity '[acc in]
                                                                               :acc {:value acc :type (type acc)}
                                                                               :in  {:value in  :type (type in)}}))))))))

(defn rf-parser
  "(rf-constructor) should return a (possibly stateful) reducing fn (rf)
  for parsing aggregates such that:
    (rf)         => Init     acc
    (rf acc in)  => Next     acc (accumulate step)
    (rf end-acc) => Complete acc"
  [rf-constructor parser-opts]
  (let [rfc
        (if (get parser-opts :no-catch?) ; Undocumented
          (do                  rf-constructor)
          (catching-parser-rfc rf-constructor parser-opts))]
    (RfParser. rfc (not-empty parser-opts))))

(defn get-parser-fn   [parser] (when (instance? FnParser parser) (.-fn  ^FnParser parser)))
(defn get-parser-rfc  [parser] (when (instance? RfParser parser) (.-rfc ^RfParser parser)))
(defn get-parser-opts [parser]
  (cond
    (instance? FnParser parser) (.-parser-opts ^FnParser parser)
    (instance? RfParser parser) (.-parser-opts ^RfParser parser)))

(defn describe-parser
  "For error messages, etc."
  [parser]
  (when parser
    (cond
      (instance? FnParser parser) {:kind :fn :fn  (.-fn  ^FnParser parser) :parser-opts (.-parser-opts ^FnParser parser)}
      (instance? RfParser parser) {:kind :rf :rfc (.-rfc ^RfParser parser) :parser-opts (.-parser-opts ^RfParser parser)})))

(comment
  (describe-parser (fn-parser identity                 {:a :A}))
  (describe-parser (rf-parser (fn [] (constantly nil)) {:a :A})))

;;;; ReadOpts, etc.

(deftype Request  [read-opts req-args])
(deftype ReadOpts [read-mode parser auto-deserialize? keywordize-maps?])

(def     nil-read-opts (ReadOpts. nil nil  nil  nil))
(def default-read-opts (ReadOpts. nil nil true true)) ; For REPL/tests, etc.

(defn inner-read-opts
  "Returns ReadOpts for internal reading by aggregates.
  We retain (nest) all options but parser."
  ^ReadOpts [^ReadOpts read-opts]
  (ReadOpts.
    (.-read-mode         read-opts)
    (.-auto-deserialize? read-opts)
    (.-keywordize-maps?  read-opts)
    nil))

(let [skipping-read-opts (ReadOpts. :skip nil nil nil)]
  (defn new-read-opts
    (^ReadOpts []
     (let [read-mode *read-mode*]
       (if (identical? read-mode :skip)
         skipping-read-opts ; Optimization, all else irrelevant
         (let [parser *parser*]

           ;; Advanced/undocumented: allow parser-opts to influence
           ;; dynamic ReadOpts. This is exactly equivalent to
           ;; (parse <...> (establish-bindings <...>)).
           (if-let [parser-opts (get-parser-opts parser)]
             (ReadOpts.
               (get parser-opts :read-mode read-mode)
               (if-let [e (find parser-opts :auto-deserialize?)] (val e) core/*auto-deserialize?*)
               (if-let [e (find parser-opts :keywordize-maps?)]  (val e) core/*keywordize-maps?*)
               parser)

             ;; Common case (no parser-opts present)
             (ReadOpts.
               read-mode
               core/*auto-deserialize?*
               parser
               core/*keywordize-maps?*))))))

    (^ReadOpts [opts] ; For REPL/tests
     (if (empty? opts)
       nil-read-opts
       (let [{:keys [read-mode auto-deserialize? parser keywordize-maps?]} opts]
         (ReadOpts.  read-mode auto-deserialize? parser keywordize-maps?))))))

(comment (enc/qb 1e6 (new-read-opts))) ; 54.84

(defn describe-read-opts
  "For error messages, etc."
  [read-opts]
  (when-let [^ReadOpts read-opts read-opts]
    {:read-mode             (.-read-mode         read-opts)
     :auto-deserialize?     (.-auto-deserialize? read-opts)
     :keywordize-maps?      (.-keywordize-maps?  read-opts)
     :parser            (-> (.-parser            read-opts) describe-parser)}))
