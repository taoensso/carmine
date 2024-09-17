(ns ^:no-doc taoensso.carmine-v4.resp.common
  "Private ns, implementation detail."
  (:refer-clojure :exclude [binding])
  (:require
   [taoensso.encore :as enc :refer [binding]]
   [taoensso.carmine-v4.classes])

  (:import
   [java.nio.charset StandardCharsets]
   [java.io DataInput DataInputStream]
   [clojure.lang ExceptionInfo]

   [taoensso.carmine_v4.classes ReplyError]))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-thaw?*
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*
  ^:dynamic taoensso.carmine-v4/*issue-83-workaround?*)

(alias 'core 'taoensso.carmine-v4)

(comment (remove-ns 'taoensso.carmine-v4.resp.common))

;;;; Utils

(def ba-crlf (enc/str->utf8-ba "\r\n"))

(defn ba->in ^DataInputStream [^bytes ba]
  (-> ba
    java.io.ByteArrayInputStream.
    java.io.BufferedInputStream.
    DataInputStream.))

(defn str->in ^DataInputStream [^String s] (ba->in (.getBytes s StandardCharsets/UTF_8)))

(defmacro with-out
  "Body -> bytes"
  [& body]
  `(let [baos# (java.io.ByteArrayOutputStream.)
         ~'out (java.io.BufferedOutputStream. baos#)]
     (do ~@body)
     (.flush       ~'out)
     (.toByteArray baos#)))

(defmacro with-out->str [& body] `(enc/utf8-ba->str (with-out ~@body)))
(defmacro with-out->in  [& body] `(ba->in           (with-out ~@body)))

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
  (defn xs->ba            ^bytes [& xs]         (xseq->ba false xs)))

(defn skip1 ^DataInputStream [^DataInputStream in] (.skipBytes in 1) in)

;;;; Blob markers

(do
  (def ba-npy (enc/str->utf8-ba "\u0000>"))
  (def ba-bin (enc/str->utf8-ba "\u0000<"))
  (def ba-nil (enc/str->utf8-ba "\u0000_")))

(defn read-blob-?marker
  "Returns e/o {nil :nil :bin :npy}, and possibly advances position
  in stream to skip (consume) any blob markers (`ba-npy`, etc.).
  Won't be called if `*auto-thaw?*` is false."
  [^DataInputStream in ^long n]
  (when (>= n 2) ; >= 2 for marker+?payload
    (.mark in 2)
    (if-not (== (.readByte in) 0) ; Possible marker iff 1st byte null
      (do (.reset in) nil)
      (enc/case-eval (.readByte in) ; 2nd byte would identify marker kind
        (int \_) :nil ; ba-nil
        (int \>) :npy ; ba-npy
        (int \<)      ; ba-bin
        (enc/cond
          (not core/*issue-83-workaround?*) :bin
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

(defn throw! [x] (throw (ex-info "Simulated throw" {:arg (enc/typed-val x)})))

(defn reply-error
  "Returns a exception that's an instance of both `ExceptionInfo` and `ReplyError`.
  Useful for distinguishing reply errors generated by Carmine/Redis, and errors
  possibly serialized as user data."
  ([msg data cause] (proxy [ExceptionInfo ReplyError] [msg data cause]))
  ([msg data      ] (proxy [ExceptionInfo ReplyError] [msg data]))
  ([ex]
   (if-let [cause (enc/ex-cause ex)]
     (proxy [ExceptionInfo ReplyError] [(enc/ex-message ex) (or (ex-data ex) {}) cause])
     (proxy [ExceptionInfo ReplyError] [(enc/ex-message ex) (or (ex-data ex) {})]))))

(comment
  (instance? ExceptionInfo (reply-error "msg" {}))
  (instance? ReplyError    (reply-error "msg" {})))

(defn ^:public reply-error?
  "Returns true iff given argument is an `ExceptionInfo` generated by Carmine
  to indicate a Redis reply error.

  Useful to distinguish between reply errors generated by Carmine/Redis,
  and errors possibly serialized as user data.

  If `ex-data-submap` is provided, it must also be a submap of the
  exception's `ex-data`:
    (reply-error? {:eid :carmine.read/parser-error} my-error)"
  ([               x] (instance? ReplyError x))
  ([ex-data-submap x]
   (and
     (instance? ReplyError x)
     (enc/submap? (ex-data x) ex-data-submap))))

;;;; Stream discards

(let [ref-b (int \;)]
  (defn discard-stream-separator
    [^DataInputStream in]
    ;; (.skip 1)
    (let [read-b (.readByte in)] ; -1 if nothing to read
      (if (== ref-b read-b)
        true
        (throw
          (ex-info "[Carmine] Missing stream separator"
            {:eid :carmine.read/missing-stream-separator
             :read {:as-byte read-b :as-char (char read-b)}}))))))

(defn discard-crlf
  [^DataInputStream in]
  ;; (.skip 2)
  (let [s (.readLine in)] ; nil if nothing to read
    (if (= s "")
      true
      (throw
        (ex-info "[Carmine] Missing CRLF"
          {:eid :carmine.read/missing-crlf
           :read s})))))

;;;; Sentinels
;; We avoid keywords for flow control due to risk of malicious user data

(do
  (defonce sentinel-null-reply              (Object.))
  (defonce sentinel-skipped-reply           (Object.))
  (defonce sentinel-end-of-aggregate-stream (Object.)))

;;;; Read mode

(def ^:dynamic *read-mode*
  "Special read mode, e/o {nil :skip :bytes <ReadThawed>}.
  Applies mostly to blobs, except notably `:skip`."
  nil)

(defmacro ^:public skip-replies
  "Establishes special read mode that discards any Redis replies
  to requests in body."
  [& body] `(binding [*read-mode* :skip] ~@body))

(defmacro ^:public normal-replies
  "Cancels any active special read mode for body."
  [& body]
  `(let [body-fn (fn [] ~@body)]
     (enc/if-not *read-mode*
       (do                        (body-fn)) ; Common case optimization
       (binding [*read-mode* nil] (body-fn)))))

(defmacro ^:public as-bytes
  "Establishes special read mode that returns raw byte arrays
  for any blob-type Redis replies to requests in body."
  [& body] `(binding [*read-mode* :bytes] ~@body))

(defmacro ^:public thaw
  "Establishes special read mode that will attempt Nippy thawing
  for any blob-type Redis replies to requests in body."
  [thaw-opts & body] `(binding [*read-mode* (ReadThawed. ~thaw-opts)] ~@body))

(deftype ReadThawed [thaw-opts])
(defn read-mode->?thaw-opts [read-mode]
  (when (instance?    ReadThawed read-mode)
    (or (.-thaw-opts ^ReadThawed read-mode) {})))

(def ^:dynamic *natural-reads?* false)

(defmacro ^:public natural-reads
  "Cancels any active special read mode or reply parser for body.
  Equivalent to (unparsed (normal-replies <body>))."
  [& body] `(binding [*natural-reads?* true] ~@body))

;;;; ReadOpts, etc.

(deftype ReadOpts [read-mode parser auto-thaw? keywordize-maps?])

(do
  (enc/defonce read-opts-natural "For \"natural\" reads" (ReadOpts. nil   nil nil  nil))
  (enc/defonce read-opts-skip    "For `:skip` read mode" (ReadOpts. :skip nil nil  nil))
  (enc/defonce read-opts-default "For REPL/tests/etc."   (ReadOpts. nil   nil true true)))

(defn in-aggregate-read-opts
  "Returns `ReadOpts` for internal reading by aggregates.
  We retain (nest) all options but parser."
  ^ReadOpts [^ReadOpts read-opts]
  (ReadOpts.
    (.-read-mode        read-opts)
    #_(.-parser         read-opts) nil
    (.-auto-thaw?       read-opts)
    (.-keywordize-maps? read-opts)))

(declare ^:dynamic *parser* get-parser-opts)

(let [read-opts-natural read-opts-natural
      read-opts-skip    read-opts-skip]

  (defn get-read-opts
    "Returns an appropriate `ReadOpts`."
    (^ReadOpts []
     (if *natural-reads?*
       read-opts-natural

       (let [read-mode *read-mode*]
         (if (identical? read-mode :skip)
           read-opts-skip ; Optimization, all else irrelevant

           (let [parser *parser*]
             ;; Advanced/undocumented: allow parser-opts to influence
             ;; dynamic ReadOpts. This is exactly equivalent to
             ;; (parse <...> (establish-bindings <...>)).
             (if-let [p-opts (get-parser-opts parser)]
               (ReadOpts.
                 (get p-opts :read-mode read-mode)
                 parser
                 (if (contains? p-opts :auto-thaw?)        (get p-opts :auto-thaw?)       core/*auto-thaw?*)
                 (if (contains? p-opts :keywordize-maps?)  (get p-opts :keywordize-maps?) core/*keywordize-maps?*))

               ;; Common case (no parser-opts present)
               (ReadOpts. read-mode parser
                 core/*auto-thaw?*
                 core/*keywordize-maps?*)))))))

    (^ReadOpts [opts] ; For REPL/tests
     (if (empty? opts)
       read-opts-natural
       (let [{:keys [read-mode parser auto-thaw? keywordize-maps?]} opts]
         (ReadOpts.  read-mode parser auto-thaw? keywordize-maps?))))))

(comment (enc/qb 1e6 (get-read-opts))) ; 43.72

(declare describe-parser)

(defn describe-read-opts
  "For error messages, etc."
  [read-opts]
  (when-let [^ReadOpts read-opts read-opts]
    {:read-mode        (.-read-mode        read-opts)
     :parser       (-> (.-parser           read-opts) describe-parser)
     :auto-thaw?       (.-auto-thaw?       read-opts)
     :keywordize-maps? (.-keywordize-maps? read-opts)}))

;;;; Reply parsing
;; We choose to keep parsing pretty simple:
;; no nesting, no auto composition, and no concurrent fn+rf parsers.
;; Note that *read-mode* and *parser* are distinct, and may interact.

(def ^:dynamic *parser* "?<Parser>" nil)

(deftype Parser [kind opts f rfc kv-rf?])
;; rfc: auto-generated (fn rf-constructor []) => <possibly-stateful-rf*>
;; parser-opts:
;;   read-mode            ; nx    ; Currently undocumented
;;   auto-thaw?           ; nx    ; ''
;;   keywordize-maps?     ; nx    ; ''
;;   kv-rf?               ; false ; ''
;;   catch-errors?        ; true  ; ''
;;   parse-error-replies? ; false
;;   parse-null-replies?  ; false

(defn           parser? [x]            (instance? Parser x))
(defn      when-parser  [x] (when      (instance? Parser x)                     x))
(defn   when-fn-parser  [x] (when (and (instance? Parser x) (.-f    ^Parser x)) x))
(defn   when-rf-parser  [x] (when (and (instance? Parser x) (.-rfc  ^Parser x)) x))
(defn- get-parser-opts  [x] (when      (instance? Parser x) (.-opts ^Parser x)))

(defn- describe-parser
  "For error messages, etc."
  [parser]
  (when-let [p ^Parser parser]
    {:opts   (.-opts   p)
     :kind   (.-kind   p)
     :kv-rf? (.-kv-rf? p)}))

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
           :parser-opts parser-opts
           :arg  (enc/typed-val x)})))))

(defn fn-parser ^Parser [parser-opts f]
  (let [parser-opts (not-empty parser-opts)
        f*
        (if (get parser-opts :catch-errors? true)
          (safe-parser-fn parser-opts f)
          (do                         f))]
    (Parser. :fn parser-opts f* nil nil)))

(defn- safe-parser-xrf
  "Returns a stateful transducer to catch any thrown errors in rf. All
  future calls to rf will noop and return that same error. Protects
  reductions from interruption due to parser errors."
  ([        error-data] (safe-parser-xrf (volatile! nil) error-data))
  ([caught_ error-data]
   (fn [rf]
     (enc/catching-rf
       (fn error-fn [extra-data cause] (vreset! caught_ (parser-error cause (conj error-data extra-data))))
       (fn
         ([]        (or @caught_ (rf)))
         ([acc]     (or @caught_ (rf acc)))
         ([acc in]  (or @caught_ (rf acc in)))
         ([acc k v] (or @caught_ (rf acc k v))))))))

(defn rf-parser
  "rf should a reducing fn such that:
    (rf)        => Init     acc
    (rf acc in) => Next     acc (accumulation step)
    (rf acc)    => Complete acc"
  ^Parser [parser-opts ?xform rf]
  (let [parser-opts (not-empty parser-opts)
        kv-rf? (if ?xform false (get parser-opts :kv-rf? false))

        error-data
        (fn [thrown-by]
          {:parser-opts parser-opts
           :xform       ?xform
           :rf          rf
           :thrown-by   thrown-by})

        ?xform
        (if (get parser-opts :catch-errors? true)

          ;; Catch errors
          (let [caught_ (volatile! nil)]
            (if-let [xform ?xform]
              ;; Currently do double wrapping to distinguish
              ;; between :rf and :xform errors
              (comp
                (safe-parser-xrf caught_ (error-data :xform))
                xform
                (safe-parser-xrf caught_ (error-data :rf)))

              (safe-parser-xrf caught_ (error-data :rf))))

          ;; Don't catch errors
          ?xform)

        rf-constructor
        (if-let [xform ?xform]
          (fn rfc [] (xform rf)) ; Possibly stateful
          (fn rfc []        rf))]

    (Parser. :rf parser-opts nil
      rf-constructor kv-rf?)))

(comment (enc/qb 1e6 (rf-parser {} nil (fn [])))) ; 72.61

(defn ^:public completing-rf
  "Like `completing` for parser reducing fn"
  ([rf init   ] (completing-rf rf init identity))
  ([rf init cf]
   (fn
     ([]        init)
     ([acc]     (cf acc))
     ([acc in]  (rf acc in))
     ([acc k v] (rf acc k v)))))

(comment ((crf conj :init)))

(enc/defalias crf completing-rf)

;;;; Reply parsing public API

(defmacro ^:public unparsed
  "Cancels any active reply parsers for body.
  See also `parse`, `parse-aggregates`."
  [& body] `(binding [*parser* nil] ~@body))

(defmacro ^:public parse
  "Establishes given reply parser for body,
    (fn parse-reply [reply]) => <parsed-reply>.

  When reply is an aggregate, parser will be applied
  to the entire aggregate as a single argument
  (vec/set/map).

  Only one parser can be active at a time.
  No parsing will occur *within* aggregates.

  Parser opts include:
    `:parse-error-replies?` (default false)
    `:parse-null-replies?`  (default false)

  Argument to parser may be affected by special read
  modes (`as-bytes`, etc.).

  See also `unparsed`, `parse-aggregates`."
  [opts f & body]
  `(binding [*parser* (fn-parser ~opts ~f)]
     ~@body))

(defmacro ^:public parse-aggregates
  "Advanced feature.

  Establishes given aggregate reply parser for body.
  Expects `rf`, a reducing fn such that:
    (rf)        => Init acc     ; e.g. (transient [])
    (rf acc in) => Next acc     ; e.g. conj!
    (rf acc)    => Complete acc ; e.g. persistent!

  This `rf` will be used to parse the elements of any
  aggregate replies in a highly efficient way.

  A transducer `xform` may be provided, or nil.

  Only one parser can be active at a time.
  Non-aggregate    replies will be unaffected.
  Nested aggregate replies will be unaffected.

  Parser opts include:
    `:parse-null-replies?` (default false)

  Argument to parser may be affected by special read
  modes (`as-bytes`, etc.).

  See also `unparsed`, `parse`, `completing-rf`."
  [opts ?xform rf & body]
  `(binding [*parser* (rf-parser ~opts ~?xform ~rf)]
     ~@body))

(let [opts {:read-mode nil}] ; Sensible assumption?
  (def as-?long-parser   (fn-parser opts enc/as-?int))
  (def as-?double-parser (fn-parser opts enc/as-?float))
  (def as-?kw-parser     (fn-parser opts enc/as-?kw))

  (def as-long-parser    (fn-parser opts enc/as-int))
  (def as-double-parser  (fn-parser opts enc/as-float))
  (def as-kw-parser      (fn-parser opts enc/as-kw)))

(do
  (defmacro ^:public as-?long   [& body] "Establishes reply parser for body: coerce replies to long, or nil."      `(binding [*parser* as-?long-parser]   ~@body))
  (defmacro ^:public as-?double [& body] "Establishes reply parser for body: coerce replies to double, or nil."    `(binding [*parser* as-?double-parser] ~@body))
  (defmacro ^:public as-?kw     [& body] "Establishes reply parser for body: coerce replies to keyword, or nil."   `(binding [*parser* as-?kw-parser]     ~@body))

  (defmacro ^:public as-long    [& body] "Establishes reply parser for body: coerce replies to long, or throw."    `(binding [*parser* as-long-parser]    ~@body))
  (defmacro ^:public as-double  [& body] "Establishes reply parser for body: coerce replies to double, or throw."  `(binding [*parser* as-double-parser]  ~@body))
  (defmacro ^:public as-kw      [& body] "Estbalishes reply parser for body: coerce replies to keyword, or throw." `(binding [*parser* as-kw-parser]      ~@body)))
