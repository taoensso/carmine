(ns taoensso.carmine.impl.resp.read.common
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string  :as str]
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]
   [taoensso.carmine.impl.resp.common :as resp-com
    :refer [str->bytes bytes->str xs->in+]]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.read.common)
  (test/run-tests 'taoensso.carmine.impl.resp.read.common))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-deserialize?*
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*)

(alias 'core 'taoensso.carmine-v4)

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
  (resp-com/carmine-reply-error
    (ex-info "[Carmine] Reply parser threw an error"
      (conj
        {:eid :carmine.resp.read/parser-error}
        data)
      cause)))

(defn- catching-parser-fn [parser-opts f]
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
  [parser-opts rfc]
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
  [parser-opts rf-constructor]
  (let [rfc
        (if (get parser-opts :no-catch?) ; Undocumented
          (do                              rf-constructor)
          (catching-parser-rfc parser-opts rf-constructor))]
    (RfParser. rfc (not-empty parser-opts))))

(deftest ^:private _catching-parser-rfc
  (let [rfc
        (catching-parser-rfc {}
          (fn []
            (fn rf
              ([      ]        :init)
              ([acc   ]        :complete)
              ([acc in] (if in :accumulate (throw (Exception. "Ex")))))))

        rf1 (rfc)
        rf2 (rfc)]

    [(is (= (rf1)          :init))
     (is (= (rf1 nil)      :complete))
     (is (= (rf1 nil true) :accumulate))
     (is (resp-com/crex-match? (rf1 nil false) {:eid :carmine.resp.read/parser-error :arity '[acc in]}) "Caught error")
     (is (resp-com/crex-match? (rf1)           {:eid :carmine.resp.read/parser-error :arity '[acc in]}) "Repeats 1st error")
     (is (= (rf2) :init) "Constructor returns unique stateful rfs")]))

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
    #_(.-parser          read-opts) nil
    (.-auto-deserialize? read-opts)
    (.-keywordize-maps?  read-opts)))

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
               parser
               (if-let [e (find parser-opts :auto-deserialize?)] (val e) core/*auto-deserialize?*)
               (if-let [e (find parser-opts :keywordize-maps?)]  (val e) core/*keywordize-maps?*))

             ;; Common case (no parser-opts present)
             (ReadOpts. read-mode parser
               core/*auto-deserialize?*
               core/*keywordize-maps?*))))))

    (^ReadOpts [opts] ; For REPL/tests
     (if (empty? opts)
       nil-read-opts
       (let [{:keys [read-mode parser auto-deserialize? keywordize-maps?]} opts]
         (ReadOpts.  read-mode parser auto-deserialize? keywordize-maps?))))))

(comment (enc/qb 1e6 (new-read-opts))) ; 54.84

(defn describe-read-opts
  "For error messages, etc."
  [read-opts]
  (when-let [^ReadOpts read-opts read-opts]
    {:read-mode             (.-read-mode         read-opts)
     :parser            (-> (.-parser            read-opts) describe-parser)
     :auto-deserialize?     (.-auto-deserialize? read-opts)
     :keywordize-maps?      (.-keywordize-maps?  read-opts)}))
