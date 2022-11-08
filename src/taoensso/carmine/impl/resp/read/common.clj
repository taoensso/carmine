(ns taoensso.carmine.impl.resp.read.common
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]
   [taoensso.carmine.impl.resp.common :as resp-com]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.read.common)
  (test/run-tests 'taoensso.carmine.impl.resp.read.common))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-deserialize?*
  ^:dynamic taoensso.carmine-v4/*keywordize-maps?*
  ^:dynamic taoensso.carmine.impl.resp.read.parsing/*parser*
            taoensso.carmine.impl.resp.read.parsing/get-parser-opts
            taoensso.carmine.impl.resp.read.parsing/describe-parser)

(alias 'core    'taoensso.carmine-v4)
(alias 'parsing 'taoensso.carmine.impl.resp.read.parsing)

;;;; Sentinels
;; As a security measure, will avoid the use of keywords for
;; flow control due to the risk of malicious user data

(defonce sentinel-null-reply              (Object.))
(defonce sentinel-skipped-reply           (Object.))
(defonce sentinel-end-of-aggregate-stream (Object.))

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
         (let [parser parsing/*parser*]

           ;; Advanced/undocumented: allow parser-opts to influence
           ;; dynamic ReadOpts. This is exactly equivalent to
           ;; (parse <...> (establish-bindings <...>)).
           (if-let [parser-opts (parsing/get-parser-opts parser)]
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

(comment (enc/qb 1e6 (new-read-opts))) ; 40.46

(defn describe-read-opts
  "For error messages, etc."
  [read-opts]
  (when-let [^ReadOpts read-opts read-opts]
    {:read-mode             (.-read-mode         read-opts)
     :parser            (-> (.-parser            read-opts) parsing/describe-parser)
     :auto-deserialize?     (.-auto-deserialize? read-opts)
     :keywordize-maps?      (.-keywordize-maps?  read-opts)}))
