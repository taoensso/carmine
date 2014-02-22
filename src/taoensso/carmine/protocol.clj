(ns taoensso.carmine.protocol
  "Facilities for actually communicating with Redis server using its
  request/response protocol. Originally adapted from Accession.

  Ref: http://redis.io/topics/protocol"
  {:author "Peter Taoussanis"}
  (:require [clojure.string         :as str]
            [taoensso.carmine.utils :as utils]
            [taoensso.nippy         :as nippy]
            [taoensso.nippy.tools   :as nippy-tools])
  (:import  [java.io DataInputStream BufferedOutputStream]
            [clojure.lang Keyword]))

;;; Outline (Carmine v3+)
;; * Dynamic context is established with `carmine/wcar`.
;; * Commands executed w/in this context push their requests (vectors) into
;;   context's request queue. Requests may have metadata for Cluster keyslots &
;;   parsers. Parsers may have metadata as a convenient+composable way of
;;   communicating special request requirements (:raw-bulk?, :thaw-opts, etc.).
;; * On `with-reply`, nested `wcar`, or `execute-requests` - queued requests
;;   will actually be sent to server as pipeline.
;; * For non-listener modes, corresponding replies will then immediately be
;;   received, parsed, + returned.

;;;; Dynamic context stuff

(defrecord Context [conn      ; active Connection
                    req-queue ; [<pulled-reqs> [<queued-req> ...]] atom
                    ])
(def ^:dynamic *context* nil) ; Context
(def ^:dynamic *parser*  nil) ; ifn (with optional meta) or nil
(def no-context-ex
  (Exception.
   (str "Redis commands must be called within the context of a"
        " connection to Redis server. See `wcar`.")))

;;;;

(defmacro ^:private bytestring
  "Redis communicates with clients using a (binary-safe) byte string protocol.
  This is the equivalent of the byte array representation of a Java String."
  [s] `(.getBytes ~s "UTF-8"))

;;; Request delimiters
(def ^bytes bs-crlf (bytestring "\r\n"))
(def ^:const ^Integer bs-* (int (first (bytestring "*"))))
(def ^:const ^Integer bs-$ (int (first (bytestring "$"))))

;; Carmine-only markers that'll be used _within_ bulk data to indicate that
;; the data requires special reply handling
(def ^bytes bs-bin (bytestring "\u0000<")) ; Binary data marker
(def ^bytes bs-clj (bytestring "\u0000>")) ; Frozen data marker

(defrecord WrappedRaw [ba])
(defn raw "Forces byte[] argument to be sent to Redis as raw, unencoded bytes."
  [x] (if (utils/bytes? x) (->WrappedRaw x)
          (throw (Exception. "Raw arg must be byte[]"))))

(defprotocol IRedisArg (coerce-bs [x] "x -> [<ba> <meta>]"))
(extend-protocol IRedisArg
  String  (coerce-bs [x] [(bytestring x) nil])
  Keyword (coerce-bs [x] [(bytestring ^String (utils/fq-name x)) nil])

  ;;; Simple number types (Redis understands these)
  Long    (coerce-bs [x] [(bytestring (str x)) nil])
  Double  (coerce-bs [x] [(bytestring (str x)) nil])
  Float   (coerce-bs [x] [(bytestring (str x)) nil])
  Integer (coerce-bs [x] [(bytestring (str x)) nil])

  ;;;
  WrappedRaw (coerce-bs [x] [(:ba x) :raw])
  nil        (coerce-bs [x] [(nippy-tools/freeze x) :mark-frozen])
  Object     (coerce-bs [x] [(nippy-tools/freeze x) :mark-frozen]))

(extend utils/bytes-class IRedisArg {:coerce-bs (fn [x] [x :mark-bytes])})

;;; Macros to actually send data to stream buffer
(defmacro ^:private send-crlf [out] `(.write ~out bs-crlf 0 2))
(defmacro ^:private send-bin  [out] `(.write ~out bs-bin  0 2))
(defmacro ^:private send-clj  [out] `(.write ~out bs-clj  0 2))
(defmacro ^:private send-*    [out] `(.write ~out bs-*))
(defmacro ^:private send-$    [out] `(.write ~out bs-$))
(defmacro ^:private send-arg
  "Send arbitrary argument along with information about its size:
  $<size of arg> crlf
  <arg data>     crlf

  Argument type will determine how it'll be stored with Redis:
    * String args become byte strings.
    * Simple numbers (integers, longs, floats, doubles) become byte strings.
    * Binary (byte array) args go through un-munged.
    * Everything else gets serialized."
  [out arg]
  `(let [out#          ~out
         arg#          ~arg
         [ba# meta#]   (coerce-bs arg#)
         ba#           (bytes   ba#)
         payload-size# (alength ba#)
         data-size#    (case meta# (:mark-bytes :mark-frozen) (+ payload-size# 2)
                             payload-size#)]

     ;; Reserve null first-byte for marked reply identification
     (when (and (nil? meta#) (> payload-size# 0) (zero? (aget ba# 0)))
       (throw (Exception. (str "Args can't begin with null terminator: " arg#))))

     (send-$ out#) (.write out# (bytestring (str data-size#))) (send-crlf out#)
     (case meta# :mark-bytes  (send-bin out#)
                 :mark-frozen (send-clj out#)
                 nil)
     (.write out# ba# 0 payload-size#)
     (send-crlf out#)))

(defn- send-requests [^BufferedOutputStream out requests]
  "Sends requests to Redis server using its byte string protocol:
    *<no. of args>     crlf
    [$<size of arg N>  crlf
      <arg data>       crlf ...]"
  ;; {:pre [(vector? requests)]}
  (doseq [req-args requests]
    (when (pos? (count req-args)) ; [] req is dummy req for `return`
      (send-* out)
      (.write out (bytestring (str (count req-args))))
      (send-crlf out)
      (doseq [arg req-args] (send-arg out arg))
      (comment (.flush out))))
  (.flush out))

(defn get-unparsed-reply
  "Implementation detail.
  BLOCKS to receive a single reply from Redis server and returns the result as
  [<type> <reply>]. Redis will reply to commands with different kinds of replies,
  identified by their first byte, Ref. http://redis.io/topics/protocol:

    * `+` for simple strings -> <string>
    * `:` for integers       -> <long>
    * `-` for error strings  -> <ex-info>
    * `$` for bulk strings   -> <clj>/<raw-bytes>    ; Marked as serialized
                             -> <bytes>/<raw-bytes>  ; Marked as binary
                             -> <string>/<raw-bytes> ; Unmarked
                             -> nil
    * `*` for arrays         -> <vector>
                             -> nil"
  [^DataInputStream in req-opts]
  (let [reply-type (char (.readByte in))]
    (case reply-type
      \+ (.readLine in)
      \: (Long/parseLong (.readLine in))
      \- (let [err-str    (.readLine in)
               err-prefix (re-find #"^\S+" err-str) ; "ERR", "WRONGTYPE", etc.
               err-prefix (when err-prefix
                            (-> err-prefix str/lower-case keyword))]
           (ex-info err-str (if-not err-prefix {} {:prefix err-prefix})))

      ;;; Bulk strings need checking for special in-data markers
      \$ (let [data-size (Integer/parseInt (.readLine in))]
           (if (== data-size -1) nil
             (if (:raw-bulk? req-opts)
               (let [data (byte-array data-size)]
                 (.readFully in data 0 data-size)
                 (.readFully in (byte-array 2) 0 2) ; Discard final crlf
                 data)

               (let [maybe-marked-type? (>= data-size 2)
                     type (if-not maybe-marked-type? :str
                            (let [h (byte-array 2)]
                              (.mark      in 2)
                              (.readFully in h 0 2)
                              (condp utils/ba= h
                                bs-clj :clj
                                bs-bin :bin
                                :str)))
                     marked-type? (not (identical? type :str))
                     payload-size (int (if marked-type? (- data-size 2) data-size))
                     payload      (byte-array payload-size)]

                 (when (and maybe-marked-type? (not marked-type?)) (.reset in))
                 (.readFully in payload 0 payload-size)
                 (.readFully in (byte-array 2) 0 2) ; Discard final crlf

                 (try
                   (case type
                     :str (String. payload 0 payload-size "UTF-8")
                     :clj (if-let [thaw-opts (:thaw-opts req-opts)]
                            (nippy/thaw payload thaw-opts)
                            (nippy/thaw payload))
                     :bin payload)
                   (catch Exception e
                     (Exception. (str "Bad reply data: " (.getMessage e)) e)))))))

      \* (let [bulk-count (Integer/parseInt (.readLine in))]
           (if (== bulk-count -1) nil ; Nb was [] with < Carmine v3
             (utils/repeatedly* bulk-count (get-unparsed-reply in req-opts))))

      (throw (Exception. (format "Server returned unknown reply type: %s"
                           (str reply-type)))))))

(defn get-parsed-reply "Implementation detail."
  [^DataInputStream in ?parser]
  (let [;; As an implementation detail, parser metadata is used as req-opts:
        ;; {:keys [raw-bulk? thaw-opts dummy-reply :parse-exceptions?]}. We
        ;; could instead choose to split parsers and req metadata but bundling
        ;; the two is efficient + quite convenient in practice. Note that we
        ;; choose to _merge_ parser metadata during parser comp.
        req-opts (meta ?parser)
        unparsed-reply (if (contains? req-opts :dummy-reply) ; May be nil!
                         (:dummy-reply req-opts)
                         (get-unparsed-reply in req-opts))]
    (if-not ?parser unparsed-reply ; Common case
      (if (and (instance? Exception unparsed-reply)
               ;; Nb :parse-exceptions? is rare & not normally used by lib
               ;; consumers. Such parsers need to be written to _not_ interfere
               ;; with our ability to interpret Cluster error messages.
               (not (:parse-exceptions? req-opts)))
        unparsed-reply ; Return unparsed
        (try (?parser unparsed-reply)
             (catch Exception e
               (Exception. (format "Parser error: %s" (.getMessage e)) e)))))))

;;;; Parsers

(defmacro parse
  "Wraps body so that replies to any wrapped Redis commands will be parsed with
  `(f reply)`. Replaces any current parser; removes parser when `f` is nil.
  See also `parser-comp`."
  [f & body] `(binding [*parser* ~f] ~@body))

(defn parser-comp "Composes parsers when f or g are nnil, preserving metadata."
  [f g] (let [m (merge (meta g) (meta f))]
          (with-meta (utils/comp-maybe f g) m)))

;;; Special parsers used to communicate metadata to request enqueuer:
(defmacro parse-raw [& body] `(parse (with-meta identity {:raw-bulk? true}) ~@body))
(defmacro parse-nippy [thaw-opts & body]
  `(parse (with-meta identity {:thaw-opts ~thaw-opts}) ~@body))

(defn return
  "Takes value and returns it unchanged as part of next reply from Redis server.
  Unlike `echo`, does not actually send any data to Redis."
  [value]
  (swap! (:req-queue *context*)
    (fn [[_ q]]
      [nil (conj q (with-meta [] ; Dummy request
                     {:parser (parser-comp *parser* ; Nb keep context's parser
                                (with-meta identity {:dummy-reply value}))
                      :expected-keyslot nil ; Irrelevant
                      }))])))

;;;; Requests

(defn execute-requests
  "Implementation detail.
  Sends given/dynamic requests to given/dynamic Redis server and optionally
  blocks to receive the relevant queued (pipelined) parsed replies."

  ;; For use with standard dynamic bindings:
  ([get-replies? replies-as-pipeline?]
     (let [{:keys [conn req-queue]} *context*
           requests ; Atomically pull reqs from dynamic queue:
           (-> (swap! req-queue (fn [[_ q]] [q []]))
               (nth 0))]
       (execute-requests conn requests get-replies? replies-as-pipeline?)))

  ;; For use with Cluster, etc.:
  ([conn requests get-replies? replies-as-pipeline?]
     (let [nreqs (count requests)]
       (when (pos? nreqs)
         (let [{:keys [in out]} conn]
           (when-not (and in out) (throw no-context-ex))
           ;; (println "Sending requests: " requests)
           (send-requests out requests)
           (when get-replies?
             (if (or (> nreqs 1) replies-as-pipeline?)
               (mapv (fn [req] (get-parsed-reply in (:parser (meta req))))
                     requests)
               (let [req (nth requests 0)
                     one-reply (get-parsed-reply in (:parser (meta req)))]
                 (if (instance? Exception one-reply) (throw one-reply)
                   one-reply)))))))))

;;;; General-purpose macros

(defmacro with-replies*
  "Implementation detail.
  Light, fresh-context version of `with-replies`. Useful for consumers that'll
  be in complete control of the connection/context."
  {:arglists '([:as-pipeline & body] [& body])}
  [& [s1 & sn :as sigs]]
  (let [as-pipeline? (identical? s1 :as-pipeline)
        body         (if as-pipeline? sn sigs)]
    `(do ~@body (execute-requests :get-replies ~as-pipeline?))))

(defmacro with-replies
  "Alpha - subject to change.
  Evaluates body, immediately returning the server's response to any contained
  Redis commands (i.e. before enclosing context ends).

  As an implementation detail, stashes and then `return`s any replies already
  queued with Redis server: i.e. should be compatible with pipelining.

  Note on parsers: if you're writing a Redis command (e.g. a fn that is intended
  to execute w/in an implicit connection context) and you're using `with-replies`
  as an implementation detail (i.e. you're interpreting replies internally), you
  probably want `(parse nil (with-replies ...))` to keep external parsers from
  leaking into your internal logic."
  {:arglists '([:as-pipeline & body] [& body])}
  [& [s1 & sn :as sigs]]
  (let [as-pipeline? (identical? s1 :as-pipeline)
        body         (if as-pipeline? sn sigs)]
    `(let [?stashed-replies# (execute-requests :get-replies :as-pipeline)]
       (try
         ;; This'll be returned to `with-replies` caller:
         (do ~@body (execute-requests :get-replies ~as-pipeline?))

         ;; Then we'll restore any stashed replies to underlying context:
         (finally
           (when ?stashed-replies#
             (parse nil ; Already parsed on stashing
               (mapv return ?stashed-replies#))))))))

(defmacro with-context "Implementation detail."
  [conn & body]
  `(binding [*context* (->Context ~conn (atom [nil []]))
             *parser*  nil]
     ~@body))
