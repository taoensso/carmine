(ns taoensso.carmine.protocol
  "Facilities for actually communicating with Redis server using its
  request/response protocol. Originally adapted from Accession.

  Ref: http://redis.io/topics/protocol"
  {:author "Peter Taoussanis"}
  (:require [clojure.string         :as str]
            [taoensso.carmine.utils :as utils]
            [taoensso.nippy.tools   :as nippy-tools])
  (:import  [java.io DataInputStream BufferedOutputStream]
            [clojure.lang Keyword]))

(defrecord Context [conn in-stream out-stream parser-queue])
(def ^:dynamic *context* nil)
(def ^:dynamic *parser*  nil)
(def ^:private no-context-error
  (Exception. (str "Redis commands must be called within the context of a"
                   " connection to Redis server. See `wcar`.")))

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

(def ^:dynamic *listener-req-mode?* "Implementation detail." nil)
(defmacro with-listener-req-mode "Implementation detail." [& body]
  `(binding [*listener-req-mode?* true] ~@body))

(defn send-request*
  "Sends a command to Redis server using its byte string protocol:
      *<no. of args>     crlf
      [$<size of arg N>  crlf
        <arg data>       crlf ...]"
  [args]
  (let [^BufferedOutputStream out (or (:out-stream *context*)
                                      (throw no-context-error))]

    (send-* out) (.write out (bytestring (str (count args)))) (send-crlf out)
    (doseq [arg args] (send-arg out arg))
    (.flush out)

    (when-not *listener-req-mode?*
      (swap! (:parser-queue *context*) conj *parser*))))

(def ^:dynamic send-request send-request*)

(defn get-basic-reply
  "BLOCKS to receive a single reply from Redis server. Applies basic parsing
  and returns the result.

  Redis will reply to commands with different kinds of replies, identified by
  their first byte:
      * `+` for single line reply.
      * `-` for error message.
      * `:` for integer reply.
      * `$` for bulk reply.
      * `*` for multi bulk reply."
  [^DataInputStream in raw?]
  (let [reply-type (char (.readByte in))]
    (case reply-type
      \+ (.readLine in)
      \- (Exception.     (.readLine in))
      \: (Long/parseLong (.readLine in))

      ;; Bulk replies need checking for special in-data markers
      \$ (let [data-size (Integer/parseInt (.readLine in))]
           (when-not (neg? data-size) ; NULL bulk reply
             (let [maybe-marked-type? (and (not raw?) (>= data-size 2))
                   type (or (when maybe-marked-type?
                              (.mark in 2)
                              (let [h (byte-array 2)]
                                (.readFully in h 0 2)
                                (condp utils/ba= h
                                  bs-clj :clj
                                  bs-bin :bin
                                  nil)))
                            (if raw? :raw :str))

                   marked-type? (case type (:clj :bin) true false)
                   payload-size (int (if marked-type? (- data-size 2) data-size))
                   payload      (byte-array payload-size)]

               (when (and maybe-marked-type? (not marked-type?)) (.reset in))
               (.readFully in payload 0 payload-size)
               (.readFully in (byte-array 2) 0 2) ; Discard final crlf

               (try
                 (case type
                   :str (String. payload 0 payload-size "UTF-8")
                   :clj (nippy-tools/thaw payload)
                   (:bin :raw) payload)
                 (catch Exception e
                   (Exception. (str "Bad reply data: " (.getMessage e)) e))))))

      \* (let [bulk-count (Integer/parseInt (.readLine in))]
           (utils/repeatedly* bulk-count (get-basic-reply in raw?)))
      (throw (Exception. (str "Server returned unknown reply type: "
                              reply-type))))))

;;;; Parsers

(defmacro parse
  "Wraps body so that replies to any wrapped Redis commands will be parsed with
  `(f reply)`. Replaces any current parser; removes parser when `f` is nil.
  See also `parser-comp`."
  [f & body] `(binding [*parser* ~f] ~@body))

(defn parser-comp "Composes parsers when f or g are nnil, preserving metadata."
  [f g] (let [m (merge (meta g) (meta f))]
          (with-meta (utils/comp-maybe f g) m)))

(defn return
  "Takes value and returns it unchanged as part of next reply from Redis server.
  Unlike `echo`, does not actually send any data to Redis."
  [value]
  (let [vfn (with-meta identity {:dummy-reply value})]
    (swap! (:parser-queue *context*) conj (parser-comp *parser* vfn))))

(defn- get-parsed-reply [^DataInputStream in parser]
  (if-not parser
    (get-basic-reply in false) ; Common case
    (let [;; Implementation detail! We use metadata to control optional
          ;; reply/parser opts. Metadata is *merged* on parser composition.
          {:keys [dummy-reply raw? parse-exceptions? thaw-opts] :as m} (meta parser)
          reply (cond (contains? m :dummy-reply) dummy-reply
                      thaw-opts (nippy-tools/with-thaw-opts thaw-opts
                                  (get-basic-reply in raw?))
                      :else     (get-basic-reply in raw?))]
      (if (and (instance? Exception reply) (not parse-exceptions?))
        reply ; Pass through w/o parsing
        (try (parser reply)
             (catch Exception e
               (Exception. (format "Parser error: %s" (.getMessage e)) e)))))))

(defn get-parsed-replies
  "Implementation detail.
  BLOCKS to receive queued (pipelined) replies from Redis server. Applies all
  parsing and returns the result. Note that Redis returns replies as a FIFO
  queue per connection."
  [as-pipeline?]
  (let [^DataInputStream in (or (:in-stream *context*) (throw no-context-error))
        pq          (:parser-queue *context*)
        parsers     @pq
        reply-count (count parsers)]
    (when (pos? reply-count)
      (reset! pq [])
      (if (or (> reply-count 1) as-pipeline?)
        (mapv #(get-parsed-reply in %) parsers)
        (let [single-reply (get-parsed-reply in (nth parsers 0))]
          (if (instance? Exception single-reply) (throw single-reply)
              single-reply))))))

;;;; General-purpose macros

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
  {:arglists '([:as-pipeline & body] [& body])} ; TODO Consider [opts-map & body]
  [& [s1 & sn :as sigs]]
  (let [as-pipeline? (identical? s1 :as-pipeline)
        body         (if as-pipeline? sn sigs)]
    `(let [stashed-replies# (get-parsed-replies :as-pipeline)]
       (try (do ~@body) ; (parse nil ~@body) would be hygienically conservative
            (get-parsed-replies ~as-pipeline?)
            (finally (parse nil (mapv return stashed-replies#)))))))

(defmacro with-context
  "Evaluates body in the context of a thread-bound connection to a Redis server."
  [conn & body]
  `(let [conn# ~conn
         {spec# :spec in-stream# :in-stream out-stream# :out-stream} conn#]
     (binding [*context* (->Context conn# in-stream# out-stream# (atom []))
               *parser*  nil]
       ~@body)))
