(ns taoensso.carmine.protocol
  "Facilities for actually communicating with Redis server using its
  request/response protocol. Originally adapted from Accession.

  Ref: http://redis.io/topics/protocol"
  {:author "Peter Taoussanis"}
  (:require [clojure.string :as str]
            [taoensso.carmine (utils :as utils)]
            [taoensso.nippy :as nippy])
  (:import  [java.io DataInputStream BufferedOutputStream]))

;; Hack to allow cleaner separation of namespaces
(utils/declare-remote taoensso.carmine.connections/get-spec
                      taoensso.carmine.connections/in-stream
                      taoensso.carmine.connections/out-stream)

(defrecord Context [in-stream out-stream parser-queue])
(def ^:dynamic *context* nil)
(def ^:dynamic *parser*  nil)
(def no-context-error
  (Exception. (str "Redis commands must be called within the context of a"
                   " connection to Redis server. See `with-conn`.")))

(def ^:private ^:const charset     "UTF-8")
(def ^:private ^:const bytes-class (Class/forName "[B"))

;; Define a special type that can be used to box values for which we'd like
;; to force automatic de/serialization (e.g. simple number types that are
;; normally converted to byte strings).
(deftype Serialized [value])

(defn bytestring
  "Redis communicates with clients using a (binary-safe) byte string protocol.
  This is the equivalent of the byte array representation of a Java String."
  ^bytes [^String s] (.getBytes s charset))

;;; Request delimiters
(def ^bytes   bs-crlf  (bytestring "\r\n"))
(def ^Integer bs-*     (int (first (bytestring "*"))))
(def ^Integer bs-$     (int (first (bytestring "$"))))

;; Carmine-only markers that'll be used _within_ bulk data to indicate that
;; the data requires special reply handling
(def ^bytes bs-bin     (bytestring "\u0000<")) ; Binary data marker
(def ^bytes bs-clj     (bytestring "\u0000>")) ; Serialized data marker

;;; Fns to actually send data to stream buffer
(defn send-crlf [^BufferedOutputStream out] (.write out bs-crlf 0 2))
(defn send-bin  [^BufferedOutputStream out] (.write out bs-bin  0 2))
(defn send-clj  [^BufferedOutputStream out] (.write out bs-clj  0 2))
(defn send-*    [^BufferedOutputStream out] (.write out bs-*))
(defn send-$    [^BufferedOutputStream out] (.write out bs-$))
(defn send-arg
  "Send arbitrary argument along with information about its size:
  $<size of arg> crlf
  <arg data>     crlf

  Argument type will determine how it'll be stored with Redis:
    * String args become byte strings.
    * Simple numbers (integers, longs, floats, doubles) become byte strings.
    * Binary (byte array) args go through un-munged.
    * Everything else gets serialized."
  [^BufferedOutputStream out arg]
  (let [type (cond (string?  arg) :string ; Most common 1st!
                   ;;(keyword? arg) :keyword
                   (or (instance? Long    arg)
                       (instance? Double  arg)
                       (instance? Integer arg)
                       (instance? Float   arg)) :simple-number
                   (instance? bytes-class arg)  :bytes
                   :else                        :serialized)

        ^bytes ba (case type
                    :string        (bytestring arg)
                    :keyword       (bytestring (name arg))
                    :simple-number (bytestring (str arg))
                    :bytes         arg
                    :serialized    (nippy/freeze-to-bytes
                                    (if (instance? Serialized arg)
                                      (.value ^Serialized arg)
                                      arg)))

        payload-size (alength ba)
        marked-type? (not (or (= type :string) (= type :simple-number)))
        data-size    (if marked-type? (+ payload-size 2) payload-size)]

    ;; To support various special goodies like serialization, we reserve
    ;; strings that begin with a null terminator
    (when (and (= type :string) (.startsWith ^String arg "\u0000"))
      (throw (Exception.
              (str "String arguments cannot begin with the null terminator: "
                   arg))))

    (send-$ out) (.write out (bytestring (str data-size))) (send-crlf out)
    (when marked-type?
      (case type
        :bytes      (send-bin out)
        :serialized (send-clj out)))
    (.write out ba 0 payload-size) (send-crlf out)))

(defn send-request!
  "Sends a command to Redis server using its byte string protocol:

      *<no. of args>     crlf
      [$<size of arg N>  crlf
        <arg data>       crlf ...]"
  [& args]
  (let [^BufferedOutputStream out (or (:out-stream *context*)
                                      (throw no-context-error))]

    (send-* out)
    (.write out (bytestring (str (count args))))
    (send-crlf out)

    (doseq [arg args] (send-arg out arg))
    (.flush out)

    (when-let [pq (:parser-queue *context*)] (swap! pq conj *parser*))))

(defn =ba? [^bytes x ^bytes y] (java.util.Arrays/equals x y))

(defn get-basic-reply!
  "BLOCKS to receive a single reply from Redis server. Applies basic parsing
  and returns the result.

  Redis will reply to commands with different kinds of replies, identified by
  their first byte:
      * `+` for single line reply.
      * `-` for error message.
      * `:` for integer reply.
      * `$` for bulk reply.
      * `*` for multi-bulk reply."
  [^DataInputStream in]
  (let [reply-type (char (.readByte in))]
    (case reply-type
      \+ (.readLine in)
      \- (Exception. (.readLine in))
      \: (Long/parseLong (.readLine in))

      ;; Bulk data replies need checking for special in-data markers
      \$ (let [data-size (Integer/parseInt (.readLine in))]
           (when-not (neg? data-size)
             (let [possibly-special-type? (>= data-size 2)
                   type (or (when possibly-special-type?
                              (.mark in 2)
                              (let [h (byte-array 2)]
                                (.readFully in h 0 2)
                                (condp =ba? h
                                  bs-clj :clj
                                  bs-bin :bin
                                  nil))) :str)

                   str?         (= type :str)
                   payload-size (int (if str? data-size (- data-size 2)))
                   payload      (byte-array payload-size)]

               (when (and possibly-special-type? str? (.reset in)))
               (.readFully in payload 0 payload-size)
               (.readFully in (byte-array 2) 0 2) ; Discard final crlf

               (case type
                 :str (String. payload 0 payload-size charset)
                 :clj (nippy/thaw-from-bytes payload)
                 :bin [payload payload-size]))))

      \* (let [bulk-count (Integer/parseInt (.readLine in))]
           (vec (repeatedly bulk-count (partial get-basic-reply! in))))
      (throw (Exception. (str "Server returned unknown reply type: "
                              reply-type))))))

(defn- apply-parser [parser reply] (if parser (parser reply) reply))
(defn- exception-check [reply] (if (instance? Exception reply) (throw reply) reply))

;; TODO Add support for a value parser that doesn't actually request anything
;; from server but just regurgitates a given value. Will be useful for `remember`.
(defn get-replies!
  "BLOCKS to receive one or more (pipelined) replies from Redis server. Applies
  all parsing and returns the result."
  [& [reply-count]]
  (let [^DataInputStream in (or (:in-stream *context*)
                                (throw no-context-error))
        parsers     @(:parser-queue *context*)
        reply-count (if reply-count reply-count (count parsers))]

    (when (pos? reply-count)
      (swap! (:parser-queue *context*) #(subvec % reply-count))

      (if (= reply-count 1)
        (->> (get-basic-reply! in)
             (apply-parser (peek parsers))
             exception-check)

        (->> (repeatedly reply-count (partial get-basic-reply! in))
             (map #(apply-parser %1 %2) parsers)
             (vec))))))

(defn get-one-reply! [] (get-replies! 1))

(defmacro with-context
  "Evaluates body in the context of a thread-bound connection to a Redis server.
  For non-listener connections, sends Redis commands to server as pipeline and
  returns the server's response."
  [connection & body]
  `(let [listener?#
         (:listener? (taoensso.carmine.connections/get-spec ~connection))]
     (binding [*context*
               (Context. (taoensso.carmine.connections/in-stream  ~connection)
                         (taoensso.carmine.connections/out-stream ~connection)
                         (when-not listener?# (atom [])))
               *parser* nil]
       ~@body
       (if listener?# nil (get-replies!)))))