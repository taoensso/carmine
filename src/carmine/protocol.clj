(ns carmine.protocol
  "Facilities for actually communicating with Redis server using its
  request/response protocol. Originally adapted from Accession."
  {:author "Peter Taoussanis"}
  (:require [clojure.string :as str]
            [carmine (utils :as utils) (serialization :as ser)])
  (:import  [java.io DataInputStream BufferedOutputStream]))

;; Hack to allow cleaner separation of ns concerns
(utils/declare-remote carmine.connections/in-stream
                      carmine.connections/out-stream)

(def ^:dynamic *context*
  "For flexibility, our server protocol fns can be called either with explicit
  context provided through arguments OR with a thread-bound context.

  A valid context should consist of AT LEAST an in and out stream but may
  contain other useful adornments. For example:

      * Flags to control encoding/decoding (serialization, etc.).
      * A counter atom to allow us to keep track of how many commands we've sent
        to server since last asking for replies while pipelining. This allows
        'with-conn' to take arbitrary body forms for power & flexibility."
  nil)

(def ^:private no-context-error
  (Exception. (str "Redis commands must be called within the context of a"
                   " connection to Redis server. See 'with-conn'.")))

(def ^:private ^:const charset     "UTF-8")
(def ^:private ^:const bytes-class (Class/forName "[B"))

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

  Binary arguments will be passed through un-munged.
  String arguments will be turned into byte strings.
  All other arguments (including numbers!) will be serialized."
  [^BufferedOutputStream out arg]
  (let [type (cond (string? arg)               :str ; Check most common first!
                   (instance? bytes-class arg) :bin
                   :else                       :clj)

        ^bytes ba (case type
                    :str (bytestring arg)
                    :bin arg
                    :clj (ser/freeze-to-bytes arg true))

        payload-size (alength ba)
        data-size    (if (= type :str) payload-size (+ payload-size 2))]

    ;; To support appropriate automatic reply parsing for specially marked
    ;; types, we need to prohibits strings that could be confused for special
    ;; markers
    (when (and (= type :str) (.startsWith ^String arg "\u0000"))
      (throw (Exception.
              (str "String arguments cannot begin with the null terminator: "
                   arg))))

    (send-$ out) (.write out (bytestring (str data-size))) (send-crlf out)
    (case type :bin (send-bin out) :clj (send-clj out) nil) ; Add marker
    (.write out ba 0 payload-size) (send-crlf out)))

(defn send-request!
  "Sends a command to Redis server using its byte string protocol:

      *<no. of args>     crlf
      [ $<size of arg N> crlf
        <arg data>       crlf ...]

  Ref: http://redis.io/topics/protocol. If explicit context isn't provided,
  uses thread-bound *context*."
  [{:keys [out-stream] :as ?context} command-name & command-args]
  (let [context               (merge *context* ?context)
        ^BufferedOutputStream out (or (:out-stream context)
                                      (throw no-context-error))
        request-args (cons command-name command-args)]

    (send-* out)
    (.write out (bytestring (str (count request-args))))
    (send-crlf out)

    (dorun (map (partial send-arg out) request-args))
    (.flush out)

    ;; Keep track of how many responses are queued with server
    (when-let [c (:atomic-reply-count context)] (swap! c inc))))

(defn =ba? [^bytes x ^bytes y] (java.util.Arrays/equals x y))

(defn get-response!
  "BLOCKs to receive queued replies from Redis server. Parses and returns them.

  Redis will reply to commands with different kinds of replies. It is possible
  to check the kind of reply from the first byte sent by the server:

      * With a single line reply the first byte of the reply will be `+`
      * With an error message the first byte of the reply will be `-`
      * With an integer number the first byte of the reply will be `:`
      * With bulk reply the first byte of the reply will be `$`
      * With multi-bulk reply the first byte of the reply will be `*`

  Error replies will be parsed as exceptions. If only a single reply is received
  and it is an error, the exception will be thrown.

  If explicit context isn't provided, uses thread-bound *context*."
  [& {:keys [in-stream reply-count] :as ?context}]
  (let [context             (merge *context* ?context)
        ^DataInputStream in (or (:in-stream context)
                                (throw no-context-error))
        count (or (:reply-count context)
                  (when-let [c (:atomic-reply-count context)] @c)
                  0)

        get-reply!
        (fn get-reply! []
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
                           payload-size (if str? data-size (- data-size 2))
                           payload      (byte-array payload-size)]

                       (when (and possibly-special-type? str? (.reset in)))
                       (.readFully in payload 0 payload-size)
                       (.readFully in (byte-array 2) 0 2) ; Discard final crlf

                       (case type
                         :str (String. payload 0 payload-size charset)
                         :clj (ser/thaw-from-bytes payload true)
                         :bin [payload payload-size]))))

              \* (let [count (Integer/parseInt (.readLine in))]
                   (doall (repeatedly count get-reply!)))
              (throw (Exception. (str "Server returned unknown reply type: "
                                      reply-type))))))]

    (case (int count)
      0 nil
      1 (let [r (get-reply!)] (if (instance? Exception r) (throw r) r))
      (doall (repeatedly count get-reply!)))))

(defmacro with-context
  "Evaluates body with thread-bound IO streams."
  [connection & body]
  `(binding [*context*
             {:in-stream  (carmine.connections/in-stream  ~connection)
              :out-stream (carmine.connections/out-stream ~connection)}]
     ~@body))

(defmacro with-context-and-response
  "Evaluates body with thread-bound IO streams and returns the server's response."
  [connection & body]
  `(binding [*context*
             {:in-stream  (carmine.connections/in-stream  ~connection)
              :out-stream (carmine.connections/out-stream ~connection)
              :atomic-reply-count (atom 0)}]
     ~@body
     (get-response!)))