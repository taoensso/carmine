(ns carmine.protocol
  "Facilities for actually communicating with Redis server using its
  request/response protocol. Originally adapted from Accession."
  (:require [clojure.string :as str]
            [carmine.utils  :as utils])
  (:import  [java.io OutputStream DataInputStream]))

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

(def ^:private ^:const charset "UTF-8")
(def ^:private ^:const crlf    "\r\n")
(defn as-bytes [s] (.getBytes ^String s charset))
(def ^:private no-context-error
  (Exception. (str "Redis commands must be executed within the context of a"
                   " connection to Redis server. See 'with-conn'.")))

(defn send-request!
  "Actually sends a command to Redis server. First encodes command and its
  arguments into Redis's simple bytestring-based communication protocol:

      *<no. of args>     crlf
      [ $<size of arg N> crlf
        <arg data>       crlf ...]

  Ref: http://redis.io/topics/protocol. If explicit context isn't provided,
  uses thread-bound *context*."
  [{:keys [out-stream] :as ?context} command-name command-name-length & args]
  (let [context           (merge *context* ?context)
        ^OutputStream out (or (:out-stream context)
                              (throw no-context-error))

        ;; This COULD be done more efficiently by working directly with bytes
        ;; but the trade-off would be much disproportionately greater complexity
        payload
        (str "*" (inc (count args))  crlf ;; +1 for command itself
             "$" command-name-length crlf
             command-name            crlf

             ;; User arguments (i.e. arguments TO command)
             (apply str (map (fn [a] (str "$" (count (as-bytes (str a))) crlf
                                         a crlf))
                             args)))]

    (when-let [c (:atomic-reply-count context)] (swap! c inc))
    (.write out ^bytes (as-bytes payload))))

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
              \$ (let [data-length (Integer/parseInt (.readLine in))]
                   (when-not (neg? data-length)
                     (let [data (byte-array data-length)
                           crlf (byte-array 2)]
                       (.read in data 0 data-length)
                       (.read in crlf 0 2)
                       (String. data charset))))
              \* (let [length (Integer/parseInt (.readLine in))]
                   (doall (repeatedly length get-reply!)))
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