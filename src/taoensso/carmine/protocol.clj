(ns taoensso.carmine.protocol
  "Facilities for actually communicating with Redis server using its
  request/response protocol. Originally adapted from Accession.

  Ref: http://redis.io/topics/protocol"
  {:author "Peter Taoussanis"}
  (:require [clojure.string :as str]
            [taoensso.carmine (utils :as utils)]
            [taoensso.nippy :as nippy])
  (:import  [java.io DataInputStream BufferedOutputStream]
            [clojure.lang PersistentQueue]))

;; Hack to allow cleaner separation of namespaces
(utils/declare-remote taoensso.carmine.connections/get-spec
                      taoensso.carmine.connections/in-stream
                      taoensso.carmine.connections/out-stream)

(defrecord Context [in-stream out-stream parser-stack])
(def ^:dynamic *context* nil)
(def ^:dynamic *parser*  nil)
(def no-context-error
  (Exception. (str "Redis commands must be called within the context of a"
                   " connection to Redis server. See `with-conn`.")))

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
                    :clj (nippy/freeze-to-bytes arg))

        payload-size (alength ba)
        data-size    (if (= type :str) payload-size (+ payload-size 2))]

    ;; To support various special goodies like serialization, we reserve
    ;; strings that begin with a null terminator
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
        <arg data>       crlf ...]"
  [& args]
  (let [^BufferedOutputStream out (or (:out-stream *context*)
                                      (throw no-context-error))]

    (send-* out)
    (.write out (bytestring (str (count args))))
    (send-crlf out)

    (dorun (map (partial send-arg out) args))
    (.flush out)

    (when-let [ps (:parser-stack *context*)] (swap! ps conj *parser*))))

(defn =ba? [^bytes x ^bytes y] (java.util.Arrays/equals x y))

(defn get-basic-reply!
  "BLOCKS to receive a single reply from Redis server. Applies basic parsing
  and returns the result.

  Redis will reply to commands with different kinds of replies. It is possible
  to check the kind of reply from the first byte sent by the server:

      * With a single line reply the first byte of the reply will be `+`
      * With an error message the first byte of the reply will be `-`
      * With an integer number the first byte of the reply will be `:`
      * With bulk reply the first byte of the reply will be `$`
      * With multi-bulk reply the first byte of the reply will be `*`"
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
(defn- skip-reply?     [reply] (= reply :taoensso.carmine.protocol/skip-reply))
(defn- exception-check [reply] (if (instance? Exception reply) (throw reply) reply))
(defn- skip-check      [reply] (if (skip-reply? reply) nil reply))

(defn get-replies!
  "BLOCKS to receive one or more (pipelined) replies from Redis server. Applies
  all parsing and returns the result."
  [reply-count]
  (let [^DataInputStream in (or (:in-stream *context*)
                                (throw no-context-error))
        parsers     @(:parser-stack *context*)
        reply-count (if (= reply-count :all) (count parsers) reply-count)]

    (when (pos? reply-count)
      (swap! (:parser-stack *context*) (partial drop reply-count))

      (if (= reply-count 1)
        (->> (get-basic-reply! in)
             (apply-parser (peek parsers))
             skip-check
             exception-check)

        (let [replies
              (->> (repeatedly reply-count (partial get-basic-reply! in))
                   (map #(apply-parser %1 %2) parsers)
                   (remove skip-reply?))]
          (if-not (next replies)
            (let [[r] replies] (exception-check r))
            (vec replies)))))))

(defn get-one-reply! [] (get-replies! 1))

(defmacro with-context
  "Evaluates body in the context of a thread-bound connection to a Redis server."
  [connection & body]
  `(let [listener?#
         (:listener? (taoensso.carmine.connections/get-spec ~connection))]
     (binding [*context*
               (Context. (taoensso.carmine.connections/in-stream  ~connection)
                         (taoensso.carmine.connections/out-stream ~connection)
                         (when-not listener?# (atom (PersistentQueue/EMPTY))))
               *parser* nil]
       ~@body
       (if listener?# nil (get-replies! :all)))))