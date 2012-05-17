(ns carmine.core
  "Deliberately simple, high-performance Redis (2.0+) client for Clojure."
  (:refer-clojure :exclude [time get set keys type sync sort eval])
  (:require [carmine
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)])
  (:import  [org.apache.commons.pool.impl GenericKeyedObjectPool]
            [carmine.connections          ConnectionPool]))

;;;; Connections
;; Better to have this stuff here so that library consumers don't need to
;; require another namespace.

(defn make-conn-pool
  "For option documentation see http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericKeyedObjectPool.html"
  [& options]
  (let [;; Defaults adapted from Jedis
        default {:lifo?                         true
                 :test-while-idle?              true
                 :num-tests-per-eviction-run    -1
                 :min-evictable-idle-time-ms    60000
                 :time-between-eviction-runs-ms 30000}]
    (ConnectionPool.
     (reduce conns/set-pool-option
             (GenericKeyedObjectPool. (conns/make-connection-factory))
             (merge default (apply hash-map options))))))

(defn make-conn-spec
  [& {:keys [host port password timeout db]
      :or   {host "127.0.0.1" port 6379 password nil timeout 0 db 0}}]
  {:host host :port port :password password :timeout timeout :db db})

(defmacro with-conn
  "Evaluates body in the context of a pooled connection to Redis server. Body
  may contain Redis commands and functions of Redis commands (e.g. 'map').

  Sends full request to server as pipeline and returns the server's response.
  Releases connection back to pool when done.

  Use 'make-conn-pool' and 'make-conn-spec' to generate the required arguments."
  [connection-pool connection-spec & body]
  `(try
     (let [conn# (conns/get-conn ~connection-pool ~connection-spec)]
       (try
         (let [response# (protocol/with-context-and-response conn# ~@body)]
           (conns/release-conn ~connection-pool conn#)
           response#)

         ;; Failed to execute body
         (catch Exception e# (conns/release-conn ~connection-pool conn# e#)
                (throw e#))))
     ;; Failed to get connection from pool
     (catch Exception e# (throw e#))))

;; For compatibility with other clients
(defmacro with-connection
  "DEPRECATED. Use 'with-conn' instead.
  Like 'with-conn' but uses a one-time, NON-pooled connection."
  [connection-spec & body]
  `(with-conn conns/non-pooled-connection-pool ~connection-spec ~@body))

;;;; Standard commands

(commands/defcommands) ; This kicks ass - big thanks to Andreas Bielk!

;;;; Command helpers

(defn zinterstore*
  "Like 'zinterstore' but automatically counts keys."
  [dest-key source-keys & options]
  (apply zinterstore dest-key
         (count source-keys) (concat source-keys options)))

(defn zunionstore*
  "Like 'zunionstore' but automatically counts keys."
  [dest-key source-keys & options]
  (apply zunionstore dest-key
         (count source-keys) (concat source-keys options)))

;; Adapted from redis-clojure
(defn- parse-sort-args [args]
  (loop [out [] remaining-args (seq args)]
    (if-not remaining-args
      out
      (let [[type & args] remaining-args]
        (case type
          :by (let [[pattern & rest] args]
                (recur (conj out "BY" pattern) rest))
          :limit (let [[offset count & rest] args]
                   (recur (conj out "LIMIT" offset count) rest))
          :get (let [[pattern & rest] args]
                 (recur (conj out "GET" pattern) rest))
          :mget (let [[patterns & rest] args]
                  (recur (into out (interleave (repeat "GET")
                                               patterns)) rest))
          :store (let [[dest & rest] args]
                   (recur (conj out "STORE" dest) rest))
          :alpha (recur (conj out "ALPHA") args)
          :asc   (recur (conj out "ASC")   args)
          :desc  (recur (conj out "DESC")  args)
          (throw (Exception. (str "Unknown sort argument: " type))))))))

(defn sort*
  "Like 'sort' but supports Clojure-idiomatic arguments: :by pattern,
  :limit offset count, :get pattern, :mget patterns, :store destination,
  :alpha, :asc, :desc."
  [key & sort-args]
  (apply sort key (parse-sort-args sort-args)))

(def ^:private hash-script
  (memoize
   (fn [script]
     (org.apache.commons.codec.digest.DigestUtils/shaHex (str script)))))

(defn evalsha*
  "Like 'evalsha' but automatically computes SHA1 hash for script."
  [script numkeys & more]
  (apply evalsha (hash-script script) numkeys more))

(defn eval*-with-conn
  "Optimistically tries to send 'evalsha' command for given script. In the event
  of a \"NOSCRIPT\" reply, reattempts with 'eval'. Returns the final command's
  result."
  [pool spec script numkeys & more]
  (try
    (with-conn pool spec (apply evalsha* script numkeys more))
    (catch Exception e
      (if (= (.substring (.getMessage e) 0 8) "NOSCRIPT")
        (with-conn pool spec (apply eval script numkeys more))
        (throw e)))))

;;;; Persistent stuff (monitoring, pub/sub, etc.)

;; Once a connection to Redis issues a command like 'p/subscribe' or 'monitor'
;; it enters an idiosyncratic state:
;;
;;     * It blocks while waiting to receive a stream of special responses issued
;;       to it (connection-local!) by the server.
;;     * It can now only issue a subset of the normal commands like
;;      'p/un/subscribe', 'quit', etc.
;;
;; To facilitate the unusual requirements we define a Listener to be a
;; combination of persistent, NON-pooled connection and threaded response
;; handler:

(defrecord Listener [connection handler state])

(defmacro with-new-listener
  "Creates a persistent connection to Redis server and a thread to listen for
  server responses on that connection.

  Incoming responses will be dispatched (along with current listener state) to
  binary handler function.

  Evaluates body within the context of the connection and returns a
  general-purpose Listener containing:

      1. The underlying persistent connection to facilitate 'close-listener' and
         'with-open-listener'.
      2. An atom containing the function given to handle incoming server
         responses.
      3. An atom containing any other optional listener state.

  Useful for pub/sub, monitoring, etc."
  [connection-spec handler initial-state & body]
  `(let [handler-atom# (atom ~handler)
         state-atom#   (atom ~initial-state)
         conn# (conns/make-new-connection (assoc ~connection-spec
                                            :persistent? true))]
     (protocol/with-context conn#

       ;; Create a thread to actually listen for and process responses from
       ;; server. Thread will close when connection closes.
       (-> (fn [] (@handler-atom# (protocol/get-response! :reply-count 1)
                                 @state-atom#))
           repeatedly doall future)

       ~@body
       (Listener. conn# handler-atom# state-atom#))))

(defmacro with-open-listener
  "Evaluates body within the context of given listener's preexisting persistent
  connection. Does NOT get response from server. Returns body's result."
  [listener & body]
  `(protocol/with-context (:connection ~listener) ~@body))

(defn close-listener [listener] (conns/close-conn (:connection listener)))

(defmacro with-new-pubsub-listener
  "A wrapper for 'with-new-listener'. Creates a persistent connection to Redis
  server and a thread to listen for published messages from channels that we'll
  subscribe to using 'p/subscribe' commands in body.

  While listening, incoming messages will be dispatched by source (channel or
  pattern) to the given message-handler functions:

       {\"channel1\" (fn [message] (prn \"Channel match: \" message))
        \"user*\"    (fn [message] (prn \"Pattern match: \" message))}

  SUBSCRIBE message:  `[\"message\" channel payload]`
  PSUBSCRIBE message: `[\"pmessage\" pattern matching-channel payload]`

  Returns the Listener to allow manual closing and adjustments to
  message-handlers."
  [connection-spec message-handlers & subscription-commands]
  `(with-new-listener (assoc ~connection-spec :pubsub? true)

     ;; Response handler (fn [response state])
     (fn [[_# source-channel# :as incoming-message#] msg-handlers#]
       (when-let [f# (clojure.core/get msg-handlers# source-channel#)]
         (f# incoming-message#)))

     ~message-handlers ; Initial state
     ~@subscription-commands))

;;;; Dev/tests

(comment
  (def pool (make-conn-pool))
  (def spec (make-conn-spec))
  (def spec (make-conn-spec :host "panga.redistogo.com"
                            :port 9475
                            :password "foobar")) ; Remote

  ;;; Basic connections
  (def conn (conns/get-conn pool spec))
  (conns/conn-alive? conn)
  (conns/release-conn pool conn) ; Return connection to pool (don't close)
  (conns/close-conn conn)        ; Actually close connection

  ;;; Basic requests
  (with-conn pool spec (ping))
  (with-connection spec (ping)) ; Degenerate pool
  (with-conn pool spec
    (ping)
    (set "key" "value")
    (incrby "key2" 12)
    (get "key")
    (get "key2")
    (set "unicode" "año")
    (get "unicode"))

  (with-conn pool spec "This is invalid") ; Malformed

  ;;; Advanced requests
  (with-conn pool spec (doall (repeatedly 5 ping)))
  (with-conn pool spec
    (doall
     (map set ["bob" "sam" "steve"] ["carell" "black" "irwin"])))

  ;;; Pub/Sub
  (def listener
    (with-new-pubsub-listener
      spec {"foo1"  (fn [x] (println "Channel match: " x))
            "foo*"  (fn [x] (println "Pattern match: " x))
            "extra" (fn [x] (println "EXTRA: " x))}
      (subscribe  "foo1" "foo2")
      (psubscribe "foo*")))

  (do (println "---")
      (with-conn pool spec
        (publish "foo1" "Message to foo1")))

  (with-open-listener listener
    (unsubscribe))

  (close-listener listener)

  ;;; Lua
  (eval*-with-conn pool spec
    "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}"
    2 "key1" "key2" "arg1" "arg2"))