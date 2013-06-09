(ns taoensso.carmine
  "Clojure Redis client & message queue."
  {:author "Peter Taoussanis"}
  (:refer-clojure :exclude [time get set keys type sync sort eval])
  (:require [clojure.string :as str]
            [taoensso.carmine
             (utils       :as utils)
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)]
            [taoensso.timbre :as timbre])
  (:import [org.apache.commons.pool.impl GenericKeyedObjectPool]
           [taoensso.carmine.connections ConnectionPool]
           [taoensso.carmine.protocol    Frozen Raw]
           java.net.URI))

;;;; Connections

(defn make-conn-pool
  "For option documentation see http://goo.gl/EiTbn"
  [& options]
  (let [;; Defaults adapted from Jedis
        defaults {:test-while-idle?              true
                  :num-tests-per-eviction-run    -1
                  :min-evictable-idle-time-ms    60000
                  :time-between-eviction-runs-ms 30000}]
    (ConnectionPool.
     (reduce conns/set-pool-option
             (GenericKeyedObjectPool. (conns/make-connection-factory))
             (merge defaults (apply hash-map options))))))

(defn- parse-uri [uri]
  (when uri
    (let [^URI uri (if (instance? URI uri) uri (URI. uri))
          [user password] (.split (str (.getUserInfo uri)) ":")
          port (.getPort uri)]
      (-> {:host (.getHost uri)}
          (#(if (pos? port) (assoc % :port     port)     %))
          (#(if password    (assoc % :password password) %))))))

(comment (parse-uri "redis://redistogo:pass@panga.redistogo.com:9475/"))

(defn make-conn-spec
  [& {:keys [uri host port password timeout-ms db] :as opts}]
  (let [defaults {:host "127.0.0.1" :port 6379}
        opts     (if-let [timeout (:timeout opts)] ; Support deprecated opt
                   (assoc opts :timeout-ms timeout)
                   opts)]
    (merge defaults opts (parse-uri uri))))

(defmacro with-conn
  "Evaluates body in the context of a thread-bound pooled connection to a Redis
  server. Sends Redis commands to server as pipeline and returns the server's
  response. Releases connection back to pool when done.

  Use `make-conn-pool` and `make-conn-spec` to generate the required arguments."
  [connection-pool connection-spec & body]
  `(let [pool# (or ~connection-pool conns/non-pooled-connection-pool)
         spec# (or ~connection-spec (make-conn-spec))
         conn# (try (conns/get-conn pool# spec#)
                    (catch Exception e#
                      (throw (Exception. "Carmine connection error" e#))))]
     (try
       (let [response# (protocol/with-context conn# ~@body)]
         (conns/release-conn pool# conn#) response#)
       (catch Exception e# (conns/release-conn pool# conn# e#) (throw e#)))))

;;;; Misc

;;; Note (number? x) checks for backwards compatibility with pre-v0.11 Carmine
;;; versions that auto-serialized simple number types
(defn as-long   [x] (when x (if (number? x) (long   x) (Long/parseLong     x))))
(defn as-double [x] (when x (if (number? x) (double x) (Double/parseDouble x))))
(defn as-bool   [x] (when x
                      (cond (or (true? x) (false? x))            x
                            (or (= x "false") (= x "0") (= x 0)) false
                            (or (= x "true")  (= x "1") (= x 1)) true
                            :else
                            (throw (Exception. (str "Couldn't coerce as bool: "
                                                    x))))))

(defmacro with-parser
  "Alpha - subject to change!!
  Wraps body so that replies to any wrapped Redis commands will be parsed with
  `(f reply)`. Replaces any current parser; removes parser when `f` is nil."
  [f & body] `(binding [protocol/*parser* ~f] ~@body))

(defmacro parse-long    [& body] `(with-parser as-long   ~@body))
(defmacro parse-double  [& body] `(with-parser as-double ~@body))
(defmacro parse-bool    [& body] `(with-parser as-bool   ~@body))
(defmacro parse-keyword [& body] `(with-parser keyword   ~@body))

(defn kname
  "Joins keywords, integers, and strings to form an idiomatic compound Redis key
  name.

  Suggested key naming style:
    * \"category:subcategory:id:field\" basic form.
    * Singular category names (\"account\" rather than \"accounts\").
    * Dashes for long names (\"email-address\" rather than \"emailAddress\", etc.)."

  [& parts] (str/join ":" (map utils/keyname (filter identity parts))))

(comment (kname :foo/bar :baz "qux" nil 10))

(defn freeze
  "Forces argument of any type (including simple number and binary types) to be
  subject to automatic de/serialization."
  [x] (protocol/Frozen. x))

(defn raw "Alpha - subject to change." [x] (protocol/Raw. x))
(defmacro parse-raw "Alpha - subject to change."
  [& body] `(with-parser (with-meta identity {:raw? true}) ~@body))

(defn return
  "Alpha - subject to change.
  Special command that takes any value and returns it unchanged as part of
  an enclosing `with-conn` pipeline response."
  [value]
  (let [vfn (constantly value)
        parser protocol/*parser*]
    (swap! (:parser-queue protocol/*context*) conj
           (with-meta (if parser (comp parser vfn) vfn)
             {:dummy-reply? true}))))

(comment (wc (return :foo) (ping) (return :bar))
         (wc (with-parser name (return :foo)) (ping) (return :bar)))

(defmacro with-replies
  "Alpha - subject to change!!
  Evaluates body, immediately returning the server's response to any contained
  Redis commands (i.e. before enclosing `with-conn` ends). Ignores any parser
  in enclosing (not _enclosed_) context.

  As an implementation detail, stashes and then `return`s any replies already
  queued with Redis server: i.e. should be compatible with pipelining."
  {:arglists '([:as-pipeline & body] [& body])}
  [& [s1 & sn :as sigs]]
  (let [as-pipeline? (= s1 :as-pipeline)
        body (if as-pipeline? sn sigs)]
    `(let [stashed-replies# (protocol/get-replies! true)]
       (try (with-parser nil ~@body) ; Herewith dragons; tread lightly
            (protocol/get-replies! ~as-pipeline?)
            (finally
             ;; doseq here broken with Clojure <1.5, Ref. http://goo.gl/5DvRt
             (with-parser nil (dorun (map return stashed-replies#))))))))

(comment (wc (echo 1) (println (with-replies (ping))) (echo 2))
         (wc (echo 1) (println (with-replies :as-pipeline (ping))) (echo 2)))

;;;; Standard commands

(commands/defcommands) ; This kicks ass - big thanks to Andreas Bielk!

;;;; Helper commands

(def hash-script
  (memoize
   (fn [script]
     (org.apache.commons.codec.digest.DigestUtils/shaHex (str script)))))

(defn evalsha*
  "Like `evalsha` but automatically computes SHA1 hash for script."
  [script numkeys key & args]
  (apply evalsha (hash-script script) numkeys key args))

(defn eval*
  "Optimistically tries to send `evalsha` command for given script. In the event
  of a \"NOSCRIPT\" reply, reattempts with `eval`. Returns the final command's
  reply."
  [script numkeys key & args]
  (let [parser protocol/*parser*
        [r & _] (->> (apply evalsha* script numkeys key args)
                     (with-replies :as-pipeline))]
    (with-parser parser
      (if (and (instance? Exception r)
               (.startsWith (.getMessage ^Exception r) "NOSCRIPT"))
        (apply eval script numkeys key args)
        (return r)))))

(def ^:private interpolate-script
  "Substitutes indexed KEYS[]s and ARGV[]s for named variables in Lua script.

  (interpolate-script \"return redis.call('set', _:my-key, _:my-val)\"
                      [:my-key] [:my-val])
  => \"return redis.call('set', KEYS[1], ARGV[1])\""
  (memoize
   (fn [script key-vars arg-vars]
     (let [;; {match replacement} e.g. {"_:my-var" "ARRAY-NAME[1]"}
           subst-map (fn [vars array-name]
                       (zipmap (map #(str "_" %) vars)
                               (map #(str array-name "[" % "]")
                                    (map inc (range)))))]
       (reduce (fn [s [match replacement]] (str/replace s match replacement))
               (str script)
               (merge (subst-map key-vars "KEYS")
                      (subst-map arg-vars "ARGV")))))))

(comment
  (interpolate-script "return redis.call('set', _:my-key, _:my-val)"
                      [:my-key] [:my-val])

  (interpolate-script "Hello _:k1 _:k1 _:k2 _:k3 _:a1 _:a2 _:a3"
                      [:k3 :k1 :k2]
                      [:a3 :a1 :a2]))

(defn lua-script
  "All singing, all dancing Lua script helper. Like `eval*` but allows script
  to use \"_:my-var\"-style named keys and args.

  Keys are given separately from other args as an implementation detail for
  clustering purposes."
  [script key-vars-map arg-vars-map]
  (apply eval* (interpolate-script script (clojure.core/keys key-vars-map)
                                          (clojure.core/keys arg-vars-map))
         (count key-vars-map)
         (into (vec (vals key-vars-map)) (vals arg-vars-map))))

(comment
  (wc (lua-script "redis.call('set', _:my-key, _:my-val)
                   return redis.call('get', 'foo')"
                  {:my-key "foo"}
                  {:my-val "bar"})))

(defn hgetall*
  "Like `hgetall` but automatically coerces reply into a hash-map. Optionally
  keywordizes map keys."
  [key & [keywordize?]]
  (with-parser
    (if keywordize?
      #(utils/keywordize-map (apply hash-map %))
      #(apply hash-map %))
    (hgetall key)))

(defn info*
  "Like `info` but automatically coerces reply into a hash-map."
  []
  (with-parser (fn [reply] (->> reply str/split-lines
                               (map #(str/split % #":"))
                               (filter #(= (count %) 2))
                               (into {})))
    (info)))

(defn zinterstore*
  "Like `zinterstore` but automatically counts keys."
  [dest-key source-keys & options]
  (apply zinterstore dest-key
         (count source-keys) (concat source-keys options)))

(defn zunionstore*
  "Like `zunionstore` but automatically counts keys."
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
  "Like `sort` but supports idiomatic Clojure arguments: :by pattern,
  :limit offset count, :get pattern, :mget patterns, :store destination,
  :alpha, :asc, :desc."
  [key & sort-args] (apply sort key (parse-sort-args sort-args)))

(defmacro atomically
  "Executes all Redis commands in body as a single transaction and returns
  server response vector or an empty vector if transaction failed.

  Body may contain a (discard) call to abort transaction."
  [watch-keys & body]
  `(try
     (with-replies ; discard "OK" and "QUEUED" replies
       (when-let [wk# (seq ~watch-keys)] (apply watch wk#))
       (multi)
       ~@body)
     ;; Body discards will result in an (exec) exception:
     (with-parser #(if (instance? Exception %) [] %) (exec))))

;;;; Persistent stuff (monitoring, pub/sub, etc.)

;; Once a connection to Redis issues a command like `p/subscribe` or `monitor`
;; it enters an idiosyncratic state:
;;
;;     * It blocks while waiting to receive a stream of special messages issued
;;       to it (connection-local!) by the server.
;;     * It can now only issue a subset of the normal commands like
;;       `p/un/subscribe`, `quit`, etc. These do NOT issue a normal server reply.
;;
;; To facilitate the unusual requirements we define a Listener to be a
;; combination of persistent, NON-pooled connection and threaded message
;; handler.

(declare close-listener)

(defrecord Listener [connection handler state]
  java.io.Closeable
  (close [this] (close-listener this)))

(defmacro with-new-listener
  "Creates a persistent connection to Redis server and a thread to listen for
  server messages on that connection.

  Incoming messages will be dispatched (along with current listener state) to
  binary handler function.

  Evaluates body within the context of the connection and returns a
  general-purpose Listener containing:

      1. The underlying persistent connection to facilitate `close-listener` and
         `with-open-listener`.
      2. An atom containing the function given to handle incoming server
         messages.
      3. An atom containing any other optional listener state.

  Useful for pub/sub, monitoring, etc."
  [connection-spec handler initial-state & body]
  `(let [handler-atom# (atom ~handler)
         state-atom#   (atom ~initial-state)
         conn# (conns/make-new-connection (assoc ~connection-spec
                                            :listener? true))
         in#   (conns/in-stream conn#)]

     (future-call ; Thread to long-poll for messages
      (bound-fn []
        (while true ; Closes when conn closes
          (let [reply# (protocol/get-basic-reply! in#)]
            (try
              (@handler-atom# reply# @state-atom#)
              (catch Throwable t#
                (timbre/error t# "Listener handler exception")))))))

     (protocol/with-context conn# ~@body)
     (Listener. conn# handler-atom# state-atom#)))

(defmacro with-open-listener
  "Evaluates body within the context of given listener's preexisting persistent
  connection."
  [listener & body]
  `(protocol/with-context (:connection ~listener) ~@body))

(defn close-listener [listener] (conns/close-conn (:connection listener)))

(defmacro with-new-pubsub-listener
  "A wrapper for `with-new-listener`. Creates a persistent connection to Redis
  server and a thread to listen for published messages from channels that we'll
  subscribe to using `p/subscribe` commands in body.

  While listening, incoming messages will be dispatched by source (channel or
  pattern) to the given message-handler functions:

       {\"channel1\" (fn [message] (prn \"Channel match: \" message))
        \"user*\"    (fn [message] (prn \"Pattern match: \" message))}

  SUBSCRIBE message:  `[\"message\" channel payload]`
  PSUBSCRIBE message: `[\"pmessage\" pattern matching-channel payload]`

  Returns the Listener to allow manual closing and adjustments to
  message-handlers."
  [connection-spec message-handlers & subscription-commands]
  `(with-new-listener (assoc ~connection-spec :pubsub-listener? true)

     ;; Message handler (fn [message state])
     (fn [[_# source-channel# :as incoming-message#] msg-handlers#]
       (when-let [f# (clojure.core/get msg-handlers# source-channel#)]
         (f# incoming-message#)))

     ~message-handlers ; Initial state
     ~@subscription-commands))

;;;; Renamed/deprecated

(def serialize "DEPRECATED. Please use `freeze`." freeze)
(def preserve  "DEPRECATED. Please use `freeze`." freeze)
(def remember  "DEPRECATED. Please use `return`."    return)
(def ^:macro skip-replies "DEPRECATED. Please use `with-replies`." #'with-replies)
(def ^:macro with-reply   "DEPRECATED. Please use `with-replies`." #'with-replies)

(defn make-keyfn "DEPRECATED. Please use `kname`."
  [& prefix-parts]
  (let [prefix (when (seq prefix-parts) (str (apply kname prefix-parts) ":"))]
    (fn [& parts] (str prefix (apply kname parts)))))

;;;; Dev/tests

(comment
  (do (def pool (make-conn-pool))
      (def spec (make-conn-spec))
      (def conn (conns/get-conn pool spec))
      (defmacro wc [& body] `(with-conn pool spec ~@body)))

  ;;; Basic connections
  (conns/conn-alive? conn)
  (conns/release-conn pool conn) ; Return connection to pool (don't close)
  (conns/close-conn conn)        ; Actually close connection

  ;;; Basic requests
  (with-conn nil nil (ping)) ; Degenerate pool, default spec
  (with-conn (make-conn-pool) (make-conn-spec) (ping))
  (with-conn pool spec (ping))
  (with-conn pool spec "invalid" (ping) (ping)))