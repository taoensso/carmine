(ns taoensso.carmine "Clojure Redis client & message queue."
  {:author "Peter Taoussanis"}
  (:refer-clojure :exclude [time get set keys type sync sort eval])
  (:require [clojure.string :as str]
            [taoensso.carmine
             (utils       :as utils)
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)]
            [taoensso.timbre      :as timbre]
            [taoensso.nippy.tools :as nippy-tools]))

;;;; Connections

(defmacro wcar
  "Evaluates body in the context of a thread-bound pooled connection to Redis
  server. Sends Redis commands to server as pipeline and returns the server's
  response. Releases connection back to pool when done.

  `conn` arg is a map with connection pool and spec options:
    {:pool {} :spec {:host \"127.0.0.1\" :port 6379}} ; Default
    {:pool {} :spec {:uri \"redis://redistogo:pass@panga.redistogo.com:9475/\"}}
    {:pool {} :spec {:host \"127.0.0.1\" :port 6379
                     :password \"secret\"
                     :timeout-ms 6000
                     :db 3}}

    ;; Redis Sentinel (see also `add-sentinel-server!`)
    {:pool {} :spec {:sentinel-group  \"my-app\"
                     :sentinel-master \"master\"}}

  For pool options, Ref. http://goo.gl/EiTbn."
  [conn & body]
  `(let [{pool-opts# :pool spec-opts# :spec} ~conn
         [pool# conn#] (conns/pooled-conn pool-opts# spec-opts#)]
     (try (let [response# (protocol/with-context conn# ~@body)]
            (conns/release-conn pool# conn#) response#)
          (catch Exception e# (conns/release-conn pool# conn# e#) (throw e#)))))

(comment (wcar {} (ping) "not-a-Redis-command" (ping))
         (with-open [p (conns/conn-pool {})]
           (wcar {:pool p} (ping) (ping))))

;;;; Sentinel (EXPERIMENTAL)

(defn- remove-sentinel-server [group-name ip & [groups]]
  (vec (remove (fn [[ip* port]] (= ip ip*))
               ((or @conns/sentinel-groups groups) group-name))))

(defn add-sentinel-server!
  "(add-sentinel-server! \"my-app\" \"127.0.0.1\" 26379)"
  [group-name ip & [port to-front?]]
  (swap! conns/sentinel-groups
         (fn [groups]
           (assoc groups group-name
                  (let [groups (remove-sentinel-server group-name ip groups)
                        server [ip (or port 26379)]]
                    (if to-front?
                      (vec (cons server groups))
                      (conj groups server)))))))

(defn remove-sentinel-server!
  "(remove-sentinel-server! \"my-app\" \"127.0.0.1\")"
  [group-name ip]
  (swap! conns/sentinel-groups
         (fn [groups]
           (assoc groups group-name
                  (remove-sentinel-server group-name ip groups)))))

(comment (add-sentinel-server!    "my-app" "127.0.0.1" 26379 true)
         (add-sentinel-server!    "my-app" "127.0.0.2")
         (remove-sentinel-server! "my-app" "127.0.0.1"))

;;;; Misc

;;; (number? x) for Carmine < v0.11.x backwards compatiblility
(defn as-long   [x] (when x (if (number? x) (long   x) (Long/parseLong     x))))
(defn as-double [x] (when x (if (number? x) (double x) (Double/parseDouble x))))
(defn as-bool   [x] (when x
                      (cond (or (true? x) (false? x))            x
                            (or (= x "false") (= x "0") (= x 0)) false
                            (or (= x "true")  (= x "1") (= x 1)) true
                            :else
                            (throw (Exception. (str "Couldn't coerce as bool: "
                                                    x))))))

(defmacro parse
  "Wraps body so that replies to any wrapped Redis commands will be parsed with
  `(f reply)`. Replaces any current parser; removes parser when `f` is nil."
  [f & body] `(binding [protocol/*parser* ~f] ~@body))

(defmacro parse-long    [& body] `(parse as-long   ~@body))
(defmacro parse-double  [& body] `(parse as-double ~@body))
(defmacro parse-bool    [& body] `(parse as-bool   ~@body))
(defmacro parse-keyword [& body] `(parse keyword   ~@body))
(defmacro parse-raw     [& body] `(parse (with-meta identity {:raw? true}) ~@body))

(defn kname
  "Joins keywords, integers, and strings to form an idiomatic compound Redis key
  name.

  Suggested key naming style:
    * \"category:subcategory:id:field\" basic form.
    * Singular category names (\"account\" rather than \"accounts\").
    * Dashes for long names (\"email-address\" rather than \"emailAddress\", etc.)."

  [& parts] (str/join ":" (map utils/keyname (filter identity parts))))

(comment (kname :foo/bar :baz "qux" nil 10))

(utils/defalias raw            protocol/raw)
(utils/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(utils/defalias freeze         nippy-tools/wrap-for-freezing
  "Forces argument of any type (incl. keywords, simple numbers, and binary types)
  to be subject to automatic de/serialization with Nippy.")

(defn return
  "Special command that takes any value and returns it unchanged as part of
  an enclosing `wcar` pipeline response."
  [value]
  (let [vfn (constantly value)]
    (swap! (:parser-queue protocol/*context*) conj
           (with-meta (if-let [p protocol/*parser*] (comp p vfn) vfn)
             {:dummy-reply? true}))))

(comment (wcar {} (return :foo) (ping) (return :bar))
         (wcar {} (parse name (return :foo)) (ping) (return :bar)))

(defmacro with-replies
  "Alpha - subject to change.
  Evaluates body, immediately returning the server's response to any contained
  Redis commands (i.e. before enclosing `wcar` ends). Ignores any parser
  in enclosing (not _enclosed_) context.

  As an implementation detail, stashes and then `return`s any replies already
  queued with Redis server: i.e. should be compatible with pipelining."
  {:arglists '([:as-pipeline & body] [& body])}
  [& [s1 & sn :as sigs]]
  (let [as-pipeline? (= s1 :as-pipeline)
        body (if as-pipeline? sn sigs)]
    `(let [stashed-replies# (protocol/get-replies true)]
       (try (parse nil ~@body) ; Herewith dragons; tread lightly
            (protocol/get-replies ~as-pipeline?)
            (finally
             ;; doseq here broken with Clojure <1.5, Ref. http://goo.gl/5DvRt
             (parse nil (dorun (map return stashed-replies#))))))))

(comment (wcar {} (echo 1) (println (with-replies (ping))) (echo 2))
         (wcar {} (echo 1) (println (with-replies :as-pipeline (ping))) (echo 2)))

;;;; Standard commands

(commands/defcommands) ; This kicks ass - big thanks to Andreas Bielk!

;;;; Helper commands

(defn redis-call
  "Sends low-level requests to Redis. Useful for DSLs, certain kinds of command
  composition, and for executing commands that haven't yet been added to the
  official `commands.json` spec.

  (redis-call [:set \"foo\" \"bar\"] [:get \"foo\"])"
  [& requests]
  (doseq [[cmd & args] requests]
    (let [cmd-parts (-> cmd name str/upper-case (str/split #"-"))]
      (protocol/send-request (into (vec cmd-parts) args)))))

(comment (wcar {} (redis-call [:set "foo" "bar"] [:get "foo"]
                              [:config-get "*max-*-entries*"])))

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
  reply.

  Redis Cluster note: keys need to all be on same shard."
  [script numkeys key & args]
  (let [[r & _] (->> (apply evalsha* script numkeys key args)
                     (with-replies :as-pipeline))]
    (parse protocol/*parser*
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
  clustering purposes (keys need to all be on same shard)."
  [script key-vars-map arg-vars-map]
  (apply eval* (interpolate-script script (clojure.core/keys key-vars-map)
                                          (clojure.core/keys arg-vars-map))
         (count key-vars-map)
         (into (vec (vals key-vars-map)) (vals arg-vars-map))))

(comment
  (wcar {} (lua-script "redis.call('set', _:my-key, _:my-val)
                        return redis.call('get', 'foo')"
                       {:my-key "foo"}
                       {:my-val "bar"})))

(defn hgetall*
  "Like `hgetall` but automatically coerces reply into a hash-map. Optionally
  keywordizes map keys."
  [key & [keywordize?]]
  (parse
    (if keywordize?
      #(utils/keywordize-map (apply hash-map %))
      #(apply hash-map %))
    (hgetall key)))

(defn info*
  "Like `info` but automatically coerces reply into a hash-map."
  []
  (parse (fn [reply] (->> reply str/split-lines
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
     (parse #(if (instance? Exception %) [] %) (exec))))

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
         in#   (:in-stream conn#)]

     (future-call ; Thread to long-poll for messages
      (bound-fn []
        (while true ; Closes when conn closes
          (let [reply# (protocol/get-basic-reply in# false)]
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
  [conn-spec message-handlers & subscription-commands]
  `(with-new-listener (assoc (conns/conn-spec ~conn-spec)
                        :pubsub-listener? true)

     ;; Message handler (fn [message state])
     (fn [[_# source-channel# :as incoming-message#] msg-handlers#]
       (when-let [f# (clojure.core/get msg-handlers# source-channel#)]
         (f# incoming-message#)))

     ~message-handlers ; Initial state
     ~@subscription-commands))

;;;; Renamed/deprecated

(def serialize "DEPRECATED: Use `freeze` instead." freeze)
(def preserve  "DEPRECATED: Use `freeze` instead." freeze)
(def remember  "DEPRECATED: Use `return` instead." return)
(def ^:macro skip-replies "DEPRECATED: Use `with-replies` instead." #'with-replies)
(def ^:macro with-reply   "DEPRECATED: Use `with-replies` instead." #'with-replies)
(def ^:macro with-parser  "DEPRECATED: Use `parse` instead."        #'parse)

(defn make-keyfn "DEPRECATED: Use `kname` instead."
  [& prefix-parts]
  (let [prefix (when (seq prefix-parts) (str (apply kname prefix-parts) ":"))]
    (fn [& parts] (str prefix (apply kname parts)))))

(defn make-conn-pool "DEPRECATED: Use `wcar` instead."
  [& opts] (conns/conn-pool (apply hash-map opts)))

(defn make-conn-spec "DEPRECATED: Use `wcar` instead."
  [& opts] (conns/conn-spec (apply hash-map opts)))

(defmacro with-conn "DEPRECATED: Use `wcar` instead."
  [connection-pool connection-spec & body]
  `(wcar {:pool ~connection-pool :spec ~connection-spec} ~@body))
