(ns taoensso.carmine "Clojure Redis client & message queue."
  {:author "Peter Taoussanis"}
  (:refer-clojure :exclude [time get set key keys type sync sort eval])
  (:require [clojure.string :as str]
            [taoensso.carmine
             (utils       :as utils)
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)]
            [taoensso.timbre      :as timbre]
            [taoensso.nippy.tools :as nippy-tools]))

;;;; Connections

(utils/defalias with-replies protocol/with-replies)

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

  A `nil` or `{}` `conn` or opts will use defaults. A `:none` pool can be used
  to skip connection pooling. For other pool options, Ref. http://goo.gl/EiTbn."
  {:arglists '([conn :as-pipeline & body] [conn & body])}
  [conn & sigs]
  `(let [{pool-opts# :pool spec-opts# :spec} ~conn
         [pool# conn#] (conns/pooled-conn pool-opts# spec-opts#)]
     (try
       (let [response# (protocol/with-context conn#
                         (with-replies ~@sigs))]
         (conns/release-conn pool# conn#)
         response#)
       (catch Exception e# (conns/release-conn pool# conn# e#) (throw e#)))))

(comment
  (wcar {} (ping) "not-a-Redis-command" (ping))
  (with-open [p (conns/conn-pool {})] (wcar {:pool p} (ping) (ping)))
  (wcar {} (ping))
  (wcar {} :as-pipeline (ping))

  (wcar {} (echo 1) (println (with-replies (ping))) (echo 2))
  (wcar {} (echo 1) (println (with-replies :as-pipeline (ping))) (echo 2)))

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

(utils/defalias parse       protocol/parse)
(utils/defalias parser-comp protocol/parser-comp)

(defmacro parse-long    [& body] `(parse as-long   ~@body))
(defmacro parse-double  [& body] `(parse as-double ~@body))
(defmacro parse-bool    [& body] `(parse as-bool   ~@body))
(defmacro parse-keyword [& body] `(parse keyword   ~@body))
(defmacro parse-raw     [& body] `(parse (with-meta identity {:raw? true}) ~@body))

(defn key
  "Joins parts to form an idiomatic compound Redis key name. Suggested style:
    * \"category:subcategory:id:field\" basic form.
    * Singular category names (\"account\" rather than \"accounts\").
    * Dashes for long names (\"email-address\" rather than \"emailAddress\", etc.)."
  [& parts] (str/join ":" (map utils/keyname parts)))

(comment (key :foo/bar :baz "qux" nil 10))

(utils/defalias raw            protocol/raw)
(utils/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(utils/defalias freeze         nippy-tools/wrap-for-freezing
  "Forces argument of any type (incl. keywords, simple numbers, and binary types)
  to be subject to automatic de/serialization with Nippy.")

(utils/defalias return protocol/return)
(comment (wcar {} (return :foo) (ping) (return :bar))
         (wcar {} (parse name (return :foo)) (ping) (return :bar)))

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
                     (with-replies :as-pipeline)
                     (parse nil) ; Nb
                     )]
    (if (and (instance? Exception r)
             (.startsWith (.getMessage ^Exception r) "NOSCRIPT"))
      (apply eval script numkeys key args)
      (return r))))

(def ^:private interpolate-script
  "Substitutes named variables for indexed KEYS[]s and ARGV[]s in Lua script.

  (interpolate-script \"return redis.call('set', _:my-key, _:my-val)\"
                      [:my-key] [:my-val])
  => \"return redis.call('set', KEYS[1], ARGV[1])\""
  (memoize
   (fn [script key-vars arg-vars]
     (let [;; {match replacement} e.g. {"_:my-var" "ARRAY-NAME[1]"}
           subst-map
           (fn [vars array-name]
             (zipmap (map #(str "_:" (name %)) vars)
                     (map #(str array-name "[" % "]")
                          (map inc (range)))))]
       (reduce (fn [s [match replacement]] (str/replace s match replacement))
               (str script)
               (->> (merge (subst-map key-vars "KEYS")
                           (subst-map arg-vars "ARGV"))
                    ;; Prevent ":foo" from replacing ":foo-bar" w/o the need for
                    ;; an insane Regex:
                    (sort-by #(- (.length ^String (first %))))))))))

(comment
  (= (interpolate-script "_:k1 _:a1 _:k2! _:a _:k3? _:k _:a2 _:a _:a3 _:a-4"
                         [:k3? :k1 :k2! :k] [:a2 :a-4 :a3 :a :a1])
     "KEYS[2] ARGV[5] KEYS[3] ARGV[4] KEYS[1] KEYS[4] ARGV[1] ARGV[4] ARGV[3] ARGV[2]"))

(defn lua
  "All singing, all dancing Lua script helper. Like `eval*` but allows script
  vars to be provided as {<var> <value> ...} maps:

  (lua \"redis.call('set', _:my-key, _:my-arg)\" {:my-key \"foo} {:my-arg \"bar\"})

  Keys are separate from other args as an implementation detail for clustering
  purposes (keys need to all be on same shard)."
  [script key-vars arg-vars]
  (apply eval* (interpolate-script script
                (when (map? key-vars) (clojure.core/keys key-vars))
                (when (map? arg-vars) (clojure.core/keys arg-vars)))
         (count key-vars)
         (into (vec (if (map? key-vars) (vals key-vars) key-vars))
               (if (map? arg-vars) (vals arg-vars) arg-vars))))

(comment
  (wcar {} (lua "redis.call('set', _:my-key, _:my-val)
                return redis.call('get', 'foo')"
                {:my-key "foo"}
                {:my-val "bar"})))

(defn hmset* "Like `hmset` but takes a map argument."
  [key m] (apply hmset key (reduce concat m)))

(defn hmget*
  "Like `hmget` but automatically coerces reply into a hash-map.
  Any parser will apply to each hash value."
  [key field & more]
  (let [fields (cons field more)
        inner-parser (when-let [p protocol/*parser*] #(mapv p %))
        outer-parser #(zipmap fields %)]
    (->> (apply hmget key fields)
         (parse (parser-comp outer-parser inner-parser)))))

(defn hgetall*
  "Like `hgetall` but automatically coerces reply into a hash-map. Optionally
  keywordizes map keys. Any parser will apply to each hash value."
  [key & [keywordize?]]
  (let [inner-parser (when-let [p protocol/*parser*] #(mapv p %))
        outer-parser (if keywordize?
                       #(utils/keywordize-map (apply hash-map %))
                       #(apply hash-map %))]
    (->> (hgetall key)
         (parse (parser-comp outer-parser inner-parser)))))

(comment (wcar {} (hmset* "hkey" {:a "aval" :b "bval" :c "cval"}))
         (wcar {} (hmset* "hkey" {})) ; ex
         (wcar {} (hmget* "hkey" :a :b))
         (wcar {} (parse str/upper-case (hmget* "hkey" :a :b)))
         (wcar {} (hmget* "hkey" "a" "b"))
         (wcar {} (hgetall* "hkey"))
         (wcar {} (parse str/upper-case (hgetall* "hkey"))))

(defn info*
  "Like `info` but automatically coerces reply into a hash-map."
  [& [clojureize?]]
  (->> (info)
       (parse
        (fn [reply]
          (let [m (->> reply str/split-lines
                       (map #(str/split % #":"))
                       (filter #(= (count %) 2))
                       (into {}))]
            (if-not clojureize?
              m
              (reduce-kv (fn [m k v] (assoc m (keyword (str/replace k "_" "-")) v))
                         {} (or m {}))))))))

(comment (wcar {} (info* :clojurize)))

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

(defmacro atomic
  "Alpha - subject to change!!
  Tool to ease Redis transactions for fun & profit. Wraps body in a `wcar`
  call that terminates with `exec`, cleans up reply, and supports automatic
  retry for failed optimistic locking.

  Body must contain a `multi` call and may contain calls to: `watch`, `unwatch`,
  `discard`, etc. Ref. http://redis.io/topics/transactions for more info.

  `return` and `parse` NOT supported after `multi` has been called.

  Like `swap!` fn, body may be called multiple times so should avoid impure or
  expensive ops.

  ;;; Atomically increment integer key without using INCR
  (atomic {} 100 ; Retry <= 100 times on failed optimistic lock, or throw ex

    (watch  :my-int-key) ; Watch key for changes
    (let [curr-val (-> (wcar {} ; Note additional connection!
                         (get :my-int-key)) ; Use val of watched key
                       (as-long)
                       (or 0))]
      (return curr-val)

      (multi) ; Start the transaction
        (set :my-int-key (inc curr-val))
        (get :my-int-key)
      ))
  => [[\"OK\" nil \"OK\" \"QUEUED\" \"QUEUED\"] ; Prelude replies
      [\"OK\" \"1\"] ; Transaction replies (`exec` reply)
      ]

  See also `lua` as an alternative method of achieving transactional behaviour."
  [conn max-cas-attempts & body]
  (assert (>= max-cas-attempts 1))
  `(let [conn#       ~conn
         max-idx#    ~max-cas-attempts
         prelude-result# (atom nil)
         exec-result#
         (wcar :as-pipeline ; To ensure `peek` is always valid
           conn# ; Hold 1 conn for all attempts
           (loop [idx# 1]
             (try (do ~@body)
                  (catch Exception e#
                    (discard) ; Always return conn to normal state
                    (throw e#)))
             (reset! prelude-result# (protocol/get-parsed-replies :as-pipeline))
             (let [r# (->> (with-replies (exec))
                           (parse nil) ; Nb
                           )]
               (if (not= r# []) ; => empty `mutli` or watched key changed
                 (return r#)
                 (if (= idx# max-idx#)
                   (throw (Exception. (format "`atomic` failed after %s attempt(s)"
                                              idx#)))
                   (recur (inc idx#)))))))]

     [@prelude-result#
      ;; Mimic normal `get-parsed-replies` behaviour here re: vectorized replies:
      (let [r# exec-result#]
        (if (next r#) r#
            (let [r# (nth r# 0)]
              (if (instance? Exception r#) (throw r#) r#))))]))

(comment
  ;; Error before exec (=> syntax, etc.)
  (wcar {} (multi) (redis-call [:invalid]) (ping) (exec))
  ;; ["OK" #<Exception: ERR unknown command 'INVALID'> "QUEUED"
  ;;  #<Exception: EXECABORT Transaction discarded because of previous errors.>]

  ;; Error during exec (=> datatype, etc.)
  (wcar {} (set "aa" "string") (multi) (ping) (incr "aa") (ping) (exec))
  ;; ["OK" "OK" "QUEUED" "QUEUED" "QUEUED"
  ;;  ["PONG" #<Exception: ERR value is not an integer or out of range> "PONG"]]

  (wcar {} (multi) (ping) (discard)) ; ["OK" "QUEUED" "OK"]
  (wcar {} (multi) (ping) (discard) (exec))
  ;; ["OK" "QUEUED" "OK" #<Exception: ERR EXEC without MULTI>]

  (wcar {} (watch "aa") (set "aa" "string") (multi) (ping) (exec))
  ;; ["OK" "OK" "OK" "QUEUED" []]

  (wcar {} (watch "aa") (set "aa" "string") (unwatch) (multi) (ping) (exec))
  ;; ["OK" "OK" "OK" "OK" "QUEUED" ["PONG"]]

  (wcar {} (multi) (multi))
  ;; ["OK" #<Exception java.lang.Exception: ERR MULTI calls can not be nested>]
  )

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
  [conn-spec handler initial-state & body]
  `(let [handler-atom# (atom ~handler)
         state-atom#   (atom ~initial-state)
         conn# (conns/make-new-connection
                (assoc (conns/conn-spec ~conn-spec) :listener? true))
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
     (->Listener conn# handler-atom# state-atom#)))

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
  `(with-new-listener (assoc ~conn-spec :pubsub-listener? true)

     ;; Message handler (fn [message state])
     (fn [[_# source-channel# :as incoming-message#] msg-handlers#]
       (when-let [f# (clojure.core/get msg-handlers# source-channel#)]
         (f# incoming-message#)))

     ~message-handlers ; Initial state
     ~@subscription-commands))

;;;; Renamed/deprecated

(defn kname "DEPRECATED: Use `key` instead. `key` does not filter nil parts."
  [& parts] (apply key (filter identity parts)))

(comment (kname :foo/bar :baz "qux" nil 10))

(def serialize "DEPRECATED: Use `freeze` instead." freeze)
(def preserve  "DEPRECATED: Use `freeze` instead." freeze)
(def remember  "DEPRECATED: Use `return` instead." return)
(def ^:macro skip-replies "DEPRECATED: Use `with-replies` instead." #'with-replies)
(def ^:macro with-reply   "DEPRECATED: Use `with-replies` instead." #'with-replies)
(def ^:macro with-parser  "DEPRECATED: Use `parse` instead."        #'parse)

(defn lua-script "DEPRECATED: Use `lua` instead." [& args] (apply lua args))

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

(defmacro atomically "DEPRECATED: Use `atomic` instead."
  [watch-keys & body]
  `(do
     (with-replies ; discard "OK" and "QUEUED" replies
       (when-let [wk# (seq ~watch-keys)] (apply watch wk#))
       (multi)
       ~@body)

     ;; Body discards will result in an (exec) exception:
     (parse (parser-comp protocol/*parser*
                         (-> #(if (instance? Exception %) [] %)
                             (with-meta {:parse-exceptions? true})))
            (exec))))

(defmacro ensure-atomically "DEPRECATED: Use `atomic` instead."
  [{:keys [max-tries] :or {max-tries 100}}
   watch-keys & body]
  `(let [watch-keys# ~watch-keys
         max-idx#    ~max-tries]
     (loop [idx# 0]
       (let [result# (with-replies (atomically watch-keys# ~@body))]
         (if (not= [] result#)
           (remember result#)
           (if (= idx# max-idx#)
             (throw (Exception. (str "`ensure-atomically` failed after " idx#
                                     " attempts")))
             (recur (inc idx#))))))))
