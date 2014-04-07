(ns taoensso.carmine "Clojure Redis client & message queue."
  {:author "Peter Taoussanis"}
  (:refer-clojure :exclude [time get set key keys type sync sort eval])
  (:require [clojure.string :as str]
            [taoensso.encore      :as encore]
            [taoensso.timbre      :as timbre]
            [taoensso.nippy.tools :as nippy-tools]
            [taoensso.carmine
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)]))

;;;; Connections

(encore/defalias with-replies protocol/with-replies)

(defmacro wcar
  "Evaluates body in the context of a thread-bound pooled connection to Redis
  server. Sends Redis commands to server as pipeline and returns the server's
  response. Releases connection back to pool when done.

  `conn-opts` arg is a map with connection pool and spec options:
    {:pool {} :spec {:host \"127.0.0.1\" :port 6379}} ; Default
    {:pool {} :spec {:uri \"redis://redistogo:pass@panga.redistogo.com:9475/\"}}
    {:pool {} :spec {:host \"127.0.0.1\" :port 6379
                     :password \"secret\"
                     :timeout-ms 6000
                     :db 3}}

  A `nil` or `{}` `conn-opts` will use defaults. A `:none` pool can be used
  to skip connection pooling. For other pool options, Ref. http://goo.gl/7ewMzM."
  {:arglists '([conn-opts :as-pipeline & body] [conn-opts & body])}
  ;; [conn-opts & [s1 & sn :as sigs]]
  [conn-opts & sigs]
  `(let [[pool# conn#] (conns/pooled-conn ~conn-opts)

         ;; To support `wcar` nesting with req planning, we mimmick
         ;; `with-replies` stashing logic here to simulate immediate writes:
         ?stashed-replies#
         (when protocol/*context*
           (protocol/execute-requests :get-replies :as-pipeline))]

     (try
       (let [response# (protocol/with-context conn#
                         (protocol/with-replies* ~@sigs))]
         (conns/release-conn pool# conn#)
         response#)

       (catch Exception e#
         (conns/release-conn pool# conn# e#) (throw e#))

       ;; Restore any stashed replies to preexisting context:
       (finally
         (when ?stashed-replies#
           (parse nil ; Already parsed on stashing
             (mapv return ?stashed-replies#)))))))

(comment
  (wcar {} (ping) "not-a-Redis-command" (ping))
  (with-open [p (conns/conn-pool {})] (wcar {:pool p} (ping) (ping)))
  (wcar {} (ping))
  (wcar {} :as-pipeline (ping))

  (wcar {} (echo 1) (println (with-replies (ping))) (echo 2))
  (wcar {} (echo 1) (println (with-replies :as-pipeline (ping))) (echo 2)))

;;;; Misc

(encore/defalias as-bool     encore/as-bool  "Returns x as bool, or nil.")
(encore/defalias as-int      encore/as-int   "Returns x as integer, or nil.")
(encore/defalias as-float    encore/as-float "Returns x as float, or nil.")
(encore/defalias as-map      encore/as-map)
(encore/defalias parse       protocol/parse)
(encore/defalias parser-comp protocol/parser-comp)

;;; Note that 'parse' has different meanings in Carmine/Encore context:
(defmacro parse-int     [& body] `(parse as-int    ~@body))
(defmacro parse-float   [& body] `(parse as-float  ~@body))
(defmacro parse-bool    [& body] `(parse as-bool   ~@body))
(defmacro parse-keyword [& body] `(parse keyword   ~@body))

(encore/defalias parse-raw   protocol/parse-raw)
(encore/defalias parse-nippy protocol/parse-nippy)

(defmacro parse-map [form & [kf vf]] `(parse #(encore/as-map % ~kf ~vf) ~form))

(defn key
  "Joins parts to form an idiomatic compound Redis key name. Suggested style:
    * \"category:subcategory:id:field\" basic form.
    * Singular category names (\"account\" rather than \"accounts\").
    * Plural _field_ names when appropriate (\"account:friends\").
    * Dashes for long names (\"email-address\" rather than \"emailAddress\", etc.)."
  [& parts] (str/join ":" (mapv #(if (keyword? %) (encore/fq-name %) (str %))
                                parts)))

(comment (key :foo/bar :baz "qux" nil 10))

(encore/defalias raw            protocol/raw)
(encore/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(encore/defalias freeze         nippy-tools/wrap-for-freezing
  "Forces argument of any type (incl. keywords, simple numbers, and binary types)
  to be subject to automatic de/serialization with Nippy.")

(encore/defalias return protocol/return)
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
    (let [cmd-parts (-> cmd name str/upper-case (str/split #"-"))
          request   (into (vec cmd-parts) args)]
      (commands/enqueue-request request (count cmd-parts)))))

(comment (wcar {} (redis-call [:set "foo" "bar"] [:get "foo"]
                              [:config-get "*max-*-entries*"])))

;;; Lua scripts

(def script-hash (memoize (fn [script]
  (org.apache.commons.codec.digest.DigestUtils/shaHex (str script)))))

(defn evalsha* "Like `evalsha` but automatically computes SHA1 hash for script."
  [script numkeys key & args] (apply evalsha (script-hash script) numkeys key args))

(defn eval*
  "Optimistically tries to send `evalsha` command for given script. In the event
  of a \"NOSCRIPT\" reply, reattempts with `eval`. Returns the final command's
  reply. Redis Cluster note: keys need to all be on same shard."
  [script numkeys key & args]
  (let [parser ; Respect :raw-bulk, otherwise ignore parser:
        (if-not (:raw-bulk? (meta protocol/*parser*)) nil
          (with-meta identity {:raw-bulk? true}) ; As `parse-raw`
          )
        [r & _] ; & _ for :as-pipeline
        (parse parser (with-replies :as-pipeline
                        (apply evalsha* script numkeys key args)))]
    (if (= (:prefix (ex-data r)) :noscript)
      ;;; Now apply context's parser:
      (apply eval script numkeys key args)
      (return r))))

(def ^:private script-subst-vars
  "Substitutes named variables for indexed KEYS[]s and ARGV[]s in Lua script."
  (memoize
   (fn [script key-vars arg-vars]
     (let [subst-map ; {match replacement} e.g. {"_:my-var" "ARRAY-NAME[1]"}
           (fn [vars array-name]
             (zipmap (map #(str "_:" (name %)) vars)
                     (map #(str array-name "[" % "]")
                          (map inc (range)))))]
       (reduce (fn [s [match replacement]] (str/replace s match replacement))
               (str script)
               (->> (merge (subst-map key-vars "KEYS")
                           (subst-map arg-vars "ARGV"))
                    ;; Stop ":foo" replacing ":foo-bar" w/o need for insane Regex:
                    (sort-by #(- (.length ^String (first %))))))))))

(comment (script-subst-vars "_:k1 _:a1 _:k2! _:a _:k3? _:k _:a2 _:a _:a3 _:a-4"
           [:k3? :k1 :k2! :k] [:a2 :a-4 :a3 :a :a1]))

(defn- script-prep-vars "-> [<key-vars> <arg-vars> <var-vals>]"
  [keys args]
  [(when (map? keys) (clojure.core/keys keys))
   (when (map? args) (clojure.core/keys args))
   (into (vec (if (map? keys) (vals keys) keys))
              (if (map? args) (vals args) args))])

(comment (script-prep-vars {:k1 :K1 :k2 :K2} {:a1 :A1 :a2 :A2})
         (script-prep-vars [:K1 :K2] {:a1 :A1 :a2 :A2}))

(defn lua
  "All singing, all dancing Lua script helper. Like `eval*` but allows script
  vars to be provided as {<var> <value> ...} maps:

  (lua \"redis.call('set', _:my-key, _:my-arg)\" {:my-key \"foo} {:my-arg \"bar\"})

  Keys are separate from other args as an implementation detail for clustering
  purposes (keys need to all be on same shard)."
  [script keys args]
  (let [[key-vars arg-vars var-vals] (script-prep-vars keys args)]
    (apply eval* (script-subst-vars script key-vars arg-vars)
           (count keys) ; Not key-vars!
           var-vals)))

(comment (wcar {}
           (lua "redis.call('set', _:my-key, _:my-val)
                 return redis.call('get', 'foo')"
                {:my-key "foo"}
                {:my-val "bar"})))

(def ^:private scripts-loaded-locally "#{[<conn-spec> <sha>]...}" (atom #{}))
(defn lua-local
  "Alpha - subject to change.
  Like `lua`, but optimized for the single-server, single-client case: maintains
  set of Lua scripts previously loaded by this client and will _not_ speculate
  on script availability. CANNOT be used in environments where Redis servers may
  go down and come back up again independently of application servers (clients)."
  [script keys args]
  (let [conn-spec (get-in protocol/*context* [:conn :spec])
        [key-vars arg-vars var-vals] (script-prep-vars keys args)
        script* (script-subst-vars script key-vars arg-vars)
        sha     (script-hash script*)
        evalsha (fn [] (apply evalsha sha (count keys) var-vals))]
    (if (contains? @scripts-loaded-locally [conn-spec sha]) (evalsha)
      (do (with-replies (parse nil (script-load script*)))
          (swap! scripts-loaded-locally conj [conn-spec sha])
          (evalsha)))))

(comment (wcar {} (lua       "return redis.call('ping')" {:_ "_"} {}))
         (wcar {} (lua-local "return redis.call('ping')" {:_ "_"} {}))

         (clojure.core/time
          (dotimes [_ 10000]
            (wcar {} (ping) (lua "return redis.call('ping')" {:_ "_"} {})
                     (ping) (ping) (ping))))

         (clojure.core/time
          (dotimes [_ 10000]
            (wcar {} (ping) (lua-local "return redis.call('ping')" {:_ "_"} {})
                     (ping) (ping) (ping)))))

;;;

(defn hmset* "Like `hmset` but takes a map argument."
  [key m] (apply hmset key (reduce concat m)))

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
  [conn-opts max-cas-attempts & body]
  (assert (>= max-cas-attempts 1))
  `(let [conn-opts#  ~conn-opts
         max-idx#    ~max-cas-attempts
         prelude-result# (atom nil)
         exec-result#
         (wcar conn-opts# ; Hold 1 conn for all attempts
           (loop [idx# 1]
             (try (reset! prelude-result#
                    (protocol/with-replies* :as-pipeline ~@body))
                  (catch Exception e#
                    ;; Always return conn to normal state:
                    (protocol/with-replies* (discard))
                    (throw e#)))
             (let [r# (protocol/with-replies* (exec))]
               (if-not (nil? r#) ; => empty `multi` or watched key changed
                 ;; Was [] with < Carmine v3
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
  ;; ["OK" "OK" "OK" "QUEUED" []]  ; Old reply fn
  ;; ["OK" "OK" "OK" "QUEUED" nil] ; New reply fn

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
         {:as conn# in# :in} (conns/make-new-connection
                              (assoc (conns/conn-spec ~conn-spec)
                                :listener? true))]

     (future-call ; Thread to long-poll for messages
      (bound-fn []
        (while true ; Closes when conn closes
          (let [reply# (protocol/get-unparsed-reply in# {})]
            (try
              (@handler-atom# reply# @state-atom#)
              (catch Throwable t#
                (timbre/error t# "Listener handler exception")))))))

     (protocol/with-context conn# ~@body
       (protocol/execute-requests (not :get-replies) nil))
     (->Listener conn# handler-atom# state-atom#)))

(defmacro with-open-listener
  "Evaluates body within the context of given listener's preexisting persistent
  connection."
  [listener & body]
  `(protocol/with-context (:connection ~listener) ~@body
     (protocol/execute-requests (not :get-replies) nil)))

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

;;;; Deprecated

(def as-long   "DEPRECATED: Use `as-int` instead."   as-int)
(def as-double "DEPRECATED: Use `as-float` instead." as-double)
(defmacro parse-long "DEPRECATED: Use `parse-int` instead."
  [& body] `(parse as-long   ~@body))
(defmacro parse-double "DEPRECATED: Use `parse-float` instead."
  [& body] `(parse as-double ~@body))

(def hash-script "DEPRECATED: Use `script-hash` instead." script-hash)

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
         (if-not (nil? result#) ; Was [] with < Carmine v3
           (remember result#)
           (if (= idx# max-idx#)
             (throw (Exception. (str "`ensure-atomically` failed after " idx#
                                     " attempts")))
             (recur (inc idx#))))))))

(defn hmget* "DEPRECATED: Use `parse-map` instead."
  [key field & more]
  (let [fields (cons field more)
        inner-parser (when-let [p protocol/*parser*] #(mapv p %))
        outer-parser #(zipmap fields %)]
    (->> (apply hmget key fields)
         (parse (parser-comp outer-parser inner-parser)))))

(defn hgetall* "DEPRECATED: Use `parse-map` instead."
  [key & [keywordize?]]
  (let [keywordize-map (fn [m] (reduce-kv (fn [m k v] (assoc m (keyword k) v))
                                         {} (or m {})))
        inner-parser (when-let [p protocol/*parser*] #(mapv p %))
        outer-parser (if keywordize?
                       #(keywordize-map (apply hash-map %))
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
