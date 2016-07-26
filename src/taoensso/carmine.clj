(ns taoensso.carmine "Clojure Redis client & message queue."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:refer-clojure :exclude [time get set key keys type sync sort eval])
  (:require [clojure.string       :as str]
            [taoensso.encore      :as enc]
            [taoensso.timbre      :as timbre]
            [taoensso.nippy.tools :as nippy-tools]
            [taoensso.carmine
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)]))

(if (vector? taoensso.encore/encore-version)
  (enc/assert-min-encore-version [2 67 2])
  (enc/assert-min-encore-version  2.67))

;;;; Connections

(enc/defalias with-replies protocol/with-replies)

(defmacro wcar
  "Evaluates body in the context of a fresh thread-bound pooled connection to
  Redis server. Sends Redis commands to server as pipeline and returns the
  server's response. Releases connection back to pool when done.

  `conn-opts` arg is a map with connection pool and spec options:
    {:pool {} :spec {:host \"127.0.0.1\" :port 6379}} ; Default
    {:pool {} :spec {:uri \"redis://redistogo:pass@panga.redistogo.com:9475/\"}}
    {:pool {} :spec {:host \"127.0.0.1\" :port 6379
                     :password \"secret\"
                     :timeout-ms 6000
                     :db 3}}

  A `nil` or `{}` `conn-opts` will use defaults. A `:none` pool can be used
  to skip connection pooling (not recommended).
  For other pool options, Ref. http://goo.gl/e1p1h3,
                               http://goo.gl/Sz4uN1 (defaults).

  Note that because of thread-binding, you'll probably want to avoid lazy Redis
  command calls in `wcar`'s body unless you know what you're doing. Compare:
  `(wcar {} (for   [k [:k1 :k2]] (car/set k :val))` ; Lazy, NO commands run
  `(wcar {} (doseq [k [:k1 :k2]] (car/set k :val))` ; Not lazy, commands run

  See also `with-replies`."
  {:arglists '([conn-opts :as-pipeline & body] [conn-opts & body])}
  [conn-opts & args] ; [conn-opts & [a1 & an :as args]]
  `(let [[pool# conn#] (conns/pooled-conn ~conn-opts)

         ;; To support `wcar` nesting with req planning, we mimic
         ;; `with-replies` stashing logic here to simulate immediate writes:
         ?stashed-replies#
         (when protocol/*context*
           (protocol/execute-requests :get-replies :as-pipeline))]

     (try
       (let [response# (protocol/with-context conn#
                         (protocol/with-replies ~@args))]
         (conns/release-conn pool# conn#)
         response#)

       (catch Throwable t# ; nb Throwable to catch assertions, etc.
         (conns/release-conn pool# conn# t#) (throw t#))

       ;; Restore any stashed replies to preexisting context:
       (finally
         (when ?stashed-replies#
           (parse nil ; Already parsed on stashing
             (enc/run! return ?stashed-replies#)))))))

(comment
  (wcar {} (ping) "not-a-Redis-command" (ping))
  (with-open [p (conns/conn-pool {})] (wcar {:pool p} (ping) (ping)))
  (wcar {} (ping))
  (wcar {} :as-pipeline (ping))

  (wcar {} (echo 1) (println (with-replies (ping))) (echo 2))
  (wcar {} (echo 1) (println (with-replies :as-pipeline (ping))) (echo 2))
  (def setupf (fn [_] (println "boo")))
  (wcar {:spec {:conn-setup-fn setupf}}))

;;;; Misc core

;;; Mostly deprecated; prefer using encore stuff directly
(defn as-int   [x] (when x (enc/as-int   x)))
(defn as-float [x] (when x (enc/as-float x)))
(defn as-bool  [x] (when x (enc/as-bool  x)))

(enc/defalias parse       protocol/parse)
(enc/defalias parser-comp protocol/parser-comp)
(enc/defalias parse-raw   protocol/parse-raw)
(enc/defalias parse-nippy protocol/parse-nippy)

(defmacro parse-int      [& body] `(parse as-int   ~@body))
(defmacro parse-float    [& body] `(parse as-float ~@body))
(defmacro parse-bool     [& body] `(parse as-bool  ~@body))
(defmacro parse-keyword  [& body] `(parse keyword  ~@body))
(defmacro parse-suppress [& body]
  `(parse (fn [_#] protocol/suppressed-reply-kw) ~@body))

(comment (wcar {} (parse-suppress (ping)) (ping) (ping)))

(enc/compile-if enc/str-join
  (defn key* [parts] (enc/str-join ":" (map #(if (keyword? %) (enc/as-qname %) (str %))) parts))
  (defn key* [parts] (str/join     ":" (map #(if (keyword? %) (enc/as-qname %) (str %))  parts))))

(defn key
  "Joins parts to form an idiomatic compound Redis key name. Suggested style:
    * \"category:subcategory:id:field\" basic form.
    * Singular category names (\"account\" rather than \"accounts\").
    * Plural _field_ names when appropriate (\"account:friends\").
    * Dashes for long names (\"email-address\" rather than \"emailAddress\", etc.)."
  [& parts] (key* parts))

(comment (key :foo/bar :baz "qux" nil 10))

(enc/defalias raw            protocol/raw)
(enc/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(enc/defalias freeze         nippy-tools/wrap-for-freezing
  "Forces argument of any type (incl. keywords, simple numbers, and binary types)
  to be subject to automatic de/serialization with Nippy.")

(enc/defalias return protocol/return)
(comment (wcar {} (return :foo) (ping) (return :bar))
         (wcar {} (parse name (return :foo)) (ping) (return :bar)))

;;;; Standard commands

(commands/defcommands) ; Defines 190+ Redis command fns as per official spec

(defn redis-call
  "Sends low-level requests to Redis. Useful for DSLs, certain kinds of command
  composition, and for executing commands that haven't yet been added to the
  official `commands.json` spec.

  (redis-call [:set \"foo\" \"bar\"] [:get \"foo\"])"
  [& requests]
  (enc/run!
    (fn [[cmd & args]]
      (let [cmd-parts (-> cmd name str/upper-case (str/split #"-"))
            request   (into (vec cmd-parts) args)]
        (commands/enqueue-request request (count cmd-parts))))
    requests))

(comment (wcar {} (redis-call [:set "foo" "bar"] [:get "foo"]
                              [:config-get "*max-*-entries*"])))

;;;; Lua/scripting support

(defn- sha1-str [x]         (org.apache.commons.codec.digest.DigestUtils/sha1Hex (str x)))
(defn- sha1-ba  [^bytes ba] (org.apache.commons.codec.digest.DigestUtils/sha1Hex ba))

(def script-hash (memoize (fn [script] (sha1-str script))))

(defn evalsha* "Like `evalsha` but automatically computes SHA1 hash for script."
  [script numkeys key & args] (apply evalsha (script-hash script) numkeys key args))

(defn eval*
  "Optimistically tries to send `evalsha` command for given script. In the event
  of a \"NOSCRIPT\" reply, reattempts with `eval`. Returns the final command's
  reply. Redis Cluster note: keys need to all be on same shard."
  [script numkeys key & args]
  (let [parser ; Respect :raw-bulk, otherwise ignore parser:
        (if-not (:raw-bulk? (meta protocol/*parser*))
          nil ; As `parse-raw`:
          (with-meta identity {:raw-bulk? true}))
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

(comment
  (script-subst-vars "_:k1 _:a1 _:k2! _:a _:k3? _:k _:a2 _:a _:a3 _:a-4"
    [:k3? :k1 :k2! :k] [:a2 :a-4 :a3 :a :a1]))

(defn- script-prep-vars "-> [<key-vars> <arg-vars> <var-vals>]"
  [keys args]
  [(when (map? keys) (clojure.core/keys keys))
   (when (map? args) (clojure.core/keys args))
   (into (vec (if (map? keys) (vals keys) keys))
              (if (map? args) (vals args) args))])

(comment
  (script-prep-vars {:k1 :K1 :k2 :K2} {:a1 :A1 :a2 :A2})
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

(comment
  (wcar {}
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

(comment
  (wcar {} (lua       "return redis.call('ping')" {:_ "_"} {}))
  (wcar {} (lua-local "return redis.call('ping')" {:_ "_"} {}))

  (enc/qb 1000
    (wcar {} (ping) (lua "return redis.call('ping')" {:_ "_"} {})
      (ping) (ping) (ping))) ; ~140

  (enc/qb 1000
    (wcar {} (ping) (lua-local "return redis.call('ping')" {:_ "_"} {})
      (ping) (ping) (ping))) ; ~135 (localhost)
  )

;;;; Misc helpers
;; These are pretty rusty + kept around mostly for back-compatibility

(defn hmset* "Like `hmset` but takes a map argument."
  [key m] (apply hmset key (reduce concat m)))

(defn info* "Like `info` but automatically coerces reply into a hash-map."
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

(defn zinterstore* "Like `zinterstore` but automatically counts keys."
  [dest-key source-keys & opts]
  (apply zinterstore dest-key (count source-keys) (concat source-keys opts)))

(defn zunionstore* "Like `zunionstore` but automatically counts keys."
  [dest-key source-keys & opts]
  (apply zunionstore dest-key (count source-keys) (concat source-keys opts)))

;; Adapted from redis-clojure
(defn- parse-sort-args [args]
  (loop [out [] remaining-args (seq args)]
    (if-not remaining-args
      out
      (let [[type & args] remaining-args]
        (case type
          :by    (let [[pattern & rest] args]      (recur (conj out "BY" pattern) rest))
          :limit (let [[offset count & rest] args] (recur (conj out "LIMIT" offset count) rest))
          :get   (let [[pattern & rest] args]      (recur (conj out "GET" pattern) rest))
          :mget  (let [[patterns & rest] args]     (recur (into out (interleave (repeat "GET")
                                                                      patterns)) rest))
          :store (let [[dest & rest] args]         (recur (conj out "STORE" dest) rest))
          :alpha (recur (conj out "ALPHA") args)
          :asc   (recur (conj out "ASC")   args)
          :desc  (recur (conj out "DESC")  args)
          (throw (ex-info (str "Unknown sort argument: " type) {:type type})))))))

(defn sort*
  "Like `sort` but supports idiomatic Clojure arguments: :by pattern,
  :limit offset count, :get pattern, :mget patterns, :store destination,
  :alpha, :asc, :desc."
  [key & sort-args] (apply sort key (parse-sort-args sort-args)))

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

(defrecord Listener [connection handler state future]
  java.io.Closeable
  (close [this] (close-listener this)))

(defmacro with-new-listener
  "Creates a persistent[1] connection to Redis server and a thread to listen for
  server messages on that connection.

  Incoming messages will be dispatched (along with current listener state) to
  (fn handler [reply state-atom]).

  Evaluates body within the context of the connection and returns a
  general-purpose Listener containing:
    1. The underlying persistent connection to facilitate `close-listener` and
       `with-open-listener`.
    2. An atom containing the function given to handle incoming server messages.
    3. An atom containing any other optional listener state.

  Useful for Pub/Sub, monitoring, etc.

  [1] You probably do *NOT* want a :timeout for your `conn-spec` here."
  [conn-spec handler initial-state & body]
  `(let [handler-atom# (atom ~handler)
         state-atom#   (atom ~initial-state)
         {:as conn# in# :in} (conns/make-new-connection
                              (assoc (conns/conn-spec ~conn-spec)
                                     :listener? true))
         future# (future-call ; Thread to long-poll for messages
                  (bound-fn []
                    (while true ; Closes when conn closes
                      (let [reply# (protocol/get-unparsed-reply in# {})]
                        (try
                          (@handler-atom# reply# @state-atom#)
                          (catch Throwable t#
                            (timbre/error t# "Listener handler exception")))))))]

     (protocol/with-context conn# ~@body
       (protocol/execute-requests (not :get-replies) nil))
     (Listener. conn# handler-atom# state-atom# future#)))

(defmacro with-open-listener
  "Evaluates body within the context of given listener's preexisting persistent
  connection."
  [listener & body]
  `(protocol/with-context (:connection ~listener) ~@body
     (protocol/execute-requests (not :get-replies) nil)))

(defn close-listener [listener]
  (conns/close-conn (:connection listener))
  (future-cancel (:future listener)))

(defmacro with-new-pubsub-listener
  "A wrapper for `with-new-listener`.

  Creates a persistent[1] connection to Redis server and a thread to
  handle messages published to channels that you subscribe to with
  `subscribe`/`psubscribe` calls in body.

  Handlers will receive messages of form:
    [<msg-type> <channel/pattern> <message-content>].

  (with-new-pubsub-listener
    {} ; Connection spec, as per `wcar` docstring [1]
    {\"channel1\" (fn [[type match content :as msg]] (prn \"Channel match: \" msg))
     \"user*\"    (fn [[type match content :as msg]] (prn \"Pattern match: \" msg))}
    (subscribe \"foobar\") ; Subscribe thread conn to \"foobar\" channel
    (psubscribe \"foo*\")  ; Subscribe thread conn to \"foo*\" channel pattern
   )

  Returns the Listener to allow manual closing and adjustments to
  message-handlers.

  [1] You probably do *NOT* want a :timeout for your `conn-spec` here."
  [conn-spec message-handlers & subscription-commands]
  `(with-new-listener (assoc ~conn-spec :pubsub-listener? true)

     ;; Message handler (fn [message state])
     (fn [[_# source-channel# :as incoming-message#] msg-handlers#]
       (when-let [f# (clojure.core/get msg-handlers# source-channel#)]
         (f# incoming-message#)))

     ~message-handlers ; Initial state
     ~@subscription-commands))

;;;; Atomic macro
;; The design here's a little on the heavy side; I'd suggest instead reaching
;; for the (newer) CAS tools or (if you need more flexibility), using a Lua
;; script or adhoc `multi`+`watch`+`exec` loop.

(defmacro atomic* "Alpha - subject to change. Low-level transaction util."
  [conn-opts max-cas-attempts on-success on-failure]
  `(let [conn-opts#  ~conn-opts
         max-idx#    ~max-cas-attempts
         _# (assert (>= max-idx# 1))
         prelude-result# (atom nil)
         exec-result#
         (wcar conn-opts# ; Hold 1 conn for all attempts
           (loop [idx# 1]
             (try (reset! prelude-result#
                    (protocol/with-replies :as-pipeline (do ~on-success)))
                  (catch Throwable t1# ; nb Throwable to catch assertions, etc.
                    ;; Always return conn to normal state:
                    (try (protocol/with-replies (discard))
                         (catch Throwable t2# nil) ; Don't mask t1#
                         )
                    (throw t1#)))
             (let [r# (protocol/with-replies (exec))]
               (if-not (nil? r#) ; => empty `multi` or watched key changed
                 ;; Was [] with < Carmine v3
                 (return r#)
                 (if (= idx# max-idx#)
                   (do ~on-failure)
                   (recur (inc idx#)))))))]

     [@prelude-result#
      (protocol/return-parsed-replies
        exec-result# (not :as-pipeline))]))

(defmacro atomic
  "Alpha - subject to change!
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
    (let [;; You can grab the value of the watched key using
          ;; `with-replies` (on the current connection), or
          ;; a nested `wcar` (on a new connection):
          curr-val (or (as-long (with-replies (get :my-int-key))) 0)]

      (return curr-val)

      (multi) ; Start the transaction
        (set :my-int-key (inc curr-val))
        (get :my-int-key)
      ))
  => [[\"OK\" nil \"OK\" \"QUEUED\" \"QUEUED\"] ; Prelude replies
      [\"OK\" \"1\"] ; Transaction replies (`exec` reply)
      ]

  See also `lua` as alternative way to get transactional behaviour."
  [conn-opts max-cas-attempts & body]
  `(atomic* ~conn-opts ~max-cas-attempts (do ~@body)
     (throw (ex-info (format "`atomic` failed after %s attempt(s)"
                       ~max-cas-attempts)
              {:nattempts ~max-cas-attempts}))))

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

  (atomic {} 100 (/ 5 0)) ; Divide by zero
  )

(comment
  (defn swap "`multi`-based `swap`"
    [k f & [nmax-attempts abort-val]]
    (loop [idx 1]
      ;; (println idx)
      (parse-suppress (watch k))
      (let [[old-val ex] (parse nil (with-replies (get k) (exists k)))
            nx?          (= ex 0)
            [new-val return-val] (enc/-vswapped (f old-val nx?))
            cas-success? (parse nil
                           (with-replies
                             (parse-suppress (multi) (set k new-val))
                             (exec)))]
        (if cas-success?
          (return return-val)
          (if (or (nil? nmax-attempts) (< idx (long nmax-attempts)))
            (recur (inc idx))
            (return abort-val)))))))

;;;; CAS tools (experimental!)
;; Working around the lack of simple CAS in Redis core, Ref http://goo.gl/M4Phx8

(defn- prep-cas-old-val [x]
  (let [^bytes bs (protocol/byte-str x)
        ;; Don't bother with sha when actual value would be shorter:
        ?sha      (when (> (alength bs) 40) (sha1-ba bs))]
    [?sha (raw bs)]))

(comment (enc/qb 1000 (prep-cas-old-val "hello there")))

(def compare-and-set "Experimental."
  (let [script (enc/slurp-resource "lua/cas-set.lua")]
    (fn self
      ([k old-val ?sha new-val]
       (lua script {:k k}
         {:old-?sha (or ?sha "")
          :old-?val (if ?sha "" old-val)
          :new-val  new-val}))

      ([k old-val new-val]
       (let [[?sha raw-bs] (prep-cas-old-val old-val)]
         (self k raw-bs ?sha new-val))))))

(def compare-and-hset "Experimental."
  (let [script (enc/slurp-resource "lua/cas-hset.lua")]
    (fn self
      ([k field old-val ?sha new-val]
       (lua script {:k k}
         {:field    field
          :old-?sha (or ?sha "")
          :old-?val (if ?sha "" old-val)
          :new-val  new-val}))

      ([k field old-val new-val]
       (let [[?sha raw-bs] (prep-cas-old-val old-val)]
         (self k field raw-bs ?sha new-val))))))

(comment
  (wcar {} (del "cas-k") (set "cas-k" 0) (compare-and-set "cas-k" 0 1))
  (wcar {} (compare-and-set "cas-k" 1 2))
  (wcar {} (get "cas-k"))

  (wcar {} (del "cas-k") (hset "cas-k" "field" 0) (compare-and-hset "cas-k" "field" 0 1))
  (wcar {} (compare-and-hset "cas-k" "field" 1 2))
  (wcar {} (hget "cas-k" "field")))

(def swap "Experimental."
  (let [cas-get (let [script (enc/slurp-resource "lua/cas-get.lua")]
                  (fn [k] (lua script {:k k} {})))]
    (fn [k f & [nmax-attempts abort-val]]
      (loop [idx 1]
        (let [[old-val ex sha]     (parse nil (with-replies (cas-get k)))
              nx?                  (= ex 0)
              ?sha                 (when-not (= sha "") sha)
              [new-val return-val] (enc/-vswapped (f old-val nx?))
              cas-success?         (= 1 (parse nil
                                          (with-replies
                                            (if nx?
                                              (setnx k new-val)
                                              (compare-and-set k old-val
                                                ?sha new-val)))))]
          (if cas-success?
            (return return-val)
            (if (or (nil? nmax-attempts) (< idx (long nmax-attempts)))
              (recur (inc idx))
              (return abort-val))))))))

(def hswap "Experimental."
  (let [cas-hget (let [script (enc/slurp-resource "lua/cas-hget.lua")]
                   (fn [k field] (lua script {:k k} {:field field})))]
    (fn [k field f & [nmax-attempts abort-val]]
      (loop [idx 1]
        (let [[old-val ex sha]     (parse nil (with-replies (cas-hget k field)))
              nx?                  (= ex 0)
              ?sha                 (when-not (= sha "") sha)
              [new-val return-val] (enc/-vswapped (f old-val nx?))
              cas-success?         (= 1 (parse nil
                                          (with-replies
                                            (if nx?
                                              (hsetnx k field new-val)
                                              (compare-and-hset k field old-val
                                                ?sha new-val)))))]
          (if cas-success?
            (return return-val)
            (if (or (nil? nmax-attempts) (< idx (long nmax-attempts)))
              (recur (inc idx))
              (return abort-val))))))))

(comment (enc/qb 100 (wcar {} (swap "swap-k" (fn [?old _] ?old)))))

;;;;

(defn reduce-scan
  "For use with `scan`, `zscan`, etc. Takes:
    - (fn rf      [acc scan-result]) -> next accumulator
    - (fn scan-fn [cursor]) -> next scan result"
  ([rf          scan-fn] (reduce-scan rf nil scan-fn))
  ([rf acc-init scan-fn]
   (loop [cursor "0" acc acc-init]
     (let [[next-cursor in] (scan-fn cursor)]
       (if (= next-cursor "0")
         (rf acc in)
         (recur next-cursor (rf acc in)))))))

(comment
  (reduce-scan (fn rf [acc in] (into acc in))
    [] (fn scan-fn [cursor] (wcar {} (scan cursor)))))

;;;; Deprecated

(enc/deprecated
  (def as-long   "DEPRECATED: Use `as-int` instead."   as-int)
  (def as-double "DEPRECATED: Use `as-float` instead." as-float)
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
               (throw
                 (ex-info (format "`ensure-atomically` failed after %s attempt(s)"
                            idx#)
                   {:nattempts idx#}))
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
    (let [inner-parser (when-let [p protocol/*parser*] #(mapv p %))
          outer-parser (if keywordize?
                         #(enc/map-keys keyword (apply hash-map %))
                         #(apply hash-map %))]
      (->> (hgetall key)
        (parse (parser-comp outer-parser inner-parser)))))

  (comment
    (wcar {} (hgetall* "hkey"))
    (wcar {} (parse (fn [kvs] (enc/reduce-kvs assoc {} kvs))
               (hgetall "hkey")))

    (wcar {} (hmset* "hkey" {:a "aval" :b "bval" :c "cval"}))
    (wcar {} (hmset* "hkey" {})) ; ex
    (wcar {} (hmget* "hkey" :a :b))
    (wcar {} (parse str/upper-case (hmget* "hkey" :a :b)))
    (wcar {} (hmget* "hkey" "a" "b"))
    (wcar {} (hgetall* "hkey"))
    (wcar {} (parse str/upper-case (hgetall* "hkey"))))

  (defn as-map [x] (enc/as-map x))
  (defmacro parse-map [form & [kf vf]]
    `(parse #(enc/as-map % ~kf ~vf) ~form)))
