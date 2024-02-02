(ns taoensso.carmine "Clojure Redis client & message queue."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:refer-clojure
   :exclude [time get set key keys type sync sort eval
             parse-long parse-double])

  (:require
   [clojure.string       :as str]
   [taoensso.encore      :as enc]
   [taoensso.timbre      :as timbre]
   [taoensso.nippy       :as nippy]
   [taoensso.nippy.tools :as nippy-tools]
   [taoensso.carmine
    (protocol    :as protocol)
    (connections :as conns)
    (commands    :as commands)]))

(enc/assert-min-encore-version [3 68 0])

;;;; Logging config

(defn set-min-log-level!
  "Sets Timbre's minimum log level for internal Carmine namespaces.
  Possible levels: #{:trace :debug :info :warn :error :fatal :report}.
  Default level: `:warn`."
  [level]
  (timbre/set-ns-min-level! "taoensso.carmine.*" level)
  (timbre/set-ns-min-level! "taoensso.carmine"   level)
  nil)

(defonce ^:private __set-default-log-level (set-min-log-level! :warn))

;;;; Connections

(enc/defalias with-replies protocol/with-replies)

(defn connection-pool
  "Returns a new, stateful connection pool for use with `wcar`.
  Backed by Apache Commons Pool 2's `GenericKeyedObjectPool`.

  Pool opts include:
    - :test-on-borrow?   ; Test conn health on acquisition from pool? (Default false)
    - :test-on-return?   ; Test conn health on return      to   pool? (Default false)
    - :test-on-idle?     ; Test conn health while idle in pool?       (Default true)

    - :min-idle-per-key  ; Min num of idle conns to keep per sub-pool (Default 0)
    - :max-idle-per-key  ; Max num of idle conns to keep per sub-pool (Default 16)
    - :max-total-per-key ; Max num of idle or active conns <...>      (Default 16)

  Pool can be shutdown with the `java.io.Closeable` `close` method.
  This will in turn call the `GenericKeyedObjectPool`'s `close` method.

  Example:

    (defonce my-pool (car/connection-pool {:test-on-borrow? true})) ; Create pool
    (wcar {:pool my-pool} (car/ping)) ; Use pool

    (.close my-pool) ; Initiate permanent shutdown of pool
    ;; Note: pool shutdown does NOT interrupt active connections.
    ;; It will prevent any new connections, and will destroy any idle or
    ;; returned connections."

  [pool-opts] (conns/conn-pool :mem/fresh pool-opts))

(defmacro wcar
  "Main entry-point for the Carmine API.
  Does the following:
    1. Establishes a connection to specified Redis server.
    2. Sends any Redis commands in body to the server as a pipeline.
    3. Reads and returns the server's reply.
    4. Destroys the connection, or returns it to connection pool.

  `conn-opts` arg is a map of `:spec`, `:pool` keys.

    `spec` describes the connection details, e.g.:
      - {:host \"127.0.0.1\" :port 6379} ; Default
      - {:uri \"redis://username:password@host.foo.com:9475/3\" ; /3 for db 3
      - {:host \"127.0.0.1\"
         :port 6379
         :ssl-fn :default ; [1]
         :username \"alice\"
         :password \"secret\"
         :timeout-ms 6000
         :db 3}

    `pool` may be:
      - The `:none` keyword (=> don't pool connections)
      - A custom pool you've created manually with `connection-pool`
      - A map of pool-opts as provided to `connection-pool` (a pool
        will be automatically created, then reused on subsequent
        calls with the same opts)

      If no `pool` value is specified, a default pool will be created
      then reused.

  Note that because of thread-binding, you'll probably want to avoid lazy
  Redis commands in `wcar`'s body. Compare:
    `(wcar {} (for   [k [:k1 :k2]] (car/set k :val))` ; Lazy,  0 commands run
    `(wcar {} (doseq [k [:k1 :k2]] (car/set k :val))` ; Eager, 2 commands run

  See also `connection-pool`, `with-replies`.

  [1] Optional `ssl-fn` conn opt takes and returns a `java.net.Socket`:
    (fn [{:keys [^Socket socket host port]}]) -> ^Socket
    `:default` => use `taoensso.carmine.connections/default-ssl-fn`."

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

       ;; Restore any stashed replies to pre-existing context:
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

(comment
  (wcar {}
    (set "temp_foo" nil)
    (get "temp_foo"))

  (seq (wcar {} (parse-raw (get "temp_foo")))))

;;;; Misc core

(defn as-int   [x] (when x (enc/as-int   x)))
(defn as-float [x] (when x (enc/as-float x)))
(defn as-bool  [x] (when x (enc/as-bool  x)))
(defn as-map
  ([x          ] (if (empty? x) {} (persistent! (enc/reduce-kvs assoc! (transient {}) x))))
  ([x & [kf vf]]
   (if (empty? x)
     {}
     (let [kf (case kf nil (fn [k _] k) :keywordize (fn [k _] (keyword k)) kf)
           vf (case vf nil (fn [_ v] v)                                    vf)]
       (persistent!
         (enc/reduce-kvs
           (fn [m k v] (assoc! m (kf k v) (vf k v)))
           (transient {}) x))))))

(comment
  (as-map ["k1" "v1" "k2" "v2"] :keywordize nil)
  (as-map ["k1" "v1" "k2" "v2"] nil (fn [_ v] (str/upper-case v))))

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

(defmacro parse-map [form & [kf vf]]
  `(parse #(as-map % ~kf ~vf) ~form))

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

;;;; Issue #83

(def issue-83-workaround?
  "Workaround for Carmine issue #83.

  Correct/intended behaviour:
    - Byte arrays written with Carmine are read back as byte arrays.

  Break introduced in v2.6.0 (April 1st 2014), issue #83:
    - Byte arrays written with Carmine are accidentally serialized
      with Nippy, and read back as serialized byte arrays.

  Workaround introduced in v2.6.1 (May 1st 2014), issue #83:
    - To help folks who had written binary data under v2.6.0,
      Carmine started trying to auto-deserialize byte arrays that
      start with the standard 4-byte Nippy header byte sequence.

      Benefits:
        b1. Folks affected by the earlier breakage can now read back
            their byte arrays as expected.

      Costs:
        c1. A very minor performance hit when reading binary values
            (because of check for a possible Nippy header).

        c2. May encourage possible dependence on the workaround if
            folks start pre-serializing Nippy data sent to Carmine,
            expecting it to be auto-thawed on read.

  c2 in particular means that it will probably never be safe to
  disable this workaround by default.

  However folks starting with Carmine after v2.6.1 and who have
  never pre-serialized Nippy data written with Carmine may prefer
  to disable the workaround.

  If you're not sure what this is or if it's safe to change, you
  should probably leave it at the default (true) value.

  To change the default (true) value:

    - Call `(alter-var-root #'taoensso.carmine/issue-83-workaround? (fn [_] false))`,
    - or set one of the following to \"false\" or \"FALSE\":
      - `taoensso.carmine.issue-83-workaround` JVM property
      - `TAOENSSO_CARMINE_ISSUE_83_WORKAROUND` env var

  Ref. https://github.com/ptaoussanis/carmine/issues/83 for more info."

  (enc/get-sys-bool* true :taoensso.carmine.issue-83-workaround))

(defn thaw-if-possible-nippy-bytes
  "If given agrgument is a byte-array starting with apparent NPY header,
  calls `nippy/thaw` against argument, otherwise passes it through.

  This util can be useful if you're manually disabling
  `issue-83-workaround` but still have some cases where you're possibly
  trying to read data affected by that issue.

  NB does not trap thaw exceptions.
  See `issue-83-workaround` for more info."

  ([x     ] (thaw-if-possible-nippy-bytes x nil))
  ([x opts] (if (protocol/possible-nippy-bytes? x) (nippy/thaw x opts) x)))

(enc/defalias return protocol/return)
(comment (wcar {} (return :foo) (ping) (return :bar))
         (wcar {} (parse name (return :foo)) (ping) (return :bar)))

;;;; Standard commands

(commands/defcommands) ; Defines 190+ Redis command fns as per official spec

(defn redis-call
  "Sends low-level requests to Redis. Useful for DSLs, certain kinds of command
  composition, and for executing commands that haven't yet been added to the
  official `commands.json` spec, Redis module commands, etc.

  (redis-call [\"set\" \"foo\" \"bar\"] [\"get\" \"foo\"])"
  [& requests]
  (enc/run!
    (fn [[cmd & args]]
      (let [cmd-parts (-> cmd name str/upper-case (str/split #"-"))
            request   (into (vec cmd-parts) args)]
        (commands/enqueue-request (count cmd-parts) request)))
    requests))

(comment (wcar {} (redis-call [:set "foo" "bar"] [:get "foo"]
                              [:config-get "*max-*-entries*"])))

;;;; Lua/scripting support

(defn- sha1-str [x]         (org.apache.commons.codec.digest.DigestUtils/sha1Hex (str x)))
(defn- sha1-ba  [^bytes ba] (org.apache.commons.codec.digest.DigestUtils/sha1Hex ba))

(def script-hash (memoize (fn [script] (sha1-str script))))

(defn- -evalsha* [script numkeys args]
  (redis-call (into [:evalsha (script-hash script) numkeys] args)))

(defn evalsha* "Like `evalsha` but automatically computes SHA1 hash for script."
  [script numkeys & args] (-evalsha* script numkeys args))

(defn- -eval* [script numkeys args]
  (let [parser ; Respect :raw-bulk, otherwise ignore parser:
        (if-not (:raw-bulk? (meta protocol/*parser*))
          nil ; As `parse-raw`:
          (with-meta identity {:raw-bulk? true}))
        [r & _] ; & _ for :as-pipeline
        (parse parser (with-replies :as-pipeline
                        (-evalsha* script numkeys args)))]

    (if (= (:prefix (ex-data r)) :noscript)
      ;;; Now apply context's parser:
      (redis-call (into [:eval script numkeys] args))
      (return r))))

(defn eval*
  "Optimistically tries to send `evalsha` command for given script. In the event
  of a \"NOSCRIPT\" reply, reattempts with `eval`. Returns the final command's
  reply. Redis Cluster note: keys need to all be on same shard."
  [script numkeys & args] (-eval* script numkeys args))

(comment
  (wcar {} (redis-call ["eval" "return 10;" 0]))
  (wcar {} (eval  "return 10;" 0))
  (wcar {} (eval* "return 10;" 0)))

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
;;
;; Current API is a bit sketchy; would do differently today. Though not bad
;; enough to be worth breaking now. Might address in a future version of Carmine.

(declare close-listener)

(defrecord Listener [connection handler state future id status_]
  java.io.Closeable
  (close [this] (close-listener this)))

(defn close-listener [listener]
  (when (compare-and-set! (:status_ listener) :running :closed)
    (conns/close-conn (:connection listener))
    (future-cancel    (:future     listener))))

(defmacro with-open-listener
  "Evaluates body within the context of given listener's pre-existing persistent
  connection."
  [listener & body]
  `(protocol/with-context (:connection ~listener) ~@body
     (protocol/execute-requests (not :get-replies) nil)))

(defn- get-ping-fn
  "Returns (fn ping-fn [action]) with actions:
    :reset!  ; Records activity
    :reset!? ; Records activity and returns true iff no activity recorded in
             ; last `msecs`"
  ;; Much simpler to implement only for listeners than as a general conns feature
  ;; (where hooking in to recording activity is non-trivial).
  [msecs]
  (let [msecs          (long msecs)
        last-activity_ (atom (System/currentTimeMillis))]

    (fn ping-fn [action]
      (case action
        :reset! (enc/reset-in! last-activity_ (System/currentTimeMillis))
        :reset!?
        (let [now (System/currentTimeMillis)]
          (enc/swap-in! last-activity_
            (fn [^long last-activity]
              (if (> (- now last-activity) msecs)
                (enc/swapped now           true)
                (enc/swapped last-activity false)))))))))

(comment (def pfn (get-ping-fn 2000)) (pfn :reset!?))

(defn parse-listener-msg
  "Parses given listener message of form:
    - [\"pong\"                              \"\"]
    - [\"message\"            <channel> <payload>]
    - [\"pmessage\" <pattern> <channel> <payload>], etc.

  and returns {:kind _ :pattern _ :channel _ :payload _ :raw _}."
  [listener-msg]
  (let [v (enc/have vector? listener-msg)]
    (case (count v)
      2 (let [[x1 x2      ] v] {:kind x1                            :payload x2  :raw v})
      3 (let [[x1 x2 x3   ] v] {:kind x1               :channel x2  :payload x3  :raw v})
      4 (let [[x1 x2 x3 x4] v] {:kind x1  :pattern x2  :channel x3  :payload x4  :raw v})
      (do                      {                                                 :raw v}))))

(comment
  (parse-listener-msg ["ping" ""])
  (parse-listener-msg ["message" "chan1" "payload"]))

(defn -call-with-new-listener
  "Implementation detail. Returns new Listener."
  [{:keys [conn-spec init-state handler-fn body-fn
           ;; Incompatible with current unextensible macro API:
           ;; ping-ms error-fn swapping-handler? ; TODO Future release
           ]}]

  (let [status_     (atom :running) ; e/o #{:running :closed :broken}
        state_      (atom init-state)
        handler-fn_ (atom handler-fn)
        future_     (atom nil)
        listener_   (atom nil)
        done?_      (atom false)

        {:keys [in] :as conn}
        (conns/make-new-connection
          (assoc (conns/conn-spec conn-spec)
            :listener? true))

        {:keys [ping-ms]} conn-spec
        ?ping-fn (when-let [ms ping-ms] (get-ping-fn ms))

        handle
        (fn [msg]
          (when-let [hf @handler-fn_]
            (let [{:keys [swap parse]} (meta hf) ; Undocumented
                  msg (if parse (parse-listener-msg msg) msg)]

              (if swap
                (swap! state_ (fn [state] (hf msg  state)))
                (do                       (hf msg @state_))))))

        handle-error
        (fn [error throwable]
          (try
            (handle
              ["carmine" "carmine:listener:error"
               {:error     error
                :throwable throwable
                :listener  @listener_}])

            true
            (catch Throwable t
              (timbre/error  t "Listener (error) handler exception")
              false)))

        done!
        (fn [throwable]
          (when (compare-and-set! done?_ false true)
            (if (compare-and-set! status_ :running :broken)
              (do ; Breaking
                (when-let [f @future_] (future-cancel f))
                (or
                  (handle-error :conn-broken throwable)
                  (if-let [t throwable]
                    (timbre/error t "Listener connection broken")
                    (timbre/error   "Listener connection broken"))))

              (or ; Closing
                (handle-error :conn-closed nil)
                (timbre/error "Listener connection closed"))))

          nil ; Never handle as msg
          )

        msg-polling-future
        (future-call
          (bound-fn []
            (loop []
              (when-not @done?_

                (when-let [pfn ?ping-fn] (pfn :reset!)) ; Record activity on conn
                (when-let [msg
                           (try
                             (protocol/get-unparsed-reply in {})

                             (catch java.net.SocketTimeoutException _
                               (when-let [ex (conns/-conn-error conn)]
                                 (done! ex)))

                             (catch Exception ex
                               (done! ex)))]

                  (try
                    (handle msg)
                    (catch Throwable t
                      (or
                        (handle-error :handler-ex t)
                        (timbre/error t "Listener handler exception")))))

                (recur)))))

        listener
        (->Listener conn handler-fn_ state_ msg-polling-future
          (enc/uuid-str) status_)]

    (reset! listener_ listener)
    (reset! future_   msg-polling-future)

    (protocol/with-context conn (body-fn)
      (protocol/execute-requests (not :get-replies) nil))

    (when-let [pfn ?ping-fn]
      (let [sleep-msecs (+ (long ping-ms) 100)
            f
            (bound-fn []
              (loop []
                (when-not @done?_
                  (Thread/sleep sleep-msecs)
                  (when (pfn :reset!?) ; Should ping now?
                    (try
                      (protocol/with-context conn (ping)
                        (protocol/execute-requests (not :get-replies) nil))
                      (catch Exception ex
                        (done! ex))))
                  (recur))))]

        (doto (Thread. ^Runnable f)
          (.setDaemon true)
          (.start))))

    listener))

(defn -call-with-new-pubsub-listener
  "Implementation detail."
  [{:keys [conn-spec handler body-fn]}]
  (let [?msg-handler-fns (when (map? handler) handler)]
    (-call-with-new-listener
      {:conn-spec  (assoc conn-spec :pubsub-listener? true)
       :init-state (when-let [m ?msg-handler-fns] m)
       :body-fn    body-fn
       :handler-fn
       (if-let [msg-handler-fns ?msg-handler-fns] ; {<chan-or-pattern> (fn [msg])}
         (fn [msg _state]
           (let [{:keys [channel pattern]} (parse-listener-msg msg)]
             (enc/cond
               :if-let [hf (clojure.core/get msg-handler-fns channel)] (hf msg)
               :if-let [hf (clojure.core/get msg-handler-fns pattern)] (hf msg)

               ;; Useful for "carmine"-kind messages
               :if-let [hf (clojure.core/get msg-handler-fns "*")]     (hf msg))))

         handler)})))

(defmacro with-new-listener
  "Creates a persistent[1] connection to Redis server and a future to listen for
  server messages on that connection.

  (fn handler [msg current-state]) will be called on each incoming message [2].

  Evaluates body within the context of the connection and returns a
  general-purpose Listener containing:

    1. The connection for use with `with-open-listener`, `close-listener`.
    2. An atom containing the handler fn.
    3. An atom containing optional listener state.

  Useful for Pub/Sub, monitoring, etc.

  Errors will be published to \"carmine:listener:error\" channel with Clojure
  payload {:keys [error throwable listener]},
    :error e/o #{:conn-closed :conn-broken :handler-ex}.

  [1] You probably do *NOT* want a :timeout for your `conn-spec` here.
  `conn-spec` can include `:ping-ms`, which'll test conn every given msecs.
  0
  [2] See also `parse-listener-msg`."
  ;; [{:keys []} & body] ; TODO Future release
  [conn-spec handler-fn init-state & body]
  `(-call-with-new-listener
     {:conn-spec  ~conn-spec
      :init-state ~init-state
      :handler-fn ~handler-fn
      :body-fn    (fn [] ~@body)}))

(defmacro with-new-pubsub-listener
  "Like `with-new-listener` but `handler` is:
  {<channel-or-pattern> (fn handler [msg])}.

  Example:

    (with-new-pubsub-listener
      {} ; Connection spec, as per `wcar` docstring [1]

      {\"channel1\" (fn [msg] (println \"Channel match: \" msg))
       \"user*\"    (fn [msg] (println \"Pattern match: \" msg))}

      (subscribe \"foobar\") ; Subscribe thread conn to \"foobar\" channel
      (psubscribe \"foo*\")  ; Subscribe thread conn to \"foo*\"   channel pattern
      )

  See `with-new-listener` for more info."
  ;; [{:keys []} & body] ; TODO Future release
  [conn-spec handler & subscription-commands]
  `(-call-with-new-pubsub-listener
     {:conn-spec ~conn-spec
      :handler   ~handler
      :body-fn   (fn [] ~@subscription-commands)}))

(comment
  (wcar {:cache-buster 1} :as-pipeline
    (return (conns/-conn-error (:conn protocol/*context*)))
    (ping)
    (subscribe "foo")
    (return (conns/-conn-error (:conn protocol/*context*)))
    (ping)
    (quit))

  (def my-listener
    (with-new-pubsub-listener {:ping-ms 3000 :cache-buster 2}
      ^:parse
      (fn [msg state]
        (let [{:keys [kind channel pattern payload raw]} msg]
          (println [:debug/global msg])
          (when (= payload "throw")
            (throw (Exception. "Whoops!")))))
      #_
      {"chan1" (fn [msg] (println [:debug/chan1 msg]))
       "*"     (fn [msg] (println [:debug/*     msg]))}

      (subscribe   "chan1")
      (psubscribe "pchan1" "*")))

  (wcar {} (publish "chan1" "msg"))
  (wcar {} (publish "chan1" "throw"))
  (close-listener my-listener))

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
            [new-val return-val] (enc/swapped-vec (f old-val nx?))
            cas-success?
            (case new-val
              :swap/abort  (do (unwatch)         true)
              :swap/delete (do (unwatch) (del k) true)
              (parse nil
                (with-replies
                  (parse-suppress (multi) (set k new-val))
                  (exec))))]

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

(let [script (enc/have (enc/slurp-resource "taoensso/carmine/lua/cas-set.lua"))]
  (defn compare-and-set "Experimental."
    ([k old-val new-val]
     (let [[?sha raw-bs] (prep-cas-old-val old-val)]
       (compare-and-set k raw-bs ?sha new-val)))

    ([k old-val ?sha new-val]
     (let [delete? (= new-val :cas/delete)]
       (lua script {:k k}
         {:old-?sha (or ?sha "")
          :old-?val (if ?sha "" old-val)
          :delete   (if delete? 1 0)
          :new-val  (if delete? "" new-val)})))))

(let [script (enc/have (enc/slurp-resource "taoensso/carmine/lua/cas-hset.lua"))]
  (defn compare-and-hset "Experimental."
    ([k field old-val new-val]
     (let [[?sha raw-bs] (prep-cas-old-val old-val)]
       (compare-and-hset k field raw-bs ?sha new-val)))

    ([k field old-val ?sha new-val]
     (let [delete? (= new-val :cas/delete)]
       (lua script {:k k}
         {:field    field
          :old-?sha (or ?sha "")
          :old-?val (if ?sha "" old-val)
          :delete   (if delete? 1 0)
          :new-val  (if delete? "" new-val)})))))

(comment
  (wcar {} (del "cas-k") (set "cas-k" 0) (compare-and-set "cas-k" 0 1))
  (wcar {} (compare-and-set "cas-k" 1 2))
  (wcar {} (get "cas-k"))

  (wcar {} (del "cas-k") (hset "cas-k" "field" 0) (compare-and-hset "cas-k" "field" 0 1))
  (wcar {} (compare-and-hset "cas-k" "field" 1 2))
  (wcar {} (hget "cas-k" "field")))

(def swap "Experimental."
  (let [cas-get
        (let [script (enc/have (enc/slurp-resource "taoensso/carmine/lua/cas-get.lua"))]
          (fn [k] (lua script {:k k} {})))]

    (fn [k f & [nmax-attempts abort-val]]
      (loop [idx 1]
        (let [[old-val ex sha]     (parse nil (with-replies (cas-get k)))
              nx?                  (= ex 0)
              ?sha                 (when-not (= sha "") sha)
              [new-val return-val] (enc/swapped-vec (f old-val nx?))
              cas-success?
              (case new-val
                :swap/abort true
                :swap/delete
                (if nx?
                  true
                  (= 1
                    (parse nil
                      (with-replies
                        (compare-and-set k old-val ?sha :cas/delete)))))

                (= 1
                  (parse nil
                    (with-replies
                      (if nx?
                        (setnx k new-val)
                        (compare-and-set k old-val ?sha new-val))))))]

          (if cas-success?
            (return return-val)
            (if (or (nil? nmax-attempts) (< idx (long nmax-attempts)))
              (recur (inc idx))
              (return abort-val))))))))

(def hswap "Experimental."
  (let [cas-hget
        (let [script (enc/have (enc/slurp-resource "taoensso/carmine/lua/cas-hget.lua"))]
          (fn [k field] (lua script {:k k} {:field field})))]

    (fn [k field f & [nmax-attempts abort-val]]
      (loop [idx 1]
        (let [[old-val ex sha]     (parse nil (with-replies (cas-hget k field)))
              nx?                  (= ex 0)
              ?sha                 (when-not (= sha "") sha)
              [new-val return-val] (enc/swapped-vec (f old-val nx?))
              cas-success?
              (case new-val
                :swap/abort true
                :swap/delete
                (if nx?
                  true
                  (= 1
                    (parse nil
                      (with-replies
                        (compare-and-hset k field old-val ?sha :cas/delete)))))

                (= 1
                  (parse nil
                    (with-replies
                      (if nx?
                        (hsetnx k field new-val)
                        (compare-and-hset k field old-val ?sha new-val))))))]

          (if cas-success?
            (return return-val)
            (if (or (nil? nmax-attempts) (< idx (long nmax-attempts)))
              (recur (inc idx))
              (return abort-val))))))))

(comment
  (enc/qb 100 (wcar {} (swap "swap-k" (fn [?old _] ?old))))
  (wcar {} (get  "swap-k"))
  (wcar {} (swap "swap-k" (fn [?old _] :swap/delete))))

;;;;

(def hmsetnx "Experimental."
  (let [script (enc/have (enc/slurp-resource "taoensso/carmine/lua/hmsetnx.lua"))]
    (fn [key field value & more]
      (-eval* script 1 (into [key field value] more)))))

(comment
  (wcar {} (hgetall "foo"))
  (wcar {} (hmsetnx "foo" "f1" "v1" "f2" "v2")))

;;;;

(defn reduce-scan
  "For use with `scan`, `hscan`, `zscan`, etc. Takes:
    - (fn rf      [acc scan-result]) -> next accumulator
    - (fn scan-fn [cursor]) -> next scan result"
  ([rf          scan-fn] (reduce-scan rf nil scan-fn))
  ([rf acc-init scan-fn]
   (loop [cursor "0" acc acc-init]
     (let [[next-cursor in] (scan-fn cursor)]
       (if (= next-cursor "0")
         (rf acc in)
         (let [result (rf acc in)]
           (if (reduced? result)
             @result
             (recur next-cursor result))))))))

(comment
  (reduce-scan (fn rf [acc in] (into acc in))
    [] (fn scan-fn [cursor] (wcar {} (scan cursor)))))

(defn reduce-hscan
  "Experimental. Like `reduce-scan` but:
    - `rf` is (fn [acc k v]), as in `reduce-kv`.
    - `rf` will never be called with the same key twice
      (i.e. automatically de-duplicates elements)."
  ([rf          scan-fn] (reduce-hscan rf nil scan-fn))
  ([rf acc-init scan-fn]
   (let [keys-seen_ (volatile! (transient #{}))]

     (reduce-scan
       (fn wrapped-rf [acc kvs]
         (enc/reduce-kvs
           (fn [acc k v]
             (if (contains? @keys-seen_ k)
               acc
               (do
                 (vswap! keys-seen_ conj! k)
                 (enc/convey-reduced (rf acc k v)))))
           acc
           kvs))

       acc-init scan-fn))))

(comment
  (wcar {} (del "test:foo"))
  (wcar {} (doseq [i (range 1e4)] (hset "test:foo" (str "k" i) (str "v" i))))
  (count
    (reduce-hscan
      (fn rf [acc k v]
        (if #_true false
          (reduced (assoc acc k v))
          (do      (assoc acc k v))))
      {}
      (fn [cursor] (wcar {} (hscan "test:foo" cursor))))))

(defn scan-keys
  "Returns a set of Redis keys that match the given pattern.
  Like the Redis `keys` command, but implemented using `scan` so safe to
  use in production.

    (scan-keys <conn-opts> \"*\")     => Set of all keys
    (scan-keys <conn-opts> \"foo:*\") => Set of all keys starting with \"foo:\""

  [conn-opts pattern]
  (persistent!
    (reduce-scan enc/into!
      (transient #{})
      (fn [cursor]
        (wcar conn-opts (scan cursor "MATCH" pattern))))))

(comment (scan-keys {} "*"))

;;;; Deprecated

(enc/deprecated
  (def      ^:deprecated as-long      "DEPRECATED: Use `as-int` instead."      as-int)
  (def      ^:deprecated as-double    "DEPRECATED: Use `as-float` instead."    as-float)
  (def      ^:deprecated hash-script  "DEPRECATED: Use `script-hash` instead." script-hash)
  (defmacro ^:deprecated parse-long   "DEPRECATED: Use `parse-int` instead."   [& body] `(parse as-long   ~@body))
  (defmacro ^:deprecated parse-double "DEPRECATED: Use `parse-float` instead." [& body] `(parse as-double ~@body))
  (defn     ^:deprecated kname
    "DEPRECATED: Use `key` instead. `key` does not filter nil parts."
    [& parts] (apply key (filter identity parts)))

  (comment (kname :foo/bar :baz "qux" nil 10))

  (def  ^:deprecated serialize            "DEPRECATED: Use `freeze` instead."       freeze)
  (def  ^:deprecated preserve             "DEPRECATED: Use `freeze` instead."       freeze)
  (def  ^:deprecated remember             "DEPRECATED: Use `return` instead."       return)
  (def  ^:deprecated ^:macro skip-replies "DEPRECATED: Use `with-replies` instead." #'with-replies)
  (def  ^:deprecated ^:macro with-reply   "DEPRECATED: Use `with-replies` instead." #'with-replies)
  (def  ^:deprecated ^:macro with-parser  "DEPRECATED: Use `parse` instead."        #'parse)
  (defn ^:deprecated lua-script "DEPRECATED: Use `lua` instead." [& args] (apply lua args))
  (defn ^:deprecated make-keyfn "DEPRECATED: Use `kname` instead."
    [& prefix-parts]
    (let [prefix (when (seq prefix-parts) (str (apply kname prefix-parts) ":"))]
      (fn [& parts] (str prefix (apply kname parts)))))

  (defn ^:deprecated make-conn-pool "DEPRECATED: Use `wcar` instead."
    [& opts] (conns/conn-pool (apply hash-map opts)))

  (defn ^:deprecated make-conn-spec "DEPRECATED: Use `wcar` instead."
    [& opts] (conns/conn-spec (apply hash-map opts)))

  (defmacro ^:deprecated with-conn "DEPRECATED: Use `wcar` instead."
    [connection-pool connection-spec & body]
    `(wcar {:pool ~connection-pool :spec ~connection-spec} ~@body))

  (defmacro ^:deprecated atomically "DEPRECATED: Use `atomic` instead."
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

  (defmacro ^:deprecated ensure-atomically "DEPRECATED: Use `atomic` instead."
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

  (defn ^:deprecated hmget* "DEPRECATED: Use `parse-map` instead."
    [key field & more]
    (let [fields (cons field more)
          inner-parser (when-let [p protocol/*parser*] #(mapv p %))
          outer-parser #(zipmap fields %)]
      (->> (apply hmget key fields)
        (parse (parser-comp outer-parser inner-parser)))))

  (defn ^:deprecated hgetall* "DEPRECATED: Use `parse-map` instead."
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
    (wcar {} (parse str/upper-case (hgetall* "hkey")))))
