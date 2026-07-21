(ns ^:no-doc taoensso.carmine-v4.scripts
  "Private implementation for the public Carmine v4 scripting and atomic APIs."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss :as truss]
   [taoensso.carmine-v4.utils       :as utils]
   [taoensso.carmine-v4.cluster     :as cluster]
   [taoensso.carmine-v4.resp        :as resp]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write]))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4/*auto-thaw?*
  taoensso.carmine-v4/eval
  taoensso.carmine-v4/evalsha
  taoensso.carmine-v4/eval_ro
  taoensso.carmine-v4/evalsha_ro
  taoensso.carmine-v4/get
  taoensso.carmine-v4/hget
  taoensso.carmine-v4/hsetnx
  taoensso.carmine-v4/set
  taoensso.carmine-v4/with-car)

(alias 'core 'taoensso.carmine-v4)

(def ^:private cached-script-hash
  (enc/cache {:size 256}
    (fn [script]
      (org.apache.commons.codec.digest.DigestUtils/sha1Hex ^String script))))

(defn- prepare-script+hash [script]
  (let [prepared-script (write/prepare-arg script)
        hash
        (if (string? prepared-script)
          (cached-script-hash prepared-script)
          (org.apache.commons.codec.digest.DigestUtils/sha1Hex
            ^bytes (write/arg-payload-bytes prepared-script)))]
    [prepared-script hash]))

(defn ^:public script-hash
  "Returns the hexadecimal SHA1 hash Redis uses for the exact encoded bytes of
  `script`."
  [script]
  (get (prepare-script+hash script) 1))

(defn ^:public evalsha*
  "Computes the Redis SHA1 hash for `script`, then calls [[evalsha]]."
  [script numkeys & args]
  (apply core/evalsha (script-hash script) numkeys args))

(defn- noscript-reply? [x]
  (com/reply-error?
    {:eid :carmine.read/redis-error-reply, :code "NOSCRIPT"}
    x))

(defn- eval-fallback*
  [called evalsha-fn eval-fn script numkeys args]
  (resp/ensure-context! called)
  (when com/*tx-effects?*
    ;; Inside MULTI, EVALSHA replies "QUEUED" so a NOSCRIPT fallback is
    ;; impossible: the call would work while the script is cached, then
    ;; fail in production after a SCRIPT FLUSH or server restart.
    (truss/ex-info!
      "[Carmine] Script fallback is not supported inside `transact!` effects"
      {:eid :carmine.script/tx-effects-not-supported
       :hint "Use `prepare-lua` + the generated evalsha command, or run the script outside the transaction."}))
  (when (and (resp/cluster-context?) (cluster/broadcast-target?))
    (truss/ex-info!
      "[Carmine] Script fallback is not supported for broadcast Cluster targets"
      {:eid :carmine.script/cluster-broadcast-not-supported
       :hint "Use the generated eval/eval_ro command directly for :masters or :nodes."}))
  (let [[script script-hash] (prepare-script+hash script)
        natural-replies? (resp/ctx-natural-replies?)
        ambient-read-mode com/*read-mode*
        ;; Function parsers run after NOSCRIPT detection via local-echo. Keep
        ;; their effective read options, RF parsing, and RESP null identity.
        internal-parser (com/defer-fn-parser com/*parser*)
        run-immediate
        (fn [eval-fn script]
          (binding [com/*read-mode*
                    (if (identical? ambient-read-mode :skip)
                      :skip+errors
                      ambient-read-mode)
                    com/*parser* internal-parser]
            (first
              (resp/with-replies
                {:natural-replies? natural-replies?
                 :as-vec? true, :error-mode :return} nil
                #(apply eval-fn script numkeys args)))))
        reply (run-immediate evalsha-fn script-hash)
        reply (if (noscript-reply? reply)
                (run-immediate eval-fn script)
                reply)]
    (resp/local-echo reply)))

(defn ^:public eval*
  "Runs `script` with `EVALSHA`. If Redis returns `NOSCRIPT`, retries with
  `EVAL`.

  `numkeys` declares how many leading `args` are Redis keys. In Cluster, all
  declared keys must belong to the same slot. This function rejects broadcast
  Cluster targets. It also rejects calls inside
  [[taoensso.carmine-v4/transact!]]'s effects function, where fallback is not
  possible. Use the generated [[eval]] command to broadcast. Each [[eval*]]
  call makes an immediate round-trip and flushes earlier requests in the current
  context."
  [script numkeys & args]
  (eval-fallback* 'eval* core/evalsha core/eval script numkeys args))

(defn ^:public eval-ro*
  "Read-only [[eval*]] variant for Redis 7+. Runs `script` with `EVALSHA_RO`. If
  Redis returns `NOSCRIPT`, retries with `EVAL_RO`.

  `numkeys` declares how many leading `args` are Redis keys. In Cluster, all
  declared keys must belong to the same slot. This function rejects broadcast
  Cluster targets. It also rejects calls inside
  [[taoensso.carmine-v4/transact!]]'s effects function, where fallback is not
  possible. Use the generated [[eval_ro]] command to broadcast. Each
  [[eval-ro*]] call makes an immediate round-trip. Redis prevents writes from
  the script."
  [script numkeys & args]
  (eval-fallback* 'eval-ro* core/evalsha_ro core/eval_ro script numkeys args))

(defn- variable-name-code-point? [^long code-point]
  (or (Character/isLetterOrDigit (int code-point))
    (case code-point
      (95 45 46 47 63 33) true ; _ - . / ? !
      false)))

(defn- lua-identifier-code-point?
  "Returns true iff `code-point`, the character before `_:` in Lua code, is a
  Unicode letter or digit, or underscore. When true, the following `_:` is not
  a Carmine variable start (e.g. `obj_:key`). This test is deliberately narrower
  than [[variable-name-code-point?]]: an operator such as `-`, `.`, or `/` must
  not hide a variable (e.g. `x-_:n` means `x - _:n`)."
  [^long code-point]
  (or (Character/isLetterOrDigit (int code-point))
    (== code-point 95))) ; _

(defn- valid-variable-name? [^String variable-name]
  (let [length (.length variable-name)]
    (and (pos? length)
      (loop [idx 0]
        (if (>= idx length)
          true
          (let [code-point (.codePointAt variable-name idx)]
            (when (variable-name-code-point? code-point)
              (recur (+ idx (Character/charCount code-point))))))))))

(defn- long-bracket-open
  "Returns `[level content-start]` for a Lua long-bracket opener at `idx`."
  [^String s ^long idx]
  (let [length (.length s)]
    (when (and (< idx length) (= (.charAt s idx) \[))
      (loop [cursor (inc idx), level 0]
        (when (< cursor length)
          (case (.charAt s cursor)
            \= (recur (inc cursor) (inc level))
            \[ [level (inc cursor)]
            nil))))))

(defn- long-bracket-end
  [^String s ^long content-start ^long level]
  (let [close-token (str "]" (apply str (repeat level "=")) "]")
        close-idx   (.indexOf s close-token (int content-start))]
    (if (neg? close-idx)
      (.length s)
      (+ close-idx (.length close-token)))))

(defn- quoted-string-end
  [^String s ^long idx ^long quote-int]
  (let [length (.length s)]
    (loop [cursor (inc idx)]
      (if (>= cursor length)
        length
        (let [c (.charAt s cursor)]
          (cond
            (= c \\)    (recur (min length (+ cursor 2)))
            (== (int c) quote-int) (inc cursor)
            :else       (recur (inc cursor))))))))

(defn- append-region!
  [^StringBuilder sb ^String s ^long start ^long end]
  (.append sb s (int start) (int end))
  sb)

(defn- line-comment-end
  [^String s ^long content-start]
  (let [length (.length s)]
    (loop [idx content-start]
      (if (>= idx length)
        length
        (let [c (.charAt s idx)]
          (if (or (= c \return) (= c \newline))
            (let [next-idx (inc idx)]
              (if (and (< next-idx length)
                    (let [next-c (.charAt s next-idx)]
                      (or (and (= c \return) (= next-c \newline))
                          (and (= c \newline) (= next-c \return)))))
                (inc next-idx)
                next-idx))
            (recur (inc idx))))))))

(defn- substitute-script-vars
  [^String script replacements]
  (let [length (.length script)
        sb     (StringBuilder. length)
        unresolved_ (volatile! [])
        prepared
        (loop [idx 0]
          (if (>= idx length)
            (.toString sb)
            (let [c (.charAt script idx)]
              (cond
                ;; Lua line or long comment.
                (and (= c \-) (< (inc idx) length)
                  (= (.charAt script (inc idx)) \-))
                (if-let [[level content-start] (long-bracket-open script (+ idx 2))]
                  (let [end (long-bracket-end script content-start level)]
                    (append-region! sb script idx end)
                    (recur (long end)))
                  (let [end (line-comment-end script (+ idx 2))]
                    (append-region! sb script idx end)
                    (recur (long end))))

                ;; Lua short string.
                (or (= c \') (= c \"))
                (let [end (quoted-string-end script idx (int c))]
                  (append-region! sb script idx end)
                  (recur (long end)))

                ;; Lua long string.
                (= c \[)
                (if-let [[level content-start] (long-bracket-open script idx)]
                  (let [end (long-bracket-end script content-start level)]
                    (append-region! sb script idx end)
                    (recur (long end)))
                  (do (.append sb c) (recur (inc idx))))

                ;; Carmine named variable in ordinary Lua code.
                (and (= c \_) (< (inc idx) length)
                  (= (.charAt script (inc idx)) \:)
                  (or (zero? idx)
                    (not
                      (lua-identifier-code-point?
                        (.codePointBefore script (int idx))))))
                (let [name-start (+ idx 2)
                      name-end
                      (loop [cursor name-start]
                        (if (< cursor length)
                          (let [code-point (.codePointAt script cursor)]
                            (if (variable-name-code-point? code-point)
                              (recur (+ cursor (Character/charCount code-point)))
                              cursor))
                          cursor))
                      variable-name (.substring script (int name-start) (int name-end))]
                  (if-let [replacement (get replacements variable-name)]
                    (.append sb ^String replacement)
                    (do
                      (vswap! unresolved_ conj variable-name)
                      (append-region! sb script idx name-end)))
                  (recur (long name-end)))

                :else
                (do (.append sb c) (recur (inc idx)))))))]
    (if-let [unresolved (not-empty (vec (distinct @unresolved_)))]
      (truss/ex-info! "[Carmine] Unresolved named Lua variables"
        {:eid :carmine.script/unresolved-variable-names
         :variables unresolved})
      prepared)))

(def ^:private cached-prepare-lua
  (enc/cache {:size 256}
    (fn [script key-vars arg-vars]
      (let [indexed-vars
            (fn [vars array-name]
              (map-indexed
                (fn [idx variable-name]
                  [variable-name (str array-name "[" (inc idx) "]")])
                vars))
            replacements
            (into {}
              (concat
                (indexed-vars key-vars "KEYS")
                (indexed-vars arg-vars "ARGV")))]
        (substitute-script-vars script replacements)))))

(defn- var-name [x]
  (let [variable-name
        (cond
          (keyword? x) (subs (str x) 1)
          (symbol?  x) (str x)
          (string?  x) x
          :else
          (truss/ex-info! "[Carmine] Named Lua variables require nameable values"
            {:eid :carmine.script/invalid-variable-name
             :reason :unexpected-type
             :variable {:value x, :type (type x)}}))]
    (when-not (valid-variable-name? variable-name)
      (truss/ex-info! "[Carmine] Invalid named Lua variable"
        {:eid :carmine.script/invalid-variable-name
         :reason :invalid-characters
         :variable {:value x, :normalized variable-name, :type (type x)}
         :allowed "Unicode letters/digits and _ - . / ? !"}))
    variable-name))

(defn- validate-vars! [key-vars arg-vars]
  (let [provided (into [] (concat key-vars arg-vars))
        duplicates (->> provided frequencies (keep (fn [[x n]] (when (> n 1) x))) sort vec)]
    (when (seq duplicates)
      (truss/ex-info! "[Carmine] Lua key and argument variable names must be unique"
        {:eid :carmine.script/duplicate-variable-names, :variables duplicates}))))

(defn ^:public prepare-lua
  "Prepares a Lua `script` that contains named Carmine variables. Does not
  perform Redis I/O.

  `key-vars` and `arg-vars` are ordered collections of keyword, symbol, or string
  names. In executable Lua code, complete `_:name` forms become indexed `KEYS[]`
  or `ARGV[]` references. Strings and comments do not change. Names may contain
  Unicode letters, digits, `_`, `-`, `.`, `/`, `?`, or `!`. Each name must be
  unique across both collections. You must supply each named variable in
  executable code, but supplied names may be unused.

  You can reuse the result with [[eval]], [[eval*]], [[evalsha*]], [[eval_ro]],
  or [[eval-ro*]]. Supply values in the same order."
  [script key-vars arg-vars]
  (let [script   (str script)
        key-vars (mapv var-name (or key-vars []))
        arg-vars (mapv var-name (or arg-vars []))]
    (validate-vars! key-vars arg-vars)
    (cached-prepare-lua script key-vars arg-vars)))

(defn- prep-vars [keys args]
  (let [key-entries (when (map? keys) (mapv (fn [[k v]] [(var-name k) v]) keys))
        arg-entries (when (map? args) (mapv (fn [[k v]] [(var-name k) v]) args))
        key-values  (vec (if key-entries (mapv second key-entries) keys))
        arg-values  (vec (if arg-entries (mapv second arg-entries) args))]
    [(some->> key-entries (mapv first))
     (some->> arg-entries (mapv first))
     key-values
     arg-values]))

(defn- lua-call [eval-fn script keys args]
  (let [[key-vars arg-vars key-values arg-values] (prep-vars keys args)
        prepared-script (prepare-lua script key-vars arg-vars)]
    (apply eval-fn prepared-script (count key-values)
      (into key-values arg-values))))

(defn ^:public lua
  "Runs a Lua `script` with optional named key and argument substitution.

  `keys` and `args` may be sequential values or ordered maps. Map keys replace
  complete `_:name` forms in executable Lua code with indexed `KEYS[]` or
  `ARGV[]` references. Strings and comments do not change. Names must be unique
  across both maps:

    (lua \"return redis.call('get', _:key)\" {:key \"my-key\"} {})

  Supply Redis keys separately from other arguments. This lets Cluster route
  and validate the call. Like [[eval*]], this function makes an immediate
  round-trip and rejects broadcast Cluster targets and calls inside
  [[taoensso.carmine-v4/transact!]]'s effects function."
  [script keys args]
  (lua-call eval* script keys args))

(defn ^:public lua-ro
  "Read-only [[lua]] variant for Redis 7+.

  Accepts the same sequential values or ordered maps as [[lua]], but uses
  [[eval-ro*]]. Redis rejects scripts that write. This function makes an
  immediate round-trip and rejects broadcast Cluster targets and calls inside
  [[taoensso.carmine-v4/transact!]]'s effects function."
  [script keys args]
  (lua-call eval-ro* script keys args))

;;;; Compare-and-set

(def ^:private compare-and-set-script
  "local current = redis.call('GET', KEYS[1])
   if current ~= false and current == ARGV[1] then
     redis.call('SET', KEYS[1], ARGV[2])
     return 1
   end
   return 0")

(def ^:private compare-and-delete-script
  "local current = redis.call('GET', KEYS[1])
   if current ~= false and current == ARGV[1] then
     redis.call('DEL', KEYS[1])
     return 1
   end
   return 0")

(def ^:private compare-and-hset-script
  "local current = redis.call('HGET', KEYS[1], ARGV[1])
   if current ~= false and current == ARGV[2] then
     redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
     return 1
   end
   return 0")

(def ^:private compare-and-hdel-script
  "local current = redis.call('HGET', KEYS[1], ARGV[1])
   if current ~= false and current == ARGV[2] then
     redis.call('HDEL', KEYS[1], ARGV[1])
     return 1
   end
   return 0")

(def ^:private compare-result-parser
  (com/fn-parser {:read-mode nil} #(= % 1)))

(defn- compare-script [called script & args]
  (resp/ensure-context! called)
  (enc/binding* [com/*parser* compare-result-parser]
    (apply core/eval script 1 args)))

(defn ^:public compare-and-set
  "Atomically replaces the value at `key` iff its exact stored bytes equal the
  standard Carmine encoding of `expected`, returning true on replacement and
  false otherwise.

  A missing key differs from every supplied value, including nil. A successful
  replacement has standard Redis `SET` behaviour and clears the key TTL. To
  create only when absent, use [[set]] with `NX`.

  This function queues one atomic single-key operation and routes normally in
  Cluster. [[bytes]] and [[freeze]] may be used to control argument encoding.
  With `:natural-replies? true`, the Redis 1 or 0 bypasses the boolean parser."
  [key expected replacement]
  (compare-script 'compare-and-set compare-and-set-script
    key expected replacement))

(defn ^:public compare-and-delete
  "Atomically deletes `key` iff its exact stored bytes equal the standard
  Carmine encoding of `expected`, returning true on deletion and false
  otherwise. A missing key differs from every supplied value, including nil.

  This function queues one atomic single-key operation and routes normally in
  Cluster. [[bytes]] and [[freeze]] may be used to control argument encoding.
  With `:natural-replies? true`, the Redis 1 or 0 bypasses the boolean parser."
  [key expected]
  (compare-script 'compare-and-delete compare-and-delete-script key expected))

(defn ^:public compare-and-hset
  "Atomically replaces `field` in the hash at `key` iff its exact stored bytes
  equal the standard Carmine encoding of `expected`, returning true on
  replacement and false otherwise.

  A missing field differs from every supplied value, including nil. A successful
  update keeps the hash key TTL. To create only when absent, use [[hsetnx]].

  This function queues one atomic single-key operation and routes normally in
  Cluster. [[bytes]] and [[freeze]] may be used to control argument encoding.
  With `:natural-replies? true`, the Redis 1 or 0 bypasses the boolean parser."
  [key field expected replacement]
  (compare-script 'compare-and-hset compare-and-hset-script
    key field expected replacement))

(defn ^:public compare-and-hdel
  "Atomically deletes `field` from the hash at `key` iff its exact stored bytes
  equal the standard Carmine encoding of `expected`, returning true on deletion
  and false otherwise. A missing field differs from every supplied value,
  including nil. If other fields remain, the hash key keeps its TTL.

  This function queues one atomic single-key operation and routes normally in
  Cluster. [[bytes]] and [[freeze]] may be used to control argument encoding.
  With `:natural-replies? true`, the Redis 1 or 0 bypasses the boolean parser."
  [key field expected]
  (compare-script 'compare-and-hdel compare-and-hdel-script key field expected))

;;;; Atomic swaps

(def ^:private no-abort-val (Object.))

(defn- parse-swap-opts [opts]
  (let [opts (or opts {})]
    (truss/have map? opts)
    (truss/have [:ks<= #{:max-attempts :retry-backoff-ms :abort-val}] opts)
    (let [max-attempts     (get opts :max-attempts 8)
          retry-backoff-ms (get opts :retry-backoff-ms)]
      (truss/have pos-int? max-attempts)
      (truss/have [:or nil? nat-int? fn?] retry-backoff-ms)
      {:max-attempts max-attempts
       :retry-backoff-ms retry-backoff-ms
       :abort-val (get opts :abort-val no-abort-val)})))

(defn- internal-swap-call [conn-mgr body-fn]
  (let [reply
        (core/with-car conn-mgr
          (fn []
            (enc/binding*
              [com/*natural-replies?* false
               com/*read-mode*         nil
               com/*parser*            nil]
              (body-fn))))]
    ;; `with-car`'s default :error-mode already throws. Retain this guard so
    ;; orchestration cannot silently reinterpret an explicitly returned error.
    (if (com/reply-error? reply) (throw reply) reply)))

(defn- read-swap-current [conn-mgr operation key field]
  (let [reply
        (core/with-car conn-mgr
          (fn []
            ;; Preserve exact stored bytes, including Carmine's blob marker.
            (enc/binding*
              [core/*auto-thaw?*       false
               com/*natural-replies?* false
               com/*read-mode*         :bytes
               com/*parser*            nil]
              (case operation
                :swap  (core/get  key)
                :hswap (core/hget key field)))))]
    (cond
      (nil? reply)             [false nil]
      (enc/bytes? reply)       [true reply]
      (com/reply-error? reply) (throw reply)
      :else
      (truss/ex-info! "[Carmine] Unexpected Redis swap read reply"
        {:eid :carmine.swap/unexpected-read-reply
         :operation operation
         :reply (enc/typed-val reply)}))))

(defn- compare-swap-mutation!
  [conn-mgr operation key field exists? old-ba new-val]
  (cond
    (identical? new-val :swap/abort)
    true

    (identical? new-val :swap/delete)
    (if-not exists?
      true
      (true?
        (internal-swap-call conn-mgr
          #(case operation
             :swap  (compare-and-delete key       (write/bytes old-ba))
             :hswap (compare-and-hdel key field (write/bytes old-ba))))))

    exists?
    (true?
      (internal-swap-call conn-mgr
        #(case operation
           :swap  (compare-and-set  key       (write/bytes old-ba) new-val)
           :hswap (compare-and-hset key field (write/bytes old-ba) new-val))))

    :else
    (let [reply
          (internal-swap-call conn-mgr
            #(case operation
               :swap  (core/set    key       new-val "NX")
               :hswap (core/hsetnx key field new-val)))]
      (case operation
        :swap  (= reply "OK")
        :hswap (= reply 1)))))

(defn- swap* [operation conn-mgr key field opts swap-fn]
  (truss/have fn? swap-fn)
  (let [{:keys [max-attempts retry-backoff-ms abort-val]} (parse-swap-opts opts)
        read-opts (com/get-read-opts)
        read-mode (:read-mode (com/describe-read-opts read-opts))
        key       (write/prepare-arg key)
        field     (when (= operation :hswap) (write/prepare-arg field))]
    (when (com/skip-read-mode? read-mode)
      (truss/ex-info! "[Carmine] Swap callback requires the current Redis value"
        {:eid :carmine.swap/skip-read-not-supported
         :operation operation}))
    (loop [attempt 1]
      (let [[exists? old-ba] (read-swap-current conn-mgr operation key field)
            old-val
            (when exists?
              (let [reply (read/complete-stored-blob read-opts old-ba)]
                (if (com/reply-error? reply) (throw reply) reply)))
            [new-val return-val] (enc/swapped-vec (swap-fn old-val (not exists?)))]
        (if (compare-swap-mutation!
              conn-mgr operation key field exists? old-ba new-val)
          return-val
          (if (< attempt max-attempts)
            (do
              (utils/backoff! retry-backoff-ms attempt)
              (recur (inc attempt)))
            (if (identical? abort-val no-abort-val)
              (truss/ex-info! "[Carmine] Optimistic swap conflicted"
                {:eid :carmine.swap/conflict
                 :operation operation
                 :attempts attempt})
              abort-val)))))))

(defn ^:public swap
  "Atomically transforms the value at `key` using optimistic retries.

  Arity forms:
    (swap conn-mgr key swap-fn)
    (swap conn-mgr key opts swap-fn)

  `swap-fn` is the final argument and is called as `(swap-fn old-val missing?)`.
  `missing?` is true iff the key is absent. The function may run more than once,
  so it must not have non-repeatable side effects. A plain callback result is
  both stored and returned. Use [[taoensso.encore/swapped]] to specify different
  stored and returned values.

  A new value of `:swap/abort` makes no change. `:swap/delete` conditionally
  deletes the observed value. Wrap either value with
  [[taoensso.encore/swapped]] to return a different value. These keywords are
  reserved directives. [[swap]] cannot store them.

  Options:
    - `:max-attempts`: Positive attempt limit (default 8).
    - `:retry-backoff-ms`: Nil, a constant delay, or
      `(fn [attempt] delay-ms)`. Nil or zero adds no delay.
    - `:abort-val`: Value to return after the last conflict. If omitted, throws
      an exception with `:eid :carmine.swap/conflict`.

  Updates compare the exact stored bytes; an absent value is created only if
  still absent. Transport failures and callback errors are not retried. A
  successful write has standard Redis `SET` behaviour and clears the TTL. This
  function performs its own Redis calls through `conn-mgr`, works with Cluster,
  and does not join an enclosing [[wcar]] pipeline. Ambient read modes control
  the decoding of `old-val`. Ambient reply parsers do not transform it.

  With an ordinary pooled manager, calling [[swap]] while already holding a
  connection from the same manager (e.g. inside [[wcar]]) can exhaust the pool
  and block. Call it outside [[wcar]], or use a separate manager."
  ([conn-mgr key      swap-fn] (swap* :swap conn-mgr key nil nil  swap-fn))
  ([conn-mgr key opts swap-fn] (swap* :swap conn-mgr key nil opts swap-fn)))

(defn ^:public hswap
  "Atomically transforms `field` in the hash at `key` using optimistic retries.

  Arity forms:
    (hswap conn-mgr key field swap-fn)
    (hswap conn-mgr key field opts swap-fn)

  The callback and options are the same as for [[swap]]. `missing?` is true iff
  the field is absent. A successful `HSET` keeps the hash key TTL.
  `:swap/delete` conditionally removes only the observed field. `:swap/abort`
  and `:swap/delete` are reserved directives; [[hswap]] cannot store them. This
  function works with Cluster and does not join an enclosing [[wcar]] pipeline.

  Like [[swap]], [[hswap]] performs its own Redis calls through `conn-mgr`. With
  an ordinary pooled manager, calling it while already holding a connection
  from the same manager (e.g. inside [[wcar]]) can exhaust the pool and block.
  Call it outside [[wcar]], or use a separate manager."
  ([conn-mgr key field      swap-fn] (swap* :hswap conn-mgr key field nil  swap-fn))
  ([conn-mgr key field opts swap-fn] (swap* :hswap conn-mgr key field opts swap-fn)))
