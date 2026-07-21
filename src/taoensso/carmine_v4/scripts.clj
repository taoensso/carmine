(ns ^:no-doc taoensso.carmine-v4.scripts
  "Private implementation for the public Carmine v4 scripting and atomic APIs."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss :as truss]
   [clojure.string :as str]
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
  taoensso.carmine-v4/script-load
  taoensso.carmine-v4/set
  taoensso.carmine-v4/with-car)

(alias 'core 'taoensso.carmine-v4)

(def ^:private cached-script-hash
  (enc/cache {:size 256}
    (fn [script]
      (org.apache.commons.codec.digest.DigestUtils/sha1Hex ^String script))))

(defn- prepare-script+hash [script]
  (let [script (truss/have string? script)]
    [script (cached-script-hash script)]))

(defn ^:public script-hash
  "Returns the SHA1 hex string that Redis uses to identify the given Lua
  `script` string, as used by `evalsha`."
  [script]
  (get (prepare-script+hash script) 1))

(defn ^:public evalsha*
  "Like `evalsha`, but takes the script and computes its hash:
    (wcar mgr (evalsha* \"return redis.call('GET', KEYS[1])\" 1 \"my-key\"))

  Redis replies with a `NOSCRIPT` error if it hasn't already loaded the script,
  e.g. after a restart. Prefer `eval*`, which handles that automatically."
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
       :hint "Use `prepare-lua` + the generated eval/eval_ro command, or run the script outside the transaction."}))
  (let [[script script-hash] (prepare-script+hash script)
        natural-replies? (resp/ctx-natural-replies?)
        ;; Function parsers run after NOSCRIPT detection via local-echo. Keep
        ;; their effective read options and RF parsing.
        internal-parser (com/defer-fn-parser com/*parser*)
        run-immediate
        (fn [eval-fn script]
          (binding [com/*parser* internal-parser]
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
  "Runs the given Lua `script`, sending only its hash when Redis has already
  loaded it:
    (wcar mgr (eval* \"return redis.call('GET', KEYS[1])\" 1 \"my-key\"))

  `numkeys` is the number of leading `args` that are Redis keys. With Cluster,
  all keys must be in the same slot.

  Tries `EVALSHA` first, then `EVAL` if Redis replies `NOSCRIPT`. To do this
  it sends any queued requests and waits for the reply, so it isn't pipelined
  and can't be used in `transact!` effects (use `eval` there).

  See also `lua`, `eval-ro*`."
  [script numkeys & args]
  (eval-fallback* 'eval* core/evalsha core/eval script numkeys args))

(defn ^:public eval-ro*
  "Like `eval*`, but for scripts that only read. Uses `EVALSHA_RO` and
  `EVAL_RO` (Redis 7+), which reject scripts that try to write."
  [script numkeys & args]
  (eval-fallback* 'eval-ro* core/evalsha_ro core/eval_ro script numkeys args))

(def ^:private cached-prepare-lua
  (enc/cache {:size 256}
    (fn [script key-vars arg-vars]
      (let [replacements ; [["_:my-var" "KEYS[1]"] ...]
            (fn [vars array-name]
              (map-indexed
                (fn [idx var]
                  [(str "_:" (if (keyword? var) (subs (str var) 1) (str var)))
                   (str array-name "[" (inc idx) "]")])
                vars))]
        (reduce (fn [script [match replacement]] (str/replace script match replacement))
          script
          ;; Longest first so that "_:foo" can't replace part of "_:foo-bar"
          (sort-by #(- (count (first %)))
            (concat
              (replacements key-vars "KEYS")
              (replacements arg-vars "ARGV"))))))))

(defn ^:public prepare-lua
  "Returns Lua `script` string with each `_:name` replaced by the matching
  `KEYS[i]` or `ARGV[i]` reference. `key-vars` and `arg-vars` are ordered
  collections of names (keywords, symbols, or strings):

    (prepare-lua \"return redis.call('set', _:k, _:v)\" [:k] [:v])
    => \"return redis.call('set', KEYS[1], ARGV[1])\"

  Replacement is textual. Used by `lua`; call it directly to use the result
  with `eval`, `eval*`, etc."
  [script key-vars arg-vars]
  (cached-prepare-lua (truss/have string? script) (vec key-vars) (vec arg-vars)))

(defn- prep-vars [keys args]
  [(when (map? keys) (clojure.core/keys keys))
   (when (map? args) (clojure.core/keys args))
   (vec (if (map? keys) (vals keys) keys))
   (vec (if (map? args) (vals args) args))])

(defn- lua-call [eval-fn script keys args]
  (let [[key-vars arg-vars key-values arg-values] (prep-vars keys args)
        prepared-script (prepare-lua script key-vars arg-vars)]
    (apply eval-fn prepared-script (count key-values)
      (into key-values arg-values))))

(defn ^:public lua
  "Runs Lua `script` on Redis, with named keys and arguments:
    (wcar mgr
      (lua \"return redis.call('set', _:k, _:v)\" {:k \"my-key\"} {:v \"my-val\"}))

  `keys` and `args` are `{name value}` maps. Each `_:name` in the script is
  replaced (textually) with a reference to the matching value. Vectors of
  values may be given instead, for use as `KEYS[i]` and `ARGV[i]`.

  Redis keys must be given in `keys` rather than `args`, so that Cluster can
  route the request. Like `eval*`, sends its request immediately."
  [script keys args]
  (lua-call eval* script keys args))

(defn ^:public lua-ro
  "Like `lua`, but runs `script` as read-only (Redis 7+). Redis rejects
  writes from the script."
  [script keys args]
  (lua-call eval-ro* script keys args))

;;;; Locally tracked scripts

(def ^:private ^java.util.Map conn-loaded-scripts
  "{<conn-input-stream> #{<script-hash>}}, weakly keyed."
  (java.util.Collections/synchronizedMap (java.util.WeakHashMap.)))

(defn- eval-local* [script numkeys & args]
  (resp/ensure-context! 'lua-local)
  (when (resp/cluster-context?)
    (truss/ex-info! "[Carmine] `lua-local` is not supported with Cluster"
      {:eid :carmine.script/cluster-not-supported
       :hint "Use `lua` or `eval*`."}))
  (when com/*tx-effects?*
    ;; Loading needs a reply, which isn't possible inside MULTI. Always
    ;; throw so that calls can't work only while the script happens to be
    ;; loaded.
    (truss/ex-info!
      "[Carmine] `lua-local` is not supported inside `transact!` effects"
      {:eid :carmine.script/tx-effects-not-supported
       :hint "Use `prepare-lua` + the generated eval/eval_ro command, or run the script outside the transaction."}))
  (let [[script script-hash] (prepare-script+hash script)
        conn-in (resp/ctx-conn-in)
        ^java.util.Set loaded
        (.computeIfAbsent conn-loaded-scripts conn-in
          (reify java.util.function.Function
            (apply [_ _] (java.util.concurrent.ConcurrentHashMap/newKeySet))))

        load-error
        (when-not (.contains loaded script-hash)
          (let [reply
                (resp/with-replies
                  {:natural-replies? true, :as-vec? false, :error-mode :return} nil
                  #(core/script-load script))]
            (if (com/reply-error? reply)
              reply
              (do (.add loaded script-hash) nil))))]

    (if load-error
      (resp/local-echo load-error) ; In place of script's reply, as `lua`
      (apply core/evalsha script-hash numkeys args))))

(defn ^:public lua-local
  "Like `lua`, but pipelined once a script is loaded on a connection: it then
  queues a single `EVALSHA` request like an ordinary command, without first
  sending any queued requests:
    (wcar mgr
      (lua-local \"return redis.call('incrby', _:k, _:n)\" {:k \"k1\"} {:n 1})
      (lua-local \"return redis.call('incrby', _:k, _:n)\" {:k \"k2\"} {:n 2}))

  To do this it keeps track of the scripts that it has loaded on each
  connection. The first use of a script on a connection sends any queued
  requests and loads the script (`SCRIPT LOAD`); later uses don't. So it's
  most useful with a pooled manager.

  There's no `NOSCRIPT` fallback, so replies will be `NOSCRIPT` errors if the
  server's script cache is cleared while connected (e.g. with `SCRIPT FLUSH`,
  or by a proxy that switches servers without closing the connection). A
  server restart is fine, since it closes the connection.

  Not supported with Cluster, or in `transact!` effects.

  See also `lua`, `prepare-lua`."
  [script keys args]
  (lua-call eval-local* script keys args))

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
  "Atomically sets `key` to `replacement` iff its current value equals
  `expected`. Queues a reply of true iff the value was set:
    (wcar mgr (compare-and-set \"my-key\" \"old\" \"new\")) ; => true

  Never matches a missing key (use `set` with `NX` for that). Like `SET`,
  clears the key's TTL.

  The comparison is against the stored bytes, so `expected` must be written
  the same way as the stored value was. Queued and pipelined like any
  single-key command.

  See also `compare-and-delete`, `compare-and-hset`, `swap`."
  [key expected replacement]
  (compare-script 'compare-and-set compare-and-set-script
    key expected replacement))

(defn ^:public compare-and-delete
  "Atomically deletes `key` iff its current value equals `expected`. Queues a
  reply of true iff the key was deleted. Never matches a missing key.

  See `compare-and-set` for how values are compared."
  [key expected]
  (compare-script 'compare-and-delete compare-and-delete-script key expected))

(defn ^:public compare-and-hset
  "Atomically sets `field` in the hash at `key` to `replacement` iff the
  field's current value equals `expected`. Queues a reply of true iff the
  field was set. Never matches a missing field (use `hsetnx` for that).

  See `compare-and-set` for how values are compared."
  [key field expected replacement]
  (compare-script 'compare-and-hset compare-and-hset-script
    key field expected replacement))

(defn ^:public compare-and-hdel
  "Atomically deletes `field` from the hash at `key` iff the field's current
  value equals `expected`. Queues a reply of true iff the field was deleted.
  Never matches a missing field.

  See `compare-and-set` for how values are compared."
  [key field expected]
  (compare-script 'compare-and-hdel compare-and-hdel-script key field expected))

;;;; Atomic swaps

(def ^:private no-conflict-val (Object.))

(defn- parse-swap-opts [opts]
  (let [opts (or opts {})]
    (utils/have-opts! :carmine.swap/invalid-opts "`swap` options" #{:max-attempts :retry-backoff-ms :conflict-val} opts)
    (let [max-attempts     (get opts :max-attempts 8)
          retry-backoff-ms (get opts :retry-backoff-ms)]
      (utils/have-opt! :carmine.swap/invalid-opts "`swap` options" :max-attempts "a positive integer" pos-int? max-attempts)
      (utils/have-opt! :carmine.swap/invalid-opts "`swap` options" :retry-backoff-ms "nil, a non-negative integer, or a fn"
        (utils/nil-or (some-fn nat-int? fn?)) retry-backoff-ms)
      {:max-attempts max-attempts
       :retry-backoff-ms retry-backoff-ms
       :conflict-val (get opts :conflict-val no-conflict-val)})))

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

(defn- swap-mutated?
  "Returns true iff the mutation was applied, false iff it conflicted.
  Throws on any other reply: the write may have been applied, so a retry
  could repeat it."
  [conn-mgr operation applied conflicted body-fn]
  (let [reply (com/reply-content (internal-swap-call conn-mgr body-fn))]
    (cond
      (= reply applied)    true
      (= reply conflicted) false
      :else
      (truss/ex-info! "[Carmine] Unexpected Redis swap write reply"
        {:eid :carmine.swap/unexpected-write-reply
         :operation operation
         :reply (enc/typed-val reply)}))))

(defn- compare-swap-mutation!
  [conn-mgr operation key field exists? old-ba new-val]
  (let [old (when exists? (write/bytes old-ba))]
    (cond
      (identical? new-val :swap/abort) true

      (identical? new-val :swap/delete)
      (or
        (not exists?)
        (swap-mutated? conn-mgr operation 1 0
          #(case operation
             :swap  (core/eval compare-and-delete-script 1 key       old)
             :hswap (core/eval compare-and-hdel-script   1 key field old))))

      exists?
      (swap-mutated? conn-mgr operation 1 0
        #(case operation
           :swap  (core/eval compare-and-set-script  1 key       old new-val)
           :hswap (core/eval compare-and-hset-script 1 key field old new-val)))

      :else
      (case operation
        :swap  (swap-mutated? conn-mgr operation "OK" nil #(core/set    key       new-val "NX"))
        :hswap (swap-mutated? conn-mgr operation 1    0   #(core/hsetnx key field new-val))))))

(defn- swap* [operation conn-mgr key field opts swap-fn]
  (truss/have fn? swap-fn)
  (let [{:keys [max-attempts retry-backoff-ms conflict-val]} (parse-swap-opts opts)
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
            swap-result          (swap-fn old-val (not exists?))
            [new-val return-val] (enc/swapped-vec swap-result)
            return-val
            (cond
              (enc/swapped? swap-result)        return-val ; Explicit
              (identical? new-val :swap/abort)  old-val
              (identical? new-val :swap/delete) nil
              :else                             return-val)]
        (if (compare-swap-mutation!
              conn-mgr operation key field exists? old-ba new-val)
          return-val
          (if (< attempt max-attempts)
            (do
              (utils/backoff! retry-backoff-ms attempt)
              (recur (inc attempt)))
            (if (identical? conflict-val no-conflict-val)
              (truss/ex-info! "[Carmine] Optimistic swap conflicted"
                {:eid :carmine.swap/conflict
                 :operation operation
                 :attempts attempt})
              conflict-val)))))))

(defn ^:public swap
  "Atomically updates the value at `key` with `swap-fn`, and returns the new
  value:
    (swap mgr \"my-key\" (fn [old-val missing?] (conj (or old-val []) :x)))

  `(swap-fn old-val missing?)` gets the current value as `get` would return
  it, and returns the value to set. Note that numbers are read back as
  strings. `missing?` is true iff the key doesn't exist.

  `swap-fn` may also return:
    - `:swap/abort`  to change nothing (returns old value).
    - `:swap/delete` to delete the key (returns nil).
    - `(taoensso.encore/swapped new-val return-val)` to set one value and
      return another. `new-val` may be one of the keywords above.

  If the value at `key` changed while `swap-fn` was running, `swap` retries
  with the newer value. So `swap-fn` may be called several times, and
  shouldn't have side effects.

  Options:
    - `:max-attempts`     - Max tries (default 8).
    - `:retry-backoff-ms` - Wait between tries: nil, msecs, or
                            `(fn [attempt])` => msecs.
    - `:conflict-val`     - Value to return when every try conflicted.
                            If omitted, throws `:carmine.swap/conflict`.

  Like `SET`, clears the key's TTL. Call outside of `wcar`: `swap` makes
  its own requests with `conn-mgr`, and isn't pipelined.

  See also `hswap`, `compare-and-set`, `transact!`."
  ([conn-mgr key      swap-fn] (swap* :swap conn-mgr key nil nil  swap-fn))
  ([conn-mgr key opts swap-fn] (swap* :swap conn-mgr key nil opts swap-fn)))

(defn ^:public hswap
  "Like `swap`, but updates `field` in the hash at `key`:
    (hswap mgr \"my-hash\" \"my-field\" (fn [old-val missing?] ...))

  `missing?` is true iff the field doesn't exist, and `:swap/delete` deletes
  only the field. Keeps the key's TTL."
  ([conn-mgr key field      swap-fn] (swap* :hswap conn-mgr key field nil  swap-fn))
  ([conn-mgr key field opts swap-fn] (swap* :hswap conn-mgr key field opts swap-fn)))
