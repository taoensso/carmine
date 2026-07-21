(ns ^:no-doc taoensso.carmine-v4.resp
  "Private RESP3 implementation. See
  <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md>."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write])

  (:import [java.util LinkedList]))

(comment (remove-ns 'taoensso.carmine-v4.resp))

(enc/declare-remote
  taoensso.carmine-v4.cluster/command-slot
  taoensso.carmine-v4.cluster/request-target
  taoensso.carmine-v4.cluster/execute-flush!)

(alias 'cluster 'taoensso.carmine-v4.cluster)

;;;; Aliases

(enc/defaliases com/reply-error?)

;;;;

(let [read-opts-natural com/read-opts-natural
      ba-command        (enc/str->utf8-ba "*1\r\n$4\r\nPING\r\n")
      ba-len            (alength ba-command)]

  (defn basic-ping!
    "Sends one minimal PING directly to Redis and reads the reply. Does not use
    `Ctx`, read modes, or parsing. Internal."
    ([in out] (basic-ping! in out nil))
    ([in ^java.io.BufferedOutputStream out push-fn]
     (.write out ba-command 0 ba-len)
     (.flush out)
     (read/read-reply (com/with-push-fn read-opts-natural push-fn) in))))

;;;; Request context

(def ^:dynamic *ctx* nil)
(deftype Ctx
  [cluster? natural-replies? pending-reqs* pending-replies* cluster-state in out
   push-fn read-opts-cache])

(deftype CachedReadOpts [frame read-opts])
(defn- new-read-opts-cache ^objects [] (object-array 1))

(deftype Req
  [read-opts args cluster-slot cluster-target supports-cluster?
   encoded-prefix n-prefix-args])
(deftype LocalEchoReq [read-opts reply])

(declare ^:private throw-no-ctx!)

(defn ^:no-doc ensure-context!
  "Returns the active request context (truthy), or throws the canonical
  no-context error."
  [called]
  (or *ctx* (throw-no-ctx! called)))

(defn ^:no-doc ctx-cluster?
  "Returns true iff given request context is a Cluster context."
  [^Ctx ctx] (.-cluster? ctx))

(defn ^:no-doc ctx-pending-request-count
  "Returns the number of requests pending in the current request context."
  []
  (if-let [^Ctx ctx *ctx*]
    (.size ^LinkedList (.-pending-reqs* ctx))
    (throw-no-ctx! 'ctx-pending-request-count)))

(defn ^:no-doc ctx-natural-replies?
  "Returns true iff the current request uses natural replies."
  []
  (if-let [^Ctx ctx *ctx*]
    (boolean (or (.-natural-replies? ctx) com/*natural-replies?*))
    (throw-no-ctx! 'ctx-natural-replies?)))

(defn ^:no-doc ctx-conn-in
  "Returns the current request context's connection input stream (stable
  for the life of the connection), or nil in a Cluster context."
  []
  (if-let [^Ctx ctx *ctx*]
    (.-in ctx)
    (throw-no-ctx! 'ctx-conn-in)))

(defn ^:no-doc ctx-pending-reply-count
  "Returns the number of replies pending in the current request context."
  []
  (if-let [^Ctx ctx *ctx*]
    (.size ^LinkedList (.-pending-replies* ctx))
    (throw-no-ctx! 'ctx-pending-reply-count)))

(defn- throw-no-ctx! [called]
  (truss/ex-info! "[Carmine] Called Redis command/s without `wcar` or `with-car` context."
    {:eid :carmine/no-context
     :called called}))

(let [read-opts-natural com/read-opts-natural]
  (defn- get-read-opts
    "Returns the `ReadOpts` for the next request in given context.

    The opts depend on ~8 dynamic vars, which are relatively expensive to
    look up per command. So we cache the last opts against the identity of
    the thread binding frame they were derived from: any `binding` (or exit
    from one) changes the frame, and so invalidates the cache.

    This helps runs of commands queued under the same bindings (e.g. a
    typical pipeline). Commands that are each wrapped in their own binding
    (e.g. `(as-long (get k1)) (as-long (get k2))`) miss the cache every time.

    The cache lives only as long as its request context, and doesn't see var
    mutations that keep the same frame (`set!`, `alter-var-root`) made
    between commands in that context."
    [^Ctx ctx]
    (if (.-natural-replies? ctx)
      read-opts-natural
      (let [^objects cache (.-read-opts-cache ctx)
            frame (clojure.lang.Var/getThreadBindingFrame)
            ^CachedReadOpts cached (aget cache 0)]
        (if (and cached (identical? frame (.-frame cached)))
          (.-read-opts cached)
          (let [read-opts (com/get-read-opts)]
            ;; Single write of an immutable pair, so readers never see a torn entry
            (aset cache 0 (CachedReadOpts. frame read-opts))
            read-opts))))))

(let [get-read-opts get-read-opts]

  (defn- ensure-request-command! [cmd-args]
    (when-let [problem (write/request-incompatible-command cmd-args)]
      (truss/ex-info! "[Carmine] Redis command is incompatible with a request/reply context"
        (merge
          {:eid :carmine/invalid-request-command
           :context :request-reply}
          problem)))
    cmd-args)

  (defn ^:no-doc cluster-context?
    "Returns true iff called in a Cluster request context."
    []
    (boolean (when-let [^Ctx ctx *ctx*] (.-cluster? ctx))))

  (defn ^:no-doc enqueue-prepared-command!
    "Internal enqueue primitive for already-prepared generated commands.
    Takes the active request context, see `ensure-context!`."
    [^Ctx ctx cmd-args cluster-slot cluster-routing supports-cluster?
     encoded-prefix n-prefix-args]
    (let [cmd-args (ensure-request-command! cmd-args)
          cluster-target
          (when (.-cluster? ctx)
            (cluster/request-target cluster-slot cluster-routing))]
      (.addLast ^LinkedList (.-pending-reqs* ctx)
        (Req. (get-read-opts ctx) cmd-args
          (when (.-cluster? ctx) cluster-slot)
          cluster-target supports-cluster? encoded-prefix n-prefix-args))
      nil))

  (defn- raw-req
    "Returns a prepared and routed `Req` for a raw command."
    ^Req [cluster? read-opts cmd-args]
    (let [cmd-args (ensure-request-command!
                     (write/prepare-command (or cmd-args [])))]
      (if-not cluster?
        (Req. read-opts cmd-args nil nil nil nil 0)
        (if (write/cluster-incompatible-command? cmd-args)
          (Req. read-opts cmd-args nil nil false nil 0) ; Rejected before dispatch
          (let [cluster-slot (cluster/command-slot cmd-args nil)]
            (Req. read-opts cmd-args cluster-slot
              (cluster/request-target cluster-slot :raw) nil nil 0))))))

  (defn ^:public rcmd*
    "Like `rcmd`, but takes the command as a vector:
      (wcar mgr (rcmd* [:SET \"my-key\" \"my-val\"])) ; => \"OK\""
    [cmd-args]
    (if-let [^Ctx ctx *ctx*]
      (do
        (.addLast ^LinkedList (.-pending-reqs* ctx)
          (raw-req (.-cluster? ctx) (get-read-opts ctx) cmd-args))
        nil)
      (throw-no-ctx! cmd-args)))

  (defn ^:public rcmds*
    "Queues zero or more arbitrary Redis commands from a vector of argument vectors:
      (wcar mgr
        (rcmds* [[:SET \"my-key\" \"my-val\"]
                 [:GET \"my-key\"]])) => [\"OK\" \"my-val\"]

    Use for DSLs and commands, including module commands, that do not have a
    native Carmine function."
    [cmds]
    (if-let [^Ctx ctx *ctx*]
      (let [^LinkedList pending-reqs (.-pending-reqs* ctx)
            read-opts                (get-read-opts   ctx)
            cluster?                 (.-cluster?      ctx)
            ;; Fully prepare and route the batch before mutating the request
            ;; queue, so any local failure leaves the context unchanged.
            reqs
            (mapv #(raw-req cluster? read-opts %) cmds)]
        (run! #(.addLast pending-reqs %) reqs)
        nil)
      (throw-no-ctx! cmds))))

(let [rcmd* rcmd*]
  (defn ^:public rcmd
    "Queues a Redis command given as arguments. Useful for commands that don't
    have a Carmine function, e.g. module commands:
      (wcar mgr (rcmd :SET \"my-key\" \"my-val\")) ; => \"OK\"

    See also `rcmd*`."
    [& cmd-args] (rcmd* cmd-args)))

(let [rcmds* rcmds*]
  (defn ^:public rcmds
    "Queues zero or more arbitrary Redis commands from argument vectors:
      (wcar mgr
        (rcmds [:SET \"my-key\" \"my-val\"]
               [:GET \"my-key\"])) => [\"OK\" \"my-val\"]

    Use for DSLs and commands, including module commands, that do not have a
    native Carmine function."
    [& cmds] (rcmds* cmds)))

(let [get-read-opts get-read-opts]
  (defn ^:public local-echo
    "Queues `x` as a reply, without sending anything to Redis:
      (wcar mgr (ping) (local-echo \"foo\")) ; => [\"PONG\" \"foo\"]

    Useful for adding your own values to the replies of a pipeline."
    [x]
    (if-let [^Ctx ctx *ctx*]
      (do
        (.addLast ^LinkedList (.-pending-reqs* ctx)
          (LocalEchoReq. (get-read-opts ctx) x))
        nil)
      (throw-no-ctx! ["LOCAL-ECHO" x])))

  (defn ^:public local-echos*
    "Like `local-echo`, but accepts a vector of zero or more values."
    [xs]
    (if-let [^Ctx ctx *ctx*]
      (let [^LinkedList pending-reqs (.-pending-reqs* ctx)
            read-opts                (get-read-opts   ctx)]
        (run!
          (fn [x] (.addLast pending-reqs (LocalEchoReq. read-opts x)))
          xs)
        nil)
      (throw-no-ctx! (into ["LOCAL-ECHOS"] xs))))

  (defn ^:public local-echos
    "Like `local-echo`, but accepts zero or more variadic values."
    [& xs] (local-echos* xs)))

(do ; Basic commands for tests
  (defn ping []    (rcmd "PING"))
  (defn echo [x]   (rcmd "ECHO" x))
  (defn rset [k v] (rcmd "SET" k v))
  (defn rget [k]   (rcmd "GET" k)))

;;;; Non-cluster API

(declare
  parse-reply-opts
  ^:private flush-pending-reqs
  ^:private complete-replies)

(def ^:private default-reply-opts
  {:as-vec? false, :error-mode :throw})

(defn- internal-reply-opts [reply-opts-or-natural? as-vec?]
  (if (map? reply-opts-or-natural?)
    ;; Map input is the canonical internal form. Public entry points validate
    ;; exactly once before passing it here; internal callers use literals.
    reply-opts-or-natural?
    (if (or reply-opts-or-natural? as-vec?)
      {:natural-replies? reply-opts-or-natural?
       :as-vec? as-vec?
       :error-mode :throw}
      default-reply-opts)))

(defn with-replies
  "Creates a possibly nested `Ctx`, flushes the requests in `body`, and returns
  their replies."

  ;; Add non-cluster ctx, used by `with-car`
  ([in out reply-opts-or-natural? as-vec? body-fn]
   (with-replies in out nil reply-opts-or-natural? as-vec? body-fn))

  ;; Add manager-scoped RESP3 push dispatch.
  ([in out push-fn reply-opts-or-natural? as-vec? body-fn]
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-reqs parent-ctx))

   (let [{:keys [natural-replies? error-mode] :as reply-opts}
         (internal-reply-opts reply-opts-or-natural? as-vec?)
         new-ctx (Ctx. false natural-replies? (LinkedList.) (LinkedList.) nil in out push-fn (new-read-opts-cache))]
     (enc/binding* [*ctx* new-ctx] (body-fn))
     (flush-pending-reqs       new-ctx)
     (complete-replies (:as-vec? reply-opts) error-mode new-ctx)))

  ;; Add cluster ctx, used by `with-car`
  ([cluster-state reply-opts-or-natural? as-vec? body-fn]
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-reqs parent-ctx))

   (let [{:keys [natural-replies? error-mode] :as reply-opts}
         (internal-reply-opts reply-opts-or-natural? as-vec?)
         new-ctx (Ctx. true natural-replies? (LinkedList.) (LinkedList.) cluster-state nil nil nil (new-read-opts-cache))]
     (enc/binding* [*ctx* new-ctx] (body-fn))
     (flush-pending-reqs       new-ctx)
     (complete-replies (:as-vec? reply-opts) error-mode new-ctx)))

  ;; Add additional ctx, used by public `with-replies`
  ([reply-opts-or-natural? as-vec? body-fn]
   (if-let [^Ctx parent-ctx *ctx*]
     (do
       (flush-pending-reqs parent-ctx)

       (let [{:keys [natural-replies? error-mode] :as reply-opts}
             (internal-reply-opts reply-opts-or-natural? as-vec?)
             new-ctx
             (if (.-cluster? parent-ctx)
               (Ctx. true  natural-replies? (LinkedList.) (LinkedList.) (.-cluster-state parent-ctx) nil nil nil (new-read-opts-cache))
               (Ctx. false natural-replies? (LinkedList.) (LinkedList.) nil
                 (.-in parent-ctx) (.-out parent-ctx) (.-push-fn parent-ctx)
                 (new-read-opts-cache)))]

         (enc/binding* [*ctx* new-ctx] (body-fn))
         (flush-pending-reqs       new-ctx)
         (complete-replies (:as-vec? reply-opts) error-mode new-ctx)))
     (throw-no-ctx! 'with-replies))))

(let [sentinel-skipped-reply com/sentinel-skipped-reply]
  (defn flush-pending-reqs
    "Given a `Ctx` with `pending-reqs*` and `pending-replies*`, updates it as
    follows:
      - Consumes (mutates) all pending-reqs*
      - Adds to  (mutates)     pending-replies*

    Returns the consumed request count, or nil if there were no pending
    requests. The return value is only for debugging and tests."
    [^Ctx ctx]
    (if (.-cluster? ctx)
      (cluster/execute-flush!
        (.-cluster-state ctx)
        (.-pending-reqs* ctx)
        (.-pending-replies* ctx))

        (let [^LinkedList pending-reqs* (.-pending-reqs* ctx)
              n-pending-reqs (.size pending-reqs*)]

          (when (> n-pending-reqs 0)
            (let [^LinkedList pending-replies* (.-pending-replies* ctx)
                  consumed-reqs (object-array n-pending-reqs)
                  input-state (com/input-state (.-in ctx))] ; Once per pipeline

              (com/begin-flush! input-state)
              (try
                ;; Consume all pending requests, writing to Redis server
                ;; without awaiting any replies (=> use pipelining).
                (let [out (.-out ctx)
                      conn-reusable?_ write/*conn-reusable?_] ; Once per pipeline
                  (dotimes [i n-pending-reqs]
                    (let [req (.removeFirst pending-reqs*)]
                      (aset consumed-reqs i req) ; Move to consumed array
                      (enc/cond!
                        (instance? Req req) ; Common case
                        (let [^Req req req]
                          (write/write-command out (.-args req)
                            (.-encoded-prefix req) (.-n-prefix-args req)
                            conn-reusable?_))

                        ;; Noop, don't actually send anything to Redis
                        (instance? LocalEchoReq req) nil)))
                  (.flush ^java.io.BufferedOutputStream out))

                ;; Now re-consume all requests to read replies from Redis server
                (dotimes [i n-pending-reqs]
                  (let [req (aget consumed-reqs i)
                        _   (aset consumed-reqs i nil) ; Release for GC
                        completed-reply
                          (enc/cond!
                            (instance? Req req) ; Common case
                            (let [read-opts
                                  (com/with-push-fn
                                    (.-read-opts ^Req req) (.-push-fn ctx))]
                              (let [reply (read/read-reply-from-state read-opts input-state)]
                                (if (com/reply-error?
                                      {:eid :carmine.read/unexpected-read-error} reply)
                                  ;; Redis error frames and parser/thaw errors are
                                  ;; safe reply values after their frame is read.
                                  ;; Protocol/framing errors are not: the input
                                  ;; position is unknown and the connection must
                                  ;; be invalidated immediately.
                                  (throw reply)
                                  reply)))

                            (instance? LocalEchoReq req)
                            (let [read-opts (.-read-opts ^LocalEchoReq req)
                                  reply     (.-reply     ^LocalEchoReq req)]
                              (read/complete-reply read-opts reply)))]

                    (if (identical? completed-reply sentinel-skipped-reply)
                      nil ; Noop
                      (.add pending-replies* completed-reply))))

                (com/end-flush! input-state true)
                (catch Throwable t
                  ;; The conn may now hold partial requests or unread replies.
                  ;; The error may be caught (e.g. in a `wcar` body), so it's
                  ;; the conn that must remember.
                  (com/end-flush! input-state false)
                  (throw t)))

              n-pending-reqs))))))

(defn- consume-list!
  ;; Note: we don't actually always NEED to consume (remove) items
  ;; while iterating, but benching shows that doing so is almost
  ;; as fast as non-consuming iteration - so we'll just always
  ;; consume to keep things simple and safe.
  ([f init ^LinkedList ll  ] (consume-list! f init ll (.size ll)))
  ([f init ^LinkedList ll n]
   (when (> ^int n 0)
     (enc/reduce-n (fn [acc _] (f acc (.removeFirst ll))) init n))))

(comment
  (defn- ll ^LinkedList [n] (let [ll (LinkedList.)] (dotimes [n n] (.add ll n)) ll))
  (ll 10)

  (defn bench1 [n]
    (enc/qb 1e5
      (doseq [x (ll n)])
      (let [l1 (ll n)] (enc/run! (fn [x]) l1))
      (let [l1 (ll n)
            l2 (LinkedList.)]
        (enc/reduce-n (fn [_ _] (.add l2 (.removeFirst l1))) nil (.size l1)))))

  (mapv bench1 [1 10 100])
  [[ 50.29  13.89  17.9]
   [114.58  34.58  35.6]
   [836.49 221.18 205.2]])

(let [reply-error? com/reply-error?]

  (defn- complete-replies
    [as-vec? error-mode ^Ctx ctx]

    (let [^LinkedList pending-replies* (.-pending-replies* ctx)
          n-replies (.size pending-replies*)]

      (enc/cond
        ;; Deliberate contract: zero replies => nil, even with `:as-vec?`
        ;; (`:as-vec?` yields nil or a NON-EMPTY vector: nil composes better
        ;; than [], e.g. `(when-let [[a b] replies] ...)`)
        (== n-replies 0) nil

        (== n-replies 1)
        (let [reply (.removeFirst pending-replies*)]
          (when (and (= error-mode :throw) (reply-error? reply))
            (throw (com/drained-reply-errors [reply] [0])))
          (if as-vec? [reply] reply))

        (> n-replies 1)
        (let [replies
              (if (> n-replies 10)
                (persistent!
                  (consume-list! conj! (transient [])
                    pending-replies* n-replies))
                (consume-list! conj []
                  pending-replies* n-replies))]

          (when (= error-mode :throw)
            (let [error-indexes
                  (persistent!
                    (reduce-kv
                      (fn [acc idx reply]
                        (if (reply-error? reply) (conj! acc idx) acc))
                      (transient []) replies))]
              (when (seq error-indexes)
                (throw (com/drained-reply-errors replies error-indexes)))))

          replies)))))

;;;;

(defn parse-reply-opts
  "Returns strict, canonical public reply options."
  [opts]
  (when-not (or (nil? opts) (map? opts))
    (truss/ex-info! "[Carmine] Expected reply options map"
      {:eid :carmine/invalid-reply-opts
       :opts (enc/typed-val opts)}))
  (if (empty? opts)
    default-reply-opts
    (let [allowed-keys #{:as-vec? :error-mode}
          unknown-keys (not-empty (set (remove allowed-keys (keys opts))))
          parsed (merge default-reply-opts opts)]
      (when unknown-keys
        (truss/ex-info! "[Carmine] Unexpected reply option keys"
          {:eid :carmine/invalid-reply-opts
           :unknown-keys unknown-keys
           :opts (enc/typed-val opts)}))
      (when-not (boolean? (:as-vec? parsed))
        (truss/ex-info! "[Carmine] Expected boolean `:as-vec?` reply option"
          {:eid :carmine/invalid-reply-opts
           :opt-key :as-vec?, :opt-val (enc/typed-val (:as-vec? parsed))}))
      (when-not (#{:throw :return} (:error-mode parsed))
        (truss/ex-info! "[Carmine] Expected `:throw` or `:return` reply error mode"
          {:eid :carmine/invalid-reply-opts
           :opt-key :error-mode, :opt-val (enc/typed-val (:error-mode parsed))}))
      parsed)))

(defn parse-body-reply-opts
  "Returns `[canonical-reply-opts body]`."
  [body]
  (let [[b1 & bn] body]
    (case b1
      :as-vec      [(parse-reply-opts {:as-vec? true}) bn]
      :as-pipeline [(parse-reply-opts {:as-vec? true}) bn] ; Undocumented alias, for back compatibility with v3
      (cond
        (keyword? b1)
        (truss/ex-info! "[Carmine] Unexpected leading reply option keyword"
          {:eid :carmine/invalid-reply-opts, :opts b1
           :expected :as-vec})

        (set? b1)
        (truss/ex-info! "[Carmine] Set reply options are not supported; use an options map"
          {:eid :carmine/invalid-reply-opts, :opts (enc/typed-val b1)
           :expected :map-or-as-vec})

        ;; Map values are ordinary runtime expressions. Only identify the macro
        ;; grammar here; strict value validation happens after evaluation.
        (map? b1) [b1 bn]
        :else
        (do
          ;; Compile-time nudge for a likely mistake: a bare `opts`-style
          ;; symbol is a body form (value discarded), NOT reply options.
          (when (and (symbol? b1)
                  (let [n (name b1)]
                    (or (= n "opts") (= n "reply-opts") (enc/str-ends-with? n "-opts"))))
            (binding [*out* *err*]
              (println
                (str
                  "[Carmine] Warning: leading `" b1 "` symbol will be treated as an "
                  "ordinary body form (value discarded), NOT as reply options. "
                  "Reply options must be a literal map (or `:as-vec`); "
                  "use `with-car` for runtime reply options."))))
          [nil body])))))
