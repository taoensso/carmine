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
  taoensso.carmine-v4.cluster/cluster-slot
  taoensso.carmine-v4.cluster/command-slot
  taoensso.carmine-v4.cluster/request-target
  taoensso.carmine-v4.cluster/execute-flush!)

(alias 'cluster 'taoensso.carmine-v4.cluster)

;;;; Aliases

(enc/defaliases com/reply-error com/reply-error?)

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
   push-fn])

(deftype Req
  [read-opts args cluster-slot cluster-target supports-cluster?
   encoded-prefix n-prefix-args])
(deftype LocalEchoReq [read-opts reply])

(declare ^:private throw-no-ctx!)

(defn ^:no-doc ensure-context!
  "Throws the canonical no-context error unless a request context is active."
  [called]
  (if *ctx* true (throw-no-ctx! called)))

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
  (defn- get-read-opts [^Ctx ctx]
    (if (.-natural-replies? ctx)
      read-opts-natural
      (com/get-read-opts))))

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

  (letfn [(enqueue-prepared!
            [cmd-args cluster-slot cluster-routing supports-cluster?
             encoded-prefix n-prefix-args]
            (if-let [^Ctx ctx *ctx*]
              (let [cmd-args (ensure-request-command! cmd-args)
                    cluster-target
                    (when (.-cluster? ctx)
                      (cluster/request-target cluster-slot cluster-routing))]
                (.addLast ^LinkedList (.-pending-reqs* ctx)
                  (Req. (get-read-opts ctx) cmd-args
                    (when (.-cluster? ctx) cluster-slot)
                    cluster-target supports-cluster? encoded-prefix n-prefix-args))
                nil)
              (throw-no-ctx! cmd-args)))]

    (defn ^:no-doc enqueue-command!
      "Internal enqueue primitive that prepares arbitrary command arguments."
      ([cmd-args cluster-slot cluster-routing supports-cluster?]
       (enqueue-command! cmd-args cluster-slot cluster-routing supports-cluster? nil 0))
      ([cmd-args cluster-slot cluster-routing supports-cluster? encoded-prefix n-prefix-args]
       (enqueue-prepared! (write/prepare-command cmd-args)
         cluster-slot cluster-routing supports-cluster? encoded-prefix n-prefix-args)))

    (defn ^:no-doc enqueue-prepared-command!
      "Internal enqueue primitive for already-prepared generated commands."
      [cmd-args cluster-slot cluster-routing supports-cluster? encoded-prefix n-prefix-args]
      (enqueue-prepared! cmd-args cluster-slot cluster-routing supports-cluster?
        encoded-prefix n-prefix-args)))

  (defn ^:public rcmd*
    "Queues one arbitrary Redis command from an argument vector:
      (wcar mgr (rcmd* [:SET \"my-key\" \"my-val\"])) => \"OK\"

    Use for DSLs and commands, including module commands, that do not have a
    native Carmine function."
    [cmd-args]
    (if-let [^Ctx ctx *ctx*]
      (let [cmd-args    (ensure-request-command!
                          (write/prepare-command (or cmd-args [])))
            cluster-slot (when (.-cluster? ctx) (cluster/command-slot cmd-args nil))
            cluster-target
            (when (.-cluster? ctx)
              (cluster/request-target cluster-slot :raw))]
        (.addLast ^LinkedList (.-pending-reqs* ctx)
          (Req. (get-read-opts ctx) cmd-args cluster-slot cluster-target nil nil 0))
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
            (mapv
              (fn [cmd-args]
                (let [cmd-args (ensure-request-command!
                                 (write/prepare-command (or cmd-args [])))
                      cluster-slot
                      (when cluster? (cluster/command-slot cmd-args nil))
                      cluster-target
                      (when cluster?
                        (cluster/request-target cluster-slot :raw))]
                  (Req. read-opts cmd-args cluster-slot cluster-target nil nil 0)))
              cmds)]
        (run! #(.addLast pending-reqs %) reqs)
        nil)
      (throw-no-ctx! cmds))))

(let [rcmd* rcmd*]
  (defn ^:public rcmd
    "Queues one arbitrary Redis command from variadic arguments:
      (wcar mgr (rcmd :SET \"my-key\" \"my-val\")) => \"OK\"

    Use for DSLs and commands, including module commands, that do not have a
    native Carmine function."
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
    "Queues a local value as a reply. Does not send data to Redis:
      (wcar mgr (local-echo \"foo\")) => \"foo\"

    Use with DSLs, [[with-replies]], or nested [[wcar]] calls."
    [x]
    (if-let [^Ctx ctx *ctx*]
      (do
        (.addLast ^LinkedList (.-pending-reqs* ctx)
          (LocalEchoReq. (get-read-opts ctx) x))
        nil)
      (throw-no-ctx! ["LOCAL-ECHO" x])))

  (defn ^:public local-echos*
    "Like [[local-echo]], but accepts a vector of zero or more values."
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
    "Like [[local-echo]], but accepts zero or more variadic values."
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

(defn- internal-reply-opts [reply-opts-or-natural? as-vec?]
  (if (map? reply-opts-or-natural?)
    (parse-reply-opts reply-opts-or-natural?)
    {:natural-replies? reply-opts-or-natural?
     :as-vec? as-vec?
     :error-mode :throw}))

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
         new-ctx (Ctx. false natural-replies? (LinkedList.) (LinkedList.) nil in out push-fn)]
     (enc/binding* [*ctx* new-ctx] (body-fn))
     (flush-pending-reqs       new-ctx)
     (complete-replies (:as-vec? reply-opts) error-mode new-ctx)))

  ;; Add cluster ctx, used by `with-car`
  ([cluster-state reply-opts-or-natural? as-vec? body-fn]
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-reqs parent-ctx))

   (let [{:keys [natural-replies? error-mode] :as reply-opts}
         (internal-reply-opts reply-opts-or-natural? as-vec?)
         new-ctx (Ctx. true natural-replies? (LinkedList.) (LinkedList.) cluster-state nil nil nil)]
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
               (Ctx. true  natural-replies? (LinkedList.) (LinkedList.) (.-cluster-state parent-ctx) nil nil nil)
               (Ctx. false natural-replies? (LinkedList.) (LinkedList.) nil
                 (.-in parent-ctx) (.-out parent-ctx) (.-push-fn parent-ctx)))]

         (enc/binding* [*ctx* new-ctx] (body-fn))
         (flush-pending-reqs       new-ctx)
         (complete-replies (:as-vec? reply-opts) error-mode new-ctx)))
     (throw-no-ctx! 'with-replies))))

(declare ^:private consume-list!)

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
                  ^LinkedList consumed-reqs*   (LinkedList.)]

              ;; Consume all pending requests, writing to Redis server
              ;; without awaiting any replies (=> use pipelining).
              (let [out (.-out ctx)]
                (consume-list!
                  (fn [_ req]
                    (.add consumed-reqs* req) ; Move to consumed list
                    (enc/cond!
                      (instance? Req req) ; Common case
                      (let [^Req req req]
                        (write/write-command out (.-args req)
                          (.-encoded-prefix req) (.-n-prefix-args req)))

                      ;; Noop, don't actually send anything to Redis
                      (instance? LocalEchoReq req) nil))
                  nil pending-reqs* n-pending-reqs)
                (.flush ^java.io.BufferedOutputStream out))

              ;; Now re-consume all requests to read replies from Redis server
              (let [in (.-in ctx)]
                (consume-list!
                  (fn [_ req]
                    (let [completed-reply
                          (enc/cond!
                            (instance? Req req) ; Common case
                            (let [read-opts
                                  (com/with-push-fn
                                    (.-read-opts ^Req req) (.-push-fn ctx))]
                              (let [reply (read/read-reply read-opts in)]
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
                  nil consumed-reqs* n-pending-reqs))

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
  (let [opts (or opts {})
        allowed-keys #{:as-vec? :natural-replies? :error-mode}
        unknown-keys (not-empty (set (remove allowed-keys (keys opts))))
        parsed
        (merge
          {:as-vec? false, :natural-replies? false, :error-mode :throw}
          opts)]
    (when unknown-keys
      (truss/ex-info! "[Carmine] Unexpected reply option keys"
        {:eid :carmine/invalid-reply-opts
         :unknown-keys unknown-keys
         :opts (enc/typed-val opts)}))
    (when-not (boolean? (:as-vec? parsed))
      (truss/ex-info! "[Carmine] Expected boolean `:as-vec?` reply option"
        {:eid :carmine/invalid-reply-opts
         :option :as-vec?, :value (enc/typed-val (:as-vec? parsed))}))
    (when-not (boolean? (:natural-replies? parsed))
      (truss/ex-info! "[Carmine] Expected boolean `:natural-replies?` reply option"
        {:eid :carmine/invalid-reply-opts
         :option :natural-replies?, :value (enc/typed-val (:natural-replies? parsed))}))
    (when-not (#{:throw :return} (:error-mode parsed))
      (truss/ex-info! "[Carmine] Expected `:throw` or `:return` reply error mode"
        {:eid :carmine/invalid-reply-opts
         :option :error-mode, :value (enc/typed-val (:error-mode parsed))}))
    parsed))

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
