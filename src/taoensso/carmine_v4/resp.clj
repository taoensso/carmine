(ns ^:no-doc taoensso.carmine-v4.resp
  "Private ns, implementation detail.
  Implementation of the Redis RESP3 protocol,
  Ref. <https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md>"
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write])

  (:import [java.util LinkedList]))

(comment (remove-ns 'taoensso.carmine-v4.resp))

(enc/declare-remote
  ^:dynamic taoensso.carmine-v4.cluster/cluster-slot)

(alias 'cluster 'taoensso.carmine-v4.cluster)

;;;; Aliases

(enc/defaliases com/reply-error com/reply-error?)

;;;;

(let [read-opts-natural com/read-opts-natural
      ba-command        (enc/str->utf8-ba "*1\r\n$4\r\nPING\r\n")
      ba-len            (alength ba-command)]

  (defn basic-ping!
    "Low-level util.
    Sends a minimally expensive single PING command directly to Redis,
    and reads reply. Forgoes `Ctx`, read mode, parsing, etc."
    [in ^java.io.BufferedOutputStream out]
    (.write out ba-command 0 ba-len)
    (.flush out)
    (read/read-reply read-opts-natural in)))

;;;; Request context

(def ^:dynamic *ctx* nil)
(deftype Ctx [cluster? natural-replies? pending-reqs* pending-replies* conn-opts in out])

(deftype Req          [read-opts args cluster-slot supports-cluster?])
(deftype LocalEchoReq [read-opts reply])

;; TODO For command generation
;; - Create and .addLast relevant Req
;; - Set `supports-cluster?` (true => supported, false => unsupported, nil => unknown)
;; - Set `cluster-slot` based on detected cluster-key
;; - Use `throw-no-ctx!` if necessary

(defn- throw-no-ctx! [called]
  (truss/ex-info! "[Carmine] Called Redis command/s without `wcar` or `with-car` context."
    {:eid :carmine/no-context
     :called called}))

(defn- throw-cluster-not-supported! [command]
  (truss/ex-info! "[Carmine] Called Redis command in Redis Cluster context that does not support Cluster."
    {:eid :carmine/cluster-not-supported
     :command command}))

(let [read-opts-natural com/read-opts-natural]
  (defn- get-read-opts [^Ctx ctx]
    (if (.-natural-replies? ctx)
      read-opts-natural
      (com/get-read-opts))))

(let [get-read-opts get-read-opts
      cluster-slot cluster/cluster-slot]

  (defn ^:public rcmd*
    "Sends 1 arbitrary command to Redis server.
    Takes a vector of args for the command call:
      (wcar {} (rcmd* [:SET \"my-key\" \"my-val\"])) => \"OK\"

    Useful for DSLs, and to call commands (including Redis module commands)
    that might not yet have a native Clojure fn provided by Carmine."
    [cmd-args]
    (if-let [^Ctx ctx *ctx*]
      (let [cluster-slot (when (.-cluster? ctx) (enc/rsome cluster-slot cmd-args))]
        (.addLast ^LinkedList (.-pending-reqs* ctx)
          (Req. (get-read-opts ctx) cmd-args cluster-slot nil))
        nil)
      (throw-no-ctx! cmd-args)))

  (defn ^:public rcmds*
    "Send >=0 arbitrary commands to Redis server.
    Takes a vector of calls, with each call a vector of args:
      (wcar {}
        (rcmds* [[:SET \"my-key\" \"my-val\"]
                 [:GET \"my-key\"]])) => [\"OK\" \"my-val\"]

    Useful for DSLs, and to call commands (including Redis module commands)
    that might not yet have a native Clojure fn provided by Carmine."
    [cmds]
    (if-let [^Ctx ctx *ctx*]
      (let [^LinkedList pending-reqs (.-pending-reqs* ctx)
            read-opts                (get-read-opts   ctx)
            cluster?                 (.-cluster?      ctx)]
        (run!
          (fn [cmd-args]
            (let [cluster-slot (when cluster? (enc/rsome cluster-slot cmd-args))]
              (.addLast pending-reqs (Req. read-opts cmd-args cluster-slot nil))))
          cmds)
        nil)
      (throw-no-ctx! cmds))))

(let [rcmd* rcmd*]
  (defn ^:public rcmd
    "Sends 1 arbitrary command to Redis server.
    Takes varargs for the command call:
      (wcar {} (rcmd :SET \"my-key\" \"my-val\")) => \"OK\"

    Useful for DSLs, and to call commands (including Redis module commands)
    that might not yet have a native Clojure fn provided by Carmine."
    [& cmd-args] (rcmd* cmd-args)))

(let [rcmds* rcmds*]
  (defn ^:public rcmds
    "Send >=0 arbitrary commands to Redis server.
    Takes vararg calls, with each call a vector of args:
      (wcar {}
        (rcmds [:SET \"my-key\" \"my-val\"]
               [:GET \"my-key\"])) => [\"OK\" \"my-val\"]

    Useful for DSLs, and to call commands (including Redis module commands)
    that might not yet have a native Clojure fn provided by Carmine."
    [& cmds] (rcmds* cmds)))

(let [get-read-opts get-read-opts]
  (defn ^:public local-echo
    "Like the `echo` command except entirely local: no data is sent to/from Redis:
      (wcar {} (local-echo \"foo\")) => \"foo\"

    Useful for DSLs and other advanced applications. Can be combined with
    `with-replies` or nested `wcar` calls to achieve some very powerful effects."
    [x]
    (if-let [^Ctx ctx *ctx*]
      (do
        (.addLast ^LinkedList (.-pending-reqs* ctx)
          (LocalEchoReq. (get-read-opts ctx) x))
        nil)
      (throw-no-ctx! ["LOCAL-ECHO" x])))

  (defn ^:public local-echos*
    "Like `local-echo`, except takes a vector of >=0 args to echo."
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
    "Like `local-echo`, except takes >=0 varargs to echo."
    [& xs] (local-echos* xs)))

(do ; Basic commands for tests
  (defn ping []    (rcmd "PING"))
  (defn echo [x]   (rcmd "ECHO" x))
  (defn rset [k v] (rcmd "SET" k v))
  (defn rget [k]   (rcmd "GET" k)))

;;;; Non-cluster API

(declare
  ^:private flush-pending-reqs
  ^:private complete-replies)

(defn with-replies
  "Establishes (possibly-nested) `Ctx`, flushes requests in body,
  and returns completed replies."

  ;; Add non-cluster ctx, used by `with-car`
  ([in out natural-replies? as-vec? body-fn]
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-reqs parent-ctx))

   (let [new-ctx (Ctx. false natural-replies? (LinkedList.) (LinkedList.) nil in out)]
     (enc/binding* [*ctx* new-ctx] (body-fn))
     (flush-pending-reqs       new-ctx)
     (complete-replies as-vec? new-ctx)))

  ;; Add cluster ctx, used by `with-car`
  ([conn-opts natural-replies? as-vec? body-fn]
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-reqs parent-ctx))

   (let [new-ctx (Ctx. true natural-replies? (LinkedList.) (LinkedList.) conn-opts nil nil)]
     (enc/binding* [*ctx* new-ctx] (body-fn))
     (flush-pending-reqs       new-ctx)
     (complete-replies as-vec? new-ctx)))

  ;; Add additional ctx, used by public `with-replies`
  ([natural-replies? as-vec? body-fn]
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-reqs parent-ctx)

     (let [new-ctx
           (if (.-cluster? parent-ctx)
             (Ctx. true  natural-replies? (LinkedList.) (LinkedList.) (.-conn-opts parent-ctx) nil nil)
             (Ctx. false natural-replies? (LinkedList.) (LinkedList.) nil (.-in parent-ctx) (.-out parent-ctx)))]

       (enc/binding* [*ctx* new-ctx] (body-fn))
       (flush-pending-reqs       new-ctx)
       (complete-replies as-vec? new-ctx)))))

(declare ^:private consume-list!)

(let [sentinel-skipped-reply com/sentinel-skipped-reply]
  (defn flush-pending-reqs [^Ctx ctx]
    "Given a `Ctx` with pending-reqs* and pending-replies*:
      - Consumes (mutates) all pending-reqs*
      - Adds to  (mutates)     pending-replies*

    Returns the number of requests consumed (used only for
    debugging/testing)."
    (if (.-cluster? ctx)
      (let [conn-opts (.-conn-opts ctx)]

        ;; See cluster ns for sketch:
        ;; 1. Use partitioning util in cluster ns
        ;; 2. Acquire conns to all shard-addrs with
        ;;    (get-conn (assoc conn-opts :server <shard-addr>))
        ;; *. Comment that future-pool could be used here
        ;; 3. Write to all shards, starting with READONLY/READWRITE (skipping replies)
        ;; 4. Read from all shards
        ;; 5. Handle cluster errors, with possible retries
        ;; 6. Stitch back replies in correct order
        (truss/ex-info! "TODO: Cluster support not yet implemented" {}))

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
                      (let [args (.-args ^Req req)]
                        (write/write-array-len out (count args))
                        (enc/run! (fn [arg] (write/write-bulk-arg arg out)) args))

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
                            (let [read-opts (.-read-opts ^Req req)]
                              (read/read-reply read-opts in))

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
    [as-vec? ^Ctx ctx]

    (if (.-cluster? ctx)
      ;; TODO Any special handling needed here?
      (truss/ex-info! "TODO: Cluster support not yet implemented" {})

      (let [^LinkedList pending-replies* (.-pending-replies* ctx)
            n-replies (.size pending-replies*)]

        (enc/cond
          (== n-replies 1)
          (let [reply (.removeFirst pending-replies*)]
            (if as-vec?
              [reply]
              (if (reply-error? reply)
                (throw reply)
                (do    reply))))

          (> n-replies 10)
          (persistent!
            (consume-list! conj! (transient [])
              pending-replies* n-replies))

          (> n-replies 0)
          (consume-list! conj []
            pending-replies* n-replies))))))

;;;;

(defn parse-body-reply-opts
  "Returns [?reply-opts body]"
  [body]
  (let [[b1 & bn] body]
    (case b1
      (:as-vec :as-pipeline) [{:as-vec? true} bn]
      (cond
        (set? b1)
        (case b1
          #{}                         [nil                      bn]
          #{:as-vec                 } [{:as-vec?          true} bn]
          #{        :natural-replies} [{:natural-replies? true} bn]
          #{:as-vec :natural-replies} [{:as-vec?          true
                                        :natural-replies? true} bn]
          (truss/ex-info! "[Carmine] Unexpected reply-opts in body"
            {:opts (enc/typed-val b1)}))

        (map? b1) [b1    bn]
        :else     [nil body]))))
