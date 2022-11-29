(ns taoensso.carmine-v4.resp
  "Private ns, implementation detail.
  Implementation of the Redis RESP3 protocol,
  Ref. https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md"
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]

   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.read   :as read]
   [taoensso.carmine-v4.resp.write  :as write])

  (:import [java.util LinkedList]))

(comment
  (remove-ns      'taoensso.carmine-v4.resp)
  (test/run-tests 'taoensso.carmine-v4.resp))

;;;; Aliases

(enc/defalias com/reply-error)
(enc/defalias com/reply-error?)

;;;;

(def ^:dynamic *ctx* "?<Ctx> context for requests and replies" nil)
(deftype Ctx [#_conn in out natural-reads? pending-reqs* pending-replies*])

(deftype Request          [read-opts args])
(deftype LocalEchoRequest [read-opts reply])

(let [read-opts-natural com/read-opts-natural]
  (defn- get-read-opts [^Ctx ctx]
    (if (.-natural-reads? ctx)
      read-opts-natural
      (com/get-read-opts))))

(defn throw-no-ctx-error! [called]
  (throw
    (ex-info "[Carmine] Carmine command/s called without expected `wcar` or `with-car` context."
      {:eid :carmine.conns/no-context
       :called called})))

(let [get-read-opts get-read-opts]

  ;; TODO These could alternatively write immediately (i.e. forgo pending-reqs*).
  ;; Awaiting Sentinel & Cluster to decide.

  (defn ^:public redis-call*
    "Sends 1 arbitrary command to Redis server.
    Takes a vector of args for the command call:
      (wcar {} (redis-call* [\"set\" \"my-key\" \"my-val\"])) => \"OK\"

    Useful for DSLs, and to call commands (including Redis module commands)
    that might not yet have a native Clojure fn provided by Carmine."
    [call-args]
    (if-let [^Ctx ctx *ctx*]
      (do
        (.addLast ^LinkedList (.-pending-reqs* ctx)
          (Request. (get-read-opts ctx) call-args))
        nil)
      (throw-no-ctx-error! call-args)))

  (defn ^:public redis-calls*
    "Send >=0 arbitrary commands to Redis server.
    Takes a vector of calls, with each call a vector of args:
      (wcar {}
        (redis-calls* [[\"set\" \"my-key\" \"my-val\"]
                       [\"get\" \"my-key\"]])) => [\"OK\" \"my-val\"]

    Useful for DSLs, and to call commands (including Redis module commands)
    that might not yet have a native Clojure fn provided by Carmine."
    [calls]
    (if-let [^Ctx ctx *ctx*]
      (let [^LinkedList pending-reqs (.-pending-reqs* ctx)
            read-opts                (get-read-opts   ctx)]
        (run!
          (fn [call-args] (.addLast pending-reqs (Request. read-opts call-args)))
          calls)
        nil)
      (throw-no-ctx-error! calls))))

(let [redis-call* redis-call*]
  (defn ^:public redis-call
    "Sends 1 arbitrary command to Redis server.
    Takes varargs for the command call:
      (wcar {} (redis-call \"set\" \"my-key\" \"my-val\")) => \"OK\"

    Useful for DSLs, and to call commands (including Redis module commands)
    that might not yet have a native Clojure fn provided by Carmine."
    [& call-args] (redis-call* call-args)))

(let [redis-calls* redis-calls*]
  (defn ^:public redis-calls
    "Send >=0 arbitrary commands to Redis server.
    Takes vararg calls, with each call a vector of args:
      (wcar {}
        (redis-call [\"set\" \"my-key\" \"my-val\"]
                    [\"get\" \"my-key\"])) => [\"OK\" \"my-val\"]

    Useful for DSLs, and to call commands (including Redis module commands)
    that might not yet have a native Clojure fn provided by Carmine."
    [& calls] (redis-calls* calls)))

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
          (LocalEchoRequest. (get-read-opts ctx) x))
        nil)
      (throw-no-ctx-error! ["LOCAL-ECHO" x])))

  (defn ^:public local-echos*
    "Like `local-echo`, except takes a vector of >=0 args to echo."
    [xs]
    (if-let [^Ctx ctx *ctx*]
      (let [^LinkedList pending-reqs (.-pending-reqs* ctx)
            read-opts                (get-read-opts   ctx)]
        (run!
          (fn [x] (.addLast pending-reqs (LocalEchoRequest. read-opts x)))
          xs)
        nil)
      (throw-no-ctx-error! (into ["LOCAL-ECHOS"] xs))))

  (defn ^:public local-echos
    "Like `local-echo`, except takes >=0 varargs to echo."
    [& xs] (local-echos* xs)))

(do ; Basic commands for testing
  (defn ping []    (redis-call "PING"))
  (defn echo [x]   (redis-call "ECHO" x))
  (defn rset [k v] (redis-call "SET" k v))
  (defn rget [k]   (redis-call "GET" k)))

;;;;

(defn- ll ^LinkedList [n] (let [ll (LinkedList.)] (dotimes [n n] (.add ll n)) ll))
(comment (ll 10))

(comment
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

(defn- consume-list!
  ;; Note: we don't actually always NEED to consume (remove) items
  ;; while iterating, but benching shows that doing so is almost
  ;; as fast as non-consuming iteration - so we'll just always
  ;; consume to keep things simple and safe.
  ([f init ^LinkedList ll  ] (consume-list! f init ll (.size ll)))
  ([f init ^LinkedList ll n]
   (when (> ^int n 0)
     (enc/reduce-n                      ; Fastest way to iterate
       (fn [acc _] (f acc (.removeFirst ll)))
       init
       n))))

(let [sentinel-skipped-reply com/sentinel-skipped-reply]
  (defn flush-pending-requests [^Ctx ctx]
    "Given a Ctx with pending-reqs* and pending-replies*:
      - Consumes (mutates) all pending-reqs*
      - Adds to  (mutates)     pending-replies*

    Returns the number of requests consumed (used only for
    debugging/testing)."

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
                  (instance? Request req) ; Common case
                  (let [args (.-args ^Request req)]
                    (write/write-array-len out (count args))
                    (enc/run! (fn [arg] (write/write-bulk-arg arg out)) args))

                  ;; Noop, don't actually send anything to Redis
                  (instance? LocalEchoRequest req) nil))
              nil pending-reqs* n-pending-reqs)
            (.flush ^java.io.BufferedOutputStream out))

          ;; Now re-consume all requests to read replies from Redis server
          (let [in (.-in ctx)]
            (consume-list!
              (fn [_ req]
                (let [completed-reply
                      (enc/cond!
                        (instance? Request req) ; Common case
                        (let [read-opts (.-read-opts ^Request req)]
                          (read/read-reply read-opts in))

                        (instance? LocalEchoRequest req)
                        (let [read-opts (.-read-opts ^LocalEchoRequest req)
                              reply     (.-reply     ^LocalEchoRequest req)]
                          (read/complete-reply read-opts reply)))]

                  (if (identical? completed-reply sentinel-skipped-reply)
                    nil ; Noop
                    (.add pending-replies* completed-reply))))
              nil consumed-reqs* n-pending-reqs))

          n-pending-reqs)))))

(declare ^:private complete-replies)

(defn with-replies
  "Establishes (possibly-nested) Ctx, flushes requests in body,
  and returns completed replies."
  ([in out natural-reads? as-vec? body-fn] ; Used by `with-carmine`, etc.
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-requests parent-ctx))

   (let [new-ctx (Ctx. in out natural-reads? (LinkedList.) (LinkedList.))]
     (binding [*ctx* new-ctx] (body-fn))
     (flush-pending-requests   new-ctx)
     (complete-replies as-vec? new-ctx)))

  ([natural-reads? as-vec? body-fn] ; Used by public `with-replies` macro
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-requests parent-ctx)

     (let [new-ctx
           (Ctx. (.-in parent-ctx) (.-out parent-ctx) natural-reads?
             (LinkedList.) (LinkedList.))]

       (binding [*ctx* new-ctx] (body-fn))
       (flush-pending-requests   new-ctx)
       (complete-replies as-vec? new-ctx)))))

(let [reply-error? com/reply-error?]

  (defn- complete-replies
    [as-vec? ^Ctx ctx]
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
          pending-replies* n-replies)))))

;;;;

(let [read-opts-natural com/read-opts-natural
      ba-command        (com/str->bytes "*1\r\n$4\r\nPING\r\n")
      ba-len            (alength ba-command)]

  (defn basic-ping!
    "Low-level util.
    Sends a minimally expensive single PING command directly to Redis,
    and reads reply. Forgoes Ctx, read mode, parsing, etc."
    [in ^java.io.BufferedOutputStream out]
    (.write out ba-command 0 ba-len)
    (.flush out)
    (read/read-reply read-opts-natural in)))

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
          #{}                       [nil                    bn]
          #{:as-vec               } [{:as-vec?        true} bn]
          #{        :natural-reads} [{:natural-reads? true} bn]
          #{:as-vec :natural-reads} [{:as-vec?        true
                                      :natural-reads? true} bn]
          (throw
            (ex-info "[Carmine] Unexpected reply-opts in body"
              {:opts {:value b1 :type (type b1)}})))

        (map? b1) [b1    bn]
        :else     [nil body]))))
