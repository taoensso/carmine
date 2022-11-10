(ns taoensso.carmine.impl.resp
  "Implementation of the Redis RESP3 protocol,
  Ref. https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md"
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test     :as test :refer [deftest testing is]]
   [taoensso.encore  :as enc  :refer [throws?]]
   [taoensso.carmine.impl.resp.common       :as resp-com]
   [taoensso.carmine.impl.resp.read.common  :as read-com]
   [taoensso.carmine.impl.resp.read         :as read]
   [taoensso.carmine.impl.resp.write        :as write]
   [taoensso.carmine.impl.resp.read.parsing :as parsing])

  (:import [java.util LinkedList]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp)
  (test/run-tests 'taoensso.carmine.impl.resp))

;;;;

(def ^:dynamic *ctx* "?<Ctx>" nil)
(deftype Ctx [#_conn in out pending-reqs* pending-replies*])

(deftype Request          [read-opts args])
(deftype LocalEchoRequest [read-opts reply])

(defn redis-call*
  "Vector-args version of `redis-call`."
  [args]
  ;; Could alternatively write immediately (i.e. forgo pending-reqs*).
  ;; Awaiting Sentinel & Cluster to decide.
  (when-let [^Ctx ctx *ctx*]
    (.addLast ^LinkedList (.-pending-reqs* ctx)
      (Request. (read-com/new-read-opts) args)))
  nil)

(defn redis-call
  "Low-level generic Redis command util.
  Sends given arguments to Redis server as a single arbitrary command call:
    (redis-call \"ping\")
    (redis-call \"set\" \"my-key\" \"my-val\")

  As with all Carmine Redis command fns: expects to be called within a `wcar`
  body, and returns nil. The server's reply to this command will be included
  in the replies returned by the enclosing `wcar`.

  `redis-call` is useful for DSLs, and to call commands (including Redis module
  commands) that might not yet have a native Clojure fn provided by Carmine."
  [& args] (redis-call* args))

(defn local-echo
  "Acts exactly like the Redis `echo` command, except entirely local: no data
  is actually sent to/from Redis server.

  As with all Carmine Redis command fns: expects to be called within a `wcar`
  body, and returns nil. The server's reply to this command will be included
  in the replies returned by the enclosing `wcar`.

  `local-echo` is useful for DSLs and other advanced applications.
  Can be combined with `with-replies` or nested `wcar` calls to achieve some
  very powerful effects."
  [reply]
  (when-let [^Ctx ctx *ctx*]
    (.addLast ^LinkedList (.-pending-reqs* ctx)
      (LocalEchoRequest. (read-com/new-read-opts) reply)))
  nil)

(do ; Basic commands for testing
  (defn ping []    (redis-call "ping"))
  (defn echo [x]   (redis-call "echo" x))
  (defn rset [k v] (redis-call "set" k v))
  (defn rget [k]   (redis-call "get" k)))

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
     (enc/reduce-n ; Fastest way to iterate
       (fn [acc _] (f acc (.removeFirst ll)))
       init
       n))))

(let [sentinel-skipped-reply read-com/sentinel-skipped-reply]
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
  "Establishes (possibly-nested) Ctx, flushing requests in body,
  and returns completed replies."
  ([in out as-vec? body-fn] ; Used by `wcar`, etc.
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-requests parent-ctx))

   (let [new-ctx (Ctx. in out (LinkedList.) (LinkedList.))]
     (binding [*ctx* new-ctx] (body-fn))
     (flush-pending-requests   new-ctx)
     (complete-replies as-vec? new-ctx)))

  ([as-vec? body-fn] ; Used by public `with-replies` macro
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-requests parent-ctx)

     (let [new-ctx
           (Ctx. (.-in parent-ctx) (.-out parent-ctx)
             (LinkedList.) (LinkedList.))]

       (binding [*ctx* new-ctx] (body-fn))
       (flush-pending-requests   new-ctx)
       (complete-replies as-vec? new-ctx)))))

(defn parse-body
  "Returns [<as-vec?> <body>] for use by \"& body\" macros
  that want to support :as-vec prefix."
  ;; {:arglists '([& body] [:as-vec & body])}
  [body]
  (let [[b1 & bn] body]
    (if (or (= b1 :as-vec) (= b1 :as-pipeline))
      [true  bn]
      [false body])))

(let [get-reply-error    resp-com/get-reply-error
      unwrap-reply-error resp-com/unwrap-reply-error]

  (defn- complete-replies
    [as-vec? ^Ctx ctx]
    (let [^LinkedList pending-replies* (.-pending-replies* ctx)
          n-replies (.size pending-replies*)]

      (enc/cond
        (== n-replies 1)
        (let [reply (.removeFirst pending-replies*)]
          (if-let [reply-error (get-reply-error reply)]
            (if as-vec? [reply-error] (throw reply-error))
            (if as-vec? [reply]              reply)))

        (> n-replies 10)
        (persistent!
          (consume-list!
            (fn [acc reply] (conj! acc (unwrap-reply-error reply)))
            (transient []) pending-replies* n-replies))

        (> n-replies 0)
        (consume-list!
          (fn [acc reply] (conj acc (unwrap-reply-error reply)))
          [] pending-replies* n-replies)))))
