(ns taoensso.carmine.impl.resp
  "Implementation of the Redis RESP3 protocol,
  Ref. https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md."
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

(defmacro debug [& body] `(when false #_true (println (format ~@body))))

(def ^:dynamic *ctx* "?<Ctx>" nil)
(deftype Ctx [#_conn in out pending-reqs* pending-replies*])

(deftype Request          [read-opts req-args])
(deftype LocalEchoRequest [read-opts reply])

(defn redis-request [req-args]
  (when-let [^Ctx ctx *ctx*]
    (.addLast ^LinkedList (.-pending-reqs* ctx)
      (Request. (read-com/new-read-opts) req-args))

    (debug "Added pending req: %s, now pending: %s"
      req-args (.size ^LinkedList (.-pending-reqs* ctx)))))

(defn local-echo
  "TODO Docstring, effected by parsers and read-mode"
  [reply]
  (when-let [^Ctx ctx *ctx*]
    (.addLast ^LinkedList (.-pending-reqs* ctx)
      (LocalEchoRequest. (read-com/new-read-opts) reply))

    (debug "Added pending req: %s, now pending: %s"
      ["LOCAL-ECHO" reply] (.size ^LinkedList (.-pending-reqs* ctx)))))

(do ; Basic commands for testing
  (defn ping []    (redis-request ["PING"]))
  (defn echo [x]   (redis-request ["ECHO" x]))
  (defn rset [k v] (redis-request ["SET" k v]))
  (defn rget [k]   (redis-request ["GET" k])))

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

(let [sentinel-skipped-reply read-com/sentinel-skipped-reply]
  (defn flush-pending-requests [^Ctx ctx]
    "Given a Ctx with pending-reqs* and pending-replies*:
      - Consumes (mutates) all pending-reqs*
      - Adds to  (mutates)     pending-replies*

    Returns the number of requests consumed (used only for
    debugging/testing)."

    (let [^LinkedList pending-reqs* (.-pending-reqs* ctx)
          n-pending-reqs (.size pending-reqs*)]

      (debug "Will flush %s requests, pending replies: %s"
        (.size ^LinkedList (.-pending-reqs*    ctx))
        (.size ^LinkedList (.-pending-replies* ctx)))

      (when (> n-pending-reqs 0)
        (let [^LinkedList pending-replies* (.-pending-replies* ctx)
              ^LinkedList consumed-reqs*   (LinkedList.)]

          ;; Consume all pending requests, writing to Redis server
          ;; without awaiting any replies (=> use pipelining).
          ;;
          ;; Note: don't NEED to CONSUME from pending-reqs* unless in
          ;; a nested ctx, but always doing so is almost as fast as not.
          (let [out (.-out ctx)]
            (enc/reduce-n
              (fn [_ _]
                (let [req (.removeFirst pending-reqs*)]
                  (.add consumed-reqs* req) ; Move to consumed list
                  (enc/cond!
                    (instance? Request req) ; Common case
                    (let [args (.-req-args ^Request req)]
                      (write/write-array-len out (count args))
                      (enc/run! (fn [arg] (write/write-bulk-arg arg out)) args))

                    ;; Noop, don't actually send anything to Redis
                    (instance? LocalEchoRequest req) nil)))
              nil
              n-pending-reqs)
            (.flush ^java.io.BufferedOutputStream out))

          ;; Now re-consume all requests to read replies from Redis server
          (let [in (.-in ctx)]
            (enc/reduce-n
              (fn [_ _]
                (let [req (.removeFirst consumed-reqs*)
                      completed-reply
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
              nil
              n-pending-reqs))

          n-pending-reqs)))))

(declare ^:private complete-replies)

(defn with-replies
  "Establishes (possibly-nested) Ctx, flushing requests in body,
  and returns completed replies."
  ([in out as-vec? body-fn] ; Used by `with-carmine`, etc.
   (debug "With replies: %s" (if *ctx* "nested" "unnested"))

   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-requests parent-ctx))

   (let [new-ctx (Ctx. in out (LinkedList.) (LinkedList.))]
     (binding [*ctx* new-ctx]
       (body-fn)
       (flush-pending-requests   new-ctx)
       (complete-replies as-vec? new-ctx))))

  ([as-vec? body-fn]
   (debug "With replies: %s" (if *ctx* "nested" "unnested"))
   (when-let [^Ctx parent-ctx *ctx*]
     (flush-pending-requests parent-ctx)

     (let [new-ctx (Ctx. (.-in parent-ctx) (.-out parent-ctx)
                     (LinkedList.) (LinkedList.))]

       (binding [*ctx* new-ctx]
         (body-fn)
         (flush-pending-requests   new-ctx)
         (complete-replies as-vec? new-ctx))))))

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
    (let [^LinkedList pending-replies* (.-pending-replies* ctx)]
      (enc/cond
        :let [n-replies (.size pending-replies*)]

        (== n-replies 1)
        (let [reply (.removeFirst pending-replies*)]
          (if-let [reply-error (get-reply-error reply)]
            (if as-vec? [reply-error] (throw reply-error))
            (if as-vec? [reply]              reply)))

        (> n-replies 10)
        (persistent!
          (enc/reduce-n
            (fn [acc _]
              (let [reply (.removeFirst pending-replies*)]
                (conj! acc (unwrap-reply-error reply))))
            (transient [])
            n-replies))

        (> n-replies 0)
        (enc/reduce-n
          (fn [acc _]
            (let [reply (.removeFirst pending-replies*)]
              (conj acc (unwrap-reply-error reply))))
          []
          n-replies)))))
