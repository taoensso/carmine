(ns ^:no-doc taoensso.carmine-v4.tx
  "Private implementation of the public v4 transaction API."
  (:require
   [taoensso.truss :as truss]
   [taoensso.carmine-v4.utils       :as utils]
   [taoensso.carmine-v4.conns       :as conns]
   [taoensso.carmine-v4.resp        :as resp]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write  :as write])
  (:import [taoensso.carmine_v4.classes ReusableConnError]))

(defn- transaction-call [f]
  (try
    (f)
    (catch Throwable t
      (if (instance? ReusableConnError t)
        ;; Fully-drained reply errors are normally safe for pool reuse, but the
        ;; connection may still retain WATCH or MULTI transaction state.
        (throw (com/reply-error (ex-message t) (ex-data t) t))
        (throw t)))))

(defn- unexpected-reply! [phase reply]
  (truss/ex-info! "[Carmine] Unexpected Redis transaction reply"
    {:eid :carmine.tx/unexpected-reply
     :phase phase
     :reply reply}))

(defn- internal-command! [phase command]
  (let [reply
        (transaction-call
          #(binding [write/*conn-reusable?_ nil]
             (resp/with-replies true false
               (fn [] (resp/rcmd* command)))))]
    (when-not (= reply "OK")
      (unexpected-reply! phase reply))
    reply))

(defn- transact*
  [conn-mgr opts plan-fn effects-fn effects-accepts-plan?]
  (let [conn-mgr (force conn-mgr)]
    (truss/have conns/conn-manager? conn-mgr)
    (truss/have map? opts)
    (truss/have [:ks<= #{:watch-keys :max-attempts :retry-backoff-ms}] opts)
    (truss/have fn? plan-fn)
    (truss/have fn? effects-fn)
    (let [{:keys [watch-keys max-attempts retry-backoff-ms]
         :or {max-attempts 8}} opts
          watch-keys (when watch-keys (vec (truss/have coll? watch-keys)))]
      (truss/have pos-int? max-attempts)
      (truss/have [:or nil? nat-int? fn?] retry-backoff-ms)
      (when (conns/mgr-cluster-server conn-mgr)
        (truss/ex-info! "[Carmine] Optimistic transactions do not support Redis Cluster"
          {:eid :carmine.tx/cluster-not-supported
           :mgr conn-mgr}))
      (when (get-in (conns/mgr-conn-opts conn-mgr)
              [:server :sentinel-opts :prefer-read-replica?])
        (truss/ex-info! "[Carmine] Transactions require a Redis master connection"
          {:eid :carmine.tx/replica-manager-not-supported
           :mgr conn-mgr}))
      ;; NB same-manager nesting (`transact!` inside `wcar`) is deliberately
      ;; ALLOWED and tested: it takes a second borrow, with the pool-exhaustion
      ;; caveat documented in the docstring.
      (conns/mgr-borrow! conn-mgr
      (fn [conn in out]
        ;; The outer context retains connection affinity across all attempts.
        ;; Only the final local result remains pending when the body returns.
        (resp/with-replies in out
          (conns/mgr-push-fn conn-mgr (conns/conn-addr conn))
          true false
          (fn []
            (loop [attempt 1]
              (when (seq watch-keys)
                (internal-command! :watch (into ["WATCH"] watch-keys)))
              (let [plan (transaction-call plan-fn)
                    n-stray-requests (resp/ctx-pending-request-count)
                    n-stray-replies  (resp/ctx-pending-reply-count)]
                (when (or (pos? n-stray-requests) (pos? n-stray-replies))
                  (truss/ex-info! "[Carmine] Transaction plan left unconsumed Redis work"
                    {:eid :carmine.tx/stray-plan-requests
                     :pending-requests n-stray-requests
                     :pending-replies n-stray-replies
                     :attempt attempt}))
                (internal-command! :multi ["MULTI"])
                (let [queued-replies
                      (binding [com/*tx-effects?* true]
                        (resp/with-replies
                          {:natural-replies? true, :as-vec? true, :error-mode :return} nil
                          #(transaction-call
                             (fn []
                               (if effects-accepts-plan?
                                 (effects-fn plan)
                                 (effects-fn))))))
                      queue-errors (into [] (filter com/reply-error?) queued-replies)
                      unexpected
                      (into []
                        (remove #(or (= % "QUEUED") (com/reply-error? %)))
                        queued-replies)]
                  (when (seq queue-errors)
                    (truss/ex-info! "[Carmine] Redis rejected a queued transaction command"
                      {:eid :carmine.tx/queued-command-rejected
                       :attempt attempt
                       :errors queue-errors}
                      (first queue-errors)))
                  (when (seq unexpected)
                    (unexpected-reply! :queue unexpected))
                  (let [exec-reply
                        ;; Ignore ambient read modes/parsers for orchestration,
                        ;; while retaining ordinary auto-thaw and map handling.
                        (transaction-call
                          #(binding [com/*read-mode* nil, com/*parser* nil
                                     write/*conn-reusable?_ nil]
                             (resp/with-replies false false
                               (fn [] (resp/rcmd "EXEC")))))]
                    (if (vector? exec-reply)
                      (resp/local-echo
                        {:replies exec-reply
                         :plan plan
                         :attempts attempt})
                      (if (< attempt max-attempts)
                        (do
                          (utils/backoff! retry-backoff-ms attempt)
                          (recur (inc attempt)))
                        (truss/ex-info! "[Carmine] Optimistic transaction conflicted"
                          {:eid :carmine.tx/conflict
                           :attempts attempt
                           :watch-keys watch-keys}))))))))))))))

(defn ^:public transact!
  "Runs a Redis transaction on one connection from `conn-mgr`.

  Arity forms:
    (transact! conn-mgr effects-fn)
    (transact! conn-mgr opts effects-fn)
    (transact! conn-mgr opts plan-fn effects-fn)

  Options:
    - `:watch-keys`: Keys to watch before each attempt.
    - `:max-attempts`: Maximum attempt count (default 8).
    - `:retry-backoff-ms`: Nil, a constant delay, or
      `(fn [attempt] delay-ms)`. Nil or zero adds no delay.

  `plan-fn` runs after WATCH and before MULTI. It may read through
  [[taoensso.carmine-v4/with-replies]]. In the four-argument form, its result is
  passed to `effects-fn`; the other forms call `effects-fn` with no arguments.
  `effects-fn` must only queue commands; this function runs them with MULTI and
  EXEC.

  Reply parsers and read modes inside `effects-fn` (e.g.
  [[taoensso.carmine-v4/as-long]] and [[taoensso.carmine-v4/as-bytes]]) do not
  affect `:replies`. Queued commands return only \"QUEUED\", and the final EXEC
  array is completed without these transformations. Use ordinary Clojure
  functions to process the returned `:replies`.

  Returns `{:replies exec-replies, :plan plan-result, :attempts n}`. Only a nil
  EXEC reply from a WATCH conflict causes a retry. A transport failure is not
  retried because Redis may have committed the transaction. After
  `:max-attempts` conflicts, throws with `:eid :carmine.tx/conflict`.

  `conn-mgr` is required. It may be a manager or a delay that resolves to one.
  This function rejects Cluster managers and Sentinel replica-preferring
  managers.
  If an error occurs, the connection is invalidated or closed so transaction
  state cannot return to a pool. See the v4 guide for failure and
  connection-ownership contracts.

  [[taoensso.carmine-v4/transact!]] borrows its own connection. If you call it
  while you hold a connection from the same manager, the pool can become
  exhausted and block. With `:max-total` 1, this causes a deadlock. Call it
  outside [[taoensso.carmine-v4/wcar]], or use a separate manager."
  ([conn-mgr effects-fn]
   (transact* conn-mgr {} (constantly nil) effects-fn false))
  ([conn-mgr opts effects-fn]
   (transact* conn-mgr opts (constantly nil) effects-fn false))
  ([conn-mgr opts plan-fn effects-fn]
   (transact* conn-mgr opts plan-fn effects-fn true)))
