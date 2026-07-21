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

(defn- throw-conflict!
  "Throws if `result` is a final conflict, otherwise returns `result`.
  Called after the borrow: EXEC has completed, so the conn was reusable."
  [watch-keys {:keys [conflict? attempts] :as result}]
  (if conflict?
    (truss/ex-info! "[Carmine] Optimistic transaction conflicted"
      {:eid :carmine.tx/conflict
       :attempts attempts
       :watch-keys watch-keys})
    result))

(defn- throw-exec-errors!
  "Throws like `wcar` if any EXEC reply is an error, otherwise returns `result`.
  Called after the borrow: EXEC has completed, so the conn was reusable."
  [{:keys [replies plan attempts] :as result}]
  (let [error-indexes
        (into [] (keep-indexed (fn [idx reply] (when (com/reply-error? reply) idx)))
          replies)]
    (if (empty? error-indexes)
      result
      (throw
        (com/drained-reply-errors replies error-indexes
          {:plan plan, :attempts attempts})))))

(defn- transact*
  [conn-mgr opts plan-fn effects-fn effects-accepts-plan?]
  (let [conn-mgr (force conn-mgr)]
    (truss/have conns/conn-manager? conn-mgr)
    (utils/have-opts! :carmine.tx/invalid-opts "`transact!` options" #{:watch-keys :max-attempts :retry-backoff-ms :error-mode} opts)
    (truss/have fn? plan-fn)
    (truss/have fn? effects-fn)
    (let [{:keys [watch-keys max-attempts retry-backoff-ms error-mode]
         :or {max-attempts 8, error-mode :throw}} opts
          watch-keys
          (when (some? watch-keys)
            (vec (utils/have-opt! :carmine.tx/invalid-opts "`transact!` options" :watch-keys "nil or a collection"
                   coll? watch-keys)))]
      (utils/have-opt! :carmine.tx/invalid-opts "`transact!` options" :max-attempts "a positive integer" pos-int? max-attempts)
      (utils/have-opt! :carmine.tx/invalid-opts "`transact!` options" :retry-backoff-ms "nil, a non-negative integer, or a fn"
        (utils/nil-or (some-fn nat-int? fn?)) retry-backoff-ms)
      (utils/have-opt! :carmine.tx/invalid-opts "`transact!` options" :error-mode "`:throw` or `:return`"
        #{:throw :return} error-mode)
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
      (cond->> (conns/mgr-borrow! conn-mgr
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
                     :attempt attempt
                     :hint "Wrap plan reads in `with-replies`"}))
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
                    (cond
                      (vector? exec-reply)
                      (resp/local-echo
                        {:replies exec-reply
                         :plan plan
                         :attempts attempt})

                      ;; Only a nil EXEC reply means that nothing was committed
                      (some? exec-reply) (unexpected-reply! :exec exec-reply)

                      (< attempt max-attempts)
                      (do
                        (utils/backoff! retry-backoff-ms attempt)
                        (recur (inc attempt)))

                      :else (resp/local-echo {:conflict? true, :attempts attempt}))))))))))
        true                  (throw-conflict! watch-keys)
        (= error-mode :throw) (throw-exec-errors!)))))

(defn ^:public transact!
  "Runs a Redis transaction (MULTI/EXEC) on a connection from `conn-mgr`:
    (transact! mgr (fn [] (incr \"k1\") (incr \"k2\")))
    => {:replies [1 1], :plan nil, :attempts 1}

  `effects-fn` queues the commands to run in the transaction. For optimistic
  locking, add `:watch-keys` and a `plan-fn` that reads the current state:
    (transact! mgr {:watch-keys [\"k\"]}
      (fn plan-fn    []  (with-replies (get \"k\")))
      (fn effects-fn [v] (set \"k\" (str v \"!\"))))

  If a watched key changes before EXEC, both functions are called again, so
  they should have no side effects besides their Redis requests.

  Options:
    `:watch-keys`       - Keys to WATCH before each attempt
    `:max-attempts`     - Throw after this many conflicts (default 8)
    `:retry-backoff-ms` - Delay between attempts: msecs, or
                          (fn [attempt]) => msecs (default: no delay)
    `:error-mode`       - `:throw` (default) or `:return`

  Returns `{:replies <exec-replies> :plan <plan-fn-result> :attempts <n>}`:
    - If a command fails when EXEC runs, throws like `wcar` (ex-data also has
      `:plan` and `:attempts`), or returns errors in `:replies` with
      `:error-mode :return`. Redis doesn't roll back the other commands.
      A command that Redis rejects when queued always throws.
    - Reply parsers and read modes (`as-long`, `as-bytes`, etc.) used in
      `effects-fn` don't apply to `:replies`. Nor does `with-thaw-opts`:
      use that around the `transact!` call instead.

  Not supported with Cluster managers. Borrows its own connection, so call it
  outside of `wcar` to avoid exhausting the pool."
  ([conn-mgr effects-fn]
   (transact* conn-mgr {} (constantly nil) effects-fn false))
  ([conn-mgr opts effects-fn]
   (transact* conn-mgr opts (constantly nil) effects-fn false))
  ([conn-mgr opts plan-fn effects-fn]
   (transact* conn-mgr opts plan-fn effects-fn true)))
