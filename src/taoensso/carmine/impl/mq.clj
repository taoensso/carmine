(ns ^:no-doc taoensso.carmine.impl.mq
  "Private implementation shared by the Carmine v3 and v4 message queues
  (`taoensso.carmine.message-queue`, `taoensso.carmine-v4.message-queue`).
  Please don't use directly, subject to change without notice.

  Independent of Carmine client version: all Redis access is through a
  client-supplied `IClient`. Sections:
    - Store:  Redis key layout, Lua scripts, and the requests on them.
    - Engine: handler invocation, worker lifecycle and polling loops.

  Public API concerns (argument parsing, option names, validation, etc.)
  belong in the client namespaces."

  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string        :as str]
   [taoensso.encore       :as enc]
   [taoensso.truss        :as truss]
   [taoensso.encore.stats :as stats]
   [taoensso.trove        :as trove]))

;;;; Client

(defprotocol IClient
  "Redis access for a particular Carmine client version and connection
  source (v3 conn opts, v4 connection manager)."
  (exec [_ reqs]
    "Sends given requests as a pipeline and returns a ?vector of their
    replies, or throws on any error reply. Each request is e/o:
      [<cmd-name> <arg> ...]                ; Redis command
      [:lua <script> <keys-map> <args-map>] ; As Carmine `lua`,
                                            ; makes an immediate round-trip")

  (open?  [_] "Returns true iff client's connection source is still usable.")
  (log-ns [_] "Returns namespace string to attribute client's logging to.")
  (sleep-cap-ms [_]
    "Returns nil (uncapped), or the max msecs that a blocking request may
    safely block for. Values < 10 mean that blocking requests are unsafe."))

(defn- exec1 "Sends given request and returns its reply."
  [client req] (nth (exec client [req]) 0))

(defmacro ^:private log!
  "Like `trove/log!`, but attributed to client's namespace."
  [client opts] `(trove/log! ~(assoc opts :ns `(log-ns ~client))))

;;;; Store

(defn redis-key
  "Same as v3 `taoensso.carmine/key`."
  [& parts] (enc/str-join ":" (map #(if (keyword? %) (enc/as-qname %) (str %))) parts))

(let [cluster-support? false] ; TODO Cluster support
  (def qkey
    (enc/fmemoize
      (fn [qname k]
        (redis-key :carmine :mq
          (if cluster-support?
            (str "{" (enc/as-qname qname) "}")
            (do                    qname))
          k)))))

(comment (enc/qb 1e6 (qkey :qname :qk)))

(let [idx-qname-start (inc (count (redis-key :carmine :mq))) ; "carmine:mq:"
      suffix-len      (count ":messages")]

  (defn qkeys->qnames
    "Returns a non-empty set of queue names for given `messages` keys, or nil.
    For keys that match (qkey <pattern> :messages)."
    [qks]
    (when-let [qks (not-empty qks)]
      (into #{} (map #(enc/substr % idx-qname-start (- (count %) suffix-len))) qks))))

(defn queues-clear!
  "Returns nil, or the given non-empty collection of queue names."
  [client qnames]
  (when (seq qnames)
    (exec client
      (reduce
        (fn [reqs qname]
          (if-not qname
            reqs
            (let [qk (partial qkey qname)]
              (conj reqs
                ["DEL"
                 (qk :messages)
                 (qk :messages-rq)
                 (qk :lock-times)
                 (qk :lock-times-rq)
                 (qk :udts)
                 (qk :locks)
                 (qk :lock-tokens)
                 (qk :backoffs)
                 (qk :nattempts)
                 (qk :done)
                 (qk :requeue)
                 (qk :mids-ready)
                 (qk :mid-circle)
                 (qk :ndry-runs)
                 (qk :next-due)
                 (qk :isleep-a)
                 (qk :isleep-b)]))))
        [] qnames))
    qnames))

(defn- kvs->map
  "`HGETALL` reply -> map. RESP2 replies are flat [k1 v1 ...] vectors,
  RESP3 replies are already maps."
  [kvs]
  (cond
    (map?   kvs) kvs
    (empty? kvs) {}
    :else (persistent! (enc/reduce-kvs assoc! (transient {}) kvs))))

(defn server-udt
  "Redis `TIME` reply [<unix-secs> <micros>] -> server clock as epoch msecs"
  ^long [[secs micros]]
  (+ (* (enc/as-int secs) 1000) (quot (enc/as-int micros) 1000)))

;;;;

(defn ->message-status
  "[<?base-status-str> <backoff?> <requeue?>] -> <status-kw>"
  ([?base-status-str backoff? requeue?]
   (case ?base-status-str
     "nx"     nil
     "queued" (if backoff? :queued-with-backoff :queued)
     "locked" (if requeue? :locked-with-requeue :locked)
     "done"
     (enc/cond
       requeue? :done-with-requeue
       backoff? :done-with-backoff
       :else    :done-awaiting-gc)
     (do        :unknown)))

  ([reply]
   (if (vector? reply)
     (let [[?bss bo? rq?] reply] (->message-status ?bss bo? rq?))
     :unknown)))

(comment (->message-status ["queued" "1" nil]))

(defn- reqs-queue-counts
  "Requests for [nlocked nbackoff nready ncircle ndone]"
  [qname]
  (let [qk (partial qkey qname)]
    [["HLEN"  (qk :locks)]
     ["HLEN"  (qk :backoffs)]
     ["LLEN"  (qk :mids-ready)]
     ["LLEN"  (qk :mid-circle)]
     ["SCARD" (qk :done)]]))

(defn- -queue-status [queue-counts]
  (let [[^long nlocked ^long nbackoff ^long nready ^long ncircle ^long ndone] queue-counts
        ntotal  (max 0 (dec (+ nready ncircle))) ; dec for eoq
        nqueued (max 0 (- ntotal (+ nlocked nbackoff ndone)))]

    {:nqueued  nqueued  ; Also called "queue-size"
     :nlocked  nlocked  ; Approx, may contain expired entries
     :nbackoff nbackoff ; Approx, may contain expired entries
     :ntotal   ntotal}))

(defn queue-size
  "Counts only, avoids the extra lookups needed for `:head-age-ms`."
  [client qname]
  (get (-queue-status (exec client (reqs-queue-counts qname))) :nqueued))

(defn queue-status [client qname]
  (let [qk (partial qkey qname)
        [head-mid & queue-counts]
        (exec client
          (into [["LINDEX" (qk :mids-ready) -1]] ; Head of ready list
            (reqs-queue-counts qname)))

        head-age-ms
        (when head-mid
          (let [[udt now] (exec client [["HGET" (qk :udts) head-mid] ["TIME"]])]
            (when udt (max 0 (- (server-udt now) (enc/as-int udt))))))]

    (assoc (-queue-status queue-counts) :head-age-ms head-age-ms)))

(defn queue-content [client qname]
  (let [qk (partial qkey qname)

        [now messages messages-rq
         lock-times lock-times-rq
         udts locks backoffs nattempts
         done requeue]
        (exec client
          [["TIME"] ; Clock that writes new expiries
           ["HGETALL"  (qk :messages)]
           ["HGETALL"  (qk :messages-rq)]
           ["HGETALL"  (qk :lock-times)]
           ["HGETALL"  (qk :lock-times-rq)]
           ["HGETALL"  (qk :udts)]
           ["HGETALL"  (qk :locks)]
           ["HGETALL"  (qk :backoffs)]
           ["HGETALL"  (qk :nattempts)]
           ["SMEMBERS" (qk :done)]
           ["SMEMBERS" (qk :requeue)]])

        now         (server-udt now)
        messages    (kvs->map messages)
        messages-rq (kvs->map messages-rq)
        udts        (kvs->map udts)
        locks       (kvs->map locks)
        backoffs    (kvs->map backoffs)
        nattempts   (kvs->map nattempts)
        done        (set done)
        requeue     (set requeue)]

    (persistent! ; {<mid> {:keys [message status ...]}}
      (reduce-kv
        (fn [m mid mcontent]
          (assoc! m mid
            (let [exp-lock    (enc/as-int (get locks    mid 0))
                  exp-backoff (enc/as-int (get backoffs mid 0))

                  locked?  (< now exp-lock)
                  backoff? (< now exp-backoff)
                  done?    (contains? done mid)
                  requeue?
                  (or
                    (contains? messages-rq mid)
                    (contains? requeue     mid))

                  backoff-ms (when backoff? (- exp-backoff now))
                  age-ms     (when-let [udt (get udts mid)]
                               (- now (enc/as-int udt)))

                  base-status
                  (enc/cond
                    done?   "done"
                    locked? "locked"
                    :else   "queued")]

              (enc/assoc-some
                {:message   mcontent
                 :status    (->message-status base-status backoff? requeue?)
                 :nattempts (get nattempts mid 0)}
                :backoff-ms backoff-ms
                :age-ms     age-ms))))

        (transient messages)
        (do        messages)))))

(defn queue-mids
  "Returns {:ready [<mid> ...], :circle [<mid> ...], :next-mid <?mid>} state
  for given named queue in O(n_mids). Used mostly for debugging/tests."
  [client qname]
  (let [[ready circle]
        (exec client
          [["LRANGE" (qkey qname :mids-ready) 0 -1]
           ["LRANGE" (qkey qname :mid-circle) 0 -1]])]

    {:ready              ready
     :circle                          circle
     :next-mid (or (peek ready) (peek circle))}))

;;;; Lua requests

;; NB these Lua resources are shared by the v3 and v4 MQ,
;; keep them compatible with both.
(let [marker   "--[[include:msg-status-fn]]"
      resource (fn [name] (truss/have (enc/slurp-resource (str "taoensso/carmine/lua/mq/" name ".lua"))))
      load-lua
      (fn [name]
        ;; Splice in the shared status fn, so that there's a single copy
        (let [script (resource name)]
          (if (str/includes? script marker)
            (str/replace     script marker (resource "msg-status-fn"))
            (do              script))))]

  (def ^:private lua-msg-status_  (delay (load-lua "msg-status")))
  (def ^:private lua-cancel_      (delay (load-lua "cancel")))
  (def ^:private lua-enqueue_     (delay (load-lua "enqueue")))
  (def ^:private lua-dequeue_     (delay (load-lua "dequeue")))
  (def ^:private lua-finalize_    (delay (load-lua "finalize")))
  (def ^:private lua-extend-lock_ (delay (load-lua "extend-lock"))))

(defn req-message-status
  "Returns request with reply for `->message-status`."
  [qname mid]
  [:lua @lua-msg-status_
   {:qk-messages    (qkey qname :messages)
    :qk-messages-rq (qkey qname :messages-rq)
    :qk-locks       (qkey qname :locks)
    :qk-backoffs    (qkey qname :backoffs)
    :qk-done        (qkey qname :done)
    :qk-requeue     (qkey qname :requeue)}
   {:mid mid}])

(defn req-enqueue
  "Returns request with reply for `enqueue-result`.
  `mcontent` should be wrapped for client's automatic Nippy freezing."
  [qname mid mcontent
   {:keys [init-backoff-ms lock-ms can-update? can-requeue? reset-init-backoff?]}]
  [:lua @lua-enqueue_
   {:qk-messages      (qkey qname :messages)
    :qk-messages-rq   (qkey qname :messages-rq)
    :qk-lock-times    (qkey qname :lock-times)
    :qk-lock-times-rq (qkey qname :lock-times-rq)
    :qk-udts          (qkey qname :udts)
    :qk-locks         (qkey qname :locks)
    :qk-lock-tokens   (qkey qname :lock-tokens)
    :qk-backoffs      (qkey qname :backoffs)
    :qk-nattempts     (qkey qname :nattempts)
    :qk-done          (qkey qname :done)
    :qk-requeue       (qkey qname :requeue)
    :qk-mids-ready    (qkey qname :mids-ready)
    :qk-mid-circle    (qkey qname :mid-circle)
    :qk-next-due      (qkey qname :next-due)
    :qk-isleep-a      (qkey qname :isleep-a)
    :qk-isleep-b      (qkey qname :isleep-b)}

   {:mid        mid
    :init-bo    (or init-backoff-ms 0)
    :lock-ms    (or lock-ms        -1)
    :can-upd?   (if can-update?         "1" "0")
    :can-rq?    (if can-requeue?        "1" "0")
    :reset-ibo? (if reset-init-backoff? "1" "0")
    :mcnt       mcontent}])

(defn enqueue-result
  "`enqueue` reply -> {:keys [success? mid action error]}"
  [mid reply]
  (enc/cond
    :let [[action error] (when (vector? reply) reply)]
    error  {:success? false, :error  (keyword error)}
    action {:success? true,  :action (keyword action), :mid mid}
    :else  {:success? false, :error  :unknown}))

(defn req-cancel
  "Returns request with reply for `cancel-result`."
  [qname mid]
  [:lua @lua-cancel_
   {:qk-messages      (qkey qname :messages)
    :qk-messages-rq   (qkey qname :messages-rq)
    :qk-lock-times-rq (qkey qname :lock-times-rq)
    :qk-locks         (qkey qname :locks)
    :qk-lock-tokens   (qkey qname :lock-tokens)
    :qk-backoffs      (qkey qname :backoffs)
    :qk-done          (qkey qname :done)
    :qk-requeue       (qkey qname :requeue)}
   {:mid mid}])

(defn cancel-result
  "`cancel` reply -> ?<prior-status-kw>"
  [reply]
  (when (vector? reply)
    (let [[?bss bo? rq? cancelled?] reply]
      (when cancelled? (->message-status ?bss bo? rq?)))))

(defn req-dequeue
  "Returns request to process next mid/s, with reply e/o:
    - [\"skip\"   <reason>]                                   ; Worker thread should skip
    - [\"sleep\"  <reason> <msecs>]                           ; Worker thread should sleep
    - [\"handle\" <mid> <mcontent> <attempt> <lock-ms> <udt>] ; Worker thread should handle

  Processes up to `max-poll-steps` mids, stopping at the first non-skip
  reply. So a single poll can do several maintenance steps (GC, requeue,
  skipping over locked or backing-off mids) before returning something to
  handle. With > 1 step, spare steps are used for circle maintenance before
  taking a mid from the ready list, so that maintenance can't be starved
  by a steady stream of new mids."
  [qname
   {:keys [default-lock-ms eoq-backoff-ms max-poll-steps]
    :or   {default-lock-ms (enc/ms :mins 60)
           eoq-backoff-ms  enc/exp-backoff
           max-poll-steps  16}}
   lease-token]

  (let [;; Precompute 5 backoffs so that `dequeue.lua` can init the backoff atomically
        [bo1 bo2 bo3 bo4 bo5]
        (cond
          (fn?     eoq-backoff-ms) (mapv           eoq-backoff-ms (range 5))
          (number? eoq-backoff-ms) (repeat 5 (long eoq-backoff-ms))
          :else
          (truss/ex-info!
            (str "[Carmine/mq] Unexpected `eoq-backoff-ms` arg: " eoq-backoff-ms)
            {:arg (enc/typed-val eoq-backoff-ms)}))]

    [:lua @lua-dequeue_
     {:qk-messages      (qkey qname :messages)
      :qk-messages-rq   (qkey qname :messages-rq)
      :qk-lock-times    (qkey qname :lock-times)
      :qk-lock-times-rq (qkey qname :lock-times-rq)
      :qk-udts          (qkey qname :udts)
      :qk-locks         (qkey qname :locks)
      :qk-lock-tokens   (qkey qname :lock-tokens)
      :qk-backoffs      (qkey qname :backoffs)
      :qk-nattempts     (qkey qname :nattempts)
      :qk-done          (qkey qname :done)
      :qk-requeue       (qkey qname :requeue)
      :qk-mids-ready    (qkey qname :mids-ready)
      :qk-mid-circle    (qkey qname :mid-circle)
      :qk-ndry-runs     (qkey qname :ndry-runs)
      :qk-next-due      (qkey qname :next-due)
      :qk-isleep-a      (qkey qname :isleep-a)
      :qk-isleep-b      (qkey qname :isleep-b)}

     {:lease-token      lease-token
      :max-steps        (long max-poll-steps)
      :default-lock-ms  default-lock-ms
      :eoq-bo1          bo1
      :eoq-bo2          bo2
      :eoq-bo3          bo3
      :eoq-bo4          bo4
      :eoq-bo5          bo5}]))

(defn finalize
  ;; Fenced by lease token only: tokens are rotated on every dequeue and
  ;; cleared on supersedure (producer update, clear), so token equality
  ;; proves ownership. Expiry equality would wrongly fence an owner whose
  ;; own extend reply was lost or is still in flight.
  [client qname mid lease-token done? backoff-ms]
  (let [qk (partial qkey qname)]
    (exec1 client
      [:lua @lua-finalize_
       {:qk-locks       (qk :locks)
        :qk-lock-tokens (qk :lock-tokens)
        :qk-backoffs    (qk :backoffs)
        :qk-done        (qk :done)}
       {:mid         mid
        :lease-token lease-token
        :done?       (if done? "1" "0")
        :backoff-ms  (if (some? backoff-ms) (long backoff-ms) -1)}])))

(defn extend-lock
  [client qname mid lease-token lock-ms]
  (let [qk (partial qkey qname)]
    (let [[action lock-expiry]
          (exec1 client
            [:lua @lua-extend-lock_
             {:qk-locks       (qk :locks)
              :qk-lock-tokens (qk :lock-tokens)}
             {:mid         mid
              :lease-token lease-token
              :lock-ms     (long lock-ms)}])]
      (when (= action "extended") (long lock-expiry)))))

(defn- wake-sleepers!
  "Pushes n sleep sentinels to wake up to n sleeping workers.
  `enqueue` deliberately leaves only a single sentinel, which can wake only
  one worker. Shutdown wants them all, so this over-signals; any surplus is
  discarded by the next end-of-circle poll."
  [client qname n]
  (let [qk-isleep-a (qkey qname :isleep-a)]
    (exec client (vec (repeat (long n) ["LPUSH" qk-isleep-a "_"])))))

;;;; Engine

(defn- inc-nstat!
  ([nstats_ k1   ] (when nstats_ (swap! nstats_ (fn [m] (enc/update-in m [k1]    (fn [?n] (inc (long (or ?n 0)))))))))
  ([nstats_ k1 k2] (when nstats_ (swap! nstats_ (fn [m] (enc/update-in m [k1 k2] (fn [?n] (inc (long (or ?n 0))))))))))

(comment (inc-nstat! (atom {}) :k1))

(defn- thread-desync-ms
  "Returns ms ± 20%"
  [ms]
  (let [r (+ 0.8 (* 0.4 ^double (rand)))]
    (int (* r (long ms)))))

(comment (repeatedly 5 #(thread-desync-ms 500)))

(defn interruptible-sleep
  "To provide an interruptible thread sleep mechanism, we:

    - On dequeue: atomically clear both signal lists after observing no work.
    - On enqueue: leave one persistent sentinel in either signal list.
    - On dequeue of a mid while more are ready: likewise, to wake a helper.
    - On sleep: block on both lists and consume whichever sentinel arrives.

  A signal sent after the dequeue decision therefore remains consumable even
  before the blocking call begins. Repeated enqueues may move the sentinel
  between lists, but cannot cancel it.

  The sleep is capped to client's `sleep-cap-ms` (if any), to reduce the
  risk of it tripping a read timeout. Clients that can't safely block
  instead get a local sleep via `sleep-locally!` (fn [ms]), which enqueues
  can't interrupt.

  Without a cap, client should allow a read timeout comfortably > msecs
  (Redis's blocking timeout resolution is ~100ms with default `hz` 10),
  otherwise sleep will be interrupted prematurely by timeout."

  ([client qname ms] (interruptible-sleep client qname ms nil))
  ([client qname ms sleep-locally!]
   (let [ms      (max (long ms) 10)
         ?cap-ms (sleep-cap-ms client)]

     (if (and ?cap-ms (< (long ?cap-ms) 10))
       (do (if sleep-locally! (sleep-locally! ms) (Thread/sleep (int ms))) nil)
       (let [ms (if ?cap-ms (min ms (long ?cap-ms)) ms)
             secs-dbl (/ (double ms) 1000.0)]
         (try ; NB client's read timeout may be insufficient!
           (exec1 client ["BRPOP" (qkey qname :isleep-a) (qkey qname :isleep-b) secs-dbl])
           (catch Throwable _ nil)))))))

(defn- lock-extender
  "Returns a single-threaded scheduler for automatic lock extensions. Its
  (daemon) thread ends when idle, so no shutdown is needed."
  ^java.util.concurrent.ScheduledThreadPoolExecutor [qname]
  (doto
    (java.util.concurrent.ScheduledThreadPoolExecutor. 1
      (reify java.util.concurrent.ThreadFactory
        (newThread [_ r]
          (doto (Thread. r (str "carmine-mq-lock-extender-" (enc/as-qname qname)))
            (.setDaemon true)))))
    (.setKeepAliveTime 10 java.util.concurrent.TimeUnit/SECONDS)
    (.allowCoreThreadTimeOut   true)
    (.setRemoveOnCancelPolicy  true)))

(defn- auto-extend-lock!
  "Schedules periodic extension of given lease for up to `max-ms`, or until
  the lease is superseded. Returns a nullary fn to end the extensions."
  [client qname mid attempt lease-token lock-ms
   ^java.util.concurrent.ScheduledThreadPoolExecutor lock-extender max-ms]
  (let [t0          (System/nanoTime)
        interval-ms (max 1 (quot (long lock-ms) 3))
        future_     (atom nil)
        end!        (fn [] (when-let [^java.util.concurrent.Future f @future_] (.cancel f false)))
        data        {:qname qname, :mid mid, :attempt attempt}]

    (reset! future_
      (.scheduleWithFixedDelay lock-extender
        (fn []
          (try
            (enc/cond
              (>= (quot (- (System/nanoTime) t0) 1000000) (long max-ms))
              (do
                (end!)
                (log! client
                  {:level :warn, :id :carmine.mq/lock-auto-extend-ended
                   :msg "Handler exceeded max time for automatic lock extension, lock may now expire"
                   :data (assoc data :max-ms max-ms)}))

              (not (open? client)) (end!)
              (not (extend-lock client qname mid lease-token lock-ms))
              (end!)) ; Lease superseded

            (catch Throwable t ; Will retry
              (log! client
                {:level :warn, :id :carmine.mq/lock-auto-extend-error
                 :data data, :error t}))))

        interval-ms interval-ms java.util.concurrent.TimeUnit/MILLISECONDS))
    end!))

(defn handle1
  [client qname handler poll-reply
   {:keys [worker queue-size lease-token server-udt nstats_ ssb-queueing-time-ms ssb-handling-time-ns
           sleep-locally! lock-extender lock-auto-extend-max-ms
           max-attempts error-retry-backoff-ms on-give-up]
    :or   {queue-size -1}}]

  (let [[kind] (when (vector? poll-reply) poll-reply)
        qname  (enc/as-qname qname)]

    (case kind
      "skip"
      (let [[_kind reason] poll-reply]
        #_(inc-nstat! nstats_ (keyword "skip" reason)) ; Noisy
        [:skipped reason])

      "sleep"
      (let [[_kind reason isleep-on ttl-ms] poll-reply
            ttl-ms (thread-desync-ms (long ttl-ms))]

        (inc-nstat! nstats_ (keyword "sleep" reason))
        (interruptible-sleep client qname ttl-ms sleep-locally!)
        [:slept reason             isleep-on ttl-ms])

      "handle"
      (let [[_kind mid mcontent attempt lock-ms udt] poll-reply
            attempt (long attempt)

            age-ms ; Prefer server clock, since that's what wrote `udt`
            (when-let [udt (enc/as-?udt udt)]
              (max 0 (- (long (or server-udt (enc/now-udt))) ^long udt)))

            mq-msg
            {:qname qname, :mid mid, :message mcontent, :attempt attempt
             :lock-ms lock-ms, :age-ms age-ms, :worker worker, :queue-size queue-size
             :lock-extend!
             #(when (extend-lock client qname mid lease-token lock-ms)
                true)}

            max-attempts  (when max-attempts (long max-attempts))
            interrupted?_ (volatile! false)
            log-data      {:qname qname, :mid mid, :attempt attempt}

            end-auto-extend!
            (enc/when-let [le lock-extender, max-ms lock-auto-extend-max-ms]
              (auto-extend-lock! client qname mid attempt lease-token lock-ms le max-ms))

            ;; Lease is kept (and auto extended) while handler and any give-up hook run
            [result status throwable backoff-ms handling-time-ns
             give-up-reason retry-backoff-ms give-up-error]
            (try
              (let [;; Possible when earlier attempts all ended by lock expiry (worker died, etc.)
                    over-max? (and max-attempts (> attempt (long max-attempts)))
                    t0        (enc/now-nano*)
                    result
                    (when-not over-max?
                      (try
                        (handler mq-msg)
                        (catch InterruptedException t
                          (vreset! interrupted?_ true)
                          {:status :error, :throwable t})
                        (catch Throwable t
                          {:status :error, :throwable t})
                        (finally
                          ;; Clear any interrupt flag left by handler so that
                          ;; it can't break finalization
                          (Thread/interrupted))))

                    handling-time-ns (when-not over-max? (- (enc/now-nano*) t0))
                    last-attempt?    (and max-attempts (>= attempt (long max-attempts)))

                    {:keys [status throwable backoff-ms]
                     :or   {status :success}} ; Status may be any value, incl. nil
                    (when (map? result) result)

                    retry-backoff-ms ; For `:error` status
                    (when (and (= status :error) (not last-attempt?))
                      (when-let [ms error-retry-backoff-ms]
                        (long (or backoff-ms (if (fn? ms) (ms attempt) ms) 0))))

                    give-up-reason
                    (enc/cond
                      over-max?        :max-attempts
                      (= status :retry) (when last-attempt? :max-attempts)
                      (= status :error)
                      (enc/cond
                        retry-backoff-ms nil
                        (and last-attempt? error-retry-backoff-ms) :max-attempts
                        :else            :error))

                    give-up-error
                    (enc/when-let [reason give-up-reason, f on-give-up]
                      (try
                        ;; Skip hook if lease was superseded (message cancelled, etc.),
                        ;; in which case the finalize below will also be fenced
                        (when (= (exec1 client ["HGET" (qkey qname :lock-tokens) mid]) lease-token)
                          (f (enc/assoc-some (assoc mq-msg :reason reason) :throwable throwable)))
                        nil
                        (catch InterruptedException t (vreset! interrupted?_ true) t)
                        (catch Throwable            t                              t)))]

                [result status throwable backoff-ms handling-time-ns
                 give-up-reason retry-backoff-ms give-up-error])
              (finally
                (when end-auto-extend! (end-auto-extend!))))

            handled? (some? handling-time-ns)

            status-nstat-k
            (when handled?
              (case status
                (:success :error :retry) (keyword "handler" (name status))
                :handler/unexpected))

            fin
            (fn [done? backoff-ms gave-up?]
              (let [reply (finalize client qname mid lease-token done? backoff-ms)]
                ;; Counters are disjoint: a fenced result had no effect on the
                ;; queue, so don't also count it as a completed handling
                (if (= reply ["stale"])
                  (do
                    (inc-nstat! nstats_ :handler/fenced)
                    (log! client
                      {:level :warn, :id :carmine.mq/stale-handler-fenced
                       :data log-data}))

                  (do
                    (when status-nstat-k (inc-nstat! nstats_ status-nstat-k))
                    (when backoff-ms     (inc-nstat! nstats_ :handler/backoff))
                    (when gave-up?       (inc-nstat! nstats_ :handler/gave-up))))
                reply))]

        (when (and handled? (== attempt 1))
          ;; queueing-time => time till handler, not time till successful handling
          (enc/when-let [ssb ssb-queueing-time-ms, ms age-ms]           (ssb ms)))
        (enc/when-let   [ssb ssb-handling-time-ns, ns handling-time-ns] (ssb ns))

        (try
          ;; Log first, so that the handler error isn't lost if `fin` throws
          (when (and handled? (= status :error))
            (log! client
              {:level :error, :id :carmine.mq/handler-error,
               :data (assoc log-data :message mcontent)
               :error throwable}))

          ;; Effects
          (enc/cond
            give-up-error
            (do
              ;; Message is kept until hook succeeds
              (log! client
                {:level :error, :id :carmine.mq/give-up-error, :msg "Will backoff and retry"
                 :data (assoc log-data :message mcontent, :reason give-up-reason)
                 :error give-up-error})
              (fin false
                (enc/exp-backoff
                  (if (= give-up-reason :max-attempts)
                    (max 1 (- attempt (dec (long max-attempts)))) ; Num tries since max reached
                    (do       attempt))
                  {:max (enc/ms :mins 1)})
                false))

            give-up-reason
            (do
              (when (= give-up-reason :max-attempts)
                (log! client
                  {:level :warn, :id :carmine.mq/max-attempts-reached
                   :data (assoc log-data :message mcontent, :max-attempts max-attempts)}))
              (fin true nil true))

            (= status :success)     (fin true  backoff-ms       false)
            (= status :retry)       (fin false backoff-ms       false)
            (= status :error)       (fin false retry-backoff-ms false)
            :else
            (do
              (fin true nil false) ; For backwards-comp with old API
              (log! client
                {:level :warn, :id :carmine.mq/unexpected-handler-status
                 :data
                 (assoc log-data :message mcontent
                   :handler-result (enc/typed-val result)
                   :handler-status (enc/typed-val status))})))
          (finally
            ;; Deliberately doesn't re-set the interrupt flag: nothing in the
            ;; worker consumes it, and it'd only break the thread's next sleep
            (when @interrupted?_
              (log! client
                {:level :warn, :id :carmine.mq/handler-interrupted
                 :data log-data}))))

        [:handled (if handled? status :gave-up)])

      ;; else
      (do
        (inc-nstat! nstats_ :poll/unexpected)
        (truss/ex-info! "[Carmine/mq] Unexpected poll reply"
          {:reply (enc/typed-val poll-reply)})))))

;;;; Workers

(defn worker-state
  "Returns the state for a new worker.
  `worker-opts` are the worker's final options, `monitor` is the ?(fn [m])
  to call on each poll (usu. the `:monitor` option)."
  [client qname worker-opts monitor]
  (let [nthreads-handler (get worker-opts :nthreads-handler)]
    {:client           client
     :qname            qname
     :worker-opts      worker-opts
     :monitor          monitor
     :running?_        (atom false)
     :closed?_         (atom false)
     :generation_      (atom 0)
     :future-pool      (when (pos-int? nthreads-handler) (enc/future-pool nthreads-handler))
     :worker-futures_  (atom [])
     :handler-threads_ (atom #{})
     :drain-wake_      (atom nil)
     :lock-extender    (when (get worker-opts :lock-auto-extend-max-ms) (lock-extender qname))
     :nstats_          (atom {})

     ;; :sstats-init opts below allow for manual persistent sstats, currently undocumented
     :ssb-queue-size       (stats/summary-stats-buffered {:buffer-size 10000 :sstats-init (get worker-opts :sstats-init/queue-size)})
     :ssb-queueing-time-ms (stats/summary-stats-buffered {:buffer-size 10000 :sstats-init (get worker-opts :sstats-init/queueing-time-ms)})
     :ssb-handling-time-ns (stats/summary-stats-buffered {:buffer-size 10000 :sstats-init (get worker-opts :sstats-init/handling-time-ns)})}))

(defn worker-summary
  "Returns worker's {:keys [qname running? closed? nthreads-handler ?nthreads-worker]}."
  [{:keys [qname worker-opts running?_ closed?_]}]
  (enc/assoc-some
    {:qname    qname
     :running? @running?_, :closed? @closed?_
     :nthreads-handler (get worker-opts :nthreads-handler)}
    :nthreads-worker   (get worker-opts :nthreads-worker)))

(defn worker-status
  "Returns worker's detailed status map, without connection info."
  [{:keys [qname worker-opts running?_ closed?_ nstats_
           ssb-queue-size ssb-queueing-time-ms ssb-handling-time-ns]}]
  {:qname    qname
   :running? @running?_, :closed? @closed?_
   :nthreads
   (enc/assoc-some {:handler (get worker-opts :nthreads-handler)}
     :worker                 (get worker-opts :nthreads-worker))

   :opts worker-opts
   :stats
   {:queue-size       (when-let [ss @ssb-queue-size]       @ss)
    :queueing-time-ms (when-let [ss @ssb-queueing-time-ms] @ss)
    :handling-time-ns (when-let [ss @ssb-handling-time-ns] @ss)
    :counts           @nstats_}})

(defn- drain-handlers!
  "Waits for active handler tasks of a stopped worker. Returns true iff
  drained, false if worker is running (or restarted), called by a handler,
  or timed out."
  [{:keys [client qname worker-opts future-pool running?_ generation_ handler-threads_]
    wake-future_ :drain-wake_}
   ?timeout-ms]
  (enc/cond
    (not future-pool) true
    (contains? @handler-threads_ (Thread/currentThread)) false
    :let [;; Read together, so that a concurrent restart can't slip between
          ?generation (locking running?_ (when-not @running?_ @generation_))]
    (nil? ?generation) false ; Running
    :let [generation ?generation
          nthreads-handler (get worker-opts :nthreads-handler)
          deadline (when ?timeout-ms (+ (System/nanoTime) (* 1000000 (long ?timeout-ms))))
          ;; Wake asynchronously, since a borrow/reply may block (e.g. when
          ;; sleeping handlers hold every pooled connection). At most one
          ;; wake in flight per worker, shared by concurrent/repeated drains.
          wake!
          (fn []
            (locking wake-future_
              (let [f @wake-future_]
                (when (or (nil? f) (realized? f))
                  (enc/catching ; E.g. executor rejection after `shutdown-agents`
                    (reset! wake-future_
                      (future
                        (enc/catching
                          (when (open? client)
                            (wake-sleepers! client qname nthreads-handler))))))))))]
    :else
    ;; Re-signal until drained. Sentinels live on queue-global keys, so
    ;; a single push isn't enough: another worker on the same queue may
    ;; consume them, and a poll that reaches `dequeue` after we push
    ;; will clear them itself before blocking.
    (loop [wait-ms 0] ; Check (and wake) before first wait
      (enc/cond
        (not= generation @generation_) false ; Restarted

        (future-pool wait-ms nil)
        (locking running?_ (and (not @running?_) (= generation @generation_)))

        :let [rem-ms (when deadline (quot (- (long deadline) (System/nanoTime)) 1000000))]
        (and rem-ms (<= (long rem-ms) 0)) false
        :else (do (wake!) (recur (if rem-ms (min 250 (long rem-ms)) 250)))))))

(defn worker-command
  "Implements all worker commands except `:start` and `:stop`."
  ([state cmd]
   (let [{:keys [client qname nstats_ ssb-queue-size ssb-queueing-time-ms ssb-handling-time-ns]} state]
     (case cmd
       :queue-size         (queue-size    client qname)
       :queue-status       (queue-status  client qname)
       :queue-content      (queue-content client qname)
       :queue-mids         (queue-mids    client qname) ; Undocumented
       :queue-clear!!                                   ; Undocumented
       (do
         (queues-clear! client [qname])
         (worker-command state :stats-clear!)
         nil)

       :stats-clear! ; Undocumented
       (do
         (reset! nstats_ {})
         (stats/summary-stats-clear! ssb-queue-size)
         (stats/summary-stats-clear! ssb-queueing-time-ms)
         (stats/summary-stats-clear! ssb-handling-time-ns)
         nil)

       :drain (worker-command state :drain nil) ; Waits for active handler tasks

       (truss/ex-info! "[Carmine/mq] Unexpected queue worker command"
         {:command (enc/typed-val cmd)}))))

  ([state cmd arg]
   (case cmd
     :drain (drain-handlers! state (truss/have [:or nil? nat-int?] arg))
     (truss/ex-info! "[Carmine/mq] Unexpected queue worker command"
       {:command (enc/typed-val cmd), :arg (enc/typed-val arg)}))))

(defn worker-stop!
  "Returns true iff worker was running."
  [{:keys [client qname running?_ generation_ worker-futures_ handler-threads_]}]
  ;; Publish lifecycle transitions and capture their futures under the same
  ;; lock as start/close. Never wait for threads while holding this lock.
  (when-let [worker-futures
             (locking running?_
               (when (compare-and-set! running?_ true false)
                 (swap! generation_ inc)
                 @worker-futures_))]
    (log! client {:level :info, :id :carmine.mq/worker-will-shut-down, :data {:qname qname}})
    (let [called-from-handler? (contains? @handler-threads_ (Thread/currentThread))]
      (when-not called-from-handler?
        (run! deref worker-futures))
      (log! client
        {:level :info, :id :carmine.mq/worker-did-shut-down
         :data {:qname qname, :handlers-drained? (empty? @handler-threads_)
                :called-from-handler? called-from-handler?}}))
    true))

(defn worker-close!
  "Permanently stops worker."
  [{:keys [running?_ closed?_] :as state}]
  (locking running?_
    (reset! closed?_ true))
  (worker-stop! state))

(defn worker-start!
  "Returns true iff worker was started. `worker` is the client's worker
  object, as provided to handler and monitor fns."
  [state worker]
  (let [{:keys [client qname worker-opts monitor running?_ closed?_ generation_ future-pool
                worker-futures_ handler-threads_ nstats_ lock-extender
                ssb-queue-size ssb-queueing-time-ms ssb-handling-time-ns]} state]

    (when-not (open? client)
      (truss/ex-info! "[Carmine/mq] Can't start worker, connection manager is closed"
        {:eid :carmine.mq/manager-closed, :qname qname}))

    (locking running?_
      (when (and (not @closed?_)
                 (pos-int? (get worker-opts :nthreads-handler))
                 (pos-int? (get worker-opts :nthreads-worker 1)) ; Back compatibility
                 (compare-and-set! running?_ false true))
        (log! client {:level :info, :id :carmine.mq/worker-starting, :data {:qname qname}})
        (let [generation (swap! generation_ inc)
              {:keys [handler nthreads-handler]} worker-opts
              qk-ndry-runs (qkey qname :ndry-runs)

              queue-size* (enc/counter 0)

              throttle-ms-fn ; (fn []) -> ?msecs
              (let [{:keys [throttle-ms]} worker-opts
                    as-?pos (fn [x] (when x (when (> (long x) 0) x)))]

                (if (fn? throttle-ms)
                  (do                                  (fn [] (as-?pos (throttle-ms @queue-size*))))
                  (if-let [ms (as-?pos throttle-ms)] (fn [] ms) (fn [] nil))))

              sleep-while-running! ; So that `stop` needn't wait for long sleeps
              (fn [ms]
                (let [deadline (+ (System/nanoTime) (* 1000000 (long ms)))]
                  (loop []
                    (let [remaining (quot (- deadline (System/nanoTime)) 1000000)]
                      (when (and (pos? remaining) @running?_ (= generation @generation_))
                        (Thread/sleep (int (min remaining 100)))
                        (recur))))))

              run-handler-thread!
              (fn [^long thread-idx]
                (let [handler-thread (Thread/currentThread)

                      ;; Consecutive errors may indicate an issue with Redis or
                      ;; handler fn (/ handler's supporting systems). Counted per
                      ;; thread so that backoff doesn't grow with thread count.
                      nconsecutive-errors* (enc/counter 0)
                      loop-error-backoff?_ (atom false)
                      loop-error!
                      (fn [throwable]
                        (if-not (open? client)
                          ;; Terminal: stop (once) without error spam
                          (when (worker-stop! state)
                            (log! client
                              {:level :warn, :id :carmine.mq/manager-closed,
                               :msg "Connection manager closed, worker has stopped"
                               :data {:qname qname}}))

                          (let [^long nce (nconsecutive-errors*)]
                            (log! client
                              {:level :error, :id :carmine.mq/worker-error, :msg "Will backoff and retry",
                               :data {:qname qname, :thread-id thread-idx, :nconsecutive-errors (inc nce)},
                               :error throwable})
                            (reset! loop-error-backoff?_ true))))]

                  (swap! handler-threads_ conj handler-thread)
                  (try
                    (when (> thread-idx 0)
                      (sleep-while-running! (thread-desync-ms 100)))

                    (loop []
                      (when (and @running?_ (= generation @generation_))

                        (when (compare-and-set! loop-error-backoff?_ true false)
                          (let [^long nce @nconsecutive-errors*]
                            (when (> nce 0)
                              (let [backoff-ms
                                    (enc/exp-backoff (min 12 nce)
                                      {:factor (or (throttle-ms-fn) 200)})]

                                (log! client
                                  {:level :error, :id :carmine.mq/worker-backoff,
                                   :msg "Thread will back off due to worker error/s",
                                   :data
                                   {:qname qname, :thread-id thread-idx, :nconsecutive-errors nce,
                                    :backoff-ms backoff-ms}})

                                (sleep-while-running! backoff-ms)))))

                        (when (and @running?_ (= generation @generation_))
                          (let [t0 (System/nanoTime)
                                lease-token (enc/uuid-str)]
                            (try
                              (let [[poll-reply server-time ndry-runs & queue-counts]
                                    (exec client
                                      (into
                                        [(req-dequeue qname worker-opts lease-token)
                                         ["TIME"] ; For handler `:age-ms`
                                         ["GET" qk-ndry-runs]]
                                        (reqs-queue-counts qname)))

                                    {:keys [nqueued]} (-queue-status queue-counts)]

                                (queue-size* :set nqueued)
                                (ssb-queue-size nqueued)

                                (when monitor
                                  (try
                                    (monitor
                                      {:queue-size      nqueued
                                       :ndry-runs       (enc/as-int (or ndry-runs 0))
                                       :poll-reply      poll-reply
                                       :worker          worker})
                                    (catch Throwable t
                                      (inc-nstat! nstats_ :monitor/error)
                                      (log! client
                                        {:level :error, :id :carmine.mq/monitor-error
                                         :data {:qname qname, :thread-id thread-idx}
                                         :error t}))))

                                (handle1 client qname handler poll-reply
                                  {:worker               worker
                                   :queue-size           nqueued
                                   :lease-token          lease-token
                                   :server-udt           (server-udt server-time)
                                   :nstats_              nstats_
                                   :ssb-queueing-time-ms ssb-queueing-time-ms
                                   :ssb-handling-time-ns ssb-handling-time-ns
                                   :sleep-locally!       sleep-while-running!
                                   :lock-extender        lock-extender
                                   :lock-auto-extend-max-ms
                                   (get worker-opts :lock-auto-extend-max-ms)
                                   :max-attempts           (get worker-opts :max-attempts)
                                   :error-retry-backoff-ms (get worker-opts :error-retry-backoff-ms)
                                   :on-give-up             (get worker-opts :on-give-up)})

                                (nconsecutive-errors* :set 0))
                              (catch Throwable t (loop-error! t)))

                            ;; Clear any interrupt flag left by handler, etc. so
                            ;; that it can't break this long-lived thread's sleeps
                            (Thread/interrupted)

                            ;; Min time between starts of this thread's polls, so
                            ;; time spent handling or sleeping counts toward it
                            (when-let [ms (throttle-ms-fn)]
                              (let [elapsed-ms (quot (- (System/nanoTime) t0) 1000000)
                                    ms (- (long ms) elapsed-ms)]
                                (when (pos? ms) (sleep-while-running! ms)))))
                          (recur))))

                    (finally
                      (swap! handler-threads_ disj handler-thread)))))]

          ;; Each handler thread loops over polls for as long as worker is
          ;; running, holding its pool slot throughout. Polls are so never
          ;; made (and leases never acquired) ahead of local handler capacity.
          ;; Slots may still be held by handlers of an earlier run, so threads
          ;; are started as slots become free.
          (reset! worker-futures_
            [(future
               (loop [idx 0]
                 (when (and (< idx (long nthreads-handler)) @running?_ (= generation @generation_))
                   (if (future-pool 100 nil (fn [] (run-handler-thread! idx)))
                     (recur (inc idx))
                     (recur      idx)))))]))

        true))))

(defn monitor-fn
  "Returns a worker monitor fn that warns when queue exceeds the prescribed
  size. A backoff timeout can be provided to rate-limit this warning."
  [log-ns qname max-queue-size warn-backoff-ms]
  (let [qname (enc/as-qname qname)
        udt-last-warning_ (atom 0)]
    (fn [{:keys [queue-size]}]
      (when (> (long queue-size) (long max-queue-size))
        (let [instant (enc/now-udt)
              udt-last-warning (long @udt-last-warning_)]
          (when (> (- instant udt-last-warning) (long (or warn-backoff-ms 0)))
            (when (compare-and-set! udt-last-warning_ udt-last-warning instant)
              (trove/log!
                {:ns log-ns, :level :warn, :id :carmine.mq/monitor-size-warning,
                 :data {:qname qname, :queue-size {:max max-queue-size, :current queue-size}}}))))))))

(defn default-throttle-ms-fn
  "Default/example (fn [queue-size]) -> ?throttle-msecs"
  [queue-size]
  (let [queue-size (long queue-size)]
    (enc/cond
      (> queue-size 256)  50 ; 20/sec * nthreads (~12 secs to clear)
      (> queue-size 128) 100 ; 10/sec * nthreads (~12 secs to clear)
      :else              200 ;  5/sec * nthreads (~25 secs to clear)
      )))

(comment (default-throttle-ms-fn 0))
