(ns taoensso.carmine.message-queue
  "Carmine-backed Clojure message queue, v2.
  All heavy lifting by Redis.

  Uses an optimized message circle architecture that is simple, reliable,
  and has pretty good throughput and latency.

  See `mq-diagram.svg` in repo for diagram of architecture,
  Ref. http://antirez.com/post/250 for initial inspiration.

  Message status e/o:
    :nil                 - Not in queue or already GC'd
    :queued              - Awaiting handler
    :queued-with-backoff - Awaiting handler, but skip until backoff expired
    :locked              - Currently with handler
    :locked-with-requeue - Currently with handler, will requeue when done
    :done-awaiting-gc    - Finished handling, awaiting GC
    :done-with-backoff   - Finished handling, awaiting GC,
                           but skip until dedupe backoff expired
    :done-with-requeue   - Will requeue, but skip until dedupe backoff expired

  Redis keys (all prefixed with `carmine:mq:<qname>:`):
    * messages      - hash: {mid mcontent} ; Message content
    * messages-rq   - hash: {mid mcontent} ; '' for requeues
    * lock-times    - hash: {mid lock-ms}  ; Optional mid-specific lock duration
    * lock-times-rq - hash: {mid lock-ms}  ; '' for requeues
    * udts          - hash: {mid  udt-first-enqueued}
    * locks         - hash: {mid    lock-expiry-time} ; Active locks
    * backoffs      - hash: {mid backoff-expiry-time} ; Active backoffs
    * nattempts     - hash: {mid attempt-count}
    * done          - mid set: awaiting gc, etc.
    * requeue       - mid set: awaiting requeue ; Deprecated

    * mids-ready    - list: mids for immediate handling     (push to left, pop from right)
    * mid-circle    - list: mids for maintenance processing (push to left, pop from right)
    * ndry-runs     - int: num times worker(s) have lapped queue w/o work to do"

  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string   :as str]
   [taoensso.encore  :as enc]
   [taoensso.carmine :as car :refer [wcar]]
   [taoensso.timbre  :as timbre]
   [taoensso.tukey   :as tukey]))

;;;; TODO/later
;; - New docs + examples in v4 Wiki.
;; - Support cbs? (Could decouple Timbre)

;;;; Utils

(enc/defalias exp-backoff enc/exp-backoff)

(comment (mapv #(exp-backoff % {}) (range 5)))

(let [cluster-support? false] ; TODO Cluster support
  (def ^:private qkey
    (enc/fmemoize
      (fn [qname k]
        (car/key :carmine :mq
          (if cluster-support?
            (str "{" (enc/as-qname qname) "}")
            (do                    qname))
          k)))))

(comment (enc/qb 1e6 (qkey :qname :qk)))

;;;; Admin

(defn queue-names
  "Returns a non-empty set of existing queue names, or nil."
  ([conn-opts        ] (queue-names conn-opts "*"))
  ([conn-opts pattern]
   (when-let [qks (not-empty (car/scan-keys conn-opts (qkey pattern)))]
     (let [qk-prefix-len (inc (count (qkey)))] ; "carmine:mq:", etc.
       (into #{} (map #(enc/get-substr-by-idx % qk-prefix-len)) qks)))))

(comment (queue-names {}))

(defn clear-queues
  "Deletes ALL content for the Carmine message queues with given names."
  {:arglists '([conn-opts qnames])}
  [conn-opts & more]
  (let [qnames ; Back compatibility
        (when-let [[x1] more]
          (if (coll? x1) ; Common case (new API)
            x1
            more))]

    (when (seq qnames)
      (wcar conn-opts
        (enc/run!
          (fn [qname]
            (when qname
              (let [qk (partial qkey qname)]
                (car/del
                  (qk :messages)
                  (qk :messages-rq)
                  (qk :lock-times)
                  (qk :lock-times-rq)
                  (qk :udts)
                  (qk :locks)
                  (qk :backoffs)
                  (qk :nattempts)
                  (qk :done)
                  (qk :requeue)
                  (qk :mids-ready)
                  (qk :mid-circle)
                  (qk :ndry-runs)))))
          qnames)))))

(defn clear-all-queues
  "Deletes ALL content for ALL Carmine message queues and returns a
  non-empty vector of the queue names that were cleared, or nil."
  [conn-opts]
  (when-let [qnames (queue-names conn-opts "*")]
    (clear-queues conn-opts qnames)
    (do                     qnames)))

(defn- kvs->map [kvs]
  (if (empty? kvs)
    {}
    (persistent! (enc/reduce-kvs assoc! (transient {}) kvs))))

(defn- ->message-status
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

(comment        (->message-status ["queued" "1" nil]))
(comment (wcar {} (message-status "qname" "mid1")))

(defn queue-status
  "Returns a detailed status map for given named queue.
  Expensive, O(n-items-in-queue) - avoid use in production."
  ([conn-opts qname     ] (queue-status conn-opts qname nil))
  ([conn-opts qname opts]
   (let [now (enc/now-udt)
         qk  (partial qkey qname)
         {:keys [incl-legacy-data?]
          :or   {incl-legacy-data? true}} opts

         m
         (zipmap
           [:messages :messages-rq
            :lock-times :lock-times-rq
            :udts :locks :backoffs :nattempts
            :done :requeue :ndry-runs
            :mids-ready :mid-circle]

           (wcar conn-opts
             (car/parse kvs->map
               (car/hgetall (qk :messages))
               (car/hgetall (qk :messages-rq))
               (car/hgetall (qk :lock-times))
               (car/hgetall (qk :lock-times-rq))
               (car/hgetall (qk :udts))
               (car/hgetall (qk :locks))
               (car/hgetall (qk :backoffs))
               (car/hgetall (qk :nattempts)))

             (->> (car/smembers (qk :done))      (car/parse set))
             (->> (car/smembers (qk :requeue))   (car/parse set))
             (->> (car/get      (qk :ndry-runs)) (car/parse-int))
             (do  (car/lrange   (qk :mids-ready) 0 -1))
             (do  (car/lrange   (qk :mid-circle) 0 -1))))

         {:keys [messages messages-rq
                 lock-times lock-times-rq
                 udts locks backoffs nattempts
                 done requeue
                 mids-ready mid-circle]} m]

     (assoc
       (if incl-legacy-data?
         (do          m)
         (select-keys m [:mids-ready :mid-circle :ndry-runs]))

       :last-mid   (first mid-circle)
       :next-mid   (or (peek  mids-ready) (peek  mid-circle))
       :queue-size (+  (count mids-ready) (count mid-circle))

       :by-mid ; {<mid> {:keys [message status ...]}}
       (persistent!
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
           (do        messages)))))))

;;;; Implementation

(do ; Lua scripts
  (def lua-msg-status_ (delay (enc/have (enc/slurp-resource "taoensso/carmine/lua/mq/msg-status.lua"))))
  (def lua-enqueue_    (delay (enc/have (enc/slurp-resource "taoensso/carmine/lua/mq/enqueue.lua"))))
  (def lua-dequeue_    (delay (enc/have (enc/slurp-resource "taoensso/carmine/lua/mq/dequeue.lua")))))

(defn message-status
  "Returns current message status, e/o:
      nil                  - Not in queue or already GC'd
      :queued              - Awaiting handler
      :queued-with-backoff - Awaiting handler, but skip until backoff expired
      :locked              - Currently with handler
      :locked-with-requeue - Currently with handler, will requeue when done
      :done-awaiting-gc    - Finished handling, awaiting GC
      :done-with-backoff   - Finished handling, awaiting GC,
                             but skip until dedupe backoff expired
      :done-with-requeue   - Will requeue, but skip until dedupe backoff expired"
  [qname mid]
  (car/parse ->message-status
    (car/lua @lua-msg-status_
      {:qk-messages    (qkey qname :messages)
       :qk-messages-rq (qkey qname :messages-rq)
       :qk-locks       (qkey qname :locks)
       :qk-backoffs    (qkey qname :backoffs)
       :qk-done        (qkey qname :done)
       :qk-requeue     (qkey qname :requeue)}
      {:now (enc/now-udt)
       :mid mid})))

(defn enqueue
  "Pushes given message (any Clojure data type) to named queue and returns
    - {:keys [mid action]} on success ; action e/o #{:added :updated}
    - {:keys [error]}      on error   ; error  e/o #{:already-queued :locked :backoff}

    Options:
      :init-backoff-ms - Optional initial backoff in msecs.
      :lock-ms         - Optional lock time in msecs. When unspecified, the
                         worker's default lock time will be used.

      :mid             - Optional unique message id (e.g. message hash) to
                         identify a specific message for dedupe/update/requeue.
                         When unspecified, a random uuid will be used.

      :can-update?     - When true, will update message content and/or lock-ms for
                         an mid still awaiting handling.
      :can-requeue?    - When true, will mark message with `:locked` or
                         `:done-with-backoff` status so that it will be
                         automatically requeued after garbage collection."

  {:arglists
   '([qname message]
     [qname message {:keys [init-backoff-ms lock-ms
                            mid can-update? can-requeue?]}])}

  [qname message & more]
  (let [opts ; Back compatibility: [a b & [c d]] -> [a b ?{:keys [c d]}]
        (when-let [[x1 x2] more]
          (if (map? x1) ; Common case (new API)
            x1
            {:mid          x1
             :can-requeue? x2}))

        parse-fn
        (fn [mid reply]
          (enc/cond
            :let [[action error] (when (vector? reply) reply)]
            error  {:error  (keyword error)}
            action {:action (keyword action), :mid mid}
            :else  {:error  :unknown}))

        {:keys [init-backoff-ms lock-ms
                mid can-update? can-requeue?]}
        opts

        ;;; Back compatibility
        mid             (or mid             (get opts :unique-message-id))
        init-backoff-ms (or init-backoff-ms (get opts :initial-backoff-ms))
        can-requeue?    (or can-requeue?    (get opts :allow-requeue?))

        mid (or mid (enc/uuid-str))]

    (car/parse
      (partial parse-fn mid)
      (car/lua @lua-enqueue_
        {:qk-messages      (qkey qname :messages)
         :qk-messages-rq   (qkey qname :messages-rq)
         :qk-lock-times    (qkey qname :lock-times)
         :qk-lock-times-rq (qkey qname :lock-times-rq)
         :qk-udts          (qkey qname :udts)
         :qk-locks         (qkey qname :locks)
         :qk-backoffs      (qkey qname :backoffs)
         :qk-nattempts     (qkey qname :nattempts)
         :qk-done          (qkey qname :done)
         :qk-requeue       (qkey qname :requeue)
         :qk-mids-ready    (qkey qname :mids-ready)
         :qk-mid-circle    (qkey qname :mid-circle)}

        {:now      (enc/now-udt)
         :mid      mid
         :mcnt     (car/freeze message)
         :foof     "1"
         :can-upd? (if can-update?    "1" "0")
         :can-rq?  (if can-requeue?   "1" "0")
         :init-bo  (or init-backoff-ms 0)
         :lock-ms  (or lock-ms        -1)}))))

(defn- dequeue
  "Processes next mid and returns:
    - [\"skip\"   <reason>]                                   ; Worker thread should skip
    - [\"sleep\"  <reason> <msecs>]                           ; Worker thread should sleep
    - [\"handle\" <mid> <mcontent> <attempt> <lock-ms> <udt>] ; Worker thread should handle"
  [qname
   {:keys [default-lock-ms eoq-backoff-ms]
    :or   {default-lock-ms (enc/ms :mins 60)
           eoq-backoff-ms  exp-backoff}}]

  (let [;; Precompute 5 backoffs so that `dequeue.lua` can init the backoff atomically
        [bo1 bo2 bo3 bo4 bo5]
        (cond
          (fn?      eoq-backoff-ms) (mapv     eoq-backoff-ms (range 5))
          (integer? eoq-backoff-ms) (repeat 5 eoq-backoff-ms)
          :else
          (throw
            (ex-info
              (str "[Carmine/mq] Unexpected `eoq-backoff-ms` arg: " eoq-backoff-ms)
              {:arg {:value eoq-backoff-ms :type (type eoq-backoff-ms)}})))]

    (car/lua @lua-dequeue_
      {:qk-messages      (qkey qname :messages)
       :qk-messages-rq   (qkey qname :messages-rq)
       :qk-lock-times    (qkey qname :lock-times)
       :qk-lock-times-rq (qkey qname :lock-times-rq)
       :qk-udts          (qkey qname :udts)
       :qk-locks         (qkey qname :locks)
       :qk-backoffs      (qkey qname :backoffs)
       :qk-nattempts     (qkey qname :nattempts)
       :qk-done          (qkey qname :done)
       :qk-requeue       (qkey qname :requeue)
       :qk-mids-ready    (qkey qname :mids-ready)
       :qk-mid-circle    (qkey qname :mid-circle)
       :qk-ndry-runs     (qkey qname :ndry-runs)}

      {:now              (enc/now-udt)
       :default-lock-ms  default-lock-ms
       :eoq-bo1          bo1
       :eoq-bo2          bo2
       :eoq-bo3          bo3
       :eoq-bo4          bo4
       :eoq-bo5          bo5})))

(comment
  (clear-queues {} :q1)
  (queue-status {} :q1)
  (wcar {} (enqueue :q1 :msg1 :mid1))
  (wcar {} (message-status :q1 :mid1))
  (wcar {} (dequeue :q1 {})))

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

(defn- handle1
  [conn-opts qname handler poll-reply nstats_]
  (enc/cond
    :let [[kind] (when (vector? poll-reply) poll-reply)]

    (= kind "skip")
    (let [[_kind reason] poll-reply]
      #_(inc-nstat! nstats_ (keyword "skip" reason)) ; Noisy
      [:skipped reason])

    (= kind "handle")
    (let [[_kind mid mcontent attempt lock-ms udt] poll-reply
          qk (partial qkey qname)

          age-ms
          (when-let [udt (enc/as-?udt udt)]
            (- (enc/now-udt) ^long udt))

          result
          (try
            (handler
              {:qname   qname    :mid     mid
               :message mcontent :attempt attempt
               :lock-ms lock-ms  :age-ms  age-ms})
            (catch Throwable t
              {:status :error :throwable t}))

          {:keys [status throwable backoff-ms]}
          (when (map? result) result)

          fin
          (fn [mid status done? backoff-ms]
            (let [done? (case status (:success :error) true false)]

              (do              (inc-nstat! nstats_ (keyword "handler" (name status))))
              (when backoff-ms (inc-nstat! nstats_ :handler/backoff))

              ;; Don't need atomicity here, simple pipeline sufficient
              (wcar conn-opts
                (when backoff-ms ; Possible done/retry backoff
                  (car/hset (qk :backoffs) mid
                    (+ (enc/now-udt) (long backoff-ms))))

                (when done? (car/sadd (qk :done)  mid))
                (do         (car/hdel (qk :locks) mid)))))]

      (case status
        :success (fin mid :success true  backoff-ms)
        :retry   (fin mid :retry   false backoff-ms)
        :error
        (do
          (fin mid :error true nil)
          (timbre/error
            (ex-info "[Carmine/mq] Handler returned `:error` status"
              {:qname qname, :mid mid, :attempt attempt, :message mcontent}
              throwable)
            "[Carmine/mq] Handler returned `:error` status"
            {:qname qname, :mid mid, :backoff-ms backoff-ms}))

        (do
          (fin mid :success true nil) ; For backwards-comp with old API
          (timbre/warn "[Carmine/mq] Handler returned unexpected status"
            {:qname qname, :mid mid, :attempt attempt, :message mcontent,
             :handler-result {:value result :type (type result)}
             :handler-status {:value status :type (type status)}})))
      [:handled status])

    (= kind "sleep")
    (let [[_kind reason ttl-ms] poll-reply
          ttl-ms (thread-desync-ms (long ttl-ms))]

      (inc-nstat! nstats_ (keyword "poll" reason))
      (Thread/sleep (int ttl-ms))
      [:slept reason ttl-ms])

    :else
    (do
      (inc-nstat! nstats_ :poll/unexpected)
      (throw
        (ex-info "[Carmine/mq] Unexpected poll reply"
          {:reply {:value poll-reply :type (type poll-reply)}})))))

;;;; Workers

(defprotocol IWorker
  "Implementation detail."
  (start [this])
  (stop  [this]))

(deftype CarmineMessageQueueWorker
  [qname opts conn-opts running?_ thread-futures_ nstats_ ssb]

  java.io.Closeable (close [this] (stop this))
  Object
  (toString [this] ; "CarmineMessageQueueWorker[nthreads=1, running]"
    (str "CarmineMessageQueueWorker["
      "nthreads=" (count @thread-futures_) ", "
      (if @running?_ "running" "shut down") "]"))

  clojure.lang.IDeref
  (deref [this]
    {:qname     qname
     :nthreads  (count @thread-futures_)
     :running?  @running?_
     :conn-opts conn-opts
     :opts      opts
     :stats
     {:queue-size (when-let [ss @ssb] @ss)
      :counts     @nstats_}

     :queue-status_
     (delay
       (queue-status conn-opts qname
         {:incl-legacy-data? false}))})

  IWorker
  (stop [_]
    (when (compare-and-set! running?_ true false)
      (timbre/info "[Carmine/mq] Queue worker shutting down" {:qname qname})
      (run! deref @thread-futures_)
      (timbre/info "[Carmine/mq] Queue worker has shut down" {:qname qname})
      true))

  (start [this]
    (when (compare-and-set! running?_ false true)
      (timbre/info "[Carmine/mq] Queue worker starting" {:qname qname})
      (let [{:keys [handler monitor nthreads throttle-ms]} opts
            qk (partial qkey qname)
            throttle-ms
            (when (and throttle-ms (> (long throttle-ms) 0))
              throttle-ms)

            start-polling-loop!
            (fn [^long thread-idx]
              (when (> thread-idx 0)
                (Thread/sleep (int (thread-desync-ms (or throttle-ms 100)))))

              (loop [nloops 0, nconsecutive-errors 0]
                (when @running?_
                  (let [ex
                        (try
                          (let [-resp
                                (wcar conn-opts
                                  (dequeue qname opts)
                                  (car/get  (qk :ndry-runs))
                                  (car/llen (qk :mids-ready))
                                  (car/llen (qk :mid-circle)))]

                            (if-let [t (enc/rfirst #(instance? Throwable %) -resp)]
                              (throw t)
                              (let [[poll-reply ndry-runs mids-ready-size mid-circle-size] -resp
                                    queue-size
                                    (+
                                      (long mids-ready-size)
                                      (long mid-circle-size))]

                                (ssb queue-size) ; -> summary-stats-buffered

                                (when monitor
                                  (monitor
                                    {:queue-size      queue-size
                                     :mid-circle-size queue-size ; Back compatibility
                                     :ndry-runs       (or ndry-runs 0)
                                     :poll-reply      poll-reply
                                     :worker          this}))

                                (handle1 conn-opts qname handler poll-reply nstats_)
                                nil ; Successful loop
                                )))
                          (catch Throwable t t))]

                    (if ex
                      (let [nce        (inc nconsecutive-errors)
                            backoff-ms (exp-backoff (min 12 nce) {:factor (or throttle-ms 200)})]
                        (timbre/error ex "[Carmine/mq] Worker error, will backoff & retry."
                          {:qname qname, :backoff-ms backoff-ms, :nconsecutive-errors nce})

                        (Thread/sleep (int backoff-ms))
                        (recur (inc nloops) nce))

                      (do
                        (when throttle-ms (Thread/sleep (int throttle-ms)))
                        (recur (inc nloops) nconsecutive-errors)))))))]

        (reset! thread-futures_
          (enc/reduce-n
            (fn [v idx] (conj v (future (start-polling-loop! idx))))
            [] nthreads)))

      true)))

(let [ns *ns*]
  (defmethod print-method CarmineMessageQueueWorker
    [x ^java.io.Writer w] (.write w (str "#" ns "." x))))

(defn worker? [x] (instance? CarmineMessageQueueWorker x))

(defn monitor-fn
  "Returns a worker monitor fn that warns when queue exceeds the prescribed
  size. A backoff timeout can be provided to rate-limit this warning."
  [qname max-queue-size warn-backoff-ms]
  (let [udt-last-warning_ (atom 0)]
    (fn [{:keys [queue-size]}]
      (when (> (long queue-size) (long max-queue-size))
        (let [instant (enc/now-udt)
              udt-last-warning (long @udt-last-warning_)]
          (when (> (- instant udt-last-warning) (long (or warn-backoff-ms 0)))
            (when (compare-and-set! udt-last-warning_ udt-last-warning instant)
              (timbre/warn "[Carmine/mq] Message queue monitor-fn size warning"
                {:qname qname, :queue-size {:max max-queue-size, :current queue-size}}))))))))

(defn worker
  "Returns a stateful threaded CarmineMessageQueueWorker to handle messages
  added to named queue with `enqueue`.

  Options:
    :handler        - (fn [{:keys [qname mid message attempt]}]) that throws
                      or returns {:status     <#{:success :error :retry}>
                                  :throwable  <Throwable>
                                  :backoff-ms <retry-or-dedupe-backoff-ms}.
    :monitor        - (fn [{:keys [queue-size ndry-runs poll-reply]}])
                      called on each worker loop iteration. Useful for queue
                      monitoring/logging. See also `monitor-fn`.
    :lock-ms        - Default time that handler may keep a message before handler
                      considered fatally stalled and message is re-queued. Must be
                      sufficiently high to prevent double handling. Can be
                      overridden on a per-message basis via `enqueue`.
    :eoq-backoff-ms - Thread sleep period each time end of queue is reached.
                      Can be a (fn [ndry-runs]) => msecs for n<=5.
                      Sleep synchronized for all queue workers.
    :nthreads       - Number of worker threads to use.
    :throttle-ms    - Thread sleep period between each poll."

  ([conn-opts qname] (worker conn-opts qname nil))
  ([conn-opts qname
    {:keys [handler monitor lock-ms eoq-backoff-ms nthreads
            throttle-ms auto-start] :as worker-opts
     :or   {handler (fn [m] (timbre/info m) {:status :success})
            monitor (monitor-fn qname 1000 (enc/ms :hours 6))
            lock-ms        (enc/ms :mins 60)
            nthreads       1
            throttle-ms    200
            eoq-backoff-ms exp-backoff
            auto-start     true}}]

   (let [worker-opts
         (conj (or worker-opts {})
           {:handler         handler
            :monitor         monitor
            :default-lock-ms lock-ms
            :eoq-backoff-ms  eoq-backoff-ms
            :nthreads        nthreads
            :throttle-ms     throttle-ms})

         w
         (CarmineMessageQueueWorker.
           qname worker-opts conn-opts (atom false) (atom []) (atom {})
           (let [ssb (tukey/summary-stats-buffered {:buffer-size 10000})]
             (ssb 0)
             ssb))

         ;; Back compatibility
         auto-start (get worker-opts :auto-start? auto-start)]

     (when auto-start
       (if (integer? auto-start) ; Undocumented
         (future (Thread/sleep (int auto-start)) (start w))
         (do                                     (start w))))

     w)))

;;;; Deprecated

(enc/deprecated
  (defn ^:deprecated make-dequeue-worker
    "DEPRECATED: Use `worker` instead."
    [pool spec & {:keys [handler-fn handler-ttl-msecs backoff-msecs throttle-msecs
                         auto-start?]}]
    (worker {:pool pool :spec spec}
      (merge (when-let [ms handler-ttl-msecs] {:lock-ms        ms})
        (when-let [ms backoff-msecs]          {:eoq-backoff-ms ms})
        (when-let [ms throttle-msecs]         {:throttle-ms    ms})
        (when-let [hf handler-fn]
          {:handler (fn [{:keys [message]}]
                      {:status (or (#{:success :error :retry} (hf message))
                                 :success)})})
        {:auto-start? auto-start?}))))
