(ns taoensso.carmine.tundra
  "Alpha - subject to change (hide the kittens!).
  Semi-automatic datastore layer for Carmine. It's like the magix.

  Redis (2.6+) keys:
    * carmine:tundra:dirty    -> set, dirty Redis keys.
    * carmine:tundra:cleaning -> set, Redis keys currently being frozen
                                 to datastore. Used for crash protection.

  Use multiple Redis instances (recommended) or Redis databases for local key
  namespacing."
  {:author "Peter Taoussanis"}
  (:require [taoensso.carmine       :as car :refer (wcar)]
            [taoensso.carmine.utils :as utils]
            [taoensso.nippy         :as nippy]
            [taoensso.nippy.tools   :as nippy-tools]
            [taoensso.timbre        :as timbre]))

;;;; Private Redis commands

(def ^:private tkey (memoize (partial car/kname "carmine" "tundra")))

(defn- extend-exists
  "Returns 0/1 for each key that doesn't/exist, extending any preexisting TTLs."
  ;; Cluster: no between-key atomicity requirements, can pipeline per shard
  [ttl-ms keys]
  (apply car/eval*
    "local result = {}
     local ttl = tonumber(ARGV[1])
     for i,k in pairs(KEYS) do
       if ttl > 0 and redis.call('ttl', k) > 0 then
         result[i] = redis.call('pexpire', k, ttl)
       else
         result[i] = redis.call('exists', k)
       end
     end
     return result"
    (count keys)
    (conj (vec keys) (or ttl-ms 0))))

(comment (wcar {} (car/ping) (extend-exists nil ["k1" "invalid" "k3"])))

(defn- pexpire-dirty-exists
  "Returns 0/1 for each key that doesn't/exist, setting (/extending) TTLs and
  marking keys as dirty."
  ;; Cluster: no between-key atomicity requirements, can pipeline per shard
  [ttl-ms keys]
  (apply car/eval*
    "local dirty_set = table.remove(KEYS) -- Last script key is set key
     local ttl = tonumber(ARGV[1])
     local result = {}
     for i,k in pairs(KEYS) do
       if ttl > 0 then
         if redis.call('pexpire', k, ttl) > 0 then
           redis.call('sadd', dirty_set, k)
           result[i] = 1
         else
           result[i] = 0
         end
       elseif redis.call('exists', k) > 0 then
         redis.call('sadd', dirty_set, k)
         result[i] = 1
       else
         result[i] = 0
       end
     end
     return result"
    (inc (count keys))
    (conj (vec keys) (tkey :dirty) (or ttl-ms 0))))

(comment (wcar {} (pexpire-dirty-exists nil ["k1" "k2" "k3"])))

;;;; Public interfaces

(defprotocol IDataStore ; Main extension point
  (put-keys   [store keyed-data] "{<k> <frozen-data> ...} -> {<k> <success?> ...}.")
  (fetch-keys [store ks] "[<k> ...] -> {<k> <frozen-data>}."))

(defprotocol IFreezer
  (freeze [freezer x] "Returns datastore-ready key data.")
  (thaw   [freezer x] "Returns Redis-ready key data."))

(defprotocol IWorker
  (start [this] "Returns true iff worker successfully started.")
  (stop  [this] "Returns true iff worker successfully stopped."))

(defprotocol ITundraStore
  (ensure-ks* [store ks])
  (dirty*     [store ks])
  (worker     [store conn opts]
    "Alpha - subject to change.
    Returns a threaded worker to routinely freeze Redis keys marked as dirty
    to datastore and mark successfully frozen keys as clean. Logs any errors.
    THESE ERRORS ARE IMPORTANT: an email or other appropriate notification
    mechanism is HIGHLY RECOMMENDED.

    Options: :frequency-ms - Interval between each freezing."))

(defn ensure-ks
  "Alpha - subject to change.
  BLOCKS to ensure given keys (previously created) are available in Redis,
  fetching them from datastore as necessary. Throws an exception if any keys
  couldn't be made available. Acts as a Redis command: call within a `wcar`
  context."
  [store & ks] (ensure-ks* store ks))

(defn dirty
  "Alpha - subject to change.
  **MARKS GIVEN KEYS FOR EXPIRY** and adds them to dirty set for freezing to
  datastore on worker's next scheduled freeze. Throws an exception if any keys
  don't exist. Acts as a Redis command: call within a `wcar` context.

  ****************************************************************************
  ** Worker MUST be running AND FUNCTIONING CORRECTLY or DATA WILL BE LOST! **
  ****************************************************************************"
  [store & ks] (dirty* store ks))

;;;; Default implementations

(defrecord NippyFreezer [opts]
  IFreezer
  (freeze [_ x]  (nippy/freeze x  opts))
  (thaw   [_ ba] (nippy/thaw   ba opts)))

(def nippy-freezer "Default Nippy Freezer." (NippyFreezer. {}))

(defrecord DiskDataStore [path]
  IDataStore
  (put-keys   [store keyed-data])
  (fetch-keys [store ks]))

(defrecord Worker [conn work-fn running? opts]
  IWorker
  (stop  [_] (let [stopped? @running?] (reset! running? false) stopped?))
  (start [_]
    (when-not @running?
      (reset! running? true)
      (future
        (let [{:keys [frequency-ms]} opts]
          (while @running?
            (try (work-fn)
              (catch Throwable t
                (timbre/fatal t
                 "CRITICAL worker error, shutting down! DATA AT RISK!!")
                (throw t)))
            (when frequency-ms (Thread/sleep frequency-ms)))))
      true)))

(defn- prep-ks [ks] (assert (utils/coll?* ks)) (vec (distinct (map name ks))))
(comment (prep-ks [nil]) ; ex
         (prep-ks [:a "a" :b :foo.bar/baz]))

(defrecord TundraStore [datastore freezer opts]
  ITundraStore
  (ensure-ks* [store ks]
    (let [ks (prep-ks ks)
          {:keys [redis-ttl-ms]} opts
          existance-replies (->> (extend-exists redis-ttl-ms ks)
                                 (car/with-replies)) ; Throws on errors
          missing-ks        (->> (mapv #(when (zero? %2) %1) ks existance-replies)
                                 (filterv identity))]

      (when-not (empty? missing-ks)
        (timbre/trace "Fetching missing keys:" missing-ks)
        (let [keyed-data ; {<redis-key> <thawed-data> ...}
              (let [frozen-data (fetch-keys datastore missing-ks)]
                (if-not freezer frozen-data
                        (utils/map-kvs nil (partial thaw freezer) frozen-data)))

              ;; Restore what we can even if some fetches failed
              restore-replies ; {<redis-key> <restore-reply> ...}
              (->> (doseq [[k data] keyed-data]
                     (if-not (utils/bytes? data)
                       (car/return (Exception. "Malformed fetch data"))
                       (car/restore k (or redis-ttl-ms 0) (car/raw data))))
                   (car/with-replies :as-pipeline) ; ["OK" "OK" ...]
                   (zipmap (keys keyed-data)))

              errors ; {<redis-key> <error> ...}
              (reduce
               (fn [m k]
                 (if-not (contains? keyed-data k)
                   (assoc m k "Fetch failed")
                   (let [^Exception rr (restore-replies k)]
                     (if (or (not (instance? Exception rr))
                             ;; Already restored:
                             (= (.getMessage rr) "ERR Target key name is busy."))
                       m (assoc m k (.getMessage rr))))))
               (sorted-map) missing-ks)]

          (when-not (empty? errors)
            (let [ex (ex-info "Failed to ensure some key(s)" errors)]
              (timbre/error ex) (throw ex)))
          nil))))

  (dirty* [store ks]
    (let [ks (prep-ks ks)
          {:keys [redis-ttl-ms]} opts
          existance-replies (->> (pexpire-dirty-exists redis-ttl-ms ks)
                                 (car/with-replies)) ; Throws on errors
          missing-ks        (->> (mapv #(when (zero? %2) %1) ks existance-replies)
                                 (filterv identity))]
      (when-not (empty? missing-ks)
        (let [ex (ex-info "Some key(s) were missing" {:missing-ks missing-ks})]
          (timbre/error ex) (throw ex)))
      nil))

  (worker [store conn wopts]
    (let [{:keys [frequency-ms auto-start?]
           :or   {frequency-ms (* 1000 60 60) ; Once/hr
                  auto-start?  true}} wopts

          work-fn
          (fn []
            (timbre/trace "Worker job running:" wopts)
            (let [kdset (tkey :dirty)
                  kcset (tkey :cleaning)
                  [ks-dirty ks-cleaning] (wcar {} (car/smembers kdset)
                                                  (car/smembers kcset))

                  ;; Grab ks from both dirty AND cleaning sets (the
                  ;; latter will be empty unless prev work-fn failed):
                  ks-to-freeze (vec (into (set ks-dirty) ks-cleaning))]

              (when-not (empty? ks-to-freeze)
                (timbre/trace "Freezing keys:" ks-to-freeze)
                (wcar {} (mapv (partial car/smove kdset kcset) ks-to-freeze))
                (let [dumps ; [<raw-data-or-nil> ...]
                      (wcar {} (car/parse-raw (mapv car/dump ks-to-freeze)))

                      keyed-data ; {<existing-redis-key> <frozen-data> ...}
                      (let [thawed-data
                            (->> (mapv vector ks-to-freeze dumps)
                                 (reduce (fn [m [k dump]]
                                           (if-not dump m (assoc m k dump))) {}))
                            thawed-data
                            (if-not freezer thawed-data
                                    (utils/map-kvs nil (partial freeze freezer)
                                                   thawed-data))]
                        thawed-data)]

                  (when-not (empty? keyed-data)
                    (let [put-replies ; {<redis-key> <success?> ...}
                          (try (put-keys datastore keyed-data)
                               (catch Exception _ nil))

                          ks-succeeded (->> (keys put-replies)
                                            (filterv put-replies))
                          ks-failed    (vec (reduce disj (set ks-to-freeze)
                                                    ks-succeeded))]

                      (wcar {} (mapv (partial car/srem kcset) ks-succeeded))
                      (when-not (empty? ks-failed) ; Don't rethrow!
                        (let [ex (ex-info "Failed to freeze some key(s)"
                                          {:failed-ks ks-failed})]
                          (timbre/error ex)))))))))

          w (Worker. conn work-fn (atom false) {:frequency-ms frequency-ms})]
      (when auto-start? (start w)) w)))

(defn tundra-store
  "Alpha - subject to change.
  Returns a TundraStore with options:
    datastore     - Storage for frozen key data.
                    See `taoensso.carmine.tundra.faraday/faraday-datastore` for
                    Faraday (DynamoDB) datastore.
    :redis-ttl-ms - Optional. Time after which frozen, inactive keys will be
                    EVICTED FROM REDIS. Defaults to ~31 days, minimum 10 hours.
    :freezer      - Optional. Preps key data to/from datastore. May provide
                    services like compression and encryption, etc. Defaults to
                    Nippy with default options (Snappy compression and no
                    encryption).

  See `ensure-ks`, `dirty`, `worker` for TundraStore API."
  [datastore & [{:keys [redis-ttl-ms freezer]
                 :or   {redis-ttl-ms (* 1000 60 60 24 31) ; Expire ~once/month
                        freezer nippy-freezer}}]]

  (assert (or (nil? freezer) (satisfies? IFreezer freezer)))
  (assert (satisfies? IDataStore datastore))
  (assert (or (nil? redis-ttl-ms) (>= redis-ttl-ms (* 1000 60 60 10)))
          (str "Bad TTL (< 10 hours): " redis-ttl-ms))

  (TundraStore. datastore freezer {:redis-ttl-ms redis-ttl-ms}))

(comment ; README

(require '[taoensso.carmine.tundra :as tundra :refer (ensure-ks dirty)])
(require '[taoensso.carmine.tundra.faraday :as tfar])
(defmacro wcar* [& body] `(wcar {} ~@body))
(timbre/set-level! :trace)

(def creds {:access-key "<AWS_DYNAMODB_ACCESS_KEY>"
            :secret-key "<AWS_DYNAMODB_SECRET_KEY>"}) ; AWS IAM credentials

;; Create a DynamoDB table for key data storage (this can take ~2m):
(tfar/ensure-table creds {:throughput {:read 1 :write 1} :block? true})

;; Create a TundraStore backed by the above table:
(def tstore
  (tundra-store
   (tfar/faraday-datastore creds
     {:key-ns :my-app.production             ; For multiple apps/envs per table
      :auto-write-units {0 1, 100 2, 200, 4} ; For automatic write-throughput scaling
      })

   {:freezer      nippy-freezer ; Use Nippy for compression/encryption
    :redis-ttl-ms (* 1000 60 60 24 31) ; Evict cold keys after one month
    }))

;; Create a threaded worker to freeze dirty keys every hour:
(def tundra-worker
  (worker tstore {:pool {} :spec {}} ; Redis connection
    {:frequency-ms (* 1000 60 60)}))

;; Let's create some new evictable keys:
(wcar* (car/mset :k1 0 :k2 0 :k3 0)
       (dirty tstore [:k1 :k2 :k3]))

;; Now imagine time passes and some keys get evicted:
(wcar* (car/del :k1 :k3))

;; And now we want to use our evictable keys:
(wcar*
 (ensure-ks tstore :k1 :k2 :k3) ; Ensures previously-created keys are available
 (car/mget :k1 :k2 :k3)         ; Gets their current value
 (mapv car/incr [:k1 :k3])      ; Modifies them
 (dirty tstore :k1 :k3)         ; Marks them for later refreezing by worker
 )

)