(ns taoensso.carmine.tundra
  "Alpha - subject to change (hide the kittens!).
  Semi-automatic datastore layer for Carmine. It's like the magix. Redis 2.6+.

  Redis keys:
    * carmine:tundra:<worker>:dirty    -> set, dirty Redis keys.
    * carmine:tundra:<worker>:cleaning -> set, Redis keys currently being frozen
                                          to datastore. Used for crash protection."
  {:author "Peter Taoussanis"}
  (:refer-clojure :exclude [ensure])
  (:require [taoensso.carmine       :as car :refer (wcar)]
            [taoensso.carmine.utils :as utils]
            [taoensso.nippy         :as nippy]
            [taoensso.nippy.tools   :as nippy-tools]
            [taoensso.timbre        :as timbre]))

;;;; TODO
;; * TStore opts should include a (mandatory) datastore key prefix for
;;   dev-modes, multiple apps, etc.
;; * README docs.
;; * Tests.
;; * Worker option to keep snapshot of previous n atomic vals? Would need to
;;   add a pair of IDataStore fns to move/snapshot & to manually fetch ks.

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

(comment (wcar {} (car/ping) (extend-exists nil ["k1" "k2" "k2"]) (car/ping)))

(defn- pexpire-dirty-exists
  "Returns 0/1 for each key that doesn't/exist, setting (/extending) TTLs and
  marking keys as dirty."
  ;; Cluster: no between-key atomicity requirements, can pipeline per shard
  [worker ttl-ms keys]
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
    (conj (vec keys) (tkey worker :dirty) (or ttl-ms 0))))

(comment (wcar {} (pexpire-dirty-exists "worker" nil ["k1" "k2" "k3"])))

;;;; Public interfaces

(defprotocol IFreezer
  (freeze [freezer x] "Returns datastore-ready key content.")
  (thaw   [freezer x] "Returns Redis-ready key content."))

(defprotocol IDataStore ; Main extension point
  (put-keys   [store keyed-content] "{<k> <frozen-cnt> ...} -> {<k> <success?> ...}.")
  (fetch-keys [store ks] "[<k> ...] -> {<k> <frozen-cnt>}."))

(defprotocol IWorker
  (start [this] "Returns true iff worker successfully started.")
  (stop  [this] "Returns true iff worker successfully stopped."))

(defprotocol ITundraStore
  (ensure [store ks]
    "Alpha - subject to change.
    BLOCKS to ensure given keys (previously created) are available in Redis,
    fetching them from datastore as necessary. Throws an exception if any keys
    couldn't be made available.

    Acts as a Redis command: call within a `wcar` context.")

  (dirty [store ks] [store worker ks]
    "Alpha - subject to change.
    **MARKS GIVEN KEYS FOR EXPIRY** and adds them to a named worker's dirty
    set for freezing to datastore on worker's next scheduled freeze. Throws an
    exception if any keys don't exist.

    ****************************************************************************
    Named worker MUST be running AND FUNCTIONING CORRECTLY or DATA WILL BE LOST!
    ****************************************************************************

    Acts as a Redis command: call within a `wcar` context.")

  (worker [store conn opts]
    "Alpha - subject to change.
    Returns a threaded worker to routinely freeze Redis keys marked as dirty
    to datastore.

    Because a key will be dirtied at _most_ once for any number of local edits
    since last freezing, we get full local write performance along with a knob
    that allows us to balance local/datastore consistency with any costs that
    may be involved (e.g. performance or tangible data transfer costs).

    Marks successfully frozen keys as clean. Logs any errors - THESE ARE
    IMPORTANT: an email or other appropriate notification mechanism is HIGHLY
    RECOMMENDED.

    Options:
      :name         - Allows multiple workers & dirty key sets.
      :frequency-ms - Interval between each freezing."))

;;;; Default implementations

(defrecord NippyFreezer [opts]
  IFreezer
  (freeze [_ x]  (nippy/freeze x  opts))
  (thaw   [_ ba] (nippy/thaw   ba opts)))

(def nippy-freezer "Default Nippy Freezer." (NippyFreezer. {}))

(defrecord DiskDataStore [path]
  IDataStore
  (put-keys   [store keyed-content])
  (fetch-keys [store ks]))

(defrecord Worker [conn wname work-fn running? opts]
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

(defrecord TundraStore [freezer datastore opts]
  ITundraStore
  (ensure [store ks]
    (let [ks (prep-ks ks)
          {:keys [redis-ttl-ms]} opts
          existance-replies (->> (extend-exists redis-ttl-ms ks)
                                 (car/with-replies)) ; Throws on errors
          missing-ks        (->> (mapv #(when (zero? %2) %1) ks existance-replies)
                                 (filterv identity))]

      (when-not (empty? missing-ks)
        (timbre/trace "Fetching missing keys: " missing-ks)
        (let [keyed-content ; {<redis-key> <thawed-content> ...}
              (let [frozen-cnt (fetch-keys datastore missing-ks)]
                (if-not freezer frozen-cnt
                        (utils/map-kvs nil (partial thaw freezer) frozen-cnt)))

              ;; Restore what we can even if some fetches failed
              restore-replies ; {<redis-key> <restore-reply> ...}
              (->> (doseq [[k cnt] keyed-content]
                     (if-not (utils/bytes? cnt)
                       (car/return (Exception. "Malformed fetch content"))
                       (car/restore k (or redis-ttl-ms 0) (car/raw cnt))))
                   (car/with-replies :as-pipeline) ; ["OK" "OK" ...]
                   (zipmap (keys keyed-content)))

              errors ; {<redis-key> <error> ...}
              (reduce
               (fn [m k]
                 (if-not (contains? keyed-content k)
                   (assoc m k "Fetch failed")
                   (let [^Exception rr (restore-replies k)]
                     (if (or (not (instance? Exception rr))
                             ;; Already restored:
                             (= (.getMessage rr) "ERR Target key name is busy."))
                       m (assoc m k (.getMessage rr))))))
               (sorted-map) ks)]

          (when-not (empty? errors)
            (let [ex (ex-info "Failed to ensure some key(s)" errors)]
              (timbre/error ex) (throw ex)))
          nil))))

  (dirty [store ks] (dirty store :default ks))
  (dirty [store worker ks]
    (let [ks (prep-ks ks)
          {:keys [redis-ttl-ms]} opts
          existance-replies (->> (pexpire-dirty-exists worker redis-ttl-ms ks)
                                 (car/with-replies)) ; Throws on errors
          missing-ks        (->> (mapv #(when (zero? %2) %1) ks existance-replies)
                                 (filterv identity))]
      (when-not (empty? missing-ks)
        (let [ex (ex-info "Some key(s) were missing" missing-ks)]
          (timbre/error ex) (throw ex)))
      nil))

  (worker [store conn wopts]
    (let [{:keys []} opts
          {:keys [wname frequency-ms auto-start?]
           :or   {wname        :default
                  frequency-ms (* 1000 60 60) ; once/hr
                  auto-start?  true}} wopts

          merged-wopts {:wname        wname
                        :frequency-ms frequency-ms
                        :auto-start?  auto-start?}

          work-fn
          (fn []
            (let [kdset (tkey wname :dirty)
                  kcset (tkey wname :cleaning)
                  [ks-dirty ks-cleaning] (wcar {} (car/smembers kdset)
                                                  (car/smembers kcset))

                  ;; Grab ks from both dirty AND cleaning sets (the
                  ;; latter will be empty unless prev work-fn failed):
                  ks-to-freeze (vec (into (set ks-dirty) ks-cleaning))]

              (when-not (empty? ks-to-freeze)
                (let [;; Atomically move all dirty ks to cleaning set
                      _ (wcar {} (mapv (partial car/smove kdset kcset) kdset))

                      ;; [<raw-cnt-or-nil> ...]
                      dumps (wcar {} (car/parse-raw (mapv car/dump) ks-to-freeze))

                      keyed-content ; {<existing-redis-key> <frozen-cnt> ...}
                      (let [thawed-cnt
                            (->> (mapv vector ks-to-freeze dumps)
                                 (reduce (fn [m [k dump]]
                                           (if-not dump m (assoc m k dump))) {}))
                            thawed-cnt
                            (if-not freezer thawed-cnt
                                    (utils/map-kvs nil (partial freeze freezer)
                                                   thawed-cnt))])]

                  (when-not (empty? keyed-content)
                    (let [put-replies ; {<redis-key> <success?> ...}
                          (try (put-keys datastore keyed-content)
                               (catch Exception _ nil))

                          ks-succeeded (->> (keys put-replies)
                                            (filterv put-replies))
                          ks-failed    (vec (disj (set ks-to-freeze)
                                                  (set ks-succeeded)))]

                      (wcar {} (mapv (partial car/srem kcset) ks-succeeded))

                      (when-not (empty? ks-failed) ; Don't rethrow!
                        (let [ex (ex-info "Failed to freeze some key(s)" ks-failed)]
                          (timbre/error ex)))))))))

          w (Worker. conn wname (atom false) merged-wopts work-fn)]
      (when auto-start? (start w)) w)))

(defn tundra-store
  "Alpha - subject to change.
  Returns a TundraStore with options:
    :redis-ttl-ms - Optional. Time after which frozen, inactive keys will be
                    EVICTED FROM REDIS. Defaults to ~31 days, minimum 10 hours.
    :freezer      - Optional. Preps key content to/from datastore. May provide
                    services like compression and encryption, etc. Defaults to
                    Nippy with default options (Snappy compression and no
                    encryption).
    :datastore    - Storage for frozen key content.
                    See `taoensso.carmine.tundra.faraday/faraday-datastore` for
                    Faraday (DynamoDB) datastore.

  ;; Creates TundraStore and a threaded worker to routinely freeze dirty keys to
  ;; to underlying datastore (DynamoDB, etc.):
  (def tstore        (tundra-store <opts>))
  (def tundra-worker (worker tsore <conn> <opts>))

  (wcar <conn>
    (mset :k1 0 :k2 0 :3 0)  ; Creates some new keys
    (dirty tstore [:k1 :k3]) ; Marks keys for later freezing by worker
   )

  ;; Some time later... keys may have been evicted if cold

  (wcar <conn>
    (ensure tstore [:k1 :k2 :k3]) ; Ensures previously-created keys are available
    (mget :k1 :k2 :3)        ; Gets their current value
    (mapv incr [:k1 :k3])    ; Modifies them
    (dirty tstore [:k1 :k3]) ; Marks them for later refreezing by worker
   )"
  [& [{:keys [redis-ttl-ms freezer datastore]
       :or   {redis-ttl-ms (* 1000 60 60 60) ; Expire ~once/month
              freezer nippy-freezer}}]]

  (assert (or (nil? freezer) (satisfies? IFreezer freezer)))
  (assert (satisfies? IDataStore datastore))
  (assert (or (nil? redis-ttl-ms) (>= redis-ttl-ms (* 1000 60 60 60)))
          (str "Bad TTL (< 1 hour): " redis-ttl-ms))

  (TundraStore. freezer datastore {:redis-ttl-ms redis-ttl-ms}))

(comment
  (require '[taoensso.carmine.tundra.faraday :as tfar])
  (put-keys (tfar/faraday-datastore creds)
            {"k1" "key1 content"
             "k2" "key2 content"})
  (fetch-keys (tfar/faraday-datastore creds) ["k1" "k2"]))