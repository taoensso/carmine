(ns taoensso.carmine.tundra
  "Semi-automatic archival datastore layer for Carmine.
  Use multiple Redis instances (recommended) or Redis databases for local key
  namespacing.

  Redis keys:
    * carmine:tundra:evictable - set, ks for which `ensure-ks` fail should throw"

  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require [taoensso.encore      :as enc]
            [taoensso.nippy       :as nippy]
            [taoensso.nippy.tools :as nippy-tools]
            [taoensso.timbre      :as timbre]
            [taoensso.carmine     :as car :refer (wcar)]
            [taoensso.carmine.message-queue :as mq])
  (:import  [java.net URLDecoder URLEncoder]))

;;;; TODO v2/refactor
;; - Why is evictable set key not configurable?
;; - General refactor (reduce-based flow, refactor stores, etc.)
;; - Drop mq for a simpler zset {<k> <udt-last-dirty>} based model:
;;   atomic-pull ks on verified write when udt unchanged against write val

;;;; Public interfaces

(defprotocol IDataStore "Extension point for additional datastores."

  ;; Done 1-at-a-time via mq, so no need for bulk ks api:
  (put-key    [dstore k v] "(put-key dstore \"key\" <frozen-val>) => <#{true <ex>}>")
  (fetch-keys [dstore ks] "(fetch-keys dstore [\"key\" ...]) => [<#{<frozen-val> <ex>}> ...]"))

(defprotocol IFreezer "Extension point for compressors, encryptors, etc."
  (freeze [freezer x] "Returns datastore-ready key val.
                       (comp put-key freeze): dump-ba -> datastore val.")
  (thaw   [freezer x] "Returns Redis-ready key val.
                       (comp thaw fetch-key): datastore val -> dump-ba."))

(defprotocol ITundraStore
  (ensure-ks* [tstore ks])
  (dirty*     [tstore ks])
  (worker     [tstore conn-opts wopts]
    "Alpha - subject to change.
    Returns a threaded message queue worker to routinely freeze Redis keys
    marked as dirty to datastore and mark successfully frozen keys as clean.
    Logs any errors. THESE ERRORS ARE **IMPORTANT**: an email or other
    appropriate notification mechanism is HIGHLY RECOMMENDED. If a worker shuts
    down and your keys are set to expire YOU WILL IRREVOCABLY **LOSE DATA**.

    Options:
      :nattempts        - Number of times worker will attempt to freeze a key to
                          datastore before failing permanently. >=1.
      :retry-backoff-ms - Amount of time (msecs) to backoff before retrying a
                          failed key freeze. >=0. Can be a (fn [attempt]) -> ms.

      :montior, :eoq-backoff-ms, :nthreads, :throttle-ms, :auto-start
      - Standard `taoensso.carmine.message-queue/worker` opts."))

(defn ensure-ks
  "BLOCKS to ensure given keys are available in Redis, fetching them from
  datastore as necessary. Throws an exception if any previously evicted keys
  couldn't be made available. Acts as a Redis command: call within a `wcar`
  context."
  [tstore & ks] {:pre [(<= (count ks) 10)] :post [(nil? %)]}
  (ensure-ks* tstore ks))

(defn dirty
  "Queues given keys for freezing to datastore. Throws an exception if any keys
  don't exist. Acts as a Redis command: call within a `wcar` context.

  If TundraStore has a :redis-ttl-ms option, **MARKS GIVEN KEYS FOR EXPIRY**!!
  ** Worker MUST be running AND FUNCTIONING CORRECTLY or DATA WILL BE LOST! **"
  [tstore & ks] {:pre [(<= (count ks) 100)] :post [(nil? %)]}
  (dirty* tstore ks))

;;;; Default implementations

(defrecord NippyFreezer [opts]
  IFreezer
  (freeze [_ x]  (nippy/freeze x  opts))
  (thaw   [_ ba] (nippy/thaw   ba opts)))

(def nippy-freezer "Default Nippy Freezer." (NippyFreezer. {}))

;;;;

(def ^:private extend-exists
  "Returns 0/1 for each key that doesn't/exist, extending any preexisting TTLs."
  ;; Cluster: no between-key atomicity requirements, can pipeline per shard
  (let [script (enc/slurp-resource "lua/tundra/extend-exists.lua")]
    (fn [ttl-ms keys] (car/lua script keys [(or ttl-ms 0)]))))

(comment (wcar {} (car/ping) (extend-exists nil ["k1" "invalid" "k3"])))

;; Could make this configurable per store but not a big deal in practice since
;; the key space is per Redis server anyway:
(def ^:private k-evictable "carmine:tundra:evictable")

(defn extend-exists-missing-ks [ttl-ms ks & [only-evictable?]]
  (let [existance-replies (->> (extend-exists ttl-ms ks)
                               (car/with-replies) ; Single bulk reply
                               (car/parse #(mapv car/as-bool %)))
        ks-missing        (->> (mapv #(when-not %2 %1) ks existance-replies)
                               (filterv identity))]
    (if-not only-evictable? ks-missing
      (let [evictable-replies
            (->> ks-missing
                 (mapv #(car/sismember k-evictable %))
                 (car/with-replies :as-pipeline)
                 (car/parse-bool))
            evictable-ks-missing
            (->> (mapv #(when %2 %1) ks-missing evictable-replies)
                 (filterv identity))]

        (timbre/tracef "extend-exists-missing-ks: %s"
          [:existance-replies existance-replies
           :ks-missing        ks-missing
           :evictable-replies evictable-replies
           :evictable-ks-missing evictable-ks-missing])

        evictable-ks-missing))))

;;;;

(def fetch-keys-delayed
  "Used to prevent multiple threads from rushing the datastore to get the same
  keys, unnecessarily duplicating work."
  (enc/memoize* 5000 fetch-keys))

(defn- prep-ks [ks] (vec (distinct (mapv enc/as-qname ks))))
(comment (prep-ks [nil]) ; Throws
         (prep-ks [:a "a" :b :foo.bar/baz]))

(defmacro catcht [& body] `(try (do ~@body) (catch Throwable t# t#)))
(defn >urlsafe-str [s] (URLEncoder/encode s "ISO-8859-1"))
(defn <urlsafe-str [s] (URLDecoder/decode s "ISO-8859-1"))
(comment (<urlsafe-str (>urlsafe-str "hello f8 8 93#**#\\// !!$")))

(defrecord TundraStore [datastore freezer opts]
  ITundraStore
  (ensure-ks* [tstore ks]
    (let [{:keys [redis-ttl-ms]} opts
          ks (prep-ks ks)
          ks-missing (extend-exists-missing-ks redis-ttl-ms ks :only-evictable)]

      (timbre/tracef "ensure-ks*: %s" {:ks ks
                                       :ks-missing ks-missing})

      (when-not (empty? ks-missing)
        (timbre/tracef "Fetching missing evictable keys: %s" ks-missing)
        (let [;;; [] e/o #{<dumpval> <throwable>}:
              throwable?    #(instance? Throwable %)
              dvals-missing (try (fetch-keys-delayed datastore
                                   (mapv >urlsafe-str ks-missing))
                                 (catch Throwable t (mapv (constantly t) ks-missing)))
              _
              (when-not (= (count dvals-missing)
                           (count ks-missing))
                (let [n-dvals-missing (count dvals-missing)
                      n-ks-missing    (count ks-missing)]
                  (throw
                    (ex-info
                      (format (str "Bad `fetch-keys` result:"
                                " unexpected val count (got %s, expected %s)."
                                " Bad DataStore implementation?")
                        n-dvals-missing
                        n-ks-missing)
                      {:n-dvals-missing n-dvals-missing
                       :n-ks-missing    n-ks-missing}))))

              dvals-missing (if (nil? freezer) dvals-missing
                                (->> dvals-missing
                                     (mapv #(if (throwable? %) %
                                                (catcht (thaw freezer %))))))

              restore-replies ; [] e/o #{"OK" <throwable>}
              (->> dvals-missing
                   (mapv (fn [k dv]
                           (if (throwable? dv) (car/return dv)
                               (if-not (enc/bytes? dv)
                                 (car/return
                                   (ex-info "Malformed fetch data" {:dv dv}))
                                 (car/restore k (or redis-ttl-ms 0) (car/raw dv)))))
                         ks-missing)
                   (car/with-replies :as-pipeline)
                   (car/parse nil))

              errors ; {<k> <throwable>}
              (->> (zipmap ks-missing restore-replies)
                   (reduce (fn [m [k v]]
                             (if-not (throwable? v) m
                               (if (and (instance? Exception v)
                                        (= (.getMessage ^Exception v)
                                           "ERR Target key name is busy."))
                                 m ; Already restored
                                 (assoc m k v))))
                           {}))]

          (when-not (empty? errors)
            (let [ex (ex-info "Failed to ensure some key(s)" errors)]
              (timbre/error ex) (throw ex)))
          nil))))

  (dirty* [tstore ks]
    (let [{:keys [tqname redis-ttl-ms]} opts
          ks (prep-ks ks)
          ks-missing     (extend-exists-missing-ks redis-ttl-ms ks)
          ks-not-missing (->> ks (filterv (complement (set ks-missing))))]

      (timbre/tracef "dirty*: %s" {:ks ks
                                   :ks-missing ks-missing
                                   :ks-not-missing ks-not-missing})

      (enc/run!
        (fn [k]
          (->> (mq/enqueue tqname k k :allow-locked-dupe) ; key as msg & mid (deduped)
               (car/with-replies :as-pipeline) ; Don't pollute pipeline
               ))
        ks-not-missing)

      (when-not (empty? ks-missing)
        (let [ex (ex-info "Some dirty key(s) were missing" {:ks ks-missing})]
          (timbre/error ex) (throw ex)))
      nil))

  (worker [tstore conn-opts wopts]
    (let [{:keys [tqname redis-ttl-ms]} opts
          {:keys [nattempts retry-backoff-ms]
           :or   {nattempts 3
                  retry-backoff-ms mq/exp-backoff}} wopts]
      (mq/worker conn-opts tqname
        (assoc wopts :handler
          (fn [{:keys [mid message attempt]}]
            (let [k message
                  put-reply ; #{true nil <throwable>}, nb inclusion of nil!
                  (catcht (->> (wcar conn-opts (car/parse-raw (car/dump k)))
                               (#(if (or (nil? %) ; Key doesn't exist
                                         (nil? freezer)) %
                                   (freeze freezer %)))
                               (#(if (nil? %) nil
                                   (put-key datastore (>urlsafe-str k) %)))))]

              (timbre/tracef "Worker loop: %s" {:k message
                                                :put-reply put-reply})

              (if (true? put-reply)
                (do (wcar conn-opts (car/sadd k-evictable k)
                               (when (and redis-ttl-ms (> redis-ttl-ms 0))
                                 (car/pexpire k redis-ttl-ms)))
                    {:status :success})
                (if (<= attempt nattempts)
                  {:status :retry
                   :backoff-ms
                   (cond (nil?     retry-backoff-ms) nil
                         (fn?      retry-backoff-ms) (retry-backoff-ms attempt)
                         (integer? retry-backoff-ms) retry-backoff-ms)}

                  {:status :error
                   :throwable
                   (cond
                    (nil? put-reply) (ex-info "Key doesn't exist" {:k k})
                    :else (ex-info "Bad put-reply" {:k k :put-reply put-reply}))})))))))))
;;;;

(defn tundra-store
  "Alpha - subject to change.
  Returns a TundraStore with options:
    datastore     - Storage for frozen key data. Default datastores:
                    `taoensso.carmine.tundra.faraday/faraday-datastore`
                    `taoensso.carmine.tundra.s3/s3-datastore`.
    :tqname       - Optional. Worker message queue name.
    :freezer      - Optional. Preps key data to/from datastore. May provide
                    services like compression and encryption, etc. Defaults to
                    Nippy with default options (Snappy compression and no
                    encryption).
    :redis-ttl-ms - Optional! Time after which frozen, inactive keys will be
                    EVICTED FROM REDIS (**DELETED!**). Minimum 10 hours. ONLY
                    use this if you have CONFIRMED that your worker is
                    successfully freezing the necessary keys to your datastore.
                    Otherwise YOU WILL IRREVOCABLY **LOSE DATA**.

  See `ensure-ks`, `dirty`, `worker` for TundraStore API."
  [datastore & [{:keys [tqname freezer redis-ttl-ms]
                 :or   {tqname :default freezer nippy-freezer}}]]
  {:pre [(satisfies? IDataStore datastore)
         (or (nil? freezer) (satisfies? IFreezer freezer))
         (or (nil? redis-ttl-ms) (>= redis-ttl-ms (* 1000 60 60 10)))]}

  (TundraStore. datastore freezer
    {:tqname (format "tundra:%s" (name tqname))
     :redis-ttl-ms redis-ttl-ms}))

(comment
  (require '[taoensso.carmine.tundra.disk :as tundra-disk])
  (def tstore (tundra-store (tundra-disk/disk-datastore "./tundra/")))
  (car/wcar {} (ensure-ks tstore "invalid"))
  (car/wcar {} (dirty tstore "invalid")))
