(ns taoensso.carmine.tundra
  "Alpha - subject to change (hide the kittens!).
  Semi-automatic datastore layer for Carmine. It's like the magix. Redis 2.6+.

  Redis keys:
    * carmine:tundra:<worker>:dirty -> set, dirty Redis keys."
  {:author "Peter Taoussanis"}
  (:refer-clojure :exclude [ensure])
  (:require [taoensso.carmine       :as car :refer (wcar)]
            [taoensso.carmine.utils :as utils]
            [taoensso.nippy         :as nippy]
            [taoensso.nippy.tools   :as nippy-tools]
            [taoensso.timbre        :as timbre]))

;;;; TODO
;; * Simpler working Faraday datastore.
;; * Roughest working prototype, ASAP.
;; * Tests.
;; * README docs.
;; * Worker option to keep snapshot of previous n atomic vals? Would need to
;;   add a pair of IDataStore fns to move/snapshot & to manually fetch ks.

;;;; Public interfaces

(defprotocol IFreezer
  (freeze [freezer x] "Returns datastore-ready key content.")
  (thaw   [freezer x] "Returns Redis-ready key content."))

(defprotocol IDataStore ; Main extension point
  (put-keys   [store content] "{<k> <frozen-content> ...} -> {<k> <success?> ...}.")
  (fetch-keys [store ks]      "[<k> ...] -> {<k> <frozen-content>}."))

(defprotocol IWorker
  (start [this] "Returns true iff worker successfully started.")
  (stop  [this] "Returns true iff worker successfully stopped."))

(defprotocol ITundraStore
  (ensure [store ks]
    "Alpha - subject to change.
    BLOCKS to ensure given keys are available in Redis, fetching them from
    datastore as necessary. Throws an exception if any keys couldn't be made
    available.

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

(defrecord DiskDataStore [path] ; TODO Implementation (PRs welcome!)
  IDataStore
  (put-keys   [store content])
  (fetch-keys [store ks]))

;; taoensso.carmine.tundra.faraday/FaradayDataStore

(defrecord Worker [conn wname job-fn running? opts]
  IWorker
  (stop  [_] (let [stopped? @running?] (reset! running? false) stopped?))
  (start [_]
    (when-not @running?
      (reset! running? true)
      (future
        (let [{:keys [frequency-ms]} opts]
          (while @running?
            (try (job-fn)
              (catch Throwable t
                (timbre/fatal t
                 "CRITICAL worker error, shutting down! DATA AT RISK!!")
                (throw t)))
            (when frequency-ms (Thread/sleep frequency-ms)))))
      true)))

(def ^:private tkey (memoize (partial car/kname "carmine" "tundra")))

(defn- extend-exists
  "Returns 0/1 for each key that doesn't/exist. Extends any preexisting TTLs."
  [ttl-ms keys]
  (apply car/eval*
    "local result = {}
     for i,k in pairs(KEYS) do
       if redis.call('ttl', k) > 0 then
         redis.call('pexpire', k, tonumber(ARGV[1]))
         result[i] = 1
       else
         -- TODO Waiting on -2/-1 return-value feature for TTL command
         result[i] = redis.call('exists', k)
       end
     end
     return result"
    (count keys)
    (conj (vec keys) (or ttl-ms "no-ttl"))))

(comment (wcar {} (car/ping)
               (extend-exists nil ["foo" "bar" "baz"])
               (car/ping)))

(defn- prep-ks [ks] (assert (utils/coll?* ks)) (vec (distinct (map name ks))))
(comment (prep-ks [:a "a" :b :foo.bar/baz])
         (prep-ks [nil]))

(defrecord TundraStore [freezer datastore opts]
  ITundraStore
  (ensure [store ks]
    (let [ks (prep-ks ks)
          {:keys [cache-ttl-ms]} opts
          existance-replies (->> (apply extend-exists cache-ttl-ms ks)
                                 (car/with-replies)) ; Immediately throw on errors
          missing-ks        (->> (mapv #(when (zero? %2) %1) ks existance-replies)
                                 (filterv identity))]

      (comment ; TODO Migrate

        (when-not (empty? missing-ks)
          (timbre/trace "Fetching missing keys: " missing-ks)
          (let [fetch-data ; {<redis-key> <data> ...}
                nil ; TODO

                ;; Restore what we can even if some fetches failed
                restore-replies ; {<redis-key> <restore-reply> ...}
                (->> (doseq [[k data] fetch-data]
                       (if-not (futils/bytes? data)
                         (car/return (Exception. "Malformed fetch data"))
                         (car/restore k (or cache-ttl-ms 0) (car/raw data))))
                     (car/with-replies :as-pipeline) ; ["OK" "OK" ...]
                     (zipmap (keys fetch-data)))

                errors ; {<redis-key> <error> ...}
                (reduce
                 (fn [m k]
                   (if-not (contains? fetch-data k)
                     (assoc m k "Fetch failed")
                     (let [^Exception rr (restore-replies k)]
                       (if (or (not (instance? Exception rr))
                               ;; Already restored:
                               (= (.getMessage rr) "ERR Target key name is busy."))
                         m (assoc m k (.getMessage rr))))))
                 (sorted-map) ks)]

            (when-not (empty? errors)
              (let [ex (ex-info "Failed to ensure some key(s)" errors)]
                (timbre/error ex) (throw ex))))))))

  (dirty  [store ks] (dirty store :default ks))
  (dirty  [store worker ks]
    (let [ks (prep-ks ks)]
      ;; TODO Not too much to do here,
      ;; 1. Set pexpire on all the keys.
      ;; 2. Add all the keys to the appropriate 'dirty' set.
      ;; 3. Throw an exception if any of the `pexpires` failed (i.e. keys didn't
      ;;    exist)?
      ))

  (worker [store conn opts]
    (let [{:keys [wname frequency-ms auto-start?]
           :or   {wname        :default
                  frequency-ms (* 1000 60 60) ; once/hr
                  auto-start?  true}} opts
          merged-opts {:wname        wname
                       :frequency-ms frequency-ms
                       :auto-start?  auto-start?}
          job-fn (fn [] ; TODO
                   )
          w (Worker. conn wname (atom false) merged-opts job-fn)]
      (when auto-start? (start w)) w)))

(defn tundra-store
  "Alpha - subject to change.
  TODO"
  [& [{:keys [freezer datastore cache-ttl-ms]
       :or   {cache-ttl-ms (* 1000 60 60 60) ; ~once/month
              }}]]
  ;; In particular, catch people from using 0 to mean nil!
  (assert (or (not cache-ttl-ms) (>= cache-ttl-ms (* 1000 60 60 60)))
          (str "Bad TTL (< 1 hour): " cache-ttl-ms)))

(comment ; TODO
  (wc (car/del "k1" "k2" "k3" "k4"))
  (far/put-item mc ttable {:worker "default" :redis-key "k3" :data "malformed-RDF"})
  (wc (ensure mc ["k1" "k4" "k3" "k2"]))

  (wcar {} (car/dump "non-existant")) ; nil
  (wcar {} (car/set "fookey" "barval")
        (car/restore "fookey*3"
                     0 ; ms ttl
                     (car/raw (wc (car/parse-raw (car/dump "fookey")))))
        (car/get "fookey*3")))