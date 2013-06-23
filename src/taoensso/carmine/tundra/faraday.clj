(ns taoensso.carmine.tundra.faraday
  "Faraday (DynamoDB) datastore implementation for Tundra."
  {:author "Peter Taoussanis"}
  (:require [taoensso.faraday       :as far]
            [taoensso.faraday.utils :as futils]
            [taoensso.timbre        :as timbre])
  (:import  [taoensso.carmine.tundra IDataStore]))

;; TODO Add auto-threading support for saturating (very) high-throughput freezing?

(def ttable :faraday.tundra.datastore)

(defn ensure-table
  "Creates the Faraday Tundra table iff it doesn't already exist.

  You'll probably want the minimum write throughput here (1 unit) since
  writes occur only in tight groups while freezing, and an :auto-write-units
  DataStore option is provided for these periods."
  [creds & [{:keys [throughput block?]}]]
  (far/ensure-table creds ttable [:redis-key :s]
    {:throughput   throughput
     :block?       block?}))

(defn- max-min-kval
  "Returns the (m k), if any, for which k (a number <= min) is greatest."
  [min m]
  (when-let [ks (seq (filter #(<= % min) (keys m)))]
    (m (apply max ks))))

(comment (max-min-kval 2  {5 :5 10 :10 50 :50})
         (max-min-kval 8  {5 :5 10 :10 50 :50})
         (max-min-kval 72 {5 :5 10 :10 50 :50}))

(defrecord FaradayDataStore [creds opts]
  IDataStore
  (put-keys [this keyed-content]
    (let [{:keys [auto-write-units]} opts
          ks     (keys keyed-content)
          nks    (count ks)
          kparts (partition-all 15 ks) ; (< (* 15 64) 1024) so => < 1MB API limit

          ;; auto-write-units is of form {<nks> <write-units> ...}
          orig-wunits (-> (far/describe-table creds ttable) :throughput :write)
          want-wunits (max-min-kval nks auto-write-units)
          ;; nthreads    (max-min-kval (or want-wunits orig-wunits)
          ;;               {0 1 50 2 200 4}) ; TODO Saturate allocated wunits
          ;; Will probably need thread partitions [of key partitions].

          adj-wunits
          (fn [wunits]
            (try @(far/update-table creds ttable {:write wunits})
                 (catch Exception e (timbre/error e "Table update error"))))

          write1-partition
          (fn [pks]
            (far/batch-write-item creds
              {ttable {:put (mapv (fn [pk] {:redis-key pk
                                           :data (keyed-content pk)})
                                  pks)}}))]

      (when want-wunits (adj-wunits want-wunits))
      (try (when (every? (comp empty? :unprocessed) (mapv write1-partition kparts))
             (zipmap ks (repeat true)))
           (finally (when want-wunits (adj-wunits orig-wunits))))))

  (fetch-keys [this ks]
    (let [kparts (partition-all 100 ks) ; API limit
          get1-partition ; {<redis-key> <frozen-data> ...}
          (fn [pks] (->> (far/batch-get-item creds
                          {ttable {:prim-kvs {:redis-key pks}
                                   :attrs [:redis-key :data]}})
                        (ttable) ; [{:worker _ :redis-key _} ...]
                        (far/items-by-attrs :redis-key)
                        (futils/map-kvs nil :data)))]
      (reduce merge {} (mapv get1-partition kparts)))))

(defn faraday-datastore
  "Alpha - subject to change.
  Returns a Faraday DataStore with options:
    :auto-write-units - {<peak-num-keys-being-frozen> <write-units>} for
                        automatic write-throughput scaling during key
                        freeze. Uses up to 4 write units/sec by default
                        when freezing 200+ dirty keys at once."
  [creds & [{:keys [auto-write-units]
             :or   {auto-write-units {0 1, 100 2, 200 4}}}]]
  (FaradayDataStore. creds {:auto-write-units auto-write-units}))

(comment
  (ensure-table creds {:throughput {:read 1 :write 1}})
  (far/describe-table creds ttable)
  (far/batch-write-item creds
    {ttable {:put [{:redis-key "k1" :data "data1"}
                   {:redis-key "k2" :data "data2"}]}}))