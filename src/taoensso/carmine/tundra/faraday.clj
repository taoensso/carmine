(ns taoensso.carmine.tundra.faraday
  "Faraday (DynamoDB) datastore implementation for Tundra.

  Use AWS Data Pipeline to setup scheduled backups of DynamoDB table(s) to S3
  (there is a template pipeline for exactly this purpose)."
  {:author "Peter Taoussanis"}
  (:require [taoensso.faraday       :as far]
            [taoensso.faraday.utils :as futils]
            [taoensso.timbre        :as timbre])
  (:import  [taoensso.carmine.tundra IDataStore]))

;; TODO Add auto-threading support for saturating (very) high-throughput freezing?
;; TODO Add support for splitting data > 64KB over > 1 keys? This may be tough
;; to do well relative to the payoff. And I'm guessing there may be an official
;; (Java) lib to offer this capability at some point?

(def default-table :faraday.tundra.datastore.default.prod)

(defn ensure-table
  "Creates an appropriate Faraday Tundra table iff it doesn't already exist.
  Options:
    :name       - Default is :faraday.tundra.datastore.default.prod. You may
                  want additional tables for different apps/environments.
    :throughput - Default is {:read 1 :write 1}. You probably won't want higher
                  write throughput here since Tundra table writes occur only in
                  tight groups while freezing, and an :auto-write-units
                  DataStore option is provided for those periods."
  [creds & [{:keys [name throughput block?]
             :or   {name default-table}}]]
  (far/ensure-table creds name [:key-ns :s]
    {:range-keydef [:redis-key :s]
     :throughput   throughput
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
  (put-keys [this keyed-data]
    (let [{:keys [table key-ns auto-write-units]} opts
          ks     (keys keyed-data)
          nks    (count ks)
          kparts (partition-all 15 ks) ; (< (* 15 64) 1024) so => < 1MB API limit

          ;; auto-write-units is of form {<nks> <write-units> ...}
          orig-wunits (-> (far/describe-table creds table) :throughput :write)
          want-wunits (max-min-kval nks auto-write-units)
          ;; nthreads    (max-min-kval (or want-wunits orig-wunits)
          ;;               {0 1 50 2 200 4}) ; TODO Saturate allocated wunits
          ;; Will probably need thread partitions [of key partitions].

          adj-wunits
          (fn [wunits]
            (try @(far/update-table creds table {:write wunits})
                 (catch Exception e (timbre/error e "Table update error"))))

          write1-partition
          (fn [pks]
            (far/batch-write-item creds
              {table {:put (mapv (fn [pk] {:key-ns key-ns
                                          :redis-key pk
                                          :data (keyed-data pk)})
                                 pks)}}))]

      (when want-wunits (adj-wunits want-wunits))
      (try (when (every? (comp empty? :unprocessed) (mapv write1-partition kparts))
             (zipmap ks (repeat true)))
           (finally (when want-wunits (adj-wunits orig-wunits))))))

  (fetch-keys [this ks]
    (let [{:keys [table key-ns]} opts
          kparts (partition-all 100 ks) ; API limit
          get1-partition ; {<redis-key> <frozen-data> ...}
          (fn [pks] (->> (far/batch-get-item creds
                          {table {:prim-kvs {:key-ns key-ns
                                             :redis-key pks}
                                  :attrs [:redis-key :data]}})
                        (table) ; [{:worker _ :redis-key _} ...]
                        (far/items-by-attrs :redis-key)
                        (futils/map-kvs nil :data)))]
      (reduce merge {} (mapv get1-partition kparts)))))

(defn faraday-datastore
  "Alpha - subject to change.
  Returns a Faraday DataStore with options:
    :table            - Tundra table name. Useful for different apps/environments
                        with individually provisioned throughput.
    :key-ns           - Optional key namespacing mechanism useful for different
                        apps/environments under a single table (i.e. shared
                        provisioned throughput).
    :auto-write-units - {<peak-num-keys-being-frozen> <write-units>} for
                        automatic write-throughput scaling during key
                        freeze. Uses up to 4 write units/sec by default
                        when freezing 400+ dirty keys at once."
  [creds & [{:keys [table key-ns auto-write-units]
             :or   {table default-table
                    key-ns :default
                    auto-write-units {0 1, 200 2, 400 4}}}]]
  (->FaradayDataStore creds {:table            table
                             :key-ns           key-ns
                             :auto-write-units auto-write-units}))

(comment
  (ensure-table creds {:throughput {:read 1 :write 1}})
  (far/describe-table creds default-table)
  (far/batch-write-item creds
    {ttable {:put [{:redis-key "k1" :data "data1"}
                   {:redis-key "k2" :data "data2"}]}}))
