(ns taoensso.carmine.tundra.faraday
  "Faraday (DynamoDB) DataStore implementation for Tundra.

  Use AWS Data Pipeline to setup scheduled backups of DynamoDB table(s) to S3
  (there is a template pipeline for exactly this purpose)."
  {:author "Peter Taoussanis"}
  (:require [taoensso.encore         :as encore]
            [taoensso.timbre         :as timbre]
            [taoensso.faraday        :as far]
            [taoensso.carmine.tundra :as tundra])
  (:import  [taoensso.carmine.tundra IDataStore]))

(def default-table :faraday.tundra.datastore.default.prod)
(defn ensure-table ; Slow, so we won't do automatically
  "Creates an appropriate Faraday Tundra table iff it doesn't already exist.
  Options:
    :name       - Default is :faraday.tundra.datastore.default.prod. You may
                  want additional tables for different apps/environments.
    :throughput - Default is {:read 1 :write 1}."
  [client-opts & [{:keys [name throughput block?]
                   :or   {name default-table}}]]
  (far/ensure-table client-opts name [:key-ns :s]
    {:range-keydef [:redis-key :s]
     :throughput   throughput
     :block?       block?}))

(defrecord FaradayDataStore [client-opts opts]
  IDataStore
  (put-key [this k v]
    (assert (encore/bytes? v))
    (far/put-item client-opts (:table opts)
                  {:key-ns (:key-ns opts)
                   :redis-key  k
                   :frozen-val v})
    true)

  (fetch-keys [this ks]
    (assert (<= (count ks) 100)) ; API limit
    (let [{:keys [table key-ns]} opts
          vals-map ; {<redis-key> <frozen-val>}
          (try
            (->> (far/batch-get-item client-opts
                   {table {:prim-kvs {:key-ns key-ns
                                      :redis-key ks}
                           :attrs [:redis-key :frozen-val]}})
                 (table) ; [{:frozen-val _ :redis-key _} ...]
                 (far/items-by-attrs :redis-key)
                 (encore/map-kvs nil :frozen-val)
                 (reduce merge {}))
            (catch Throwable t (zipmap ks (repeat t))))]
      (mapv #(get vals-map % (Exception. "Missing value")) ks))))

(defn faraday-datastore
  "Alpha - subject to change.
  Returns a Faraday DataStore with options:
    :table  - Tundra table name. Useful for different apps/environments with
              individually provisioned throughput.
    :key-ns - Optional key namespacing mechanism useful for different
              apps/environments under a single table (i.e. shared provisioned
              throughput).
  Supported Freezer io types: byte[]s."
  [client-opts & [{:keys [table key-ns]
                   :or   {table default-table
                          key-ns :default}}]]
  (->FaradayDataStore client-opts {:table  table
                                   :key-ns key-ns}))

(comment
  (ensure-table client-opts {:throughput {:read 1 :write 1}})
  (far/describe-table client-opts default-table)
  (def dstore  (faraday-datastore client-opts))
  (def hardkey (tundra/>urlsafe-str "foo:bar /♡\\:baz "))
  (tundra/put-key dstore hardkey (.getBytes "hello world"))
  (String. (first (tundra/fetch-keys dstore [hardkey]))))
