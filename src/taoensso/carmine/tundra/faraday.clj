(ns taoensso.carmine.tundra.faraday
  "Faraday (DynamoDB) DataStore implementation for Tundra.

  Use AWS Data Pipeline to setup scheduled backups of DynamoDB table(s) to S3
  (there is a template pipeline for exactly this purpose)."
  {:author "Peter Taoussanis"}
  (:require [taoensso.faraday :as far]
            [taoensso.timbre  :as timbre])
  (:import  [taoensso.carmine.tundra IDataStore]))

(def default-table :faraday.tundra.datastore.default.prod)
(defn ensure-table
  "Creates an appropriate Faraday Tundra table iff it doesn't already exist.
  Options:
    :name       - Default is :faraday.tundra.datastore.default.prod. You may
                  want additional tables for different apps/environments.
    :throughput - Default is {:read 1 :write 1}."
  [creds & [{:keys [name throughput block?]
             :or   {name default-table}}]]
  (far/ensure-table creds name [:key-ns :s]
    {:range-keydef [:redis-key :s]
     :throughput   throughput
     :block?       block?}))

(defrecord FaradayDataStore [creds opts]
  IDataStore
  (put-key [this k v]
    (far/put-item creds (:table opts) {:key-ns (:key-ns opts)
                                       :redis-key k
                                       :data      v})
    true)

  (fetch-key [this k]
    (-> (far/get-item creds (:table opts) {:key-ns (:key-ns opts)
                                           :redis-key k}
                      {:attrs [:data]})
        :data)))

(defn faraday-datastore
  "Alpha - subject to change.
  Returns a Faraday DataStore with options:
    :table            - Tundra table name. Useful for different apps/environments
                        with individually provisioned throughput.
    :key-ns           - Optional key namespacing mechanism useful for different
                        apps/environments under a single table (i.e. shared
                        provisioned throughput).

  Supported Freezer io types: byte[]s, strings."
  [creds & [{:keys [table key-ns]
             :or   {table default-table
                    key-ns :default}}]]
  (->FaradayDataStore creds {:table  table
                             :key-ns key-ns}))

(comment
  (ensure-table creds {:throughput {:read 1 :write 1}})
  (far/describe-table creds default-table)
  (require '[taoensso.carmine.tundra :as tundra])
  (def dstore (faraday-datastore creds))
  (tundra/put-key dstore "foo:bar:baz" (.getBytes "hello world"))
  (String. (tundra/fetch-key dstore "foo:bar:baz")))
