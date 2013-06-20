(ns taoensso.carmine.tundra.faraday
  "Faraday (DynamoDB) datastore implementation for Tundra."
  {:author "Peter Taoussanis"}
  (:require [taoensso.faraday       :as far]
            [taoensso.faraday.utils :as futils]
            [taoensso.timbre        :as timbre])
  (:import  [taoensso.carmine.tundra IDataStore]))

(def ttable :faraday.tundra.datastore)

(defn ensure-table
  "Creates the Faraday Tundra table iff it doesn't already exist.

  You'll probably want the minimum write throughput here (1 units) since
  writes will usually occur in tight groups while freezing, and a
  :peak-write-config DataStore option is provided for these periods."
  [creds & [{:keys [throughput block?]}]]
  (far/ensure-table creds ttable [:worker :s]
    {:throughput   throughput
     :range-keydef [:redis-key :s]
     :block?       block?}))

(defrecord FaradayDataStore [creds opts]
  IDataStore
  (put-keys [store content]
    (let [{:keys [peak-write-config]} opts]

      ;;; TODO
      ;; * content is frozen and ready-to-go
      ;; * No point in doing write throttling this table.
      ;; * Check {<max-ks> <write-units> ...} map for auto write scaling.
      ;; * partition-all as 15-groups for stitching because (< (* 15 64) 1024),
      ;;   an easy way of ensuring we'll be under 1MB limit even if every key
      ;;   is max possible size.
      ;; * Return {<k> <success?> ...}

      ))

  (fetch-keys [store ks]

    ;;; TODO
    ;; * partition-all as 100-groups for stitching.
    ;; * No need for consistent reads: there's presumably been at least _hours_
    ;;   since last write.
    ;; * Return {<k> <frozen-content> ...}.

    ;; fetch-data ; {<redis-key> <data> ...}
    ;; (->> (far/batch-get-item creds
    ;;        {ttable {:prim-kvs {:worker    (name worker)
    ;;                            :redis-key missing-ks}
    ;;                 :attrs [:redis-key :data]}})
    ;;      (ttable) ; [{:worker _ :redis-key _} ...]
    ;;      (far/items-by-attrs :redis-key))

    ))

(defn faraday-datastore
  ""
  [creds & [{:keys [peek-write-config]}]])

(comment ; TODO
  (def creds {:access-key "" :secret-key ""})

  (ensure-table creds {:throughput {:read 1 :write 1}})
  (far/describe-table creds ttable)

  (let [ttable  ttable
        tworker :test-worker
        tkey (partial car/kname "carmine" "tundra" "temp" "test")
        [k1 k2 k3 k4 :as ks] (mapv tkey ["k1 k2 k3 k4"])]

    (wcar (apply car/del ks))
    (far/batch-write-item creds {ttable {:delete [{:worker   (name tworker)
                                                   :redis-key k1}]}})

    (far/scan creds tundra/ttable {:attr-conds {:worker [:eq ["default"]]}})))