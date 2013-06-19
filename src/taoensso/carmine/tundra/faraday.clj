(ns taoensso.carmine.tundra.faraday
  "Faraday (DynamoDB) datastore implementation for Tundra."
  {:author "Peter Taoussanis"}
  (:require [taoensso.faraday       :as far]
            [taoensso.faraday.utils :as futils]
            [taoensso.timbre        :as timbre])
  (:import  [taoensso.carmine.tundra IDataStore]))

(def ttable :faraday.tundra.datastore)

(defn ensure-table
  "Creates the Faraday Tundra table iff it doesn't already exist."
  [creds & [{:keys [throughput block?]}]]
  (far/ensure-table creds
    {:name         ttable
     :throughput   throughput
     :hash-keydef  {:name :worker    :type :s}
     :range-keydef {:name :redis-key :type :s}
     :block?       block?}))

(defrecord FaradayDataStore [creds]
  IDataStore
  ;; TODO Logic to break jobs into multiple requests as necessary.

  ;; Important DynamoDB limits apply, Ref. http://goo.gl/wA06O
  ;;   * Max item size: 64KB (incl. key size).
  ;;   * Batch GET: <= 100 items, <= 1MB total.
  ;;   * Batch PUT: <= 25 items,  <= 1MB total.

  ;; (/ 1024 64) => we'll be safe on both GETs & PUTS if we limit item
  ;; size to, say, 60KB and 16 items per request

  (put-keys   [store content]
    ;; TODO
    )
  (fetch-keys [store ks]
    ;; Doesn't need to be a consistent read since there's presumably been at
    ;; least HOURS since last write :-)

    ;; TODO
    ))

(comment ; TODO
  (def creds {:access-key "" :secret-key ""})
  (ensure-table creds {:read 1 :write 1})
  (far/describe-table creds ttable)

  (let [ttable  ttable
        tworker :test-worker
        tkey (partial car/kname "carmine" "tundra" "temp" "test")
        [k1 k2 k3 k4 :as ks] (mapv tkey ["k1 k2 k3 k4"])]

    (wcar (apply car/del ks))
    (far/batch-write-item creds {ttable {:delete [{:worker   (name tworker)
                                                   :redis-key k1}]}})

    (far/scan creds tundra/ttable {:attr-conds {:worker [:eq ["default"]]}})))