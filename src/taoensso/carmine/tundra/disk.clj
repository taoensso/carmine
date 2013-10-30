(ns taoensso.carmine.tundra.disk
  "Simple disk-based DataStore implementation for Tundra."
  {:author "Peter Taoussanis"}
  (:require [taoensso.timbre :as timbre])
  (:import  [taoensso.carmine.tundra IDataStore]))

;; TODO

;; (defrecord DiskDataStore [path]
;;   IDataStore
;;   (put-key   [dstore k v])
;;   (fetch-key [dstore k]))
