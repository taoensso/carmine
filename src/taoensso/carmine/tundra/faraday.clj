(ns taoensso.carmine.tundra.faraday
  "TODO"
  {:author "Peter Taoussanis"}
  (:require [taoensso.faraday       :as far]
            [taoensso.faraday.utils :as futils]
            [taoensso.timbre        :as timbre])
  (:import  [taoensso.carmine.tundra.IDataStore]))

(defrecord FaradayDataStore [creds password]
  IDataStore
  (dump-key [_ key])
  (restore-key [_ key]))