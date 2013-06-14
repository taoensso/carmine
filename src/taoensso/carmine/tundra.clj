(ns taoensso.carmine.tundra
  "Something very cool may be here soon..."
  {:author "Peter Taoussanis"}
  (:require [taoensso.carmine       :as car]
            [taoensso.carmine.utils :as cutils]
            [taoensso.nippy         :as nippy ]
            [taoensso.timbre        :as timbre]))

;;;; Interfaces

(defprotocol IFreezer
  "TODO"
  (freeze ^bytes [freezer x])
  (thaw [freezer ba]))

(defprotocol IDataStore
  "TODO"
  (dump-key    [store key])
  (restore-key [store key]))

;;;; Default implementations

(defrecord NippyFreezer [opts]
  IFreezer
  (freeze [_ x])
  (thaw [_ ba]))

(defrecord DiskDataStore [path]
  IDataStore
  (dump-key [_ key])
  (restore-key [_ key]))

;;;; Redis commands