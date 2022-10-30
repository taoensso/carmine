(ns taoensso.carmine.impl.resp
  "Implementation of the Redis RESP3 protocol,
  Ref. https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md"
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string  :as str]
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]

   [taoensso.carmine.impl.common          :as com]
   [taoensso.carmine.impl.resp.common     :as resp-common]
   [taoensso.carmine.impl.resp.read       :as resp-read]
   [taoensso.carmine.impl.resp.write      :as resp-write]
   [taoensso.carmine.impl.resp.read.blobs :as resp-blobs]))

(comment
  (remove-ns 'taoensso.carmine.impl.resp)
  (test/run-tests
    'taoensso.carmine.impl.resp.common
    'taoensso.carmine.impl.resp.read
    'taoensso.carmine.impl.resp.write
    'taoensso.carmine.impl.resp))

;;;; TODO

;; - Document *kv-fns* and provide binding util/s
;; - Finish *push-fn* implementation, document

;; - Interaction between laziness and new lazy arg->ba implementation?

;; - [Perf] Do further benching (grep for `.read`) with:
;; - `jedis.RedisInputStream`: readLineBytes, readIntCrLf, readLongCrLf
;; - `jedis.RedisOutputStream`: writeCrLf, writeIntCrLf

;; - Could add `to-streaming-freeze` that uses the RESP3 API streaming bulk
;;   type to freeze objects directly to output stream (i.e. without intermediary
;;   ba)? Probably most useful for large objects, but complex, involves tradeoffs,
;;   and how often would this be useful?

;; - Add freeze & thaw stuff (incl. bindings)

;; - Check all errors: eids, messages, data
;; - Check all dynamic bindings and sys-vals, ensure accessible

;;;;

(enc/defalias com/str->bytes)
(enc/defalias com/bytes->str)

(enc/defalias resp-read/read-reply)
(enc/defalias resp-write/write-requests)

(enc/defalias resp-write/to-bytes)
(enc/defalias resp-write/to-frozen)

(enc/defalias resp-blobs/as-bytes)
(enc/defalias resp-blobs/as-thawed)
(enc/defalias resp-blobs/as-skipped)
;; etc. ; TODO
