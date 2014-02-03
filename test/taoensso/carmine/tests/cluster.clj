(ns taoensso.carmine.tests.cluster
  (:require [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.cluster :as clu]))


(expect true (clu/moved? (Exception. "MOVED 12182 127.0.0.1:7002")))
(expect false (clu/moved? (Exception. "ERR other")))

(expect [12 50] (clu/parse-slots "12-50"))
(expect nil? (clu/parse-slots "[12-50]"))

(expect [12182 {:host "127.0.0.1" :port 7002}] (clu/parse-redirect "MOVED 12182 127.0.0.1:7002"))

(expect 44950 (clu/crc16 "foo"))
(expect 0x31c3 (clu/crc16 "123456789"))

(expect 12182 (clu/keyslot "foo"))

(comment (test/run-tests '[taoensso.carmine.tests.cluster]))
