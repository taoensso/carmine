(ns carmine.benchmarks)

;; TODO

;; (comment

;;   ;;    Ref: +/- 48k SETs/s for redis-benchmark  -n 100k -c 5
;;   ;;         +/- 14k SETs/s for jedis-benchmark* -n 100k -t 5 -c 5 -s 1
;;   ;;         +/- 28k SETs/s for redis-clojure :requests 100k :clients 5
;;   ;; Result: +/- 33k SETs/s for accession
;;   ;;  Notes: +/- 10-15% time spent generating query, cmp #=(command ...)
;;   ;;             which leaves about 20% unaccounted-for impact relative to
;;   ;;             redis-benchmark.
;;   ;;  * https://github.com/sheki/jedis-benchmark

;;   ;; TODO Doing something wrong? Why are the Jedis results so bad?

;;   (let [spec (make-conn-spec)
;;         pool (make-conn-pool :max-total 5 :test-on-borrow false
;;                                    :test-on-return false)]
;;     (println "---")
;;     (time
;;      (let [per (int (/ 100000 5)) ; Desired ops / concurrent clients
;;            op  (fn [] (dotimes [n per]
;;                        (with-connection pool spec
;;                          (set "key" "value"))))
;;            c1 (future (op)) c2 (future (op)) c3 (future (op))
;;            c4 (future (op)) c5 (future (op))]
;;        [@c1 @c2 @c3 @c4 @c5]))))