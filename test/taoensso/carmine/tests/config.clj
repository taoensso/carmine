(ns taoensso.carmine.tests.config
  "Shared test configuration for Carmine tests")

;; Redis Cloud connection configuration
;; Change these values to point to your Redis instance
;; (def conn-opts
;;  {:spec {:host "redis-12345.c49334.us-east-1-mz.ec2.cloud.rlrcp.com"
;;          :port 12345
;;          :username "default"
;;          :db 0
;;          :password "YOUR_PASSWORD"
;;          :timeout-ms 4000}})

;; for localhost, use empty map
(def conn-opts {})



;; Benchmark configuration for tests
;; Use smaller values for faster test runs
(def benchmark-opts
  {:n-commands 10000
    :warmup-laps 5000})


