(ns taoensso.carmine.tests.config)

(def conn-opts
  "Connection opts passed to `wcar`.
  Example for Redis cloud:
    {:spec
      {:host       \"redis-12345.c49334.us-east-1-mz.ec2.cloud.rlrcp.com\"
       :port       12345
       :username   \"default\"
       :password   \"YOUR_PASSWORD\"
       :timeout-ms 4000
       :db         0}}"
  {})
