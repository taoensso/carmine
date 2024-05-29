Carmine includes a simple **distributed message queue** originally inspired by a [post](http://oldblog.antirez.com/post/250) by Redis's original author Salvatore Sanfilippo.

# API

See linked docstrings below for features and usage:

| Name                                                                                                                            | Description                                                              |
| :------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------ |
| [`worker`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#worker)                         | Returns a worker for named queue. Deref worker for detailed status info! |
| [`enqueue`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#enqueue)                       | Enqueues given message for processing by active worker/s.                |
| [`set-min-log-level!`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#set-min-log-level!) | Sets minimum log level for message queue logs.                           |

# Example


```clojure
(def my-conn-opts {:pool {<opts>} :spec {<opts>}})

(def my-worker
  (car-mq/worker my-conn-opts "my-queue"
    {:handler
     (fn [{:keys [message attempt]}]
       (try
         (println "Received" message)
         {:status :success}
         (catch Throwable _
           (println "Handler error!")
           {:status :retry})))}))

(wcar* (car-mq/enqueue "my-queue" "my message!"))

;; Deref your worker to get detailed status info
@my-worker =>
{:qname     "my-queue"
 :opts      <opts-map>
 :conn-opts <my-conn-opts>
 :running?  true
 :nthreads  {:worker 1, :handler 1}
 :stats
 {:queue-size        {:last 1332, :max 1352, :p90 1323, ...}
  :queueing-time-ms  {:last 203,  :max 4774, :p90 300,  ...}
  :handling-time-ms  {:last 11,   :max 879,  :p90 43,   ...}
  :counts
  {:handler/success     5892
   :handler/retry       808
   :handler/error       2
   :handler/backoff     2034
   :sleep/end-of-circle 350}}}
```

# Semantics

The following semantics are provided:

- Messages are **persistent** (durable as per Redis config).
- Messages are **handled once and only once**.
- Messages are **handled in loose order** (exact order may be affected by the number of concurrent handler threads, and retry/backoff features, etc.).
- Messages are **fault-tolerant** (preserved until acknowledged as handled).
- Messages support optional per-message **de-duplication**, preventing the same message from being simultaneously queued more than once within a configurable per-message backoff period.
- Messages are encoded using [nippy](https://github.com/taoensso/nippy) and stored as string values in Redis hashes. Each message (after being encoded) may have a max size of 512MB
