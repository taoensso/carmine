Carmine includes a simple **distributed message queue** originally inspired by a [post](http://oldblog.antirez.com/post/250) by Redis's original author Salvatore Sanfilippo.

> **Note**: [Carmine 3.3](../releases/tag/v3.3.0-RC1) is introducing major improvements to Carmine's message queue. I plan to update and expand this documentation before the final 3.3 release. - [Peter](https://www.taoensso.com)

# Example usage

```clojure
(:require [taoensso.carmine.message-queue :as car-mq]) ; Add to `ns` macro

(def my-worker
  (car-mq/worker {:pool {<opts>} :spec {<opts>}} "my-queue"
   {:handler (fn [{:keys [message attempt]}]
               (println "Received" message)
               {:status :success})}))

(wcar* (car-mq/enqueue "my-queue" "my message!"))
%> Received my message!

(car-mq/stop my-worker)
```

The following guarantees are provided:

- Messages are persistent (durable) as per Redis config.
- Each message will be handled once and only once.
- Handling is fault-tolerant: a message cannot be lost due to handler crash.
- Message de-duplication can be requested on an ad hoc (per message) basis.

   In these cases, the same message cannot ever be entered into the queue more than once simultaneously or within a (per message) specifiable post-handling backoff period.

See the [message queue API](https://taoensso.github.io/carmine/taoensso.carmine.message-queue.html) for more info.