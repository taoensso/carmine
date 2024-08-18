# Lua scripting

Redis offers powerful [Lua scripting](https://redis.io/docs/interact/programmability/eval-intro) capabilities.

As an example, let's write our own version of the `set` command:

```clojure
(defn my-set
  [key value]
  (car/lua "return redis.call('set', _:my-key, 'lua '.. _:my-val)"
    {:my-key key}   ; Named     key variables and their values
    {:my-val value} ; Named non-key variables and their values
    ))

(wcar*
  (my-set  "foo" "bar")
  (car/get "foo"))
=> ["OK" "lua bar"]
```

Script primitives are also provided: [`eval`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#eval), [`eval-sha`](https://taoensso.github.io/carmine/taoensso.carmine.html#var-evalsha), [`eval*`](https://taoensso.github.io/carmine/taoensso.carmine.html#var-eval*), [`eval-sha*`](https://taoensso.github.io/carmine/taoensso.carmine.html#var-evalsha*).

# Helpers

The [`lua`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#lua) command above is a good example of a Carmine "helper".

Carmine will never surprise you by interfering with the standard Redis command API. But there are times when it might want to offer you a helping hand (if you want it). Compare:

```clojure
(wcar* (car/zunionstore  "dest-key" 3 "zset1" "zset2" "zset3"  "WEIGHTS" 2 3 5))
(wcar* (car/zunionstore* "dest-key"  ["zset1" "zset2" "zset3"] "WEIGHTS" 2 3 5))
```

Both of these calls are equivalent but the latter counted the keys for us. [`zunionstore*`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#zunionstore*) is another helper: a slightly more convenient version of a standard command, suffixed with a `*` to indicate that it's non-standard.

Helpers currently include: [`atomic`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#atomic), [`eval*`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#eval*), [`evalsha*`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#evalsha*), [`info*`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#info*), [`lua`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#lua), [`sort*`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#sort*), [`zinterstore*`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#zinterstore*), and [`zunionstore*`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#zunionstore*).

# Pub/Sub and Listeners

Carmine has a flexible **listener API** to support persistent-connection features like [monitoring](https://redis.io/commands/monitor/) and Redis's [Pub/Sub](https://redis.io/docs/interact/pubsub/) facility:

```clojure
(def my-listener
  (car/with-new-pubsub-listener (:spec server1-conn)
    {"channel1" (fn f1 [msg] (println "f1:" msg))
     "channel*" (fn f2 [msg] (println "f2:" msg))
     "ch*"      (fn f3 [msg] (println "f3:" msg))}
   (car/subscribe  "channel1")
   (car/psubscribe "channel*" "ch*")))
```

Exactly 1 handler fn will trigger per published message *exactly* matching each active subscription:

  - `channel1` handler (`f1`) will trigger for messages to `channel1`.
  - `channel*` handler (`f2`) will trigger for messages to `channel1`, `channel2`, etc.
  - `ch*` handler (`f3`) will trigger for messages to `channel1`, `channel2`, etc.

So publishing to "channel1" in this example will trigger all 3x handlers:

```clojure
(wcar* (car/publish "channel1" "Hello to channel1!"))

;; Will trigger:

(f1 [ "message" "channel1"            "Hello to channel1!"])
(f2 ["pmessage" "channel*" "channel1" "Hello to channel1!"])
(f3 ["pmessage" "ch*"      "channel1" "Hello to channel1!"])
```

You can adjust subscriptions and/or handlers:

```clojure
(with-open-listener my-listener
  (car/unsubscribe) ; Unsubscribe from every channel (leaving patterns alone)
  (car/subscribe "channel3"))

(swap! (:state my-listener) ; {<channel-or-pattern-string> (fn [msg])}
  assoc "channel3" (fn [x] (println "do something")))
```

**Remember to close listeners** when you're done with them:

```clojure
(car/close-listener my-listener)
```

Note that subscriptions are **connection-local**: you can have three different listeners each listening for different messages and using different handlers.

# Reply parsing

Want a little more control over how server replies are parsed? See [`parse`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine#parse):

```clojure
(wcar*
  (car/ping)
  (car/parse clojure.string/lower-case (car/ping) (car/ping))
  (car/ping))
=> ["PONG" "pong" "pong" "PONG"]
```

# Distributed locks

See the [`locks`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.locks) namespace for a simple distributed lock API:

```clojure
(:require [taoensso.carmine.locks :as locks]) ; Add to `ns` macro

(locks/with-lock
  {:pool {<opts>} :spec {<opts>}} ; Connection details
  "my-lock" ; Lock name/identifier
  1000 ; Time to hold lock
  500  ; Time to wait (block) for lock acquisition
  (println "This was printed under lock!"))
```

# Tundra

> **Deprecation notice**: Tundra isn't currently being actively maintained, though it will continue to be supported until Carmine 4. If you are using Tundra, [please let me know](https://www.taoensso.com/contact-me)!

Redis is a beautifully designed datastore that makes some explicit engineering tradeoffs. Probably the most important: your data _must_ fit in memory. Tundra helps relax this limitation: only your **hot** data need fit in memory. How does it work?

 1. Use Tundra's [`dirty`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.tundra#dirty) command **any time you modify/create evictable keys**
 2. Use [`worker`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.tundra#ITundraStore) to create a threaded worker that'll **automatically replicate dirty keys to your secondary datastore**
 3. When a dirty key hasn't been used in a specified TTL, it will be **automatically evicted from Redis** (eviction is optional if you just want to use Tundra as a backup/just-in-case mechanism)
 4. Use [`ensure-ks`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.tundra#ensure-ks) **any time you want to use evictable keys** - this'll extend their TTL or fetch them from your datastore as necessary

That's it: two Redis commands, and a worker!

Tundra uses Redis' own dump/restore mechanism for replication, and Carmine's own [Message queue](./3-Message-queue) to coordinate the replication worker.

It's possible to easily extend support to **any K/V-capable datastore**.  
Implementations are provided out-the-box for:

- [Disk](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.tundra.disk)
- [Amazon S3](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.tundra.s3)
- [Amazon DynamoDB](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.tundra.faraday) via [Faraday](https://www.taoensso.com/faraday)

## Example usage

```clojure
(:require [taoensso.carmine.tundra :as tundra :refer (ensure-ks dirty)]
          [taoensso.carmine.tundra.s3]) ; Add to ns

(def my-tundra-store
  (tundra/tundra-store
    ;; A datastore that implements the necessary (easily-extendable) protocol:
    (taoensso.carmine.tundra.s3/s3-datastore {:access-key "" :secret-key ""}
      "my-bucket/my-folder")))

;; Now we have access to the Tundra API:
(comment
 (worker    my-tundra-store {} {}) ; Create a replication worker
 (dirty     my-tundra-store "foo:bar1" "foo:bar2" ...) ; Queue for replication
 (ensure-ks my-tundra-store "foo:bar1" "foo:bar2" ...) ; Fetch from replica when necessary
)
```

Note that the [Tundra API](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.tundra) makes it convenient to use several different datastores simultaneously (perhaps for different purposes with different latency requirements).