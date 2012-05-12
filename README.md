# Carmine, a Redis client for Clojure

[Redis](http://www.redis.io/) is _awesome_ and it's getting [more awesome](http://www.redis.io/commands/eval) [every day](http://redis.io/topics/cluster-spec). It deserves a great Clojure client.

## Aren't There Already A Bunch of Clients?

Sort of. There's:

 * [redis-clojure](https://github.com/tavisrudd/redis-clojure) which is mature and capable, but based on an older version of the [Redis protocol](http://redis.io/topics/protocol).
 * [clj-redis](https://github.com/mmcgrana/clj-redis) which is based on [Jedis](https://github.com/xetorthio/jedis), the Java client. Unfortunately it's not particularly mature or active. And the dependence on Jedis brings with it an uncomfortable (and unnecessary) impedence mismatch.
 * [Accession](https://github.com/abedra/accession), the newest, is based on the Redis 2.0 "unified" protocol. It's beautifully simple and very easy to extend to new Redis features. But it lacks connection pooling and some of the niceties from redis-clojure.

Carmine is basically a little **Accession fork** that attempts to merge **the best bits from Accession and redis-clojure**.

Think of it as an unofficial, "experimental production" branch of Accession. My hope is that if anything useful comes from the experiment, Assession will cherry-pick back into "master".

## Goals

 * Target Redis 2.0+, Clojure 1.3+, [Leiningen 2](https://github.com/technomancy/leiningen/wiki/Upgrading).
 * Simplicity. That's it. Redis is simple. It's communication protocol is _dead simple_. IMO everything else (including performance, reliability, feature parity, even documentation!) will flow naturally from keeping the client simple.

## Getting Started

### Leiningen

Depend on `[carmine "0.7.0-SNAPSHOT"]` in your `project.clj`.

**NOTE**: I'm struggling to login to Clojars atm so please use a `git clone` in the meantime.

### Requires

```clojure
(ns my-redis-app
  (:require [carmine (core :as redis) (connections :as conns)]))
```

### Make A Connection

You'll usually want to define one connection spec and pool that you'll reuse:

```clojure
(def pool (conns/make-conn-pool :test-while-idle true))
(def spec (conns/make-conn-spec :host "127.0.0.1"
                                :port 9000
                                :password "foobar"
                                :timeout 4000))
```

See [here](http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericKeyedObjectPool.html) for pool options.

### Executing Commands

Sending commands is easy:

```clojure
(conns/with-conn pool spec
  (redis/ping)
  (redis/set "foo" "bar")
  (redis/get "foo"))
=> ("PONG" "OK" "bar")
```

Note that sending multiple commands at once like this will employ [pipelining](http://redis.io/topics/pipelining). The responses will be queued server-side and returned all at once as a seq.

If the server responds with an error, an exception is thrown:

```clojure
(conns/with-conn pool spec
  (redis/spop "foo" "bar"))
=> Exception ERR Operation against a key holding the wrong kind of value
```

But what if we're pipelining?

```clojure
(conns/with-conn pool spec
  (redis/set "foo" "bar")
  (redis/spop "foo")
  (redis/get "foo"))
=> ("OK" #<Exception ERR Operation against ...> "bar")
```

### Pub/Sub

Carmine supports Redis's [Publish/Subscribe](http://redis.io/topics/pubsub) feature in a convenient, natural way:

```clojure
(def listener
  (redis/make-listener
   spec {"foo1"  (fn f1 [resp] (println "Channel match: " resp))
         "foo*"  (fn f2 [resp] (println "Pattern match: " resp))}
   (redis/subscribe  "foo1" "foo2")
   (redis/psubscribe "foo*")))
```

Note the map of response handlers. `f1` will trigger when a message is published to channel `foo`. `f2` will trigger when a message is published to `foo1`, `foobar`, foobaz`, etc.

Publish messages:

```clojure
(conns/with-conn pool spec
  (redis/publish "foo1" "Message to foo1"))
```

Which will trigger:

```
(f1 '("message" "foo1" "Message to foo1"))
;; AND
(f2 '("pmessage" "foo*" "foo1" "Message to foo1"))
```

Adjust subscriptions and/or handlers:

```clojure
(with-open-listener listener
  (unsubscribe) ; Unsubscribe from every channel (leave patterns alone)
  (subscribe "extra"))

(swap! (:handlers listener) assoc "extra" (fn [x] (println "EXTRA: " x)))
```

Finally, close the listener when you're done with it:

```clojure
(close-listener listener)
```

### Lua

TODO: Write docs

## Performance

Redis is probably most famous for being _fast_. In principle (mostly because it's so darned simple), it should be possible for Carmine to get "reasonably close to" the reference client's performance.

It'll never be _as_ fast because of the JVM's string handling and the need to un/wrap before communicating with the server. But by avoiding any other unnecessary fluff, I'm hoping that it'll be possible to close-in on the theoretical maximum performance of a JVM-based client.

There's also some interesting serialization possibilities I'd like to look at later...

### Client Benchmarks

TODO: Comparison benchmarks

## Testing

Carmine is still currently *experimental*. It **has not yet been thoroughly tested in production** and it's API is subject to change. Also, it may finish the last carton of milk without telling anyone. Don't say I didn't warn you, because I did.

To run tests against all supported Clojure versions, use
```bash
lein2 all test
```

## Contact & Contribution

Reach me (Peter Taoussanis) at p.taoussanis/at/gmail.com for questions/comments/suggestions/whatever. I'm very open to ideas if you have any!

## License

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.