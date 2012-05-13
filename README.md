# Carmine, a Redis client for Clojure

[Redis](http://www.redis.io/) is _awesome_ and it's getting [more awesome](http://www.redis.io/commands/eval) [every day](http://redis.io/topics/cluster-spec). It deserves a great Clojure client.

## Aren't There Already A Bunch of Clients?

Sort of. There's:

 * [redis-clojure](https://github.com/tavisrudd/redis-clojure) which is mature and capable, but based on an older version of the [Redis protocol](http://redis.io/topics/protocol).
 * [clj-redis](https://github.com/mmcgrana/clj-redis) which is based on [Jedis](https://github.com/xetorthio/jedis), the Java client. Unfortunately it's not particularly mature or active. And the dependence on Jedis brings with it an uncomfortable (and unnecessary) impedence mismatch.
 * [Accession](https://github.com/abedra/accession), the newest, is based on the Redis 2.0 "unified" protocol. It's beautifully simple and very easy to extend to new Redis features. But it lacks connection pooling and some of the niceties from redis-clojure.

Carmine is basically a little **Accession fork** that attempts to merge **the best bits from Accession and redis-clojure**.

Think of it as an unofficial, "experimental production" branch of Accession. My hope is that if anything useful comes from the experiment, Assession will cherry-pick back into "master".

**Update**: Andreas Bielk also has a [client](https://github.com/wallrat/labs-redis-clojure) that I haven't had a chance to look at yet. Will do shortly!

## Goals

 * Stop the insanity! There's *too many clients* and too many people working at reimplementing the *same things* in essentially the *same way*. By bringing together the work of others, I'm hoping to encourage more folks to **pool their efforts** and get behind one banner. (Rah-rah and all that).
 * Target Redis 2.0+, Clojure 1.3+, [Leiningen 2](https://github.com/technomancy/leiningen/wiki/Upgrading).
 * Simplicity. That's it. Redis is simple. Its communication protocol is _dead simple_. IMO everything else (including performance, reliability, feature parity, even documentation!) will follow naturally from **keeping the client simple**.

## Getting Started

### Leiningen

Depend on `[carmine "0.7.0-SNAPSHOT"]` in your `project.clj`.

### Requires

```clojure
(ns my-redis-app
  (:require [carmine (core :as redis) (connections :as conns)]))
```

### Make A Connection

You'll usually want to define one connection spec and pool that you'll reuse:

```clojure
(def pool (conns/make-conn-pool :test-while-idle true))
(def spec (conns/make-conn-spec :host     "127.0.0.1"
                                :port     9000
                                :password "foobar"
                                :timeout  4000))
```

The defaults are sensible, but see [here](http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericKeyedObjectPool.html) for pool options if you want to fiddle.

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

What about something more elaborate?

```clojure
(conns/with-conn pool spec
  (redis/ping)
  (redis/hmset "myhash" "field1" "Hello" "field2" "World")
  (redis/hget  "myhash" "field1")
  (redis/hmget "myhash" "field1" "field2" "nofield")
  (redis/set    "foo" "31")
  (redis/incrby "foo" "42")
  (redis/get    "foo"))
=> ("PONG" "OK" "Hello" ("Hello" "World" nil) "OK" 73 "73")
```

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
   spec {"foobar" (fn f1 [resp] (println "Channel match: " resp))
         "foo*"   (fn f2 [resp] (println "Pattern match: " resp))}
   (redis/subscribe  "foobar" "foobaz")
   (redis/psubscribe "foo*")))
```

Note the map of response handlers. `f1` will trigger when a message is published to channel `foobar`. `f2` will trigger when a message is published to `foobar`, `foobaz`, `foo Abraham Lincoln`, etc.

Publish messages:

```clojure
(conns/with-conn pool spec
  (redis/publish "foobar" "Message to foobar"))
```

Which will trigger:

```clojure
(f1 '("message" "foobar" "Message to foobar"))
;; AND ALSO
(f2 '("pmessage" "foo*" "foobar" "Message to foobar"))
```

You can adjust subscriptions and/or handlers:

```clojure
(with-open-listener listener
  (unsubscribe) ; Unsubscribe from every channel (leave patterns alone)
  (psubscribe "an-extra-channel"))

(swap! (:handlers listener) assoc "*extra*" (fn [x] (println "EXTRA: " x)))
```

**Remember to close the listener** when you're done with it:

```clojure
(close-listener listener)
```

Note that subscriptions are *connection-local*: you can have three different listeners each listening for different messages, using different handlers. This is great stuff.

### Lua

Redis 2.6 introduced a remarkably powerful feature: [Lua](http://en.wikipedia.org/wiki/Lua_(programming_language)) scripting!

You can send a script to Redis:

```clojure
```

TODO: Write docs

## Performance

Redis is probably most famous for being [_fast_](http://redis.io/topics/benchmarks). In principle (mostly because it's so darned simple), it should be possible for Carmine to get "reasonably close to" the reference client's performance.

It'll never be _as_ fast because of the JVM's string handling and the need to un/wrap before communicating with the server. But by avoiding any other unnecessary fluff, I'm hoping that it'll be possible to close-in on the theoretical maximum performance of a JVM-based client.

There's also some interesting serialization possibilities I'd like to look at later...

### Client Benchmarks

TODO: Comparison benchmarks

## Testing

Carmine is still currently *experimental*. It **has not yet been thoroughly tested in production** and its API is subject to change. Also, it may finish the last carton of milk without telling anyone. Don't say I didn't warn you, because I did.

To run tests against all supported Clojure versions, use
```bash
lein2 all test
```

## Contact & Contribution

Reach me (Peter Taoussanis) at p.taoussanis at gmail.com for questions/comments/suggestions/whatever. I'm very open to ideas if you have any! Seriously: try me ;)

## License

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.