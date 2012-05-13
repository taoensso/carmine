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
 * Target Redis 2.0+, Clojure 1.3+, [Leiningen 2](https://github.com/technomancy/leiningen/wiki/Upgrading). Support Redis [2.6](http://antirez.com/post/redis-2.6-is-near.html)+.
 * Simplicity. That's it. Redis is simple. Its communication protocol is _dead simple_. IMO everything else (including performance, reliability, feature parity, even documentation!) will follow naturally from **keeping the client simple**.

## Getting Started

### Leiningen [![Build Status](https://secure.travis-ci.org/ptaoussanis/carmine.png)](http://travis-ci.org/ptaoussanis/carmine)

Depend on `[carmine "0.7.0-SNAPSHOT"]` in your `project.clj`.

### Requires

```clojure
(ns my-redis-app
  (:require [carmine (core :as r) (connections :as conns)]))
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

The defaults are sensible but see [here](http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericKeyedObjectPool.html) for pool options if you want to fiddle.

Unless you need the added flexibility of specifying the pool and spec for every command, you can save some typing with a little macro:

```clojure
(defmacro redis
  "Like 'with-conn' but doesn't need the pool or spec to be given."
  [& commands] `(conns/with-conn pool spec ~@commands))
```

### Executing Commands

Sending commands is easy:

```clojure
(redis
 (r/ping)
 (r/set "foo" "bar")
 (r/get "foo"))
=> ("PONG" "OK" "bar")
```

Note that sending multiple commands at once like this will employ [pipelining](http://redis.io/topics/pipelining). The responses will be queued server-side and returned all at once as a seq.

What about something more elaborate?

```clojure
(redis
 (r/ping)
 (r/hmset  "myhash" "field1" "Hello" "field2" "World")
 (r/hget   "myhash" "field1")
 (r/hmget  "myhash" "field1" "field2" "nofield")
 (r/set    "foo" "31")
 (r/incrby "foo" "42")
 (r/get    "foo"))
=> ("PONG" "OK" "Hello" ("Hello" "World" nil) "OK" 73 "73")
```

If the server responds with an error, an exception is thrown:

```clojure
(redis (r/spop "foo" "bar"))
=> Exception ERR Operation against a key holding the wrong kind of value
```

But what if we're pipelining?

```clojure
(redis
 (r/set  "foo" "bar")
 (r/spop "foo")
 (r/get  "foo"))
=> ("OK" #<Exception ERR Operation against ...> "bar")
```

### Pub/Sub

Carmine supports Redis's [Publish/Subscribe](http://redis.io/topics/pubsub) feature in a convenient, natural way:

```clojure
(def listener
  (r/make-listener
   spec {"foobar" (fn f1 [resp] (println "Channel match: " resp))
         "foo*"   (fn f2 [resp] (println "Pattern match: " resp))}
   (r/subscribe  "foobar" "foobaz")
   (r/psubscribe "foo*")))
```

Note the map of response handlers. `f1` will trigger when a message is published to channel `foobar`. `f2` will trigger when a message is published to `foobar`, `foobaz`, `foo Abraham Lincoln`, etc.

Publish messages:

```clojure
(redis (r/publish "foobar" "Message to foobar"))
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
  (r/unsubscribe) ; Unsubscribe from every channel (leave patterns alone)
  (r/psubscribe "an-extra-channel"))

(swap! (:handlers listener) assoc "*extra*" (fn [x] (println "EXTRA: " x)))
```

**Remember to close the listener** when you're done with it:

```clojure
(r/close-listener listener)
```

Note that subscriptions are *connection-local*: you can have three different listeners each listening for different messages, using different handlers. This is great stuff.

### Lua

Redis 2.6 introduced a remarkably powerful feature: [Lua scripting](http://redis.io/commands/eval)!

You can send a script to be run in the context of the Redis server:

```clojure
(redis
 (r/eval "return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}" ; The script
         2 "key1" "key2" "arg1" "arg2"))
=> ("key1" "key2" "arg1" "arg2")
```

Big script? Save on bandwidth by sending the SHA1 of a script you've previously sent:

```clojure
(redis
 (r/evalsha "a42059b356c875f0717db19a51f6aaca9ae659ea" ; The script's hash
            2 "key1" "key2" "arg1" "arg2"))
=> ("key1" "key2" "arg1" "arg2")
```

Don't know if the script has already been sent or not? Try this:

```clojure
(r/eval*-with-conn pool spec
  "return redis.call('set',KEYS[1],'bar')" ; The script
  1 "foo")
=> "OK"
```

The `eval*-with-conn` instructs Carmine to optimistically try an `evalsha` command, but fall back to `eval` if the script isn't already cached with the server.

And this is a good example of...

### Helpers

Carmine will never surprise you by interfering with the standard Redis [command API](http://redis.io/commands). But there *are* times when it might want to offer you a helping hand (if you want it).

Compare:

```clojure
(redis (zunionstore "dest-key" 3 "zset1" "zset2" "zset3" "WEIGHTS" 2 3 5))
;; with
(redis (zunionstore* "dest-key" ["zset1" "zset2" "zset3"] "WEIGHTS" 2 3 5))
```

Both of these calls are equivalent but the latter counted the keys for us. `zunionstore*` is a helper: a slightly more convenient version of a standard command, suffixed with a `*` to indicate that it's non-standard.

Helpers currently include: `zinterstore*`, `zunionstore*`, `evalsha*`, `eval*-with-conn`, and `sort*`.

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