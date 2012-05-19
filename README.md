# Carmine, a Redis client for Clojure

[Redis](http://www.redis.io/) is *awesome* and it's getting [more awesome](http://www.redis.io/commands/eval) [every day](http://redis.io/topics/cluster-spec). It deserves a great Clojure client.

## Aren't There Already A Bunch of Clients?

Plenty. Among them there's [redis-clojure](https://github.com/tavisrudd/redis-clojure), [clj-redis](https://github.com/mmcgrana/clj-redis) based on [Jedis](https://github.com/xetorthio/jedis), [Accession](https://github.com/abedra/accession), and (the newest) [labs-redis-clojure](https://github.com/wallrat/labs-redis-clojure).

Each has its strengths. But these strengths often fail to overlap, leaving one with no easy answer to an obvious question: *which one should you use*?

Carmine is basically an attempt to **cohesively bring together the best bits from each client**.

## Goals

 * Stop the insanity! There's *too many clients* and too many people working at reimplementing the *same things* in essentially the *same way*. By bringing together the work of others, I'm hoping to encourage more folks to **pool their efforts** and get behind one banner. (Rah-rah and all that).
 * **Modern targets**: Redis 2.0+, Clojure 1.3+, [Leiningen 2](https://github.com/technomancy/leiningen/wiki/Upgrading). Full support for Redis [2.6](http://antirez.com/post/redis-2.6-is-near.html)+.
 * **Simplicity**. Redis is simple. Its [communication protocol](http://redis.io/topics/protocol) is *dead simple*. IMO everything else (including performance, reliability, [feature parity](http://redis.io/commands), even documentation!) will follow naturally from *keeping the client simple*.
 * Balance. Appreciate that simplicity needs to be gauged relative to capability or **power**. Aim to keep the client *as simple as possible, but not one bit simpler*.

## Getting Started

### Leiningen [![Build Status](https://secure.travis-ci.org/ptaoussanis/carmine.png)](http://travis-ci.org/ptaoussanis/carmine)

Depend on `[carmine "0.8.0-SNAPSHOT"]` in your `project.clj`.

### Requires

```clojure
(ns my-redis-app
  (:require [carmine (core :as r)]))
```

### Make A Connection

You'll usually want to define one connection spec and pool that you'll reuse:

```clojure
(def pool (r/make-conn-pool :test-while-idle true))
(def spec (r/make-conn-spec :host     "127.0.0.1"
                            :port     9000
                            :password "foobar"
                            :timeout  4000))
```

The defaults are sensible but see [here](http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericKeyedObjectPool.html) for pool options if you want to fiddle.

Unless you need the added flexibility of specifying the pool and spec for every command, you can save some typing with a little macro:

```clojure
(defmacro redis
  "Like 'with-conn' but doesn't need the pool or spec to be given."
  [& body] `(conns/with-conn pool spec ~@body))
```

### Executing Basic Commands

Sending commands is easy:

```clojure
(redis
 (r/ping)
 (r/set "foo" "bar")
 (r/get "foo"))
=> ("PONG" "OK" "bar")
```

Note that sending multiple commands at once like this will employ [pipelining](http://redis.io/topics/pipelining). The replies will be queued server-side and returned all at once as a seq.

What about something a little more elaborate?

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

## Documentation and Command Coverage

Like [labs-redis-clojure](https://github.com/wallrat/labs-redis-clojure), Carmine uses the [official Redis command reference](https://github.com/antirez/redis-doc/blob/master/commands.json) to generate its own command API.

This means that not only is Carmine's command coverage *always complete*, but it's also **fully documented**:

```clojure
(use 'clojure.repl)
(doc r/sort)
=> "SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]

Sort the elements in a list, set or sorted set.

Available since: 1.0.0.

Time complexity: O(N+M*log(M)) where N is the number of elements in the list or set to sort, and M the number of returned elements. When the elements are not sorted, complexity is currently O(N) as there is a copy step that will be avoided in next releases."
```

*Yeah*. Andreas Bielk, you rock.

## Getting Fancy

### Commands Are (Just) Functions

In Carmine, Redis commands are *real functions*. Which means you can *use* them like real functions:

```clojure
(redis
  (doall (repeatedly 5 r/ping)))
=> ("PONG" "PONG" "PONG" "PONG" "PONG")

(let [first-names ["Salvatore"  "Rich"]
      surnames    ["Sanfilippo" "Hickey"]]
  (redis
   (doall (map #(r/set %1 %2) first-names surnames))
   (doall (map r/get first-names))))
=> ("OK" "OK" "Sanfilippo" "Hickey")

(redis
  (doall (map #(r/set (str "key-" %) (rand-int 10)) (range 3)))
  (doall (map #(r/get (str "key-" %)) (range 3))))
=> ("OK" "OK" "OK" "OK" "0" "6" "6" "2")
```

And since real functions can compose, so can Carmine's. By nesting `with-conn` (`redis`) calls, you can fully control how composition and pipelining interact:

```clojure
(let [hash-key "awesome-people"]
  (redis
    (r/hmset hash-key "Rich" "Hickey" "Salvatore" "Sanfilippo")
    (doall (map (partial r/hget hash-key)
                ;; Execute with own connection & pipeline, then return result
                ;; for composition:
                (redis (r/hkeys hash-key))))))
=> ("OK" "Sanfilippo" "Hickey")
```

### Listen Closely

Carmine has a flexible **Listener** API to support persistent-connection features like [monitoring](http://redis.io/commands/monitor) and Redis's fantastic [Publish/Subscribe](http://redis.io/topics/pubsub) feature:

```clojure
(def listener
  (r/with-new-pubsub-listener
   spec {"foobar" (fn f1 [msg] (println "Channel match: " msg))
         "foo*"   (fn f2 [msg] (println "Pattern match: " msg))}
   (r/subscribe  "foobar" "foobaz")
   (r/psubscribe "foo*")))
```

Note the map of message handlers. `f1` will trigger when a message is published to channel `foobar`. `f2` will trigger when a message is published to `foobar`, `foobaz`, `foo Abraham Lincoln`, etc.

Publish messages:

```clojure
(redis (r/publish "foobar" "Hello to foobar!"))
```

Which will trigger:

```clojure
(f1 '("message" "foobar" "Hello to foobar!"))
;; AND ALSO
(f2 '("pmessage" "foo*" "foobar" "Hello to foobar!"))
```

You can adjust subscriptions and/or handlers:

```clojure
(with-open-listener listener
  (r/unsubscribe) ; Unsubscribe from every channel (leave patterns alone)
  (r/psubscribe "an-extra-channel"))

(swap! (:state listener) assoc "*extra*" (fn [x] (println "EXTRA: " x)))
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

Carmine will never surprise you by interfering with the standard Redis command API. But there *are* times when it might want to offer you a helping hand (if you want it).

Compare:

```clojure
(redis (zunionstore "dest-key" 3 "zset1" "zset2" "zset3" "WEIGHTS" 2 3 5))
;; with
(redis (zunionstore* "dest-key" ["zset1" "zset2" "zset3"] "WEIGHTS" 2 3 5))
```

Both of these calls are equivalent but the latter counted the keys for us. `zunionstore*` is a helper: a slightly more convenient version of a standard command, suffixed with a `*` to indicate that it's non-standard.

Helpers currently include: `zinterstore*`, `zunionstore*`, `evalsha*`, `eval*-with-conn`, and `sort*`.

## Performance

Redis is probably most famous for being [*fast*](http://redis.io/topics/benchmarks). Carmine does what it can to hold up its end and currently performs well:

![Performance comparison chart](https://github.com/ptaoussanis/carmine/raw/master/benchmarks/chart20120519.png)

Accession did not finish the test. [Detailed benchmark information] (https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE) is available on Google Docs.

In principle it should be possible to close-in on the theoretical maximum performance of a JVM-based client. This will be an ongoing effort.

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
