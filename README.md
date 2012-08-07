Current [semantic](http://semver.org/) version:

```clojure
[com.taoensso/carmine "0.10.0"]
```

**Breaking changes** since _0.9.x_:
 * *Ring session-store*: `connection-pool` and `connection-spec` are now mandatory args.

# Carmine, a Redis client for Clojure

[Redis](http://www.redis.io/) is *awesome* and it's getting [more awesome](http://www.redis.io/commands/eval) [every day](http://redis.io/topics/cluster-spec). It deserves a great Clojure client.

## Aren't There Already A Bunch of Clients?

Plenty: there's [redis-clojure](https://github.com/tavisrudd/redis-clojure), [clj-redis](https://github.com/mmcgrana/clj-redis) based on [Jedis](https://github.com/xetorthio/jedis), [Accession](https://github.com/abedra/accession), and (the newest) [labs-redis-clojure](https://github.com/wallrat/labs-redis-clojure).

Each has its strengths but these strengths often fail to overlap, leaving one with no easy answer to an obvious question: *which one should you use*?

Carmine is an attempt to **cohesively bring together the best bits from each client**. And by bringing together the work of others I'm hoping to encourage more folks to **pool their efforts** and get behind one banner. (Rah-rah and all that).

## What's In The Box?
 * A **high-performance**, all-Clojure client.
 * **Modern targets**: Redis 2.0+ (with full [2.6](http://antirez.com/post/redis-2.6-is-near.html) support), Clojure 1.3+, [Leiningen 2](https://github.com/technomancy/leiningen/wiki/Upgrading) support (not mandatory).
 * Industrial strength **connection pooling**.
 * Complete and accurate command definitions with **full documentation**.
 * Composable, **first-class command functions**.
 * Flexible, high-performance **binary-safe serialization**.
 * Full support for **Lua scripting**, **Pub/Sub**, etc.
 * Full support for custom **reply parsing**.
 * **Command helpers** (`atomically`, `lua-script`, `sort*`, etc.).
 * Pluggable Ring session-store.

## Status [![Build Status](https://secure.travis-ci.org/ptaoussanis/carmine.png?branch=master)](http://travis-ci.org/ptaoussanis/carmine)

Carmine is still currently *experimental*. It **has not yet been thoroughly tested in production** and its API is subject to change. Also, it may finish the last carton of milk without telling anyone. To run tests against all supported Clojure versions, use:

```bash
lein2 all test
```

### Known issue with Java 7 on OSX

Carmine uses [Snappy](http://code.google.com/p/snappy-java/) which currently has a minor path issue with Java 7 on OSX. Please see [here](https://github.com/ptaoussanis/carmine/issues/5#issuecomment-6450607) for a workaround until a proper fix is available.

## Getting Started

### Leiningen

Depend on Carmine in your `project.clj`:

```clojure
[com.taoensso/carmine "0.10.0"]
```

and `require` the library:

```clojure
(ns my-app (:require [taoensso.carmine :as r]))
```

### Make A Connection

You'll usually want to define one connection pool and spec that you'll reuse:

```clojure
(def pool (r/make-conn-pool :max-active 8))
(def spec-server1 (r/make-conn-spec :host     "127.0.0.1"
                                    :port     6379
                                    :password "foobar"
                                    :timeout  4000))
```

The defaults are sensible but see [here](http://commons.apache.org/pool/apidocs/org/apache/commons/pool/impl/GenericKeyedObjectPool.html) for pool options if you want to fiddle.

Unless you need the added flexibility of specifying the pool and spec for each request, you can save some typing with a little macro:

```clojure
(defmacro carmine
  "Acts like (partial with-conn pool spec-server1)."
  [& body] `(r/with-conn pool spec-server1 ~@body))
```

### Basic Commands

Sending commands is easy:

```clojure
(carmine
 (r/ping)
 (r/set "foo" "bar")
 (r/get "foo"))
=> ["PONG" "OK" "bar"]
```

Note that sending multiple commands at once like this will employ [pipelining](http://redis.io/topics/pipelining). The replies will be queued server-side and returned all at once as a vector.

If the server responds with an error, an exception is thrown:

```clojure
(carmine (r/spop "foo" "bar"))
=> Exception ERR Operation against a key holding the wrong kind of value
```

But what if we're pipelining?

```clojure
(carmine
 (r/set  "foo" "bar")
 (r/spop "foo")
 (r/get  "foo"))
=> ["OK" #<Exception ERR Operation against ...> "bar"]
```

### Automatic Serialization

Carmine uses [Nippy](https://github.com/ptaoussanis/nippy) under the hood and understands all of Clojure's [rich data types](http://clojure.org/datatypes), letting you use them with Redis painlessly:

```clojure
(carmine
  (r/set "clj-key" {:bigint (bigint 31415926535897932384626433832795)
                    :vec    (vec (range 5))
                    :set    #{true false :a :b :c :d}
                    :bytes  (byte-array 5)
                    ;; ...
                    })
  (r/get "clj-key")
=> ["OK" {:bigint 31415926535897932384626433832795N
          :vec    [0 1 2 3 4]
          :set    #{true false :a :c :b :d}
          :bytes  #<byte [] [B@4d66ea88>}]
```

Any argument to a Redis command that's *not* a string will be automatically serialized using a **high-speed, binary-safe protocol** that falls back to Clojure's own Reader for tougher jobs.

**WARNING**: With Carmine you **must** manually string-ify arguments that you want Redis to interpret/store in its own native (string) format:

```clojure
(carmine
  ;; String argument
  (r/set  "has-string" "13")
  (r/incr "has-string")
  (r/get  "has-string")        ; This will return a string

  ;; Float argument
  (r/set  "has-serialized" 13) ; Will be serialized! Not what we want here.
  (r/incr "has-serialized")    ; This will throw an exception!
  (r/get  "has-serialized")    ; This will return a (deserialized) float.
  )
=> ["OK" 14 "14" "OK" #<Exception ...> 13]
```

This scheme is consistent, unambiguous, and simple. But it requires a little carefulness while you're getting used to it as it's different from the way most other clients work.

### Documentation and Command Coverage

Like [labs-redis-clojure](https://github.com/wallrat/labs-redis-clojure), Carmine uses the [official Redis command reference](https://github.com/antirez/redis-doc/blob/master/commands.json) to generate its own command API. Which means that not only is Carmine's command coverage *always complete*, but it's also **fully documented**:

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

### Lua

Redis 2.6 introduced a remarkably powerful feature: server-side Lua scripting! As an example, let's write our own version of the `set` command:

```clojure
(defn my-set
  [key value]
  (r/lua-script "return redis.call('set', _:my-key, 'lua '.. _:my-value)"
                {:my-key key}     ; Named key variables and their values
                {:my-value value} ; Named non-key variables and their values
                ))

(carmine (my-set "foo" "bar")
         (get "foo"))
=> ["OK" "lua bar"]
```

Script primitives are also provided: `eval`, `eval-sha`, `eval*`, `eval-sha*`. See the [Lua scripting docs](http://redis.io/commands/eval) for more info.

### Helpers

The `lua-script` command above is a good example of a Carmine *helper*.

Carmine will never surprise you by interfering with the standard Redis command API. But there are times when it might want to offer you a helping hand (if you want it). Compare:

```clojure
(carmine (r/zunionstore "dest-key" "3" "zset1" "zset2" "zset3" "WEIGHTS" "2" "3" "5"))
(carmine (r/zunionstore* "dest-key" ["zset1" "zset2" "zset3"] "WEIGHTS" "2" "3" "5"))
```

Both of these calls are equivalent but the latter counted the keys for us. `zunionstore*` is another helper: a slightly more convenient version of a standard command, suffixed with a `*` to indicate that it's non-standard.

Helpers currently include: `atomically`, `eval*`, `evalsha*`, `hgetall*`, `info*`, `lua-script`, `sort*`, `zinterstore*`, and `zunionstore*`. See their docstrings for more info.

### Commands Are (Just) Functions

In Carmine, Redis commands are *real functions*. Which means you can *use* them like real functions:

```clojure
(carmine
  (doall (repeatedly 5 r/ping)))
=> ["PONG" "PONG" "PONG" "PONG" "PONG"]

(let [first-names ["Salvatore"  "Rich"]
      surnames    ["Sanfilippo" "Hickey"]]
  (carmine
   (doall (map #(r/set %1 %2) first-names surnames))
   (doall (map r/get first-names))))
=> ["OK" "OK" "Sanfilippo" "Hickey"]

(carmine
  (doall (map #(r/set (str "key-" %) (rand-int 10)) (range 3)))
  (doall (map #(r/get (str "key-" %)) (range 3))))
=> ["OK" "OK" "OK" "OK" "0" "6" "6" "2"]
```

And since real functions can compose, so can Carmine's. By nesting `with-conn`/`carmine` calls, you can fully control how composition and pipelining interact:

```clojure
(let [hash-key "awesome-people"]
  (carmine
    (r/hmset hash-key "Rich" "Hickey" "Salvatore" "Sanfilippo")
    (doall (map (partial r/hget hash-key)
                ;; Execute with own connection & pipeline then return result
                ;; for composition:
                (carmine (r/hkeys hash-key))))))
=> ["OK" "Sanfilippo" "Hickey"]
```

### Listen Closely

Carmine has a flexible **Listener** API to support persistent-connection features like [monitoring](http://redis.io/commands/monitor) and Redis's fantastic [Publish/Subscribe](http://redis.io/topics/pubsub) feature:

```clojure
(def listener
  (r/with-new-pubsub-listener
   server1-spec {"foobar" (fn f1 [msg] (println "Channel match: " msg))
                 "foo*"   (fn f2 [msg] (println "Pattern match: " msg))}
   (r/subscribe  "foobar" "foobaz")
   (r/psubscribe "foo*")))
```

Note the map of message handlers. `f1` will trigger when a message is published to channel `foobar`. `f2` will trigger when a message is published to `foobar`, `foobaz`, `foo Abraham Lincoln`, etc.

Publish messages:

```clojure
(carmine (r/publish "foobar" "Hello to foobar!"))
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

### Custom Reply Parsing

Want a little more control over how server replies are parsed? You have all the control you need:

```clojure
(carmine
  (r/ping)
  (r/with-parser clojure.string/lower-case (r/ping) (r/ping))
  (r/ping))
=> ["PONG" "pong" "pong" "PONG"]
```

### Low-level Binary Data

Carmine's serializer has no problem handling arbitrary byte[] data. But the serializer involves overhead that may not always be desireable. So for maximum flexibility Carmine gives you automatic, *zero-overhead* read and write facilities for raw binary data:

```clojure
(carmine
  (r/set "bin-key" (byte-array 50))
  (r/get "bin-key"))
=> ["OK" [#<byte[] [B@7c3ab3b4> 50]]
```

## Performance

Redis is probably most famous for being [*fast*](http://redis.io/topics/benchmarks). Carmine does what it can to hold up its end and currently performs well:

![Performance comparison chart](https://github.com/ptaoussanis/carmine/raw/master/benchmarks/chart.png)

Accession could not complete the requests. [Detailed benchmark information](https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE) is available on Google Docs.

In principle it should be possible to get close to the theoretical maximum performance of a JVM-based client. This will be an ongoing effort but please note that my first concern for Carmine is **performance-per-unit-power** rather than *absolute performance*. For example Carmine willingly pays a small throughput penalty to support binary-safe arguments and again for composable commands. 

Likewise, I'll happily trade a little less throughput for simpler code.

### YourKit

Carmine was developed with the help of the [YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp). YourKit, LLC kindly supports open source projects by offering an open source license. They also make the [YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).

## Carmine supports the ClojureWerkz Project Goals

ClojureWerkz is a growing collection of open-source, batteries-included [Clojure libraries](http://clojurewerkz.org/) that emphasise modern targets, great documentation, and thorough testing.

## Contact & Contribution

Reach me (Peter Taoussanis) at *ptaoussanis at gmail.com* for questions/comments/suggestions/whatever. I'm very open to ideas if you have any!

I'm also on Twitter: [@ptaoussanis](https://twitter.com/#!/ptaoussanis).

## License

Copyright &copy; 2012 Peter Taoussanis

Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
