Current [semantic](http://semver.org/) version:

```clojure
[com.taoensso/carmine "1.7.0-beta6"] ; Development
[com.taoensso/carmine "1.6.0"]       ; Stable
```

**NOTE:** 1.7.x should be backwards-compatible with 1.6.x, but I'd recommend you test it if you're using a lot of low-level facilities (manual pipeline control and reply parsing, etc.). Please do report any problems!

# Carmine, a Clojure Redis client & message queue

[Redis](http://www.redis.io/) is *awesome* and it's getting [more awesome](http://www.redis.io/commands/eval) [every day](http://redis.io/topics/cluster-spec). It deserves a great Clojure client.

## Aren't There Already A Bunch of Clients?

Plenty: there's [redis-clojure](https://github.com/tavisrudd/redis-clojure), [clj-redis](https://github.com/mmcgrana/clj-redis) based on [Jedis](https://github.com/xetorthio/jedis), [Accession](https://github.com/abedra/accession), and (the newest) [labs-redis-clojure](https://github.com/wallrat/labs-redis-clojure).

Each has its strengths but these strengths often fail to overlap, leaving one with no easy answer to an obvious question: *which one should you use*?

Carmine is an attempt to **cohesively bring together the best bits from each client**. And by bringing together the work of others I'm hoping to encourage more folks to **pool their efforts** and get behind one banner. (Rah-rah and all that).

## What's In The Box?
 * Small, uncomplicated **all-Clojure** library.
 * **Good performance**.
 * **Modern targets**: Redis 2.0+ (with full [2.6](http://antirez.com/post/redis-2.6-is-near.html) support), Clojure 1.3+, [Leiningen 2](https://github.com/technomancy/leiningen/wiki/Upgrading) support (not mandatory).
 * Industrial strength **connection pooling**.
 * Complete and accurate command definitions with **full documentation**.
 * Composable, **first-class command functions**.
 * Flexible, high-performance **binary-safe serialization**.
 * Full support for **Lua scripting**, **Pub/Sub**, etc.
 * Full support for custom **reply parsing**.
 * **Command helpers** (`atomically`, `lua-script`, `sort*`, etc.).
 * Simple, high-performance **message queue** (Redis 2.6+).
 * Simple, high-performance **distributed lock** (Redis 2.6+).
 * **Ring session-store**.

## Getting Started

### Leiningen

Depend on Carmine in your `project.clj`:

```clojure
[com.taoensso/carmine "1.6.0"]
```

and `require` the library:

```clojure
(ns my-app (:require [taoensso.carmine :as car]))
```

### Connections

You'll usually want to define one connection pool and spec that you'll reuse:

```clojure
(def pool         (car/make-conn-pool)) ; See docstring for additional options
(def spec-server1 (car/make-conn-spec)) ; ''
```

Unless you need the added flexibility of specifying the pool and spec for each request, you can save some typing with a little macro:

```clojure
(defmacro wcar [& body] `(car/with-conn pool spec-server1 ~@body))
```

### Basic Commands

Sending commands is easy:

```clojure
(wcar (car/ping)
      (car/set "foo" "bar")
      (car/get "foo"))
=> ["PONG" "OK" "bar"]
```

Note that sending multiple commands at once like this will employ [pipelining](http://redis.io/topics/pipelining). The replies will be queued server-side and returned all at once as a vector.

If the server responds with an error, an exception is thrown:

```clojure
(wcar (car/spop "foo" "bar"))
=> Exception ERR Operation against a key holding the wrong kind of value
```

But what if we're pipelining?

```clojure
(wcar (car/set  "foo" "bar")
      (car/spop "foo")
      (car/get  "foo"))
=> ["OK" #<Exception ERR Operation against ...> "bar"]
```

### Serialization

The only value type known to Redis internally is the [byte string](http://redis.io/topics/data-types). But Carmine uses [Nippy](https://github.com/ptaoussanis/nippy) under the hood and understands all of Clojure's [rich data types](http://clojure.org/datatypes), letting you use them with Redis painlessly:

```clojure
(wcar (car/set "clj-key" {:bigint (bigint 31415926535897932384626433832795)
                          :vec    (vec (range 5))
                          :set    #{true false :a :b :c :d}
                          :bytes  (byte-array 5)
                          ;; ...
                          })
      (car/get "clj-key"))
=> ["OK" {:bigint 31415926535897932384626433832795N
          :vec    [0 1 2 3 4]
          :set    #{true false :a :c :b :d}
          :bytes  #<byte [] [B@4d66ea88>}]
```

Types are handled as follows:
 * Clojure strings become Redis strings.
 * Simple Clojure numbers (integers, longs, floats, doubles) become Redis strings.
 * Everything else gets automatically de/serialized.

You can force automatic de/serialization for an argument of any type by wrapping it with `car/serialize`.

### Documentation and Command Coverage

Like [labs-redis-clojure](https://github.com/wallrat/labs-redis-clojure), Carmine uses the [official Redis command reference](https://github.com/antirez/redis-doc/blob/master/commands.json) to generate its own command API. Which means that not only is Carmine's command coverage *always complete*, but it's also **fully documented**:

```clojure
(use 'clojure.repl)
(doc car/sort)
=> "SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]

Sort the elements in a list, set or sorted set.

Available since: 1.0.0.

Time complexity: O(N+M*log(M)) where N is the number of elements in the list or set to sort, and M the number of returned elements. When the elements are not sorted, complexity is currently O(N) as there is a copy step that will be avoided in next releases."
```

*Yeah*. Andreas Bielk, you rock.

### Lua

Redis 2.6 introduced a remarkably powerful feature: server-side Lua scripting! As an example, let's write our own version of the `set` command:

```clojure
(defn my-set
  [key value]
  (car/lua-script "return redis.call('set', _:my-key, 'lua '.. _:my-value)"
                  {:my-key key}     ; Named key variables and their values
                  {:my-value value} ; Named non-key variables and their values
                  ))

(wcar (my-set "foo" "bar")
      (car/get "foo"))
=> ["OK" "lua bar"]
```

Script primitives are also provided: `eval`, `eval-sha`, `eval*`, `eval-sha*`. See the [Lua scripting docs](http://redis.io/commands/eval) for more info.

### Helpers

The `lua-script` command above is a good example of a Carmine *helper*.

Carmine will never surprise you by interfering with the standard Redis command API. But there are times when it might want to offer you a helping hand (if you want it). Compare:

```clojure
(wcar (car/zunionstore "dest-key" 3 "zset1" "zset2" "zset3" "WEIGHTS" 2 3 5))
(wcar (car/zunionstore* "dest-key" ["zset1" "zset2" "zset3"] "WEIGHTS" 2 3 5))
```

Both of these calls are equivalent but the latter counted the keys for us. `zunionstore*` is another helper: a slightly more convenient version of a standard command, suffixed with a `*` to indicate that it's non-standard.

Helpers currently include: `atomically`, `eval*`, `evalsha*`, `hgetall*`, `info*`, `lua-script`, `sort*`, `zinterstore*`, and `zunionstore*`. See their docstrings for more info.

### Commands Are (Just) Functions

In Carmine, Redis commands are *real functions*. Which means you can *use* them like real functions:

```clojure
(wcar (doall (repeatedly 5 car/ping)))
=> ["PONG" "PONG" "PONG" "PONG" "PONG"]

(let [first-names ["Salvatore"  "Rich"]
      surnames    ["Sanfilippo" "Hickey"]]
  (wcar (doall (map #(car/set %1 %2) first-names surnames))
        (doall (map car/get first-names))))
=> ["OK" "OK" "Sanfilippo" "Hickey"]

(wcar (doall (map #(car/set (str "key-" %) (rand-int 10)) (range 3)))
      (doall (map #(car/get (str "key-" %)) (range 3))))
=> ["OK" "OK" "OK" "OK" "0" "6" "6" "2"]
```

And since real functions can compose, so can Carmine's. By nesting `with-conn`/`wcar` calls, you can fully control how composition and pipelining interact:

```clojure
(let [hash-key "awesome-people"]
  (wcar (car/hmset hash-key "Rich" "Hickey" "Salvatore" "Sanfilippo")
        (doall (map (partial car/hget hash-key)
                    ;; Execute with own connection & pipeline then return result
                    ;; for composition:
                    (wcar (car/hkeys hash-key))))))
=> ["OK" "Sanfilippo" "Hickey"]
```

### Listeners & Pub/Sub

Carmine has a flexible **Listener** API to support persistent-connection features like [monitoring](http://redis.io/commands/monitor) and Redis's fantastic [Publish/Subscribe](http://redis.io/topics/pubsub) feature:

```clojure
(def listener
  (car/with-new-pubsub-listener
   spec-server1 {"foobar" (fn f1 [msg] (println "Channel match: " msg))
                 "foo*"   (fn f2 [msg] (println "Pattern match: " msg))}
   (car/subscribe  "foobar" "foobaz")
   (car/psubscribe "foo*")))
```

Note the map of message handlers. `f1` will trigger when a message is published to channel `foobar`. `f2` will trigger when a message is published to `foobar`, `foobaz`, `foo Abraham Lincoln`, etc.

Publish messages:

```clojure
(wcar (car/publish "foobar" "Hello to foobar!"))
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
  (car/unsubscribe) ; Unsubscribe from every channel (leave patterns alone)
  (car/psubscribe "an-extra-channel"))

(swap! (:state listener) assoc "*extra*" (fn [x] (println "EXTRA: " x)))
```

**Remember to close the listener** when you're done with it:

```clojure
(car/close-listener listener)
```

Note that subscriptions are *connection-local*: you can have three different listeners each listening for different messages, using different handlers. This is great stuff.

### Reply Parsing

Want a little more control over how server replies are parsed? You have all the control you need:

```clojure
(wcar (car/ping)
      (car/with-parser clojure.string/lower-case (car/ping) (car/ping))
      (car/ping))
=> ["PONG" "pong" "pong" "PONG"]
```

### Binary Data

Carmine's serializer has no problem handling arbitrary byte[] data. But the serializer involves overhead that may not always be desireable. So for maximum flexibility Carmine gives you automatic, *zero-overhead* read and write facilities for raw binary data:

```clojure
(wcar (car/set "bin-key" (byte-array 50))
      (car/get "bin-key"))
=> ["OK" [#<byte[] [B@7c3ab3b4> 50]]
```

### Message Queue

**Redis 2.6+ only, currently ALPHA QUALITY**

Redis makes a great [message queue server](http://antirez.com/post/250):

```clojure
(:require [taoensso.carmine.message-queue :as mq]) ; Add to `ns` macro

(def my-worker
  (mq/make-dequeue-worker
   pool spec-server1 "my-queue"
   :handler-fn (fn [msg] (println "Received" msg))))

(mq/enqueue "my-queue" "my message!")
%> Received my message!

(mq/stop my-worker)
```

Look simple? It is. But it's also distributed, fault-tolerant, and _fast_. See the `taoensso.carmine.message-queue` namespace for details.

### Distributed Locks

```clojure
(:require [taoensso.carmine.locks :as locks]) ; Add to `ns` macro

(locks/with-lock "my-lock"
  1000 ; Time to hold lock
  500  ; Time to wait (block) for lock acquisition
  (println "This was printed under lock!"))
```

Again: simple, distributed, fault-tolerant, and _fast_. See the `taoensso.carmine.locks` namespace for details.

## Performance

Redis is probably most famous for being [fast](http://redis.io/topics/benchmarks). Carmine does what it can to hold up its end and currently performs well:

![Performance comparison chart](https://github.com/ptaoussanis/carmine/raw/master/benchmarks/chart.png)

Accession could not complete the requests. [Detailed benchmark information](https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE) is available on Google Docs. Note that these numbers are for _unpipelined_ requests: you could do a _lot_ more with pipelining.

In principle it should be possible to get close to the theoretical maximum performance of a JVM-based client. This will be an ongoing effort but please note that my first concern for Carmine is **performance-per-unit-power** rather than *absolute performance*. For example Carmine willingly pays a small throughput penalty to support binary-safe arguments and again for composable commands. 

Likewise, I'll happily trade a little less throughput for simpler code.

### YourKit

Carmine was developed with the help of the [YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp). YourKit, LLC kindly supports open source projects by offering an open source license. They also make the [YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).

## Carmine Supports the ClojureWerkz and CDS Project Goals

ClojureWerkz is a growing collection of open-source, batteries-included [Clojure libraries](http://clojurewerkz.org/) that emphasise modern targets, great documentation, and thorough testing.

CDS (Clojure Documentation Site) is a contributor-friendly community project aimed at producing top-notch [Clojure tutorials](http://clojure-doc.org/) and documentation.

## Contact & Contribution

Reach me (Peter Taoussanis) at [taoensso.com](https://www.taoensso.com) for questions/comments/suggestions/whatever. I'm very open to ideas if you have any! I'm also on Twitter: [@ptaoussanis](https://twitter.com/#!/ptaoussanis).

## License

Copyright &copy; 2012 Peter Taoussanis. Distributed under the [Eclipse Public License](http://www.eclipse.org/legal/epl-v10.html), the same as Clojure.
