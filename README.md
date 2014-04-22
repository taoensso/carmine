**[API docs][]** | **[CHANGELOG][]** | [other Clojure libs][] | [Twitter][] | [contact/contributing](#contact--contributing) | current ([semantic][]) version:

```clojure
[com.taoensso/carmine "2.6.0"] ; Stable
```

v2.6 is a **major, backwards-compatible release** that lays the groundwork for an upcoming v3 with Redis Cluster support. See the [CHANGELOG][] for details.

# Carmine, a Clojure Redis client & message queue

[Redis](http://www.redis.io/) is _awesome_ and it's getting [more awesome](http://www.redis.io/commands/eval) [every day](http://redis.io/topics/cluster-spec). It deserves a great Clojure client.

##### Aren't there already a bunch of clients?

Plenty: there's [redis-clojure](https://github.com/tavisrudd/redis-clojure), [clj-redis](https://github.com/mmcgrana/clj-redis) based on [Jedis](https://github.com/xetorthio/jedis), [Accession](https://github.com/abedra/accession), and (the newest) [labs-redis-clojure](https://github.com/wallrat/labs-redis-clojure). Each has its strengths but these strengths often fail to overlap, leaving one with no easy answer to an obvious question: _which one should you use_?

Carmine is an attempt to **cohesively bring together the best bits from each client**. And by bringing together the work of others I'm hoping to encourage more folks to **pool their efforts** and get behind one banner. (Rah-rah and all that).

## What's in the box™?
  * Small, uncomplicated **all-Clojure** library.
  * **Fully documented**, up-to-date API, with full support for the latest Redis versions.
  * **Great performance**.
  * Industrial strength **connection pooling**.
  * Composable, **first-class command functions**.
  * Flexible, high-performance **binary-safe serialization** using [Nippy](https://github.com/ptaoussanis/nippy).
  * Full support for **Lua scripting**, **Pub/Sub**, etc.
  * Full support for custom **reply parsing**.
  * **Command helpers** (`atomic`, `lua`, `sort*`, etc.).
  * **Ring session-store**.
  * Simple, high-performance **message queue** (Redis 2.6+, stable v2+).
  * Simple, high-performance **distributed lock** (Redis 2.6+, stable v2+).
  * Pluggable **compression** and **encryption** support. (v2+)
  * Includes _Tundra_, an API for **replicating data to an additional datastore**. (Redis 2.6+, v2+)

## Getting started

### Dependencies

Add the necessary dependency to your [Leiningen][] `project.clj` and `require` the library in your ns:

```clojure
[com.taoensso/carmine "2.6.0"] ; project.clj
(ns my-app (:require [taoensso.carmine :as car :refer (wcar)])) ; ns
```

### Connections

You'll usually want to define a single connection pool, and one connection spec for each of your Redis servers.

```clojure
(def server1-conn {:pool {<opts>} :spec {<opts>}}) ; See `wcar` docstring for opts
(defmacro wcar* [& body] `(car/wcar server1-conn ~@body))
```

### Basic commands

Sending commands is easy:

```clojure
(wcar* (car/ping)
       (car/set "foo" "bar")
       (car/get "foo"))
=> ["PONG" "OK" "bar"]
```

Note that sending multiple commands at once like this will employ [pipelining](http://redis.io/topics/pipelining). The replies will be queued server-side and returned all at once as a vector. By default, if a single command is in the body of `wcar` the value of the command is returned. To always return a vector, add the `:as-pipeline` argument to `wcar`.

If the server responds with an error, an exception is thrown:

```clojure
(wcar* (car/spop "foo"))
=> Exception ERR Operation against a key holding the wrong kind of value
```

But what if we're pipelining?

```clojure
(wcar* (car/set  "foo" "bar")
       (car/spop "foo")
       (car/get  "foo"))
=> ["OK" #<Exception ERR Operation against ...> "bar"]
```

### Serialization

The only value type known to Redis internally is the [byte string](http://redis.io/topics/data-types). But Carmine uses [Nippy](https://github.com/ptaoussanis/nippy) under the hood and understands all of Clojure's [rich datatypes](http://clojure.org/datatypes), letting you use them with Redis painlessly:

```clojure
(wcar* (car/set "clj-key" {:bigint (bigint 31415926535897932384626433832795)
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
 * Keywords become Redis strings. (v2+)
 * Simple Clojure numbers (integers, longs, floats, doubles) become Redis strings.
 * Everything else gets automatically de/serialized.

You can force automatic de/serialization for an argument of any type by wrapping it with `car/serialize`.

### Documentation and command coverage

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
  (car/lua "return redis.call('set', _:my-key, 'lua '.. _:my-val)"
                  {:my-key key}   ; Named key variables and their values
                  {:my-val value} ; Named non-key variables and their values
                  ))

(wcar* (my-set "foo" "bar")
       (car/get "foo"))
=> ["OK" "lua bar"]
```

Script primitives are also provided: `eval`, `eval-sha`, `eval*`, `eval-sha*`. See the [Lua scripting docs](http://redis.io/commands/eval) for more info.

### Helpers

The `lua` command above is a good example of a Carmine *helper*.

Carmine will never surprise you by interfering with the standard Redis command API. But there are times when it might want to offer you a helping hand (if you want it). Compare:

```clojure
(wcar* (car/zunionstore "dest-key" 3 "zset1" "zset2" "zset3" "WEIGHTS" 2 3 5))
(wcar* (car/zunionstore* "dest-key" ["zset1" "zset2" "zset3"] "WEIGHTS" 2 3 5))
```

Both of these calls are equivalent but the latter counted the keys for us. `zunionstore*` is another helper: a slightly more convenient version of a standard command, suffixed with a `*` to indicate that it's non-standard.

Helpers currently include: `atomic`, `eval*`, `evalsha*`, `info*`, `lua`, `sort*`, `zinterstore*`, and `zunionstore*`. See their docstrings for more info.

### Commands are (just) functions

In Carmine, Redis commands are *real functions*. Which means you can *use* them like real functions:

```clojure
(wcar* (doall (repeatedly 5 car/ping)))
=> ["PONG" "PONG" "PONG" "PONG" "PONG"]

(let [first-names ["Salvatore"  "Rich"]
      surnames    ["Sanfilippo" "Hickey"]]
  (wcar* (mapv #(car/set %1 %2) first-names surnames)
         (mapv car/get first-names)))
=> ["OK" "OK" "Sanfilippo" "Hickey"]

(wcar* (mapv #(car/set (str "key-" %) (rand-int 10)) (range 3))
       (mapv #(car/get (str "key-" %)) (range 3)))
=> ["OK" "OK" "OK" "OK" "0" "6" "6" "2"]
```

And since real functions can compose, so can Carmine's. By nesting `wcar` calls, you can fully control how composition and pipelining interact:

```clojure
(let [hash-key "awesome-people"]
  (wcar* (car/hmset hash-key "Rich" "Hickey" "Salvatore" "Sanfilippo")
         (mapv (partial car/hget hash-key)
               ;; Execute with own connection & pipeline then return result
               ;; for composition:
               (wcar* (car/hkeys hash-key)))))
=> ["OK" "Sanfilippo" "Hickey"]
```

### Listeners & Pub/Sub

Carmine has a flexible **Listener** API to support persistent-connection features like [monitoring](http://redis.io/commands/monitor) and Redis's fantastic [Publish/Subscribe](http://redis.io/topics/pubsub) feature:

```clojure
(def listener
  (car/with-new-pubsub-listener (:spec server1-conn)
    {"foobar" (fn f1 [msg] (println "Channel match: " msg))
     "foo*"   (fn f2 [msg] (println "Pattern match: " msg))}
   (car/subscribe  "foobar" "foobaz")
   (car/psubscribe "foo*")))
```

Note the map of message handlers. `f1` will trigger when a message is published to channel `foobar`. `f2` will trigger when a message is published to `foobar`, `foobaz`, `foo Abraham Lincoln`, etc.

Publish messages:

```clojure
(wcar* (car/publish "foobar" "Hello to foobar!"))
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

### Reply parsing

Want a little more control over how server replies are parsed? You have all the control you need:

```clojure
(wcar* (car/ping)
       (car/with-parser clojure.string/lower-case (car/ping) (car/ping))
       (car/ping))
=> ["PONG" "pong" "pong" "PONG"]
```

### Binary data

Carmine's serializer has no problem handling arbitrary byte[] data. But the serializer involves overhead that may not always be desireable. So for maximum flexibility Carmine gives you automatic, *zero-overhead* read and write facilities for raw binary data:

```clojure
(wcar* (car/set "bin-key" (byte-array 50))
       (car/get "bin-key"))
=> ["OK" #<byte[] [B@7c3ab3b4>]
```

### Message queue

Redis makes a great [message queue server](http://antirez.com/post/250):

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

Look simple? It is. But it's also distributed, fault-tolerant, and _fast_.

Guarantees:
  * Messages are persistent (durable) as per Redis config.
  * Each message will be handled once and only once.
  * Handling is fault-tolerant: a message cannot be lost due to handler crash.
  * Message de-duplication can be requested on an ad hoc (per message) basis. In these cases, the same message cannot ever be entered into the queue more than once simultaneously or within a (per message) specifiable post-handling backoff period.

See the [API docs](http://ptaoussanis.github.io/carmine/taoensso.carmine.message-queue.html) for details.

### Distributed locks

```clojure
(:require [taoensso.carmine.locks :as locks]) ; Add to `ns` macro

(locks/with-lock "my-lock"
  {:pool {<opts>} :spec {<opts>}} ; Connection details
  1000 ; Time to hold lock
  500  ; Time to wait (block) for lock acquisition
  (println "This was printed under lock!"))
```

Again: simple, distributed, fault-tolerant, and _fast_. See the `taoensso.carmine.locks` namespace for details.

## Tundra (beta)

Redis is a beautifully designed datastore that makes some explicit engineering tradeoffs. Probably the most important: your data _must_ fit in memory. Tundra helps relax this limitation: only your **hot** data need fit in memory. How does it work?

  1. Use Tundra's `dirty` command **any time you modify/create evictable keys**.
  2. Use `worker` to create a threaded worker that'll **automatically replicate dirty keys to your secondary datastore**.
  3. When a dirty key hasn't been used in a specified TTL, it will be **automatically evicted from Redis** (eviction is optional if you just want to use Tundra as a backup/just-in-case mechanism).
  4. Use `ensure-ks` **any time you want to use evictable keys**. This will extend their TTL or fetch them from your datastore as necessary.

That's it: two Redis commands, and a worker! Tundra uses Redis' own dump/restore mechanism for replication, and Carmine's own message queue to coordinate the replication worker.

Tundra can be _very_ easily extended to **any K/V-capable datastore**. Implementations are provided out-the-box for: **Disk**, **Amazon S3** and **DynamoDB**.

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

Note that this API makes it convenient to use several different datastores simultaneously (perhaps for different purposes with different latency requirements).

See the relevant [docstrings](http://ptaoussanis.github.io/carmine/taoensso.carmine.tundra.html) for details.

## Performance

Redis is probably most famous for being [fast](http://redis.io/topics/benchmarks). Carmine does what it can to hold up its end and currently performs well:

![Performance comparison chart](https://github.com/ptaoussanis/carmine/raw/master/benchmarks.png)

[Detailed benchmark information](https://docs.google.com/spreadsheet/ccc?key=0AuSXb68FH4uhdE5kTTlocGZKSXppWG9sRzA5Y2pMVkE) is available on Google Docs. Note that these numbers are for _unpipelined_ requests: you could do a _lot_ more with pipelining.

In principle it should be possible to get close to the theoretical maximum performance of a JVM-based client. This will be an ongoing effort but please note that my first concern for Carmine is **performance-per-unit-power** rather than *absolute performance*. For example Carmine willingly pays a small throughput penalty to support binary-safe arguments and again for composable commands. 

Likewise, I'll happily trade a little less throughput for simpler code.

##### YourKit

Carmine was developed with the help of the [YourKit Java Profiler](http://www.yourkit.com/java/profiler/index.jsp). YourKit, LLC kindly supports open source projects by offering an open source license. They also make the [YourKit .NET Profiler](http://www.yourkit.com/.net/profiler/index.jsp).

## This project supports the CDS and ![ClojureWerkz](https://raw.github.com/clojurewerkz/clojurewerkz.org/master/assets/images/logos/clojurewerkz_long_h_50.png) goals

  * [CDS][], the **Clojure Documentation Site**, is a **contributer-friendly** community project aimed at producing top-notch, **beginner-friendly** Clojure tutorials and documentation. Awesome resource.

  * [ClojureWerkz][] is a growing collection of open-source, **batteries-included Clojure libraries** that emphasise modern targets, great documentation, and thorough testing. They've got a ton of great stuff, check 'em out!

## Contact & contributing

`lein start-dev` to get a (headless) development repl that you can connect to with [Cider][] (emacs) or your IDE.

Please use the project's GitHub [issues page][] for project questions/comments/suggestions/whatever **(pull requests welcome!)**. Am very open to ideas if you have any!

Otherwise reach me (Peter Taoussanis) at [taoensso.com][] or on [Twitter][]. Cheers!

## License

Copyright &copy; 2012-2014 Peter Taoussanis. Distributed under the [Eclipse Public License][], the same as Clojure.


[API docs]: <http://ptaoussanis.github.io/carmine/>
[CHANGELOG_]: <https://github.com/ptaoussanis/carmine/blob/master/CHANGELOG.md>
[CHANGELOG]: <https://github.com/ptaoussanis/carmine/releases>
[other Clojure libs]: <https://www.taoensso.com/clojure-libraries>
[Twitter]: <https://twitter.com/ptaoussanis>
[semantic]: <http://semver.org/>
[Leiningen]: <http://leiningen.org/>
[CDS]: <http://clojure-doc.org/>
[ClojureWerkz]: <http://clojurewerkz.org/>
[issues page]: <https://github.com/ptaoussanis/carmine/issues>
[commit history]: <https://github.com/ptaoussanis/carmine/commits/master>
[Cider]: <https://github.com/clojure-emacs/cider>
[taoensso.com]: <https://www.taoensso.com>
[Eclipse Public License]: <https://raw2.github.com/ptaoussanis/carmine/master/LICENSE>
