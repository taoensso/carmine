# Setup

## Dependency

Add the [relevant dependency](../#latest-releases) to your project:

```clojure
Leiningen: [com.taoensso/carmine               "x-y-z"] ; or
deps.edn:   com.taoensso/carmine {:mvn/version "x-y-z"}
```

And setup your namespace imports:

```clojure
(ns my-app (:require [taoensso.carmine :as car :refer [wcar]]))
```

## Configure connections

You'll usually want to define a single **connection pool**, and one **connection spec** for each of your Redis servers, example:

```clojure
(defonce my-conn-pool (car/connection-pool {})) ; Create a new stateful pool
(def     my-conn-spec {:uri "redis://redistogo:pass@panga.redistogo.com:9475/"})
(def     my-wcar-opts {:pool my-conn-pool, :spec my-conn-spec})
```

This `my-wcar-opts` can then be provided to Carmine's `wcar` ("with Carmine") API:

```clojure
(wcar my-wcar-opts (car/ping)) ; => "PONG"
```

`wcar` is the main entry-point to Carmine's API. See its [docstring](https://taoensso.github.io/carmine/taoensso.carmine.html#var-wcar) for **lots more info** on connection options!

You can create a `wcar` partial for convenience:

```clojure
(defmacro wcar* [& body] `(car/wcar ~my-wcar-opts ~@body))
```

# Usage

## Command pipelines

Calling multiple Redis commands in a single `wcar` body uses efficient [Redis pipelining](https://redis.io/docs/manual/pipelining/) under the hood, and returns a pipeline reply (vector) for easy destructuring:

```clojure
(wcar*
  (car/ping)
  (car/set "foo" "bar")
  (car/get "foo")) ; => ["PONG" "OK" "bar"] (3 commands -> 3 replies)
```

If the number of commands you'll be calling might vary, it's possible to request that Carmine _always_ return a destructurable pipeline-style reply:

```clojure
(wcar* :as-pipeline (car/ping)) ; => ["PONG"] ; Note the pipeline-style reply
```

If the server replies with an error, an exception is thrown:

```clojure
(wcar* (car/spop "foo"))
=> Exception ERR Operation against a key holding the wrong kind of value
```

But what if we're pipelining?

```clojure
(wcar*
  (car/set  "foo" "bar")
  (car/spop "foo")
  (car/get  "foo"))
=> ["OK" #<Exception ERR Operation against ...> "bar"]
```

## Serialization

The only scalar type native to Redis is the [byte string](https://redis.io/docs/data-types/). But Carmine uses [Nippy](https://www.taoensso.com/nippy) under the hood to seamlessly support all of Clojure's rich data types: 

```clojure
(wcar*
  (car/set "clj-key"
    {:bigint (bigint 31415926535897932384626433832795)
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

Clojure type| Redis type
-- | --
Strings| Redis strings
Keywords | Redis strings
Simple numbers | Redis strings
Everything else | Auto de/serialized with [Nippy](https://www.taoensso.com/nippy)

You can force automatic de/serialization for an argument of any type by wrapping it with [`car/freeze`](https://taoensso.github.io/carmine/taoensso.carmine.html#var-freeze).

## Command coverage

Carmine uses the [official Redis command spec](https://github.com/redis/redis-doc/blob/master/commands.json) to auto-generate a Clojure function for **every Redis command**. These are accessible from the main Carmine namespace:

```clojure
;; Example: car/sort is a Clojure function for the Redis SORT command
(clojure.repl/doc car/sort)

=> "SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC|DESC] [ALPHA] [STORE destination]

Sort the elements in a list, set or sorted set.

Available since: 1.0.0.

Time complexity: O(N+M*log(M)) where N is the number of elements in the list or set to sort, and M the number of returned elements. When the elements are not sorted, complexity is currently O(N) as there is a copy step that will be avoided in next releases."
```

Each Carmine release will always use the latest Redis command spec.

But if a new Redis command hasn't yet made it to Carmine, or if you want to use a Redis command not in the official spec (a [Redis module](https://redis.io/resources/modules/) command for example) - you can always use Carmine's [`redis-call`](https://taoensso.github.io/carmine/taoensso.carmine.html#var-redis-call) function to issue **arbitrary Redis commands**.

So you're **not constrained** by the commands provided by Carmine.

## Commands are functions

Carmine's auto-generated command functions are **real functions**, which means that you can use them like any other Clojure function:

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

And since real functions can compose, so can Carmine's.

By nesting `wcar` calls, you can fully control how composition and pipelining interact:

```clojure
(let [hash-key "awesome-people"]
  (wcar*
    (car/hmset hash-key "Rich" "Hickey" "Salvatore" "Sanfilippo")
    (mapv (partial car/hget hash-key)
      ;; Execute with own connection & pipeline then return result
      ;; for composition:
      (wcar* (car/hkeys hash-key)))))
=> ["OK" "Sanfilippo" "Hickey"]
```

# Performance

Redis is probably most famous for being *fast*. Carmine hold up its end and usually performs within ~10% of the official C `redis-benchmark` utility despite offering features like command composition, reply parsing, etc.

Benchmarks are [included](../blob/master/src/taoensso/carmine/benchmarks.clj) in the Carmine GitHub repo and can be easily run from your own environment.