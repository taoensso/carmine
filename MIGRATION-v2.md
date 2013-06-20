# Carmine v2 migration guide

Carmine v2 is **mostly** backwards compatible with Carmine v1. You may or may not need to make any code changes to upgrade.

## BREAKING CHANGES

### Distributed locks API

**AFFECTS** Folks using the `taoensso.carmine.locks` API.

**CHANGES** The old API used an atom for connection config. Instead, every API fn/macro now takes an explicit `conn` arg.

**MIGRATING** Add an explicit `conn` arg to each fn/macro call (this is a map of form `{:pool <pool-opts> :spec <spec-opts>}`). Use the empty map `{}` or `nil` for pool+spec defaults.

### Raw binary data return type

**AFFECTS** Folks that read+write raw (unserialized) binary arguments.

**CHANGES** Raw binary data used to be returned as a `[<ba> <ba-len>]` vector. It's now returned as unwrapped bytes, `<ba>`.

**MIGRATING** Writing doesn't change. You'll need to check where you may be _reading_ raw bytes and assuming that it's in the form of a vector. This'll usually be a destructuring like `(let [[ba size] (wcar* (car/get "foo"))])`. In this case you'll want to change to something like `(let [ba (wcar* (car/get "foo"))])`. Use `(alength ba)` to get the size.

### Keyword args are now stringified, not serialized

**AFFECTS** Folks that write single-arg keywords and are assuming that when they're read back they'll still be keywords. This isn't usually idiomatic usage, but I have seen it done. This change does **not** affect keywords within larger data structures like maps, vectors, lists, etc.

**CHANGES** `(wcar* (car/set "foo" :bar) (car/get "foo"))` previously returned `:bar` but now returns `"bar"`.

**MIGRATING** If you want the same behavior as before, wrap your single-arg keywords with `freeze` when writing: `(wcar* (car/set "foo" (car/freeze :bar)))`.

The more idiomatic usage would be to write your keywords as unserialized strings, then to use `keyword` or `parse-keyword` to keywordize them on reading. So something like: `(wcar* (car/set "foo" :bar) (car/parse-keyword (car/get "foo")))` to return `:bar`. This is similar to how one would use `as-long`/`parse-long`, `as-double`/`parse-double`, `as-bool`/`parse-bool`, etc.

## NON-breaking (suggested) changes

### `with-conn`, `make-conn-pool`, `make-conn-spec` have been deprecated

**AFFECTS** Everyone.

**DETAILS** `with-conn` has been deprecated in favor of `wcar`. Idiomatic usage used to be:

```clojure
(def conn-pool (car/make-conn-pool <opts>))
(def conn-spec (car/make-conn-spec <opts>))
(defmacro wcar [& body] `(car/with-conn pool spec ~@body))
(wcar (car/ping)) => "PONG"
```

and is now:
```clojure
(:require [taoensso.carmine :as car :refer (wcar)]) ; ns
(def  server1-conn {:pool {<opts>} :spec {<opts>}})
(wcar server1-conn (car/ping)) => "PONG"
```

### `make-dequeue-worker` has been deprecated

**AFFECTS** Folks using the `taoensso.carmine.message-queue` API.

**DETAILS** `make-dequeue-worker` has been deprecated in favor of `worker`. See the `worker` docstring for details.