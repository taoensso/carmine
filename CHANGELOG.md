## 2013-06-26 - v2.0.0-beta1

  * Refactored a bunch of code for simplicity+performance (~20% improved roundtrip times).

  * Upgraded to [Nippy v2][Nippy GitHub] for pluggable compression+crypto (see [Nippy CHANGELOG][Nippy CHANGELOG] for details).

  * **DEPRECATED**: `with-conn`, `make-conn-pool`, `make-conn-spec` -> `wcar`.
  ```clojure

  ;;; New idiomatic usage
  (ns my-app (:require [taoensso.carmine :as car :refer (wcar)]))
  (def server1-conn {:pool {<opts>} :spec {<opts>}})
  (wcar server1-conn (car/ping)) => "PONG"

  ;; or
  (defmacro wcar* [& body] `(car/wcar server1-conn ~@body))

  ;;; Old
  (def conn-pool (car/make-conn-pool <opts>))
  (def conn-spec (car/make-conn-spec <opts>))
  (defmacro wcar* [& body] `(car/with-conn conn-pool conn-spec ~@body))
  ```

  * **BREAKING**: Keyword args now get stringified instead of serialized.
    ```clojure
    (wcar* (car/set "foo" :bar) (car/get "foo")) => "bar" ; New behaviour
    (wcar* (car/set "foo" :bar) (car/get "foo")) => :bar  ; Old

    ;; Idiomatic usage is now:
    (wcar* (car/set "foo" :bar) (car/parse-keyword (car/get "foo"))) => :bar

    ;; Or use `freeze` if you want to serialize the input arg like before:
    (wcar* (car/set "foo" (car/freeze :bar)) (car/get "foo")) => :bar
    ```

  * **BREAKING**: Raw binary data return type is now unwrapped.
    ```clojure
    (wcar* (car/get "raw-bytes")) => [B@4d66ea88> ; New behaviour
    (wcar* (car/get "raw-bytes")) => [<length> [B@4d66ea88>]] ; Old
    ```

  * **BREAKING**: Distributed locks API has changed.
    The old API used an atom for connection config. The new API takes an explicit `conn` arg for every fn/macro.

  * Added message queue per-message backoffs.
    ```clojure
    ;; Handler fns can now return a response of form:
    {:status     <#{:success :error :retry}>
     :throwable  <Throwable>
     :backoff-ms <retry-backoff-ms>}
    ```

  * Added `redis-call` command for executing arbitrary Redis commands: `(redis-call [:set "foo" "bar"] [:get "foo"])`. See docstring for details.

  * Improved a number of error messages.

  * Fixed a number of subtle reply-parsing bugs.

  * Remove alpha status: `parse`, `return`, message queue ns.

  * **DEPRECATED**: `with-parser` -> `parse`.

  * **DEPRECATED**: `make-dequeue-worker` -> `worker`. See docstring for details.

----

## 2013-06-09 - v1.12.0

  * Allow pipelined (non-throwing) Lua script exceptions.
  * Allow pipelined (non-throwing) parser exceptions.
  * Improved parser error messages.
  * Fix a number of subtle `with-replies` and `eval*` bugs.

----

## 2013-06-05 - v1.10.0

  * Simplify reply parsing, remove `with-mparser` (vestigial).
  * Add additional reply parsing tests.
  * Added `raw` command and `parse-raw` fn for completely unprocessed Redis comms.

----

## 2013-06-01 - v1.9.0

  * **BREAKING**: Drop official Clojure 1.3 support.
  * Performance tweaks.
  * Internal message queue improvements, add interpretable handler return status.

----

## 2013-05-29 - v1.8.0

  * **DEPRECATED**: `make-conn-spec` `:timeout` opt -> `:timeout-ms`.
  * Internal message queue improvements.

----

## 2013-04-19 - v1.7.0

  * **DEPRECATED**: `make-keyfn` -> `kname`.
  * Pub/sub listener now catches handler exceptions.
  * Add experimental (alpha) distributed locks API.
  * `as-bool` now throws on incoercible arg.
  * Fix `interpolate-script` memoization bug (big perf. boost).
  * **BREAKING**: Remove `skip-replies`.
  * Allow `with-parser` to clear current parsers with `nil` fn arg.

----

## 2013-03-04 - v1.6.0

  * Clean up Pub/sub listener thread (don't keep head!).
  * Add URI support to `make-conn-spec`.

----

###### For older versions please see the [commit history][]

[commit history]: https://github.com/ptaoussanis/carmine/commits/master
[API docs]: http://ptaoussanis.github.io/carmine
[Nippy GitHub]: https://github.com/ptaoussanis/nippy
[Nippy CHANGELOG]: https://github.com/ptaoussanis/carmine/blob/master/CHANGELOG.md
[Nippy API docs]: http://ptaoussanis.github.io/nippy
[Taoensso libs]: https://www.taoensso.com/clojure-libraries