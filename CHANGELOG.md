> This project uses [Break Versioning](https://github.com/ptaoussanis/encore/blob/master/BREAK-VERSIONING.md) as of **Aug 16, 2014**.

## v2.16.0 / 2017 Mar 24

```clojure
[com.taoensso/carmine "2.16.0"]
```

> This is a non-breaking maintenance release

* **New**: Update Nippy to [v2.13.0](https://github.com/ptaoussanis/nippy/releases/tag/v2.13.0)
* **New**: Update to latest Redis commands spec
* [#192] **Fix**: `protocol/get-parsed-reply` fn (@firesofmay)
* [#193] **Impl**: Migrate unit tests from Expectations to clojure.test (@firesofmay)

## v2.15.1 / 2017 Jan 7

```clojure
[com.taoensso/carmine "2.15.1"]
```

> This is a non-breaking hotfix release

* [#189] **Hotfix**: broken `redis-call`

## v2.15.0 / 2016 Oct 20

```clojure
[com.taoensso/carmine "2.15.0"]
```

> This is a non-breaking maintenance release

* **Fix**: message-queue errors not being correctly identified.
* **Impl**: refactor automatic command definitions.
* **Impl**: refactor some low-level protocol code.

## v2.14.0 / 2016 Jul 24

```clojure
[com.taoensso/carmine "2.14.0"]
```

> This is a non-breaking release that upgrades Carmine's Nippy dependency

* **Impl**: Switch to Nippy v2.12.0 for serialization performance improvements
* [#174] **New**: Message queue: add `qname` to arg map passed to queue handler (@dparis)
* [#175] **New**: Improve error message for mq handler errors

## v2.13.1 / 2016 Jun 21

```clojure
[com.taoensso/carmine "2.13.1"]
```

> This is a hotfix release and **recommended upgrade** for all users of `v2.13.0`

* **Fix**: Regression from v2.12.2 could cause DataInputStream errors in some cases [#173]

## v2.13.0 / 2016 Jun 10

```clojure
[com.taoensso/carmine "2.13.0"]
```

> This is a **non-breaking** performance and housekeeping release

* **New**: updated Redis `commands.json` (bitops, etc.)
* **New**: added `reduce-scan` util for use with `scan`, `zscan`, etc.
* **Impl**: refactored some core protocol + connections stuff (performance improvements)
* **Fix**: `atomic*` was broken for non-const `max-cas-attempts` [#165 @MysteryMachine]
* **Fix**: Message queue: catch unexpected `dequeue` errors [#87 @gsnewmark]


## v2.12.2 / 2016 Jan 14

> This is a minor, non-breaking release

* **New**: Allow reclaiming resources for pub/sub on closing [#149 @mpenet]
* **Fix**: Add missing Tundra disk store type hint

```clojure
[com.taoensso/carmine "2.12.2"]
```


## v2.12.1 / 2015 Nov 30

* **Hotfix**: broken Faraday data store for Tundra [#147]

```clojure
[com.taoensso/carmine "2.12.1"]
```


## v2.12.0 / 2015 Sep 30

> This is a significant **feature release** focused on new CAS utils

* **New**: conn spec URIs now support /db path [#113 @olek]
* **New**: `with-replies` now supports nesting
* **New**: added experimental CAS utils: `swap`, `hswap`
* **New**: updated commands.json (2015 Aug 5)
* **Perf**: bumped Nippy dependency to v2.10.0
* **Perf**: minor low-level Redis protocol optimizations
* **Fix**: Tundra S3 datastore wasn't closing input stream
* **Docs**: `wcar` docstring now has a warning re: laziness [#138]

```clojure
[com.taoensso/carmine "2.12.0"]
```


## v2.11.1 / 2015 June 5

> This is a non-breaking hotfix release

* **Fix**: message queue workers use 2 conns when they only need 1 [#135 @gfredericks]

```clojure
[com.taoensso/carmine "2.11.1"]
```


## v2.11.0 / 2015 June 4

> This is a non-breaking maintenance release

* **Performance**: upgrade to Nippy v2.9.0
* **New**: update Redis commands.json (May 29)
* **Fix**: commons-pool losing connections over time [#127 @cespare]
* **Fix**: Tundra disk store endless loop when failing to create ouput dir
* **Misc**: bump dependencies (incl. Encore + Faraday)

```clojure
[com.taoensso/carmine "2.11.0"]
```


## v2.10.0 / 2015 May 6

> This is a non-breaking performance release

* **Implementation**: switch `doseq` -> (faster) `run!` calls

```clojure
[com.taoensso/carmine "2.10.0"]
```


## v2.9.2 / 2015 Apr 1

> This is a non-breaking hotfix release

* **Fix**: fragile `make-new-connection` destructuring [@chairmanwow @mavbozo #130]
* **New**: `return` can now take multiple args

```clojure
[com.taoensso/carmine "2.9.2"]
```


## v2.9.1 / 2015 Mar 26

> This is a non-breaking hotfix release

* **Fix**: `atomic` no longer masks pre-discard exceptions

```clojure
[com.taoensso/carmine "2.9.1"]
```


## v2.9.0 / 2014 Dec 11

> This release adds more control over connection timeouts and introduces a default (4 second) timeout for _acquiring_ a connection.

 * **CHANGE**: Cleaned up Listener docstrings.
 * **NEW** [#120]: Connection specs now support `:read-timeout-ms` and `:conn-timeout-ms` (the latter defaults to 4 seconds).


## v2.8.0 / 2014 Nov 23

> This is a major but backwards-compatible update focused on performance tuning + general housekeeping.

 * **CHANGE** **NB**: Upgraded to Nippy v2.7.0 for serialized data perf+size improvements. See Nippy's [changelog](https://github.com/ptaoussanis/nippy/releases/tag/v2.7.0-RC1) for details.
 * **CHANGE**: Lua scripts are now separated from Clojure code for syntax highlighting + easier debugging.
 * **CHANGE** [#108]: result stashing (notably Lua scripts) now uses one less TCP roundtrip.
 * **CHANGE**: `compare-and-set` optimization: now uses hashing for all arg types.
 * **NEW** [#106]: Experimental: conn specs can now take a `:conn-setup-fn` option (useful for pre-loading Lua scripts, etc.).
 * **NEW** [#107]: Experimental: reply suppression via `parse-suppress` (useful with `atomic`, etc.).
 * **NEW**: Updated to latest [commands.json](https://github.com/antirez/redis-doc/blob/master/commands.json) (2014, Oct 8).


## v2.7.1 / 2014 Oct 4

 * **NEW**: Experimental `compare-and-set` fn.


## v2.7.0 / 2014 Aug 27

> This is a significant release that **may be breaking** for those using custom connection pool options.

 * **POSSIBLY BREAKING**: Upgraded to apache-commons-pool v2, bumped default max active conns (8->16). If you're using custom connection pool options, please confirm that none of your options have been removed in v2 (an exception will be thrown for invalid options).
 * **CHANGE**: New lock-free connection pool cache (improved performance under contention).
 * **CHANGE**: All `Exception`s are now `ExceptionInfo`s.
 * **CHANGE**: `wcar, `atomic*`, `atomic` now catch `Throwable`s rather than `Exception`s (assertions in particular).
 * **NEW**: New commands! (Updated official commands.json spec).
 * **NEW**: Added `atomic*` (alpha) low-level transactions util (#93).

## v2.6.2 / 2014 May 3

> This is a hotfix release.

 * [#84] **FIX** empty-string writes (they were throwing exceptions) (@bzg).


## v2.6.1 / 2014 May 1

> This is an **important fix release**, please update from `v2.6.0` ASAP.

 * [#83] Nb **FIX** unnecessary serialization of binary arguments (@mishok13).


## v2.6.0 / 2014 Apr 1

> Adds support for new [HyperLogLog](http://antirez.com/news/75) commands (currently requires a Redis >=2.8.9 client).

 * Updated official commands.json spec.
 * Bumped `Encore` dependency.


## v2.5.0 / 2014 Mar 30

> **Major, backwards-compatible release** that refactors a lot of internal code for performance, reliability, and as groundwork for an upcoming v3 with Redis Cluster support.

### Changes

 * Moved most utils to external `encore` dependency.
 * **DEPRECATED**: `as-long`, `as-double`, `parse-long`, `parse-double` -> `as-int`, `as-float`, `parse-int`, `parse-float`.
 * Completely refactored protocol design in prep for Redis Cluster support.
 * Misc performance & reliability improvements.

### Fixes

 * Fix atomic regression (c9ca09d40).
 * Fix broken Tundra tests, improve test reliability.
 * Fix `atomic` exception handling (wasn't throwing swap exceptions as it should).


## v2.4.6 / 2014 Jan 31

### Fixes

 * #71 Fix bad `atomic` arg order (neilmock).
 * #72 Fix bad mq handler connection arg (neilmock).


## v2.4.5 / 2014 Jan 22

> This is a **backwards compatible bug fix release**. Recommended upgrade.

### New

 * Updated `commands.json` to bring in Redis 2.8 SCAN commands.

### Fixes

 * #68 Pub/Sub bug preventing use with passwords & db selects (sritchie).
 * `as-map` (and by extension `parse-map`) post condition bug: should allow nils.
 * `lua-local` vector-args bug.
 * Fix Tundra message queue name formatting.
 * Fix `lua` + `parse-raw` support (chenfisher).
 * #71 Message queue workers no longer terminate on errors (notably connection errors) but will retry after backoff.
 * Fix regression introduced by #70.


## v2.3.1 → v2.4.0 (major update!)

  * **IMPORTANT** Message queues: pre-2.4.0-beta1 queues *should* be compatible with 2.4.0-beta1+, but I would recommend **draining your old queues before upgrading** to 2.4.0-beta1+ to be on the safe side. That is: if you have any queued work outstanding - finish processing the work **before upgrading Carmine**.
  * **BREAKING** Tundra: the datastore protocol has changed (been simplified). `put-keys`, `fetch-keys` -> `put-key`, `fetch-keys`.
  * **POTENTIALLY BREAKING** Parsers: `with-replies` now passes enclosing parser state to its body. This only affects you if you have used `with-replies` to write custom Redis commands that operate within an implicit context _and_ you interpret the `with-replies` result internally. The new docstring contains details.
  * **DEPRECATED**: `hmget*` and `hmgetall*`. In future use the `parse-map` macro which is: faster, more flexible, pipeline-capable.

  * Parsers: completely refactored design for robustness+flexibility.
  * Parsers: new unit-test suite.
  * Parsers: fixed a number of subtle bugs, mostly internal.
  * Parsers: added `parser-comp` fn for composing parsers (see docstring for details).
  * Parsers: added `parse-nippy` macro for convenient per-cmd control of thaw opts.
  * Parsers: added `parse-map` for parsing multi-bulk replies as Clojure hashmaps.

  * Message queues: completely refactored design for robustness+efficiency.
  * Message queues: new unit-test suite.
  * Message queues: fixed a number of bugs, mostly garbage-collection issues.
  * Message queues: `enqueue` now accepts an optional, custom unique message id (e.g. message hash).
  * Message queues: handlers may now return {:status :success :backoff-ms `<msecs>`} for de-duplicate backoff. `enqueue` will return an error in these cases (see docstring for details).
  * Message queues: `message-status` can now be called within pipelines.
  * Message queues: `message-status` now has more detailed return types (see docstring for details).
  * Message queues: multi-worker end-of-queue backoffs are now synchronized more efficiently.
  * Message queues: workers now accept an optional monitor fn for queue-status logging, etc. (see docstring for details). A default monitor is provided that will warn when queue size > 1000 items.
  * Message queues: workers now accept an optional `nthreads` arg for spinning up synchronized multi-threaded workers.
  * Message queues: handlers now receive message id along with other args.
  * Message queues: `enqueue` now takes an optional flag to specify dedupe behaviour when message is locked.
  * Message queues: `queue-status` now shows next & last mid.
  * Message queues: add support for delayed start (use `:auto-start <ms>` option).
  * Message queues: log notice on worker start/stop.

  * Tundra: completely refactored design for robustness+simplicity - now use standard message queue for replication worker.
  * Tundra: replication workers have inherited message queue features like retries, backoffs, etc.
  * Tundra: added S3 DataStore and skeleton secondary Carmine DataStore implementations.
  * Tundra: new unit test suite.
  * Tundra: eviction TTL is now set only _after_ first successful replication, providing an extra level of safety.
  * Tundra: `ensure-ks` now no longer throws when attempting to ensure non-evictable keys.
  * Tundra: added simple disk-based DataStore implementation.

  * Scripts: added experimental `lua-local` for higher script performance in single-server environments.


## v2.2.3 → v2.3.1
  * **DEPRECATED**: `atomically`, `ensure-atomically` -> `atomic`. The new macro is faster, more robust, more flexible. See docstring for details.
  * Official Redis command fns now contain a `:redis-api` metadata key that describes the first version of Redis to support the command.


## v2.2.0 → v2.2.3
  * Fix race condition for pool creation (thanks cespare!).
  * Fix unnecessary reflection for pool creation (thanks harob!).


## v2.1.4 → v2.2.0
  * Add `hmset*`, `hmget*` helpers.
  * Add `:clojurize?` option to `info*` helper.
  * Allow `hmget*`, `hgetall*`, `zinterstore*` to work with custom parsers.


## v2.1.0 → v2.1.4
  * Fixed `lua` clashing var name regex bug (thanks to Alex Kehayias for report).


## v2.0.0 → v2.1.0
  * Like `with-replies`, `wcar` macro can now take a first `:as-pipeline` arg:
  ```clojure
  (wcar {} (car/ping)) => "PONG"
  (wcar {} :as-pipeline (car/ping)) => ["PONG"]
  ```
  * **DEPRECATED**: `kname` -> `key`. The new fn does NOT automatically filter input parts for nil. This plays better with Redis' key pattern matching style, but may require manual filtering in some cases.


## v1.12.0 → v2.0.0

  * Refactored a bunch of code for simplicity+performance (~20% improved roundtrip times).
  * Upgraded to [Nippy v2](https://github.com/ptaoussanis/nippy) for pluggable compression+crypto. See the [Nippy CHANGELOG](https://github.com/ptaoussanis/carmine/commits/master) for details.
  * Added early (alpha) Tundra API for semi-automatic cold data archiving. See the [README](https://github.com/ptaoussanis/carmine#tundra) for details.

  * **DEPRECATED**: `with-conn`, `make-conn-pool`, `make-conn-spec` -> `wcar`:
  ```clojure
  ;;; Old usage
  (def conn-pool (car/make-conn-pool <opts>))
  (def conn-spec (car/make-conn-spec <opts>))
  (defmacro wcar* [& body] `(car/with-conn conn-pool conn-spec ~@body))

  ;;; New idiomatic usage
  (ns my-app (:require [taoensso.carmine :as car :refer (wcar)]))
  (def server1-conn {:pool {<opts>} :spec {<opts>}})
  (wcar server1-conn (car/ping)) => "PONG"

  ;; or
  (defmacro wcar* [& body] `(car/wcar server1-conn ~@body))
  ```

  * **BREAKING**: Keyword args now get stringified instead of serialized:
    ```clojure
    (wcar* (car/set "foo" :bar) (car/get "foo")) => :bar  ; Old behaviour
    (wcar* (car/set "foo" :bar) (car/get "foo")) => "bar" ; New

    ;; Idiomatic usage is now:
    (wcar* (car/set "foo" :bar) (car/parse-keyword (car/get "foo"))) => :bar

    ;; Or use `freeze` if you want to serialize the input arg like before:
    (wcar* (car/set "foo" (car/freeze :bar)) (car/get "foo")) => :bar
    ```

  * **BREAKING**: Raw binary data return type is now unwrapped:
    ```clojure
    (wcar* (car/get "raw-bytes")) => [<length> [B@4d66ea88>]] ; Old behaviour
    (wcar* (car/get "raw-bytes")) => [B@4d66ea88> ; New
    ```

  * **BREAKING**: Distributed locks API has changed:
    The old API used an atom for connection config. The new API takes an explicit `conn` arg for every fn/macro.

  * Added message queue per-message backoffs:
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
  * Added support for unnamed vector (non-map) keys/args to `lua-script`.
  * **DEPRECATED**: `lua-script` -> `lua`.
  * **DEPRECATED**: `ring/make-carmine-store` -> `ring/carmine-store`.
  * Add `ensure-atomically`. See docstring for details.

## v1.10.0 → v1.12.0

  * Allow pipelined (non-throwing) Lua script exceptions.
  * Allow pipelined (non-throwing) parser exceptions.
  * Improved parser error messages.
  * Fix a number of subtle `with-replies` and `eval*` bugs.


## v1.9.0 → v1.10.0

  * Simplify reply parsing, remove `with-mparser` (vestigial).
  * Add additional reply parsing tests.
  * Added `raw` and `parse-raw` fns for completely unprocessed Redis comms.


## v1.8.0 → v1.9.0

  * **BREAKING**: Drop official Clojure 1.3 support.
  * Performance tweaks.
  * Internal message queue improvements, add interpretable handler return status.


## v1.7.0 → v1.8.0

  * **DEPRECATED**: `make-conn-spec` `:timeout` opt -> `:timeout-ms`.
  * Internal message queue improvements.


## v1.6.0 → v1.7.0

  * **DEPRECATED**: `make-keyfn` -> `kname`.
  * Pub/sub listener now catches handler exceptions.
  * Add experimental (alpha) distributed locks API.
  * `as-bool` now throws on incoercible arg.
  * Fix `interpolate-script` memoization bug (big perf. boost).
  * **BREAKING**: Remove `skip-replies`.
  * Allow `with-parser` to clear current parsers with `nil` fn arg.


## v1.5.0 → v1.6.0

  * Clean up Pub/sub listener thread (don't keep head!).
  * Add URI support to `make-conn-spec`.
