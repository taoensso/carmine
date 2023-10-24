This project uses [**Break Versioning**](https://www.taoensso.com/break-versioning).

---

# `v3.3.1` (2023-10-24)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/carmine/versions/3.3.1), this project uses [Break Versioning](https://www.taoensso.com/break-versioning).

This is a **minor hotfix release** and should be a safe upgrade for users of `v3.3.0`.

## Fixes since `v3.3.0`

* [#289] [fix] >1 arity `parse-map` broken since v3.3.0

---

# `v3.3.0` (2023-10-12)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/carmine/versions/3.3.0), this project uses [Break Versioning](https://www.taoensso.com/break-versioning).

This is a **major feature release** focused on significantly improving the performance and observability of Carmine's message queue.

There are **BREAKING CHANGES** for the message queue API, see [migration info](https://github.com/taoensso/carmine/wiki/0-Breaking-changes#carmine-v32x-to-v33x).

Please **test carefully and report any unexpected problems**, thank you! 🙏

## Changes since `v3.2.0`

* ⚠️ 1804ef97 [mod] **[BREAKING]** [#278] Carmine message queue API changes (see [migration info](https://github.com/taoensso/carmine/wiki/0-Breaking-changes))

## Fixes since `v3.2.0`

* 2e6722bf [fix] [#281 #279] `parse-uri`: only provide ACL params when non-empty (@frwdrik)
* c68c995c [fix] [#283] Support new command.json spec format used for `XTRIM`, etc.

## New since `v3.2.0`

* Several major message queue [architectural improvements](https://github.com/taoensso/carmine/wiki/0-Breaking-changes#architectural-improvements)
* Several major message queue [API improvements](https://github.com/taoensso/carmine/wiki/0-Breaking-changes#api-improvements)
* c9c8d810 [new] Update commands to match latest `commands.json` spec
* 19d97ebd [new] [#282] Add support for TLS URIs to URI parser
* 5cfdbbbd [new] Add `scan-keys` util
* 37f0030a [new] Add `set-min-log-level!` util
* GraalVM compatibility is now tested during build

## Other improvements since `v3.3.0`

* Uses latest Nippy ([v3.3.0](https://github.com/taoensso/nippy/releases/tag/v3.3.0))
* Various internal improvements

---

# `v3.3.0-RC1` (2023-07-18)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/carmine/versions/3.3.0-RC1)

This is a **major feature release** which includes **possible breaking changes** for users of Carmine's message queue.

The main objective is to introduce a rewrite of Carmine's message queue that *significantly* improves **queue performance and observability**.

## If you use Carmine's message queue:

  - Please see the 1804ef97 commit message for important details.
  - Please **test this release** carefully before deploying to production, and once deployed **monitor for any unexpected behaviour**.

My sincere apologies for the possible breaks. My hope is that:

  - Few users will actually be affected by these breaks.
  - If affected, migration should be simple.
  - Ultimately the changes will be positive - and something that all queue users can benefit from.

## Changes since `v3.2.0`

* ⚠️ 1804ef97 [mod] [BREAK] [#278] Merge work from message queue v2 branch (**see linked commit message for details**)

## Fixes since `v3.2.0`

* 2e6722bf [fix] [#281 #279] `parse-uri`: only provide ACL params when non-empty (@frwdrik)
* c68c995c [fix] [#283] Support new command.json spec format used for `XTRIM`, etc.

## New since `v3.2.0`

* f79a6404 [new] Update commands to match latest `commands.json` spec
* 19d97ebd [new] [#282] Add support for TLS URIs to URI parser
* 5cfdbbbd [new] Add `scan-keys` util
* 37f0030a [new] Add `set-min-log-level!` util
* GraalVM compatibility is now tested during build

---

# `v3.2.0` (2022-12-03)

```clojure
[com.taoensso/carmine "3.2.0"]
```

> This is a major feature + maintenance release. It should be **non-breaking**.  
> See [here](https://github.com/taoensso/encore#recommended-steps-after-any-significant-dependency-update) for recommended steps when updating any Clojure/Script dependencies.

## Changes since `v3.1.0`

* [#279] [#257] Any username in conn-spec URI will now automatically be used for ACL in AUTH

## Fixes since `v3.1.0`

* Fix warning of parse var replacements (`parse-long`, `parse-double`)

## New since `v3.1.0`

* [#201] [#224] [#266] Improve `wcar` documentation, alias pool constructor in main ns
* [#251] [#257] [#270] Add support for username (ACL) in AUTH (@lsevero @sumeet14)
* Update to latest commands.json (from 2022-09-22)
* [#259] [#261] Ring middleware: add `:extend-on-read?` option (@svdo)
* [Message queue] Print extra debug info on invalid handler status

## Other improvements since `v3.1.0`

* [#264] Remove unnecessary reflection in Tundra (@frwdrik)
* Stop using deprecated Encore vars
* [#271] Refactor tests, fix flakey results
* Update dependencies
* Work [underway](https://github.com/users/ptaoussanis/projects/2) on Carmine v4, which'll introduce support for RESP3, Redis Sentinel, Redis Cluster, and more.

---

# v3.1.0 (2020-11-24)

```clojure
[com.taoensso/carmine "3.1.0"]
```

> This is a minor feature release. It should be non-breaking.

## New since `v3.0.0`

* [#244 #245] Message queue: add `initial-backoff-ms` option to `enqueue` (@st3fan)

---

# v3.0.0 (2020-09-22)

```clojure
[com.taoensso/carmine "3.0.0"]
```

> This is a major feature + security release. It may be **BREAKING**.  
> See [here](https://github.com/taoensso/encore#recommended-steps-after-any-significant-dependency-update) for recommended steps when updating any Clojure/Script dependencies.

## Changes since `v2.20.0`

  - **[BREAKING]** Bumped minimum Clojure version from `v1.5` to `v1.7`.
  - **[BREAKING]** Bump Nippy to [v3](https://github.com/taoensso/nippy/releases/tag/v3.0.0) for an **important security fix**. See [here](https://github.com/taoensso/nippy/issues/130) for details incl. **necessary upgrade instructions** for folks using Carmine's automatic de/serialization feature.
  - Bump Timbre to [v5](https://github.com/taoensso/timbre/releases/tag/v5.0.0).

## New since `v2.20.0`

  - Update to [latest Redis commands spec](https://github.com/redis/redis-doc/blob/25555fe05a571454fa0f11dca28cb5796e04112f/commands.json).
  - Listeners (incl. Pub/Sub) significantly improved:
    - [#15] Allow `conn-alive?` check for listener connections
    - [#219] Try ping on socket timeouts for increased reliability (@ylgrgyq)
    - [#207] Publish connection and handler errors to handlers, enabling convenient reconnect logic (@aravindbaskaran)
    - [#207] Support optional ping-ms keep-alive for increased reliability (@aravindbaskaran)
    - Added `parse-listener-msg` util for more easily writing handler fns
    - Allow `with-new-pubsub-listener` to take a single direct handler-fn

---

# Earlier releases

See [here](https://github.com/taoensso/carmine/releases) for earlier releases.