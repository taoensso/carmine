This project uses [**Break Versioning**](https://www.taoensso.com/break-versioning).

---

# `v3.5.0` (2025-11-06)

- **Dependency**: [on Clojars](https://clojars.org/com.taoensso/carmine/versions/3.5.0)
- **Versioning**: [Break Versioning](https://www.taoensso.com/break-versioning)

This is a **major maintenance release** with many improvements. It includes a [breaking change](https://github.com/taoensso/carmine/commit/ee75844c6515f6e692fd6fdb2c6fe57ee7aaede1) regarding Carmine's logging. Please report any unexpected problems to the [Slack channel](http://taoensso.com/carmine/slack) or [GitHub](https://github.com/taoensso/carmine/issues).

A big thanks to all contributors and testers on this release! 🙏

\- [Peter Taoussanis](https://www.taoensso.com)

## Since `v3.4.1` (2024-05-30)

### Changes

- **➤ \[mod] [BREAKING]** Switch logging: [Timbre](https://www.taoensso.com/timbre) -> [Trove](https://www.taoensso.com/trove) \[ee75844c]
- \[mod] Drop support for Clojure 1.9 \[70a60652]

### New

- \[new] First publicly available experimental Carmine v4 core for early testers (currently undocumented)
- \[new] Update command spec \[52c0cc0d]
- \[new] Now use [Truss contextual exceptions](https://github.com/taoensso/truss#contextual-exceptions) for all errors \[3c5ede77]
- \[new] \[#322] Tests: use shared config, make benchmarks easier to tune (@kaizer113) \[3a21e98b]
- \[new] \[mq] Add experimental `:reset-init-backoff?` opt for debouncing, etc. \[983fde98]
- \[new] \[mq] [#315] [#316] Allow zero-thread workers (@adam-jijo-swym) \[7e9adce1]
- \[new] \[#313] Replace lua code with native command (@GabrielHVM) \[ed109343]
- \[doc] \[#314] Fix wiki typos (@mardukbp) \[6c20de1d]
- \[doc] \[#317] `wcar` docstring improvements \[d4265aad]
- \[doc] Add migration info re: v3.3 MQ holding connections longer \[8dc6fc14]

### Fixes

- None

---

# `v3.4.1` (2024-05-30)

> **Dep**: Carmine is [on Clojars](https://clojars.org/com.taoensso/carmine/versions/3.4.1).  
> **Versioning**: Carmine uses [Break Versioning](https://www.taoensso.com/break-versioning).

This is a **hotfix release** that should be **non-breaking**.

And as always **please report any unexpected problems** - thank you! 🙏

\- [Peter Taoussanis](https://www.taoensso.com)

## Fixes since `v3.4.0` (2024-05-28)

* 9dd67207 [fix] [mq] [#305] Typo in final loop error handler
* 81f58d80 [fix] [mq] Monitor `:ndry-runs` arg should be an int
* 41e5ed34 [fix] [mq] Properly assume `:success` handler status by default

---

# `v3.4.0` (2024-05-28)

> **Dep**: Carmine is [on Clojars](https://clojars.org/com.taoensso/carmine/versions/3.4.0).  
> **Versioning**: Carmine uses [Break Versioning](https://www.taoensso.com/break-versioning).

This is a **security and maintenance release** that should be **non-breaking** for most users.

⚠️ It addresses a [**security vulnerability**](https://github.com/taoensso/nippy/security/advisories/GHSA-vw78-267v-588h) in [Nippy](https://www.taoensso.com/nippy)'s upstream compression library and is **recommended for all existing users**. Please review the [relevant Nippy release info](https://github.com/taoensso/nippy/releases/tag/v3.4.2), and **ensure adequate testing** in your environment before updating production data.

And as always **please report any unexpected problems** - thank you! 🙏

\- [Peter Taoussanis](https://www.taoensso.com)

## Changes since `v3.3.2` (2023-10-24)

- Updated to [latest stable Nippy](https://github.com/taoensso/nippy/releases/tag/v3.4.2). This should be a non-breaking change for most users, but **please ensure adequate testing** in your environment before updating production data.

## Fixes since `v3.3.2` (2023-10-24)

* 105cecb7 [fix] [mq] [#299] Fix `queue-names` and `queues-clear-all!!!` typos
* 95c52aaa [fix] [mq] [#301] Document behaviour on handler errors
* 9c8b9963 [fix] [#307] Fix Pub/Sub channel handlers masking pattern handlers
* Fixed several typos in docs/README (@chage, @J0sueTM, @mohammedamarnah)

## New since `v3.3.2` (2023-10-24)

* 12c4100d [new] Update Redis command spec (2024-05-27)
* Several wiki and docstring improvements

---

# `v3.3.2` (2023-10-24)

> 📦 [Available on Clojars](https://clojars.org/com.taoensso/carmine/versions/3.3.2), this project uses [Break Versioning](https://www.taoensso.com/break-versioning).

This is a **minor hotfix release** and should be a safe upgrade for users of `v3.3.0`.

## Fixes since `v3.3.1`

* be9d4cd1 [#289] [fix] Typo in previous hotfix

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
