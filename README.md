<a href="https://www.taoensso.com/clojure" title="More stuff by @ptaoussanis at www.taoensso.com"><img src="https://www.taoensso.com/open-source.png" alt="Taoensso open source" width="340"/></a>  
[**API**][cljdoc] | [**Wiki**][GitHub wiki] | Latest releases: [v3.7.1](../../releases/tag/v3.7.1) (2026-11-21) / [v4.0.0-alpha1](../../releases/tag/v4.0.0-alpha1) (TODO)

[![Main tests][Main tests SVG]][Main tests URL]
[![Graal tests][Graal tests SVG]][Graal tests URL]

# Carmine

### [Redis](https://en.wikipedia.org/wiki/Redis) client + message queue for Clojure

Redis and Clojure are individually awesome, and **even better together**.

Carmine is a mature Redis client for Clojure that offers an idiomatic Clojure API with plenty of **speed**, **power**, and **ease-of-use**.

## Why Carmine?

- High-performance **pure-Clojure** library
- [Fully documented API](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine) with support for the **latest Redis commands and features**
- Easy-to-use, production-ready **connection pooling**
- Auto **de/serialization** of Clojure data types via [Nippy](https://www.taoensso.com/nippy)
- Fast, simple [message queue](../../wiki/3-Message-queue) API
- Fast, simple [distributed lock](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.locks) API

## Version 4

The next generation of Carmine is now available as a **preview** for early testing and feedback. Highlights:

- Improved connection API with Sentinel and Cluster support
- Modern core with full RESP3 protocol support
- Numerous API, reliability, and efficiency improvements

For more info: [v4 preview guide](../../wiki/5-Carmine-v4) |
[v4 API](https://cljdoc.org/d/com.taoensso/carmine/4.0.0-alpha1/api/taoensso.carmine-v4)

From v4, Carmine ships **two coexisting APIs** in one artifact:

| API | Namespace             | Status                          | Available in   |
| --- | --------------------- | ------------------------------- | -------------- |
| v3  | `taoensso.carmine`    | Mature, stable (fixes only)     | `v3.x`, `v4.x` |
| v4  | `taoensso.carmine-v4` | Preview, may still change       | `v4.x`         |

So **both APIs** will be maintained and supported moving forward under the `4.x` artifact line.

- **Existing projects** can update to `v4.x` with no code changes, then keep using the v3 API or mix and match
- **New projects** will likely want to start on the v4 API once it's stable
- New `v3.x` releases will stop once `v4.x` is stable
- A library using the v3 API and another using the v4 API can share one `v4.x` Carmine dependency

## Compatibility

| Redis flavour                                                                             | Features                            | Carmine v3 | Carmine v4 (preview) |
| :---------------------------------------------------------------------------------------- | :---------------------------------- | :--------: | :------------------: |
| Single node                                                                               | Simplest setup                      | ✅          | ✅                    |
| Redis [Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/) | High availability                   | -          | ✅                    |
| Redis [Cluster](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)   | High availability, sharding         | -          | ✅ ([partial][v4 status])  |
| Redis [Enterprise](https://redis.io/docs/latest/operate/rs/)                              | High availability, sharding         | ✅          | ✅                    |
| Redis [Cloud](https://redis.io/cloud/)                                                    | High availability, sharding, hosted | ✅          | ✅                    |

## Documentation

- [Wiki][GitHub wiki] (getting started, usage, etc.)
- API reference via [cljdoc][cljdoc]
- Support: [GitHub issues][]

## Funding

You can [help support][sponsor] continued work on this project and [others][my work], thank you!! 🙏

## License

Copyright &copy; 2014-2026 [Peter Taoussanis][].  
Licensed under [EPL 1.0](LICENSE.txt) (same as Clojure).

<!-- Common -->

[GitHub releases]: ../../releases
[GitHub issues]:   ../../issues
[GitHub wiki]:     ../../wiki
[v4 status]:       ../../wiki/5-Carmine-v4#status

[Peter Taoussanis]: https://www.taoensso.com
[sponsor]:          https://www.taoensso.com/sponsor
[my work]:          https://www.taoensso.com/clojure-libraries

<!-- Project -->

[cljdoc]: https://cljdoc.org/d/com.taoensso/carmine/

[Clojars SVG]: https://img.shields.io/clojars/v/com.taoensso/carmine.svg
[Clojars URL]: https://clojars.org/com.taoensso/carmine

[Main tests SVG]:  https://github.com/taoensso/carmine/actions/workflows/main-tests.yml/badge.svg
[Main tests URL]:  https://github.com/taoensso/carmine/actions/workflows/main-tests.yml
[Graal tests SVG]: https://github.com/taoensso/carmine/actions/workflows/graal-tests.yml/badge.svg
[Graal tests URL]: https://github.com/taoensso/carmine/actions/workflows/graal-tests.yml
