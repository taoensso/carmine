<a href="https://www.taoensso.com/clojure" title="More stuff by @ptaoussanis at www.taoensso.com"><img src="https://www.taoensso.com/open-source.png" alt="Taoensso open source" width="340"/></a>  
[**API**][cljdoc] | [**Wiki**][GitHub wiki] | [Slack][] | Latest release: [v3.5.0](../../releases/tag/v3.5.0) (2025-11-06)

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

## Compatibility

Redis is available in a few different flavours depending on your needs:

|                                                                                           | Features                            | Support?            |
| ----------------------------------------------------------------------------------------- | ----------------------------------- | ------------------- |
| Single node                                                                               | Simplest setup                      | Yes                 |
| Redis [Sentinel](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/) | High availability                   | No (possibly later) |
| Redis [Cluster](https://redis.io/docs/latest/operate/oss_and_stack/management/scaling/)   | High availability, sharding         | No (possibly later) |
| Redis [Enterprise](https://redis.io/docs/latest/operate/rs/)                              | High availability, sharding         | Yes                 |
| Redis [Cloud](https://redis.io/cloud/)                                                    | High availability, sharding, hosted | Yes                 |

## Documentation

- [Wiki][GitHub wiki] (getting started, usage, etc.)
- API reference via [cljdoc][cljdoc]
- Support: [Slack][] or [GitHub issues][]

## Funding

You can [help support][sponsor] continued work on this project and [others][my work], thank you!! 🙏

## License

Copyright &copy; 2014-2025 [Peter Taoussanis][].  
Licensed under [EPL 1.0](LICENSE.txt) (same as Clojure).

<!-- Common -->

[GitHub releases]: ../../releases
[GitHub issues]:   ../../issues
[GitHub wiki]:     ../../wiki
[Slack]: https://www.taoensso.com/carmine/slack

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
