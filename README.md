<a href="https://www.taoensso.com/clojure" title="More stuff by @ptaoussanis at www.taoensso.com"><img src="https://www.taoensso.com/open-source.png" alt="Taoensso open source" width="340"/></a>  
[**API**][cljdoc] | [**Wiki**][GitHub wiki] | [Latest releases](#latest-releases) | [Slack channel][]

# Carmine

### [Redis](https://en.wikipedia.org/wiki/Redis) client + message queue for Clojure

Redis and Clojure are individually awesome, and **even better together**.

Carmine is a mature Redis client for Clojure that offers an idiomatic Clojure API with plenty of **speed**, **power**, and **ease-of-use**.

## Latest release/s

- `2024-05-30` `v3.4.1`: [release info](../../releases/tag/v3.4.1)

[![Main tests][Main tests SVG]][Main tests URL]
[![Graal tests][Graal tests SVG]][Graal tests URL]

See [here][GitHub releases] for earlier releases.

## Why Carmine?

- High-performance **pure-Clojure** library
- [Fully documented API](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine) with support for the **latest Redis commands and features**
- Easy-to-use, production-ready **connection pooling**
- Auto **de/serialization** of Clojure data types via [Nippy](https://www.taoensso.com/nippy)
- Fast, simple [message queue](../../wiki/3-Message-queue) API
- Fast, simple [distributed lock](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.locks) API

## Documentation

- [Wiki][GitHub wiki] (getting started, usage, etc.)
- API reference via [cljdoc]
- Support: [Slack channel][] or [GitHub issues][]

## Funding

You can [help support][sponsor] continued work on this project, thank you!! 🙏

## License

Copyright &copy; 2014-2024 [Peter Taoussanis][].  
Licensed under [EPL 1.0](LICENSE.txt) (same as Clojure).

<!-- Common -->

[GitHub releases]: ../../releases
[GitHub issues]:   ../../issues
[GitHub wiki]:     ../../wiki
[Slack channel]: https://www.taoensso.com/carmine/slack

[Peter Taoussanis]: https://www.taoensso.com
[sponsor]:          https://www.taoensso.com/sponsor

<!-- Project -->

[cljdoc]: https://cljdoc.org/d/com.taoensso/carmine/

[Clojars SVG]: https://img.shields.io/clojars/v/com.taoensso/carmine.svg
[Clojars URL]: https://clojars.org/com.taoensso/carmine

[Main tests SVG]:  https://github.com/taoensso/carmine/actions/workflows/main-tests.yml/badge.svg
[Main tests URL]:  https://github.com/taoensso/carmine/actions/workflows/main-tests.yml
[Graal tests SVG]: https://github.com/taoensso/carmine/actions/workflows/graal-tests.yml/badge.svg
[Graal tests URL]: https://github.com/taoensso/carmine/actions/workflows/graal-tests.yml
