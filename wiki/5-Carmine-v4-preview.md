# Carmine v4 preview

> Carmine v4 is experimental and its API may change before release. Carmine v3
> remains the production default; the rest of this wiki currently documents v3.

V4 is a substantial rewrite for current Redis deployments. Its goals are to
support RESP3 and high-availability Redis directly, make resources and failure
boundaries explicit, and replace the v3 message queue with a safer and more
observable design.

## Why v4

- Native RESP2/RESP3 support, including typed replies, attributes, and pushes.
- Native Redis Sentinel and Redis Cluster support.
- Explicit, observable connection management without hidden global pools.
- Dedicated transaction and supervised Pub/Sub APIs for connection-affine work.
- One documented Clojure function for each command in the pinned Redis spec.
- A new Redis 7+ message queue with fenced leases, server-time scheduling,
  priorities, coalescing, and retained dead letters.

## Connection management

The largest everyday API change is that v4 operations take a long-lived
connection manager, rather than a v3 pool/spec options map on each `wcar` call:

```clojure
(require '[taoensso.carmine-v4 :as car])

(defonce mgr
  (car/conn-manager
    {:conn-opts {:server "redis://localhost:6379/"}}))

(car/wcar mgr
  (car/set "user:1" "Ada")
  (car/get "user:1"))
;; => ["OK" "Ada"]
```

Create one manager for each Redis configuration and close it during application
shutdown. Managers expose statistics, callbacks, redacted diagnostics, and
explicit clear/close operations. Pooled, unpooled, and Cluster managers are
available; standalone and Sentinel configuration use the ordinary pooled
manager.

## Main changes from v3

| Concern | V3 | V4 preview |
|---|---|---|
| Request entry point | `wcar` takes pool/spec options | `wcar` takes a connection manager |
| Arbitrary commands | `redis-call` | `rcmd` / `rcmd*` |
| Reply conversion | Composable `parse-*` helpers | One active parser, with explicit special read modes |
| Transactions | `atomic` / `atomic*` | Phase-separated `transact!` |
| Sentinel and Cluster | Application-managed | Native support |
| Pub/Sub | Listener helpers | Dedicated supervised listener with explicit lifecycle |
| Message queue | V3 lock-based queue | Separate Redis 7+ fenced-lease queue |

V3 and v4 can run in the same process and share ordinary Redis data. Their
Nippy formats are compatible, but they do not share pools, managers, or
configuration. Connection-stateful commands such as `WATCH`, `MULTI`, and
subscriptions must use the dedicated v4 transaction and Pub/Sub APIs.

Distributed locks, Ring sessions, and Tundra have not been ported to v4.
Cluster routing currently targets masters; replica reads, Cluster transactions,
and Cluster Pub/Sub are not supported.

## Message queue

The v4 queue provides persistent, at-least-once delivery. A claim has an opaque
lease token; settlement and extension check that token so a stale handler cannot
change a newer claim. Handlers must still make external operations idempotent,
and retryable producers should supply a stable explicit message ID.

Redis server time controls schedules and leases. Messages move through ready,
scheduled, and leased states, then are acknowledged, discarded, retried, or
retained as dead letters. The queue also supports priorities, coalescing and
revisions, live configuration updates, exact queue-level observability, and
optional durability barriers. See the
[state diagram](../blob/dev/doc/v4/mq-architecture.svg).

The v4 queue uses a separate keyspace and data model, so v3 and v4 queues can
coexist during a cutover. Migration is an explicit, non-destructive snapshot
copy from a paused v3 queue; it is not a live bridge.

## API documentation

- [Core v4 API](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine-v4)
- [V4 message queue](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine-v4.message-queue)
- [V3-to-v4 queue migration](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine-v4.message-queue.migration)
- [Benchmark baseline](../blob/dev/doc/v4/benchmarks.md)
