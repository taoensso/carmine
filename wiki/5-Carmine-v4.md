# Carmine v4

> **Preview**: v4's API may still change before its final release, so
> please use it for evaluation and feedback rather than production. Carmine v3
> remains the production default, and the rest of this wiki documents v3.

Carmine v4 is a substantial rewrite of Carmine's core that adds RESP3,
Sentinel and Cluster support. It lives in its own `taoensso.carmine-v4`
namespace alongside v3, so you can try it without changing existing v3 code.

## Why v4

- **RESP3** by default, with richer reply types (e.g. maps), attributes, and
  pushes. RESP2 is still supported.
- **Sentinel and Cluster** support built in: master discovery and failover,
  routing by key, and handling of Cluster redirects.
- **Explicit connection managers** in place of hidden global pools. Each has
  its own options, statistics and callbacks, and is closed when you say so.
- **Errors that are hard to miss**: a Redis error reply throws by default,
  where v3 returned it as a value among the other replies.
- **Simpler transactions and Pub/Sub**: `transact!` runs WATCH/MULTI/EXEC and
  retries on conflict. Pub/Sub listeners reconnect and resubscribe on their
  own.
- **The same message queue**, now also usable with Sentinel.
- **Speed** similar to v3 for single commands, often faster for large
  pipelines.

## Status

Try v4 if you'd like Sentinel, Cluster or RESP3 support, or want to help shape
the API before it's final. Stay on v3 for anything that needs a stable API.

- **Getting it**: v4 ships in the ordinary Carmine artifact from
  `v4.0.0-alpha1`. That's a pre-release, so ask for it explicitly:
  `com.taoensso/carmine {:mvn/version "4.0.0-alpha1"}`.
- **Requirements**: Redis 6+ for RESP3, which v4 uses by default. Set
  `:init {:resp3? false}` to use RESP2 with older servers. The message queue
  is best with Redis 6+: on older servers new messages can't wake idle
  workers early.
- **Not ported to v4**: distributed locks, Ring sessions, and Tundra. The v3
  versions still work alongside v4.
- **Cluster support is the most experimental part** of v4, and the least
  tested against real deployments. It doesn't yet support transactions,
  Pub/Sub, the message queue, or `lua-local` (these throw if tried), and
  reads always go to masters.
- **Reference**: [core API][v4 API], [message queue API][v4 MQ API],
  [changelog](../blob/master/CHANGELOG.md).
- **Feedback** is very welcome on [GitHub](../issues), especially on the
  API, Sentinel and Cluster behaviour, and migrating from v3.

## Quick start

V4 operations take a long-lived connection manager, rather than a pool/spec
options map on each `wcar` call:

```clojure
(require '[taoensso.carmine-v4 :as car])

(defonce mgr
  (car/conn-manager
    {:conn-opts {:server "redis://localhost:6379/"}}))

(car/wcar mgr
  (car/set "user:1" "Ada")
  (car/get "user:1"))
;; => ["OK" "Ada"]

(car/conn-manager-close! mgr) ; During application shutdown
```

Create one manager for each Redis configuration. Authentication, database,
TLS and timeouts are connection options:

```clojure
{:conn-opts
 {:server      ["redis.example.com" 6380]
  :init        {:auth {:username "default", :password "secret"}
                :select-db 3}
  :socket-opts {:ssl true, :read-timeout-ms 5000}}}

;; Or as a URI:
{:conn-opts {:server "rediss://default:secret@redis.example.com:6380/3"}}
```

Reads time out after 10 seconds by default, so a hung server can't block
callers forever. For blocking or slow commands (e.g. `BLPOP`, long Lua
scripts), use a manager with a higher or nil `:read-timeout-ms`. The message
queue and Pub/Sub handle their own blocking waits.

See [`default-conn-opts`][] and [`default-pool-opts`][] for all options. To
monitor a manager, see `conn-manager-stats` and the `:cbs` connection option.

### Sentinel and Cluster

A Sentinel or Cluster spec is a long-lived stateful value that tracks the
deployment. Create it once and share it between managers:

```clojure
(def sentinel-spec
  (car/sentinel-spec
    {"my-master" [["10.0.0.1" 26379] ["10.0.0.2" 26379] ["10.0.0.3" 26379]]}))

(def mgr
  (car/conn-manager
    {:conn-opts {:server {:master-name "my-master", :sentinel-spec sentinel-spec}}}))
```

```clojure
(def cluster-spec
  (car/cluster-spec [["10.0.1.1" 7000] ["10.0.1.2" 7000] ["10.0.1.3" 7000]]))

(def mgr
  (car/conn-manager
    {:conn-opts {:server {:cluster-spec cluster-spec}}}))

(car/wcar mgr
  (car/set  "user:1" "Ada")   ; Requests are routed by key,
  (car/set  "user:2" "Grace") ; and may go to different nodes
  (car/mget "{user:1}:name" "{user:1}:age") ; Keys of one command must share a slot
  (car/with-cluster-target :any (car/dbsize))) ; Most commands without a key need a target
```

If the Sentinels themselves need authentication or TLS, set that in the
[`sentinel-spec`][] options. To read from replicas, add
`:sentinel-opts {:prefer-read-replica? true}` to `:server`.

Some features aren't supported with Cluster yet, see [Status](#status).

## Migrating from v3

V3 and v4 can run in the same process and share Redis data, including Nippy
data and message queues. So you can migrate one part of an application at a
time. They don't share pools or other configuration.

In short:

1. Require `taoensso.carmine-v4` in place of `taoensso.carmine`.
2. Create a connection manager, and pass it to `wcar` in place of the
   pool/spec options map.
3. Expect Redis error replies to throw, see [Errors](#errors).
4. Expect some replies as richer types (e.g. `HGETALL` returns a map with
   string keys; bind `*keywordize-maps?*` for keyword keys), or set
   `:init {:resp3? false}` to keep RESP2 replies.

The rest of this section is reference, to look up as needed.

### Connections

Give the `:pool` and `:spec` options of v3's `wcar` to a manager instead, as
`:pool-opts` and `:conn-opts`. V3 spec keys map to v4 `:conn-opts` as follows:

| V3 spec | V4 `:conn-opts` |
|---|---|
| `:host`, `:port`, `:uri` | `:server` |
| `:username`, `:password` | `:init :auth` |
| `:db` | `:init :select-db` |
| `:timeout-ms`, `:conn-timeout-ms`, `:read-timeout-ms` | `:socket-opts :connect-timeout-ms`, `:read-timeout-ms` |
| `:ssl-fn` | `:socket-opts :ssl` (`true` in place of `:default`) |
| `:pool :none` | Use `conn-manager-unpooled` |
| `:pool {:max-total-per-key _, :max-idle-per-key _}` | `:pool-opts {:max-total _, :max-idle _}` |

Some defaults differ: connections use RESP3, the connect timeout is 400ms,
and the read timeout is 10s (v3 had none).

### Renamed and replaced

| V3 | V4 |
|---|---|
| `(wcar {:pool _ :spec _} ...)` | `(wcar mgr ...)` |
| `(wcar opts :as-pipeline ...)` | `(wcar mgr :as-vec ...)` |
| `redis-call` | `rcmd`, `rcmd*` |
| `return` | `local-echo` |
| `raw` | `bytes` |
| `parse-raw` | `as-bytes`, with `*auto-thaw?*` bound false |
| `parse-nippy` | `with-thaw-opts` |
| `parse-suppress` | `skip-replies` |
| `parse-int`, `parse-long` | `as-?long` (nil if missing or not numeric), or `as-long` (error if missing or not numeric) |
| `parse-float`, `parse-double` | `as-?double`, or `as-double` |
| `parse-keyword` | `as-?kw`, or `as-kw` |
| `atomic`, `atomic*` | `transact!`, with `:max-attempts` |
| `(wcar opts (swap k f))` | `(swap mgr k f)`, outside `wcar` |
| `(reduce-scan rf init scan-fn)` | `(scan-reduce init scan-fn rf)`; `rf` now gets one element at a time, not a page |
| `reduce-hscan` | `scan-reduce-kv`; de-duplication is now opt-in via `:dedupe?`, since it keeps every seen key in memory |
| `with-new-pubsub-listener` | `pubsub-listener`, with one `:on-message` |
| `with-open-listener`, `close-listener` | `pubsub-subscribe!` / `pubsub-unsubscribe!`, `pubsub-close!` |

`key`, `parse-map`, `parser-comp`, `parse-listener-msg` and the generic
`with-new-listener` have no v4 equivalent.

### Errors

A Redis error reply now throws by default, after all replies in the `wcar`
have been read. V3 threw only for a single reply, and otherwise returned the
error as a value. Other commands in the `wcar` still ran. The exception's data
includes every reply, and its cause is the first error:

```clojure
(try
  (car/wcar mgr
    (car/set  "k" "v")
    (car/incr "k"))
  (catch clojure.lang.ExceptionInfo e
    (ex-data e)))
;; => {:eid :carmine/reply-errors
;;     :replies ["OK" <reply error>], :error-indexes [1]}
```

To get reply errors as values instead, use `(car/wcar mgr {:error-mode :return} ...)`
and test replies with `car/reply-error?`. A reply error's data has e.g.
`:code "WRONGTYPE"` in place of v3's `:prefix :wrongtype`, and
`(car/reply-error? {:code "WRONGTYPE"} x)` matches both such a reply and an
exception thrown by `wcar` for one.

A connection failure during a request (e.g. a read timeout) throws with
`:eid :carmine.conns/conn-io-error` and the server's `:addr`. The request may
or may not have run.

### Transactions

[`transact!`][] replaces `atomic`. Call it outside `wcar`. To use watched
values, read them in a first function and queue writes in a second:

```clojure
(car/transact! mgr {:watch-keys ["counter"]}
  (fn [] (car/with-replies (car/as-?long (car/get "counter"))))
  (fn [current] (car/set "counter" (inc (or current 0)))))
;; => {:replies ["OK"], :plan <previous value>, :attempts <attempt count>}
```

A WATCH conflict calls both functions again, so keep other side effects out of
them. Like `wcar`, `transact!` throws if a command fails (use
`:error-mode :return` to get errors as values). Raw `WATCH` and `MULTI`
commands still work inside one `wcar`, but the connection is then closed
rather than returned to its pool.

### Pub/Sub

```clojure
(def listener
  (car/pubsub-listener
    {:conn-opts  {:server ["127.0.0.1" 6379]}
     :init-subs  {:channels ["updates"], :patterns ["news.*"]}
     :on-message (fn [{:keys [channel payload]}] (println channel payload))}))

(car/pubsub-subscribe!   listener {:channels ["alerts"]})
(car/pubsub-unsubscribe! listener {:channels ["updates"]})
(car/wcar mgr (car/publish "alerts" {:any "value"}))
(car/pubsub-close! listener)
```

Differences from v3's `with-new-pubsub-listener`:

- One `:on-message` function replaces the per-channel handler map. It gets a
  map like `{:kind :message, :channel _, :payload _}` (`:pmessage` adds
  `:pattern`) in place of v3's vector. To dispatch by channel as before:
  `(fn [{:keys [channel] :as msg}] (when-let [f (get handlers channel)] (f msg)))`.
- An optional `:on-event` function gets everything else: subscription
  acknowledgements, and listener events such as `:conn-error`, `:recovered`
  and `:closed`. This replaces v3's `"carmine:listener:error"` channel.
- After a connection failure the listener reconnects and resubscribes by
  itself. Messages published in the meantime are lost, as always with Redis
  Pub/Sub.
- The listener pings Redis every 10 seconds by default (`:ping-ms`), and also
  reconnects if the connection then stays silent.
- `pubsub-subscribe!` and `pubsub-unsubscribe!` wait for Redis to confirm the
  change, and return true once it has, after which it's safe to publish. From
  inside `:on-message` they can't wait, and return false.
- The listener takes its own `:conn-opts` (the same map that you'd give a
  manager), and owns a dedicated connection until closed.

`:on-message` and `:on-event` block the listener while they run, so hand slow
work to another thread. See [`pubsub-listener`][] for details.

### Serialization

Like v3, v4 automatically freezes non-native arguments with
[Nippy](https://www.taoensso.com/nippy) and thaws the replies that it marked.
Custom Nippy options (e.g. a `:password` for encryption) use the same
`with-thaw-opts` and `taoensso.nippy.tools/*freeze-opts*` as v3, so existing
configuration carries over.

Two things to know:

- Requests use the options in effect when they're queued, and Pub/Sub
  listeners those in effect when they're created. So bind options around the
  `wcar` body or around listener creation.
- `(thaw opts ...)` also thaws blobs that Carmine didn't mark, e.g. Nippy data
  written by other clients. For ordinary Carmine data with a password, prefer
  `with-thaw-opts`.

### Other behaviour changes

- `swap` and `hswap` try up to 8 times by default, then throw
  `:carmine.swap/conflict` (v3 retried without limit).
- `compare-and-set` and `compare-and-hset` reply `true`/`false` (v3 replied
  `1`/`0`), and are now pipelined like other commands. In place of a
  `:cas/delete` new value, use `compare-and-delete` or `compare-and-hdel`.
- `lua-local` now tracks loaded scripts per connection, so it's safe across
  server restarts.
- `*issue-83-workaround?*` is false by default (v3's `issue-83-workaround?`
  was true). Enable it only if you might read data written by Carmine v2.6.0
  to v2.6.1.
- Character arguments are now sent as text, so `\A` and `"A"` name the same
  key (v3 froze characters with Nippy).

### Message queue

`taoensso.carmine-v4.message-queue` ports the v3 [message queue](./3-Message-queue)
to v4 connection managers. It keeps v3's Lua scripts, Redis key layout and
delivery semantics, so v3 and v4 clients can share queues without a data
migration. Standalone and Sentinel managers are supported.

To migrate:

1. Change the namespace to `taoensso.carmine-v4.message-queue`.
2. Pass a connection manager wherever v3 took `conn-opts`: to `worker`,
   `queue-status`, `queues-clear!!`, etc.
3. Keep calling `enqueue`, `message-status` and `cancel` inside a `wcar`:
   `(car/wcar mgr (mq/enqueue qname msg opts))`. Or pass the manager as first
   argument instead: `(mq/enqueue mgr qname msg opts)`.
4. Give workers a suitable manager, ideally their own. Its pool needs a
   connection for each handler thread.
5. Shut down in order: close workers (`.close` or `with-open`), then the
   manager. To let active handlers finish first, call
   `(mq/drain worker timeout-ms)` after closing each worker.
6. If you use custom Nippy options, wrap worker creation (or `mq/start`) and
   queue inspection in `with-thaw-opts`.
7. Replace the old forms below. They were removed and now throw, as do
   unknown (e.g. misspelled) options:

| Removed | Use instead |
|---|---|
| `(enqueue q msg mid can-requeue?)` | `(enqueue q msg {:mid _, :can-requeue? _})` |
| `:unique-message-id` | `:mid` |
| `:initial-backoff-ms` | `:init-backoff-ms` |
| `:allow-requeue?` | `:can-requeue?` |
| `:nthreads`, `:nthreads-worker` | `:nthreads-handler` (or `:auto-start false` in place of 0 threads) |
| `:auto-start?`, `:auto-start <msecs>` | `:auto-start <boolean>`, with `mq/start` to start later |
| `:throttle-ms :auto` | `:throttle-ms default-throttle-ms-fn` |
| `(queues-clear!! mgr q1 q2)` | `(queues-clear!! mgr [q1 q2])` |
| `clear-queues` | `queues-clear!!` |
| `make-dequeue-worker` | `worker` |

Behaviour differences from v3:

- A worker stops if its manager closes. A handler still active then can't
  finalize, so its message is redelivered after its lock expires.
- `enqueue`, `message-status` and `cancel` throw inside `transact!` effects.
- Functions that write reject Sentinel managers that prefer read replicas.

[v4 API]:    https://cljdoc.org/d/com.taoensso/carmine/4.0.0-alpha1/api/taoensso.carmine-v4
[v4 MQ API]: https://cljdoc.org/d/com.taoensso/carmine/4.0.0-alpha1/api/taoensso.carmine-v4.message-queue

[`default-conn-opts`]: https://cljdoc.org/d/com.taoensso/carmine/4.0.0-alpha1/api/taoensso.carmine-v4#default-conn-opts
[`default-pool-opts`]: https://cljdoc.org/d/com.taoensso/carmine/4.0.0-alpha1/api/taoensso.carmine-v4#default-pool-opts
[`sentinel-spec`]:     https://cljdoc.org/d/com.taoensso/carmine/4.0.0-alpha1/api/taoensso.carmine-v4#sentinel-spec
[`transact!`]:         https://cljdoc.org/d/com.taoensso/carmine/4.0.0-alpha1/api/taoensso.carmine-v4#transact!
[`pubsub-listener`]:   https://cljdoc.org/d/com.taoensso/carmine/4.0.0-alpha1/api/taoensso.carmine-v4#pubsub-listener
