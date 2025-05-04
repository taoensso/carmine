This page details possible **breaking changes and migration instructions** for Carmine.

My apologies for the trouble. I'm very mindful of the costs involved in breaking changes, and I try hard to avoid them whenever possible. When there is a very good reason to break, I'll try to batch breaks and to make migration as easy as possible.

Thanks for your understanding - [Peter Taoussanis](https://www.taoensso.com)

# Carmine `v3.2.x` to `v3.3.x`

There are **breaking changes** to the **Carmine message queue API** that **may affect** a small proportion of message queue users.

- If you **DO NOT** use Carmine's [message queue API](https://github.com/taoensso/carmine/wiki/3-Message-queue), no migration should be necessary.
- If you **DO** use Carmine's message queue API, **please read the below checklist carefully**!!

## Migration checklist for message queue users

1. **Please be aware**: v3.3 introduces major changes (improvements) to Carmine's message queue architecture and API.
   
   While the changes have been extensively tested and used for months on private systems, there is always a non-zero chance of unexpected issues when so much code is touched.
   
   Therefore please **carefully test Carmine v3.3** before deploying to production, and please **carefully monitor your queues** for expected behaviour and performance.
   
   New tools are provided for queue monitoring. See the [API improvements](#api-improvements) section for details.
   
2. [`enqueue`](https://taoensso.github.io/carmine/taoensso.carmine.message-queue.html#var-enqueue) **return value has changed**.
   
   This change is relevant to you iff you use the return value of `enqueue` calls (most users do not). Check your `enqueue` call sites to be sure.
   
   The fn previously returned `<mid>` (message id) on success, or `{:carmine.mq/error <message-status>}` on error.
   
   The fn now **always returns a map** with `{:keys [success? mid action error]}`.
   
   See the updated `enqueue` [docstring](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#enqueue) for details.
   
3. [`queue-status`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#queue-status) **return value has changed**.
   
   This change is relevant to you iff you use the `queue-status` util.
   
   The fn previously returned a detailed map of **all queue content** in O(queue-size) and was rarely used.
   
   The fn now returns a small map in O(1) with `{:keys [nwaiting nlocked nbackoff ntotal]}`.
   
   If you want the detailed map of all queue content in O(queue-size),
   use the new [`queue-content`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#queue-content) util.
   
4. **The definition of "queue-size" has changed**.
   
   The old definition: total size of queue.  
   The new definition: total size of queue, LESS mids that may be locked or in backoff.
   
   I.e. the new definition now better represents **the number of messages awaiting processing**.
   
   Most users won't be negatively affected by this change since the new definition better corresponds to how most users actually understood the term before.
   
5. `clear-queues` **has been deprecated**.

   This utility is now called [`queues-clear!!`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#queues-clear!!) to better match the rest of the API.
   
6. **Handling order of queued messages has changed**.

   To improve latency, queue workers now prioritize handling of **newly queued** messages before **re-visiting old messages** for queue maintenance (re-handling, GC, etc.).
   
   Most users shouldn't be negatively affected by this change since Carmine's message queue has never given any specific ordering guarantees (and cannot, for various reasons).
   
7. **Queue connections may be held longer**.
   
   To improve latency, queue sleeping is now **active** rather than **passive**, allowing Redis to immediately signal Carmine's message queue when new messages arrive.
   
   This change means that each worker thread (default 1) may now *hold a connection open* while sleeping. The maximum hold time is determined by [`:throttle-ms`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#worker) (usu. in the order of ~10-400 msecs).
   
   So if you have many queues and/or many queue worker threads, you *may* want to increase the maximum number of connections in your connection pool to ensure that you always have available connections.

## Architectural improvements

- **Significantly improved latency** (esp. worst-case latency) of handling new messages.
  
- **Decouple threads for handling and queue maintenance**. Thread counts can now be individually customized.
  
- Worker end-of-queue backoff sleeps are now interrupted by new messages. So **sleeping workers will automatically awaken when new messages arrive**.

- Prioritize requeues (treat as "ready"). So **requeues no longer need to wait for a queue cycle to be reprocessed**.

- **Smart worker throttling.** The `:throttle-ms` worker option can now be a function of the current queue size, enabling **dynamic worker throttling**.
  
  The default `:throttle-ms` value is now `:auto`, which uses such a function.
  
  See the updated `worker` [docstring](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#worker) for details.

- **Worker threads are now auto desynchronized** to reduce contention.

## API improvements

- Added `enqueue` option: `:lock-ms` to support **per-message lock times** [#223](https://github.com/taoensso/carmine/issues/223).
- Added `enqueue` option: `:can-update?` to support **message updating**.
- Handler fn data now includes `:worker`, `:queue-size` keys.
- Handler fn data now includes `:age-ms` key, enabling **easy integration with Tufte or other profiling tools**.
- Added utils: [`queue-size`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#queue-size), [`queue-names`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#queue-names), [`queue-content`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#queue-content), [`queues-clear!!`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#queues-clear!!), [`queues-clear-all!!!`](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#queues-clear-all!!!).
- `Worker` object's string/pprint representation is now more useful.
- `Worker` object can now be derefed to get **state and stats useful for monitoring**.
  
  In particular, the new `:stats` key contains **detailed statistics on queue size, queueing time, handling time, etc**.

- `Worker` objects can now be invoked as fns to execute common actions.
  
  Actions [include](https://cljdoc.org/d/com.taoensso/carmine/CURRENT/api/taoensso.carmine.message-queue#worker): `:start`, `:stop`, `:queue-size`, `:queue-status`.

- Various improvements to docstrings, error messages, and logging output.
- Improved message queue [state diagram](https://github.com/taoensso/carmine/blob/master/mq-architecture.svg).
- General improvements to implementation, observability, and tests.
