(ns taoensso.carmine.cluster
  "EXPERIMENTAL support for Redis Cluster atop Carmine."
  {:author "Ben Poweski"}
  (:require [clojure.string   :as str]
            [taoensso.carmine :as car]
            [taoensso.carmine
             (utils       :as utils)
             (protocol    :as protocol)
             (connections :as conns)
             (commands    :as commands)]))

;; TODO Migrate to new design

;;; Description of new design:
;; The new design should be significantly more flexible + performant for use
;; with Cluster:
;; * `protocol/*context*` now contains a request queue (atom []).
;; * Redis commands previously wrote directly to io buffer, they now push
;;   'requests' to this queue instead.
;; * A 'request' looks like ["GET" "my-key" "my-val"] and has optional
;;   metadata which includes `:expected-keyslot` - hashed cluster key (crc16).
;;
;; * Request pushing + metadata is all handled by `commands/enqueue-request`.
;;
;; * Before actually fetching server replies, all queued requests are written to
;;   io buffer with `protocol/execute-requests`.
;;
;; * This fn works with dynamic arguments (default), or with an explicit
;;   Connection and requests. It is fast + flexible (gives us a lot of room to
;;   make Cluster-specific adjustments).

;;; Sketch of suggested implementation:
;; * `:cluster` should be provided as part of connection :spec options.
;; * The `protocol/execute-requests` fn could be modded so that when the
;;   :cluster key is present, behaviour is delegated to
;;   `cluster/execute-requests`.
;; * This fn groups requests by the (cached) keyslot->server info:
;;   (let [plan (group-by <grouping-fn> requests)]
;;    <...>).
;; * Futures can then call `protocol/execute-requests` with the explicit
;;   expected connections + planned requests & deliver replies to promises.
;; * All Cluster replies could be inspected for Cluster errors like MOVE, etc.
;;   This is easier than before since exceptions now have
;;   `(ex-data <ex>)` with a :prefix key that'll be :moved, :ack, :wrongtype, etc.
;; * After all promises have returned, we could regroup for any moved keys and
;;   loop, continuing to accumulate appropriate replies.
;; * We eventually return all the replies in the same order they were provided.
;;   Parsers & other goodies will just work as expected since all that info is
;;   attached to the requests themselves.

(comment ; Example

  ;; Step 1:
  (wcar {:cluster "foo"}
    (car/get "key-a")
    (car/get "key-b")
    (car/get "key-c"))

  ;; Step 2:
  ;; protocol/execute-requests will receive requests as:
  ;; [["GET" "key-a"] ["GET" "key-b"] ["GET" "key-c"]]
  ;; Each will have :expected-keyslot metadata.
  ;; None have :parser metadata in this case, but it wouldn't make a difference
  ;; to our handling here.

  ;; Step 3:
  ;; Does our cache know which servers serve each of the above slots?[1]
  ;; Group requests per server + `execute-requests` in parallel with
  ;; appropriate Connections specified (will override the dynamic args).

  ;; Step 4:
  ;; Wait for all promises to be fulfilled (a timeout may be sensible).

  ;; Step 5:
  ;; Identify (with `(:prefix (ex-data <ex>))`) which replies are Cluster/timeout
  ;; errors that imply we should try again.

  ;; Step 6:
  ;; Continue looping like this until we've got all expected replies or we're
  ;; giving up for certain replies.

  ;; Step 7:
  ;; Return all replies in order as a single vector (i.e. the consumer won't be
  ;; aware which nodes served which replies).

  ;; [1]
  ;; Since slots are distributed to servers in _ranges_, we can do this quite
  ;; efficiently.
  ;; Let's say we request a key at slot 42 and determine that it's at
  ;; 127.0.0.1:6379 so we cache {42 <server 127.0.0.1:6379>}.
  ;;
  ;; Now we want a key at slot 78 but we have no idea where it is. We can scan
  ;; our cache and find the nearest known slot to 78 and use that for our first
  ;; attempt. So with n nodes, we'll have at most n-1 expected-slot misses.
  )
