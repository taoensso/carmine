(ns ^:no-doc taoensso.carmine-v4.cluster
  "Private ns, implementation detail.
  Implementation of the Redis Cluster protocol,
  Ref. <https://redis.io/docs/reference/cluster-spec/>"
  (:require
   [taoensso.encore :as enc :refer [have have?]]
   ;;[taoensso.carmine-v4.utils :as utils]
   ;;[taoensso.carmine-v4.conns :as conns]
   [taoensso.carmine-v4.resp.common :as com]
   ;;[taoensso.carmine-v4.resp  :as resp]
   ;;[taoensso.carmine-v4.opts  :as opts]
   )

  #_
  (:import [java.util.concurrent.atomic AtomicLong]))

(comment (remove-ns 'taoensso.carmine-v4.cluster))

;;;; 1st sketch

;; Update: might now be best with some sort of
;; dedicated cluster ConnManager that can delegate
;; to shard pools, etc.

;; Without cluster:
;; - with-car [conn-opts]
;;   - get-conn [conn-opts], with-conn
;;     - With non-cluster ctx [in out]
;;       - Flush any pending reqs to allow nesting
;;       - New reqs -> pending reqs
;;       - Flush
;;         - Write reqs
;;         - Read replies

;; With cluster:
;; - with-car [conn-opts]
;;   - With cluster ctx [conn-opts]
;;     - Flush any pending reqs to allow nesting
;;     - New reqs -> pending reqs
;;     - Flush
;;       - get-conn [conn-opts], with-conn for each shard
;;       - Write reqs
;;       - Read replies

;; [ ]
;; conn-opts to incl {:keys [cluster-spec cluster-opts]} :server
;; - => Can use Sentinel or Cluster, not both
;; - cluster-spec constructor will take initial set of shard addrs
;; - cluster-opts to contain :conn-opts to use when updating state, etc.
;; - Ensure Sentinel conn-opts doesn't include Sentinel or Cluster server x
;; - Ensure Cluster  conn-opts doesn't include Sentinel or Cluster server
;; - Ensure :select-db is nil/0 when using Cluster

;; Slots:
;; - Slot range is divided between different shards (shard-addrs)
;; - Each Req will have optional slot field
;; - Slots can be determined automatically for auto-generated commands:
;;   - First arg after command name seems to usu. indicate "key".
;;   - If there's any additional keys, their slots would anyway need to agree
;; - `rcmd` and co. expect `cluster-key` to be called manually on the appropriate arg

;; [ ]
;; Stateful ClusterSpec:
;; - shards-state_: sorted map {[<slot-lo> <slot-hi>] {:master <addr> :replicas #{<addr>s}}}
;; - "Stable" when no ongoing reconfig (how to detect?)
;; - ^:private slot->shard-addr [spec parsed-cluster-opts slot]
;;   - Check :prefer-read-replica? in cluster-opts
;;   - Returns (or ?random-replica master)
;;   - Some slots may not have shard-addr, even after updating state
;;
;; - ^:public update-shards! [spec parsed-cluster-opts async?]
;;   - Use locking and/or delay with timeout (fire future on CAS state->updating)
;;   - Use :conn-opts in cluster-opts
;;   - Use `SENTINEL SHARDS` or `SENTINEL SLOTS` command (support both)
;;
;;   - Stats incl.: n-shards, n-reshards, n-moved, n-ask etc.
;;   - Cbs   incl.: on-changed-shards, on-key-moved, etc.

;; [ ]
;; Cluster specific flush [conn-opts] implementation:
;; - [1] First partition reqs into n target shards
;;   - [1b] Check `cluster-slot` and `supports-cluster?` (must be true or nil)
;; - [2] Acquire conns to n target shards (use wcar conn-opts with injected shard-addr)
;;   - [2b] If :prefer-read-replica? in cluster-opts -
;;          Call READONLY/READWRITE (skipping replies)
;; - [*] Mention that we _could_ use fp to write & read to each shard simultaneously
;; - [3] Write to all shards
;; - [4] Read replies from all shards
;; - [5] If there's any -MOVED, -ASK, or conn (?) errors:
;;   - Ask ClusterSpec to update-shards! (async?)
;;   - Retry these reqs
;; - [6] Carefully stitch back replies in correct order
;; - [7] Ensure that nesting works as expected

;; Details on partitioning scheme (could be pure, data-oriented fn):
;; - Loop through all reqs in order
;; - If req -> slot -> shard-addr, add [req req-idx] to partition for that shard-addr
;; - If req -> nil, add [req req-idx] to last non-nil partition,
;;                  or buffer until first non-nil partition.
;; - If never any non-nil partition: choose random shard-addr.

;; Conn pooling:
;; - Pool to operate solely on [host port] servers, injected by slot->addr in flush.
;; - I.e. pool needs no invalidation or kop-key changes.

;;;; Misc

;; - Would use Cluster or Sentinel, not both.
;;   - Sentinel provides best availability, and some read  perf via replicas.
;;   - Cluster  provides some availability, and read+write perf via sharding.

;; - Cluster supports all 1-key commands, and >1 key commands iff all keys
;;   in same hash slot.
;; - Select command not allowed.
;; - Optional hash tags allow manual control of hash slots.

;; - Cluster "stable" when no ongoing reconfig (i.e. hash slots being moved)
;; - Each node has unique node-id
;; - Nodes can change host without changing node-id (problematic?)

;; - Cluster has internal concept of {<slot> <node-id>}
;; - Client should store state like
;;   {[<slot-lo> <slot-hi>] {:master <addr> :replicas #{<addr>s}}}, more
;;
;;   - To get cluster topology:
;;     - CLUSTER SHARDS (Redis >= v7),
;;     - CLUSTER SLOTS  (Redis <= v6), deprecated
;;     - Client cannot assume that all slots will be accounted for,
;;       may need to re-fetch topology or try a random node
;;
;;   - Update topology when:
;;     - Initially empty
;;     - Any command saw a -MOVED error (use locking?)

;; - Possible Cluster errors:
;;   - -MOVED => permanently moved
;;     -MOVED 3999 127.0.0.1:6381 ; (3999 = key slot) => try host:port
;;     -MOVED 3999 :6380 ; => unknown endpoint, try <same-host>:port
;;
;;     - On redirection error:
;;       - Either update cache for specific slot, or whole topology
;;       - Prefer whole topology (since one move usu. => more)
;;
;;   - ASK =>
;;       - Send this query (ONCE) to specified endpoint, don't update cache
;;       - Start redirected query with ASKING
;;
;;   - TRYAGAIN => reshard in progress, wait to retry or throw

;; - Possible READONLY / READWRITE commands during :init?
;;   (Nb should affect kop-key)

;; - Redis v7+ "Shared pub/sub" implications?

;;;; Key slots

(def ^:private ^:const num-key-slots 16384)
(let [xmodem-crc16-lookup
      (long-array
        [0x0000,0x1021,0x2042,0x3063,0x4084,0x50a5,0x60c6,0x70e7,
         0x8108,0x9129,0xa14a,0xb16b,0xc18c,0xd1ad,0xe1ce,0xf1ef,
         0x1231,0x0210,0x3273,0x2252,0x52b5,0x4294,0x72f7,0x62d6,
         0x9339,0x8318,0xb37b,0xa35a,0xd3bd,0xc39c,0xf3ff,0xe3de,
         0x2462,0x3443,0x0420,0x1401,0x64e6,0x74c7,0x44a4,0x5485,
         0xa56a,0xb54b,0x8528,0x9509,0xe5ee,0xf5cf,0xc5ac,0xd58d,
         0x3653,0x2672,0x1611,0x0630,0x76d7,0x66f6,0x5695,0x46b4,
         0xb75b,0xa77a,0x9719,0x8738,0xf7df,0xe7fe,0xd79d,0xc7bc,
         0x48c4,0x58e5,0x6886,0x78a7,0x0840,0x1861,0x2802,0x3823,
         0xc9cc,0xd9ed,0xe98e,0xf9af,0x8948,0x9969,0xa90a,0xb92b,
         0x5af5,0x4ad4,0x7ab7,0x6a96,0x1a71,0x0a50,0x3a33,0x2a12,
         0xdbfd,0xcbdc,0xfbbf,0xeb9e,0x9b79,0x8b58,0xbb3b,0xab1a,
         0x6ca6,0x7c87,0x4ce4,0x5cc5,0x2c22,0x3c03,0x0c60,0x1c41,
         0xedae,0xfd8f,0xcdec,0xddcd,0xad2a,0xbd0b,0x8d68,0x9d49,
         0x7e97,0x6eb6,0x5ed5,0x4ef4,0x3e13,0x2e32,0x1e51,0x0e70,
         0xff9f,0xefbe,0xdfdd,0xcffc,0xbf1b,0xaf3a,0x9f59,0x8f78,
         0x9188,0x81a9,0xb1ca,0xa1eb,0xd10c,0xc12d,0xf14e,0xe16f,
         0x1080,0x00a1,0x30c2,0x20e3,0x5004,0x4025,0x7046,0x6067,
         0x83b9,0x9398,0xa3fb,0xb3da,0xc33d,0xd31c,0xe37f,0xf35e,
         0x02b1,0x1290,0x22f3,0x32d2,0x4235,0x5214,0x6277,0x7256,
         0xb5ea,0xa5cb,0x95a8,0x8589,0xf56e,0xe54f,0xd52c,0xc50d,
         0x34e2,0x24c3,0x14a0,0x0481,0x7466,0x6447,0x5424,0x4405,
         0xa7db,0xb7fa,0x8799,0x97b8,0xe75f,0xf77e,0xc71d,0xd73c,
         0x26d3,0x36f2,0x0691,0x16b0,0x6657,0x7676,0x4615,0x5634,
         0xd94c,0xc96d,0xf90e,0xe92f,0x99c8,0x89e9,0xb98a,0xa9ab,
         0x5844,0x4865,0x7806,0x6827,0x18c0,0x08e1,0x3882,0x28a3,
         0xcb7d,0xdb5c,0xeb3f,0xfb1e,0x8bf9,0x9bd8,0xabbb,0xbb9a,
         0x4a75,0x5a54,0x6a37,0x7a16,0x0af1,0x1ad0,0x2ab3,0x3a92,
         0xfd2e,0xed0f,0xdd6c,0xcd4d,0xbdaa,0xad8b,0x9de8,0x8dc9,
         0x7c26,0x6c07,0x5c64,0x4c45,0x3ca2,0x2c83,0x1ce0,0x0cc1,
         0xef1f,0xff3e,0xcf5d,0xdf7c,0xaf9b,0xbfba,0x8fd9,0x9ff8,
         0x6e17,0x7e36,0x4e55,0x5e74,0x2e93,0x3eb2,0x0ed1,0x1ef0])]

  (defn- crc16
    "Returns hash for given bytes using the Redis Cluster CRC16 algorithm,
    Ref. <https://redis.io/docs/reference/cluster-spec/> (Appendix A).

    Thanks to @bpoweski for this implementation."
    [^bytes ba]
    (let [len (alength ba)]
      (loop [n   0
             crc 0] ; Inlines faster than `enc/reduce-n`
        (if (>= n len)
          crc
          (recur (unchecked-inc n)
            (bit-xor (bit-and (bit-shift-left crc 8) 0xffff)
              (aget xmodem-crc16-lookup
                (-> (bit-shift-right crc 8)
                  (bit-xor (aget ba n))
                  (bit-and 0xff))))))))))

(defn- ba->key-slot [^bytes ba] (mod (crc16 ba) num-key-slots))
(defn- tag-str->key-slot [^String tag-str] (ba->key-slot (enc/str->utf8-ba tag-str)))

(defprotocol IClusterKey (^:public cluster-key [redis-key] "TODO: Docstring"))
(deftype      ClusterKey [^bytes ba ^long slot]
  clojure.lang.IDeref (deref       [this] slot) ; For tests
  IClusterKey         (cluster-key [this] this))

(extend-type (Class/forName "[B")
  IClusterKey (cluster-key [ba] (ClusterKey. ba (ba->key-slot ba))))

(extend-type String
  IClusterKey
  (cluster-key [s]
    (let [s-ba (enc/str->utf8-ba s)]
      (if-let [tag-str
               (when (enc/str-contains? s "{")
                 (when-let [match (re-find #"\{(.*?)\}" s)]
                   (when-let [^String tag (get match 1)] ; "bar" in "foo{bar}{baz}"
                     (when-not (.isEmpty tag) tag))))]

        (ClusterKey. s-ba (tag-str->key-slot tag-str))
        (ClusterKey. s-ba (ba->key-slot      s-ba))))))

(defn cluster-slot [x] (when (instance? ClusterKey x) (.-slot ^ClusterKey x)))

(comment
  (enc/qb 1e5 ; [7.59 22.92]
    (cluster-key        "foo")
    (cluster-key "ignore{foo}")))

;;;;

(comment
  (def sm
    (sorted-map
      [12 30] "a"
      [16 18] "b"))

  (defn find-entry [sm ^long n]
    (reduce-kv
      (fn [acc lohi v]
        (if (and
              (>= n ^long (get lohi 0))
              (<= n ^long (get lohi 1)))
          (reduced v)
          nil))
      nil
      sm))

  (comment (find-entry sm 16)))
