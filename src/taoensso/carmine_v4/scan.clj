(ns ^:no-doc taoensso.carmine-v4.scan
  "Private implementation of the public v4 scan API."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss :as truss]
   [taoensso.carmine-v4.utils :as utils]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp :as resp]))

(enc/declare-remote
  taoensso.carmine-v4/with-car)

(alias 'core 'taoensso.carmine-v4)

(defn ^:no-doc dummy-scan-fn
  "Private, do not use. Returns a simulated scan function for REPL use."
  [num-steps elements-fn]
  (let [c (java.util.concurrent.atomic.AtomicLong. (long num-steps))]
    (fn [cursor]
      (let [idx (.addAndGet c -1)]
        (if (<= idx 0)
          ["0" (elements-fn idx)]
          ["x" (elements-fn idx)])))))

(defn ^:no-doc scan-reduce-elements
  "Private, do not use. Low-level scan reducer for internal use with scans such
  as `scan`, `sscan`, `hscan`, and `zscan`.

  Takes:
    - (fn scan-fn [cursor])       -> [next-cursor page-elements]
    - (fn rf      [acc elements]) -> next accumulator

  Starts with cursor \"0\". Stops after it reduces the page whose next cursor is
  \"0\", or when `rf` returns a reduced value. With `xform`, acts as `transduce`
  and gives it one complete page at a time."
  ([      init scan-fn rf] (scan-reduce-elements nil init scan-fn rf))
  ([xform init scan-fn rf]
   (let [rf (if xform (xform rf) rf)
         rv
         (loop [cursor "0" acc init]
           (let [[next-cursor next-in] (scan-fn cursor)
                 ;; Normalize cursors read under an ambient `:bytes` read mode
                 ;; (e.g. `as-bytes`) so the "0" termination test and cursor
                 ;; reuse still work.
                 next-cursor
                 (if (enc/bytes? next-cursor)
                   (enc/utf8-ba->str next-cursor)
                   (do                next-cursor))]
             (let [result (rf acc next-in)]
               (if (reduced? result)
                 @result
                 (if (= next-cursor "0")
                   result
                   (recur next-cursor result))))))]
     (if xform (rf rv) rv))))

(let [marker (Object.)]
 (defn- scan-dedupe-key [k]
   (if (enc/bytes? k)
     ;; Byte arrays use identity equality, so keep a private immutable snapshot
     ;; whose ByteBuffer provides content equality and hashing.
     [marker (java.nio.ByteBuffer/wrap (aclone ^bytes k))]
     k)))

(defn ^:public scan-reduce
  "Reduces over all the elements of a Redis cursor scan, fetching one page
  at a time:
    (scan-reduce [] ; Collect matching keys
      (fn [cursor] (wcar mgr (scan cursor :match \"user:*\")))
      conj)

    - `(scan-fn cursor)` does one scan call, and returns its
      `[next-cursor elements]` reply.
    - `(rf acc element)` returns the next `acc`, or a `reduced` value to stop.

  Use for `scan`, `sscan`, etc. Use `scan-reduce-kv` for `hscan` and `zscan`,
  which return pairs.

  Redis may return an element more than once. With `{:dedupe? true}` opts,
  `rf` is called at most once per element; this keeps every element seen in
  memory until the scan ends. A scan isn't a snapshot: elements added or
  removed during the scan may or may not be seen.

  See also `scan-keys`."
  ([     init scan-fn rf] (scan-reduce nil init scan-fn rf))
  ([opts init scan-fn rf]
   (let [opts (or opts {})
         _ (utils/have-opts! :carmine.scan/invalid-opts "scan options" #{:dedupe?} opts)
         _ (utils/have-opt!  :carmine.scan/invalid-opts "scan options" :dedupe? "nil or a boolean"
             (utils/nil-or boolean?) (:dedupe? opts))
         dedupe? (boolean (:dedupe? opts))
         seen_ (when dedupe? (volatile! (transient #{})))]
     (scan-reduce-elements nil init scan-fn
       (fn wrapped-rf [acc elements]
         (reduce
           (if dedupe?
             (fn [acc element]
               (let [dedupe-key (scan-dedupe-key element)]
                 (if (contains? @seen_ dedupe-key)
                   acc
                   (do
                     (vswap! seen_ conj! dedupe-key)
                     (enc/convey-reduced (rf acc element))))))
             (fn [acc element]
               (enc/convey-reduced (rf acc element))))
           acc elements))))))

(comment
  (count
    (scan-reduce {:dedupe? true} []
      (fn scan-fn [cursor] (core/with-car mgr #(resp/rcmd :SCAN cursor :MATCH "*")))
      (completing (fn rf [acc in] (conj acc in)))))

  (count
    (scan-reduce {:dedupe? true} []
      (dummy-scan-fn 8 (fn [_] (repeatedly 8 #(rand-int 10))))
      (completing (fn rf [acc in] (conj acc in))))))

(defn ^:public scan-reduce-kv
  "Like `scan-reduce`, but for scans that return pairs, such as `hscan` and
  `zscan`:
    (scan-reduce-kv {} ; Collect hash as map
      (fn [cursor] (wcar mgr (hscan \"my-hash\" cursor)))
      assoc)

  `(rf acc k v)` returns the next `acc`. With `{:dedupe? true}` opts, `rf` is
  called at most once per `k`."
  ([     init scan-fn rf] (scan-reduce-kv nil init scan-fn rf))
  ([opts init scan-fn rf]
   (let [opts (or opts {})
         _ (utils/have-opts! :carmine.scan/invalid-opts "scan options" #{:dedupe?} opts)
         _ (utils/have-opt!  :carmine.scan/invalid-opts "scan options" :dedupe? "nil or a boolean"
             (utils/nil-or boolean?) (:dedupe? opts))
         dedupe? (boolean (:dedupe? opts))
         seen_ (when dedupe? (volatile! (transient #{})))]
     (scan-reduce-elements nil init scan-fn
       (fn wrapped-rf [acc kvs]
         (when (odd? (count kvs))
           (truss/ex-info! "[Carmine] Expected an even number of scan elements"
             {:eid :carmine.scan/odd-key-value-elements
              :elements (enc/typed-val kvs)}))
         (enc/reduce-kvs
           (if dedupe?
             (fn [acc k v]
               (let [dedupe-key (scan-dedupe-key k)]
                 (if (contains? @seen_ dedupe-key)
                   acc
                   (do
                     (vswap! seen_ conj! dedupe-key)
                     (enc/convey-reduced (rf acc k v))))))
             (fn [acc k v]
               (enc/convey-reduced (rf acc k v))))
           acc kvs))))))

(comment
  (core/with-car mgr #(resp/rcmd :HMSET "my-hash" "k1" "v1" "k2" "v2"))
  (scan-reduce-kv {}
    (fn scan-fn [cursor] (core/with-car mgr #(resp/rcmd :HSCAN "my-hash" cursor)))
    (fn rf [acc k v] (assoc acc k v))))

(defn ^:public scan-keys
  "Returns the set of all keys in the current database, or those that match
  the given Redis glob `pattern`:
    (scan-keys mgr \"user:*\") ; => #{\"user:1\" \"user:2\" ...}

  Uses `SCAN`, so is safe to use on large databases. It isn't a snapshot:
  keys added or removed during the scan may or may not be included.

  Options:
    - `:count` - Hint for the number of keys to examine per `SCAN` call.
    - `:type`  - Only return keys of this Redis type, e.g. `:hash`.

  With Cluster, scans one node only: choose it with `with-cluster-target`.

  See also `scan-reduce`."
  ([conn-mgr]
   (scan-keys conn-mgr nil nil))
  ([conn-mgr pattern]
   (scan-keys conn-mgr pattern nil))
  ([conn-mgr pattern opts]
   (let [opts  (or opts {})
         _     (utils/have-opts! :carmine.scan/invalid-opts "`scan-keys` options" #{:count :type} opts)
         count (:count opts)
         type  (:type opts)
         _     (utils/have-opt! :carmine.scan/invalid-opts "`scan-keys` options" :count "nil or a positive integer"
                 (utils/nil-or pos-int?) count)
         _     (utils/have-opt! :carmine.scan/invalid-opts "`scan-keys` options" :type "nil, a keyword, or a string"
                 (utils/nil-or (some-fn keyword? string?)) type)
         ;; Byte arrays need content-based dedupe. No clone needed: they're
         ;; private until return.
         seen-bytes (java.util.HashSet.)]
     (scan-reduce #{}
       (fn [cursor]
         (core/with-car conn-mgr
           (fn []
             ;; Cursor traversal requires the complete page. Preserve ordinary
             ;; decoding modes, including bytes, but not skipped/transformed pages.
             (binding [com/*parser* nil
                       com/*read-mode*
                       (when-not (com/skip-read-mode? com/*read-mode*)
                         com/*read-mode*)]
               (resp/rcmd*
                 (cond-> ["SCAN" cursor]
                   (some? pattern) (into ["MATCH" pattern])
                   (some? count)   (into ["COUNT" count])
                   (some? type)    (into ["TYPE" type])))))))
       (fn [acc key]
         (if (and (enc/bytes? key)
               (not (.add seen-bytes (java.nio.ByteBuffer/wrap ^bytes key))))
           acc
           (conj acc key)))))))
