(ns ^:no-doc taoensso.carmine-v4.scan
  "Private implementation of the public v4 scan API."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss :as truss]
   [taoensso.carmine-v4.resp :as resp]))

(enc/declare-remote
  taoensso.carmine-v4/with-car)

(alias 'core 'taoensso.carmine-v4)

(defn ^:no-doc dummy-scan-fn
  "Private, do not use. Returns a simulated scan function for internal tests."
  [num-steps elements-fn]
  (let [c (java.util.concurrent.atomic.AtomicLong. (long num-steps))]
    (fn [cursor]
      (let [idx (.addAndGet c -1)]
        (if (<= idx 0)
          ["0" (elements-fn idx)]
          ["x" (elements-fn idx)])))))

(defn ^:no-doc scan-reduce-elements
  "Private, do not use. Low-level scan reducer for internal use with scans such
  as [[scan]], [[sscan]], [[hscan]], and [[zscan]].

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
  "Reduces individual elements returned across a Redis cursor scan.

  Takes:
    - (fn scan-fn [cursor])      -> [next-cursor page-elements]
    - (fn rf      [acc element]) -> next accumulator

  `scan-fn` is first called with cursor \"0\" and should typically call
  [[scan]] or [[sscan]], which return pages of elements. Use [[scan-reduce-kv]]
  for [[hscan]] and other scans that return alternating pairs. Scanning stops
  after it reduces the page whose next cursor is \"0\", or when `rf` returns a
  reduced value.

  When given `xform`, acts as `transduce`. Redis may return an element more than
  once during a scan. By default, this function keeps these duplicates. Pass
  `{:dedupe? true}` to call `rf` at most once for each element. This retains
  seen elements for the complete reduction. Unlike the `distinct` transducer,
  it compares byte arrays by content.

  Example:
    (scan-reduce {:dedupe? true} []
      (fn [cursor] (wcar mgr (rcmd :SCAN cursor :MATCH \"user:*\")))
      conj)"
  ([init scan-fn rf]
   (scan-reduce nil nil init scan-fn rf))
  ([xform-or-opts init scan-fn rf]
   (if (map? xform-or-opts)
     (scan-reduce xform-or-opts nil init scan-fn rf)
     (scan-reduce nil xform-or-opts init scan-fn rf)))
  ([opts xform init scan-fn rf]
   (let [opts (or opts {})
         _ (truss/have map? opts)
         _ (truss/have [:ks<= #{:dedupe?}] opts)
         _ (when (contains? opts :dedupe?)
             (truss/have boolean? (:dedupe? opts)))
         dedupe? (boolean (:dedupe? opts))
         seen_ (when dedupe? (volatile! (transient #{})))
         rf (if xform (xform rf) rf)
         rv
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
               acc elements)))]
     (if xform (rf rv) rv))))

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
  "Reduces key/value pairs returned across a Redis cursor scan.

  Takes:
    - (fn scan-fn [cursor])  -> [next-cursor flat-key-value-elements]
    - (fn rf      [acc k v]) -> next accumulator

  Use for calls such as [[hscan]], whose page elements alternate between keys
  and values. Starts with cursor \"0\". Stops after it reduces the page whose next
  cursor is \"0\", or when `rf` returns a reduced value. Redis may return a key
  more than once during a scan. By default, this function keeps these
  duplicates. Pass `{:dedupe? true}` to call `rf` at most once for each key.
  This retains seen keys for the complete reduction.

  Example:
    (scan-reduce-kv {}
      (fn [cursor] (wcar mgr (rcmd :HSCAN \"user:1\" cursor)))
      assoc)"
  ([     init scan-fn rf] (scan-reduce-kv nil init scan-fn rf))
  ([opts init scan-fn rf]
   (let [opts (or opts {})
         _ (truss/have map? opts)
         _ (truss/have [:ks<= #{:dedupe?}] opts)
         _ (when (contains? opts :dedupe?)
             (truss/have boolean? (:dedupe? opts)))
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
  "Returns the set of keys found by a complete Redis `SCAN`.

  `pattern` is an optional Redis glob for `MATCH`. `opts` accepts `:count`, a
  positive SCAN work hint, and `:type`, a string or keyword for Redis 6+ SCAN
  TYPE. The result is a set with duplicate keys removed; byte-array keys are
  compared by content.

  Each page uses `conn-mgr` through [[wcar]]. With a Cluster manager, bind an
  explicit single-node target. This function does not combine scans across
  Cluster masters.

  Example:
    (scan-keys mgr \"user:*\" {:count 1000, :type :hash})"
  ([conn-mgr]
   (scan-keys conn-mgr nil nil))
  ([conn-mgr pattern]
   (scan-keys conn-mgr pattern nil))
  ([conn-mgr pattern opts]
   (let [opts  (or opts {})
         _     (truss/have map? opts)
         _     (truss/have [:ks<= #{:count :type}] opts)
         count (:count opts)
         type  (:type opts)
         _     (truss/have [:or nil? pos-int?] count)
         _     (truss/have [:or nil? keyword? string?] type)]
     (scan-reduce {:dedupe? true} #{}
       (fn [cursor]
         (core/with-car conn-mgr
           (fn []
             (resp/rcmd*
               (cond-> ["SCAN" cursor]
                 (some? pattern) (into ["MATCH" pattern])
                 (some? count)   (into ["COUNT" count])
                 (some? type)    (into ["TYPE" type]))))))
       conj))))
