(ns ^:no-doc taoensso.carmine-v4.utils
  "Private ns, implementation detail."
  (:require
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]))

(comment (remove-ns 'taoensso.carmine-v4.utils))

(let [not-found (Object.)
      empty? (fn [x] (== (count x) 0))
      merge2
      (fn [left right]
        (reduce-kv
          (fn rf [rm lk lv]
            (let [rv (get rm lk not-found)]
              (enc/cond
                (identical? rv not-found) (assoc rm lk lv)
                (map?       rv)
                (if (map? lv)
                  (assoc rm lk (reduce-kv rf rv lv))
                  (do    rm))
                :else rm)))
          right left))]

  (defn merge-opts
    "Like `enc/nested-merge`, but optimised for merging opts.
    Opt vals are used in ascending order of preference:
      `o3` > `o2` > `o1`"
    ([      o1] o1)
    ([   o1 o2] (if (empty? o2) o1 (merge2 o1 o2)))
    ([o1 o2 o3]
     (if (empty? o3)
       (if (empty? o2)
         o1
         (if (empty? o1)
           o2
           (merge2 o1 o2)))

       (if (empty? o2)
         (if (empty? o1)
           o3
           (merge2 o1 o3))

         (if (empty? o1)
           (merge2 o2 o3)
           (merge2 (merge2 o1 o2) o3)))))))

(comment (enc/qb 1e6 (merge-opts {:a 1} {:a 2} {:a 3}))) ; 75.67

(defn dissoc-k [m in-k dissoc-k]
  (if-let [in-v (get m in-k)]
    (if (map? in-v)
      (assoc m in-k (dissoc in-v dissoc-k))
      (do    m))
    (do      m)))

(defn dissoc-ks [m in-k dissoc-ks]
  (if-let [in-v (get m in-k)]
    (if (map? in-v)
      (assoc m in-k (reduce dissoc in-v dissoc-ks))
      (do    m))
    (do      m)))

(defn get-at
  "Optimized `get-in`."
  ([m k1      ] (when m               (get m k1)))
  ([m k1 k2   ] (when m (when-let [m2 (get m k1)]               (get m2 k2))))
  ([m k1 k2 k3] (when m (when-let [m2 (get m k1)] (when-let [m3 (get m2 k2)] (get m3 k3))))))

(defmacro get-first-contained [m & ks]
  (when ks
    `(if (contains?         ~m ~(first ks))
       (get                 ~m ~(first ks))
       (get-first-contained ~m ~@(next ks)))))

(comment (clojure.walk/macroexpand-all '(get-first-contained opts :k1 :k2 :k3)))

;;;;

(defn cb-notify!
  "Notifies callbacks by calling them with @data_."
  ([cb      data_] (when cb (truss/catching (cb (force data_)))))
  ([cb1 cb2 data_]
   (when cb1 (truss/catching (cb1 (force data_))))
   (when cb2 (truss/catching (cb2 (force data_)))))

  ([cb1 cb2 cb3 data_]
   (when cb1 (truss/catching (cb1 (force data_))))
   (when cb2 (truss/catching (cb2 (force data_))))
   (when cb3 (truss/catching (cb3 (force data_))))))

(let [get-data_
      (fn [error cbid]
        (let [data (assoc (ex-data error) :cbid cbid)
              data
              (if-let [cause (or (get data :cause) (ex-cause error))]
                (assoc data :cause cause)
                (do    data))]
          (delay data)))]

  (defn cb-notify-and-throw!
    "Notifies callbacks with error data, then throws error."
    ([cbid cb          error] (cb-notify! cb          (get-data_ error cbid)) (throw error))
    ([cbid cb1 cb2     error] (cb-notify! cb1 cb2     (get-data_ error cbid)) (throw error))
    ([cbid cb1 cb2 cb3 error] (cb-notify! cb1 cb2 cb3 (get-data_ error cbid)) (throw error))))

(comment
  (cb-notify-and-throw! :cbid1 println
    (truss/ex-info "Error msg" {:x :X} (Exception. "Cause"))))
