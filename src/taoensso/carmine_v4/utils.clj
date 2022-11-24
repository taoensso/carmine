(ns taoensso.carmine-v4.utils
  "Private ns, implementation detail."
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]))

(comment
  (remove-ns      'taoensso.carmine-v4.utils)
  (test/run-tests 'taoensso.carmine-v4.utils))

;;;;

(let [merge2
      (fn [left right]
        (reduce-kv
          (fn rf [rm lk lv]
            (if (map? lv)
              (if (contains? rm lk)
                (assoc rm lk (reduce-kv rf (get rm lk) lv))
                (assoc rm lk                           lv))

              (if (contains? rm lk)
                (do    rm)
                (assoc rm lk lv))))

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

(deftest ^:private _merge-opts
  [(is (= (merge-opts {:a 1 :b 1}       {:a      2})  {:a 2,       :b 1}))
   (is (= (merge-opts {:a {:a1 1} :b 1} {:a {:a1 2}}) {:a {:a1 2}, :b 1}))

   (is (= (merge-opts {:a 1} {:a 2} {:a 3}) {:a 3}))

   (is (= (merge-opts {:a 1} {:a 2} {    }) {:a 2}))
   (is (= (merge-opts {:a 1} {    } {:a 3}) {:a 3}))
   (is (= (merge-opts {    } {:a 2} {:a 3}) {:a 3}))])

(comment (enc/qb 1e6 (merge-opts {:a 1} {:a 2} {:a 3}))) ; 148.25

;;;;

(defmacro safely
  "Silently swallows anything thrown by body."
  [& body] `(try (do ~@body) (catch Throwable ~'_)))

(defn cb-notify!
  "Notifies callbacks by calling them with @data_."
  ([cb      data_] (when cb (safely (cb @data_))))
  ([cb1 cb2 data_]
   (when cb1 (safely (cb1 @data_)))
   (when cb2 (safely (cb2 @data_)))))

(defn cb-notify-and-throw!
  "Notifies callbacks with error data, then throws error."
  ([cbid cb error]
   (when cb
     (let [data (assoc (ex-data error) :cbid cbid)
           data
           (if-let [cause (or (get data :cause) (ex-cause error))]
             (assoc data :cause cause)
             (do    data))]
       (safely (cb data))))

   (throw error))

  ([cbid cb1 cb2 error]
   (when (or cb1 cb2)
     (let [data (assoc (ex-data error) :cbid cbid)
           data
           (if-let [cause (or (get data :cause) (ex-cause error))]
             (assoc data :cause cause)
             (do    data))]

       (when cb1 (safely (cb1 data)))
       (when cb2 (safely (cb2 data)))))

   (throw error)))

(comment
  (cb-and-throw! println :cbid1
    (ex-info "Error msg" {:x :X} (Exception. "Cause"))))

;;;;

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

(deftest ^:private _dissoc-utils
  [(is (= (dissoc-k  {:a {:b :B :c :C :d :D}} :a  :b)     {:a {:c :C, :d :D}}))
   (is (= (dissoc-ks {:a {:b :B :c :C :d :D}} :a [:b :d]) {:a {:c :C}}))])

(defn get-at "Optimized `get-in`"
  ([m k1      ] (when m               (get m k1)))
  ([m k1 k2   ] (when m (when-let [m2 (get m k1)]               (get m2 k2))))
  ([m k1 k2 k3] (when m (when-let [m2 (get m k1)] (when-let [m3 (get m2 k2)] (get m3 k3))))))

(defmacro get-first-contained [m & ks]
  (when ks
    `(if (contains?         ~m ~(first ks))
       (get                 ~m ~(first ks))
       (get-first-contained ~m ~@(next ks)))))

(deftest ^:private _get-first-contained
  [(is (= (let [m {:a :A    :b :B}] (get-first-contained m :q :r :a :b)) :A))
   (is (= (let [m {:a false :b :B}] (get-first-contained m :q :r :a :b)) false))])

(comment
  (clojure.walk/macroexpand-all
    '(get-first-contained {:a :A :b :B}
       :q :r)))
