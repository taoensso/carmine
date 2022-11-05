(ns taoensso.carmine.impl.resp.read.parsing
  "Reply parsing stuff.
  We choose to keep parsing capabilities relatively simple:
  no nesting, no auto composition, and no concurrent fn+rf parsers.

  Note that *read-mode* and *parser* are distinct, and may interact."
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.test    :as test :refer [deftest testing is]]
   [taoensso.encore :as enc  :refer [throws?]]
   [taoensso.carmine.impl.resp.common :as resp-com
    :refer [throw!]]))

(comment
  (remove-ns      'taoensso.carmine.impl.resp.read.parsing)
  (test/run-tests 'taoensso.carmine.impl.resp.read.parsing))

;;;;

(def ^:private ^:dynamic *parser* "?<Parser>" nil)

;; Parser opts are an advanced/undocumented feature for internal use
;; rfc is (fn rf-constructor [])-><possibly-stateful-rf*>
(deftype Parser [kind opts f rfc kv-rf?])

(defn         parser? [x]            (instance? Parser x))
(defn     when-parser [x] (when      (instance? Parser x) x))
(defn  when-fn-parser [x] (when (and (instance? Parser x) (.-f    ^Parser x)) x))
(defn  when-rf-parser [x] (when (and (instance? Parser x) (.-rfc  ^Parser x)) x))
(defn get-parser-opts [x] (when      (instance? Parser x) (.-opts ^Parser x)))

(defn describe-parser
  "For error messages, etc."
  [parser]
  (when-let [p ^Parser parser]
    {:opts   (.-opts   p)
     :kind   (.-kind   p)
     :kv-rf? (.-kv-rf? p)}))

(comment
  [(describe-parser (fn-parser {:o :O}     (fn [])))
   (describe-parser (rf-parser {:o :O} nil (fn [])))])

(defn- parser-error
  [cause data]
  (resp-com/reply-error
    (ex-info "[Carmine] Reply parser threw an error"
      (enc/assoc-nx data :eid :carmine.resp.read/parser-error)
      cause)))

(defn- safe-parser-fn [parser-opts f]
  (fn  safe-parser-fn [x]
    (try
      (f x)
      (catch Throwable t
        (parser-error  t
          {:kind        :fn
           :parser-opts parser-opts
           :arg         {:value x :type (type x)}})))))

(defn fn-parser ^Parser [parser-opts f]
  (let [parser-opts (not-empty parser-opts)
        f
        (if (get parser-opts :catch? true)
          (safe-parser-fn f parser-opts)
          (do             f))]
    (Parser. :fn parser-opts f nil nil)))

(defn- safe-parser-xrf
  "Returns a stateful transducer to catch any thrown errors in rf. All
  future calls to rf will noop and return that same error. Protects
  reductions from interruption due to parser errors."
  ([        error-data] (safe-parser-xrf (volatile! nil) error-data))
  ([caught_ error-data]
   (fn [rf]
     (enc/catching-rf
       (fn error-fn [extra-data cause] (vreset! caught_ (parser-error cause (conj error-data extra-data))))
       (fn
         ([]        (or @caught_ (rf)))
         ([acc]     (or @caught_ (rf acc)))
         ([acc in]  (or @caught_ (rf acc in)))
         ([acc k v] (or @caught_ (rf acc k v))))))))

(defn rf-parser
  "rf should a reducing function such that:
    (rf)        => Init     acc
    (rf acc in) => Next     acc (accumulation step)
    (rf acc)    => Complete acc"
  ^Parser [parser-opts ?xform rf]
  (let [parser-opts (not-empty parser-opts)
        kv-rf? (if ?xform false (get parser-opts :kv-rf? true))

        error-data
        (fn [thrown-by]
          {:parser-opts parser-opts
           :xform       ?xform
           :rf          rf
           :thrown-by   thrown-by})

        ?xform
        (if (get parser-opts :catch? true)

          ;; Catch errors
          (let [caught_ (volatile! nil)]
            (if-let [xform ?xform]
              ;; Currently do double wrapping to distinguish
              ;; between :rf and :xform errors
              (comp
                (safe-parser-xrf caught_ (error-data :xform))
                xform
                (safe-parser-xrf caught_ (error-data :rf)))

              (safe-parser-xrf caught_ (error-data :rf))))

          ;; Don't catch errors
          ?xform)

        rf-constructor
        (if-let [xform ?xform]
          (fn rfc [] (xform rf)) ; Possibly stateful
          (fn rfc []        rf))]

    (Parser. :rf parser-opts nil
      rf-constructor kv-rf?)))

(comment (enc/qb 1e6 (rf-parser {} nil (fn [])))) ; 72.61

(defn- test-rf-parser [kvs? ?xform rf init coll]
  (let [rf* ((.-rfc (rf-parser {} ?xform rf)))]
    (identity ; As (rf* completing [acc] acc)
      (if kvs?
        (reduce-kv rf* init coll)
        (reduce    rf* init coll)))))

(deftest ^:private _rf-parser
  [(testing "Basics"
     [(is (=   (test-rf-parser false nil (fn [acc in] (conj acc in)) [] [:a :b]) [:a :b]))
      (is (->> (test-rf-parser false nil (fn [acc in] (throw!   in)) [] [:a :b])
            (resp-com/reply-error? {:thrown-by :rf})) "Identifies rf error")

      (is (=   (test-rf-parser false (map identity) (fn [acc in] (conj acc in)) [] [:a :b]) [:a :b]))
      (is (->> (test-rf-parser false (map throw!)   (fn [acc in] (conj acc in)) [] [:a :b])
            (resp-com/reply-error? {:thrown-by :xform})) "Identifies xform error")

      (is (=   (test-rf-parser true nil (fn [acc k v] (assoc acc k v))  {} {:a :A}) {:a :A}))
      (is (->> (test-rf-parser true nil (fn [acc k v] (throw!   [k v])) {} {:a :A})
            (resp-com/reply-error? {:thrown-by :rf}))
        "kv-rf supported when no user-supplied xform")])

   (testing "Stateful short-circuiting"
     (let [xform (map        (fn [    in] (if (neg? in) (throw! in) in)))
           rf    (completing (fn [acc in] (if (odd? in) (throw! in) in)))]

       [(testing "Permanently short-circuit on rf error"
          (let [rf* ((.-rfc (rf-parser {} xform rf)))]
            [(is (=   (rf* :acc   ) :acc))
             (is (=   (rf* :acc  2) 2))
             (is (->> (rf* :acc  3) (resp-com/reply-error? {:thrown-by :rf :args {:in {:value 3}}})))
             (is (->> (rf* :acc  2) (resp-com/reply-error? {:thrown-by :rf :args {:in {:value 3}}})))
             (is (->> (rf* :acc -2) (resp-com/reply-error? {:thrown-by :rf :args {:in {:value 3}}})))]))

        (testing "Permanently short-circuit on xform error"
          (let [rf* ((.-rfc (rf-parser {} xform rf)))]
            [(is (=   (rf* :acc   ) :acc))
             (is (=   (rf* :acc  2) 2))
             (is (->> (rf* :acc -2) (resp-com/reply-error? {:thrown-by :xform :args {:in {:value -2}}})))
             (is (->> (rf* :acc  2) (resp-com/reply-error? {:thrown-by :xform :args {:in {:value -2}}})))
             (is (->> (rf* :acc  3) (resp-com/reply-error? {:thrown-by :xform :args {:in {:value -2}}})))]))]))])
