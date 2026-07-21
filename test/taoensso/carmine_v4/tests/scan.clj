(ns taoensso.carmine-v4.tests.scan
  "Carmine v4 scan reduction and materialization tests."
  (:require
   [clojure.string      :as str]
   [clojure.test        :as test :refer [deftest testing is]]
   [taoensso.encore     :as enc]
   [taoensso.truss      :as truss :refer [throws?]]
   [taoensso.carmine    :as v3-core]
   [taoensso.carmine-v4          :as car  :refer [wcar with-replies]]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp.write  :as write]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.utils    :as utils]
   [taoensso.carmine-v4.opts     :as opts]
   [taoensso.carmine-v4.conns    :as conns]
   [taoensso.carmine-v4.sentinel :as sentinel]
   [taoensso.carmine-v4.cluster  :as cluster]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tk  "Test key" support/test-key)
(def tc  "Unparsed test conn-opts" support/test-conn-opts)
(def tc+ "Parsed test conn-opts" support/parsed-test-conn-opts)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

(defn- redis-major-version [] (support/redis-major-version))

;;;; Scans

(defn- scan-pages-fn [calls_ pages]
  (let [pages_ (atom pages)]
    (fn [cursor]
      (swap! calls_ conj cursor)
      (let [page (first @pages_)]
        (swap! pages_ next)
        page))))

(deftest _scan-reduce
  (testing "Elements and cursors"
    (let [calls_ (atom [])
          result
          (car/scan-reduce []
            (scan-pages-fn calls_
              [["next-1" [1 2]]
               ["next-2" [3]]
               ["0"      [4 5]]])
            conj)]
      [(is (= result [1 2 3 4 5]))
       (is (= @calls_ ["0" "next-1" "next-2"]))]))

  (testing "Early reduction"
    (doseq [[expected pages]
            [[[1 2] [["next" [1 2 3]] ["0" [4]]]]
             [[1 2] [["0"    [1 2 3]]]]]]
      (let [calls_ (atom [])
            result
            (car/scan-reduce [] (scan-pages-fn calls_ pages)
              (fn [acc x]
                (let [acc (conj acc x)]
                  (if (= x 2) (reduced acc) acc))))]
        [(is (= result expected)
           "Reduced values are unwrapped on intermediate and final pages")
         (is (= (count @calls_) 1)
           "No extra page is requested after early reduction")])))

  (testing "Transduction"
    (let [completions_ (atom 0)
          track-completion
          (fn [rf]
            (fn
              ([] (rf))
              ([acc] (swap! completions_ inc) (rf acc))
              ([acc x] (rf acc x))))
          result
          (car/scan-reduce
            (comp (map inc) (filter odd?) (distinct) (take 3) track-completion)
            []
            (scan-pages-fn (atom [])
              [["next" [0 1 2 2]]
               ["0"    [4 6 8]]])
            conj)]
      [(is (= result [1 3 5]))
       (is (= @completions_ 1)
         "Transducer completion runs exactly once after early reduction")]))

  (testing "Opt-in de-duplication"
    (let [pages [["next" [(enc/str->utf8-ba "a")]]
                 ["0"    [(enc/str->utf8-ba "a")
                           (enc/str->utf8-ba "b")]]]
          raw-result
          (car/scan-reduce []
            (scan-pages-fn (atom []) pages)
            (fn [acc x] (conj acc (enc/utf8-ba->str x))))
          result
          (car/scan-reduce {:dedupe? true} []
            (scan-pages-fn (atom []) pages)
            (fn [acc x] (conj acc (enc/utf8-ba->str x))))]
      [(is (= raw-result ["a" "a" "b"]))
       (is (= result ["a" "b"])
         "Byte-array elements are de-duplicated by content")]))

  (testing "Options compose with transduction and validate strictly"
    [(is (= (car/scan-reduce {:dedupe? true} (map inc) []
              (scan-pages-fn (atom []) [["0" [1 1 2]]])
              conj)
            [2 3]))
     (is (throws? :ex-info
           (car/scan-reduce {:dedupe :yes} []
             (scan-pages-fn (atom []) [["0" []]]) conj)))
     (is (throws? :ex-info
           (car/scan-reduce {:dedupe? :yes} []
             (scan-pages-fn (atom []) [["0" []]]) conj)))]))

(deftest _scan-reduce-kv
  (testing "Raw duplicate semantics by default"
    (let [calls_ (atom [])
          result
          (car/scan-reduce-kv {}
            (scan-pages-fn calls_
              [["next" ["a" 1 "b" 2]]
               ["0"    ["a" 9 "c" 3]]])
            assoc)]
      [(is (= result {"a" 9, "b" 2, "c" 3})
         "Repeated keys reach the reducer in Redis reply order")
       (is (= @calls_ ["0" "next"]))]))

  (testing "Opt-in de-duplication"
    (is (= (car/scan-reduce-kv {:dedupe? true} {}
             (scan-pages-fn (atom [])
               [["next" ["a" 1 "b" 2]]
                ["0"    ["a" 9 "c" 3]]])
             assoc)
          {"a" 1, "b" 2, "c" 3})
      "The first value seen for a repeated key wins when requested"))

  (testing "Reduced value on final page"
    (let [result
          (car/scan-reduce-kv {:dedupe? true} []
            (scan-pages-fn (atom []) [["0" ["a" 1 "b" 2 "c" 3]]])
            (fn [acc k v]
              (let [acc (conj acc [k v])]
                (if (= k "b") (reduced acc) acc))))]
      (is (= result [["a" 1] ["b" 2]]))))

  (testing "Binary key de-duplication"
    (let [pages [["next" [(enc/str->utf8-ba "a") 1]]
                 ["0"    [(enc/str->utf8-ba "a") 2
                           (enc/str->utf8-ba "b") 3]]]
          raw-result
          (car/scan-reduce-kv []
            (scan-pages-fn (atom []) pages)
            (fn [acc k v]
              (conj acc [(enc/utf8-ba->str k) v])))
          result
          (car/scan-reduce-kv {:dedupe? true} []
            (scan-pages-fn (atom []) pages)
            (fn [acc k v]
              (conj acc [(enc/utf8-ba->str k) v])))]
      [(is (= raw-result [["a" 1] ["a" 2] ["b" 3]]))
       (is (= result [["a" 1] ["b" 3]]))]))

  (testing "Malformed page"
    (is (->>
          (car/scan-reduce-kv {}
            (scan-pages-fn (atom []) [["0" ["a" 1 "b"]]])
            assoc)
          (throws? :ex-info {:eid :carmine.scan/odd-key-value-elements}))))

  (testing "Strict options"
    [(is (throws? :ex-info
           (car/scan-reduce-kv {:dedupe :yes} []
             (scan-pages-fn (atom []) [["0" []]]) conj)))
     (is (throws? :ex-info
           (car/scan-reduce-kv {:dedupe? :yes} []
             (scan-pages-fn (atom []) [["0" []]]) conj)))]))

(deftest _scan-bytes-cursors
  (testing "Byte-array cursors (ambient `:bytes` read mode) are normalized"
    (let [calls_ (atom [])
          result
          (car/scan-reduce []
            (scan-pages-fn calls_
              [[(enc/str->utf8-ba "next-1") [1 2]]
               [(enc/str->utf8-ba "0")      [3]]])
            conj)]
      [(is (= result [1 2 3]))
       (is (= @calls_ ["0" "next-1"])
         "Cursors are reused as UTF-8 strings and \"0\" terminates the scan")]))

  (testing "`scan-keys` completes under `as-bytes`"
    (let [prefix  (tk (str "scan-bytes:" (java.util.UUID/randomUUID)))
          k1      (str prefix ":k1")
          k2      (str prefix ":k2")
          pattern (str prefix ":*")]
      (wcar mgr_
        (car/set k1 "v1")
        (car/set k2 "v2"))
      (let [result (car/as-bytes (car/scan-keys mgr_ pattern {:count 1}))]
        [(is (every? enc/bytes? result))
         (is (= (into #{} (map enc/utf8-ba->str) result) #{k1 k2}))]))))

(deftest _scan-keys
  (let [prefix (tk (str "scan-keys:" (java.util.UUID/randomUUID)))
        string-key (str prefix ":string")
        hash-key   (str prefix ":hash")
        pattern    (str prefix ":*")]
    (wcar mgr_
      (car/set string-key "value")
      (car/hset hash-key "field" "value"))
    [(is (= (car/scan-keys mgr_ pattern {:count 1})
            #{string-key hash-key}))
     (when (>= (redis-major-version) 6)
       (is (= (car/scan-keys mgr_ pattern {:count 1, :type :hash})
              #{hash-key})))
     (is (throws? :ex-info (car/scan-keys mgr_ pattern {:count 0})))
     (is (throws? :ex-info (car/scan-keys mgr_ pattern {:match pattern})))]))
