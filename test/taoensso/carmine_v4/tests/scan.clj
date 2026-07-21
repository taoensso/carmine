(ns taoensso.carmine-v4.tests.scan
  "Carmine v4 scan reduction and materialization tests."
  (:require
   [clojure.test        :refer [deftest testing is]]
   [taoensso.encore     :as enc]
   [taoensso.truss      :refer [throws?]]
   [taoensso.carmine-v4          :as car  :refer [wcar]]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.resp     :as resp]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tk  "Test key" support/test-key)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

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

  (testing "Options validate strictly"
    [(is (throws? :ex-info {:eid :carmine.scan/invalid-opts}
           (car/scan-reduce {:dedupe :yes} []
             (scan-pages-fn (atom []) [["0" []]]) conj)))
     (is (throws? :ex-info {:eid :carmine.scan/invalid-opts}
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
    [(is (throws? :ex-info {:eid :carmine.scan/invalid-opts}
           (car/scan-reduce-kv {:dedupe :yes} []
             (scan-pages-fn (atom []) [["0" []]]) conj)))
     (is (throws? :ex-info {:eid :carmine.scan/invalid-opts}
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

(deftest _scan-keys-reply-bindings
  (doseq [[wrap bytes?]
          [[(fn [f] (car/skip-replies (f))) false]
           [(fn [f] (car/parse (constantly nil) (f))) false]
           [(fn [f] (car/as-bytes (car/parse (constantly nil) (f)))) true]]]
    ;; Only one final page is available. An erroneous second scan hits EOF
    ;; and fails immediately, so a cursor regression cannot hang this test.
    (let [in (com/str->in "*2\r\n$1\r\n0\r\n*1\r\n$1\r\nk\r\n")
          out (java.io.BufferedOutputStream. (java.io.ByteArrayOutputStream.))]
      (with-redefs [car/with-car
                    (fn [_ body-fn]
                      (resp/with-replies in out false false body-fn))]
        (let [result (wrap #(car/scan-keys nil))]
          (is (= (if bytes? (into #{} (map enc/utf8-ba->str) result) result)
                #{"k"})))))))

(deftest _scan-keys-dedupe
  (doseq [bytes? [false true]]
    (let [in (com/str->in
               (str "*2\r\n$1\r\n1\r\n*1\r\n$1\r\nk\r\n"
                    "*2\r\n$1\r\n0\r\n*2\r\n$1\r\nk\r\n$1\r\nj\r\n"))
          out (java.io.BufferedOutputStream. (java.io.ByteArrayOutputStream.))]
      (with-redefs [car/with-car
                    (fn [_ body-fn]
                      (resp/with-replies in out false false body-fn))]
        (let [result (if bytes? (car/as-bytes (car/scan-keys nil)) (car/scan-keys nil))]
          [(is (= (count result) 2) "A key repeated across pages is returned once")
           (is (= (if bytes? (into #{} (map enc/utf8-ba->str) result) result)
                 #{"k" "j"}))])))))

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
     (when (>= (support/redis-major-version) 6)
       (is (= (car/scan-keys mgr_ pattern {:count 1, :type :hash})
              #{hash-key})))
     (is (throws? :ex-info {:eid :carmine.scan/invalid-opts} (car/scan-keys mgr_ pattern {:count 0})))
     (is (throws? :ex-info {:eid :carmine.scan/invalid-opts} (car/scan-keys mgr_ pattern {:match pattern})))]))
