(ns taoensso.carmine-v4.tests.swaps
  "Carmine v4 optimistic swap tests."
  (:require
   [clojure.test :refer [deftest testing is]]
   [taoensso.encore :as enc]
   [taoensso.truss :as truss :refer [throws?]]
   [taoensso.carmine-v4 :as car :refer [wcar]]
   [taoensso.carmine-v4.tests.test-support :as support]))

(def tk   support/test-key)
(def mgr_ support/manager_)

(support/use-clean-redis-fixture!)

(deftest _swap
  (let [key (tk (str "swap:" (java.util.UUID/randomUUID)))
        seen_ (atom [])]
    (wcar mgr_ (car/del key))
    (try
      [(testing "Create, update, return override, abort, delete, and stored nil"
         [(is (= (car/swap mgr_ key
                   (fn [old missing?]
                     (swap! seen_ conj [old missing?])
                     {:n 1}))
                 {:n 1}))
          (is (= @seen_ [[nil true]]))
          (is (= (car/swap mgr_ key
                   (fn [old missing?]
                     (is (false? missing?))
                     (update old :n inc)))
                 {:n 2}))
          (is (= (car/swap mgr_ key
                   (fn [old _] (enc/swapped (assoc old :n 3) :returned)))
                 :returned))
          (is (= (wcar mgr_ (car/get key)) {:n 3}))
          (is (= (car/swap mgr_ key (fn [_ _] :swap/abort)) :swap/abort))
          (is (= (car/swap mgr_ key
                   (fn [_ _] (enc/swapped :swap/abort :custom-abort)))
                 :custom-abort))
          (is (= (wcar mgr_ (car/get key)) {:n 3}))
          (is (= (car/swap mgr_ key (fn [_ _] :swap/delete)) :swap/delete))
          (is (= (wcar mgr_ (car/exists key)) 0))
          (is (= (car/swap mgr_ key (fn [_ missing?] (is missing?) :swap/delete))
                 :swap/delete))
          (wcar mgr_ (car/set key nil))
          (is (= (car/swap mgr_ key
                   (fn [old missing?]
                     (is (nil? old))
                     (is (false? missing?))
                     :stored-nil))
                 :stored-nil))])

       (testing "The CAS token retains exact stored bytes and stays isolated"
         (let [raw (byte-array [(unchecked-byte 0xff)
                                (unchecked-byte 0xfe)
                                (unchecked-byte 0xfd)])]
           (wcar mgr_ (car/set key (car/bytes raw)))
           (is (= (car/swap mgr_ key
                    (fn [old missing?]
                      (is (string? old))
                      (is (false? missing?))
                      "updated"))
                  "updated"))
           (is (= (wcar mgr_ (car/get key)) "updated"))

           (wcar mgr_ (car/set key (car/bytes raw)))
           (let [attempts_ (atom 0)]
             [(is (= (car/as-bytes
                       (car/swap mgr_ key {:max-attempts 1}
                         (fn [^bytes old _]
                           (swap! attempts_ inc)
                           (aset-byte old 0 (byte 9))
                           (enc/swapped (car/bytes old) :updated))))
                     :updated)
                "Mutating callback bytes does not mutate the retained CAS token")
              (is (= @attempts_ 1))
              (is (= (seq (wcar mgr_ (car/as-bytes (car/get key))))
                    [9 -2 -3]))])))

       (testing "Read modes apply to the callback's observed value"
         (let [before (byte-array [(byte 1) (byte 2)])
               after  (byte-array [(byte 3) (byte 4)])]
           (wcar mgr_ (car/set key before))
           (is (= (seq
                    (car/as-bytes
                      (car/swap mgr_ key
                        (fn [old _]
                          (is (= (seq old) (seq before)))
                          after))))
                  (seq after)))
           (is (= (seq (wcar mgr_ (car/as-bytes (car/get key))))
                  (seq after)))))

       (testing "Ambient reply parsers do not transform the observed value"
         (wcar mgr_ (car/set key "before"))
         (is (= (car/parse nil (constantly :parsed)
                  (car/swap mgr_ key
                    (fn [old _]
                      (is (= old "before"))
                      "after")))
                "after")))

       (testing "SET semantics clear TTL"
         (wcar mgr_ (car/psetex key 60000 "ttl"))
         (is (= (car/swap mgr_ key (fn [_ _] "updated")) "updated"))
         (is (= (wcar mgr_ (car/pttl key)) -1)))]

       (finally
         (wcar mgr_ (car/del key))))))

(deftest _hswap
  (let [key (tk (str "hswap:" (java.util.UUID/randomUUID)))]
    (wcar mgr_ (car/del key))
    (try
      [(is (= (car/hswap mgr_ key "field"
                (fn [old missing?]
                  (is (nil? old))
                  (is missing?)
                  {:n 1}))
              {:n 1}))
       (is (= (car/hswap mgr_ key "field"
                (fn [old missing?]
                  (is (false? missing?))
                  (update old :n inc)))
              {:n 2}))
       (wcar mgr_ (car/hset key "keep" "yes"))
       (wcar mgr_ (car/pexpire key 60000))
       (is (= (car/hswap mgr_ key "field" (fn [_ _] :swap/abort))
              :swap/abort))
       (is (pos? (wcar mgr_ (car/pttl key))))
       (is (= (car/hswap mgr_ key "field" (fn [_ _] :swap/delete))
              :swap/delete))
       (is (= (wcar mgr_ (car/hexists key "field")) 0))
       (is (pos? (wcar mgr_ (car/pttl key))))
       (is (= (car/hswap mgr_ key "field"
                (fn [old missing?]
                  (is (nil? old))
                  (is missing?)
                  "created"))
              "created"))
       (is (pos? (wcar mgr_ (car/pttl key)))
         "HSETNX creation on an existing hash preserves its TTL")
       (wcar mgr_ (car/hset key "field" nil))
       (is (= (car/hswap mgr_ key "field"
                (fn [old missing?]
                  (is (nil? old))
                  (is (false? missing?))
                  "stored-nil"))
              "stored-nil"))
       (is (pos? (wcar mgr_ (car/pttl key)))
         "A successful HSET-style swap preserves the hash key TTL")]
       (finally
         (wcar mgr_ (car/del key))))))

(deftest _swap-wrong-types
  (let [string-key (tk (str "swap-wrong-string:" (java.util.UUID/randomUUID)))
        hash-key   (tk (str "swap-wrong-hash:"   (java.util.UUID/randomUUID)))
        calls_     (atom 0)
        swap-fn    (fn [old _] (swap! calls_ inc) old)]
    (wcar mgr_
      (car/set string-key "string")
      (car/hset hash-key "field" "hash"))
    (try
      [(is (throws? car/reply-error?
             (car/hswap mgr_ string-key "field" swap-fn)))
       (is (throws? car/reply-error?
             (car/swap mgr_ hash-key swap-fn)))
       (is (zero? @calls_)
         "Redis read errors propagate before the callback runs")]
      (finally
        (wcar mgr_ (car/del string-key) (car/del hash-key))))))

(deftest _swap-retries
  (let [key (tk (str "swap-retries:" (java.util.UUID/randomUUID)))]
    (wcar mgr_ (car/set key "0"))
    (try
      [(testing "A conflict retries with the new value and applies backoff"
         (let [attempts_ (atom 0)
               backoffs_ (atom [])]
           [(is (= (car/swap mgr_ key
                     {:max-attempts 2
                      :retry-backoff-ms
                      (fn [attempt]
                        (swap! backoffs_ conj attempt)
                        0)}
                     (fn [old _]
                       (when (= (swap! attempts_ inc) 1)
                         (wcar mgr_ (car/set key "10")))
                       (enc/swapped (str (inc (Long/parseLong old))) :done)))
                   :done))
            (is (= @attempts_ 2))
            (is (= @backoffs_ [1]))
            (is (= (wcar mgr_ (car/get key)) "11"))]))

       (testing "Exhaustion throws or returns an explicit abort value"
         (let [conflict-fn
               (fn [old _]
                 (wcar mgr_ (car/incr key))
                 old)
               error
               (truss/throws
                 (car/swap mgr_ key {:max-attempts 2} conflict-fn))]
           [(is (truss/submap? (ex-data error)
                  {:eid :carmine.swap/conflict
                   :operation :swap
                   :attempts 2}))
            (is (= (car/swap mgr_ key
                     {:max-attempts 1, :abort-val :conflicted}
                     conflict-fn)
                   :conflicted))]))

       (testing "Conditional delete conflicts retry"
         (let [attempts_ (atom 0)]
           [(wcar mgr_ (car/set key "delete-me"))
            (is (= (car/swap mgr_ key {:max-attempts 2}
                     (fn [_ _]
                       (when (= (swap! attempts_ inc) 1)
                         (wcar mgr_ (car/set key "changed")))
                       :swap/delete))
                   :swap/delete))
            (is (= @attempts_ 2))
            (is (= (wcar mgr_ (car/exists key)) 0))]))

       (testing "Hash updates, deletes, and exhaustion retry on conflicts"
         (let [field "field"
               attempts_ (atom 0)]
           [(wcar mgr_ (car/hset key field "0"))
            (is (= (car/hswap mgr_ key field {:max-attempts 2}
                     (fn [old _]
                       (when (= (swap! attempts_ inc) 1)
                         (wcar mgr_ (car/hset key field "10")))
                       (str (inc (Long/parseLong old)))))
                   "11"))
            (is (= @attempts_ 2))
            (reset! attempts_ 0)
            (is (= (car/hswap mgr_ key field {:max-attempts 2}
                     (fn [_ _]
                       (when (= (swap! attempts_ inc) 1)
                         (wcar mgr_ (car/hset key field "changed")))
                       :swap/delete))
                   :swap/delete))
            (is (= @attempts_ 2))
            (is (= (wcar mgr_ (car/hexists key field)) 0))
            (wcar mgr_ (car/hset key field "conflict"))
            (is (= (car/hswap mgr_ key field
                     {:max-attempts 1, :abort-val :conflicted}
                     (fn [old _]
                       (wcar mgr_ (car/hset key field (str old "!")))
                       old))
                   :conflicted))
            (wcar mgr_ (car/del key) (car/set key "ready"))]))

       (testing "Options are strict and the callback remains last"
         [(is (throws? :ex-info (car/swap mgr_ key {:max-attempts 0} identity)))
          (is (throws? :ex-info (car/swap mgr_ key {:unknown true} identity)))
          (let [attempts_ (atom 0)]
            (is (= (car/swap mgr_ key
                     {:max-attempts 2, :retry-backoff-ms (constantly nil)}
                     (fn [old _]
                       (when (= (swap! attempts_ inc) 1)
                         (wcar mgr_ (car/set key (str old "!"))))
                       "resolved"))
                   "resolved")
              "A nil backoff value skips the sleep and retries"))
          (is (throws? :ex-info
                (car/swap mgr_ key
                  {:max-attempts 2, :retry-backoff-ms (constantly -1)}
                  (fn [old _]
                    (wcar mgr_ (car/set key (str old "!")))
                    old)))
            "Negative backoff values are rejected")
          (is (throws? :ex-info
                (car/skip-replies
                  (car/swap mgr_ key (fn [old _] old)))))])]

       (finally
         (wcar mgr_ (car/del key))))))
