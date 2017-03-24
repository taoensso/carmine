(ns taoensso.carmine.tests.tundra
  (:require
   [clojure.test     :as test :refer [is deftest]]
   [taoensso.carmine :as car  :refer [wcar]]
   [taoensso.carmine.message-queue :as mq]
   [taoensso.carmine.tundra        :as tundra]
   [taoensso.carmine.tundra.disk   :as tdisk]))

(comment
  (remove-ns      'taoensso.carmine.tests.tundra)
  (test/run-tests 'taoensso.carmine.tests.tundra))

(defmacro wcar* [& body] `(car/wcar {} ~@body))
(def tkey (partial car/key :carmine :tundra :test))
(def tqname "carmine-tundra-tests")
(def mqname (format "tundra:%s" (name tqname))) ; Nb has prefix

(defn clean-up! []
  (mq/clear-queues {} mqname)
  (when-let [ks (seq (wcar* (car/keys (tkey :*))))]
    (wcar* (apply car/del ks)
           (apply car/srem @#'tundra/k-evictable ks)))
  (wcar* (car/srem @#'tundra/k-evictable (tkey :invalid-evictable))))

(defn cleanup-fixture [f] (clean-up!) (f) (clean-up!))
(test/use-fixtures :once cleanup-fixture)

(def dstore (tdisk/disk-datastore "./carmine-tundra-test-temp"))

(defn- s->ba [^String s] (.getBytes s "UTF-8"))
(defn- ba->s [^bytes ba] (String.  ba "UTF-8"))

(deftest datastore-tests
  (test/testing "Basic put and fetch case"
    (tundra/put-key dstore (tkey :foo) (s->ba "hello world"))
    (is (= "hello world"
           (-> dstore
               (tundra/fetch-keys [(tkey :foo)])
               first
               ba->s))))

  (test/testing "Update val case"
    (tundra/put-key dstore (tkey :foo) (s->ba "hello world 1"))
    (tundra/put-key dstore (tkey :foo) (s->ba "hello world 2"))
    (is (= "hello world 2"
           (-> dstore
               (tundra/fetch-keys [(tkey :foo)])
               first
               ba->s))))

  (test/testing "Exception Case"
    (is (instance? java.nio.file.NoSuchFileException
                   (first (tundra/fetch-keys dstore [(tkey :invalid)]))))))

(deftest tundra-api-test-1
  (let [tstore (tundra/tundra-store dstore)]
    (test/testing "Bad cases"
      (is (thrown? Exception (wcar* (tundra/dirty tstore (tkey :invalid)))))
      (is (= nil (wcar* (tundra/ensure-ks tstore (tkey :invalid)))))
      (is (thrown? Exception
                   (wcar* (car/sadd @#'tundra/k-evictable (tkey :invalid-evictable))
                          (tundra/ensure-ks tstore (tkey :invalid-evictable))))))

    (test/testing "API never pollutes enclosing pipeline"
      (is (= ["OK" "PONG" 1]
             (wcar* (car/set (tkey 0) "0")
                    (car/ping)
                    ;; Won't throw since `(tkey :invalid)` isn't in evictable set:
                    (tundra/ensure-ks tstore (tkey 0) (tkey :invalid))
                    (tundra/dirty     tstore (tkey 0))
                    (car/del (tkey 0) "0")))))))


(deftest tundra-api-test-2
  (is (= [[:clj-val] [:clj-val] [:clj-val-new]]
         (let [_       (clean-up!)
               tstore  (tundra/tundra-store dstore {:tqname tqname})
               tworker (tundra/worker tstore {} {:eoq-backoff-ms 100 :throttle-ms 100})]

           (wcar* (car/mset (tkey 0) [:clj-val]
                            (tkey 1) [:clj-val]
                            (tkey 2) [:clj-val])) ; Reset vals

           ;; Invalid keys don't prevent valid keys from being processed (will still
           ;; throw, but only _after_ all possible dirtying):
           (wcar* (try (tundra/dirty tstore (tkey :invalid) (tkey :invalid-evictable)
                                     (tkey 0) (tkey 1) (tkey 2))
                       (catch Exception _ nil)))

           (Thread/sleep 8000) ; Wait for replication
           (mq/stop tworker)
           (wcar* (car/del (tkey 0))
                  (car/set (tkey 2) [:clj-val-new])) ; Make some local modifications

           ;; Invalid keys don't prevent valid keys from being processed (will still
           ;; throw, but only _after_ all possible fetches)
           (wcar* (try (tundra/ensure-ks tstore (tkey :invalid) (tkey :invalid-evictable)
                                         (tkey 0) (tkey 1) (tkey 2))
                       (catch Exception _ nil)))

           (wcar* (car/mget (tkey 0) (tkey 1) (tkey 2)))))))


(deftest tundra-api-test-3
  (is (= [-1 -1 -1] ; nil eviction timeout (default) is respected!
         (let [tstore  (tundra/tundra-store dstore {:tqname tqname})
               tworker (tundra/worker tstore {} {:eoq-backoff-ms 100 :throttle-ms 100})]
           (wcar* (car/mset (tkey 0) "0" (tkey 1) "1" (tkey 2) "1") ; Clears timeouts
                  (tundra/dirty tstore (tkey 0) (tkey 1) (tkey 2)))
           (Thread/sleep 8000) ; Wait for replication
           (mq/stop tworker)
           (wcar* (tundra/ensure-ks tstore (tkey 0) (tkey 1) (tkey 2))
                  (mapv #(car/ttl (tkey %)) [0 1 2]))))))

(deftest tundra-api-test-4
  (test/testing "nnil eviction timeout is applied & extended correctly"
    (is ((fn [[t0 t1 t2]]
           (and (= t0 -1)
             (> t1  0)
             (> t2  t1)))

         (let [_       (clean-up!)
               tstore  (tundra/tundra-store dstore {:redis-ttl-ms (* 1000 60 60 24)
                                                    :tqname tqname})
               tworker (tundra/worker tstore {} {:eoq-backoff-ms 100 :throttle-ms 100
                                                 :auto-start false})]

           (wcar* (car/set (tkey 0) "0") ; Clears timeout
             (tundra/dirty tstore (tkey 0)))

           [(wcar* (car/pttl (tkey 0))) ; `dirty` doesn't set ttl immediately
            (do (mq/start tworker)
                (Thread/sleep 5000) ; Wait for replication (> exp-backoff)
                (mq/stop tworker)
                (wcar* (car/pttl (tkey 0)))) ; Worker sets ttl after successful put
            (do (Thread/sleep 1000)
                (wcar* (tundra/ensure-ks tstore (tkey 0))
                       (car/pttl (tkey 0))))])))))

(comment (clean-up!)
         (mq/queue-status {} mqname))
