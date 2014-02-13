(ns taoensso.carmine.tests.tundra
  (:require [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.message-queue :as mq]
            [taoensso.carmine.tundra        :as tundra]
            [taoensso.carmine.tundra.s3     :as ts3])
  (:import  [com.amazonaws.services.s3.model AmazonS3Exception]))

(comment (test/run-tests '[taoensso.carmine.tests.tundra]))

(defmacro wcar* [& body] `(car/wcar {:pool {} :spec {}} ~@body))
(def tkey (partial car/key :carmine :tundra :test))
(def tqname "carmine-tundra-tests")
(defn clean-up! []
  (mq/clear-queues {} tqname)
  (when-let [ks (seq (wcar* (car/keys (tkey :*))))]
    (wcar* (apply car/del ks))))

(defn- before-run {:expectations-options :before-run} [] (clean-up!))
(defn- after-run  {:expectations-options :after-run}  [] (clean-up!))

(defonce creds {:access-key (get (System/getenv) "AWS_S3_ACCESS_KEY")
                :secret-key (get (System/getenv) "AWS_S3_SECRET_KEY")})

(def dstore (ts3/s3-datastore creds "ensso-store/tundra"))

(defn- s->ba [^String s] (.getBytes s "UTF-8"))
(defn- ba->s [^bytes ba] (String.  ba "UTF-8"))

;;;; S3 DataStore

(expect "hello world" ; Basic put & fetch
  (do (tundra/put-key dstore (tkey "foo") (s->ba "hello world"))
      (-> (tundra/fetch-keys dstore [(tkey "foo")]) (first) (ba->s))))

(expect "hello world 2" ; Update val
  (do (tundra/put-key dstore (tkey "foo") (s->ba "hello world 1"))
      (tundra/put-key dstore (tkey "foo") (s->ba "hello world 2"))
      (-> (tundra/fetch-keys dstore [(tkey "foo")]) (first) (ba->s))))

(expect AmazonS3Exception (first (tundra/fetch-keys dstore [(tkey "invalid")])))

;;;; Tundra API

(let [tstore (tundra/tundra-store dstore)]
  (expect (and (:access-key creds) (:secret-key creds)))
  (expect Exception (wcar* (tundra/dirty     tstore (tkey "invalid"))))
  (expect nil       (wcar* (tundra/ensure-ks tstore (tkey "invalid"))))
  (expect Exception (wcar* (car/sadd @#'tundra/k-evictable (tkey "invalid-evictable"))
                           (tundra/ensure-ks tstore (tkey "invalid-evictable"))))

  ;; API never pollutes enclosing pipeline
  (expect ["OK" "PONG" 1] (wcar* (car/set (tkey 0) "0")
                                 (car/ping)
                                 (tundra/ensure-ks tstore (tkey 0) (tkey "invalid") )
                                 (tundra/dirty     tstore (tkey 0))
                                 (car/del (tkey 0) "0"))))

(expect [[:clj-val] [:clj-val] [:clj-val-new]]
  (let [tstore  (tundra/tundra-store dstore {:tqname tqname})
        tworker (tundra/worker tstore {} {:eoq-backoff-ms 100 :throttle-ms 100})]
    (wcar* (car/mset (tkey 0) [:clj-val]
                     (tkey 1) [:clj-val]
                     (tkey 2) [:clj-val])) ; Reset vals

    ;; Invalid keys don't prevent valid keys from being processed
    (wcar* (try (tundra/dirty tstore (tkey "invalid") (tkey "invalid-evictable")
                                     (tkey 0) (tkey 1) (tkey 2))
                  (catch Exception _ nil)))

    (Thread/sleep 8000) ; Wait for replication
    (mq/stop tworker)
    (wcar* (car/del (tkey 0))
           (car/set (tkey 2) [:clj-val-new])
           (car/with-replies)) ; Make some local modifications

    ;; Invalid keys don't prevent valid keys from being processed
    (wcar* (try (tundra/ensure-ks tstore (tkey "invalid") (tkey "invalid-evictable")
                                         (tkey 0) (tkey 1) (tkey 2))
                (catch Exception _ nil)))
    (wcar* (car/mget (tkey 0) (tkey 1) (tkey 2)))))

(expect [-1 -1 -1] ; nil eviction timeout (default) is respected!
  (let [tstore  (tundra/tundra-store dstore {:tqname tqname})
        tworker (tundra/worker tstore {} {:eoq-backoff-ms 100 :throttle-ms 100})]
    (wcar* (car/mset (tkey 0) "0" (tkey 1) "1" (tkey 2) "1") ; Clears timeouts
           (tundra/dirty tstore (tkey 0) (tkey 1) (tkey 2))
           (Thread/sleep 8000) ; Wait for replication
           (mq/stop tworker)
           (tundra/ensure-ks tstore (tkey 0) (tkey 1) (tkey 2)))
    (wcar* (mapv #(car/ttl (tkey %)) [0 1 2]))))

(expect ; nnil eviction timeout is applied & extended correctly
 (fn [[t0 t1 t2]]
   (and (= t0 -1)
        (> t1  0)
        (> t2  t1)))

 (let [tstore  (tundra/tundra-store dstore {:redis-ttl-ms (* 1000 60 60 24)
                                            :tqname tqname})
       tworker (tundra/worker tstore {} {:eoq-backoff-ms 100 :throttle-ms 100
                                         :auto-start false})]
   (wcar* (car/set (tkey 0) "0") ; Clears timeout
          (tundra/dirty tstore (tkey 0)))

   (wcar* (car/pttl (tkey 0))
          (mq/start tworker)
          (Thread/sleep 3000) ; Wait for replication
          (mq/stop tworker)
          (car/pttl (tkey 0))
          (Thread/sleep 1000)
          (tundra/ensure-ks tstore (tkey 0))
          (car/pttl (tkey 0)))))

(comment (clean-up!)
         (mq/queue-status {} tqname))
