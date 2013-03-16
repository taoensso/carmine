(ns taoensso.carmine.locks
  "Alpha - subject to change.
  Distributed lock implementation for Carmine based on work by Ronen Narkis
  and Josiah Carlson.

  Redis keys:
    * carmine:lock:<lock-name> -> ttl str, lock owner's UUID

  Ref. http://goo.gl/5UalQ for implementation details."
  (:require [taoensso.carmine       :as car]
            [taoensso.carmine.utils :as utils]
            [taoensso.timbre        :as timbre]))

(utils/defonce* config "Alpha - subject to change."
  (atom {:conns {:pool (car/make-conn-pool)
                 :spec (car/make-conn-spec)}}))

(defn set-config! [[k & ks] val] (swap! config assoc-in (cons k ks) val))

(defmacro ^:private wcar
  [& body]
  `(let [{pool# :pool spec# :spec} (:conns @config)]
     (car/with-conn pool# spec# ~@body)))

(def ^:private lkey (car/make-keyfn "carmine" "lock"))

(defn acquire-lock
  "Attempts to acquire a distributed lock, returning an owner UUID iff successful."
  [lock-name lock-timeout-ms wait-timeout-ms]
  (let [max-udt (+ wait-timeout-ms (System/currentTimeMillis))
        uuid    (str (java.util.UUID/randomUUID))]
    (wcar ; Hold one connection for all attempts
     (loop []
       (when (> max-udt (System/currentTimeMillis))
         (if (-> (car/lua-script
                  "if redis.call('setnx', _:lkey, _:uuid) == 1 then
                    redis.call('pexpire', _:lkey, _:lock-timeout-ms)
                    return 1
                  else
                    return 0
                  end"
                  {:lkey            (lkey lock-name)}
                  {:uuid            uuid
                   :lock-timeout-ms lock-timeout-ms})
                 car/parse-bool car/with-replies)
           (car/return uuid)
           (do (Thread/sleep 1) (recur))))))))

(comment (acquire-lock "my-lock" 2000 500))

(defn release-lock
  "Attempts to release a distributed lock, returning true iff successful."
  [lock-name owner-uuid]
  (-> (car/lua-script
        "if redis.call('get', _:lkey) == _:uuid then
         redis.call('del', _:lkey)
         return 1
       else
         return 0
       end"
        {:lkey (lkey lock-name)}
        {:uuid owner-uuid})
       car/parse-bool wcar))

(comment
  (when-let [uuid (acquire-lock "my-lock" 2000 500)]
    [(Thread/sleep 100)
     (release-lock "my-lock" uuid)
     (release-lock "my-lock" uuid)]))

(defn have-lock?
  [lock-name owner-uuid]
  (-> (car/lua-script
       "if redis.call('get', _:lkey) == _:uuid then
         return 1
       else
         return 0
       end"
       {:lkey (lkey lock-name)}
       {:uuid owner-uuid})
      car/parse-bool wcar))

(comment
  (when-let [uuid (acquire-lock "my-lock" 2000 500)]
    [(Thread/sleep 100)
     (have-lock? "my-lock" uuid)
     (Thread/sleep 2000)
     (have-lock? "my-lock" uuid)]))

(defmacro with-lock
  "Attempts to acquire a distributed lock, executing body and then releasing
  lock when successful. Returns {:result <body's result>} on successful release,
  or nil if the lock could not be acquired. If the lock is successfully acquired
  but expires before being released, throws an exception."
  [lock-name lock-timeout-ms wait-timeout-ms & body]
  `(when-let [uuid# (acquire-lock ~lock-name ~lock-timeout-ms ~wait-timeout-ms)]
     (try
       {:result (do ~@body)} ; Wrapped to distinguish nil body result
       (catch Throwable t# (throw t#))
       (finally
        (when-not (release-lock ~lock-name uuid#)
          (throw (RuntimeException. (str "Lock expired before it was released: "
                                         ~lock-name))))))))

(comment
  (timbre/set-level! :debug)
  (with-lock "my-lock" 2000 500 (Thread/sleep 1000) "foo!")  ; {:result "foo!"}
  (with-lock "my-lock" 2000 500 (Thread/sleep 1000) (/ 1 0)) ; ex
  (with-lock "my-lock" 2000 500 (Thread/sleep 2500) "foo!")  ; ex
  (do (future (with-lock "my-lock" 2000 500  (Thread/sleep 1000) (println "foo!")))
      (future (with-lock "my-lock" 2000 500  (Thread/sleep 1000) (println "bar!")))
      (future (with-lock "my-lock" 2000 2000 (Thread/sleep 1000) (println "baz!")))))

(defn- release-all-locks! []
  (when-let [lkeys (seq (wcar (car/keys (lkey "*"))))]
    (wcar (apply car/del lkeys))))

(comment (release-all-locks!))