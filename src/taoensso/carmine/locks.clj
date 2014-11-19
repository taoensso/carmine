(ns taoensso.carmine.locks
  "Alpha - subject to change.
  Distributed lock implementation for Carmine based on work by Ronen Narkis
  and Josiah Carlson. Redis 2.6+.

  Redis keys:
    * carmine:lock:<lock-name> -> ttl str, lock owner's UUID.

  Ref. http://goo.gl/5UalQ for implementation details."
  (:require [taoensso.timbre  :as timbre]
            [taoensso.carmine :as car :refer (wcar)]))

(def ^:private lkey (partial car/key :carmine :lock))

(defn acquire-lock
  "Attempts to acquire a distributed lock, returning an owner UUID iff successful."
  ;; TODO Waiting on http://goo.gl/YemR7 for simpler (non-Lua) solution
  [conn-opts lock-name timeout-ms wait-ms]
  (let [max-udt (+ wait-ms (System/currentTimeMillis))
        uuid    (str (java.util.UUID/randomUUID))]
    (wcar conn-opts ; Hold one connection for all attempts
     (loop []
       (when (> max-udt (System/currentTimeMillis))
         (if (-> (car/lua
                  "if redis.call('setnx', _:lkey, _:uuid) == 1 then
                    redis.call('pexpire', _:lkey, _:timeout-ms);
                    return 1;
                  else
                    return 0;
                  end"
                  {:lkey       (lkey lock-name)}
                  {:uuid       uuid
                   :timeout-ms timeout-ms})
                 car/with-replies car/as-bool)
           (car/return uuid)
           (do (Thread/sleep 1) (recur))))))))

(comment (acquire-lock {} "my-lock" 2000 500))

(defn release-lock
  "Attempts to release a distributed lock, returning true iff successful."
  [conn-opts lock-name owner-uuid]
  (wcar conn-opts
    (car/parse-bool
     (car/lua
      "if redis.call('get', _:lkey) == _:uuid then
         redis.call('del', _:lkey);
         return 1;
       else
         return 0;
       end"
      {:lkey (lkey lock-name)}
      {:uuid owner-uuid}))))

(comment
  (when-let [uuid (acquire-lock {} "my-lock" 2000 500)]
    [(Thread/sleep 100)
     (release-lock {} "my-lock" uuid)
     (release-lock {} "my-lock" uuid)]))

(defn have-lock? [conn-opts lock-name owner-uuid]
  (= (wcar conn-opts (car/get (lkey lock-name))) owner-uuid))

(comment
  (when-let [uuid (acquire-lock {} "my-lock" 2000 500)]
    [(Thread/sleep 100)
     (have-lock? {} "my-lock" uuid)
     (Thread/sleep 2000)
     (have-lock? {} "my-lock" uuid)]))

(defmacro with-lock
  "Attempts to acquire a distributed lock, executing body and then releasing
  lock when successful. Returns {:result <body's result>} on successful release,
  or nil if the lock could not be acquired. If the lock is successfully acquired
  but expires before being released, throws an exception."
  [conn-opts lock-name timeout-ms wait-ms & body]
  `(let [conn-opts# ~conn-opts]
     (when-let [uuid# (acquire-lock conn-opts# ~lock-name ~timeout-ms ~wait-ms)]
       (try
         {:result (do ~@body)} ; Wrapped to distinguish nil body result
         (catch Throwable t# (throw t#))
         (finally
          (when-not (release-lock conn-opts# ~lock-name uuid#)
            (throw (ex-info (str "Lock expired before it was released: "
                              ~lock-name)
                     {:lock-name ~lock-name}))))))))

(comment
  (timbre/set-level! :debug)
  (with-lock {} "my-lock" 2000 500 (Thread/sleep 1000) "m")     ; {:result "m"}
  (with-lock {} "my-lock" 2000 500 (Thread/sleep 1000) (/ 1 0)) ; ex
  (with-lock {} "my-lock" 2000 500 (Thread/sleep 2500) "m")     ; ex
  (do (future (with-lock {} "my-lock" 2000 500  (Thread/sleep 1000) (println "m")))
      (future (with-lock {} "my-lock" 2000 500  (Thread/sleep 1000) (println "m")))
      (future (with-lock {} "my-lock" 2000 2000 (Thread/sleep 1000) (println "m")))))

(defn- release-all-locks! [conn-opts]
  (when-let [lkeys (seq (wcar conn-opts (car/keys (lkey :*))))]
    (wcar conn-opts (apply car/del lkeys))))

(comment (release-all-locks! {}))
