(ns taoensso.carmine.ring
  "Carmine-backed Ring session store."
  {:author "Peter Taoussanis"}
  (:require [ring.middleware.session]
            [taoensso.encore  :as enc]
            [taoensso.carmine :as car :refer (wcar)]))

(defrecord CarmineSessionStore [conn-opts prefix ttl-secs extend-on-read?]
  ring.middleware.session.store/SessionStore
  (read-session [_ k]
    (last
      (wcar conn-opts :as-pipeline
        (when (and extend-on-read? ttl-secs)
          (car/expire k ttl-secs))
        (car/get k))))

  (delete-session [_ k] (wcar conn-opts (car/del k)) nil)
  (write-session  [_ k data]
    (let [k (or k (str prefix ":" (enc/uuid-str)))]
      (wcar conn-opts
        (if ttl-secs
          (car/setex k ttl-secs data)
          (car/set   k          data)))
      k)))

(defn carmine-store
  "Creates and returns a Carmine-backed Ring SessionStore.
  Options include:
    :expiration-secs - How long session data should persist after last
                       write. nil => persist forever.
    :extend-on-read? - If true, expiration will also be extended by
                       session reads."
  [conn-opts & [{:keys [key-prefix expiration-secs extend-on-read?]
                 :or   {key-prefix      "carmine:session"
                        expiration-secs (enc/secs :days 30)
                        extend-on-read? false}}]]
  (->CarmineSessionStore conn-opts key-prefix expiration-secs extend-on-read?))

(enc/deprecated
  (defn ^:deprecated make-carmine-store ; 1.x backwards compatiblity
    "DEPRECATED. Use `carmine-store` instead."
    [& [s1 s2 & sn :as args]]
    (if (instance? taoensso.carmine.connections.ConnectionPool s1)
      (carmine-store {:pool s1 :spec s2} (apply hash-map sn))
      (apply carmine-store args))))
