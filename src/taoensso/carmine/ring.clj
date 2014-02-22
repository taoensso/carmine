(ns taoensso.carmine.ring
  "Carmine-backed Ring session store. Adapted from clj-redis-session."
  {:author "Peter Taoussanis"}
  (:require [ring.middleware.session.store :as session-store]
            [taoensso.carmine :as car :refer (wcar)]))

(defrecord CarmineSessionStore [conn-opts prefix ttl-secs]
  session-store/SessionStore
  (read-session   [_ key] (or (when key (wcar conn-opts (car/get key))) {}))
  (delete-session [_ key] (wcar conn-opts (car/del key)) nil)
  (write-session  [_ key data]
    (let [key (or key (str prefix ":" (java.util.UUID/randomUUID)))]
      (wcar conn-opts (if ttl-secs (car/setex key ttl-secs data)
                                   (car/set   key          data)))
      key)))

(defn carmine-store
  "Creates and returns a Carmine-backed Ring SessionStore. Use `expiration-secs`
  to specify how long session data will survive after last write. When nil,
  sessions will never expire."
  [conn-opts & [{:keys [key-prefix expiration-secs]
                 :or   {key-prefix      "carmine:session"
                        expiration-secs (* 60 60 24 30)}}]]
  (->CarmineSessionStore conn-opts key-prefix expiration-secs))

(defn make-carmine-store ; 1.x backwards compatiblity
  "DEPRECATED. Use `carmine-store` instead."
  [& [s1 s2 & sn :as sigs]]
  (if (instance? taoensso.carmine.connections.ConnectionPool s1)
    (carmine-store {:pool s1 :spec s2} (apply hash-map sn))
    (apply carmine-store sigs)))
