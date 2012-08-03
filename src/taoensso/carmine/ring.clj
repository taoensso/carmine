(ns taoensso.carmine.ring
  "Carmine-backed Ring session store. Adapted from clj-redis-session."
  {:author "Peter Taoussanis"}
  (:require [ring.middleware.session.store :as session-store]
            [taoensso.carmine :as carmine])
  (:import  [java.util UUID]))

(defn new-session-key [prefix] (str prefix ":" (UUID/randomUUID)))

(defmacro wc [& body] `(carmine/with-conn @~'pool-atom @~'spec-atom ~@body))

(defprotocol ICarmineSessionStore
  (reset-conn [this pool spec]))

(defrecord CarmineSessionStore [pool-atom spec-atom prefix expiration]
  session-store/SessionStore
  (read-session   [_ key] (or (when key (wc (carmine/get key))) {}))
  (delete-session [_ key] (wc (carmine/del key)) nil)
  (write-session  [_ key data]
    (let [key (or key (new-session-key prefix))]
      (if expiration
        (wc (carmine/setex key expiration data))
        (wc (carmine/set key data)))
      key))

  ICarmineSessionStore
  (reset-conn [_ pool spec]
    (reset! pool-atom pool)
    (reset! spec-atom spec)))

(defn make-carmine-store
  "Creates and returns a Carmine-backed Ring SessionStore. Use `expiration-secs`
  to specify how long session data will survive after last write. When nil,
  sessions will never expire."
  [& {:keys [connection-pool connection-spec key-prefix expiration-secs]
      :or   {key-prefix       "carmine:session"
             expiration-secs  (str (* 60 60 24 30))}}]
  (CarmineSessionStore. (atom connection-pool) (atom connection-spec)
                        key-prefix (str expiration-secs)))