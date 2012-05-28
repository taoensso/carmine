(ns carmine.ring
  "Carmine-backed Ring session store. Adapted from clj-redis-session."
  (:require [ring.middleware.session.store :as session-store]
            [carmine.core :as carmine])
  (:import  [java.util UUID]))

(defn new-session-key [prefix] (str prefix ":" (str (UUID/randomUUID))))

(defmacro wc [& body] `(carmine.core/with-conn ~'pool ~'spec ~@body))

(defrecord CarmineSessionStore [pool spec prefix expiration]
  session-store/SessionStore
  (read-session   [_ key] (or (when key (wc (carmine/get key))) {}))
  (delete-session [_ key] (wc (carmine/del key)) nil)
  (write-session  [_ key data]
    (let [key (or key (new-session-key prefix))]
      (if expiration
        (wc (carmine/setex key expiration data))
        (wc (carmine/set key data)))
      key)))

(defn make-carmine-store
  "Create and return a Carmine-backed Ring SessionStore. Use 'expiration-secs'
  to specify how long session data will survive after last write. When nil,
  sessions will never expire."
  [& {:keys [connection-pool connection-spec key-prefix expiration-secs]
      :or   {key-prefix       "carmine-session"
             expiration-secs  (str #=(* 60 60 24 30))}}]
  (CarmineSessionStore. connection-pool connection-spec
                        key-prefix (str expiration-secs)))