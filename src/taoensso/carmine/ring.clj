(ns taoensso.carmine.ring
  "Carmine-backed Ring session store. Adapted from clj-redis-session."
  {:author "Peter Taoussanis"}
  (:require [ring.middleware.session.store :as session-store]
            [taoensso.carmine :as car])
  (:import  [java.util UUID]))

(defn new-session-key [prefix] (str prefix ":" (UUID/randomUUID)))

(defmacro wcar
  [& body]
  `(let [{pool# :pool spec# :spec} @~'conn-atom]
     (car/with-conn pool# spec# ~@body)))

(defprotocol ICarmineSessionStore
  (reset-conn [this pool spec]))

(defrecord CarmineSessionStore [conn-atom prefix expiration]
  session-store/SessionStore
  (read-session   [_ key] (or (when key (wcar (car/get key))) {}))
  (delete-session [_ key] (wcar (car/del key)) nil)
  (write-session  [_ key data]
    (let [key  (or key (new-session-key prefix))
          data (assoc data :session-id key)]
      (if expiration
        (wcar (car/setex key expiration data))
        (wcar (car/set key data)))
      key))

  ICarmineSessionStore
  (reset-conn [_ pool spec] (reset! conn-atom {:pool pool :spec spec})))

(defn make-carmine-store
  "Creates and returns a Carmine-backed Ring SessionStore. Use `expiration-secs`
  to specify how long session data will survive after last write. When nil,
  sessions will never expire."
  [connection-pool connection-spec
   & {:keys [key-prefix expiration-secs]
      :or   {key-prefix       "carmine:session"
             expiration-secs  (str (* 60 60 24 30))}}]
  (CarmineSessionStore. (atom {:pool connection-pool :spec connection-spec})
                        key-prefix (str expiration-secs)))