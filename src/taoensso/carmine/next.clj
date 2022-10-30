(ns taoensso.carmine.next
  "Experimental, baggage-free prototype modern Redis client.
  This can act as a proving ground for new stuff like:
    - RESP3  support
    - Module support
    - Sentinel &/or Cluster support
    - General API improvements"

  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require
   [clojure.string :as str]
   [clojure.test   :as test :refer [deftest testing is]]
   [taoensso.encore :as enc :refer [throws?]]
   [taoensso.carmine :as core]
   [taoensso.carmine
    [connections :as conns]
    [protocol    :as protocol]
    [commands    :as cmds]]

   [taoensso.carmine.impl.resp        :as resp]
   [taoensso.carmine.impl.resp.common :as rcom]))

(comment (remove-ns 'taoensso.carmine.next))

;;;; TODO
;; - Investigate Sentinel &/or Cluster
;; - Investigate new conns design (incl. breakage tests, etc.)
;; - Refactor stashing :as-pipeline, etc.
;;
;; - Non-breaking core upgrade feasible? Note pub/sub differences.
;;
;; - (wcar {:hello {}}) and/or (wcar {:init-fn _}) support
;;     HELLO <protocol-version> [AUTH <username> <password>]

(defn conn [] (conns/make-new-connection {:host "127.0.0.1" :port 6379}))
(comment (keys (conn))) ; (:socket :spec :in :out)

(comment
  (protocol/with-context (conn)
    (protocol/with-replies
      (cmds/enqueue-request 1 ["SET" "KX" "VY"])
      (cmds/enqueue-request 1 ["GET" "KX"]))))

;;;;

(comment
  (def c (conn))

  (let [c (conn)]
    (resp/write-requests (:out c) [["PING"]])
    (resp/read-reply     (:in  c)))

  (let [c (conn)]
    (resp/write-requests (:out c) [["SET" "K1" 1] ["GET" "K1"]])
    [(resp/read-reply    (:in  c))
     (resp/read-reply    (:in  c))])

  (let [c (conn)]
    (resp/write-requests (:out c) [["HELLO" 3] ["SET" "K1" 1] ["GET" "K1"]])
    [(resp/read-reply    (:in  c))
     (resp/read-reply    (:in  c))
     (resp/read-reply    (:in  c))])

  (let [c (conn)]
    (resp/write-requests (:out c) [["SET" "K1" {:a :A}] ["GET" "K1"]])
    [(resp/read-reply    (:in  c))
     (resp/read-reply    (:in  c))])

  (let [c (conn)] (.read (:in c))) ; Inherently blocking

  (let [c (conn)] (conns/close-conn c) (.read (:in c))) ; Closed
  )

(comment
  (let [c1 (conn)
        c2 (conn)]

    (enc/qb 1e4

      ;; v3
      (protocol/with-context c1
        (protocol/with-replies
          (cmds/enqueue-request 1 ["ECHO" "FOO"])))

      (do ; v4
        (resp/write-requests (:out c2) [["ECHO" "FOO"]])
        (resp/read-reply     (:in  c2))
        ))) ; [286.97 224.95]
  )
