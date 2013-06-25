(ns taoensso.carmine.tests.tundra
  (:require [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.faraday :as far]
            [taoensso.carmine.tundra         :as tundra :refer (ensure-ks dirty)]
            [taoensso.carmine.tundra.faraday :as tfar]))

;;;; Config

(def tkey (partial car/kname :carmine :temp :test :tundra))
(defonce creds {:access-key (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY")
                :secret-key (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY")})

(def ttable :faraday.tests.tundra)
(def tstore (tundra/tundra-store (tfar/faraday-datastore creds)))
(def tundra-worker (tundra/worker tstore {} {:frequency-ms 1000
                                             :auto-start?  false}))

(defn- cleanup []
  (when-let [tks (seq (wcar {} (car/keys (tkey :*))))]
    (wcar (apply car/del tks)))
  (far/batch-write-item creds
    {ttable {:delete [{:key-ns :default :redis-key [:k1 :k2 :k3]}]}}))

(comment (far/delete-table creds ttable))

(defn- before-run {:expectations-options :before-run} []
  (println "Setting up testing environment...")
  (tfar/ensure-table creds
    {:name        ttable
     :throughput  {:read 1 :write 1}
     :block?      false})
  (cleanup)
  (println "Ready to roll..."))

(defn- after-run {:expectations-options :after-run} [] (cleanup))

;;;; Tests

(def ^:private stress-data {:a :A :b :B :c [0 1 2 3 4]})

;; `dirty` marks ks as dirty w/o modifying them
(expect ["OK" ["0" "0" stress-data] #{"k1" "k2" "k3"}]
        (wcar {} (car/mset :k1 0 :k2 0 :k3 stress-data)
                 (dirty tstore :k1 :k2 :k3)
                 (car/mget :k1 :k2 :k3)
                 (->> (car/smembers (#'tundra/tkey :dirty))
                      (car/with-parser set))))

;; Worker freezes ks and marks ks as clean w/o modifying them
(expect [#{} 1 1 ["1" "1" stress-data]]
        (do (tundra/start tundra-worker)
            (Thread/sleep 8000) ; Wait for freezing
            (wcar {} (->> (car/smembers (#'tundra/tkey :dirty))
                          (car/with-parser set))
                          (mapv car/incr [:k1 :k2]) ; NOT marking as dirty!
                          (car/mget :k1 :k2 :k3))))

;; `ensure` restores evicted ks (and only to previously frozen state)
(expect [2 1 2 ["1" "2" stress-data]] ; :k2 evicted but not dirty
        (wcar {} (car/del :k1 :k3) ; Evict
                 (tundra/ensure-ks tstore :k1 :k2 :k3)
                 (mapv car/incr [:k1 :k2])
                 (car/mget :k1 :k2 :k3)))

(expect Exception (wcar {} (ensure-ks tstore :kinvalid)))
(expect Exception (wcar {} (dirty     tstore :kinvalid)))