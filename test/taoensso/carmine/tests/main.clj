(ns taoensso.carmine.tests.main
  (:require [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.benchmarks :as benchmarks]))

(defmacro wcar* [& body] `(car/wcar {:pool {} :spec {}} ~@body))
(defn tkey [key] (car/key :carmine :temp :test))
(defn clean-up-tkeys! [] (when-let [ks (seq (wcar* (car/keys (tkey :*))))]
                           (wcar* (apply car/del ks))))

(defn- before-run {:expectations-options :before-run} [] (clean-up-tkeys!))
(defn- after-run  {:expectations-options :after-run}  [] (clean-up-tkeys!))

;;;; atomic

(expect Exception (car/atomic {} 1)) ; Missing multi
(expect Exception (car/atomic {} 1 (car/multi))) ; Empty multi
(expect Exception (car/atomic {} 1 (car/multi) (car/discard))) ; Like missing multi

(expect [["OK" "QUEUED"] "PONG"] (car/atomic {} 1 (car/multi) (car/ping)))
(expect [["OK" "QUEUED" "QUEUED"] ["PONG" "PONG"]]
        (car/atomic {} 1 (car/multi) (car/ping) (car/ping)))
(expect [["echo" "OK" "QUEUED"] "PONG"]
        (car/atomic {} 1 (car/return "echo") (car/multi) (car/ping)))

(expect Exception (car/atomic {} 1
                    (car/multi)
                    (car/ping)
                    (/ 1 0) ; Throws client-side
                    (car/ping)))

(expect Exception (car/atomic {} 1
                    (car/multi)
                    (car/redis-call [:invalid]) ; Server-side error, before exec
                    (car/ping)))

;; Ignores extra multi [error] while queuing:
(expect "PONG" (-> (car/atomic {} 1 (car/multi) (car/multi) (car/ping))
                   second))

(expect "PONG" (-> (car/atomic {} 1
                     (car/multi)
                     (car/parse (constantly "foo") (car/ping)))
                   second)) ; No parsers

(expect Exception (car/atomic {} 5
                    (car/watch (tkey :watched))
                    (car/set (tkey :watched) (rand))
                    (car/multi)
                    (car/ping))) ; Contending changes to watched key

(expect [[["OK" "OK" "QUEUED"] "PONG"] 3]
        (let [idx (atom 1)]
          [(car/atomic {} 3
             (car/watch (tkey :watched))
             (when (< @idx 3)
               (swap! idx inc)
               (car/set (tkey :watched) (rand))) ; Contend only first 2 attempts
             (car/multi)
             (car/ping))
           @idx]))

(expect (benchmarks/bench {}))
