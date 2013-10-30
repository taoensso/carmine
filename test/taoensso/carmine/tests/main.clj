(ns taoensso.carmine.tests.main
  (:require [clojure.string :as str]
            [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.protocol   :as protocol]
            [taoensso.carmine.benchmarks :as benchmarks]))

(comment (test/run-tests '[taoensso.carmine.tests.main]))

(defmacro wcar* [& body] `(car/wcar {:pool {} :spec {}} ~@body))
(def tkey (partial car/key :carmine :temp :test))
(defn clean-up-tkeys! [] (when-let [ks (seq (wcar* (car/keys (tkey :*))))]
                           (wcar* (apply car/del ks))))

(defn- before-run {:expectations-options :before-run} [] (clean-up-tkeys!))
(defn- after-run  {:expectations-options :after-run}  [] (clean-up-tkeys!))

;;;; Parsers
;; Like middleware, comp order can get confusing

;; Basic parsing
(expect ["PONG" "pong" "pong" "PONG"]
  (wcar {} (car/ping) (car/parse str/lower-case (car/ping) (car/ping)) (car/ping)))

;; Cancel parsing
(expect "YoLo" (wcar {} (->> (car/echo "YoLo")
                             (car/parse nil)
                             (car/parse str/upper-case))))

;; No auto-composition: last-applied (inner) parser wins
(expect "yolo" (wcar {} (->> (car/echo "YoLo")
                             (car/parse str/lower-case)
                             (car/parse str/upper-case))))

;; Dynamic composition: prefer inner
(expect "yolo" (wcar {} (->> (car/echo "YoLo")
                             (car/parse (car/parser-comp str/lower-case
                                                         protocol/*parser*))
                             (car/parse str/upper-case))))

;; Dynamic composition: prefer outer
(expect "YOLO" (wcar {} (->> (car/echo "YoLo")
                             (car/parse (car/parser-comp protocol/*parser*
                                                         str/lower-case))
                             (car/parse str/upper-case))))

;; Dynamic composition with metadata (`return` uses metadata and auto-composes)
(expect "yolo" (wcar {} (->> (car/return "YoLo") (car/parse str/lower-case ))))

;;; Exceptions pass through by default
(expect Exception (wcar {} (->> (car/return (Exception. "boo")) (car/parse keyword))))
(expect vector?   (wcar {} (->> (do (car/return (Exception. "boo"))
                                    (car/return (Exception. "boo")))
                                (car/parse keyword))))

;; Parsers can elect to handle exceptions, even over `return`
(expect "Oh noes!" (wcar {} (->> (car/return (Exception. "boo"))
                                 (car/parse
                                  (-> #(if (instance? Exception %) "Oh noes!" %)
                                      (with-meta {:parse-exceptions? true}))))))

;; Lua (`with-replies`) errors bypass normal parsers
(expect Exception (wcar {} (->> (car/lua "invalid" {:_ :_} {}) (car/parse keyword))))

;; Parsing passes `with-replies` barrier
(expect [["ONE" "three"] "two"]
  (let [p (promise)]
    [(wcar {} (car/echo "ONE") (car/parse str/lower-case
                                 (deliver p (car/with-replies (car/echo "TWO")))
                                 (car/echo "THREE")))
     @p]))

;;;; `atomic`

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
