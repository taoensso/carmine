(ns taoensso.carmine.tests.locks
  (:require [expectations     :as test :refer :all]
            [taoensso.carmine :as car  :refer (wcar)]
            [taoensso.carmine.locks
             :refer (acquire-lock release-lock with-lock)]))

(comment (test/run-tests '[taoensso.carmine.tests.locks]))
(defn- before-run {:expectations-options :before-run} [])
(defn- after-run  {:expectations-options :after-run}  [])

;;; Basic locking

(expect (acquire-lock {} :2 2000 2000)) ; For 2 secs
(expect (not (acquire-lock {} :2 2000 200))) ; Too early
(expect (do (Thread/sleep 1000)
            (acquire-lock {} :2 2000 2000)))

;;; Releasing

(expect-let
  [uuid (acquire-lock {} :3 2000 2000)]
  (fn [[x y z]] (and (nil? x) y z))
  [(acquire-lock {} :3 2000 200) ; Too early
   (release-lock {} :3 uuid)
   (acquire-lock {} :3 2000 10)])

;;; Already released

(expect-let
  [uuid (acquire-lock {} :4 2000 2000)
   f1   (future (release-lock {} :4 uuid))]
  false
  (do (Thread/sleep 200) ; Let future run
      (release-lock {} :4 uuid)))

;;; Locking scope

(expect #(not (nil? %))
  (do (try (with-lock {} :5 2000 2000 (throw (Exception.)))
           (catch Exception e nil))
      (acquire-lock {} :5 2000 2000)))

;;; Locking failure

(expect nil (do (acquire-lock {} :6 3000 2000)
                (with-lock {} :6 2000 10)))

;;; `with-lock` expiry

(expect Exception (with-lock {} 9 500 2000 (Thread/sleep 1000)))
(expect nil
  (do (future (with-lock {} :10 500 2000 (Thread/sleep 1000)))
      (Thread/sleep 100) ; Give future time to acquire lock
      (with-lock {} :10 3000 10 :foo)))
(expect {:result :foo}
  (do (future (with-lock {} :11 500 2000 (Thread/sleep 1000)))
      (Thread/sleep 600) ; Give future time to acquire + lose lock
      (with-lock {} :11 3000 10 :foo)))
