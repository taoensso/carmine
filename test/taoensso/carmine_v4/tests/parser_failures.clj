(ns taoensso.carmine-v4.tests.parser-failures
  "Parser failures preserve assertions, cancellation and connection ownership."
  (:require
   [clojure.test :refer [deftest testing is]]
   [taoensso.carmine-v4 :as car]
   [taoensso.carmine-v4.resp.common :as com]
   [taoensso.carmine-v4.tests.test-support :as support]))

(defn- observe-throw [f]
  (try
    (try
      {:value (f)}
      (catch Throwable t
        {:thrown t, :interrupted? (.isInterrupted (Thread/currentThread))}))
    (finally (Thread/interrupted))))

(defn- failing-parser [phase failure]
  (let [fail! (fn [& _] (throw failure))
        rf (fn
             ([] (if (= phase :init) (fail!) []))
             ([acc] (if (= phase :complete) (fail!) acc))
             ([acc x] (if (= phase :step) (fail!) (conj acc x))))]
    (if (= phase :fn)
      (com/fn-parser {} fail!)
      (com/rf-parser {}
        (case phase
          :xform-construction fail!
          :xform-step (map fail!)
          nil)
        rf))))

(def ^:private phases [:fn :init :step :complete :xform-construction :xform-step])

(defn- pipeline! [mgr mode parser]
  (car/with-car mgr {:error-mode mode}
    (fn []
      (binding [com/*parser* parser]
        (car/rcmd "EVAL" "return {1,2,3}" 0))
      (car/echo "tail"))))

(deftest _ordinary-parser-failures-drain-and-reuse
  (with-open [mgr (car/conn-manager
                   {:conn-opts support/test-conn-opts
                    :pool-opts {:max-total 1, :max-idle 1, :test-on-borrow? false}})]
    (doseq [phase phases
            mode [:throw :return]
            failure [(Exception. "ordinary parser failure")
                     (AssertionError. "parser assertion")]]
      (testing (str phase " " mode " " (class failure))
        (let [before (car/wcar mgr (car/rcmd "CLIENT" "ID"))
              result (observe-throw #(pipeline! mgr mode (failing-parser phase failure)))
              replies (if (= mode :return) (:value result) (:replies (ex-data (:thrown result))))
              parser-error (first replies)]
          (is (car/reply-error? {:eid :carmine.read/parser-error} parser-error))
          (is (identical? failure (ex-cause parser-error)))
          (is (= "tail" (second replies)))
          (is (= before (car/wcar mgr (car/rcmd "CLIENT" "ID")))
            "Ordinary parser failures drain the pipeline and preserve the connection"))))))

(deftest _critical-parser-failures-propagate-and-invalidate
  (with-open [mgr (car/conn-manager
                   {:conn-opts support/test-conn-opts
                    :pool-opts {:max-total 1, :max-idle 1, :test-on-borrow? false}})]
    (doseq [phase phases
            mode [:throw :return]
            failure [(LinkageError. "synthetic critical parser failure")
                     (InterruptedException. "parser interrupted")]]
      (testing (str phase " " mode " " (class failure))
        (let [before (car/wcar mgr (car/rcmd "CLIENT" "ID"))
              result (observe-throw #(pipeline! mgr mode (failing-parser phase failure)))]
          (is (identical? failure (:thrown result)))
          (is (= (instance? InterruptedException failure) (:interrupted? result)))
          (is (= "fresh" (car/wcar mgr (car/echo "fresh")))
            "Unread aggregate elements and the pipeline tail cannot leak to the next borrower")
          (is (not= before (car/wcar mgr (car/rcmd "CLIENT" "ID")))
            "A partial read invalidates the connection even under :error-mode :return"))))))
