(ns taoensso.carmine.utils
  {:author "Peter Taoussanis"})

(defmacro declare-remote
  "Declares the given ns-qualified names. Useful for circular dependencies."
  [& names]
  (let [orig-ns (str *ns*)]
    `(do ~@(map (fn [n] (let [ns (namespace n)
                             v  (name n)]
                         `(do (in-ns '~(symbol ns))
                              (declare ~(symbol v))))) names)
         (in-ns '~(symbol orig-ns)))))

(defmacro case-eval
  "Like `case` but evaluates test constants for their compile-time value."
  [e & clauses]
  (let [;; Don't evaluate default expression!
        default (when (odd? (count clauses)) (last clauses))
        clauses (if default (butlast clauses) clauses)]
    `(case ~e
       ~@(map-indexed (fn [i# form#] (if (even? i#) (eval form#) form#))
                      clauses)
       ~(when default default))))