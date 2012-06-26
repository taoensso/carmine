(ns carmine.utils "Misc shared stuff"
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