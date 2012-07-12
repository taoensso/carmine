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

(defmacro time-ns
  "Returns number of nanoseconds it takes to execute body."
  [& body]
  `(let [t0# (System/nanoTime)]
     ~@body
     (- (System/nanoTime) t0#)))

(defmacro bench
  "Repeatedly executes form and returns time taken to complete execution."
  [num-laps form & {:keys [warmup-laps num-threads as-ms?]
                :or   {as-ms? true}}]
  `(try (when ~warmup-laps (dotimes [_# ~warmup-laps] ~form))
        (let [nanosecs#
              (if-not ~num-threads
                (time-ns (dotimes [_# ~num-laps] ~form))
                (let [laps-per-thread# (int (/ ~num-laps ~num-threads))]
                  (time-ns
                   (->> (fn [] (future (dotimes [_# laps-per-thread#] ~form)))
                        (repeatedly ~num-threads)
                        doall
                        (map deref)
                        dorun))))]
          (if ~as-ms? (Math/round (/ nanosecs# 1000000.0)) nanosecs#))
        (catch Exception e# (str "DNF: " (.getMessage e#)))))