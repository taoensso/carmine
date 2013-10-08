(ns taoensso.carmine.commands
  "Define an appropriate function for EVERY Redis command. This is done by
  parsing the official Redis command reference (JSON) which includes up-to-date
  docstrings, argument specs, etc. This awesome approach was adapted from
  labs-redis-clojure."
  {:author "Peter Taoussanis"}
  (:require [clojure.java.io   :as io]
            [clojure.string    :as str]
            [clojure.data.json :as json]
            [taoensso.carmine.protocol :as protocol]))

(defn- args->params-vec
  "Parses refspec argument map into simple defn-style parameter vector:
  '[key value & more], etc."
  [args]
  (let [num-non-optional (count (take-while #(not (:optional %)) args))
        num-non-multiple (count (take-while #(not (:multiple %)) args))

        ;; Stop explicit naming on the 1st optional arg (exclusive) or 1st
        ;; multiple arg (inclusive)
        num-fixed        (min num-non-optional (inc num-non-multiple))

        fixed-args       (->> args (take num-fixed)
                              (map :name) flatten (map symbol) vec)
        has-more? (seq (filter #(or (:optional %) (:multiple %)) args))]
    (if has-more? (conj fixed-args '& 'args) fixed-args)))

(defn- args->params-docstring
  "Parses refspec argument map into Redis reference-doc-style explanatory
  string: \"BRPOP key [key ...] timeout\", etc."
  [args]
  (let [parse
        #(let [{:keys [command type name enum multiple optional]} %
               name (if (and (coll? name) (not (next name))) (first name) name)
               s (cond command (str command " "
                                    (cond enum         (str/join "|" enum)
                                          (coll? name) (str/join " " name)
                                          :else name))
                       enum (str/join "|" enum)
                       :else name)
               s (if multiple (str s " [" s " ...]") s)
               s (if optional (str "[" s "]") s)]
           s)]
    (str/join " " (map parse args))))

(defmacro defcommand
  "Actually defines an appropriate function for Redis command given its name in
  reference (\"CONFIG SET\") and its refspec.

  Defined function will require a *context* binding to run."
  [command-name {args :arguments :as refspec} debug-mode?]
  (let [fn-name (-> command-name (str/replace #" " "-") str/lower-case)
        fn-docstring (str command-name " "
                          (args->params-docstring args)
                          "\n\n" (:summary refspec) ".\n\n"
                          "Available since: " (:since refspec) ".\n\n"
                          "Time complexity: " (:complexity refspec))
        fn-params      (args->params-vec args)
        request-params (into (str/split command-name #" ")
                             fn-params)
        [ps [_ varps]] (split-with #(not= '& %) request-params)
        ps             (vec ps)]
    (if debug-mode?
      `(println ~fn-name ":" \" ~(args->params-docstring args) \"
                "->" ~(str fn-params))
      (if-not varps
        `(defn ~(symbol fn-name)
           {:doc ~fn-docstring
            :redis-api true}
           ~fn-params
           (protocol/send-request ~ps))
        `(defn ~(symbol fn-name)
           {:doc ~fn-docstring
            :redis-api true}
           ~fn-params
           (protocol/send-request (into ~ps ~varps)))))))

(defn- get-command-reference
  "Returns parsed JSON official command reference.
  From https://github.com/antirez/redis-doc/blob/master/commands.json"
  []
  (-> "commands.json" io/resource io/reader slurp
      (clojure.data.json/read-str :key-fn keyword)))

(defmacro defcommands
  "Defines an appropriate function for every command in reference. If debug?
  then only PRINTS information about functions that would be defined."
  ([] `(defcommands false))
  ([debug-mode?]
     (let [ref (get-command-reference)]
       `(do ~@(map (fn [k v] `(defcommand ~(name k) ~v ~debug-mode?))
                   (keys ref) (vals ref))))))

(comment
  (defcommands true) ; Debug
  (def cref (get-command-reference))
  (-> cref keys count)
  (-> cref keys sort)
  (-> cref :SORT :arguments)
  (cref (keyword "SCRIPT EXISTS")))
