(ns taoensso.carmine.commands
  "Define an appropriate function for EVERY Redis command. This is done by
  parsing the official Redis command reference (JSON) which includes up-to-date
  docstrings, argument specs, etc. This awesome approach was adapted from
  labs-redis-clojure."
  {:author "Peter Taoussanis"}
  (:require [clojure.string    :as str]
            [clojure.java.io   :as io]
            [clojure.data.json :as json]
            [taoensso.encore           :as encore]
            [taoensso.carmine.utils    :as utils]
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

(comment ; Debug
  (let [refspec (get-command-reference)]
    (spit "commands.list" ; println
      (with-out-str
        (println (format "%s commands in refspec:\n---" (count (keys refspec))))
        (doseq [rk (sort (keys refspec))]
          (println (format "%s - %s"
                     (str/lower-case (name rk))
                     (args->params-vec (:arguments (get refspec rk))))))))))

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

(def ^:private ^:const num-keyslots 16384)
(defn keyslot
  "Returns the Redis Cluster key slot ℕ∈[0,num-keyslots) for given key arg using
  the CRC16 algorithm, Ref. http://redis.io/topics/cluster-spec Appendix A."
  [x]
  (encore/cond-throw
   (encore/bytes? x) (mod (utils/crc16 x) num-keyslots)
   (string?       x)
   (let [;; Hash only *first* '{<part>}' when present + non-empty:
         tag     (nth (re-find #"\{(.*?)\}" x) 1)
         to-hash (if (and tag (not= tag "")) tag x)]
     (mod (utils/crc16 (.getBytes ^String to-hash "UTF-8"))
          num-keyslots))))

(comment (keyslot "foobar")
         (keyslot "ignore-this{foobar}")
         (time (dotimes [_ 10000] (keyslot "hello")))
         (time (dotimes [_ 10000] (keyslot "hello{world}"))))

;;;;

(defmacro enqueue-request
  "Implementation detail.
  Takes a request like [\"SET\" \"my-key\" \"my-val\"] and adds it to context's
  request queue with relevant metadata from dynamic environment."
  [request
   cluster-key-idx ; Since cmd may have multiple parts, etc.
   ]
  ;; {:pre [(vector? request) (or (nil? cluster-key-idx)
  ;;                              (pos? cluster-key-idx))]}
  `(let [{conn# :conn req-queue# :req-queue} protocol/*context*
         _# (when-not req-queue# (throw protocol/no-context-ex))
         parser#  protocol/*parser*
         request# ~request
         bytestring-req# (mapv protocol/coerce-bs request#)

         cluster-keyslot#
         (if-not (get-in conn# [:spec :cluster]) request#
           (let [cluster-key-idx# ~cluster-key-idx
                 _# (assert (pos? cluster-key-idx#))
                 cluster-key#
                 (let [k-uncoerced# (nth request# cluster-key-idx#)]
                   (if (string? k-uncoerced#) (keyslot k-uncoerced#)
                     (let [k-coerced# (nth bytestring-req# cluster-key-idx#)]
                       (keyslot k-coerced#))))]))

         request# ; User-readable request, nice for debugging
         (with-meta request#
           {:parser parser# ; Parser metadata will be used as req-opts
            :expected-keyslot cluster-keyslot#
            :bytestring-req   bytestring-req#})]

     ;; We could also choose to throw here for non-Cluster commands being used
     ;; in Cluster mode. For the moment choosing to let Redis server reply with
     ;; relevant errors since: 1. There's no command spec info on non/cluster
     ;; keys yet, and 2. The list of supported commands will probably be
     ;; evolving rapidly for the foreseeable future.

     ;; (println "Enqueue request: " request#)
     (swap! req-queue# (fn [[_# q#]] [nil (conj q# request#)]))))

(defmacro defcommand [cmd-name {args :arguments :as refspec}]
  (let [fn-name      (-> cmd-name (str/replace #" " "-") str/lower-case)
        fn-docstring (str cmd-name " "
                       (args->params-docstring args)
                       "\n\n" (:summary refspec) ".\n\n"
                       "Available since: " (:since refspec) ".\n\n"
                       "Time complexity: " (:complexity refspec))

        fn-args   (args->params-vec args)   ; ['key 'value '& 'more]
        cmd-parts (str/split cmd-name #" ") ; ["CONFIG" "SET"]
        req-args  (into cmd-parts fn-args)  ; ["CONFIG" "SET" 'key 'value '& 'more]

        ;; [("CONFIG" "SET" 'key 'value) ('& 'more)]:
        [req-args-main [_ req-args-more]] (split-with #(not= '& %) req-args)
        req-args-main (vec req-args-main)

        ;; We assume for now that cluster key always follows immediately after
        ;; command parts since this seems to hold. We could adjust later for any
        ;; exceptions that may come up:
        cluster-key-idx (count cmd-parts)]

    `(defn ~(symbol fn-name)
       {:doc ~fn-docstring
        :redis-api (or (:since ~refspec) true)}
       ~fn-args
       (let [req-args-main# ~req-args-main
             req-args-more# ~req-args-more
             request# ; ["SET" "my-key" "my-val"]
             (if-not req-args-more# req-args-main#
               (into req-args-main# req-args-more#))]

         (enqueue-request request# ~cluster-key-idx)))))

(defn- get-command-reference
  "Returns parsed JSON official command reference.
  From https://github.com/antirez/redis-doc/blob/master/commands.json"
  [] (-> "commands.json" io/resource io/reader slurp
         (clojure.data.json/read-str :key-fn keyword)))

(defmacro defcommands []
  (let [refspec (get-command-reference)]
    `(do ~@(map (fn [k v] `(defcommand ~(name k) ~v))
             (keys refspec) (vals refspec)))))

(comment
  (def cref (get-command-reference))
  (-> cref keys count)
  (-> cref keys sort)
  (-> cref :SORT :arguments)
  (cref (keyword "SCRIPT EXISTS")))
