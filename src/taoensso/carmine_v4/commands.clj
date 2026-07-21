(ns ^:no-doc taoensso.carmine-v4.commands
  "Generated native Redis command functions for Carmine v4."
  (:require
   [clojure.string :as str]
   [taoensso.encore :as enc]
   [taoensso.truss  :as truss]
   [taoensso.carmine-v4.resp    :as resp]
   [taoensso.carmine-v4.resp.write :as write]
   [taoensso.carmine-v4.cluster :as cluster]))

(defonce ^:private command-spec
  (if-let [edn (enc/slurp-resource "carmine-commands.edn")]
    (enc/read-edn edn)
    (truss/ex-info! "[Carmine] Failed to find command definitions"
      {:eid :carmine.commands/missing-command-spec})))

(defonce ^:private route-spec
  (if-let [edn (enc/slurp-resource "carmine-v4-command-routes.edn")]
    (enc/read-edn edn)
    (truss/ex-info! "[Carmine] Failed to find v4 command routes"
      {:eid :carmine.commands/missing-route-spec})))

(defn- cluster-docstring [cluster-routing]
  (case cluster-routing
    :exact
    (str "Cluster: Key-routed. Routing keys are inferred, and known cross-slot "
      "calls are rejected before I/O.")

    :partial
    (str "Cluster: Partial support. Known keys select the node, but Carmine "
      "cannot validate additional dynamic or incomplete keys before I/O. Redis "
      "requires all keys to use one hash slot.")

    :single-node
    (str "Cluster: Single-node by default. A targetless call uses one available "
      "master node.")

    :unsupported
    "Cluster: Unsupported by Carmine. The command is rejected before I/O."

    :explicit-target
    (str "Cluster: Requires an explicit target. Use [[with-cluster-target]], or "
      "use [[cluster-key]] for a keyed raw command. Targetless calls are "
      "rejected before I/O. `:masters` and `:nodes` may broadcast node-wide "
      "commands.")

    (truss/ex-info! "[Carmine] Unexpected v4 Cluster routing classification"
      {:eid :carmine.commands/invalid-cluster-routing
       :cluster-routing cluster-routing})))

(defn enqueue-command
  "Internal implementation used by generated command functions."
  [command route cluster-routing supports-cluster? encoded-prefix n-prefix-args]
  (resp/ensure-context! command)
  (let [command (write/prepare-command command)
        slot (when (resp/cluster-context?)
               (cluster/command-slot command route))]
    (resp/enqueue-prepared-command! command slot cluster-routing supports-cluster?
      encoded-prefix n-prefix-args)))

(defmacro defcommand [cmd-name spec]
  (let [{:keys [fn-name fn-docstring fn-params-fixed fn-params-more
                req-args-fixed]} spec
        {:keys [key-specs supports-cluster? cluster-routing route-kind]}
        (get route-spec cmd-name)
        route {:kind route-kind, :key-specs key-specs}
        command-parts (str/split cmd-name #" ")
        n-command-parts (count command-parts)
        fn-docstring
        (str fn-docstring "\n\n" (cluster-docstring cluster-routing))
        fn-sym (symbol fn-name)
        encoded-prefix-sym (gensym "encoded-prefix")]
    `(let [~encoded-prefix-sym (write/encode-command-prefix ~command-parts)]
       ~(if fn-params-more
          `(defn ~fn-sym ~fn-docstring
             {:redis-api true, :redis-command ~cmd-name
              :redis-key-specs ~key-specs
              :redis-cluster-routing ~cluster-routing
              :redis-cluster-route-kind ~route-kind
              :supports-cluster? ~supports-cluster?}
             ~`(~fn-params-fixed
                (enqueue-command ~req-args-fixed ~route ~cluster-routing ~supports-cluster?
                  ~encoded-prefix-sym ~n-command-parts))
             ~`(~fn-params-more
                (enqueue-command (into ~req-args-fixed ~'args)
                  ~route ~cluster-routing ~supports-cluster?
                  ~encoded-prefix-sym ~n-command-parts)))
          `(defn ~fn-sym ~fn-docstring
             {:redis-api true, :redis-command ~cmd-name
              :redis-key-specs ~key-specs
              :redis-cluster-routing ~cluster-routing
              :redis-cluster-route-kind ~route-kind
              :supports-cluster? ~supports-cluster?}
             ~fn-params-fixed
             (enqueue-command ~req-args-fixed ~route ~cluster-routing ~supports-cluster?
               ~encoded-prefix-sym ~n-command-parts))))))

(defmacro defcommands []
  `(do ~@(map (fn [[cmd-name spec]] `(defcommand ~cmd-name ~spec)) command-spec)))

(def ^:private cluster-deny
  #{"SELECT" "MOVE" "SWAPDB"
    "MULTI" "EXEC" "DISCARD" "WATCH" "UNWATCH"
    ;; Connection-affine protocol/session state is incompatible with pooled
    ;; per-command Cluster routing.
    "ASKING" "AUTH" "HELLO" "PSYNC" "QUIT" "REPLCONF" "RESET"
    "READONLY" "READWRITE" "SYNC"
    "CLIENT CACHING" "CLIENT NO-EVICT" "CLIENT NO-TOUCH" "CLIENT REPLY"
    "CLIENT SETINFO" "CLIENT SETNAME" "CLIENT TRACKING"
    "MONITOR"
    "SUBSCRIBE" "PSUBSCRIBE" "SSUBSCRIBE"
    "UNSUBSCRIBE" "PUNSUBSCRIBE" "SUNSUBSCRIBE"})

(def ^:private cluster-allow-single-node #{"PING" "ECHO"})

(def ^:private cluster-multi-node-policies
  #{"request_policy:all_shards"
    "request_policy:all_nodes"
    "request_policy:special"})

(defn- recognized-key-spec? [spec]
  (and (contains? #{"index" "keyword"}
         (get-in spec [:begin_search :type]))
       (contains? #{"range" "keynum"}
         (get-in spec [:find_keys :type]))))

(defn- command-route [cmd-name command]
  (let [key-specs       (:key_specs command)
        recognized      (filterv recognized-key-spec? key-specs)
        complete?       (and (seq key-specs)
                          (= (count recognized) (count key-specs))
                          (not-any? :incomplete key-specs))
        request-policy  (some #(when (str/starts-with? % "request_policy:") %)
                          (:hints command))
        migrate?        (= cmd-name "MIGRATE")
        cluster-routing
        (cond
          (contains? cluster-deny cmd-name)                :unsupported
          (contains? cluster-allow-single-node cmd-name)   :single-node
          migrate?                                         :exact
          (contains? cluster-multi-node-policies request-policy) :explicit-target
          complete?                                        :exact
          (seq recognized)                                 :partial
          :else                                            :explicit-target)
        supports-cluster?
        (case cluster-routing
          :unsupported   false
          true)
        effective-key-specs
        (cond
          migrate? nil ; Custom grammar-aware routing handles both valid forms.
          (#{:exact :partial} cluster-routing) recognized
          :else nil)]
    (cond->
      (array-map
        :key-specs effective-key-specs
        :cluster-routing cluster-routing
        :supports-cluster? supports-cluster?)
      migrate? (assoc :route-kind :migrate))))

(defn- generate-command-routes
  "Generates Carmine v4 Cluster routing definitions from the official Redis
  command map."
  [commands]
  (into (sorted-map)
    (map
      (fn [[cmd-name command]]
        (let [cmd-name (name cmd-name)]
          [cmd-name (command-route cmd-name command)])))
    commands))

(defn ^:no-doc update-command-routes!
  "Regenerates the checked-in v4 routing overlay from Redis command JSON.
  Requires the maintainer dev profile with `clojure.data.json`."
  []
  (let [read-json (requiring-resolve 'clojure.data.json/read-str)
        commands (read-json (enc/slurp-resource "redis-commands.json") :key-fn keyword)
        routing (generate-command-routes commands)
        output
        (str "{\n"
          (apply str
            (map (fn [[k v]] (str (pr-str k) " " (pr-str v) "\n")) routing))
          "}")]
    (spit "resources/carmine-v4-command-routes.edn" output)
    (count routing)))
