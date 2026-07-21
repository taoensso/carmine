(ns taoensso.carmine-v4.tests.command-artifacts
  "Reproducibility checks for pinned and generated Redis command artifacts."
  (:require
   [clojure.data.json :as json]
   [clojure.test :refer [deftest is]]
   [taoensso.encore :as enc]
   [taoensso.carmine.commands :as commands]
   [taoensso.carmine-v4.commands :as v4-commands]))

(defn- resource-string [name]
  (or (enc/slurp-resource name)
    (throw (ex-info "Missing command artifact resource" {:resource name}))))

(defn- redis-command-map []
  (json/read-str (resource-string "redis-commands.json") :key-fn keyword))

(deftest _command-artifacts-are-reproducible
  (let [redis-commands (redis-command-map)
        expected-command-spec (#'commands/generate-command-spec redis-commands)
        expected-routes (#'v4-commands/generate-command-routes redis-commands)
        actual-command-spec (enc/read-edn (resource-string "carmine-commands.edn"))
        actual-routes (enc/read-edn (resource-string "carmine-v4-command-routes.edn"))
        expected-command-edn (#'commands/artifact-edn expected-command-spec)
        expected-routes-edn (#'commands/artifact-edn expected-routes)]
    (is (= actual-command-spec expected-command-spec)
      "The public command API is derived from the pinned Redis JSON")
    (is (= actual-routes expected-routes)
      "The Cluster routing overlay is derived from the pinned Redis JSON")
    (is (= (resource-string "carmine-commands.edn") expected-command-edn)
      "The command API artifact has canonical reproducible bytes")
    (is (= (resource-string "carmine-v4-command-routes.edn") expected-routes-edn)
      "The Cluster routing artifact has canonical reproducible bytes")))
