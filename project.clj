(defproject com.taoensso/carmine "2.21.0-RC1"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Clojure Redis client & message queue"
  :url "https://github.com/ptaoussanis/carmine"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "Same as Clojure"}
  :min-lein-version "2.3.3"
  :global-vars {*warn-on-reflection* true
                *assert* true}

  :dependencies
  [[com.taoensso/encore              "3.1.0"]
   [com.taoensso/timbre              "5.0.0"]
   [com.taoensso/nippy               "3.0.0"]
   [org.apache.commons/commons-pool2 "2.8.1"]
   [commons-codec/commons-codec      "1.15"]]

  :plugins
  [[lein-pprint  "1.3.2"]
   [lein-ancient "0.6.15"]
   [lein-codox   "0.10.7"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server"]}
   :provided {:dependencies [[org.clojure/clojure    "1.7.0"]]}
   :1.7      {:dependencies [[org.clojure/clojure    "1.7.0"]]}
   :1.8      {:dependencies [[org.clojure/clojure    "1.8.0"]]}
   :1.9      {:dependencies [[org.clojure/clojure    "1.9.0"]]}
   :1.10     {:dependencies [[org.clojure/clojure    "1.10.1"]]}
   :test     {:dependencies [[org.clojure/test.check "1.1.0"]]}
   :depr     {:jvm-opts ["-Dtaoensso.elide-deprecated=true"]}
   :dev
   [:1.10 :test :server-jvm :depr
    {:dependencies
     [[org.clojure/data.json "1.0.0"]
      [com.taoensso/faraday  "1.9.0"]
      [clj-aws-s3            "0.3.10"]
      [ring/ring-core        "1.8.1"]]}]}

  :test-paths ["test" "src"]

  :aliases
  {"start-dev"  ["with-profile" "+dev" "repl" ":headless"]
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "test-all"   ["with-profile" "+1.10:+1.9:+1.8:+1.7" "test"]}

  :repositories
  {"sonatype-oss-public"
   "https://oss.sonatype.org/content/groups/public/"})
