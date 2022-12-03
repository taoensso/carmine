(defproject com.taoensso/carmine "3.2.0"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Clojure Redis client & message queue"
  :url "https://github.com/ptaoussanis/carmine"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"
            :distribution :repo
            :comments "Same as Clojure"}
  :min-lein-version "2.3.3"
  :global-vars
  {*warn-on-reflection* true
   *assert*             true
   *unchecked-math*     false #_:warn-on-boxed}

  :dependencies
  [[com.taoensso/encore              "3.49.0"]
   [com.taoensso/timbre              "6.1.0"]
   [com.taoensso/nippy               "3.2.0"]
   [com.taoensso/tukey               "0.7.0"]
   [org.apache.commons/commons-pool2 "2.11.1"]
   [commons-codec/commons-codec      "1.15"]]

  :plugins
  [[lein-pprint  "1.3.2"]
   [lein-ancient "0.7.0"]
   [lein-codox   "0.10.8"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server"]}
   :provided {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.11    {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.10    {:dependencies [[org.clojure/clojure "1.10.1"]]}
   :c1.9     {:dependencies [[org.clojure/clojure "1.9.0"]]}

   :depr     {:jvm-opts ["-Dtaoensso.elide-deprecated=true"]}
   :test     {:dependencies [[org.clojure/test.check "1.1.1"]]}
   :dev
   [:c1.11 :test :server-jvm :depr
    {:dependencies
     [[org.clojure/data.json "2.4.0"]
      [com.taoensso/faraday  "1.11.4"]
      [clj-aws-s3            "0.3.10"]
      [ring/ring-core        "1.9.6"]]}]}

  :test-paths ["test" #_"src"]

  :aliases
  {"start-dev"  ["with-profile" "+dev" "repl" ":headless"]
   "deploy-lib" ["do" #_["build-once"] ["deploy" "clojars"] ["install"]]

   "test-all"
   ["do" ["clean"]
    "with-profile" "+c1.11:+c1.10:+c1.9" "test"]}

  :repositories
  {"sonatype-oss-public"
   "https://oss.sonatype.org/content/groups/public/"})
