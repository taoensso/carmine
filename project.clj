(defproject com.taoensso/carmine "3.2.0"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Clojure Redis client & message queue"
  :url "https://github.com/ptaoussanis/carmine"
  :min-lein-version "2.3.3"

  :license
  {:name "Eclipse Public License 1.0"
   :url  "http://www.eclipse.org/legal/epl-v10.html"}

  :global-vars
  {*warn-on-reflection* true
   *assert*             true
   *unchecked-math*     false #_:warn-on-boxed}

  :dependencies
  [[com.taoensso/encore              "3.62.1"]
   [com.taoensso/timbre              "6.2.1"]
   [com.taoensso/nippy               "3.2.0"]
   [com.taoensso/tufte               "2.5.0"]
   [org.apache.commons/commons-pool2 "2.11.1"]
   [commons-codec/commons-codec      "1.16.0"]]

  :plugins
  [[lein-pprint  "1.3.2"]
   [lein-ancient "0.7.0"]
   [com.taoensso.forks/lein-codox "0.10.9"]]

  :codox
  {:language #{:clojure #_:clojurescript}
   :base-language :clojure}

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :depr       {:jvm-opts ["-Dtaoensso.elide-deprecated=true"]}
   :server-jvm {:jvm-opts ^:replace ["-server"]}

   :provided {:dependencies [[org.clojure/clojure    "1.11.1"]]}
   :c1.11    {:dependencies [[org.clojure/clojure    "1.11.1"]]}
   :c1.10    {:dependencies [[org.clojure/clojure    "1.10.1"]]}
   :c1.9     {:dependencies [[org.clojure/clojure    "1.9.0"]]}
   :test     {:dependencies [[org.clojure/test.check "1.1.1"]]}
   :dev
   [:c1.11 :test :server-jvm :depr
    {:dependencies
     [[org.clojure/data.json "2.4.0"]
      [com.taoensso/faraday  "1.12.0"]
      [clj-aws-s3            "0.3.10"]
      [ring/ring-core        "1.10.0"]]}]

   :graal-tests
   {:dependencies [[org.clojure/clojure "1.11.1"]
                   [com.github.clj-easy/graal-build-time "0.1.4"]]
    :main taoensso.graal-tests
    :aot [taoensso.graal-tests]
    :uberjar-name "graal-tests.jar"}}

  :test-paths ["test" #_"src"]

  :aliases
  {"start-dev"  ["with-profile" "+dev" "repl" ":headless"]
   ;; "build-once" ["do" ["clean"] "cljsbuild" "once"]
   "deploy-lib" ["do" #_["build-once"] ["deploy" "clojars"] ["install"]]

   "test-clj"   ["with-profile" "+c1.11:+c1.10:+c1.9" "test"]
   "test-all"   ["do" ["clean"] "test-clj" #_"test-cljs"]}

  :repositories
  {"sonatype-oss-public"
   "https://oss.sonatype.org/content/groups/public/"})
