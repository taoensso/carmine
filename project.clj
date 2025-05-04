(defproject com.taoensso/carmine "3.5.0-SNAPSHOT"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Redis client + message queue for Clojure"
  :url "https://github.com/taoensso/carmine"

  :license
  {:name "Eclipse Public License - v 1.0"
   :url  "https://www.eclipse.org/legal/epl-v10.html"}

  :test-paths ["test" #_"src"]

  :dependencies
  [[com.taoensso/encore              "3.145.0"]
   [com.taoensso/nippy               #_"3.5.0" "3.6.0-alpha1"]
   [com.taoensso/timbre              "6.7.0"]
   [org.apache.commons/commons-pool2 "2.12.1"]
   [commons-codec/commons-codec      "1.18.0"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :provided {:dependencies [[org.clojure/clojure "1.12.0"]]}
   :c1.12    {:dependencies [[org.clojure/clojure "1.12.0"]]}
   :c1.11    {:dependencies [[org.clojure/clojure "1.11.4"]]}
   :c1.10    {:dependencies [[org.clojure/clojure "1.10.3"]]}

   :graal-tests
   {:source-paths ["test"]
    :main taoensso.graal-tests
    :aot [taoensso.graal-tests]
    :uberjar-name "graal-tests.jar"
    :dependencies
    [[org.clojure/clojure                  "1.11.1"]
     [com.github.clj-easy/graal-build-time "1.0.5"]]}

   :dev
   {:jvm-opts ["-server" "-Dtaoensso.elide-deprecated=true"]
    :global-vars
    {*warn-on-reflection* true
     *assert*             true
     *unchecked-math*     false #_:warn-on-boxed}

    :dependencies
    [[org.clojure/test.check "1.1.1"]
     [org.clojure/data.json  "2.5.1"]
     [com.taoensso/faraday   "1.12.3"]
     [clj-aws-s3             "0.3.10"]
     [ring/ring-core         "1.14.1"]]

    :plugins
    [[lein-pprint  "1.3.2"]
     [lein-ancient "0.7.0"]]}}

  :aliases
  {"start-dev"     ["with-profile" "+dev" "repl" ":headless"]
   ;; "build-once" ["do" ["clean"] ["cljsbuild" "once"]]
   "deploy-lib"    ["do" #_["build-once"] ["deploy" "clojars"] ["install"]]

   "test-clj"     ["with-profile" "+c1.12:+c1.11:+c1.10" "test"]
   ;; "test-cljs" ["with-profile" "+c1.12" "cljsbuild"   "test"]
   "test-all"     ["do" ["clean"] ["test-clj"] #_["test-cljs"]]})
