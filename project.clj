(defproject com.taoensso/carmine "3.3.2"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Redis client + message queue for Clojure"
  :url "https://github.com/taoensso/carmine"

  :license
  {:name "Eclipse Public License - v 1.0"
   :url  "https://www.eclipse.org/legal/epl-v10.html"}

  :dependencies
  [[com.taoensso/encore              "3.68.0"]
   [com.taoensso/nippy               "3.3.0"]
   [com.taoensso/timbre              "6.3.1"]
   [com.taoensso/tufte               "2.6.3"]
   [org.apache.commons/commons-pool2 "2.12.0"]
   [commons-codec/commons-codec      "1.16.0"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :provided {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.11    {:dependencies [[org.clojure/clojure "1.11.1"]]}
   :c1.10    {:dependencies [[org.clojure/clojure "1.10.1"]]}
   :c1.9     {:dependencies [[org.clojure/clojure "1.9.0"]]}

   :test
   {:jvm-opts ["-Dtaoensso.elide-deprecated=true"]
    :global-vars
    {*warn-on-reflection* true
     *assert*             true
     *unchecked-math*     false #_:warn-on-boxed}

    :dependencies
    [[org.clojure/test.check "1.1.1"]]}

   :graal-tests
   {:dependencies [[org.clojure/clojure "1.11.1"]
                   [com.github.clj-easy/graal-build-time "1.0.5"]]
    :main taoensso.graal-tests
    :aot [taoensso.graal-tests]
    :uberjar-name "graal-tests.jar"}

   :dev
   [:c1.11 :test
    {:jvm-opts ["-server"]
     :dependencies
     [[org.clojure/data.json "2.4.0"]
      [com.taoensso/faraday  "1.12.0"]
      [clj-aws-s3            "0.3.10"]
      [ring/ring-core        "1.10.0"]]

     :plugins
     [[lein-pprint  "1.3.2"]
      [lein-ancient "0.7.0"]
      [com.taoensso.forks/lein-codox "0.10.10"]]

     :codox
     {:language #{:clojure #_:clojurescript}
      :base-language :clojure}}]}

  :test-paths ["test" #_"src"]

  :aliases
  {"start-dev"     ["with-profile" "+dev" "repl" ":headless"]
   ;; "build-once" ["do" ["clean"] ["cljsbuild" "once"]]
   "deploy-lib"    ["do" #_["build-once"] ["deploy" "clojars"] ["install"]]

   "test-clj"     ["with-profile" "+c1.11:+c1.10:+c1.9" "test"]
   ;; "test-cljs" ["with-profile" "+test" "cljsbuild"   "test"]
   "test-all"     ["do" ["clean"] ["test-clj"] #_["test-cljs"]]})
