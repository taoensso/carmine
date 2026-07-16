(defproject com.taoensso/carmine "3.5.0"
  :author "Peter Taoussanis <https://www.taoensso.com>"
  :description "Redis client + message queue for Clojure"
  :url "https://github.com/taoensso/carmine"

  :license
  {:name "Eclipse Public License - v 1.0"
   :url  "https://www.eclipse.org/legal/epl-v10.html"}

  :test-paths ["test" #_"src"]

  :dependencies
  [[com.taoensso/encore              "3.169.1"]
   [com.taoensso/nippy               "3.6.2"]
   [com.taoensso/trove               "1.1.0"]
   ;; v2.13.x changes `GenericObjectPool.invalidateObject` (POOL-424)
   ;; in a way that can mask errors (POOL-431); revisit after:
   ;; https://github.com/apache/commons-pool/pull/454
   [org.apache.commons/commons-pool2 "2.12.1"]
   [commons-codec/commons-codec      "1.22.0"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :provided {:dependencies [[org.clojure/clojure "1.12.5"]]}
   :c1.13    {:dependencies [[org.clojure/clojure "1.13.0-alpha4"]]}
   :c1.12    {:dependencies [[org.clojure/clojure "1.12.5"]]}
   :c1.11    {:dependencies [[org.clojure/clojure "1.11.4"]]}
   :c1.10    {:dependencies [[org.clojure/clojure "1.10.3"]]}

   :graal-tests
   {:source-paths ["test"]
    :main taoensso.graal-tests
    :aot [taoensso.graal-tests]
    :uberjar-name "graal-tests.jar"
    :dependencies
    [[org.clojure/clojure                  "1.11.4"]
     [com.github.clj-easy/graal-build-time "1.0.6"]]}

   :dev
   {;; :jvm-opts ["-server" "-Dtaoensso.elide-deprecated=true"]
    :global-vars
    {*warn-on-reflection* true
     *assert*             true
     *unchecked-math*     false #_:warn-on-boxed}

    ;; clj-aws-s3's legacy AWS SDK declares joda-time with an open version range.
    :managed-dependencies
    [[joda-time "2.14.2"]]

    :dependencies
    [[org.clojure/test.check "1.1.3"]
     [org.clojure/data.json  "2.5.2"]
     [com.taoensso/faraday   "1.12.3"]
     [clj-aws-s3             "0.3.10"]
     [ring/ring-core         "1.15.5"]]

    :plugins
    [[lein-pprint  "1.3.2"]
     [lein-ancient "1.0.0"]]}}

  :test-selectors
  {:v3 (fn [{:keys [ns]} & _] (.startsWith (str ns) "taoensso.carmine."))
   :v4 (fn [{:keys [ns]} & _] (.startsWith (str ns) "taoensso.carmine-v4."))}

  :aliases
  {"start-dev"     ["with-profile" "+dev" "repl" ":headless"]
   ;; "build-once" ["do" ["clean"] ["cljsbuild" "once"]]
   "deploy-lib"    ["do" #_["build-once"] ["deploy" "clojars"] ["install"]]

   "test-v3"      ["with-profile" "+c1.13:+c1.12:+c1.11:+c1.10" "test" ":v3"]
   "test-v4"      ["with-profile" "+c1.13:+c1.12:+c1.11:+c1.10" "test" ":v4"]
   "test-clj"     ["with-profile" "+c1.13:+c1.12:+c1.11:+c1.10" "test"]
   ;; "test-cljs" ["with-profile" "+c1.12" "cljsbuild"         "test"]
   "test-all"     ["do" ["clean"] ["test-clj"] #_["test-cljs"]]})
