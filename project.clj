(defproject com.taoensso/carmine "2.16.0"
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
  [[org.clojure/clojure              "1.5.1"]
   [com.taoensso/encore              "2.90.1"]
   [com.taoensso/timbre              "4.8.0"]
   [com.taoensso/nippy               "2.13.0"]
   [org.apache.commons/commons-pool2 "2.4.2"]
   [commons-codec/commons-codec      "1.10"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server"]}
   :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
   :1.6  {:dependencies [[org.clojure/clojure "1.6.0"]]}
   :1.7  {:dependencies [[org.clojure/clojure "1.7.0"]]}
   :1.8  {:dependencies [[org.clojure/clojure "1.8.0"]]}
   :1.9  {:dependencies [[org.clojure/clojure "1.9.0-alpha10"]]}
   :test {:dependencies [;; 2.1.4 has breaking changes
                         [org.clojure/test.check  "0.9.0"]
                         [com.taoensso/faraday    "1.9.0"]
                         [clj-aws-s3              "0.3.10"]
                         [ring/ring-core          "1.5.1"]]}
   :dev
   [:1.9 :test :server-jvm
    {:dependencies [[org.clojure/data.json "0.2.6"]]
     :plugins
     [[lein-ancient "0.6.10"]
      [lein-codox   "0.10.3"]]}]}

  :test-paths ["test" "src"]

  :codox
  {:language :clojure
   :source-uri "https://github.com/ptaoussanis/carmine/blob/master/{filepath}#L{line}"}

  :aliases
  {"test-all"   ["with-profile" "+1.9:+1.8:+1.7:+1.6:+1.5" "test"]
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "start-dev"  ["with-profile" "+dev" "repl" ":headless"]}

  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"})
