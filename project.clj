(defproject com.taoensso/carmine "2.12.2"
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
   [com.taoensso/encore              "2.50.0"]
   [com.taoensso/timbre              "4.3.1"]
   [com.taoensso/nippy               "2.11.1"]
   [org.apache.commons/commons-pool2 "2.4.2"]
   [commons-codec/commons-codec      "1.10"]
   [org.clojure/data.json            "0.2.6"]]

  :profiles
  {;; :default [:base :system :user :provided :dev]
   :server-jvm {:jvm-opts ^:replace ["-server"]}
   :1.5  {:dependencies [[org.clojure/clojure     "1.5.1"]]}
   :1.6  {:dependencies [[org.clojure/clojure     "1.6.0"]]}
   :1.7  {:dependencies [[org.clojure/clojure     "1.7.0"]]}
   :1.8  {:dependencies [[org.clojure/clojure     "1.8.0"]]}
   :test {:dependencies [;; 2.1.4 has breaking changes
                         ;; TODO Migrate expectations->clojure.test
                         [expectations            "2.1.3"]
                         [org.clojure/test.check  "0.9.0"]
                         [com.taoensso/faraday    "1.8.0"]
                         [clj-aws-s3              "0.3.10"]
                         [ring/ring-core          "1.4.0"]]
          :plugins [[lein-expectations "0.0.8"]
                    [lein-autoexpect   "1.7.0"]]}
   :dev
   [:1.7 :test
    {:plugins [[lein-ancient "0.6.4"]
               [lein-codox   "0.9.4"]]}]}

  :test-paths ["test" "src"]

  :codox
  {:language :clojure
   :source-uri "https://github.com/ptaoussanis/carmine/blob/master/{filepath}#L{line}"}

  :aliases
  {"test-all"   ["with-profile" "+1.5:+1.6:+1.7:+1.8" "expectations"]
   "test-auto"  ["with-profile" "+test" "autoexpect"]
   "deploy-lib" ["do" "deploy" "clojars," "install"]
   "start-dev"  ["with-profile" "+server-jvm" "repl" ":headless"]}

  :repositories {"sonatype-oss-public"
                 "https://oss.sonatype.org/content/groups/public/"})
