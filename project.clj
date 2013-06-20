(defproject com.taoensso/carmine "2.0.0-alpha1"
  :description "Clojure Redis client & message queue"
  :url "https://github.com/ptaoussanis/carmine"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure         "1.4.0"]
                 [org.clojure/tools.macro     "0.1.2"]
                 [commons-pool/commons-pool   "1.6"]
                 [commons-codec/commons-codec "1.6"]
                 [org.clojure/data.json       "0.2.1"]
                 [expectations                "1.4.43"]
                 [com.taoensso/timbre         "2.1.2"]
                 [com.taoensso/nippy          "2.0.0-beta1"]]
  :profiles {:1.4   {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5   {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :dev   {:dependencies [[ring/ring-core "1.1.0"]]}
             :test  {:dependencies [[ring/ring-core "1.1.0"]]}
             :bench {:dependencies [[org.clojars.tavisrudd/redis-clojure "1.3.1"]
                                    [clj-redis "0.0.12"]
                                    [accession "0.1.1"]]}}
  :aliases {"test-all"    ["with-profile" "test,1.4:test,1.5" "do" "test,"
                           "expectations"]
            "test-auto"   ["with-profile" "test" "autoexpect"]
            "start-dev"   ["with-profile" "dev,test,bench" "repl" ":headless"]
            "start-bench" ["trampoline" "start-dev"]}
  :plugins [[lein-expectations "0.0.7"]
            [lein-autoexpect   "0.2.5"]
            [codox             "0.6.4"]]
  :min-lein-version "2.0.0"
  :warn-on-reflection true)
