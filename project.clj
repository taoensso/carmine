(defproject com.taoensso/carmine "1.7.0-beta5"
  :description "Clojure Redis client & message queue"
  :url "https://github.com/ptaoussanis/carmine"
  :license {:name "Eclipse Public License"
            :url  "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure         "1.3.0"]
                 [org.clojure/tools.macro     "0.1.1"]
                 [commons-pool/commons-pool   "1.6"]
                 [commons-codec/commons-codec "1.6"]
                 [org.clojure/data.json       "0.2.1"]
                 [com.taoensso/timbre         "1.5.3"]
                 [com.taoensso/nippy          "1.2.0"]]
  :profiles {:1.3   {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4   {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5   {:dependencies [[org.clojure/clojure "1.5.0-alpha3"]]}
             :dev   {:dependencies [[ring/ring-core      "1.1.0"]]}
             :test  {:dependencies [[ring/ring-core      "1.1.0"]]}
             :bench {:dependencies [[org.clojars.tavisrudd/redis-clojure "1.3.1"]
                                    [clj-redis "0.0.12"]
                                    [accession "0.1.1"]]}}

  :aliases {"test-all" ["with-profile" "test,1.3:test,1.4:test,1.5" "test"]}
  :min-lein-version "2.0.0"
  :warn-on-reflection true)
