(defproject com.taoensso/carmine "0.11.3"
  :description "Simple, high-performance Redis (2.0+) client for Clojure."
  :url "https://github.com/ptaoussanis/carmine"
  :license {:name "Eclipse Public License"}
  :dependencies [[org.clojure/clojure         "1.3.0"]
                 [commons-pool/commons-pool   "1.6"]
                 [commons-codec/commons-codec "1.6"]
                 [org.clojure/data.json       "0.1.2"]
                 [com.taoensso/timbre         "0.8.0"]
                 [com.taoensso/nippy          "0.10.4"]]
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
