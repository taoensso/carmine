(defproject carmine "0.8.2-SNAPSHOT"
  :description "Deliberately simple, high-performance Redis (2.0+) client for Clojure."
  :url "https://github.com/ptaoussanis/carmine"
  :license {:name "Eclipse Public License"}
  :dependencies [[org.clojure/clojure           "1.3.0"]
                 [commons-pool/commons-pool     "1.6"]
                 [commons-codec/commons-codec   "1.6"]
                 [org.clojure/data.json         "0.1.2"
                  :exclusions [org.clojure/clojure]]]
  :profiles {:1.3   {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4   {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5   {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :dev   {:dependencies [[ring/ring-core  "1.1.0"]]}
             :bench {:dependencies [[org.clojars.tavisrudd/redis-clojure "1.3.1"]
                                    [clj-redis  "0.0.12"]
                                    [accession  "0.1.1"]
                                    #_[labs-redis "0.1.0-SNAPSHOT"]]}}
  :repositories {"sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}}
  :aliases {"all" ["with-profile" "1.3:1.4:1.5"]}
  :min-lein-version "2.0.0"
  :warn-on-reflection true)
