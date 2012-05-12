(defproject carmine "0.7.0-SNAPSHOT"
  :description "Deliberately simple, high-performance Redis (2.0+) client for Clojure."
  :url "https://github.com/ptaoussanis/carmine"
  :license {:name "Eclipse Public License"}
  :dependencies [[commons-pool/commons-pool "1.6"]]
  :profiles {:1.3   {:dependencies [[org.clojure/clojure "1.3.0"]]}
             :1.4   {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5   {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :dev   {:dependencies [[marginalia      "0.7.0"]]
                     :plugins      [[lein-swank      "1.4.4"]
                                    [lein-marginalia "0.7.0"]
                                    [lein-clojars    "0.8.0"]]}
             :bench {:dependencies [[org.clojars.tavisrudd/redis-clojure "1.3.1"]
                                    [clj-redis "0.0.12"]
                                    [accession "0.1.1"]]}}
  :repositories {"sonatype" {:url "http://oss.sonatype.org/content/repositories/releases"
                             :snapshots false
                             :releases {:checksum :fail :update :always}}
                 "sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                                                      :releases {:checksum :fail :update :always}}}
  :aliases {"all" ["with-profile" "1.3:1.4:1.5"]}
  :min-lein-version "2.0.0"
  :warn-on-reflection true)