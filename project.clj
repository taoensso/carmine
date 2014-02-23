(defproject com.taoensso/carmine "3.0.0-SNAPSHOT"
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
  [[org.clojure/clojure         "1.4.0"]
   [org.clojure/tools.macro     "0.1.5"]
   [commons-pool/commons-pool   "1.6"]
   [commons-codec/commons-codec "1.9"]
   [org.clojure/data.json       "0.2.4"]
   [com.taoensso/timbre         "3.0.0"]
   [com.taoensso/nippy          "2.6.0-alpha3"]]

  :test-paths ["test" "src"]
  :profiles
  {:build {:hooks ^:replace []} ; Workaround to avoid :dev hooks during deploy
   :1.5  {:dependencies [[org.clojure/clojure "1.5.1"]]}
   :1.6  {:dependencies [[org.clojure/clojure "1.6.0-beta1"]]}
   :test {:dependencies [[expectations            "1.4.56"]
                         [reiddraper/simple-check "0.5.6"]
                         [clj-aws-s3              "0.3.8"]]
          :plugins [[lein-expectations "0.0.8"]
                    [lein-autoexpect   "1.2.2"]]}
   :dev
   [:1.6 :test
    {:jvm-opts ^:replace ["-server"]
     :hooks []
     :dependencies [[ring/ring-core       "1.2.1"]
                    [com.taoensso/faraday "1.0.2"]]
     :plugins []}]}

  :plugins [[lein-ancient "0.5.4"]
            [codox        "0.6.7"]]

  ;; :codox {:sources ["target/classes"]} ; For use with cljx
  :aliases
  {"test-all"   ["with-profile" "+test:+1.5,+test:+1.6,+test" "expectations"]
   "test-auto"  ["with-profile" "+test" "autoexpect"]
   "start-dev"  ["with-profile" "+dev" "repl" ":headless"]
   "codox"      ["with-profile" "+test" "doc"]
   "deploy-lib" ["with-profile" "+dev,+build" "do" "deploy" "clojars," "install"]}

  :repositories
  {"sonatype"
   {:url "http://oss.sonatype.org/content/repositories/releases"
    :snapshots false
    :releases {:checksum :fail}}
   "sonatype-snapshots"
   {:url "http://oss.sonatype.org/content/repositories/snapshots"
    :snapshots true
    :releases {:checksum :fail :update :always}}})
