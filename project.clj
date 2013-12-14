(defproject org.elasticsearch/stream2es "1.0-SNAPSHOT"
  :description "Index streams into ES."
  :url "http://github.com/elasticsearch/elasticsearch/stream2es"
  :license {:name "Apache 2"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :resource-paths ["etc" "resources"]
  :dependencies [[cheshire "5.0.1"]
                 [clj-http "0.6.3"]
                 [org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.cli "0.2.2"]
                 [slingshot "0.10.3"]
                 [org.tukaani/xz "1.3"]
                 [log4j "1.2.17"]
                 [criterium "0.4.2"]]
  )
