(defproject crow/service "2.3-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [integrant "0.6.3"]
                 [clj-time "0.14.2"]
                 [ns-tracker "0.3.1"]
                 [async-connect "0.2.0-SNAPSHOT"]
                 [crow/core "2.3-SNAPSHOT"]]
  :profiles {:dev {:resource-paths ["resources-dev"]}
             :uberjar {:aot :all}})
