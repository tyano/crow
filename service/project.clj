(defproject crow/service "2.3-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "_"]
                 [integrant "0.6.3"]
                 [clj-time "_"]
                 [ns-tracker "0.3.1"]
                 [crow/core "_"]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:dev {:resource-paths ["resources-dev"]}
             :uberjar {:aot :all}})
