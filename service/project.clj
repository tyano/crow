(defproject crow/service "2.5.1-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "_" :upgrade false]
                 [integrant "0.7.0"]
                 [clj-time "_" :upgrade false]
                 [ns-tracker "0.4.0"]
                 [crow/core "_" :upgrade false]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:dev {:resource-paths ["resources-dev"]}})
