(defproject crow/client "2.3-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "_"]
                 [crow/core "_"]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:dev {:resource-paths ["resources-dev"]}
             :uberjar {:aot :all}})
