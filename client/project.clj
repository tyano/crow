(defproject crow/client "2.4.1-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "_" :upgrade false]
                 [crow/core "_" :upgrade false]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:dev {:resource-paths ["resources-dev"]}})
