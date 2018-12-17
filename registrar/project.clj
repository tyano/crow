(defproject crow/registrar "2.4-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "_"]
                 [clj-time "_"]
                 [crow/core "_"]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:dev {:resource-paths ["resources-dev"]}
             :logging {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
                       :resource-paths ["resources-logging"]}}
  :main crow.registrar
  )
