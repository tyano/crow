(defproject crow/registrar "2.4.1-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "_" :upgrade false]
                 [clj-time "_" :upgrade false]
                 [crow/core "_" :upgrade false]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:dev {:resource-paths ["resources-dev"]}
             :logging {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
                       :resource-paths ["resources-logging"]}}
  :main crow.registrar
  )
