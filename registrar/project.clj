(defproject crow/registrar "2.3-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [async-connect "0.2.0-SNAPSHOT"]
                 [clj-time "0.14.2"]
                 [crow/core "2.3-SNAPSHOT"]]
  :profiles {:dev {:resource-paths ["resources-dev"]}
             :logging {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
                       :resource-paths ["resources-logging"]}
             :uberjar {:aot :all}}
  :main crow.registrar
  )
