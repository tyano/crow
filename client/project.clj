(defproject crow/client "2.3-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [async-connect "0.2.0-SNAPSHOT"]
                 [crow/core "2.3-SNAPSHOT"]]
  :profiles {:dev {:resource-paths ["resources-dev"]}
             :uberjar {:aot :all}})
