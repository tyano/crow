(defproject crow/core "2.3-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [org.clojure/tools.logging "0.4.0"]
                 [clj-http "3.7.0"]
                 [clj-time "0.14.2"]
                 [async-connect "0.2.0-SNAPSHOT"]
                 [clojure-msgpack "1.2.1"]
                 [com.shelf/messagepack-framedecoder "1.0-SNAPSHOT"]]

  :profiles {:dev {}
             :uberjar {:aot :all}})
