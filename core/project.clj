(defproject crow/core "2.4.1-SNAPSHOT"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "_" :upgrade false]
                 [org.clojure/tools.logging "0.5.0"]
                 [clj-http "3.10.0"]
                 [clj-time "_" :upgrade false]
                 [async-connect "0.2.3-SNAPSHOT"]
                 [clojure-msgpack "1.2.1"]
                 [com.shelf/messagepack-framedecoder "1.0-SNAPSHOT"]]
  :plugins [[lein-modules "0.3.11"]]
  :profiles {:dev {}})
