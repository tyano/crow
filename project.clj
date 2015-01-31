(defproject crow "0.1.0-SNAPSHOT"
  :description "Crow is a library for collaborating distributed service implemented by Clojure."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [aleph "0.4.0-beta1"]
                 [clojure-msgpack "0.1.1-SNAPSHOT"]
                 [clj-time "0.9.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]]
  :plugins [[lein-midje "3.1.3"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]}}
  :aot [crow.registrar-source])
