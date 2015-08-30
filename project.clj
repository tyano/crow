(defproject crow "1.0-SNAPSHOT"
  :description "Crow is a library for collaborating distributed service implemented by Clojure."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [aleph "0.4.0"]
                 [clojure-msgpack "1.1.1"]
                 [clj-time "0.10.0"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/tools.logging "0.3.1"]
                 [slingshot "0.12.2"]
                 [com.shelf/messagepack-framedecoder "1.0-SNAPSHOT"]]
  :plugins [[lein-midje "3.1.3"]]
  :repositories {"javelindev-snapshots" "http://javelindev.jp/repository/snapshots"}
  :profiles {:dev {:dependencies [[midje "1.7.0"]
                                  [ch.qos.logback/logback-classic "1.1.3"]]
                   :resource-paths ["resources-dev"]
                   :jvm-opts ["-Djava.net.preferIPv4Stack=true"]}}
  :aot [crow.registrar-source
        crow.registrar
        crow.protocol
        crow.discovery
        crow.marshaller]
  :main crow.registrar)

(cemerick.pomegranate.aether/register-wagon-factory!
   "scp" #(let [c (resolve 'org.apache.maven.wagon.providers.ssh.external.ScpExternalWagon)]
                      (clojure.lang.Reflector/invokeConstructor c (into-array []))))
