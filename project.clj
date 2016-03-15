(defproject crow "1.0-SNAPSHOT"
  :description "Crow is a library for collaborating distributed service implemented by Clojure."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [aleph "0.4.1-beta2"]
                 [clojure-msgpack "1.1.2"]
                 [clj-time "0.11.0"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/tools.logging "0.3.1"]
                 [slingshot "0.12.2"]
                 [com.shelf/messagepack-framedecoder "1.0-SNAPSHOT"]
                 [prismatic/schema "1.0.4"]
                 [ns-tracker "0.3.0"]]
  :plugins [[lein-midje "3.2"]]
  :repositories {"javelindev-snapshots" "http://javelindev.jp/repository/snapshots"}
  :profiles {:dev {:dependencies [[midje "1.8.3"]
                                  [ch.qos.logback/logback-classic "1.1.3"]]
                   :resource-paths ["resources-dev"]
                   :jvm-opts ["-Djava.net.preferIPv4Stack=true"]}
             :uberjar {:dependencies [[ch.qos.logback/logback-classic "1.1.3"]]
                       :resource-paths ["resources-release"]
                       :aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}}
  :aot [crow.registrar-source
        crow.registrar
        crow.protocol
        crow.discovery
        crow.marshaller
        crow.remote]
  :main crow.registrar)

(cemerick.pomegranate.aether/register-wagon-factory!
   "scp" #(let [c (resolve 'org.apache.maven.wagon.providers.ssh.external.ScpExternalWagon)]
                      (clojure.lang.Reflector/invokeConstructor c (into-array []))))
