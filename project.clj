(defproject crow "2.0-SNAPSHOT"
  :description "Crow is a library for collaborating with distributed services implemented by Clojure."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-beta3"]
                 [async-connect "0.1.0-SNAPSHOT"]
                 [clojure-msgpack "1.2.1"]
                 [clj-time "0.14.0"]
                 [org.clojure/core.async "0.3.443"]
                 [org.clojure/tools.logging "0.4.0"]
                 [slingshot "0.12.2"]
                 [com.shelf/messagepack-framedecoder "1.0-SNAPSHOT"]
                 [ns-tracker "0.3.1"]
                 [clj-http "3.7.0"]
                 [integrant "0.6.1"]
                 [org.clojure/test.check "0.9.0"]]
  :plugins [[lein-midje "3.2"]]
  :repositories {"javelindev-snapshots" "http://javelindev.jp/repository/snapshots"}
  :profiles {:dev {:dependencies [[midje "1.9.0-alpha6"]
                                  [ch.qos.logback/logback-classic "1.2.3"]]
                   :resource-paths ["resources-dev"]
                   :jvm-opts ["-Djava.net.preferIPv4Stack=true"]}
             :uberjar {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
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
