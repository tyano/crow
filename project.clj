(defproject crow "2.1-SNAPSHOT"
  :description "Crow is a library for collaborating with distributed services implemented by Clojure."
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [async-connect "0.2.0-SNAPSHOT"]
                 [clojure-msgpack "1.2.1"]
                 [clj-time "0.14.2"]
                 [org.clojure/core.async "0.4.474"]
                 [org.clojure/tools.logging "0.4.0"]
                 [slingshot "0.12.2"]
                 [com.shelf/messagepack-framedecoder "1.0-SNAPSHOT"]
                 [ns-tracker "0.3.1"]
                 [clj-http "3.7.0"]
                 [integrant "0.6.3"]
                 [org.clojure/test.check "0.9.0"]]
  :plugins [[lein-midje "3.2"]]
  :repositories [["Shelf Public Release Repository" {:url "https://shelf-maven-repo.s3-ap-northeast-1.amazonaws.com/release"
                                                     :snapshots false
                                                     :update :never}]
                 ["Shelf Public Snapshot Repository" {:url "https://shelf-maven-repo.s3-ap-northeast-1.amazonaws.com/snapshot"
                                                      :snapshots true
                                                      :update :always}]]
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :profiles {:dev {:dependencies [[midje "1.9.1"]
                                  [ch.qos.logback/logback-classic "1.2.3"]]
                   :resource-paths ["resources-dev"]
                   :jvm-opts ["-Djava.net.preferIPv4Stack=true"]}
             :uberjar {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
                       :resource-paths ["resources-release"]
                       :aot :all}}
  :main crow.registrar
  :javac-options ["-source" "1.8" "-target" "1.8"])
