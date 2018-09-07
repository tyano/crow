(defproject crow "2.3-SNAPSHOT"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]]
  :plugins [[lein-modules "0.3.11"]]
  :modules {:dirs ["core"
                   "registrar"
                   "service"
                   "client"]})
