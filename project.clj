(defproject crow "2.4.1-SNAPSHOT"
  :description "Crow is a library for building and collaborating with distributed services implemented by Clojure."
  :url "https://github.com/tyano/crow"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-modules "0.3.11"]]
  :modules {:dirs ["core"
                   "registrar"
                   "service"
                   "client"]
            :inherited {:repositories [["clojars" {:url "https://clojars.org/repo"
                                                   :username [:env/clojars_username :gpg]
                                                   :password [:env/clojars_password :gpg]}]]
                        :description "Crow is a library for building and collaborating with distributed services implemented by Clojure."
                        :url "https://github.com/tyano/crow"}
            :versions {org.clojure/clojure "1.10.1"
                       clj-time "0.15.1"
                       crow/core :version}})
