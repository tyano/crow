(ns crow.registrar-source
  (:require [clojure.string :refer [split]]
            [clj-http.client :as http]
            [clojure.spec :as s])
  (:import [java.io BufferedReader StringReader]))


(defprotocol RegistrarSource
  (registrars [source] "fetch address and port of service registrars. the return value is a vector of maps with keys :address and :port."))


(s/def :crow/registrar-source #(satisfies? RegistrarSource %))

;;; An implementation of RegistrarSource which fetchs text from
;;; remote http resource accessible by 'source-url'.
;;; The text must be lines separated by \r\n.
;;; Each line must be:
;;;    hostname:port-number
(defrecord UrlRegistrarSource [source-url]
  RegistrarSource
  (registrars [source]
    (when-let [body (:body (http/get source-url))]
      (with-open [rdr (StringReader. body)]
        (doall
          (for [line (line-seq rdr)]
            (let [[address port-str] (split line #":")]
              {:address address, :port (Long/valueOf ^String port-str)})))))))

(defn url-registrar-source [source-url] (UrlRegistrarSource. source-url))

(defrecord StaticRegistrarSource [address port]
  RegistrarSource
  (registrars [_] [{:address address, :port port}]))

(defn static-registrar-source [address port] (StaticRegistrarSource. address port))
