(ns crow.registrar-source
  (:require [byte-streams :as bs]
            [clojure.string :refer [split]]
            [aleph.http :as http]
            [manifold.deferred :refer [chain] :as d]))


(defprotocol RegistrarSource
  (registrars [source] "fetch address and port of service registrars. the return value is a vector of maps with keys :address and :port."))

;;; An implementation of RegistrarSource which fetchs text from
;;; remote http resource accessible by 'source-url'.
;;; The text must be lines separated by \r\n.
;;; Each line must be:
;;;    hostname:port-number
(defrecord UrlRegistrarSource [source-url]
  RegistrarSource
  (registrars [source]
    (-> @(http/get source-url)
      (:body)
      (bs/to-line-seq)
      ((fn [lines]
        (for [line lines]
          (let [[address port-str] (split line #":")]
            {:address address, :port (Long/valueOf ^String port-str)})))))))

(defn url-registrar-source [source-url] (UrlRegistrarSource. source-url))

(defrecord StaticRegistrarSource [address port]
  RegistrarSource
  (registrars [_] [{:address address, :port port}]))

(defn static-registrar-source [address port] (StaticRegistrarSource. address port))
