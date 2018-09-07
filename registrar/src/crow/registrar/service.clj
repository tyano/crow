(ns crow.registrar.service
  (:require [clojure.spec.alpha :as s]))

(s/def :crow.registrar/service-info
  (s/keys :req [::address
                ::port
                ::service-id
                ::service-name
                ::attributes
                ::expire-at]))

(s/fdef new-service-info
    :ret :crow.registrar/service-info)

(defn new-service-info
  [address port service-id name attributes expire-at]
  #::{:address address
      :port port
      :service-id service-id
      :name name
      :attributes attributes
      :expire-at expire-at})
