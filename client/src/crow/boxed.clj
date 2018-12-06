(ns crow.boxed
  (:require [crow.utils :refer [select-ns-keys]]
            [crow.discovery.service :as service-info]
            [box.core :as box])
  (:import [box.core Box]))


(defprotocol ServiceInfo
  (service-info [this] "return a vector of a service instance which execute an remote call and a service-descriptor
    which used for finding the service."))

(extend-type Box
  ServiceInfo
  (service-info
    [this]
    (when-let [invoke-info (::invoke-info this)]
      {:service (select-ns-keys invoke-info
                                :crow.discovery.service)
       :service-descriptor (select-ns-keys invoke-info
                                           :crow.service-descriptor)})))


(extend-type nil
  ServiceInfo
  (service-info [_] nil))

(defn with-service-info
  [boxed-value invoke-info]
  (assoc boxed-value
         ::invoke-info
         invoke-info))
