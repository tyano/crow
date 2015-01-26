(ns crow.service)

(defrecord Service [ip-address service-id-atom expire-at-atom registrars])

(defn new-service
  [ip-address]
  (Service. ip-address (atom nil) (atom nil) (atom [])))

(defn service-id
  [service]
  (deref (:service-id-atom service)))

(defn expire-at
  [service]
  (deref (:expire-at-atom service)))





