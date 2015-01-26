(ns crow.service)

(defrecord Service [service-id-atom expire-at-atom registrars])

(defn new-service
  ([]
    (Service. (atom nil) (atom nil) (atom [])))
  ([service-id]
    (Service. (atom service-id) (atom nil) (atom []))))

(defn service-id
  [service]
  (deref (:service-id-atom service)))

(defn expire-at
  [service]
  (deref (:expire-at-atom service)))
