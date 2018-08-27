(ns crow.spec
  (:require [clojure.spec.alpha :as s]))

(s/def ::found-service
  (s/keys :req-un
          [:service/address
           :service/port
           :service/service-id
           :service/service-name
           :service/attributes]))

(s/def ::found-services (s/coll-of ::found-service))
