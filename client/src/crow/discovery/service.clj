(ns crow.discovery.service
  (:require [clojure.spec.alpha :as s]))

(s/def ::address string?)
(s/def ::port pos-int?)
(s/def ::service-id string?)
(s/def ::service-name string?)
(s/def ::attributes map?)

(s/def :crow.discovery/service
  (s/keys :req [::address
                ::port
                ::service-id
                ::service-name
                ::attributes]))
