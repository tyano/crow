(ns crow.service-descriptor
  (:require [clojure.spec.alpha :as s]))


(s/def ::service-name string?)
(s/def ::attributes (s/nilable (s/map-of keyword? any?)))
(s/def :crow/service-descriptor
  (s/keys :req [::service-name ::attributes]))
