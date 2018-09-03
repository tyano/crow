(ns crow.call-descriptor
  (:require [clojure.spec.alpha :as s]))

(s/def ::target-ns string?)
(s/def ::fn-name string?)
(s/def ::fn-args (s/nilable (s/coll-of any?)))
(s/def :crow/call-descriptor
  (s/keys :req [::target-ns
                ::fn-name
                ::fn-args]))
