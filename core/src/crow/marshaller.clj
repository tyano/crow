(ns crow.marshaller
  (:require [clojure.spec.alpha :as s]))

(defprotocol ObjectMarshaller
  (marshal [this context obj] "convert an object to serialized form.")
  (unmarshal [this context obj] "convert an marshalled object to a clojure object."))

(s/def ::context map?)
(s/def ::data vector?)

(s/def ::unmarshalled
  (s/keys :req [::context ::data]))
