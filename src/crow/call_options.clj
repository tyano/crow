(ns crow.call-options
  (:require [clojure.spec.alpha :as s]))

(s/def ::timeout-ms pos-int?)
(s/def :crow/call-options
  (s/keys :opt [::timeout-ms]))
