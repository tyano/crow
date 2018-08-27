(ns crow.id-store
  (:refer-clojure :exclude [read])
  (:require [clojure.string :refer [blank?]]
            [clojure.java.io :refer [writer reader]]
            [clojure.spec.alpha :as s])
  (:import [java.io FileNotFoundException Writer Reader]))

(defprotocol ServiceIdStore
  (write [this service-id] "write service-id into persistent store.")
  (read [this] "read service-id from persistent store."))

(extend-protocol ServiceIdStore
  nil
  (write [this service-id] nil)
  (read [this] nil))

(s/def :crow/id-store
  #(satisfies? ServiceIdStore %))


(defrecord FileIdStore [file-path]
  ServiceIdStore
  (write [this service-id]
    (when (not (blank? service-id))
      (with-open [w ^Writer (writer file-path :append false)]
        (.write w service-id)
        (.newLine w))))
  (read [this]
    (try
      (with-open [r ^Reader (reader file-path)]
        (.readLine r))
      (catch FileNotFoundException ex
        nil))))

