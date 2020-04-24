(ns crow.marshaller.edn
  (:require [clojure.edn :as edn]
            [crow.marshaller :refer [ObjectMarshaller] :as marshal]))

(defn primitive-array?
  [^Class obj-class]
  (and
    (.isArray obj-class)
    (.isPrimitive (.getComponentType obj-class))))

(def wrapper-class
  #{java.lang.Character
    java.lang.Byte
    java.lang.Short
    java.lang.Integer
    java.lang.Long
    java.lang.Float
    java.lang.Double
    clojure.lang.BigInt
    clojure.lang.Ratio})

(defn wrapper-array?
  [^Class obj-class]
  (boolean
    (and
      (.isArray obj-class)
      (wrapper-class (.getComponentType obj-class)))))

(defn object-type
  [obj]
  (let [obj-class ^Class (class obj)]
    (cond
      (nil? obj)                   :primitive
      (.isPrimitive obj-class)     :primitive
      (wrapper-class obj-class)    :primitive
      (primitive-array? obj-class) :primitive-array
      (wrapper-array? obj-class)   :primitive-array
      :else :object)))

(defmulti marshal-data "convert an object into other msgpack-safe object." (fn [edn-opts obj] (object-type obj)))
(defmulti unmarshal-data "convert an object from msgpack-safe format to a real object." (fn [edn-opts obj] (class obj)))

(defmethod marshal-data :primitive [edn-opts obj] obj)
(defmethod marshal-data :primitive-array [edn-opts obj] obj)
(defmethod marshal-data :default [edn-opts obj] (pr-str obj))

(defmethod unmarshal-data String [edn-opts obj] (edn/read-string edn-opts obj))
(defmethod unmarshal-data :default [edn-opts obj] obj)

(defrecord EdnObjectMarshaller
  [edn-opts]
  ObjectMarshaller
  (marshal [this context obj] #::marshal{:context context :data [(marshal-data edn-opts obj)]})
  (unmarshal [this context obj] #::marshal{:context context :data [(unmarshal-data edn-opts obj)]}))

(defn edn-object-marshaller
  ([edn-opts]
   (->EdnObjectMarshaller edn-opts))
  ([]
   (edn-object-marshaller {})))

