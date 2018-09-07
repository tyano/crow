(ns crow.marshaller
  (:require [clojure.edn :as edn]))


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

(defmulti marshal-data "convert an object into other msgpack-safe object." (fn [obj] (object-type obj)))
(defmulti unmarshal-data "convert an object from msgpack-safe format to a real object." (fn [obj] (class obj)))

(defmethod marshal-data :primitive [obj] obj)
(defmethod marshal-data :primitive-array [obj] obj)
(defmethod marshal-data :default [obj] (pr-str obj))

(defmethod unmarshal-data String [obj] (edn/read-string obj))
(defmethod unmarshal-data :default [obj] obj)

(defprotocol ObjectMarshaller
  (marshal [this obj] "convert an object to serialized form.")
  (unmarshal [this obj] "convert an marshalled object to a clojure object."))

(defrecord EdnObjectMarshaller
  []
  ObjectMarshaller
  (marshal [this obj] (marshal-data obj))
  (unmarshal [this obj] (unmarshal-data obj)))

