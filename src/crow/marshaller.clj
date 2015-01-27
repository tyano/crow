(ns crow.marshaller
  (:require [clojure.edn :as edn]))


(defn- primitive-array?
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

(defn- wrapper-array?
  [^Class obj-class]
  (boolean
    (and
      (.isArray obj-class)
      (wrapper-class (.getComponentType obj-class)))))

(defn- object-type
  [obj]
  (let [obj-class ^Class (class obj)]
    (cond
      (nil? obj)                   :primitive
      (.isPrimitive obj-class)     :primitive
      (wrapper-class obj-class)    :primitive
      (primitive-array? obj-class) :primitive-array
      (wrapper-array? obj-class)   :primitive-array
      :else :object)))

(defmulti marshal "convert an object into other msgpack-safe object." (fn [obj] (object-type obj)))
(defmulti unmarshal "convert an object from msgpack-safe format to a real object." (fn [obj] (class obj)))

(defmethod marshal :primitive [obj] obj)
(defmethod marshal :primitive-array [obj] obj)
(defmethod marshal :default [obj] (pr-str obj))

(defmethod unmarshal String [obj] (edn/read-string obj))
(defmethod unmarshal :default [obj] obj)
