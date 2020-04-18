(ns crow.marshaller.compact
  (:require [clojure.spec.alpha :as s]
            [crow.marshaller :refer [ObjectMarshaller] :as marshal]
            [crow.protocol :refer [pack-and-combine]]
            [msgpack.core :refer [pack unpack] :as msgpack]
            [msgpack.macros :refer [extend-msgpack]]
            [msgpack.clojure-extensions]))


(def ^:dynamic *current-context* nil)

(defrecord FieldId [id])

(extend-msgpack FieldId 2000
  [obj]
  (pack (:id obj))
  [bytedata]
  (FieldId. (unpack bytedata)))

(defn field-id?
  [obj]
  (instance? FieldId obj))

(defrecord ContextChange [keymap])

(extend-msgpack ContextChange 2001
  [obj]
  (pack (:keymap obj))
  [bytedata]
  (ContextChange. (unpack bytedata)))

(defn context-change?
  [obj]
  (instance? ContextChange obj))

(defn- make-context-from-keys
  [context keys]
  (reduce
   (fn [{:keys [keymap last-field-id] :or {keymap {}, last-field-id 0} :as ctx} k]
     (if (contains? keymap k)
       ctx
       (let [new-id (inc last-field-id)]
         (-> ctx
             (update :keymap assoc k (->FieldId new-id))
             (update :last-field-id new-id)))))
   context
   keys))

(declare resolve-with-context compact-with-context)


(s/fdef compact-map-with-context
    :ret (s/keys :req-un [::context ::data]))

(defn- compact-map-with-context
  [context mapdata]
  (let [key-coll (keys mapdata)
        new-context (make-context-from-keys context key-coll)]
    (reduce
     (fn [{:keys [context] :as r} [k v]]
       (let [new-key (get (:keymap context) k k)
             {next-context :context data :data} (compact-with-context new-context v)]
         (-> r
             (update :data assoc new-key data)
             (assoc :context next-context))))
     {:context new-context
      :data    {}}
     mapdata)))


(s/fdef compact-with-context
    :ret (s/keys :req-un [::context ::data]))

(defn- compact-with-context
  [context obj]
  (cond
    (map? obj)
    (compact-map-with-context context obj)

    (sequential? obj)
    (reduce
     (fn [{:keys [context] :as r} v]
       (let [{next-context :context data :data} (compact-with-context context v)]
         (-> r
             (update :data conj data)
             (assoc :context next-context))))
     {:context context :data []}
     obj)

    :else
    {:context context
     :data obj}))

(defn- resolve-map-with-context
  [context mapdata]
  ;; convert all keys in a map from field-id to keyword
  ;; context is a map with keyword -> FieldId.
  ;; we must invert it before resolving a map.
  (let [inverted (update context :keymap map-invert)]
    (->> mapdata
         (map (fn [[k v]]
                (vector
                 (get (:keymap inverted) k k)
                 (unmarshal-with-context context v))))
         (into {}))))

(defn- resolve-with-context
  [context obj]
  (cond
    (map? obj)
    (resolve-map-with-context context obj)

    (sequential? obj)
    (map #(resolve-with-context context %) obj)

    :else
    obj))

(defn unmarshall-data
  [context bytedata]
  (let [obj (unpack bytedata)]
    (cond
      (context-change? obj)
      #::marshal{:context (update context :keymap merge (:keymap obj))
                 :data    []}

      :else
      #::marshal{:context context
                 :data    [(resolve-with-context context obj)]})))

(defn marshall-data
  [context obj]
  (let [{next-context :context data :data} (compact-with-context context obj)]
    #::marshal{:context next-context
               :data    [data]}))

(defrecord CompactObjectMarshaller []
  ObjectMarshaller
  (marshal [this context obj]
    (marshall-data context obj))

  (unmarshal [this context obj]
    (unmarshall-data context obj)))
