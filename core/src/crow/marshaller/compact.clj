(ns crow.marshaller.compact
  (:require [clojure.spec.alpha :as s]
            [clojure.set :refer [map-invert]]
            [clojure.tools.logging :refer [debug]]
            [crow.marshaller :refer [ObjectMarshaller] :as marshal]
            [crow.logging :refer [trace-pr]]
            [msgpack.core :refer [pack unpack] :as msgpack]
            [msgpack.macros :refer [extend-msgpack]]
            [msgpack.clojure-extensions]
            [clojure.tools.logging :as log]))


(defrecord FieldId [id])

(extend-msgpack FieldId 40
  [obj]
  (pack (:id obj))
  [bytedata]
  (FieldId. (unpack bytedata)))

(defn field-id?
  [obj]
  (instance? FieldId obj))

(defrecord ContextChange [keymap])

(extend-msgpack ContextChange 41
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
   (fn [{::keys [keymap last-field-id added-keymap] :or {keymap {}, last-field-id 0, added-keymap {}} :as ctx} k]
     (if (contains? keymap k)
       ctx
       (let [new-id (inc last-field-id)
             field-id (->FieldId new-id)]
         (-> ctx
             (assoc ::last-field-id new-id)
             (update ::keymap assoc k field-id)
             (update ::added-keymap assoc k field-id)))))
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
       (let [new-key (get (::keymap context) k k)
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
  "Uncompact mapdata with context and returns a map with keys :context and :resolved.
  the value of :resolved key is a resolved (uncompacted) map object.
  :context is next context object."
  [context mapdata]
  ;; convert all keys in a map from field-id to keyword
  ;; ::keymap is a map of keyword -> FieldId.
  ;; we must invert it before resolving a map.
  (let [inverted (update context ::keymap map-invert)]
    (reduce
     (fn [{:keys [context] :as r} [k v]]
       (let [new-key (get (::keymap inverted) k k)
             {next-context :context data :data} (resolve-with-context context v)]
         (-> r
             (update :resolved assoc new-key (first data))
             (assoc :context next-context))))
     {:context  context
      :resolved {}}
     mapdata)))

(defn- resolve-with-context
  "Resolve compacted objects with context.
  The 'data' key of the return value of this function always is a vector containing only 1 value or an empty vector.
  Empty vector means no result (it's not same with NIL), so that the result must be ignored.
  If it isn't an empty vector, the first item of the vector is the resolved item.
  The 'context' key is next context."
  [context obj]
  (if (context-change? obj)
    (do
      (debug "context! " (pr-str obj))
      {:context  (update context ::keymap merge (:keymap obj))
       :data     []})

    (let [{:keys [resolved] :as result}
          (cond
            (map? obj)
            (resolve-map-with-context context obj)

            (sequential? obj)
            (reduce
             (fn [{:keys [context] :as r} v]
               (let [{next-context :context data :data} (resolve-with-context context v)]
                 (-> r
                     (update :resolved concat data)
                     (assoc :context next-context))))
             {:context context :resolved []}
             obj)

            :else
            {:context context
             :resolved obj})]
      (-> result
          (dissoc :resolved)
          (assoc :data (vector resolved))))))

(defn unmarshall-data
  "Unmarshaling a marshalled object with context.
  Return value of this function is always a vector.
  Some marshalled object contains no value but contains information for uncompacting next objects.
  Such no-value object will be returned as an empty vector.
  Not empty objects always are returned as a vector containing one unmarshalled object."
  [context obj]
  (let [{:keys [data], new-context :context} (resolve-with-context context obj)]
    #::marshal{:context new-context
               :data    data}))

(defn marshall-data
  "Marshalling a object with context.
  This marshaller will compact maps by replacing map-keys with simple numbers.
  The marshalling result of one object might create multiple objects. Some of them will be
  'ContextChange' object which contains original key informations. So the result of this function
  always a vector."
  [context obj]
  (let [{{::keys [added-keymap] :as new-context} :context data :data} (compact-with-context context obj)]
    #::marshal{:context (dissoc new-context ::added-keymap)
               :data    (if (seq added-keymap)
                          [(ContextChange. added-keymap) data]
                          [data])}))

(defrecord CompactObjectMarshaller []
  ObjectMarshaller
  (marshal [this context obj]
    (marshall-data context obj))

  (unmarshal [this context obj]
    (unmarshall-data context obj)))