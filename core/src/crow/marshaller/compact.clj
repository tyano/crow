(ns crow.marshaller.compact
  (:require [clojure.spec.alpha :as s]
            [clojure.set :refer [map-invert]]
            [crow.marshaller :refer [ObjectMarshaller marshal unmarshal] :as marshaller]
            [crow.marshaller.edn :refer [edn-object-marshaller]]
            [crow.logging :refer [trace-pr debug-pr]]
            [msgpack.core :refer [pack unpack] :as msgpack]
            [msgpack.macros :refer [extend-msgpack]]
            [msgpack.clojure-extensions]))


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
       (let [new-id (inc last-field-id)]
         (-> ctx
             (assoc ::last-field-id new-id)
             (update ::keymap assoc k new-id)
             (update ::added-keymap assoc k new-id)))))
   context
   keys))

(declare resolve-with-context compact-with-context)


(s/fdef compact-map-with-context
    :ret (s/keys :req-un [::context ::data]))

(defn- compact-map-with-context
  [context mapdata]
  (let [{key-coll :data current-context :context} (compact-with-context context (keys mapdata))
        new-context (make-context-from-keys current-context key-coll)]
    (reduce
     (fn [{:keys [context] :as r} [k v]]
       (let [{marshalled-key :data current-context :context} (compact-with-context context k)
             new-key (get (::keymap current-context) marshalled-key marshalled-key)
             {next-context :context data :data} (compact-with-context current-context v)]
         (-> r
             (update :data assoc new-key data)
             (assoc :context next-context))))
     {:context new-context
      :data    {}}
     mapdata)))


(s/fdef compact-with-context
    :ret (s/keys :req-un [::context ::data]))

(defn- compact-with-context
  [{::keys [internal-marshaller] :as context} obj]
  (cond
    (map? obj)
    (compact-map-with-context context obj)

    (sequential? obj)
    (reduce
     (fn [{:keys [context] :as r} v]
       (let [{next-context :context data :data} (compact-with-context context v)]
         (trace-pr "data:" data)
         (-> r
             (update :data conj data)
             (assoc :context next-context))))
     {:context context :data []}
     obj)

    :else
    {:context context
     :data (-> (marshal internal-marshaller context obj)
               ::marshaller/data
               first)}))

(defn- resolve-map-with-context
  "Uncompact mapdata with context and returns a map with keys :context and :data.
  the value of :data key is A VECTOR of a resolved (uncompacted) map object.
  :context is next context object."
  [context mapdata]
  ;; convert all keys in a map from field-id to keyword
  ;; ::keymap is a map of keyword -> Number.
  ;; we must invert it before resolving a map.
  (let [inverted (update context ::keymap map-invert)
        {:keys [context resolved]} (reduce
                                    (fn [{:keys [context] :as r} [k v]]
                                      (let [new-key (get (::keymap inverted) k k)
                                            {key-context :context [unmarshalled-key] :data} (resolve-with-context context new-key)
                                            {next-context :context [data] :data} (resolve-with-context key-context v)]
                                        (-> r
                                            (update :resolved assoc unmarshalled-key data)
                                            (assoc :context next-context))))
                                    {:context  context
                                     :resolved {}}
                                    mapdata)]
    {:context context
     :data [resolved]}))

(defn- resolve-with-context
  "Resolve compacted objects with context.
  The 'data' key of the return value of this function always is a vector containing only 1 value or an empty vector.
  Empty vector means no result (it's not same with NIL), so that the result must be ignored.
  If it isn't an empty vector, the first item of the vector is the resolved item.
  The 'context' key is next context."
  [{::keys [internal-marshaller] :as context} obj]
  (cond
    (context-change? obj)
    (do
      (trace-pr "context! " obj)
      {:context  (update context ::keymap merge (:keymap obj))
       :data     []})

    (map? obj)
    (resolve-map-with-context context obj)

    (sequential? obj)
    (-> (reduce
         (fn [{:keys [context] :as r} v]
           (let [{next-context :context data :data} (resolve-with-context context v)]
             (if-not (seq data)
               (assoc r :context next-context)
               (-> r
                   (update :data concat data)
                   (assoc :context next-context)))))
         {:context context :data []}
         obj)
        (update :data vector))

    :else
    {:context context
     :data (-> (unmarshal internal-marshaller context obj)
               ::marshaller/data)}))

(defn unmarshal-data
  "Unmarshaling a marshalled object with context.
  Return value of this function is always a vector.
  Some marshalled object contains no value but contains information for uncompacting next objects.
  Such no-value object will be returned as an empty vector.
  Not empty objects always are returned as a vector containing one unmarshalled object."
  [context obj]
  (let [{:keys [data], new-context :context} (resolve-with-context context obj)]
    (trace-pr "unmarshalled:" data)
    #::marshaller{:context new-context
                  :data data}))

(defn marshal-data
  "Marshalling a object with context.
  This marshaller will compact maps by replacing map-keys with simple numbers.
  The marshalling result of one object might create multiple objects. Some of them will be
  'ContextChange' object which contains original key informations. So the result of this function
  always a vector."
  [context obj]
  (let [{{::keys [added-keymap] :as new-context} :context data :data} (compact-with-context context obj)]
    (trace-pr "marshalled:" data)
    #::marshaller{:context (dissoc new-context ::added-keymap)
                  :data    (if (seq added-keymap)
                             [(ContextChange. added-keymap) data]
                             [data])}))

(defrecord CompactObjectMarshaller [internal-marshaller]
  ObjectMarshaller
  (marshal [this context obj]
    (marshal-data (assoc context ::internal-marshaller internal-marshaller) obj))

  (unmarshal [this context obj]
    (unmarshal-data (assoc context ::internal-marshaller internal-marshaller) obj)))

(defn compact-object-marshaller
  ([internal-marshaller]
   (let [marshaller (or internal-marshaller (edn-object-marshaller))]
     (->CompactObjectMarshaller marshaller)))
  ([]
   (compact-object-marshaller nil)))
