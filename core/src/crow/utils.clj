(ns crow.utils)

(defn extract-exception
  [th]
  (let [info (ex-data th)
        type (cond
                (some? info)
                (or (:type info) :error)

                :else
                :error)]
    [type th]))

(defn select-ns-keys
  [datamap ns-name]
  (->> datamap
       (filter #(= (name ns-name) (namespace (key %))))
       (into {})))

(defn assoc-key-ns
  [datamap ns-name]
  (->> datamap
       (map #(vector (keyword (name ns-name) (name (key %))) (val %)))
       (into {})))
