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
