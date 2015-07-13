(ns crow.utils
  (:require [slingshot.support :refer [get-context]]))


(defn extract-exception
  [{:keys [object throwable wrapper]}]
  (let [th   (or wrapper throwable)
        type (cond
                (instance? Throwable object)
                :error

                (associative? object)
                (or (:type object) :error)

                :else
                :error)]
    [type th]))
