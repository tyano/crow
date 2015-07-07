(ns crow.utils
  (:require [slingshot.support :refer [get-context]]))


(defn extract-exception
  [ex]
  (let [{:keys [object throwable]} (get-context ex)
        type (cond
                (instance? Throwable object) :error
                (associative? object)        (or (:type object) :error)
                :else                        :error)]
    [type throwable]))
