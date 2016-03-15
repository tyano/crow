(ns crow.middleware.chain)

(defn chain
  [& wrappers]
  (fn [handler]
    (reduce
      (fn [h wrapper]
        (wrapper h))
      handler
      wrappers)))
