(ns crow.boxed)

(defprotocol Boxed
  (unbox [this] "retrieve a value this Boxed object is holding, but if the value is an exception, throw the exception."))

(defn box
  [value]
  (reify
    Boxed
    (unbox
      [this]
      (if (instance? Throwable value)
        (throw value)
        value))

    Object
    (toString
      [this]
      (str "crow.boxed/Boxed[value=" value "]"))))
