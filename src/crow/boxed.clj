(ns crow.boxed)

(defprotocol Boxed
  (service-info [this] "return a vector of a service instance which execute an remote call and a service-descriptor
    which used for finding the service.")
  (unbox [this] "retrieve a value this Boxed object is holding, but if the value is an exception, throw the exception."))

(defn box
  ([used-service-desc used-service value]
    (reify
      Boxed
      (service-info
        [this]
        [used-service used-service-desc])

      (unbox
        [this]
        (if (instance? Throwable value)
          (throw value)
          value))

      Object
      (toString
        [this]
        (str "crow.boxed/Boxed[value=" value ", service-info=" [used-service used-service-desc] "]"))))
  ([value]
    (reify
      Boxed
      (service-info [this] nil)

      (unbox
        [this]
        (if (instance? Throwable value)
          (throw value)
          value))

      Object
      (toString
        [this]
        (str "crow.boxed/Boxed[value=" value "]")))))
