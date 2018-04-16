(ns crow.boxed)

(defprotocol Boxed
  (service-info [this] "return a vector of a service instance which execute an remote call and a service-descriptor
    which used for finding the service.")

  (success? [this] "return true if a value wrapped by this box is not an exception.")
  (failure? [this] "return true if a value wrapped by this box is an exception.")
  (value [this] "Just return a wrapped value without throwing an exception."))


(defn- success-value?
  [value]
  (boolean (or (nil? value)
               (not (instance? Throwable value)))))

(defn- failure-value?
  [value]
  (not (success-value? value)))

(defn box
  ([used-service-desc used-service value]
    (reify
      Boxed
      (service-info
        [this]
        {:service used-service
         :service-descriptor used-service-desc})

      (success? [this] (success-value? value))
      (failure? [this] (failure-value? value))
      (value [this] value)

      clojure.lang.IDeref
      (deref
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
      (success? [this] (success-value? value))
      (failure? [this] (failure-value? value))
      (value [this] value)

      clojure.lang.IDeref
      (deref
        [this]
        (if (instance? Throwable value)
          (throw value)
          value))

      Object
      (toString
        [this]
        (str "crow.boxed/Boxed[value=" value "]")))))

(extend-protocol Boxed
  nil
  (service-info [_] nil)
  (success? [_] false)
  (failure? [_] false)
  (value [_] nil))
