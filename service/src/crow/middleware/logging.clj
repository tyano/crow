(ns crow.middleware.logging
  (:require [clojure.tools.logging :as log]))

(defn wrap-pre-logger
  [level log-msg]
  (fn [handler]
    (fn [msg]
      (log/log level log-msg)
      (handler msg))))

(defn wrap-post-logger
  [level log-msg]
  (fn [handler]
    (fn [msg]
      (let [r (handler msg)]
        (log/log level log-msg)
        r))))
