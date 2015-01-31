(ns crow.discovery
  (:require [crow.protocol :refer [discovery]]
            [crow.registrar-source :as source]
            [crow.service :as service]))


(defn- discover-with
  [registrar service-name attribute]
  )

(defn discover
  [registrar-source service-name attribute]
  (let [registrars (source/registrars registrar-source)]
    ))
