(ns crow.discovery
  (:require [crow.protocol :refer [discovery]]
            [crow.registrar-source :as source]
            [crow.service :as service]
            [clojure.set :refer [difference]]))

(def active-registrars (ref #{}))
(def dead-registrars (ref #{}))

(defn- reset-registrars!
  [registrar-source]
  (let [new-registrars (source/registrars registrar-source)]
    (dosync
      (io!
        (alter active-registrars
          (fn [_]
            (difference new-registrars @dead-registrars)))))))

(defn- next-registrar
  []
  (dosync
    (first (shuffle @active-registrars))))

(defn- abandon-registrar!
  [reg]
  (dosync
    (io!
      (let [reg (first (shuffle (alter active-registrars disj reg)))]
        (alter dead-registrars conj reg)
        reg))))

(defn- discover-with
  [registrar service-name attribute]
  )

(defn discover
  [registrar-source service-name attribute]
  (when-not (seq @active-registrars)
    (reset-registrars! registrar-source)))

