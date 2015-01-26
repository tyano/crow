(ns crow.registrar
  (:require [aleph.tcp :refer [start-server] :as tcp]
            [manifold.stream :refer [put! take!] :as s]
            [manifold.deferred :as d]
            [clj-time.core :refer [now after?] :as t]
            [crow.protocol :refer [lease lease-expired registration invalid-message
                                   unpack-message join-request? heart-beat?] :as p]
            [clojure.core.async :refer [go-loop chan <! onto-chan thread]]
            [msgpack.core :refer [pack] :as msgpack])
  (:import [java.util UUID]))

(def ^:const default-renewal-ms 10000)
(def ^:const default-watch-interval 2000)

(defrecord Registrar [renewal-ms watch-interval services])

(defn new-registrar
  ([]
    (Registrar. default-renewal-ms default-watch-interval (atom {})))
  ([renewal-ms watch-interval]
    (Registrar. renewal-ms watch-interval (atom {}))))

(defn- new-service-id
  []
  (str (UUID/randomUUID)))

(defn accept-service-registration
  [registrar]
  (let [service-id (new-service-id)
        services   (swap! (:services registrar) #(assoc % service-id (now)))
        expire-at  (services service-id)]
    (registration service-id expire-at)))


(defn accept-heartbeat
  [registrar service-id]
  (let [services  (swap! (:services registrar)
                    (fn [service-map]
                      (if (service-map service-id)
                        (update-in service-map [service-id] (fn [_] (now)))
                        service-map)))
        expire-at (services service-id)]
    (if expire-at
      (lease expire-at)
      (lease-expired service-id))))

(defn service-expired
  [registrar service-id]
  (swap! (:services registrar) #(dissoc % service-id)))


(defn- check-expiration
  [ch]
  (go-loop []
    (let [[service-id expire-at] (<! ch)]
      (when (after? (now) expire-at)
        (service-expired service-id)))
    (recur)))

(defn- watch-services
  [registrar ch]
  (thread
    (loop []
      (when (seq (:services registrar))
        (onto-chan ch (:services registrar) false))
      (Thread/sleep (:watch-interval registrar))
      (recur))))

(defn process-registrar
  [registrar]
  (let [ch (chan)]
    (watch-services registrar ch)
    (check-expiration ch)))


(defn registrar-handler
  [registrar stream info]
  (let [data   (take! stream)
        msg    (unpack-message data)
        result (cond
                  (join-request? msg) (accept-service-registration registrar)
                  (heart-beat? msg)   (accept-heartbeat registrar (:service-id msg))
                  :else               (invalid-message msg))]
    (put! stream (pack result))))


(defn start-registrar-service
  "レジストラサーバを起動して、サービスからの要求を待ち受けます。"
  [port & {:keys [renewal-ms watch-interval] :or {renewal-ms default-renewal-ms, watch-interval default-watch-interval}}]
  (let [registrar (new-registrar renewal-ms watch-interval)
        handler   (partial registrar-handler registrar)]
    (tcp/start-server handler {:port port})))

