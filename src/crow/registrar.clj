(ns crow.registrar
  (:require [aleph.tcp :refer [start-server] :as tcp]
            [manifold.deferred :refer [let-flow chain]]
            [manifold.stream :refer [connect] :as s]
            [clj-time.core :refer [now after? plus millis] :as t]
            [crow.protocol :refer [lease lease-expired registration invalid-message
                                   join-request? heart-beat? discovery? ping?
                                   protocol-error send! recv! ack
                                   service-found service-not-found
                                   read-message] :as p]
            [msgpack.core :refer [pack] :as msgpack]
            [clojure.core.async :refer [go-loop chan <! onto-chan thread]]
            [crow.service :as sv]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [byte-streams :refer [to-byte-array]])
  (:import [java.util UUID]))

(def ^:const default-renewal-ms 10000)
(def ^:const default-watch-interval 2000)

(defrecord Registrar [renewal-ms watch-interval services])
(defrecord ServiceInfo [ip-address service-id name attributes expire-at])

(defn new-registrar
  ([]
    (Registrar. default-renewal-ms default-watch-interval (atom {})))
  ([renewal-ms watch-interval]
    (Registrar. renewal-ms watch-interval (atom {}))))

(defn- new-service-id
  []
  (str (UUID/randomUUID)))

(defn accept-service-registration
  [registrar ip-address sid service-name attributes]
  (log/trace "service registration:" service-name (pr-str attributes))
  (let [service-id (or sid (new-service-id))
        expire-at  (-> (now) (plus (millis (:renewal-ms registrar))))
        services   (swap! (:services registrar)
                      #(assoc % service-id (ServiceInfo.
                                              ip-address
                                              service-id
                                              service-name
                                              attributes
                                              expire-at)))]
    (trace-pr "registered:"
      (registration service-id expire-at))))


(defn accept-heartbeat
  [registrar service-id renewal-ms]
  (log/trace "accept-heartbeat:" service-id)
  (let [expire-at (-> (now) (plus (millis (:renewal-ms registrar))))
        services  (swap! (:services registrar)
                    (fn [service-map]
                      (if (service-map service-id)
                        (update-in service-map [service-id]
                          (fn [old-info]
                            (assoc old-info :expire-at expire-at)))
                        service-map)))
        current   (services service-id)]
    (trace-pr "heartbeat response:"
      (if current
        (lease expire-at)
        (lease-expired service-id)))))

(defn service-expired
  [registrar service-id]
  (swap! (:services registrar) #(dissoc % service-id)))


(defn- check-expiration
  [registrar ch]
  (go-loop []
    (let [[service-id service-info] (<! ch)]
      (when (after? (now) (:expire-at service-info))
        (trace-pr "service expired:" service-info)
        (service-expired registrar service-id)))
    (recur)))

(defn- watch-services
  [registrar ch]
  (thread
    (loop []
      (when (seq @(:services registrar))
        (onto-chan ch @(:services registrar) false))
      (Thread/sleep (:watch-interval registrar))
      (recur))))

(defn process-registrar
  [registrar]
  (let [ch (chan)]
    (watch-services registrar ch)
    (check-expiration registrar ch)))

(defn- service-matches?
  [service service-name attributes]
  (if-let [attrs (not-empty attributes)]
    (and (= service-name (:name service))
         ((set (:attributes service)) attrs))
    (= service-name (:name service))))

(defn- find-matched-services
  [registrar service-name attributes]
  (filter #(service-matches? %) (deref (:services registrar))))

(defn accept-discovery
  [registrar service-name attributes]
  (log/trace "discovery: service-name:" service-name " attributes:" (pr-str attributes))
  (trace-pr "discovery response:"
    (if-let [services (not-empty (find-matched-services registrar service-name attributes))]
      (let [service-coll (map #(into {} %) services)]
        (service-found service-coll))
      (service-not-found service-name attributes))))


(defn- handle-request
  [registrar renewal-ms msg]
  (cond
    (ping? msg)         (do (log/trace "received a ping.") (ack))
    (join-request? msg) (let [{:keys [ip-address service-id service-name attributes]} msg]
                          (accept-service-registration registrar ip-address service-id service-name attributes))
    (heart-beat? msg)   (accept-heartbeat registrar (:service-id msg) renewal-ms)
    (discovery? msg)    (accept-discovery registrar (:service-name msg) (:attributes msg))
    :else               (invalid-message msg)))

(defn registrar-handler
  [registrar renewal-ms stream info]
  (let [source (->> stream
                  (s/map read-message)
                  (s/map (partial handle-request registrar renewal-ms))
                  (s/map pack))]
    (s/connect source stream)))

(defn start-registrar-service
  "レジストラサーバを起動して、サービスからの要求を待ち受けます。"
  [port & {:keys [renewal-ms watch-interval] :or {renewal-ms default-renewal-ms, watch-interval default-watch-interval}}]
  (let [registrar (new-registrar renewal-ms watch-interval)
        handler   (partial registrar-handler registrar renewal-ms)]
    (log/info (str "#### REGISTRAR SERVICE (port: " port ") starts."))
    (process-registrar registrar)
    (tcp/start-server handler {:port port})))


(defn -main
  [& args]
  (let [port-str (first args)
        opts     (partition 2 (rest args))]
    (when-not port-str
      (throw (IllegalArgumentException. "port must be supplied as a first arg.")))
    (let [optmap (into {} (for [[k v] opts]
                            (case k
                              "-r" [:renewal-ms (Long/valueOf v)]
                              "-w" [:watch-interval (Long/valueOf v)]
                              (throw (IllegalArgumentException. (str "Unknown option: " k))))))]
      (apply start-registrar-service
                (Long/valueOf port-str)
                (concat (into [] optmap))))))


