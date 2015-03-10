(ns crow.registrar
  (:require [aleph.tcp :refer [start-server] :as tcp]
            [manifold.deferred :refer [let-flow chain]]
            [manifold.stream :refer [connect] :as s]
            [clj-time.core :refer [now after? plus millis] :as t]
            [crow.protocol :refer [lease lease-expired registration invalid-message
                                   join-request? heart-beat? discovery? ping?
                                   protocol-error ack
                                   service-found service-not-found] :as p]
            [crow.request :refer [read-message frame-decorder]]
            [msgpack.core :refer [pack] :as msgpack]
            [clojure.core.async :refer [go-loop chan <! onto-chan thread]]
            [crow.service :as sv]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr debug-pr info-pr]]
            [byte-streams :refer [to-byte-array]]
            [clojure.set :refer [superset?]])
  (:import [java.util UUID]))

(def ^:const default-renewal-ms 10000)
(def ^:const default-watch-interval 2000)

(defrecord Registrar [name renewal-ms watch-interval services])
(defrecord ServiceInfo [address port service-id name attributes expire-at])

(defn new-registrar
  ([name renewal-ms watch-interval]
    (Registrar. name renewal-ms watch-interval (atom {}))))

(defn- new-service-id
  []
  (str (UUID/randomUUID)))

(defn accept-service-registration
  [registrar address port sid service-name attributes]
  (log/debug "service registration:" address port sid service-name (pr-str attributes))
  (let [service-id (or sid (new-service-id))
        expire-at  (-> (now) (plus (millis (:renewal-ms registrar))))
        services   (swap! (:services registrar)
                      #(assoc % service-id (ServiceInfo.
                                              address
                                              port
                                              service-id
                                              service-name
                                              attributes
                                              expire-at)))]
    (debug-pr "registered:"
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
        (info-pr "service expired:" service-info)
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
         (superset? (set (:attributes service)) (set attrs)))
    (= service-name (:name service))))

(defn- find-matched-services
  [registrar service-name attributes]
  (filter #(service-matches? % service-name attributes) (vals (deref (:services registrar)))))

(defn accept-discovery
  [registrar service-name attributes]
  (log/debug "discovery: service-name:" service-name " attributes:" (pr-str attributes))
  (debug-pr "discovery response:"
    (if-let [services (not-empty (find-matched-services registrar service-name attributes))]
      (let [service-coll (for [svc services]
                            {:address      (:address svc)
                             :port         (:port svc)
                             :service-name (:name svc)
                             :attributes   (:attributes svc)})]
        (service-found service-coll))
      (do
        (log/debug "service not found.")
        (log/trace "current registared services:")
        (doseq [svc @(:services registrar)]
          (trace-pr "" svc))
        (service-not-found service-name attributes)))))


(defn- handle-request
  [registrar renewal-ms msg]
  (cond
    (ping? msg)         (do (log/trace "received a ping.") (ack))
    (join-request? msg) (let [{:keys [address port service-id service-name attributes]} msg]
                          (accept-service-registration registrar address port service-id service-name attributes))
    (heart-beat? msg)   (accept-heartbeat registrar (:service-id msg) renewal-ms)
    (discovery? msg)    (accept-discovery registrar (:service-name msg) (:attributes msg))
    :else               (invalid-message msg)))

(defn registrar-handler
  [registrar renewal-ms buffer-size stream info]
  (let [source (->> stream
                  (s/buffer buffer-size)
                  (s/map read-message)
                  (s/map (partial handle-request registrar renewal-ms))
                  (s/map pack))]
    (s/connect source stream)))


(defn start-registrar-service
  "Starting a registrar and wait requests.
  An argument is a map of configurations of keys:

  :port a waiting port number.
  :renewal-ms  milliseconds for make each registered services expired. Services must send a 'lease' request before the expiration.
  :watch-internal  milliseconds for checking each service is expired or not."
  [{:keys [port name renewal-ms watch-interval buffer-size] :or {port 4000, renewal-ms default-renewal-ms, watch-interval default-watch-interval, buffer-size 10}}]
  (let [registrar (new-registrar name renewal-ms watch-interval)
        handler   (partial registrar-handler registrar renewal-ms buffer-size)]
    (log/info (str "#### REGISTRAR SERVICE (name: " (pr-str name) " port: " port ") starts."))
    (process-registrar registrar)
    (tcp/start-server
      handler
      {:port port
       :pipeline-transform #(.addFirst % "framer" (frame-decorder))})))


(defn -main
  [name port-str & args]
  (let [opts (partition 2 args)]
    (when-not port-str
      (throw (IllegalArgumentException. "port must be supplied as a first arg.")))
    (let [optmap (into {} (for [[k v] opts]
                            (case k
                              "-r" [:renewal-ms (Long/valueOf v)]
                              "-w" [:watch-interval (Long/valueOf v)]
                              (throw (IllegalArgumentException. (str "Unknown option: " k))))))]
      (start-registrar-service
                (merge {:port (Long/valueOf port-str), :name name} optmap)))))

