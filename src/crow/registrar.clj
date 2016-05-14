(ns crow.registrar
  (:require [aleph.tcp :refer [start-server] :as tcp]
            [aleph.netty :as netty]
            [manifold.deferred :refer [let-flow chain] :as d]
            [manifold.stream :refer [connect buffer] :as s]
            [clj-time.core :refer [now after? plus millis] :as t]
            [crow.protocol :refer [lease lease-expired registration invalid-message
                                   join-request? heart-beat? discovery? ping?
                                   protocol-error ack call-exception
                                   service-found service-not-found] :as p]
            [crow.request :refer [frame-decorder wrap-duplex-stream format-stack-trace] :as request]
            [clojure.core.async :refer [go-loop chan <! onto-chan timeout]]
            [crow.service :as sv]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr debug-pr info-pr]]
            [byte-streams :refer [to-byte-array]]
            [clojure.set :refer [superset?]]
            [crow.utils :refer [extract-exception]]
            [slingshot.support :refer [get-context]])
  (:import [java.util UUID])
  (:gen-class))

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
  (log/info "service registration:" address port sid service-name (pr-str attributes))
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
    (info-pr "registered:"
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
    (log/trace "check-expiration")
    (let [[service-id service-info] (<! ch)]
      (when (after? (now) (:expire-at service-info))
        (info-pr "service expired:" service-info)
        (service-expired registrar service-id)))
    (recur)))

(defn- watch-services
  [registrar ch]
  (go-loop []
    (log/trace "watch-services")
    (when (seq @(:services registrar))
      (onto-chan ch @(:services registrar) false))
    (<! (timeout (:watch-interval registrar)))
    (recur)))

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
  [registrar renewal-ms stream info timeout-ms]
  (->
    (d/loop []
      (-> (s/try-take! stream ::none timeout-ms ::none)
        (d/chain
          (fn [msg]
            (if (= ::none msg)
              ::none
              (d/future (handle-request registrar renewal-ms msg))))
          (fn [msg']
            (when-not (= ::none msg')
              (s/try-put! stream msg' timeout-ms ::timeout)))
          (fn [result]
            (when (some? result)
              (cond
                (= result ::timeout)
                (log/error "Registrar Timeout: Couldn't write response.")

                (true? result)
                (d/recur)

                :else
                (log/error "Registrar Error: Couldn't write response.")))))
        (d/catch
          (fn [ex]
            (log/error ex "An Error ocurred.")
            (let [[type throwable] (extract-exception (get-context ex))]
              (s/try-put! stream (call-exception type (format-stack-trace throwable)) timeout-ms)
              nil)))))
    (d/finally
      (fn []
        (s/close! stream)))))

(defn start-registrar-service
  "Starting a registrar and wait requests.
  An argument is a map of configurations of keys:

  :port a waiting port number.
  :renewal-ms  milliseconds for make each registered services expired. Services must send a 'lease' request before the expiration.
  :watch-internal  milliseconds for checking each service is expired or not."
  [{:keys [port name renewal-ms watch-interval send-recv-timeout]
    :or {port 4000, renewal-ms default-renewal-ms, watch-interval default-watch-interval send-recv-timeout nil}}]

  (let [registrar (new-registrar name renewal-ms watch-interval)]
    (process-registrar registrar)
    (let [server
            (tcp/start-server
              (fn [stream info]
                (registrar-handler registrar renewal-ms (wrap-duplex-stream stream) info send-recv-timeout))
              {:port port
               :pipeline-transform #(.addFirst % "framer" (frame-decorder))})]
      (log/info (str "#### REGISTRAR SERVICE (name: " (pr-str name) " port: " (netty/port server) ") starts."))
      server)))


(defn -main
  [name port-str & args]
  (let [opts (partition 2 args)]
    (when-not port-str
      (throw (IllegalArgumentException. "port must be supplied as a first arg.")))
    (let [optmap (into {} (for [[k v] opts]
                            (case k
                              "-r" [:renewal-ms (Long/valueOf v)]
                              "-w" [:watch-interval (Long/valueOf v)]
                              (throw (IllegalArgumentException. (str "Unknown option: " k))))))
          server (start-registrar-service
                    (merge {:port (Long/valueOf port-str), :name name} optmap))]
      (.. (Runtime/getRuntime) (addShutdownHook (Thread. (fn [] (.close server) (println "SERVER STOPPED.")))))
      (while true
        (Thread/sleep 1000)))))

