(ns crow.registrar
  (:require [clojure.spec.alpha :as s]
            [clojure.core.async :refer [chan go-loop thread <! >! <!! >!! timeout alt! alts!]]
            [clojure.spec.test.alpha :refer [instrument instrumentable-syms]]
            [clojure.string :as string]
            [clojure.core.async :refer [go-loop chan <! onto-chan timeout]]
            [clojure.tools.logging :as log]
            [clojure.set :refer [superset?]]
            [clj-time.core :refer [now after? plus millis] :as t]
            [async-connect.server :refer [run-server close-wait] :as async-server]
            [async-connect.box :refer [boxed]]
            [async-connect.message :as message]
            [crow.protocol :refer [lease lease-expired registration invalid-message
                                   join-request? heart-beat? discovery? ping?
                                   protocol-error ack call-exception
                                   service-found service-not-found] :as p]
            [crow.request :refer [frame-decorder format-stack-trace packer unpacker] :as request]
            [crow.service :as sv]
            [crow.request :refer [write-with-timeout read-with-timeout]]
            [crow.logging :refer [trace-pr debug-pr info-pr]]
            [crow.utils :refer [extract-exception]])
  (:import [java.util UUID]
           [io.netty.channel
              ChannelPipeline
              ChannelHandler]
           [io.netty.handler.codec.bytes
              ByteArrayDecoder
              ByteArrayEncoder])
  (:gen-class))

(def ^:const default-renewal-ms 10000)
(def ^:const default-watch-interval 2000)

(s/def ::service-info
  (s/keys :req-un [:service/address
                   :service/port
                   :service/service-id
                   :service/name
                   :service/attributes
                   :service/expire-at]))

(s/def ::registrar
  (s/keys :req-un [:registrar/name
                   :registrar/renewal-ms
                   :registrar/watch-interval
                   :registrar/services]))

(s/fdef new-registrar
    :ret ::registrar)

(defn new-registrar
  ([name renewal-ms watch-interval]
   {:name name
    :renewal-ms renewal-ms
    :watch-interval watch-interval
    :services (atom {})}))


(s/fdef new-service-info
    :ret ::service-info)

(defn new-service-info
  [address port service-id name attributes expire-at]
  {:address address
   :port port
   :service-id service-id
   :name name
   :attributes attributes
   :expire-at expire-at})

(defn- new-service-id
  []
  (str (UUID/randomUUID)))

(defn accept-service-registration
  [registrar address port sid service-name attributes]
  (log/info "service registration:" address port sid service-name (pr-str attributes))
  (let [service-id (or sid (new-service-id))
        expire-at  (-> (now) (plus (millis (:renewal-ms registrar))))
        services   (swap! (:services registrar)
                      #(assoc % service-id (new-service-info
                                              address
                                              port
                                              service-id
                                              service-name
                                              attributes
                                              expire-at)))]
    (info-pr "registered:"
      (registration service-id expire-at))))


(defn accept-heartbeat
  [registrar service-id]
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
                             :service-id   (:service-id svc)
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
  [registrar msg]
  (boxed
    (try
      (cond
        (ping? msg)         (do (log/trace "received a ping.") (ack))
        (join-request? msg) (let [{:keys [address port service-id service-name attributes]} msg]
                              (accept-service-registration registrar address port service-id service-name attributes))
        (heart-beat? msg)   (accept-heartbeat registrar (:service-id msg))
        (discovery? msg)    (accept-discovery registrar (:service-name msg) (:attributes msg))
        :else               (invalid-message msg))
      (catch Throwable th th))))

(defn- make-registrar-handler
  [registrar timeout-ms]
  {:pre [registrar]}
  (fn [context read-ch write-ch]
    (go-loop []
      (when-let [msg (<! read-ch)]
        (when (try
                (let [result (<! (thread (handle-request registrar @msg)))
                      resp   #::message{:data @result :flush? true}]
                  (if timeout-ms
                    (alt!
                      [[write-ch resp]]
                      ([v ch] v)

                      [(if timeout-ms (timeout timeout-ms) (chan))]
                      ([v ch]
                        (log/error "Registrar Timeout: Couldn't write response.")
                        false))
                    (>! write-ch resp)))
                (catch Throwable ex
                  (log/error ex "An Error ocurred.")
                  (let [[type throwable] (extract-exception ex)
                        ex-msg (call-exception type (format-stack-trace throwable))]
                    (alts! [[write-ch #::message{:data ex-msg :flush? true}] (timeout timeout-ms)])
                    false)))
          (recur))))))

(defn- channel-initializer
  [netty-ch config]
  (try
    ;; This function may be called on a instance repeatedly by spec-checking.
    ;; so this function must be idempotent.
    (let [pipeline ^ChannelPipeline (.pipeline netty-ch)]
      (doseq [^String n (.names pipeline)]
        (when-let [handler (.context pipeline n)]
          (.remove pipeline ^String n))))

    (.. netty-ch
      (pipeline)
      (addLast "messagepack-framedecoder" (frame-decorder))
      (addLast "bytes-decoder" (ByteArrayDecoder.))
      (addLast "bytes-encoder" (ByteArrayEncoder.)))

    netty-ch

    (catch Throwable th
      (log/error th "init error")
      (throw th))))

(defn start-registrar-service
  "Starting a registrar and wait requests.
  An argument is a map of configurations of keys:

  :port a waiting port number.
  :renewal-ms  milliseconds for make each registered services expired. Services must send a 'lease' request before the expiration.
  :watch-internal  milliseconds for checking each service is expired or not."
  [{:keys [address port name renewal-ms watch-interval send-recv-timeout]
    :or {port 4000, renewal-ms default-renewal-ms, watch-interval default-watch-interval send-recv-timeout nil}}]

  (let [registrar (new-registrar name renewal-ms watch-interval)]
    (process-registrar registrar)
    (let [server (run-server
                   #::async-server{:address                address
                                   :port                   port
                                   :channel-initializer    channel-initializer
                                   :read-channel-builder   (fn [ch] (chan 50 unpacker))
                                   :write-channel-builder  (fn [ch] (chan 50 packer))
                                   :server-handler-factory (fn [host port]
                                                            (make-registrar-handler registrar send-recv-timeout))})]
      (log/info (str "#### REGISTRAR SERVICE (name: " (pr-str name) " port: " (async-server/port server) ") starts."))
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
                              "-m" [:mode v]
                              (throw (IllegalArgumentException. (str "Unknown option: " k))))))
          _      (when (and (:mode optmap) (= (:mode optmap) "development"))
                    (log/info "[SPEC] instrument")
                    (let [targets (->> (instrumentable-syms)
                                       (filter #(string/starts-with? (namespace %) "crow.")))]
                      (instrument targets)))
          server (start-registrar-service
                    (merge {:port (Long/valueOf port-str), :name name} optmap))]
      (close-wait server #(println "SERVER STOPPED.")))))

