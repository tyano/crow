(ns crow.service
  (:require [aleph.tcp :refer [put! take!] :as tcp]
            [manifold.deferred :refer [let-flow] :as d]
            [crow.protocol :refer [join-request heart-beat
                                   lease? lease-expired? registration?
                                   unpack-message] :as protocol]
            [crow.registrar-source :as source]
            [clojure.core.async [chan thread go-loop <! onto-chan] :as async]
            [clojure.set :refer [difference]]))



(defrecord Service [ip-address service-id-atom expire-at-atom registrars])

(defn new-service
  [ip-address]
  (Service. ip-address (atom nil) (atom [])))

(defn service-id
  [service]
  (deref (:service-id-atom service)))

(defn expire-at
  [service]
  (deref (:expire-at-atom service)))

(defn- send-request
  [registrar-address registrar-port req]
  (let-flow [stream (tcp/client {:host registrar-address, :post registrar-port})]
    (put! stream req)
    (let [resp (take! stream)]
      (unpack-message resp))))

;;;TODO service-idはクライアント側からも指定できること。
(defn join-service
  "send a join request to a registrar and get a new service-id"
  [service registrar-address registrar-port]
  (let [req (join-request (:ip-address service))
        msg (send-request registrar-address registrar-port req)]
    (when (registration? msg)
      (swap! (:service-id-atom service) (fn [id] (or id (:service-id msg)))))))


(defn send-heart-beat
  [service registrar-address registrar-port]
  (let [req (heart-beat (service-id service))]
    (let-flow [stream (tcp/client {:host registrar-address, :post registrar-port})]
      (put! stream req)
      (let [resp (take! stream)
            msg  (send-request registrar-address registrar-port req)]
        (cond
          (lease? msg) (swap! (:expire-at-atom service) (fn [_] (:expire-at msg)))
          (lease-expired? msg) msg)))))


;;; registrars - a vector of registrar-info, which is a map with :address and :port of a registrar.
;;; service-map - a map of service-id (key) and a set of registrar-infos of already joined regsitrar (value)
(defrecord JoinManager [registrars service-map])

(defn join-manager [] (JoinManager. (atom #{}) (atom {})))

(defn joined?
  "true if 'service-id' is already join to the registrar described by 'registrar-info'."
  [join-mgr service-id registrar-info]
  (boolean
    (let [service-map (deref (:service-map join-mgr))]
      (when-let [registrars (service-map service-id)]
        (registrars registrar-info)))))

(defn reset-registrars!
  [join-mgr registrars]
  (swap! (deref (:registrars join-mgr)) (fn [_] registrars)))

(defn joiner
  [join-ch]
  (go-loop []
    (let [{:keys [service {:keys [address port]}]} (<!! join-ch)]
      (thread (join-service service address port)))
    (recur)))

(defn join-handler
  [join-mgr service-ch join-ch]
  (go-loop []
    (let [service     (<! service-ch)
          service-map (deref (:service-map join-mgr))
          registrars  (deref (:registrars join-mgr))
          joined      (if (service-id service)
                        (service-map (:service-id service))
                        [])
          not-joined  (difference registrars joind)]
      (onto-chan join-ch not-joined))
    (recur)))

(defn start-join-manager
  [registrar-source]
  (let [service-ch (chan)
        join-ch (chan)]
    (thread
      (loop []
        (let [registrars (source/registrars registrar-source)]

        (recur)))))




