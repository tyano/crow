(ns crow.join-manager
  (:require [aleph.tcp :as tcp]
            [crow.protocol :refer [join-request heart-beat
                                   lease? lease-expired? registration?
                                   send! recv!] :as protocol]
            [crow.registrar-source :as source]
            [clojure.core.async :refer [chan thread go-loop <! >! onto-chan] :as async]
            [clojure.set :refer [difference]]
            [crow.service :refer [service-id write]]))


(defn- send-request
  [registrar-address registrar-port req]
  (let [stream @(tcp/client {:host registrar-address, :post registrar-port})]
    (send! stream req)
    (recv! stream)))

;;;TODO service-idはクライアント側からも指定できること。
(defn- join-service!
  "send a join request to a registrar and get a new service-id"
  [join-mgr service registrar-address registrar-port]
  (let [req (join-request (:ip-address service) (service-id service) (:name service) (:attributes service))
        msg (send-request registrar-address registrar-port req)]
    (when (registration? msg)
      (let [old-sid (service-id service)]
        (swap! (:service-id-atom service)
          (fn [id] (or id (:service-id msg))))
        (swap! (:service-map join-mgr)
          (fn [service-map]
            (update-in service-map [service]
              (fn [regs]
                (conj regs {:address registrar-address, :port registrar-port})))))
        (let [id-store (:id-store service)
              sid      (service-id service)]
          (write id-store sid))))))

(declare join)

(defn- send-heart-beat!
  [service service-ch registrar-address registrar-port]
  (let [req (heart-beat (service-id service))
        msg (send-request registrar-address registrar-port req)]
      (cond
        (lease? msg) (swap! (:expire-at-atom service) (fn [_] (:expire-at msg)))
        (lease-expired? msg) (join service-ch service))))


;;; registrars - a vector of registrar-info, which is a map with :address and :port of a registrar.
;;; service-map - a map of service-id (key) and a set of registrar-infos of already joined regsitrar (value)
(defrecord JoinManager [registrars service-map])

(defn- join-manager [] (JoinManager. (atom #{}) (atom {})))

(defn- joined?
  "true if 'service-id' is already join to the registrar described by 'registrar-info'."
  [join-mgr service-id registrar-info]
  (boolean
    (let [service-map (deref (:service-map join-mgr))]
      (when-let [registrars (service-map service-id)]
        (registrars registrar-info)))))

(defn- reset-registrars!
  [join-mgr registrars]
  (swap! (deref (:registrars join-mgr)) (fn [_] registrars)))

(defn- run-join-processor
  [join-ch]
  (go-loop []
    (let [{service :service, {:keys [address port]} :registrar-info} (<! join-ch)]
      (thread (join-service! service address port)))
    (recur)))

(defn- run-service-acceptor
  [join-mgr service-ch join-ch]
  (go-loop []
    (let [service     (<! service-ch)
          service-map (deref (:service-map join-mgr))
          registrars  (deref (:registrars join-mgr))
          joined      (if (service-id service)
                        (service-map (:service-id service))
                        [])
          not-joined  (difference registrars joined)
          join-req    (for [reg not-joined]
                        {:service service, :registrar-info reg})]
      (onto-chan join-ch join-req))
    (recur)))

(defn- run-registrar-fetcher
  [join-mgr registrar-source fetch-registrar-interval-ms]
  (thread
    (loop []
      (let [registrars (source/registrars registrar-source)]
        (reset-registrars! join-mgr registrars))
      (Thread/sleep fetch-registrar-interval-ms)
      (recur))))

(defn- run-heart-beat-processor
  [join-mgr service-ch heart-beat-interval-ms]
  (thread
    (loop []
      (let [service-map (deref (:service-map join-mgr))]
        (doseq [[service registrars] service-map]
          (doseq [reg registrars]
            (send-heart-beat! service service-ch (:address reg) (:port reg)))))
      (Thread/sleep heart-beat-interval-ms)
      (recur))))

(defn start-join-manager
  [registrar-source fetch-registrar-interval-ms heart-beat-interval-ms]
  (let [service-ch (chan)
        join-ch    (chan)
        join-mgr   (join-manager)]
    (run-registrar-fetcher registrar-source fetch-registrar-interval-ms)
    (run-service-acceptor join-mgr service-ch join-ch)
    (run-join-processor join-ch)
    (run-heart-beat-processor join-mgr service-ch heart-beat-interval-ms)
    service-ch))

(defn join
  [service-ch service]
  (>! service-ch service))

