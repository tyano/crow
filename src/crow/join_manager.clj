(ns crow.join-manager
  (:require [aleph.tcp :as tcp]
            [manifold.deferred :refer [let-flow chain] :as d]
            [crow.protocol :refer [join-request heart-beat
                                   lease? lease-expired? registration?] :as protocol]
            [crow.registrar-source :as source]
            [crow.request :as request]
            [crow.id-store :refer [write]]
            [clojure.core.async :refer [chan thread go-loop <! >!! onto-chan] :as async]
            [clojure.set :refer [difference]]
            [slingshot.slingshot :refer [throw+]]
            [clojure.tools.logging :as log]
            [clj-time.core :refer [now plus after? millis] :as t]
            [crow.logging :refer [trace-pr]]))

(def should-stop (atom false))

(defn- service-id
  [service]
  (deref (:service-id-atom service)))

(defn- join!
  [join-mgr service registrar-address registrar-port msg]
  (let [old-sid (service-id service)
        expire-at (:expire-at msg)]
    (swap! (:service-id-atom service) #(or % (:service-id msg)))
    (swap! (:service-map join-mgr)    #(assoc % (service-id service) service))
    (swap! (:service-registrars-map join-mgr)
      (fn [service-registrars-map]
        (update-in service-registrars-map [(service-id service)]
          (fn [regs]
            (conj regs {:address registrar-address, :port registrar-port, :expire-at expire-at})))))
    (let [sid (service-id service)]
      (let [id-store (:id-store service)]
        (write id-store sid))
      (log/trace (str "joined! service: " (pr-str service) ". service-id: " (service-id service) "."))
      sid)))

;;;TODO service-idはクライアント側からも指定できること。
(defn- join-service!
  "send a join request to a registrar and get a new service-id"
  [join-mgr service registrar-address registrar-port]
  (log/trace "Joinning" (pr-str service) "to" (pr-str {:address registrar-address, :port registrar-port}))
  (let [req (join-request (:ip-address service) (service-id service) (:name service) (:attributes service))]
    (-> (request/send registrar-address registrar-port req)
        (chain
          (fn [msg]
            (cond
              (false? msg) (throw+ {:type ::connection-error})
              (registration? msg) (join! join-mgr service registrar-address registrar-port msg)
              :else (do
                      (trace-pr "illegal message:" msg)
                      (throw+ {:type ::illegal-response
                               :message msg
                               :info {:service service
                                      :registrar-address registrar-address
                                      :registrar-port registrar-port}})))))
        (d/catch Throwable #(log/error % "Error in deffered.")))))

(declare join)

(defn- send-heart-beat!
  [join-mgr service service-ch {:keys [address port expire-at]}]
  (let [req (heart-beat (service-id service))
        msg @(request/send address port req)]
    (trace-pr "msg: " msg)
    (cond
      (false? msg)  false
      (lease? msg)  (do
                      (log/trace "Lease Renewal: " (service-id service))
                      (swap! (:service-registrars-map join-mgr)
                        (fn [service-registrars-map]
                          (update-in service-registrars-map [(service-id service)]
                            (fn [regs]
                              (-> (remove #(and (= (:address %) address) (= (:port %) port)) regs)
                                  (conj {:address address, :port port, :expire-at (:expire-at msg)}))))))
                      true)
      (lease-expired? msg)
                    (do
                      (join service-ch service)
                      true)
      :else (do
              (trace-pr "illegal message:" msg)
              (throw+ {:type ::illegal-response
                       :message msg
                       :info {:service service
                              :registrar-address address
                              :registrar-port port}})))))

;        (d/catch Throwable #(log/error "Error in deffered." %)))))


;;; registrars - a vector of registrar, which is a map with :address and :port of a registrar.
;;; service-registrars-map - a map of service-id (key) and a set of registrars of already joined regsitrar (value)
(defrecord JoinManager [registrars service-registrars-map service-map])

(defn- join-manager [] (JoinManager. (atom #{}) (atom {}) (atom {})))

(defn- joined?
  "true if 'service-id' is already join to the registrar."
  [join-mgr service-id registrar]
  (boolean
    (let [service-registrars-map (deref (:service-registrars-map join-mgr))]
      (when-let [registrars (service-registrars-map service-id)]
        (registrars registrar)))))

(defn- reset-registrars!
  [join-mgr registrars]
  (swap! (:registrars join-mgr) (fn [_] registrars)))

(defn- run-join-processor
  [join-mgr join-ch]
  (go-loop []
    (if @should-stop
      (log/trace "join-processor stopped.")
      (do
        (let [{service :service, {:keys [address port]} :registrar, :as join-info} (<! join-ch)]
          (when (seq join-info)
            (join-service! join-mgr service address port)))
        (recur)))))

(defn- run-service-acceptor
  [join-mgr service-ch join-ch]
  (go-loop []
    (if @should-stop
      (log/trace "service-acceptor stopped.")
      (do
        (let [service     (<! service-ch)
              service-registrars-map (deref (:service-registrars-map join-mgr))
              registrars  (deref (:registrars join-mgr))
              joined      (if (service-id service)
                            (map #(dissoc % :expire-at) (service-registrars-map service-id))
                            [])
              not-joined  (difference registrars joined)
              join-req    (for [reg not-joined]
                            {:service service, :registrar reg})]
          (trace-pr "join-req: " join-req)
          (onto-chan join-ch join-req))
        (recur)))))

(defn- run-registrar-fetcher
  [join-mgr registrar-source fetch-registrar-interval-ms]
  (thread
    (loop []
      (if @should-stop
        (log/trace "registrar-fetcher stopped.")
        (do
          (try
            (log/trace "Resetting registrars from registrar-source.")
            (let [registrars (source/registrars registrar-source)]
              (reset-registrars! join-mgr registrars))
            (Thread/sleep fetch-registrar-interval-ms)
            (catch Throwable th
              (log/error th "registrar-fetcher error.")))
          (recur))))))

(defn- run-heart-beat-processor
  [join-mgr service-ch heart-beat-interval-ms]
  (thread
    (loop []
        (if @should-stop
          (log/trace "heart-beat-processor stopped.")
          (do
            (try
              (let [service-registrars-map (deref (:service-registrars-map join-mgr))
                    service-map (deref (:service-map join-mgr))]
                (doseq [[service-id registrars]     service-registrars-map
                        {:keys [expire-at] :as reg} registrars]
                  (when (after? (plus (now) (millis heart-beat-interval-ms)) expire-at)
                    (when-let [service (service-map service-id)]
                      (log/trace "send heart-beat from" (pr-str service) "to" (pr-str reg))
                      (send-heart-beat! join-mgr service service-ch reg)))))
              (Thread/sleep 500)
              (catch Throwable th
                (log/error th "heart-beat error.")))
            (recur))))))

(defn start-join-manager
  [registrar-source fetch-registrar-interval-ms heart-beat-interval-ms]
  (let [service-ch (chan)
        join-ch    (chan)
        join-mgr   (join-manager)]
    (run-registrar-fetcher join-mgr registrar-source fetch-registrar-interval-ms)
    (run-service-acceptor join-mgr service-ch join-ch)
    (run-join-processor join-mgr join-ch)
    (run-heart-beat-processor join-mgr service-ch heart-beat-interval-ms)
    service-ch))

(defn join
  [service-ch service]
  (>!! service-ch service))

