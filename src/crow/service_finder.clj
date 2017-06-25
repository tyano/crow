(ns crow.service-finder
  (:require [clojure.core.async :refer [go-loop timeout <! >! <!! go chan]]
            [clojure.set :refer [difference]]
            [crow.protocol :refer [ping ack?]]
            [crow.request :as request]
            [crow.registrar-source :as source]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr debug-pr info-pr]]
            [async-connect.box :refer [boxed]]
            [clojure.spec.alpha :as s]
            [async-connect.client]))

(s/def :service-finder/dead-registrar-check-interval-ms pos-int?)
(s/def :service-finder/active-registrars #(instance? clojure.lang.Ref %))
(s/def :service-finder/dead-registrars #(instance? clojure.lang.Ref %))
(s/def :service-finder/connection-factory :async-connect.client/connection-factory)
(s/def :service-finder/registrar-source :crow/registrar-source)
(s/def :crow/service-finder
  (s/keys :req [:service-finder/connection-factory
                :service-finder/registrar-source
                :service-finder/dead-registrar-check-interval-ms
                :service-finder/active-registrars
                :service-finder/dead-registrars]))

;;; checker thread for dead registrars.
;;; If a dead registrar revived, it will return 'ack' for 'ping' request.
;;; If 'ack' is returned, remove the registrar from dead-registrars ref and
;;; add it into active-registrars.
(defn- start-check-dead-registrars-task
  [{:keys [:service-finder/connection-factory
           :service-finder/dead-registrar-check-interval-ms
           :service-finder/dead-registrars
           :service-finder/active-registrars]
      :or {dead-registrar-check-interval-ms 30000}
      :as finder}]
  (go-loop []
    (let [current-dead-registrars @dead-registrars]
      (doseq [{:keys [address port] :as registrar} current-dead-registrars]
        (try
          (trace-pr "checking: " registrar)
          (let [send-data #:send-request{:connection-factory connection-factory
                                         :address address
                                         :port port
                                         :data (ping)}
                msg (some-> (<! (request/send send-data)) (deref))]
            (if (ack? msg)
              (do
                (info-pr "registrar revived: " registrar)
                (dosync
                  (alter dead-registrars disj registrar)
                  (alter active-registrars conj registrar)))
              (log/error (str "Invalid response:" msg))))
          (catch Throwable e
            (log/debug e))))
      (<! (timeout dead-registrar-check-interval-ms))
      (recur))))


(defn init-service-finder
  "Initialize a servce-finder instance.
  all 'service-finder' must be an Associative (a map or a record) and
  must have some keys, so this functions assoc the keys to the associative
  passed as 'finder'.
    all finder must repeatedly check registrars the finder have. this function
  starts a go-loop for the task.
    returns a initialized service-finder."
  [finder registrar-source]
  {:pre [registrar-source (s/valid? (s/keys :req [:service-finder/connection-factory]) finder)]}
  (let [finder (assoc finder
                  :service-finder/dead-registrar-check-interval-ms 30000
                  :service-finder/registrar-source registrar-source
                  :service-finder/active-registrars (ref #{})
                  :service-finder/dead-registrars   (ref #{}))]
    (start-check-dead-registrars-task finder)
    finder))

;; COMMON FUNCTIONS FOR SERVICE FINDER

(defn reset-registrars!
  [{:keys [:service-finder/registrar-source
           :service-finder/dead-registrars
           :service-finder/active-registrars]}]
  (let [new-registrars (source/registrars registrar-source)]
    (dosync
      (alter active-registrars
        (fn [_]
          (difference (set new-registrars) @dead-registrars))))))

(defn abandon-registrar!
  [{:keys [:service-finder/dead-registrars
           :service-finder/active-registrars]}
   reg]
  (dosync
    (let [other-reg (first (shuffle (alter active-registrars disj reg)))]
      (alter dead-registrars conj reg)
      other-reg)))


;; STANDARD SERVICE FINDER
(defn standard-service-finder
  [connection-factory registrar-source]
  {:pre [registrar-source]}
  (-> {:service-finder/connection-factory connection-factory}
      (init-service-finder registrar-source)))


;; CACHED SERVICE FINDER

(defprotocol ServiceCache
  (clear-cache [finder])
  (remove-service [finder service-desc service])
  (reset-services [finder service-desc service-coll])
  (add-services [finder service-desc service-coll])
  (find-services [finder service-desc]))

(extend-protocol ServiceCache
  Object
  (clear-cache [finder] finder)
  (remove-service [finder service-desc service] finder)
  (reset-services [finder service-desc service-coll] finder)
  (add-services [finder service-desc service-coll] finder)
  (find-services [finder service-desc] nil))

(defrecord CachedServiceFinder
  [service-map]

  ServiceCache
  (clear-cache
    [finder]
    (reset! service-map {})
    finder)

  (remove-service
    [finder service-desc service]
    (when (and service-desc service)
      (swap! service-map update service-desc disj service))
    finder)

  (reset-services
    [finder service-desc service-coll]
    (when service-desc
      (trace-pr "reset-services - service-desc : services: " [service-desc service-coll])
      (swap! service-map assoc service-desc (set service-coll)))
    finder)

  (add-services
    [finder service-desc service-coll]
    (when (and service-desc (seq service-coll))
      (swap! service-map update service-desc #(apply conj (or % #{}) service-coll)))
    finder)

  (find-services
    [finder service-desc]
    (when service-desc
      (let [services (get @service-map service-desc)]
        (debug-pr "find-services - service-desc : found-services: " [service-desc services])
        services))))

(defn- send-ping
  [{:keys [:service-finder/connection-factory] :as finder}
   {:keys [address port] :as service}
   timeout-ms
   send-retry-count
   send-retry-interval-ms]

  (let [send-data #:send-request{:connection-factory connection-factory
                                 :address address
                                 :port port
                                 :data (ping)
                                 :timeout-ms timeout-ms
                                 :send-retry-count send-retry-count
                                 :send-retry-interval-ms send-retry-interval-ms}
        read-ch   (request/send send-data)
        result-ch (chan)]
    (go
      (let [result (try
                      (let [resp (some-> (<! read-ch) (deref))]
                          (cond
                            (nil? resp)
                            false

                            (ack? resp)
                            true

                            :else
                             (throw (IllegalStateException. (str "Unknown response: " (pr-str resp))))))

                      (catch Throwable th
                        ;; a service doesn't respond.
                        ;; remove the service from a cache
                        (log/info th (str "service " service " is dead."))
                        (swap! (:service-map finder)
                          (fn [service-map]
                            (into {}
                              (map
                                (fn [[service-desc service-coll]]
                                  [service-desc (set (filter #(not= service %) service-coll))])
                                service-map))))
                        false))]
        (>! result-ch (boxed result))))
    result-ch))

(defn- check-cached-services
  [finder timeout-ms send-retry-count send-retry-interval-ms]
  (let [service-map @(:service-map finder)
        services    (distinct (apply concat (vals service-map)))]
    (doseq [service services]
      (send-ping finder service timeout-ms send-retry-count send-retry-interval-ms))))

(defn start-check-cached-services-task
  "Check the activity of cached services and if services are down, remove the services from a cache."
  [cached-finder check-interval-ms timeout-ms send-retry-count send-retry-interval-ms]
  (go-loop []
    (check-cached-services cached-finder timeout-ms send-retry-count send-retry-interval-ms)
    (<! (timeout check-interval-ms))
    (recur)))

(defn cached-service-finder
  [connection-factory registrar-source check-interval-ms timeout-ms send-retry-count send-retry-interval-ms]
  {:pre [registrar-source]}
  (let [finder (-> (CachedServiceFinder. (atom {}))
                   (assoc :service-finder/connection-factory connection-factory)
                   (init-service-finder registrar-source))]
    (start-check-cached-services-task finder check-interval-ms timeout-ms send-retry-count send-retry-interval-ms)
    finder))



