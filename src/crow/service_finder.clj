(ns crow.service-finder
  (:require [clojure.core.async :refer [go-loop timeout <!]]
            [clojure.set :refer [difference]]
            [crow.protocol :refer [ping ack?]]
            [crow.request :as request]
            [crow.registrar-source :as source]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr info-pr]]
            [manifold.deferred :refer [chain] :as d]))

(def ^:dynamic *dead-registrar-check-interval-ms* 30000)



;;; checker thread for dead registrars.
;;; If a dead registrar revived, it will return 'ack' for 'ping' request.
;;; If 'ack' is returned, remove the registrar from dead-registrars ref and
;;; add it into active-registrars.
(defn- start-check-dead-registrars-task
  [finder]
  (go-loop []
    (let [current-dead-registrars @(:dead-registrars finder)]
      (doseq [{:keys [address port] :as registrar} current-dead-registrars]
        (try
          (trace-pr "checking: " registrar)
          (let [msg @(request/send address port (ping) nil)]
            (if (ack? msg)
              (do
                (info-pr "registrar revived: " registrar)
                (dosync
                  (alter (:dead-registrars finder) disj registrar)
                  (alter (:active-registrars finder) conj registrar)))
              (log/error (str "Invalid response:" msg))))
          (catch Throwable e
            (log/debug e))))
      (<! (timeout *dead-registrar-check-interval-ms*))
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
  {:pre [registrar-source (or (nil? finder) (associative? finder))]}
  (let [finder (assoc finder
                  :registrar-source  registrar-source
                  :active-registrars (ref #{})
                  :dead-registrars   (ref #{}))]
    (start-check-dead-registrars-task finder)
    finder))

;; COMMON FUNCTIONS FOR SERVICE FINDER

(defn reset-registrars!
  [finder]
  (let [new-registrars (source/registrars (:registrar-source finder))]
    (dosync
      (alter (:active-registrars finder)
        (fn [_]
          (difference (set new-registrars) @(:dead-registrars finder)))))))

(defn abandon-registrar!
  [finder reg]
  (dosync
    (let [other-reg (first (shuffle (alter (:active-registrars finder) disj reg)))]
      (alter (:dead-registrars finder) conj reg)
      other-reg)))


;; STANDARD SERVICE FINDER

(defrecord StandardServiceFinder [])

(defn standard-service-finder
  [registrar-source]
  {:pre [registrar-source]}
  (-> (StandardServiceFinder.)
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
        (trace-pr "find-services - service-desc : found-services: " [service-desc services])
        services))))

(defn- send-ping
  [finder {:keys [address port] :as service} timeout-ms send-retry-count send-retry-interval-ms]
  (try
    (let [req (ping)]
      @(-> (request/send address port req timeout-ms send-retry-count send-retry-interval-ms)
           (chain
             (fn [resp]
               (cond
                 (ack? resp)
                 true

                 :else
                 (throw (IllegalStateException. "Unknown response")))))))
    (catch Throwable th
      ;; a service doesn't respond.
      ;; remove the service from a cache
      (log/info (str "service " service " is dead."))
      (swap! (:service-map finder)
        (fn [service-map]
          (into {}
            (map
              (fn [[service-desc service-coll]]
                [service-desc (filter #(not= service %) service-coll)])
              service-map))))
      false)))

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
  [registrar-source check-interval-ms timeout-ms send-retry-count send-retry-interval-ms]
  {:pre [registrar-source]}
  (let [finder (-> (CachedServiceFinder. (atom {}))
                 (init-service-finder registrar-source))]
    (start-check-cached-services-task finder check-interval-ms timeout-ms send-retry-count send-retry-interval-ms)
    finder))



