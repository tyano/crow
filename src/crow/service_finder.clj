(ns crow.service-finder
  (:require [clojure.core.async :refer [go-loop timeout <! >! <!! go chan]]
            [clojure.set :refer [difference]]
            [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [crow.protocol :refer [ping ack?]]
            [crow.request :as request]
            [crow.registrar-source :as source]
            [crow.logging :refer [trace-pr debug-pr info-pr]]
            [crow.spec :as crow-spec]
            [async-connect.box :refer [boxed]]
            [async-connect.client :as client])
  (:import [java.util UUID Date]))

(s/def ::id #(instance? UUID %))
(s/def ::dead-registrar-check-interval-ms pos-int?)
(s/def ::active-registrars #(instance? clojure.lang.Ref %))
(s/def ::dead-registrars #(instance? clojure.lang.Ref %))
(s/def ::connection-factory ::client/connection-factory)
(s/def ::registrar-source :crow/registrar-source)
(s/def ::stopped #(instance? clojure.lang.IAtom %))

(s/def :crow/service-finder
  (s/keys :req [::id
                ::connection-factory
                ::registrar-source
                ::dead-registrar-check-interval-ms
                ::active-registrars
                ::dead-registrars
                ::stopped]))

;;; checker thread for dead registrars.
;;; If a dead registrar revived, it will return 'ack' for 'ping' request.
;;; If 'ack' is returned, remove the registrar from dead-registrars ref and
;;; add it into active-registrars.
(defn- start-check-dead-registrars-task
  [{::keys [connection-factory
            dead-registrar-check-interval-ms
            dead-registrars
            active-registrars
            stopped]
      :or {dead-registrar-check-interval-ms 30000}
      :as finder}]
  (go-loop []
    (if (true? @stopped)
      (log/info (str "service-finder: check-dead-registrars-task stopped. " (select-keys finder [::id])))
      (let [current-dead-registrars @dead-registrars]
        (doseq [{:keys [address port] :as registrar} current-dead-registrars]
          (try
            (trace-pr "checking: " registrar)
            (let [send-data #::request{:connection-factory connection-factory
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
        (recur)))))


(defn init-service-finder
  "Initialize a servce-finder instance.
  all 'service-finder' must be an Associative (a map or a record) and
  must have some keys, so this functions assoc the keys to the associative
  passed as 'finder'.
    all finder must repeatedly check registrars the finder have. this function
  starts a go-loop for the task.
    returns a initialized service-finder."
  [finder registrar-source]
  {:pre [registrar-source (s/valid? (s/keys :req [::connection-factory]) finder)]}
  (let [finder (merge finder #::{:id (UUID/randomUUID)
                                 :dead-registrar-check-interval-ms 30000
                                 :registrar-source  registrar-source
                                 :active-registrars (ref #{})
                                 :dead-registrars   (ref #{})
                                 :stopped           (atom false)})]
    (start-check-dead-registrars-task finder)
    finder))

;; COMMON FUNCTIONS FOR SERVICE FINDER

(defn reset-registrars!
  [{::keys [registrar-source
                          dead-registrars
                          active-registrars]}]
  (let [new-registrars (source/registrars registrar-source)]
    (dosync
      (alter active-registrars
        (fn [_]
          (difference (set new-registrars) @dead-registrars))))))

(defn abandon-registrar!
  [{::keys [dead-registrars active-registrars]} reg]
  (dosync
    (let [other-reg (first (shuffle (alter active-registrars disj reg)))]
      (alter dead-registrars conj reg)
      other-reg)))


(defprotocol StoppableFinder
  (stop-finder [finder] "stop a service finder and release all resources this finder is holding."))

(defn stoppable?
  [finder]
  (satisfies? StoppableFinder finder))

(defn stop-finder*
  [finder]
  (when-let [stopped (::stopped finder)]
    (reset! stopped true)))

;; STANDARD SERVICE FINDER
(defrecord StandardServiceFinder
  []
  StoppableFinder
  (stop-finder [finder] (stop-finder* finder)))

(defn standard-service-finder
  [connection-factory registrar-source]
  {:pre [registrar-source]}
  (-> #::{:connection-factory connection-factory}
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


(s/def ::service
  (s/keys :req-un []))

(s/def :cache/time inst?)
(s/def ::cache-info
  (s/keys :req
          [:cache/service
           :cache/time]))

(defrecord CachedServiceFinder
  [service-map time-cache]

  StoppableFinder
  (stop-finder
    [finder]
    (stop-finder* finder)
    (dosync
     (ref-set service-map {})
     (ref-set time-cache {}))
    nil)

  ServiceCache
  (clear-cache
    [finder]
    (dosync
     (ref-set service-map {})
     (ref-set time-cache {}))
    finder)

  (remove-service
    [finder service-desc service]
    (when (and service-desc service)
      (dosync
       (alter service-map update service-desc disj service)
       (alter time-cache dissoc (:service-id service))))
    finder)

  (reset-services
    [finder service-desc service-coll]
    (when service-desc
      (trace-pr "reset-services - service-desc : services: " [service-desc service-coll])
      (s/assert ::crow-spec/found-services service-coll)
      (dosync
       (let [old-services (get @service-map service-desc [])]
         (alter service-map assoc service-desc (set service-coll))
         (alter time-cache
                (fn [cache]
                  (let [reset-cache (reduce #(dissoc %1 (:service-id %2))
                                            cache
                                            old-services)]
                    (reduce
                     #(assoc %1 (:service-id %2) (Date.))
                     reset-cache
                     (distinct service-coll))))))))
    finder)

  (add-services
    [finder service-desc service-coll]
    (when (and service-desc (seq service-coll))
      (s/assert ::crow-spec/found-services service-coll)
      (dosync
       (ref-set service-map update service-desc #(apply conj (or % #{}) service-coll))

       (ref-set time-cache (fn [cache]
                             (reduce
                              #(assoc %1 (:service-id %2) (Date.))
                              cache
                              (distinct service-coll))))))
    finder)

  (find-services
    [finder service-desc]
    (when service-desc
      (let [services (get @service-map service-desc)]
        (debug-pr "find-services - service-desc : found-services: " [service-desc services])
        services))))


(s/fdef remove-service-from-cache
    :args (s/cat :finder :crow/service-finder
                 :service ::crow-spec/found-service)
    :ret map?)

(defn- remove-service-from-cache
  [finder service]
  (dosync
   (alter (:service-map finder)
          (fn [service-map]
            (into {}
                  (map
                    (fn [[service-desc service-coll]]
                      [service-desc (set (filter #(not= service %) service-coll))])
                    service-map))))
   (alter (:time-cache finder) dissoc (:service-id service)))
  (log/debug (str "service " service " now is removed from service-cache"))
  nil)


(s/fdef check-cached-services
    :args (s/cat :finder :crow/service-finder
                 :cache-timeout-ms pos-int?))

(defn- check-cached-services
  [finder cache-timeout-ms]
  (let [service-map      @(:service-map finder)
        services         (distinct (apply concat (vals service-map)))
        expired-services (->> (for [service services]
                                (let [time (get @(:time-cache finder) (:service-id service) (Date.))
                                      diff (- (.. (Date.) (getTime)) (.. time (getTime)))]
                                  (when (> diff cache-timeout-ms) service)))
                              (filter some?))]
    (doseq [service expired-services]
      (remove-service-from-cache finder service))))

(defn start-check-cached-services-task
  "Check the activity of cached services and if services are down, remove the services from a cache."
  [cached-finder check-interval-ms cache-timeout-ms]
  (go-loop []
    (if (true? @(::stopped cached-finder))
      (log/info (str "service-finder: check-cached-services-task stopped. " (pr-str (select-keys cached-finder [::id]))))
      (do
        (check-cached-services cached-finder cache-timeout-ms)
        (<! (timeout check-interval-ms))
        (recur)))))

(defn cached-service-finder
  [connection-factory registrar-source check-interval-ms cache-timeout-ms]
  {:pre [registrar-source]}
  (let [finder (-> (->CachedServiceFinder (ref {}) (ref {}))
                   (assoc ::connection-factory connection-factory)
                   (init-service-finder registrar-source))]
    (start-check-cached-services-task finder check-interval-ms cache-timeout-ms)
    finder))



