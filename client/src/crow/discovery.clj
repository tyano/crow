(ns crow.discovery
  (:refer-clojure :exclude [send])
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [<! <!! >! go]]
            [crow.protocol :refer [discovery service-found? service-not-found? call-exception?]]
            [crow.request :as request]
            [crow.registrar-source :as source]
            [crow.service-finder :refer [reset-registrars! abandon-registrar!] :as finder]
            [crow.logging :refer [trace-pr info-pr]]
            [crow.service-descriptor :as service-desc]
            [crow.call-options :as call-opts]
            [crow.discovery.service :as service-info]
            [crow.utils :refer [assoc-key-ns select-ns-keys]]
            [async-connect.box :refer [boxed]]))


(s/def ::discovery-info
  (s/keys :req [::service-desc/service-name
                ::service-desc/attributes
                ::call-opts/timeout-ms]))


(defn- discover-with
  [{::finder/keys [connection-factory] :as finder}
   {:keys [address port] :as registrar}
   {::service-desc/keys [service-name attributes]
    ::call-opts/keys [timeout-ms] :as discovery-info}]
  (let [req     (discovery service-name attributes)
        result  (try
                  (let [data #::request{:connection-factory connection-factory
                                        :address address,
                                        :port port
                                        :data req
                                        :timeout-ms timeout-ms}
                        msg (some-> (<!! (request/send data)) (deref))]
                    (cond
                      (nil? msg)
                      nil

                      (service-found? msg)
                      (do
                        (trace-pr "service-found: " msg)
                        (->> (:services msg)
                             (map #(assoc-key-ns % :crow.discovery.service))
                             (vec)))

                      (service-not-found? msg)
                      nil

                      (call-exception? msg)
                      (let [type-str    (:type msg)
                            stack-trace (:stack-trace msg)]
                        (throw (ex-info "Discovery failed for an exception." {:type (keyword type-str), :stack-trace stack-trace})))

                      (= ::request/timeout msg)
                      nil

                      :else
                      (throw (ex-info "Illegal Response." {:type ::illegal-reponse, :response msg}))))
                  (catch Throwable th
                    (log/error th "An error occured when sending a discovery request.")
                    (abandon-registrar! finder registrar)
                    (throw th)))]
    (s/assert (s/coll-of :crow.discovery/service) result)
    (finder/reset-services finder
                           (select-ns-keys discovery-info :crow.service-descriptor)
                           result)
    result))

(defn discover
  [{::finder/keys [active-registrars registrar-source] :as finder}
   discovery-info]
  ;; find services from a cache if finder has a cache.
  (if-let [services (seq (finder/find-services finder (select-ns-keys discovery-info :crow.service-descriptor)))]
    services
    (if-let [registrars (seq (dosync
                              (if-let [registrars (not-empty (ensure active-registrars))]
                                registrars
                                (do
                                  (reset-registrars! finder)
                                  (ensure active-registrars)))))]
      (loop [regs (shuffle registrars) result nil]
        (cond
          result result
          (not (seq regs)) (throw (ex-info "Service Not Found." {:type ::service-not-found, :source registrar-source}))
          :else (let [reg (first regs)]
                  (recur (rest regs) (discover-with finder reg discovery-info)))))
      (throw (ex-info "Registrar doesn't exist." {:type ::registrar-doesnt-exist, :source registrar-source})))))
