(ns crow.discovery
  (:refer-clojure :exclude [send])
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [<! <!! >! go]]
            [crow.protocol :refer [discovery service-found? service-not-found? call-exception?]]
            [crow.request :as request]
            [crow.registrar-source :as source]
            [crow.service :as service]
            [crow.service-finder :refer [reset-registrars! abandon-registrar!] :as finder]
            [crow.spec :as crow-spec]
            [crow.logging :refer [trace-pr info-pr]]
            [async-connect.box :refer [boxed]]))


(defn- discover-with
  [{::finder/keys [connection-factory] :as finder}
   {:keys [address port] :as registrar}
   {:keys [service-name attributes] :as service-desc}
   {:keys [timeout-ms send-retry-count send-retry-interval-ms] :or {send-retry-count 3 send-retry-interval-ms (long 500)} :as options}]
  (trace-pr "options:" options)
  (let [req     (discovery service-name attributes)
        result  (try
                  (let [data #::request{:connection-factory connection-factory
                                        :address address,
                                        :port port
                                        :data req
                                        :timeout-ms timeout-ms
                                        :send-retry-count send-retry-count
                                        :send-retry-interval-ms send-retry-interval-ms}
                        msg (some-> (<!! (request/send data)) (deref))]
                    (cond
                      (nil? msg)
                      nil

                      (service-found? msg)
                      (do
                        (trace-pr "service-found: " msg)
                        (vec (:services msg)))

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
    (s/assert ::crow-spec/found-services result)
    (finder/reset-services finder service-desc result)
    result))

(defn discover
  [{::finder/keys [active-registrars registrar-source] :as finder}
   {:keys [service-name attributes] :as service-desc}
   options]
  ;; find services from a cache if finder has a cache.
  (if-let [services (seq (finder/find-services finder service-desc))]
    services
    (do
      (when-not (seq @active-registrars)
        (reset-registrars! finder))
      (if-let [registrars (seq @active-registrars)]
        (loop [regs (shuffle registrars) result nil]
          (cond
            result result
            (not (seq regs)) (throw (ex-info "Service Not Found." {:type ::service-not-found, :source registrar-source}))
            :else (let [reg (first regs)]
                    (recur (rest regs) (discover-with finder reg service-desc options)))))
        (throw (ex-info "Registrar doesn't exist." {:type ::registrar-doesnt-exist, :source registrar-source}))))))

