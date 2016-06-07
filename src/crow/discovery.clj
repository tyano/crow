(ns crow.discovery
  (:refer-clojure :exclude [send])
  (:require [crow.protocol :refer [discovery service-found? service-not-found? call-exception?]]
            [crow.request :as request]
            [crow.registrar-source :as source]
            [crow.service :as service]
            [crow.request :as request]
            [crow.service-finder :refer [reset-registrars! abandon-registrar!] :as finder]
            [aleph.tcp :as tcp]
            [manifold.deferred :refer [chain] :as d]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+]]
            [crow.logging :refer [trace-pr info-pr]]))


(defn- discover-with
  [finder
   {:keys [address port] :as registrar}
   {:keys [service-name attributes] :as service-desc}
   {:keys [timeout-ms send-retry-count send-retry-interval-ms] :or {timeout-ms Long/MAX_VALUE send-retry-count 3 send-retry-interval-ms (long 500)} :as options}]
  (trace-pr "options:" options)
  (let [req     (discovery service-name attributes)
        result  @(-> (request/send address port req timeout-ms send-retry-count send-retry-interval-ms)
                    (chain
                      (fn [msg]
                        (cond
                          (service-found? msg)
                          (do
                             (trace-pr "service-found: " msg)
                             (vec (:services msg)))

                          (service-not-found? msg)
                          nil

                          (call-exception? msg)
                          (let [type-str    (:type msg)
                                stack-trace (:stack-trace msg)]
                            (throw+ {:type (keyword type-str), :stack-trace stack-trace}))

                          (identical? request/timeout msg)
                          nil

                          (identical? request/drained msg)
                          nil

                          :else
                          (throw+ {:type ::illegal-reponse, :response msg}))))
                      (d/catch Throwable
                         (fn [th]
                            (log/error th "An error occured when sending a discovery request.")
                            (abandon-registrar! finder registrar)
                            (throw th))))]
    (if (instance? Throwable result)
      (throw result)
      (do
        (finder/reset-services finder service-desc result)
        result))))

(defn discover
  [finder {:keys [service-name attributes] :as service-desc} options]
  ;; find services from a cache if finder has a cache.
  (if-let [services (seq (finder/find-services finder service-desc))]
    services
    (do
      (when-not (seq @(:active-registrars finder))
        (reset-registrars! finder))
      (if-let [registrars (seq @(:active-registrars finder))]
        (loop [regs (shuffle registrars) result nil]
          (cond
            result result
            (not (seq regs)) (throw+ {:type ::service-not-found, :source (:registrar-source finder)})
            :else (let [reg (first regs)]
                    (recur (rest regs) (discover-with finder reg service-desc options)))))
        (throw+ {:type ::registrar-doesnt-exist, :source (:registrar-source finder)})))))

