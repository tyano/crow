(ns crow.discovery
  (:require [crow.protocol :refer [discovery service-found? service-not-found? send! recv! ping ack?]]
            [crow.registrar-source :as source]
            [crow.service :as service]
            [crow.request :as request]
            [clojure.set :refer [difference]]
            [aleph.tcp :as tcp]
            [manifold.deferred :refer [let-flow chain] :as d]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+]]
            [clojure.core.async :refer [thread]]))

(def ^:dynamic *dead-registrar-check-interval-ms* 30000)


(declare dead-registrars)

;;; checker thread for dead registrars.
;;; If a dead registrar revived, it will return 'ack' for 'ping' request.
;;; If 'ack' is returned, remove the registrar from dead-registrars ref and
;;; add it into active-registrars.
(defn- start-check-dead-registrars-task
  [finder]
  (thread
    (while true
      (let [current-dead-registrars (vec (dead-registrars finder))]
        (doseq [{:keys [address port] :as registrar} current-dead-registrars]
          (let [stream @(tcp/client {:host address, :port port})]
            (send! stream (ping))
            (let [msg (recv! stream)]
              (if (ack? msg)
                (dosync
                  (alter (:dead-registrars finder) disj registrar)
                  (alter (:active-registrars finder) conj registrar))
                (log/error "Invalid response:" msg)))))
        (Thread/sleep *dead-registrar-check-interval-ms*)))))



(defrecord ServiceFinder [registrar-source active-registrars dead-registrars])

(defn service-finder
  [registrar-source]
  (let [finder (ServiceFinder. registrar-source (ref #{}) (ref #{}))]
    (start-check-dead-registrars-task finder)
    finder))

(defn active-registrars [finder] (deref (:active-registrars finder)))
(defn dead-registrars [finder] (deref (:dead-registrars finder)))



(defn- reset-registrars!
  [finder]
  (let [new-registrars (source/registrars (:registrar-source finder))]
    (dosync
      (alter (:active-registrars finder)
        (fn [_]
          (difference new-registrars (dead-registrars finder)))))))

(defn- abandon-registrar!
  [finder reg]
  (dosync
    (let [reg (first (shuffle (alter (:active-registrars finder) disj reg)))]
      (alter (:dead-registrars finder) conj reg)
      reg)))

(defn- discover-with
  [{:keys [address port] :as registrar} service-name attribute]
  (let [req     (discovery service-name attribute)
        result  @(-> (request/send address port req)
                     (chain
                       (fn [msg]
                         (cond
                           (service-found? msg)
                              msg
                           (service-not-found? msg)
                              nil
                           :else
                              (throw+ {:type ::illegal-reponse, :response msg}))))
                     (d/catch Throwable
                        (fn [th]
                           (log/error "An error occured when sending a discovery request." th)
                           (abandon-registrar! registrar)
                           th)))]
    (if (instance? Throwable result)
      (throw result)
      result)))

(defn discover
  [finder service-name attribute]
  (when-not (seq (active-registrars finder))
    (reset-registrars! finder))
  (if-let [registrars (seq (active-registrars finder))]
    (loop [regs (shuffle registrars) result nil]
      (cond
        result result
        (not (seq regs)) (throw+ {:type ::service-not-found, :source (:registrar-source finder)})
        :else (let [reg (first regs)]
                (recur (rest regs) (discover-with reg service-name attribute)))))
    (throw+ {:type ::registrar-doesnt-exist, :source (:registrar-source finder)})))

