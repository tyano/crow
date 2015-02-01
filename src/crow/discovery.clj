(ns crow.discovery
  (:require [crow.protocol :refer [discovery service-found? service-not-found? send! recv! ping ack?]]
            [crow.registrar-source :as source]
            [crow.service :as service]
            [clojure.set :refer [difference]]
            [aleph.tcp :as tcp]
            [manifold.deferred :refer [let-flow chain]]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+]]
            [clojure.core.async :refer [thread]]))

(def active-registrars (ref #{}))
(def dead-registrars (ref #{}))

(def check-thread-started (atom false))

(def ^:dynamic *dead-registrar-check-interval-ms* 30000)

;;; checker thread for dead registrars.
;;; If a dead registrar revived, it will return 'ack' for 'ping' request.
;;; If 'ack' is returned, remove the registrar from dead-registrars ref and
;;; add it into active-registrars.
(defn- start-check-dead-registrars-task
  []
  (thread
    (while true
      (let [current-dead-registrars (vec @dead-registrars)]
        (doseq [{:keys [address port] :as registrar} current-dead-registrars]
          (let [stream @(tcp/client {:host address, :port port})]
            (send! stream (ping))
            (let [msg (recv! stream)]
              (if (ack? msg)
                (dosync
                  (alter dead-registrars disj registrar)
                  (alter active-registrars conj registrar))
                (log/error "Invalid response:" msg)))))
        (Thread/sleep *dead-registrar-check-interval-ms*)))))

(defn- reset-registrars!
  [registrar-source]
  (let [new-registrars (source/registrars registrar-source)]
    (dosync
      (alter active-registrars
        (fn [_]
          (difference new-registrars @dead-registrars))))))

(defn- abandon-registrar!
  [reg]
  (dosync
    (let [reg (first (shuffle (alter active-registrars disj reg)))]
      (alter dead-registrars conj reg)
      reg)))

(defn- discover-with
  [{:keys [address port] :as registrar} service-name attribute]
  (let [req    (discovery service-name attribute)
        stream @(tcp/client {:host address, :port port})]
    (try
      (send! stream req)
      (let [resp (recv! stream)]
        (cond
          (service-found? resp) resp
          (service-not-found? resp) nil
          :else (do
                  (log/error "Illegal response:" (pr-str resp))
                  nil)))
      (catch Throwable th
        (log/error "An error occured when sending a discovery request." th)
        (abandon-registrar! registrar)
        nil))))

(defn discover
  [registrar-source service-name attribute]
  (when-not (seq @active-registrars)
    (reset-registrars! registrar-source))
  (locking check-thread-started
    (when-not @check-thread-started
      (start-check-dead-registrars-task)
      (reset! check-thread-started true)))
  (if-let [registrars (seq @active-registrars)]
    (loop [regs (shuffle registrars) result nil]
      (cond
        result result
        (not (seq regs)) (throw+ {:type ::service-not-found, :source registrar-source})
        :else (let [reg (first regs)]
                (recur (rest regs) (discover-with reg service-name attribute)))))
    (throw+ {:type ::registrar-doesnt-exist, :source registrar-source})))

