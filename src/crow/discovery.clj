(ns crow.discovery
  (:refer-clojure :exclude [send])
  (:require [crow.protocol :refer [discovery service-found? service-not-found? ping ack? call-exception?]]
            [crow.request :as request]
            [crow.registrar-source :as source]
            [crow.service :as service]
            [crow.request :as request]
            [clojure.set :refer [difference]]
            [aleph.tcp :as tcp]
            [manifold.deferred :refer [let-flow chain] :as d]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+]]
            [clojure.core.async :refer [thread go-loop timeout <!]]
            [crow.logging :refer [trace-pr info-pr]]))

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
          (let [msg @(request/send* address port (ping) nil)]
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

(defrecord ServiceFinder [registrar-source active-registrars dead-registrars])

(defn service-finder
  [registrar-source]
  (let [finder (ServiceFinder. registrar-source (ref #{}) (ref #{}))]
    (start-check-dead-registrars-task finder)
    finder))


(defn- reset-registrars!
  [finder]
  (let [new-registrars (source/registrars (:registrar-source finder))]
    (dosync
      (alter (:active-registrars finder)
        (fn [_]
          (difference (set new-registrars) @(:dead-registrars finder)))))))

(defn- abandon-registrar!
  [finder reg]
  (dosync
    (let [other-reg (first (shuffle (alter (:active-registrars finder) disj reg)))]
      (alter (:dead-registrars finder) conj reg)
      other-reg)))

(defn- discover-with
  [finder
   {:keys [address port] :as registrar}
   service-name
   attribute
   {:keys [timeout-ms send-retry-count retry-interval-ms] :or {timeout-ms Long/MAX_VALUE send-retry-count 3 retry-interval-ms (long 500)} :as options}]
  (trace-pr "options:" options)
  (let [req     (discovery service-name attribute)
        result  @(-> (request/send address port req timeout-ms send-retry-count retry-interval-ms)
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

                          (= request/timeout msg)
                          nil

                          (= request/drained msg)
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
      result)))

(defn discover
  [finder service-name attribute options]
  (when-not (seq @(:active-registrars finder))
    (reset-registrars! finder))
  (if-let [registrars (seq @(:active-registrars finder))]
    (loop [regs (shuffle registrars) result nil]
      (cond
        result result
        (not (seq regs)) (throw+ {:type ::service-not-found, :source (:registrar-source finder)})
        :else (let [reg (first regs)]
                (recur (rest regs) (discover-with finder reg service-name attribute options)))))
    (throw+ {:type ::registrar-doesnt-exist, :source (:registrar-source finder)})))

