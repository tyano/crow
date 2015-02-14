(ns crow.join-manager
  (:require [aleph.tcp :as tcp]
            [manifold.deferred :refer [let-flow chain] :as d]
            [crow.protocol :refer [join-request heart-beat ping
                                   ack? lease? lease-expired? registration?] :as protocol]
            [crow.registrar-source :as source]
            [crow.request :as request]
            [crow.id-store :refer [write]]
            [clojure.core.async :refer [chan thread go-loop <! >!! onto-chan] :as async]
            [clojure.set :refer [difference select] :as st]
            [slingshot.slingshot :refer [throw+]]
            [clojure.tools.logging :as log]
            [clj-time.core :refer [now plus after? millis] :as t]
            [crow.logging :refer [trace-pr debug-pr]]))

(def should-stop (atom false))

(defrecord JoinManager [registrars dead-registrars managed-services])
(defn- join-manager [] (JoinManager. (ref #{}) (ref #{}) (ref #{})))




(defn- service-id
  [service]
  (deref (:service-id-ref service)))

(defn- same-service?
  [s1 s2]
  (and (= (:name s1) (:name s2)) (= (:attributes s1) (:attributes s2))))

(defn- same-registrar?
  [r1 r2]
  (and (= (:address r1) (:address r2)) (= (:port r1) (:port r2))))



(defn- accept-lease!
  [join-mgr service {:keys [address port] :as registrar} expire-at]
  (dosync
    (alter (:registrars service)
      #(-> (select (complement (partial same-registrar? registrar)) %)
           (conj {:address address, :port port, :expire-at expire-at})))))

(defn- joined-to-registrar!
  [join-mgr service {:keys [address port] :as registrar} sid expire-at]
  (dosync
    (ref-set (:service-id-ref service) sid) ;storing a new service id from message. this action must be done at first.
    (alter (:registrars service)
      #(-> (select (complement (partial same-registrar? registrar)) %)
           (conj {:address address, :port port, :expire-at expire-at})))
    (alter (:managed-services join-mgr) conj service)))

(defn- service-expired!
  [join-mgr service {:keys [address port]}]
  (let [registrar {:address address, :port port}]
    (dosync
      (alter (:registrars service) #(select (complement (partial same-registrar? registrar)) %)))))

(defn- registrar-died!
  [join-mgr service {:keys [address port]}]
  (let [registrar {:address address, :port port}]
    (dosync
      (alter (:registrars join-mgr) disj registrar)
      (alter (:dead-registrars join-mgr) conj registrar)
      (alter (:registrars service) #(select (complement (partial same-registrar? registrar)) %)))))

(defn- reset-registrars!
  [join-mgr registrars]
  (dosync
    (alter (:registrars join-mgr) (fn [_] (difference (set registrars) @(:dead-registrars join-mgr))))))

(defn- registrar-revived!
  [join-mgr registrar]
  (dosync
    (alter (:dead-registrars join-mgr) disj registrar)
    (alter (:registrars join-mgr) conj registrar)))




(defn- join!
  [join-mgr service registrar msg]
  (let [received-sid (:service-id msg)
        expire-at    (:expire-at msg)]
    (joined-to-registrar! join-mgr service registrar received-sid expire-at)
    (let [sid (service-id service)]
      (let [id-store (:id-store service)]
        (write id-store sid))
      (log/debug (str "joined! service: " (pr-str service) ". service-id: " sid "."))
      sid)))

(defn- join-service!
  "send a join request to a registrar and get a new service-id"
  [join-mgr service {:keys [address port] :as registrar}]
  (log/debug "Joinning" (pr-str service) "to" (pr-str registrar))
  (let [req (join-request (:address service) (:port service) (service-id service) (:name service) (:attributes service))]
    (-> (request/send address port req)
        (chain
          (fn [msg]
            (cond
              (false? msg) (throw+ {:type ::connection-error})
              (registration? msg) (join! join-mgr service registrar msg)
              :else (do
                      (debug-pr "illegal message:" msg)
                      (throw+ {:type ::illegal-response
                               :message msg
                               :info {:service service
                                      :registrar-address address
                                      :registrar-port port}})))))
        (d/catch
          (fn [e]
            (dosync
              (alter (:managed-services join-mgr) conj service)
              (registrar-died! join-mgr service registrar))
            (throw e))))))

(declare join)

(defn- send-heart-beat!
  [join-mgr service {:keys [address port expire-at], :as registrar}]
  (let [req (heart-beat (service-id service))]
    (-> (request/send address port req)
        (chain
          (fn [msg]
            (cond
              (false? msg)
                  false
              (lease? msg)
                  (do
                    (log/trace "Lease Renewal: " (service-id service))
                    (accept-lease! join-mgr service registrar (:expire-at msg))
                    true)
              (lease-expired? msg)
                  (do
                    (log/trace "expired: " (service-id service))
                    (service-expired! join-mgr service registrar)
                    false)
              :else
                  (do
                    (trace-pr "illegal message:" msg)
                    (throw+ {:type ::illegal-response
                             :message msg
                             :info {:service service
                                    :registrar-address address
                                    :registrar-port port}})))))
        (d/catch
          (fn [e]
            (registrar-died! join-mgr service registrar)
            (throw e))))))


(defn- joined?
  "true if a service is already join to the registrar."
  [service registrar]
  (boolean
    (when-let [registrars (not-empty @(:registrars service))]
      (registrars registrar))))

(defn- run-join-processor
  [join-mgr join-ch]
  (go-loop []
    (if @should-stop
      (log/info "join-processor stopped.")
      (do
        (try
          (let [{:keys [service registrar], :as join-info} (<! join-ch)]
            (when (seq join-info)
              (-> (join-service! join-mgr service registrar)
                  (d/catch
                    #(log/error % "An exception occured when joining.")))))
          (catch Throwable e
            (log/error e "join-processor error.")))
        (recur)))))

(defn- run-service-acceptor
  [join-mgr service-ch join-ch]
  (go-loop []
    (if @should-stop
      (log/info "service-acceptor stopped.")
      (do
        (try
          (when-let [service (<! service-ch)]
            (let [[joined-registrars registrars] (dosync [@(:registrars service) @(:registrars join-mgr)])
                  joined      (map #(dissoc % :expire-at) joined-registrars)
                  not-joined  (difference registrars joined)
                  join-req    (for [reg not-joined] {:service service, :registrar reg})]
                (when (seq join-req)
                  (onto-chan join-ch join-req false))))
          (catch Throwable e
            (log/error e "service-acceptor error.")))
        (recur)))))

(defn- run-registrar-fetcher
  [join-mgr registrar-source fetch-registrar-interval-ms]
  (thread
    (loop []
      (if @should-stop
        (log/info "registrar-fetcher stopped.")
        (do
          (try
            (log/debug "Resetting registrars from registrar-source.")
            (let [registrars (source/registrars registrar-source)]
              (reset-registrars! join-mgr registrars))
            (Thread/sleep fetch-registrar-interval-ms)
            (catch Throwable th
              (log/error th "registrar-fetcher error.")))
          (recur))))))

(defn- run-heart-beat-processor
  [join-mgr heart-beat-interval-ms]
  (thread
    (loop []
      (if @should-stop
        (log/info "heart-beat-processor stopped.")
        (do
          (try
            (doseq [[service reg]
                      (dosync
                        (for [service @(:managed-services join-mgr)
                              {:keys [expire-at] :as reg} @(:registrars service)
                              :when (after? (plus (now) (millis heart-beat-interval-ms)) expire-at)]
                          [service reg]))]
              (log/debug "send heart-beat from" (pr-str service) "to" (pr-str reg))
              (-> (send-heart-beat! join-mgr service reg)
                  (d/catch
                    #(log/error % "Could not send heart-beat to " (pr-str reg)))))
            (Thread/sleep 500)

            (catch Throwable th
              (log/error th "heart-beat-processor error.")))
          (recur))))))

(defn- run-join-to-expired-registrar
  [join-mgr service-ch rejoin-interval-ms]
  (thread
    (loop []
      (if @should-stop
        (log/info "join-to-expired-registrar stopped.")
        (do
          (try
            (onto-chan service-ch @(:managed-services join-mgr) false)
            (Thread/sleep rejoin-interval-ms)
            (catch Throwable e
              (log/error e "join-to-expired-registrar error.")))
          (recur))))))

(defn- run-dead-registrar-checker
  [join-mgr dead-registrar-check-interval]
  (thread
    (loop []
      (if @should-stop
        (log/info "dead-registrar-checker stopped.")
        (do
          (try
            (doseq [{:keys [address port] :as registrar} @(:dead-registrars join-mgr)]
              (let [req (ping)]
                @(chain (request/send address port req)
                  (fn [resp]
                    (cond
                      (ack? resp)
                        (do
                          (log/trace "A registrar revived: " (pr-str registrar))
                          (registrar-revived! join-mgr registrar))
                      :else nil)))))
            (catch Throwable th
              ;; dead-registrar-checker usually get an error when checking registrars,
              ;; because the purpose of this thread is accessing to 'dead' registrars for checking
              ;; it is alive or not. If a registrar is 'dead' yet, the access will cause an error.
              ;; So if we print the error with ERROR level, verbose error logs will be printed.
              ;; It should be printed only debugging.
              (log/debug th "dead-registrar-checker error.")))
          (Thread/sleep dead-registrar-check-interval)
          (recur))))))

(defn start-join-manager
  [registrar-source fetch-registrar-interval-ms dead-registrar-check-interval heart-beat-interval-ms rejoin-interval-ms]
  (let [service-ch (chan)
        join-ch    (chan)
        join-mgr   (join-manager)]
    (run-registrar-fetcher join-mgr registrar-source fetch-registrar-interval-ms)
    (run-service-acceptor join-mgr service-ch join-ch)
    (run-join-processor join-mgr join-ch)
    (run-heart-beat-processor join-mgr heart-beat-interval-ms)
    (run-join-to-expired-registrar join-mgr service-ch rejoin-interval-ms)
    (run-dead-registrar-checker join-mgr dead-registrar-check-interval)
    service-ch))

(defn join
  [service-ch service]
  (>!! service-ch service))

