(ns crow.join-manager
  (:require [crow.protocol :refer [join-request heart-beat ping
                                   ack? lease? lease-expired? registration?] :as protocol]
            [crow.registrar-source :as source]
            [crow.request :as request]
            [crow.id-store :refer [write]]
            [clojure.core.async :refer [chan go-loop <! >! timeout <!! >!! onto-chan go] :as async]
            [clojure.set :refer [difference select] :as st]
            [slingshot.slingshot :refer [throw+]]
            [clojure.tools.logging :as log]
            [clj-time.core :refer [now plus after? millis] :as t]
            [crow.logging :refer [trace-pr debug-pr]]
            [async-connect.box :refer [boxed]])
  (:import [java.net
            InetAddress
            Inet4Address
            Inet6Address
            NetworkInterface]))

(def should-stop (atom false))

(defrecord JoinManager [connection-factory registrars dead-registrars managed-services])
(defn- join-manager [connection-factory] (JoinManager. connection-factory (ref #{}) (ref #{}) (ref #{})))




(defn- service-id
  [service]
  (deref (:service-id-ref service)))

(defn sort-addresses
  [addresses]
  (sort-by #(cond (instance? Inet4Address %) 0
                  (instance? Inet6Address %) 1
                  :else 2)
           addresses))

(defn- find-one-public-address
  []
  (or
    (->> (enumeration-seq (NetworkInterface/getNetworkInterfaces))
         (filter #(not (.isLoopback %)))
         (mapcat #(enumeration-seq (.getInetAddresses %)))
         (sort-addresses)
         (first))
    (->> (enumeration-seq (NetworkInterface/getNetworkInterfaces))
         (filter #(.isLoopback %))
         (first))))

(defn service-address
  [service]
  (or (:address service)
      (some-> (find-one-public-address) (.getHostAddress))))

(defn- same-service?
  [s1 s2]
  (and (= (:name s1) (:name s2)) (= (:attributes s1) (:attributes s2))))

(defn- same-registrar?
  [r1 r2]
  (and (= (:address r1) (:address r2)) (= (:port r1) (:port r2))))

(defn- remove-registrar-fn
  [registrar]
  (fn [registrar-set]
    (select (complement (partial same-registrar? registrar)) registrar-set)))

(defn- remove-registrar
  [registrar registrar-set]
  ((remove-registrar-fn registrar) registrar-set))


(defn- accept-lease!
  [join-mgr service {:keys [address port] :as registrar} expire-at]
  (dosync
    (alter (:registrars service)
      #(-> (remove-registrar registrar %)
           (conj {:address address, :port port, :expire-at expire-at})))))

(defn- joined-to-registrar!
  [join-mgr service {:keys [address port] :as registrar} sid expire-at]
  (dosync
    (ref-set (:service-id-ref service) sid) ;storing a new service id from message. this action must be done at first.
    (alter (:registrars service)
      #(-> (remove-registrar registrar %)
           (conj {:address address, :port port, :expire-at expire-at})))))

(defn- service-expired!
  [join-mgr service {:keys [address port]}]
  (let [registrar {:address address, :port port}]
    (dosync
      (alter (:registrars service) (remove-registrar-fn registrar)))))

(defn- registrar-died!
  [join-mgr service {:keys [address port]}]
  (let [registrar {:address address, :port port}]
    (dosync
      (alter (:registrars join-mgr) disj registrar)
      (alter (:dead-registrars join-mgr) conj registrar)
      (alter (:registrars service) (remove-registrar-fn registrar)))))

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

(defn- error-info
  [{:keys [address port] :as registrar} service]
  {:service service
   :registrar-address address
   :registrar-port port})

(defn- join-service!
  "send a join request to a registrar and get a new service-id"
  [{:keys [connection-factory] :as join-mgr} service {:keys [address port] :as registrar} timeout-ms send-retry-count send-retry-interval-ms]
  (log/debug "Joinning" (pr-str service) "to" (pr-str registrar))
  (let [req (join-request (service-address service) (:port service) (service-id service) (:name service) (:attributes service))
        send-data #:send-request{:connection-factory connection-factory
                                 :address address
                                 :port port
                                 :data req
                                 :timeout-ms timeout-ms
                                 :send-retry-count send-retry-count
                                 :send-retry-interval-ms send-retry-interval-ms}
        read-ch   (request/send send-data)
        result-ch (chan)]

    (go
      (let [result (try
                      (let [msg (some-> (<! read-ch) (deref))]
                        (cond
                          (registration? msg)
                          (join! join-mgr service registrar msg)

                          (= :crow.request/timeout msg)
                          (do
                            (trace-pr msg)
                            (throw+ {:type msg
                                     :info (error-info registrar service)}))

                          :else
                          (do
                            (debug-pr "illegal message:" msg)
                            (throw+ {:type ::illegal-response
                                     :message msg
                                     :info (error-info registrar service)}))))
                      (catch Throwable e
                        (dosync
                          (registrar-died! join-mgr service registrar))
                        e))]
        (>! result-ch (boxed result))))
    result-ch))

(declare join)

(defn- send-heart-beat!
  [{:keys [connection-factory] :as join-mgr} service {:keys [address port] :as registrar} timeout-ms send-retry-count send-retry-interval-ms]
  (let [req (heart-beat (service-id service))
        send-data #:send-request{:connection-factory connection-factory
                                 :address address
                                 :port port
                                 :data req
                                 :timeout-ms timeout-ms
                                 :send-retry-count send-retry-count
                                 :send-retry-interval-ms send-retry-interval-ms}
        read-ch   (request/send send-data)
        result-ch (chan)]
    (go
      (let [result (try
                      (let [msg (some-> (<! read-ch) (deref))]
                        (cond
                          (lease? msg)
                          (do
                            (log/trace "Lease Renewal: " (service-id service))
                            (accept-lease! join-mgr service registrar (:expire-at msg))
                            true)

                          (lease-expired? msg)
                          (do
                            (log/info "expired: " (service-id service))
                            (service-expired! join-mgr service registrar)
                            false)

                          (= :crow.request/timeout)
                          (do
                            (trace-pr msg)
                            (throw+ {:type msg
                                     :info (error-info registrar service)}))

                          :else
                          (do
                            (trace-pr "illegal message:" msg)
                            (throw+ {:type ::illegal-response
                                     :message msg
                                     :info (error-info registrar service)}))))
                      (catch Throwable e
                        (registrar-died! join-mgr service registrar)
                        e))]
        (>! result-ch (boxed result))))
    result-ch))


(defn- joined?
  "true if a service is already join to the registrar."
  [service registrar]
  (boolean
    (when-let [registrars (not-empty @(:registrars service))]
      (registrars registrar))))

(defn- run-join-processor
  [join-mgr join-ch timeout-ms send-retry-count send-retry-interval-ms]
  (go-loop []
    (if @should-stop
      (log/info "join-processor stopped.")
      (do
        (try
          (let [{:keys [service registrar], :as join-info} (<! join-ch)]
            (when (seq join-info)
              (try
                (some-> (<! (join-service! join-mgr service registrar timeout-ms send-retry-count send-retry-interval-ms))
                        (deref))
                (catch Throwable th
                  (log/error th "An exception occured when joining.")))))
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
                  not-joined  (difference registrars joined)]
              (try
                (if (seq not-joined)
                  (let [join-req (for [reg not-joined] {:service service, :registrar reg})]
                    (trace-pr "join targets:" not-joined)
                    (onto-chan join-ch join-req false))
                  (log/trace "There are no join targets!"))
                (finally
                  (dosync
                    (alter (:managed-services join-mgr) conj service))))))
          (catch Throwable e
            (log/error e "service-acceptor error.")))
        (recur)))))

(defn- run-registrar-fetcher
  [join-mgr registrar-source fetch-registrar-interval-ms]
  (go-loop []
    (if @should-stop
      (log/info "registrar-fetcher stopped.")
      (do
        (try
          (log/trace "Resetting registrars from registrar-source.")
          (let [registrars (source/registrars registrar-source)]
            (reset-registrars! join-mgr registrars))
          (<! (timeout fetch-registrar-interval-ms))
          (catch Throwable th
            (log/error th "registrar-fetcher error.")))
        (recur)))))

(defn- run-heart-beat-processor
  [join-mgr heart-beat-buffer-ms timeout-ms send-retry-count send-retry-interval-ms]
  (go-loop []
    (if @should-stop
      (log/info "heart-beat-processor stopped.")
      (do
        (try
          (doseq [[service reg]
                    (dosync
                      (for [service @(:managed-services join-mgr)
                            {:keys [expire-at] :as reg} @(:registrars service)
                            :when (after? (plus (now) (millis heart-beat-buffer-ms)) expire-at)]
                        [service reg]))]
            (log/trace "send heart-beat from" (pr-str service) "to" (pr-str reg))
            (go
              (try
                (some-> (<! (send-heart-beat! join-mgr service reg timeout-ms send-retry-count send-retry-interval-ms))
                        (deref))
                (catch Throwable th
                  (log/error th "Could not send heart-beat to " (pr-str reg))))))
          (<! (timeout 500))

          (catch Throwable th
            (log/error th "heart-beat-processor error.")))
        (recur)))))

(defn- run-join-to-expired-registrar
  [join-mgr service-ch rejoin-interval-ms]
  (go-loop []
    (if @should-stop
      (log/info "join-to-expired-registrar stopped.")
      (do
        (try
          (onto-chan service-ch @(:managed-services join-mgr) false)
          (<! (timeout rejoin-interval-ms))
          (catch Throwable e
            (log/error e "join-to-expired-registrar error.")))
        (recur)))))

(defn- run-dead-registrar-checker
  [{:keys [connection-factory] :as join-mgr} dead-registrar-check-interval timeout-ms send-retry-count send-retry-interval-ms]
  (go-loop []
    (if @should-stop
      (do
        (log/info "dead-registrar-checker stopped.")
        (boxed nil))
      (do
        (doseq [{:keys [address port] :as registrar} @(:dead-registrars join-mgr)]
          (try
            (let [req (ping)
                  send-data #:send-request{:connection-factory connection-factory
                                           :address address
                                           :port port
                                           :data req
                                           :timeout-ms timeout-ms
                                           :send-retry-count send-retry-count
                                           :send-retry-interval-ms send-retry-interval-ms}
                  resp (some-> (<! (request/send send-data)) (deref))]
             (cond
               (ack? resp)
               (do
                 (log/info "A registrar revived: " (pr-str registrar))
                 (boxed (registrar-revived! join-mgr registrar)))

               :else
               (boxed nil)))
            (catch Throwable th
              ;; dead-registrar-checker usually get an error when checking registrars,
              ;; because the purpose of this thread is accessing to 'dead' registrars for checking
              ;; it is alive or not. If a registrar is 'dead' yet, the access will cause an error.
              ;; So if we print the error with ERROR level, verbose error logs will be printed.
              ;; It should be printed only in debugging time.
              (log/debug th "dead-registrar-checker error.")
              (boxed nil))))
        (<! (timeout dead-registrar-check-interval))
        (recur)))))

(defn start-join-manager
  [connection-factory registrar-source fetch-registrar-interval-ms dead-registrar-check-interval heart-beat-buffer-ms rejoin-interval-ms
   send-recv-timeout-ms send-retry-count send-retry-interval-ms]
  (let [service-ch (chan)
        join-ch    (chan)
        join-mgr   (join-manager connection-factory)]
    (run-registrar-fetcher join-mgr registrar-source fetch-registrar-interval-ms)
    (run-service-acceptor join-mgr service-ch join-ch)
    (run-join-processor join-mgr join-ch send-recv-timeout-ms send-retry-count send-retry-interval-ms)
    (run-heart-beat-processor join-mgr heart-beat-buffer-ms send-recv-timeout-ms send-retry-count send-retry-interval-ms)
    (run-join-to-expired-registrar join-mgr service-ch rejoin-interval-ms)
    (run-dead-registrar-checker join-mgr dead-registrar-check-interval send-recv-timeout-ms send-retry-count send-retry-interval-ms)
    service-ch))

(defn join
  [service-ch service]
  (>!! service-ch service))

