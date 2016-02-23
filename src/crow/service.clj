(ns crow.service
  (:require [aleph.tcp :as tcp]
            [manifold.stream :refer [connect buffer] :as s]
            [manifold.deferred :refer [let-flow] :as d]
            [crow.protocol :refer [remote-call? invalid-message protocol-error call-result call-exception] :as p]
            [crow.request :refer [frame-decorder wrap-duplex-stream format-stack-trace] :as request]
            [crow.join-manager :refer [start-join-manager join]]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [crow.registrar-source :refer [static-registrar-source]]
            [crow.id-store :refer [->FileIdStore] :as id]
            [slingshot.slingshot :refer [try+]]
            [crow.utils :refer [extract-exception]]
            [slingshot.support :refer [get-context]]))


(defrecord Service
  [address
   port
   service-id-ref
   registrars
   name
   attributes
   id-store
   public-ns-set])

(defn new-service
  ([address port name attributes id-store public-ns-set]
    (Service. address port (ref nil) (ref #{}) name attributes id-store public-ns-set))
  ([address port service-id name attributes id-store public-ns-set]
    (Service. address port (ref service-id) (ref #{}) name attributes id-store public-ns-set)))

(defn service-id
  [service]
  (deref (:service-id-ref service)))

(def ^:const error-namespace-is-not-public 400)
(def ^:const error-target-not-found 401)

(defn- handle-remote-call
  [public-ns-set {:keys [target-ns fn-name args] :as req}]
  (log/debug "remote-call: " (pr-str req))
  (trace-pr "remote-call response:"
    (let [target-fn (when (find-ns (symbol target-ns)) (find-var (symbol target-ns fn-name)))]
      (cond
        (not target-fn)
        (protocol-error error-target-not-found
                        (format "the fn %s/%s is not found." target-ns fn-name))

        (not (public-ns-set target-ns))
        (protocol-error error-namespace-is-not-public
                        (format "namespace '%s' is not public for remote call" target-ns))

        :else
          (try+
            (let [r (apply target-fn args)]
              (call-result r))
            (catch Object ex
              (log/error (:throwable &throw-context) "An error occurred in a function.")
              (let [[type throwable] (extract-exception &throw-context)]
                (call-exception type (format-stack-trace throwable)))))))))

(defn- handle-request
  [service msg]
  (cond
    (remote-call? msg) (handle-remote-call (:public-ns-set service) msg)
    :else (invalid-message msg)))

(defn- service-handler
  [service stream info timeout-ms]
  (->
    (d/loop []
      (-> (s/try-take! stream ::none timeout-ms ::none)
        (d/chain
          (fn [msg]
            (if (= ::none msg)
              ::none
              (d/future (handle-request service msg))))
          (fn [msg']
            (when-not (= ::none msg')
              (s/try-put! stream msg' timeout-ms ::timeout)))
          (fn [result]
            (when (some? result)
              (cond
                (= result ::timeout)
                (log/error "Service Timeout: Couldn't write response.")

                (true? result)
                (d/recur)

                :else
                (log/error "Service Error: Couldn't write response.")))))
        (d/catch
          (fn [ex]
            (log/error ex "An Error ocurred.")
            (let [[type throwable] (extract-exception (get-context ex))]
              (s/try-put! stream (call-exception type (format-stack-trace throwable)) timeout-ms)
              nil)))))
    (d/finally
      (fn []
        (s/close! stream)))))

(defn start-service
  [{:keys [address port name attributes id-store public-namespaces registrar-source
           fetch-registrar-interval-ms heart-beat-buffer-ms dead-registrar-check-interval
           rejoin-interval-ms send-recv-timeout
           send-retry-count retry-interval]
      :or {address "localhost" attributes {} send-recv-timeout nil send-retry-count 3 retry-interval 500} :as config}]
  {:pre [port (not (clojure.string/blank? name)) id-store (seq public-namespaces) registrar-source fetch-registrar-interval-ms heart-beat-buffer-ms]}
  (apply require (map symbol public-namespaces))
  (let [sid     (id/read id-store)
        service (new-service address port sid name attributes id-store (set public-namespaces))]
    (tcp/start-server
      (fn [stream info]
        (service-handler service (wrap-duplex-stream stream) info send-recv-timeout))
      {:port port
       :pipeline-transform #(.addFirst % "framer" (frame-decorder))})
    (let [join-mgr (start-join-manager registrar-source
                                       fetch-registrar-interval-ms
                                       dead-registrar-check-interval
                                       heart-beat-buffer-ms
                                       rejoin-interval-ms
                                       send-recv-timeout
                                       send-retry-count
                                       retry-interval)]
      (join join-mgr service))))

(defn -main
  [& [service-name port-str :as args]]
  (when (< (count args) 2)
    (throw (IllegalArgumentException. "service-name and port must be supplied.")))
  (let [port (Long/valueOf ^String port-str)
        config {:address  "localhost"
                :port     port
                :name     service-name
                :id-store (->FileIdStore "/tmp/example.id")
                :public-namespaces #{"clojure.core"}
                :registrar-source (static-registrar-source "localhost" 4000)
                :fetch-registrar-interval-ms 30000
                :heart-beat-buffer-ms      4000
                :dead-registrar-check-interval 10000
                :rejoin-interval-ms 10000}]
    (log/info (str "#### SERVICE (name: " service-name ", port: " port ") starts."))
    (start-service config)))
