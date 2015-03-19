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
            [crow.id-store :refer [->FileIdStore] :as id]))


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
        (not (public-ns-set target-ns))
          (protocol-error error-namespace-is-not-public
                          (format "namespace '%s' is not public for remote call" target-ns))
        (not target-fn)
          (protocol-error error-target-not-found
                          (format "the fn %s/%s is not found." target-ns fn-name))
        :else
          (try
            (let [r (apply target-fn args)]
              (call-result r))
            (catch Throwable th
              (call-exception (format-stack-trace th))))))))

(defn- handle-request
  [service msg]
  (cond
    (remote-call? msg) (handle-remote-call (:public-ns-set service) msg)
    :else (invalid-message msg)))

(defn- service-handler
  [service buffer-size stream info]
  (let [source (buffer buffer-size stream)]
    (d/loop []
      (-> (s/take! source ::none)
        (d/chain
          (fn [msg]
            (if (= msg ::none)
              ::none
              (d/future (handle-request service msg))))
          (fn [msg']
            (when-not (= msg' ::none)
              (s/put! stream msg')))
          (fn [result]
            (when result
              (d/recur))))
        (d/catch
          (fn [ex]
            (log/error ex "An Error ocurred.")
            (s/put! stream (call-exception (format-stack-trace ex)))
            (s/close! stream)))))))

(defn start-service
  [{:keys [address port name attributes id-store public-namespaces registrar-source
           fetch-registrar-interval-ms heart-beat-buffer-ms dead-registrar-check-interval
           rejoin-interval-ms buffer-size] :or {address "localhost" attributes {} buffer-size 10} :as config}]
  {:pre [port (not (clojure.string/blank? name)) id-store (seq public-namespaces) registrar-source fetch-registrar-interval-ms heart-beat-buffer-ms]}
  (apply require (map symbol public-namespaces))
  (let [sid     (id/read id-store)
        service (new-service address port sid name attributes id-store (set public-namespaces))]
    (tcp/start-server
      (fn [stream info]
        (service-handler service buffer-size (wrap-duplex-stream stream) info))
      {:port port
       :pipeline-transform #(.addFirst % "framer" (frame-decorder))})
    (let [join-mgr (start-join-manager registrar-source
                                       fetch-registrar-interval-ms
                                       dead-registrar-check-interval
                                       heart-beat-buffer-ms
                                       rejoin-interval-ms)]
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
