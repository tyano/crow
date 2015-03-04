(ns crow.service
  (:require [aleph.tcp :as tcp]
            [manifold.stream :refer [connect] :as s]
            [msgpack.core :refer [pack] :as msgpack]
            [crow.protocol :refer [remote-call? invalid-message protocol-error call-result call-exception] :as p]
            [crow.request :refer [read-message frame-decorder]]
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


(defn- format-stack-trace
  [exception]
  (log/error exception "an error occured.")
  (let [elements (cons (str (.getName (class exception)) ": " (.getMessage exception))
                    (loop [^Throwable ex exception elems []]
                      (if (nil? ex)
                        elems
                        (let [calls (-> (map str (.getStackTrace ex))
                                        (as-> x (if (empty? elems) x (cons "Caused by:" x))))]
                          (recur (.getCause ex) (apply conj elems calls))))))]
    (apply str (map println-str elements))))

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
  [service stream info]
  (let [source (->> stream
                  (s/map read-message)
                  (s/map (partial handle-request service))
                  (s/map pack))]
    (s/connect source stream)))

(defn start-service
  [{:keys [address port name attributes id-store public-namespaces registrar-source fetch-registrar-interval-ms heart-beat-buffer-ms dead-registrar-check-interval rejoin-interval-ms], :as config, :or {address "localhost" attributes {}}}]
  {:pre [port (not (clojure.string/blank? name)) id-store (seq public-namespaces) registrar-source fetch-registrar-interval-ms heart-beat-buffer-ms]}
  (apply require (map symbol public-namespaces))
  (let [sid     (id/read id-store)
        service (new-service address port sid name attributes id-store (set public-namespaces))]
    (tcp/start-server
      (partial service-handler service)
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

