(ns crow.service
  (:refer-clojure :exclude [read])
  (:require [aleph.tcp :as tcp]
            [manifold.stream :refer [put! take!] :as s]
            [crow.protocol :refer [unpack-message remote-call? invalid-message protocol-error call-result call-exception] :as p]
            [crow.marshaller :refer [marshal unmarshal]]
            [msgpack.core :refer [defext pack unpack] :as msgpack]))

(defprotocol ServiceIdStore
  (write [this service-id] "write service-id into persistent store.")
  (read [this] "read service-id from persistent store."))

(defrecord Service
  [ip-address
   service-id-atom
   expire-at-atom
   registrars
   name
   attributes
   id-store
   public-ns-set])

(defn new-service
  ([ip-address name attributes id-store public-ns-set]
    (Service. ip-address (atom nil) (atom nil) (atom []) name attributes id-store public-ns-set))
  ([ip-address service-id name attributes id-store public-ns-set]
    (Service. ip-address (atom service-id) (atom nil) (atom []) name attributes id-store public-ns-set)))

(defn service-id
  [service]
  (deref (:service-id-atom service)))

(defn expire-at
  [service]
  (deref (:expire-at-atom service)))



(defn- format-stack-trace
  [exception]
  (let [elements (loop [^Throwable ex exception elems []]
                    (if (nil? ex)
                      elems
                      (let [calls (-> (map str (.getStackTrace ex))
                                      (as-> x (if (empty? elems) x (cons "Caused by:" x))))]
                        (recur (.getCause ex) (apply conj elems calls)))))]
    (map println-str elements)))

(def ^:const error-namespace-is-not-public 400)
(def ^:const error-target-not-found 401)

(defn- handle-remote-call
  [public-ns-set {:keys [target-ns fn-name args]}]
  (let [target-fn (find-var (symbol target-ns fn-name))]
    (cond
      (not (public-ns-set target-ns)) (protocol-error error-namespace-is-not-public (format "namespace '%' is not public for remote call"))
      (not target-fn) (protocol-error error-target-not-found (format "the fn %s/%s is not found." target-ns fn-name))
      :else
        (let [unmarshalled (map unmarshal args)]
          (try
            (let [r (apply target-fn unmarshalled)
                  marshalled (marshal r)]
              (call-result marshalled))
            (catch Throwable th
              (call-exception (format-stack-trace th))))))))



(defn- service-handler
  [service stream info]
  (let [data   (take! stream)
        msg    (unpack-message data)
        result (if (remote-call? msg)
                  (handle-remote-call (:public-ns-set service) msg)
                  (invalid-message msg))]
    (put! stream (pack result))))

(defn start-service
  [service port]
  (tcp/start-server (partial service-handler service) {:port port}))


