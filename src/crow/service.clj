(ns crow.service
  (:refer-clojure :exclude [read])
  (:require [aleph.tcp :as tcp]
            [manifold.stream :refer [connect] :as s]
            [msgpack.core :refer [pack] :as msgpack]
            [crow.protocol :refer [remote-call? invalid-message protocol-error call-result call-exception
                                   send! recv! read-message] :as p]
            [crow.marshaller :refer [marshal unmarshal]]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [clojure.string :refer [blank?]]
            [clojure.java.io :refer [writer reader]])
  (:import [java.io FileNotFoundException]))

(defprotocol ServiceIdStore
  (write [this service-id] "write service-id into persistent store.")
  (read [this] "read service-id from persistent store."))

(extend-protocol ServiceIdStore
  nil
  (write [this service-id] nil)
  (read [this] nil))


(defrecord FileIdStore [file-path]
  ServiceIdStore
  (write [this service-id]
    (when (not (blank? service-id))
      (with-open [w (writer file-path :append false)]
        (.write w service-id)
        (.newLine w))))
  (read [this]
    (try
      (with-open [r (reader file-path)]
        (.readLine r))
      (catch FileNotFoundException ex
        nil))))


(defrecord Service
  [address
   port
   service-id-atom
   registrars
   name
   attributes
   id-store
   public-ns-set])

(defn new-service
  ([address port name attributes id-store public-ns-set]
    (Service. address port (atom nil) (atom []) name attributes id-store public-ns-set))
  ([address port service-id name attributes id-store public-ns-set]
    (Service. address port (atom service-id) (atom []) name attributes id-store public-ns-set)))

(defn service-id
  [service]
  (deref (:service-id-atom service)))


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
  (log/trace "remote-call: " (str target-ns "/" fn-name " " (apply str args)))
  (trace-pr "remote-call response:"
    (let [target-fn (find-var (symbol target-ns fn-name))]
      (cond
        (not (public-ns-set target-ns))
          (protocol-error error-namespace-is-not-public
                          (format "namespace '%s' is not public for remote call" target-ns))
        (not target-fn)
          (protocol-error error-target-not-found
                          (format "the fn %s/%s is not found." target-ns fn-name))
        :else
          (let [unmarshalled (map unmarshal args)]
            (try
              (let [r (apply target-fn unmarshalled)
                    marshalled (marshal r)]
                (call-result marshalled))
              (catch Throwable th
                (call-exception (format-stack-trace th)))))))))

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
  [{:keys [address port name attributes id-store public-namespaces], :as config, :or {address "localhost" attributes {}}}]
  {:pre [port (not (clojure.string/blank? name)) id-store (seq public-namespaces)]}
  (let [service (new-service address port name attributes id-store (set public-namespaces))]
    (tcp/start-server (partial service-handler service) {:port port})))

(defn -main
  [& [service-name port-str :as args]]
  (when (< (count args) 2)
    (throw (IllegalArgumentException. "service-name and port must be supplied.")))
  (let [port (Long/valueOf ^String port-str)
        config {:address  "localhost"
                :port     port
                :name     service-name
                :id-store (FileIdStore. "/tmp/example.id")
                :public-namespaces #{"clojure.core"}}]
    (log/info (str "#### SERVICE (name: " service-name ", port: " port ") starts."))
    (start-service config)))

