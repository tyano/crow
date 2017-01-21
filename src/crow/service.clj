(ns crow.service
  (:require [async-connect.server :refer [run-server close-wait]]
            [async-connect.box :refer [boxed]]
            [clojure.core.async :refer [chan go-loop thread <! >! <!! >!! alt! alts! timeout]]
            [crow.protocol :refer [remote-call? ping? invalid-message protocol-error call-result call-exception ack] :as p]
            [crow.request :refer [frame-decorder wrap-duplex-stream format-stack-trace packer unpacker] :as request]
            [crow.join-manager :refer [start-join-manager join]]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [crow.registrar-source :refer [static-registrar-source]]
            [crow.id-store :refer [->FileIdStore] :as id]
            [slingshot.slingshot :refer [try+]]
            [crow.utils :refer [extract-exception]]
            [slingshot.support :refer [get-context]])
  (:import [io.netty.handler.codec.bytes
              ByteArrayDecoder
              ByteArrayEncoder]))


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
    (ping? msg)        (do (log/trace "received a ping.") (ack))
    (remote-call? msg) (handle-remote-call (:public-ns-set service) msg)
    :else (invalid-message msg)))

(defn- make-service-handler
  [service timeout-ms & [{:keys [middleware]}]]
  (fn [read-ch write-ch]
    (go-loop []
      (when-let [msg (<! read-ch)]
        (when
          (try
            (let [result (<! (thread
                                (boxed
                                  (try
                                    (if middleware
                                      (let [wrapper-fn (middleware (partial handle-request service))]
                                        (wrapper-fn msg))
                                      (handle-request service msg))
                                    (catch Throwable th th)))))
                  resp   {:message @result, :flush? true}]
                (alt!
                  [[write-ch resp]]
                  ([v ch] v)

                  [(if timeout-ms (timeout timeout-ms) (chan))]
                  ([v ch]
                    (log/error "Service Timeout: Couldn't write response.")
                    false)))
            (catch Throwable ex
              (log/error ex "An Error ocurred.")
              (let [[type throwable] (extract-exception (get-context ex))
                    ex-msg (call-exception type (format-stack-trace throwable))]
                (alts! [[write-ch {:message ex-msg, :flush? true}] (timeout timeout-ms)])
                false)))
          (recur))))))


(defn- channel-initializer
  [netty-ch config]
  (.. netty-ch
    (pipeline)
    (addLast "messagepack-framedecoder" (frame-decorder))
    (addLast "bytes-decoder" (ByteArrayDecoder.))
    (addLast "bytes-encoder" (ByteArrayEncoder.))))


(defn start-service
  [{:keys [address port name attributes id-store public-namespaces registrar-source
           fetch-registrar-interval-ms heart-beat-buffer-ms dead-registrar-check-interval
           rejoin-interval-ms send-recv-timeout
           send-retry-count send-retry-interval-ms]
      :or {address "localhost" attributes {} send-recv-timeout nil send-retry-count 3 send-retry-interval-ms 500}
      :as config}]
  {:pre [port (not (clojure.string/blank? name)) id-store (seq public-namespaces) registrar-source fetch-registrar-interval-ms heart-beat-buffer-ms]}
  (apply require (map symbol public-namespaces))
  (let [sid     (id/read id-store)
        service (new-service address port sid name attributes id-store (set public-namespaces))
        server  (run-server
                  {:server.config/port port
                   :server.config/channel-initializer channel-initializer
                   :server.config/read-channel-builder #(chan 50 unpacker)
                   :server.config/write-channel-builder #(chan 50 packer)
                   :server.config/server-handler (make-service-handler service send-recv-timeout config)})
        join-mgr (start-join-manager registrar-source
                                     fetch-registrar-interval-ms
                                     dead-registrar-check-interval
                                     heart-beat-buffer-ms
                                     rejoin-interval-ms
                                     send-recv-timeout
                                     send-retry-count
                                     send-retry-interval-ms)]
    (join join-mgr service)
    (log/info (str "#### SERVICE (name: " name ", port: " port ") starts."))
    server))

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
                :rejoin-interval-ms 10000}
        server (start-service config)]
    (close-wait server #(println "SERVER STOPPED."))))
