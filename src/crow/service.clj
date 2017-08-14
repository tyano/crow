(ns crow.service
  (:require [async-connect.server :refer [run-server close-wait] :as async-server]
            [async-connect.box :refer [boxed]]
            [clojure.core.async :refer [chan go-loop thread <! >! <!! >!! alt! alts! timeout]]
            [crow.protocol :refer [remote-call? ping? invalid-message protocol-error call-result call-exception ack] :as p]
            [crow.request :refer [frame-decorder format-stack-trace packer unpacker] :as request]
            [crow.join-manager :refer [start-join-manager join]]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [crow.registrar-source :refer [static-registrar-source]]
            [crow.id-store :refer [->FileIdStore] :as id]
            [slingshot.slingshot :refer [try+]]
            [crow.utils :refer [extract-exception]]
            [slingshot.support :refer [get-context]]
            [async-connect.pool :refer [pooled-connection-factory]]
            [crow.request :as request]
            [clojure.string :refer [index-of]])
  (:import [io.netty.handler.codec.bytes
              ByteArrayDecoder
              ByteArrayEncoder]
           [io.netty.channel
              ChannelPipeline
              ChannelHandler]))


;; SERVICE INTERFACES
(defn add-handler
  [handler-map {:keys [handler-namespace handler-name handler-fn] :as handler-def}]
  (assoc handler-map
    {:namespace handler-namespace
     :name handler-name}
    handler-fn))

(defn fn-handler
  [handler-ns handler-name handler-fn]
  {:handler-namespace handler-ns
   :handler-name handler-name
   :handler-fn handler-fn})

(defn var-handler
  [target-var]
  (let [metadata (meta target-var)]
    (fn-handler
      (-> metadata :ns ns-name name)
      (-> metadata :name name)
      target-var)))

(defmacro handler
  [fn-name & handler-descs]
  (let [first-desc        (first handler-descs)
        descs             (rest handler-descs)
        has-comments?     (string? first-desc)
        arg-list          (if has-comments? (first descs) first-desc)
        body              (if has-comments? (rest descs) descs)
        fn-namespace      (namespace fn-name)
        handler-name      (name fn-name)
        handler-namespace (or fn-namespace
                              (name (ns-name *ns*)))]
    `{:handler-namespace ~handler-namespace
      :handler-name ~handler-name
      :handler-fn (fn ~arg-list ~@body)}))

(defn build-handler-map
  [handler-map & handlers]
  (reduce
    (fn [m handler]
      (add-handler m handler))
    handler-map
    handlers))

(defmacro defhandlermap
  [map-name & handlers]
  `(def ~map-name (build-handler-map {} ~@handlers)))

(defn build-handler-map-from-namespace
  ([target-ns xf]
    (let [base-tr  (filter #(fn? (var-get %)))
          xforms   (comp base-tr xf)
          handlers (sequence xforms (vals (ns-publics target-ns)))]
      (apply build-handler-map {} handlers)))
  ([target-ns]
    (build-handler-map-from-namespace target-ns (map var-handler))))

;; SERVER IMPLEMENTATIONS

(defrecord Service
  [address
   port
   service-id-ref
   registrars
   name
   attributes
   id-store])

(defn new-service
  ([address port name attributes id-store]
    (Service. address port (ref nil) (ref #{}) name attributes id-store))
  ([address port service-id name attributes id-store]
    (Service. address port (ref service-id) (ref #{}) name attributes id-store)))

(defn service-id
  [service]
  (deref (:service-id-ref service)))

(def ^:const error-namespace-is-not-public 400)
(def ^:const error-target-not-found 401)

(defn- handle-remote-call
  [handler-map {:keys [target-ns fn-name args] :as req}]
  (log/debug "remote-call: " (pr-str req))
  (trace-pr "remote-call response:"
    (if-let [target-fn (get handler-map {:namespace target-ns, :name fn-name}) #_(when (find-ns (symbol target-ns)) (find-var (symbol target-ns fn-name)))]
      (try+
        (let [r (apply target-fn args)]
          (call-result r))
        (catch Object ex
          (log/error (:throwable &throw-context) "An error occurred in a function.")
          (let [[type throwable] (extract-exception &throw-context)]
            (call-exception type (format-stack-trace throwable)))))

      (protocol-error error-target-not-found
                      (format "the fn %s/%s is not found." target-ns fn-name)))))

(defn- handle-request
  [handler-map service msg]
  (cond
    (ping? msg)        (do (log/trace "received a ping.") (ack))
    (remote-call? msg) (handle-remote-call handler-map msg)
    :else (invalid-message msg)))

(defn- make-service-handler
  [handler-map service timeout-ms & [{:keys [:crow/middleware]}]]
  (fn [read-ch write-ch]
    (go-loop []
      (when-let [msg (<! read-ch)]
        (when
          (try
            (let [result (<! (thread
                                (boxed
                                  (try
                                    (if middleware
                                      (let [wrapper-fn (middleware (partial handle-request handler-map service))]
                                        (wrapper-fn @msg))
                                      (handle-request handler-map service @msg))
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
  (try
    ;; This function may be called on a instance repeatedly by spec-checking.
    ;; so this function must be idempotent.
    (let [pipeline ^ChannelPipeline (.pipeline netty-ch)]
      (doseq [^String n (.names pipeline)]
        (when-let [handler (.context pipeline n)]
          (.remove pipeline ^String n))))

    (.. netty-ch
      (pipeline)
      (addLast "messagepack-framedecoder" (frame-decorder))
      (addLast "bytes-decoder" (ByteArrayDecoder.))
      (addLast "bytes-encoder" (ByteArrayEncoder.)))

    netty-ch

    (catch Throwable th
      (log/error th "init error")
      (throw th))))

(defn start-service
  [{:keys [:service/address
           :service/port
           :service/name
           :service/attributes
           :service/id-store
           :join-manager/fetch-registrar-interval-ms
           :join-manager/heart-beat-buffer-ms
           :join-manager/dead-registrar-check-interval
           :join-manager/rejoin-interval-ms
           :join-manager/send-recv-timeout
           :join-manager/send-retry-count
           :join-manager/send-retry-interval-ms
           :join-manager/connection-factory
           :join-manager/registrar-source]
      :or {port 0
           attributes {}
           send-recv-timeout 2000
           send-retry-count 3
           send-retry-interval-ms 500}
      :as config}
   handler-map]
  {:pre [port (not (clojure.string/blank? name)) id-store registrar-source fetch-registrar-interval-ms heart-beat-buffer-ms]}
  (let [sid     (id/read id-store)
        service-fn (fn [address port] (new-service address port sid name attributes id-store))
        join-mgr (start-join-manager connection-factory
                                     registrar-source
                                     fetch-registrar-interval-ms
                                     dead-registrar-check-interval
                                     heart-beat-buffer-ms
                                     rejoin-interval-ms
                                     send-recv-timeout
                                     send-retry-count
                                     send-retry-interval-ms)
        server (run-server
                  {:server.config/address address
                   :server.config/port port
                   :server.config/channel-initializer channel-initializer
                   :server.config/read-channel-builder #(chan 50 unpacker)
                   :server.config/write-channel-builder #(chan 50 packer)
                   :server.config/server-handler-factory
                      (fn [host port]
                        (let [service (service-fn host port)
                              service-handler (make-service-handler handler-map service send-recv-timeout config)]
                          (join join-mgr service)
                          service-handler))})]
    (log/info (str "#### SERVICE (name: " name ", port: " (async-server/port server) ") starts."))
    server))

