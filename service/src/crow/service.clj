(ns crow.service
  (:require [clojure.string :refer [index-of]]
            [clojure.spec.alpha :as s]
            [clojure.core.async :refer [chan go-loop thread <! >! <!! >!! alt! alts! alt!! timeout]]
            [clojure.tools.logging :as log]
            [async-connect.server :refer [run-server close-wait] :as async-server]
            [async-connect.message :as message]
            [async-connect.pool :refer [pooled-connection-factory]]
            [databox.core :as box]
            [crow.protocol :refer [remote-call? ping? invalid-message protocol-error call-result
                                   sequential-item-start sequential-item-start?
                                   sequential-item sequential-item?
                                   sequential-item-end sequential-item-end?
                                   call-exception ack] :as p]
            [crow.request :refer [frame-decorder format-stack-trace packer unpacker] :as request]
            [crow.join-manager :refer [start-join-manager stop-join-manager join]]
            [crow.logging :refer [trace-pr]]
            [crow.registrar-source :refer [static-registrar-source]]
            [crow.id-store :refer [->FileIdStore] :as id]
            [crow.utils :refer [extract-exception]])
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

(s/def :crow/service
  (s/keys :req-un
          [:service/address
           :service/port
           :service/service-id-ref
           :service/registrars
           :service/name
           :service/attributes
           :crow/id-store]))

(defn- make-service-map
  [address
   port
   service-id-ref
   registrars
   name
   attributes
   id-store]
  {:address address
   :port port
   :service-id-ref service-id-ref
   :registrars registrars
   :name name
   :attributes attributes
   :id-store id-store})

(defn new-service
  ([address port name attributes id-store]
    (make-service-map address port (ref nil) (ref #{}) name attributes id-store))
  ([address port service-id name attributes id-store]
    (make-service-map address port (ref service-id) (ref #{}) name attributes id-store)))

(defn service-id
  [service]
  (deref (:service-id-ref service)))

(defn- iterable?
  [data]
  (boolean
   (when data
     (or (set? data) (sequential? data))))) ;; sequential? will be false if the data is a set.

(def ^:const error-namespace-is-not-public 400)
(def ^:const error-target-not-found 401)

(defn- send-one-data!!
  [write-ch data timeout-ms]
  (alt!!
    [[write-ch data]]
    ([v ch] v)

    [(if timeout-ms (timeout timeout-ms) (chan))]
    ([v ch]
     (log/error "Service Timeout: Couldn't write response.")
     false)))

(defn- make-call-exception
  [ex]
  (let [[type throwable] (extract-exception ex)]
    (call-exception type (format-stack-trace throwable))))

(defn- handle-remote-call
  [handler-map
   {:keys [write-ch service timeout-ms] :as write-params}
   {:keys [target-ns fn-name args] :as req}]

  (log/debug "remote-call: " (pr-str req))
  (if-let [target-fn (get handler-map {:namespace target-ns, :name fn-name}) #_(when (find-ns (symbol target-ns)) (find-var (symbol target-ns fn-name)))]
    (let [r (apply target-fn args)]
      (if (iterable? r)
        (do
          (send-one-data!! write-ch
                           #::message{:data (sequential-item-start)
                                      :flush? false}
                           timeout-ms)

          (loop [items r write-count 0]
            (if-let [item (first items)] ;; this call will realize a lazy sequence and the realization might make an exception.
              ;; handle one item.
              (do
                (trace-pr "remote-call response:" item)
                (when (send-one-data!! write-ch
                                       #::message{:data (sequential-item item)
                                                  :flush? (>= write-count 10)}
                                       timeout-ms)
                  (recur (rest items) (if (>= write-count 10) 0 (inc write-count)))))

              ;; all items ware handled. send a sequential-item-end.
              (let [resp (sequential-item-end)]
                (trace-pr "remote-call response:" resp)
                (send-one-data!! write-ch
                                 #::message{:data resp
                                            :flush? true}
                                 timeout-ms)))))

        (do
          (trace-pr "remote-call response:" r)
          (send-one-data!! write-ch #::message{:data (call-result r) :flush? true} timeout-ms))))

    (send-one-data!! write-ch
                     #::message{:data (protocol-error error-target-not-found
                                               (format "the fn %s/%s is not found." target-ns fn-name))
                                :flush? true}
                     timeout-ms)))


(defn- handle-request
  [handler-map {:keys [write-ch timeout-ms] :as write-params} msg]
  (cond
    (ping? msg)
    (do
      (log/trace "received a ping.")
      (send-one-data!! write-ch #::message{:data (ack) :flush? true} timeout-ms))

    (remote-call? msg)
    (handle-remote-call handler-map write-params msg)

    :else
    (send-one-data!! write-ch #::message{:data (invalid-message msg) :flush? true} timeout-ms)))


(defn- make-service-handler
  [handler-map service timeout-ms & [{:keys [:crow/middleware]}]]
  (fn [context read-ch write-ch]
    (let [write-params {:service service
                        :write-ch write-ch
                        :timeout-ms timeout-ms}]
      (go-loop []
        (when-let [msg (<! read-ch)]
          (try
            (let [result (<! (thread
                               (box/value
                                (try
                                  (if middleware
                                    (let [wrapper-fn (middleware (partial handle-request handler-map write-params))]
                                      (wrapper-fn @msg))
                                    (handle-request handler-map write-params @msg))
                                  (catch Throwable th th)))))]
              @result)

            (catch Throwable ex
              (log/error ex "An Error ocurred.")
              (when-let [data (not-empty (ex-data ex))]
                (log/error data))
              (alt!
                [[write-ch #::message{:data (make-call-exception ex) :flush? true}]]
                ([v ch] v)

                [(if timeout-ms (timeout timeout-ms) (chan))]
                ([v ch]
                 false))))
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
  [{:service/keys [address remote-address port name attributes id-store]
    :join-manager/keys [fetch-registrar-interval-ms
                        heart-beat-buffer-ms
                        dead-registrar-check-interval
                        rejoin-interval-ms
                        send-recv-timeout
                        connection-factory
                        registrar-source]
      :or {port 0
           attributes {}
           send-recv-timeout 2000}
      :as config}
   handler-map]
  {:pre [port (not (clojure.string/blank? name)) id-store registrar-source fetch-registrar-interval-ms heart-beat-buffer-ms]}
  (let [sid     (id/read id-store)
        service-fn (fn [port] (new-service remote-address port sid name attributes id-store))
        join-mgr (start-join-manager connection-factory
                                     registrar-source
                                     fetch-registrar-interval-ms
                                     dead-registrar-check-interval
                                     heart-beat-buffer-ms
                                     rejoin-interval-ms
                                     send-recv-timeout)
        server (run-server
                 #::async-server{:address                address
                                 :port                   port
                                 :channel-initializer    channel-initializer
                                 :read-channel-builder   (fn [ch] (chan 50 unpacker))
                                 :write-channel-builder  (fn [ch] (chan 50 packer))
                                 :server-handler-factory (fn [host port]
                                                            (let [service (service-fn port)
                                                                  service-handler (make-service-handler handler-map service send-recv-timeout config)]
                                                              (join join-mgr service)
                                                              (log/info (str "#### SERVICE (name: " name ", service-address: " (or remote-address host) ", listening-address: " host ", port: " port ") starts."))
                                                              service-handler))
                                 :shutdown-hook          (fn [{:keys [host port]}]
                                                           (stop-join-manager join-mgr)
                                                           (log/info (str "#### SERVICE (name: " name ", service-address: " (or remote-address host) ", listening-address: " host ", port: " port ") stops.")))})]
    server))

