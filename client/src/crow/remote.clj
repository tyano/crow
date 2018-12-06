(ns crow.remote
  (:require [async-connect.client :as client]
            [box.core :as box]
            [crow.protocol :refer [remote-call call-result?
                                   sequential-item-start? sequential-item? sequential-item-end?
                                   call-exception? protocol-error?]]
            [crow.request :as request]
            [crow.boxed :refer [with-service-info service-info]]
            [crow.discovery :refer [discover] :as discovery]
            [crow.discovery.service :as service-info]
            [crow.service-finder :refer [standard-service-finder] :as finder]
            [crow.logging :refer [debug-pr]]
            [crow.service-descriptor :as service-desc]
            [crow.call-descriptor :as call-desc]
            [crow.call-options :as call-opts]
            [clojure.core.async :refer [chan <!! >! <! close! go pipe]]
            [clojure.core.async.impl.protocols :refer [ReadPort WritePort]]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s])
  (:import [java.net ConnectException]
           [java.util.concurrent TimeoutException]))

(s/def :async/channel (s/and #(satisfies? ReadPort %) #(satisfies? WritePort %)))


(s/def ::invoke-info
  (s/keys :req [::service-desc/service-name
                ::service-desc/attributes
                ::call-desc/target-ns
                ::call-desc/fn-name
                ::call-desc/fn-args
                ::service-info/address
                ::service-info/port
                ::call-opts/timeout-ms]))


(defn- handle-message
  [msg]
  (cond
    (protocol-error? msg)
    (throw (ex-info "Protocol Error."
                    {:type       :protocol-error,
                     :error-code (:error-code msg),
                     :message    (:message msg)}))

    (call-exception? msg)
    (let [type-str    (:type msg)
          stack-trace (:stack-trace msg)]
      (throw
       (ex-info "Remote function failed."
                {:type        (keyword type-str)
                 :stack-trace stack-trace})))

    (sequential-item-start? msg)
    (do
      (log/debug "sequential-item-start")
      [::sequential-item-start ::recur])

    (sequential-item? msg)
    (do
      (log/trace "sequential-item")
      [(:obj msg) ::recur])

    (sequential-item-end? msg)
    (do
      (log/debug "sequential-item-end")
      [::sequential-item-end ::end])

    (call-result? msg)
    [(:obj msg) ::end]

    (= ::request/timeout msg)
    (throw (TimeoutException.))

    :else
    (throw (ex-info (str "No such message format: " (pr-str msg)) {}))))


(s/fdef invoke
  :args (s/cat :ch :async/channel
               :factory ::client/connection-factory
               :invoke-info ::invoke-info)
  :ret  :async/channel)

(defn invoke
  [ch
   factory
   {::service-info/keys [address port]
    ::call-desc/keys    [target-ns fn-name fn-args]
    ::call-opts/keys    [timeout-ms] :as invoke-info}]

  (log/trace "invoke")
  (let [data (remote-call target-ns fn-name fn-args)]
    (try
      (let [send-data #::request {:connection-factory factory
                                  :address            address
                                  :port               port
                                  :data               data
                                  :timeout-ms         timeout-ms}
            result-ch (request/send send-data)]

        (-> result-ch
            (pipe
             (chan 1
                   (box/map
                    (fn [msg]
                      (let [[obj next-action] (handle-message msg)]
                        (when (= next-action ::end)
                          (close! result-ch))
                        obj))
                    {:throw? true})
                   (fn [ex]
                     (close! result-ch)
                     (-> (box/value ex)
                         (with-service-info invoke-info)))))
            (pipe ch)))

      (catch Throwable th
        (go
          (>! ch (-> (box/value th)
                     (with-service-info invoke-info)))
          (close! ch))))
    ch))


(s/fdef find-services
  :args (s/cat :finder :crow/service-finder
               :discovery-info ::discovery/discovery-info)
  :ret  (s/coll-of :crow.discovery/service))

(defn find-services
  [finder discovery-info]
  (when-not finder
    (throw (ex-info "Finder not found." {:type :finder-not-found, :message "ServiceFinder doesn't exist! You must start a service finder by start-service-finder at first!"})))
  (discover finder discovery-info))


(s/fdef find-service
  :args (s/cat :finder :crow/service-finder
               :discovery-info ::discovery/discovery-info)
  :ret  :crow.discovery/service)

(defn find-service
  [finder discovery-info]
  (first (shuffle (find-services finder discovery-info))))

(s/def ::async-call-info
  (s/keys :req [::service-desc/service-name
                ::service-desc/attributes
                ::call-desc/target-ns
                ::call-desc/fn-name
                ::call-desc/fn-args
                ::call-opts/timeout-ms]))

(s/fdef async-fn*
  :args (s/cat :ch :async/channel
               :finder :crow/service-finder
               :call-info ::async-call-info)
  :ret  :async/channel)

(defn async-fn*
  [ch
   {::finder/keys [connection-factory] :as finder}
   call-info]
  (log/debug (str "remote call. service: " (pr-str call-info)))
  (if-let [service (find-service finder call-info)]
    (invoke ch connection-factory (merge call-info service))
    (throw (ex-info (format "Service Not Found: service-name=%s, attributes=%s"
                            (::service-desc/service-name call-info)
                            (pr-str (::service-desc/attributes call-info)))
                    call-info))))

(defn async-fn
  [ch
   finder
   call-info]
  (let [fn-ch (chan 1)]
    (async-fn* fn-ch finder call-info)
    (go
      (try
        (loop [sequential-result? false]
          (when-let [boxed-data (<! fn-ch)]
            (let [msg @boxed-data]
              (log/trace "received message:" (pr-str msg))
              (cond
                (= ::sequential-item-start msg)
                (recur true)

                (= ::sequential-item-end msg)
                nil

                sequential-result?
                (do
                  (>! ch boxed-data)
                  (recur true))

                :else
                (>! ch boxed-data)))))

        (catch Throwable th
          (>! ch (box/value th)))
        (finally
          (close! fn-ch)
          (close! ch))))
    ch))

(defmacro parse-call-list
  ([call-list]
    (let [namespace-fn (first call-list)
          namespace-str (str namespace-fn)
          ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
          target-ns (first ns-fn-coll)
          fn-name (last ns-fn-coll)
          args (vec (rest call-list))]
      `{::service-desc/service-name ~target-ns
        ::service-desc/attributes {}
        ::call-desc/target-ns ~target-ns
        ::call-desc/fn-name ~fn-name
        ::call-desc/fn-args ~args}))

  ([service-namespace attributes call-list]
    (let [service-name (name service-namespace)
          namespace-fn (first call-list)
          namespace-str (str namespace-fn)
          ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
          target-ns (first ns-fn-coll)
          fn-name (last ns-fn-coll)
          args (vec (rest call-list))]
      `{::service-desc/service-name ~service-name
        ::service-desc/attributes ~attributes
        ::call-desc/target-ns ~target-ns
        ::call-desc/fn-name ~fn-name
        ::call-desc/fn-args ~args})))

(defmacro async
  ([ch finder call-list options]
   `(let [call-info# (merge (parse-call-list ~call-list) ~options)]
      (async-fn ~ch ~finder call-info#)))

  ([finder call-list options]
    `(async (chan) ~finder ~call-list ~options))

  ([ch finder service-namespace attributes call-list options]
   `(let [call-info# (merge (parse-call-list ~service-namespace ~attributes ~call-list)
                              ~options)]
      (async-fn ~ch ~finder call-info#)))

  ([finder service-namespace attributes call-list options]
    `(async (chan) ~finder ~service-namespace ~attributes ~call-list ~options)))

(defn handle-exception
  [boxed-result & finders]
  (when-let [{:keys [service service-descriptor]} (service-info boxed-result)]
    (doseq [finder finders]
      (finder/remove-service finder service-descriptor service))))

(defn handle-result
  [result & finders]
  (try
    @result
    (catch Throwable th
      (apply handle-exception result finders)
      (throw th))))

(defn <!!+
  "read a channel. if the result value is an instance of
   Throwable, then throw the exception. Otherwise returns the
   result.
   This fn is a kind of <!! macro of core.async, so calling
   this fn will block current thread."
  [ch & finders]
  (when ch
    (when-let [result (<!! ch)]
      (apply handle-result result finders))))

(defmacro <!+
  "read a channel. if the result value is an instance of
   Throwable, then throw the exception. Otherwise returns the
   result.
   This macro is a kind of <! macro of core.async, so this macro
   must be called in (go) block. it it why this is a macro, not fn.
   all contents of this macro is expanded into a go block."
  [ch & finders]
  `(let [ch# ~ch
         finders# ~(vec finders)]
      (when-let [result# (<! ch#)]
        (apply handle-result result# finders#))))


(s/fdef try-call
  :args (s/cat :ch :async/channel
               :finder :crow/service-finder
               :call-info ::async-call-info)
  :ret  any?)

(defn try-call
  [ch finder call-info]
  (try
    (async-fn* ch finder call-info)
    (loop [result [] sequential-result? false]
      (if-let [boxed-data (<!! ch)]
        (let [msg @boxed-data]
          (log/trace "received message:" (pr-str msg))
          (cond
            (= ::sequential-item-start msg)
            (recur [] true)

            (= ::sequential-item-end msg)
            result

            sequential-result?
            (recur (conj result msg) true)

            :else
            msg))
        result))
    (finally
      (close! ch))))

(defmacro call
  ([ch finder call-list opts]
   `(let [call-info# (merge (parse-call-list ~call-list)
                            ~opts)]
        (try-call ~ch ~finder call-info#)))

  ([finder call-list opts]
   `(call (chan) ~finder ~call-list ~opts))

  ([ch finder service-namespace attributes call-list opts]
   `(let [call-info# (merge (parse-call-list ~service-namespace ~attributes ~call-list)
                            ~opts)]
      (log/debug (str "call-info: " (pr-str call-info#)))
      (try-call ~ch ~finder call-info#)))

  ([finder service-namespace attributes call-list opts]
   `(call (chan) ~finder ~service-namespace ~attributes ~call-list ~opts)))

(defmacro with-service
  ([ch factory service call-list opts]
   `(let [invoke-info# (merge (parse-call-list ~call-list)
                              ~opts
                              ~service)]
      (invoke ~ch ~factory invoke-info#)))

  ([factory service call-list opts]
    `(with-service (chan) ~factory ~service ~call-list ~opts)))
