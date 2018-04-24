(ns crow.remote
  (:require [async-connect.client :as client]
            [crow.protocol :refer [remote-call call-result?
                                   sequential-item-start? sequential-item? sequential-item-end?
                                   call-exception? protocol-error?]]
            [crow.request :as request]
            [crow.boxed :refer [box service-info value]]
            [crow.discovery :refer [discover]]
            [crow.service-finder :refer [standard-service-finder] :as finder]
            [crow.logging :refer [debug-pr]]
            [clojure.core.async :refer [chan <!! >! <! close! go pipe]]
            [clojure.core.async.impl.protocols :refer [ReadPort WritePort]]
            [clojure.tools.logging :as log]
            [clojure.spec.alpha :as s])
  (:import [java.net ConnectException]))

(s/def :async/channel (s/and #(satisfies? ReadPort %) #(satisfies? WritePort %)))

(s/def ::service-name string?)
(s/def ::attributes (s/nilable (s/map-of keyword? any?)))
(s/def ::service-descriptor
  (s/keys :req-un [::service-name ::attributes]))

(s/def ::target-ns string?)
(s/def ::fn-name string?)
(s/def ::fn-args (s/nilable (s/coll-of any?)))
(s/def ::call-descriptor
  (s/keys :req-un [::target-ns
                   ::fn-name
                   ::fn-args]))

(s/def ::timeout-ms pos-int?)
(s/def ::send-retry-count pos-int?)
(s/def ::send-retry-interval-ms pos-int?)
(s/def ::remote-call-retry-count pos-int?)
(s/def ::remote-call-retry-interval-ms pos-int?)
(s/def ::discovery-options
  (s/keys :opt-un [::timeout-ms
                   ::send-retry-count
                   ::send-retry-interval-ms
                   ::remote-call-retry-count
                   ::remote-call-retry-interval-ms]))

(s/def ::address string?)
(s/def ::port pos-int?)
(s/def ::service
  (s/keys :req-un [::address, ::port]))

(s/fdef invoke
  :args (s/cat :ch :async/channel
               :factory ::client/connection-factory
               :service-desc ::service-descriptor
               :service ::service
               :call-desc ::call-descriptor
               :discovery-opts ::discovery-options)
  :ret  :async/channel)

(defn invoke
  [ch
   factory
   service-desc
   {:keys [address port] :as service}
   {:keys [target-ns fn-name fn-args]}
   {:keys [timeout-ms send-retry-count send-retry-interval-ms]
      :or {send-retry-count 3, send-retry-interval-ms 500}}]

  (log/trace "invoke")
  (let [data (remote-call target-ns fn-name fn-args)]
    (go
      (try
        (let [send-data #::request{:connection-factory factory
                                   :address address
                                   :port port
                                   :data data
                                   :timeout-ms timeout-ms
                                   :send-retry-count send-retry-count
                                   :send-retry-interval-ms send-retry-interval-ms}
              result-ch (request/send send-data)]

          (loop []
            (when-let [result (<! result-ch)]
              (let [msg @result]
                (cond
                  (protocol-error? msg)
                  (>! ch (box (throw (ex-info "Protocol Error." {:type :protocol-error, :error-code (:error-code msg), :message (:message msg)}))))

                  (call-exception? msg)
                  (>! ch
                      (box
                        (let [type-str    (:type msg)
                              stack-trace (:stack-trace msg)]
                          (throw (ex-info "Remote function failed." {:type (keyword type-str) :stack-trace stack-trace})))))

                  (sequential-item-start? msg)
                  (do
                    (log/debug "sequential-item-start")
                    (>! ch (box ::sequential-item-start))
                    (recur))

                  (sequential-item? msg)
                  (do
                    (log/trace "sequential-item")
                    (>! ch (box (:obj msg)))
                    (recur))

                  (sequential-item-end? msg)
                  (do
                    (log/debug "sequential-item-end")
                    (>! ch (box ::sequential-item-end)))

                  (call-result? msg)
                  (>! ch (box (:obj msg)))

                  (= ::request/timeout msg)
                  (>! ch (box msg))

                  :else
                  (>! ch (box
                          (throw (IllegalStateException. (str "No such message format: " (pr-str msg))))))))))

          (close! result-ch)
          (close! ch))

        (catch Throwable th
          (>! ch (box service-desc service th)))))
    ch))


(s/fdef find-services
  :args (s/cat :finder :crow/service-finder
               :service-desc ::service-descriptor
               :options ::discovery-options)
  :ret  (s/coll-of ::service))

(defn find-services
  [finder service-desc options]
  (when-not finder
    (throw (ex-info "Finder not found." {:type :finder-not-found, :message "ServiceFinder doesn't exist! You must start a service finder by start-service-finder at first!"})))
  (discover finder service-desc options))


(s/fdef find-service
  :args (s/cat :finder :crow/service-finder
               :service-desc ::service-descriptor
               :options ::discovery-options)
  :ret  ::service)

(defn find-service
  [finder service-desc options]
  (first (shuffle (find-services finder service-desc options))))


(s/fdef async-fn*
  :args (s/cat :ch :async/channel
               :finder :crow/service-finder
               :service-desc ::service-descriptor
               :call-desc ::call-descriptor
               :options ::discovery-options)
  :ret  :async/channel)

(defn async-fn*
  [ch
   {::finder/keys [connection-factory] :as finder}
   service-desc
   call-desc
   options]
  (log/debug (str "remote call. service: " (pr-str service-desc) ", fn: " (pr-str call-desc)))
  (if-let [service (find-service finder service-desc options)]
    (invoke ch connection-factory service-desc service call-desc options)
    (throw (ex-info (format "Service Not Found: service-name=%s, attributes=%s"
                            (:service-name service-desc)
                            (pr-str (:attributes service-desc)))
                    {:service-descriptor service-desc}))))


(defn- timeout?
  [msg]
  (boolean
   (when msg
     (= msg :request/timeout))))

(defn- connection-error?
  [msg]
  (boolean
   (when msg
     (when-let [info (ex-data msg)]
       (= :request/connection-error (:type info))))))

(defn- connect-exception?
  [msg]
  (when msg
    (instance? ConnectException msg)))


(defn async-fn
  [ch
   finder 
   service-desc
   call-desc
   options]
  (let [inner-ch  (chan 1 (comp (filter #(and (not= ::sequential-item-start (value %))
                                              (not= ::sequential-item-end (value %))))
                                (map #(cond
                                        (timeout? (value %))
                                        (box (ex-info "Timeout." {:type ::connection-error, :kind :request/timeout}))

                                        :else
                                        %))))
        result-ch (or ch (chan))]
    (pipe (async-fn* inner-ch finder service-desc call-desc options)
          result-ch)))

(defmacro parse-call-list
  ([call-list]
    (let [namespace-fn (first call-list)
          namespace-str (str namespace-fn)
          ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
          target-ns (first ns-fn-coll)
          fn-name (last ns-fn-coll)
          args (vec (rest call-list))]
      `(vector
          {:service-name ~target-ns
           :attributes {}}
          {:target-ns ~target-ns
           :fn-name ~fn-name
           :fn-args ~args})))

  ([service-namespace attributes call-list]
    (let [service-name (name service-namespace)
          namespace-fn (first call-list)
          namespace-str (str namespace-fn)
          ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
          target-ns (first ns-fn-coll)
          fn-name (last ns-fn-coll)
          args (vec (rest call-list))]
      `(vector
          {:service-name ~service-name
           :attributes ~attributes}
          {:target-ns ~target-ns
           :fn-name ~fn-name
           :fn-args ~args}))))

(defmacro async
  ([ch finder call-list options]
   `(let [[service-desc# call-desc#] (parse-call-list ~call-list)]
      (async-fn ~ch ~finder service-desc# call-desc# ~options)))

  ([finder call-list options]
    `(async (chan) ~finder ~call-list ~options))

  ([ch finder service-namespace attributes call-list options]
   `(let [[service-desc# call-desc#] (parse-call-list ~service-namespace ~attributes ~call-list)]
      (async-fn ~ch ~finder service-desc# call-desc# ~options)))

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

(s/fdef make-call-fn
  :args (s/cat :ch :async/channel
               :finder :crow/service-finder
               :service-desc ::service-descriptor
               :call-desc ::call-descriptor
               :options ::discovery-options)
  :ret  (s/fspec :args empty? :ret any?))

(defn- make-call-fn
  [ch finder service-desc call-desc options]
  (fn []
    (try
      (let [result-ch (async-fn* ch finder service-desc call-desc options)]
        (loop [result [] sequential-result? false]
          (if-let [box (<!! result-ch)]
            (let [msg @box]
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
            result)))

      (catch ConnectException ex
        (throw ex))

      (catch Throwable th
        (if-let [data (when-let [info (ex-data th)]
                        (when (= ::conneciton-error (:type info))
                          info))]
          data
          (throw th))))))



(defn- need-retry?
  [msg]
  (boolean
   (when msg
     (or (timeout? msg) (connection-error? msg) (connect-exception? msg)))))


(s/fdef try-call
  :args (s/cat :ch :async/channel
               :finder :crow/service-finder
               :service-desc ::service-descriptor
               :call-desc ::call-descriptor
               :call-opts ::discovery-options)
  :ret  any?)

(defn try-call
  [ch finder service-desc call-desc
   {:keys [remote-call-retry-count remote-call-retry-interval-ms]
    :or {remote-call-retry-count 3 remote-call-retry-interval-ms 500} :as options}]

  (let [call-fn (make-call-fn ch finder service-desc call-desc options)]
    (loop [result nil retry 0]
      (if (> retry remote-call-retry-count)
        (cond
          (timeout? result)
          (throw (ex-info "Timeout." {:type ::connection-error, :kind ::timeout}))

          (instance? Throwable result)
          (throw result)

          :else
          result)

        (let [r (call-fn)]
          (cond
            (need-retry? r)
            (let [new-retry (inc retry)]
              (log/debug (format "RETRY! %d/%d" new-retry remote-call-retry-count))
              (Thread/sleep (* remote-call-retry-interval-ms new-retry))
              (recur r new-retry))

            :else
            r))))))

(defmacro call
  ([ch finder call-list opts]
    `(let [[service-desc# call-desc#] (parse-call-list ~call-list)]
        (try-call ~ch ~finder service-desc# call-desc# ~opts)))

  ([finder call-list opts]
    `(call (chan) ~finder ~call-list ~opts))

  ([ch finder service-namespace attributes call-list opts]
    `(let [[service-desc# call-desc#] (parse-call-list ~service-namespace ~attributes ~call-list)]
        (log/debug (str "service-desc: " (pr-str service-desc#)))
        (log/debug (str "call-desc: " (pr-str call-desc#)))
        (try-call ~ch ~finder service-desc# call-desc# ~opts)))

  ([finder service-namespace attributes call-list opts]
    `(call (chan) ~finder ~service-namespace ~attributes ~call-list ~opts)))

(defmacro with-service
  ([ch factory service call-list opts]
   `(let [[service-desc# call-desc#] (parse-call-list ~call-list)]
      (invoke ~ch ~factory service-desc# ~service call-desc# ~opts)))

  ([factory service call-list opts]
    `(with-service (chan) ~factory ~service ~call-list ~opts)))





