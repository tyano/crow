(ns crow.remote
  (:require [crow.protocol :refer [remote-call call-result? call-exception? protocol-error?]]
            [crow.request :as request]
            [crow.boxed :refer [box unbox service-info]]
            [clojure.core.async :refer [>!! chan <!! <! close! go]]
            [clojure.core.async.impl.protocols :refer [ReadPort WritePort]]
            [crow.discovery :refer [discover]]
            [crow.service-finder :refer [standard-service-finder] :as finder]
            [crow.logging :refer [debug-pr]]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.spec :as s])
  (:import [java.net ConnectException]))

(s/def :async/channel (s/and #(satisfies? ReadPort %) #(satisfies? WritePort %)))

(s/def :crow/service-name string?)
(s/def :crow/attributes (s/nilable (s/map-of keyword? any?)))
(s/def :crow/service-descriptor
  (s/keys :req-un [:crow/service-name :crow/attributes]))

(s/def :crow/target-ns string?)
(s/def :crow/fn-name string?)
(s/def :crow/fn-args (s/nilable (s/coll-of any?)))
(s/def :crow/call-descriptor
  (s/keys :req-un [:crow/target-ns
                   :crow/fn-name
                   :crow/fn-args]))

(s/def :crow/timeout-ms pos-int?)
(s/def :crow/send-retry-count pos-int?)
(s/def :crow/send-retry-interval-ms pos-int?)
(s/def :crow/remote-call-retry-count pos-int?)
(s/def :crow/remote-call-retry-interval-ms pos-int?)
(s/def :crow/discovery-options
  (s/keys :opt-un [:crow/timeout-ms
                   :crow/send-retry-count
                   :crow/send-retry-interval-ms
                   :crow/remote-call-retry-count
                   :crow/remote-call-retry-interval-ms]))

(s/def :crow/address string?)
(s/def :crow/port pos-int?)
(s/def :crow/service
  (s/keys :req-un [:crow/address, :crow/port]))

(s/def :crow/call-options
  (s/keys :opt-un [:crow/timeout-ms]))

(s/fdef invoke
  :args (s/cat :ch :async/channel
               :factory :async-connect.client/connection-factory
               :service-desc :crow/service-descriptor
               :service :crow/service
               :call-desc :crow/call-descriptor
               :discovery-opts :crow/discovery-options)
  :ret  :async/channel)

(defn invoke
  [ch
   factory
   service-desc
   {:keys [address port] :as service}
   {:keys [target-ns fn-name fn-args]}
   {:keys [timeout-ms send-retry-count send-retry-interval-ms]
      :or {send-retry-count 3, send-retry-interval-ms 500}}]

  (let [msg (remote-call target-ns fn-name fn-args)]
    (go
      (try
        (let [msg  (some-> (<! (request/send factory address port msg timeout-ms send-retry-count send-retry-interval-ms)) (deref))
              resp (cond
                      (protocol-error? msg)
                      (throw+ {:type :protocol-error, :error-code (:error-code msg), :message (:message msg)})

                      (call-exception? msg)
                      (let [type-str    (:type msg)
                            stack-trace (:stack-trace msg)]
                        (throw+ {:type (keyword type-str)} stack-trace))

                      (call-result? msg)
                      (:obj msg)

                      (identical? :crow.request/timeout msg)
                      msg

                      :else
                      (throw (IllegalStateException. (str "No such message format: " (pr-str msg)))))]
          (>!! ch (box resp)))
        (catch Throwable th
          (>!! ch (box service-desc service th)))))
    ch))


(s/fdef find-services
  :args (s/cat :finder :crow/service-finder
               :service-desc :crow/service-descriptor
               :options :crow/discovery-options)
  :ret  (s/coll-of :crow/service))

(defn find-services
  [finder service-desc options]
  (when-not finder
    (throw+ {:type :finder-not-found, :message "ServiceFinder doesn't exist! You must start a service finder by start-service-finder at first!"}))
  (discover finder service-desc options))


(s/fdef find-service
  :args (s/cat :finder :crow/service-finder
               :service-desc :crow/service-descriptor
               :options :crow/discovery-options)
  :ret  :crow/service)

(defn find-service
  [finder service-desc options]
  (first (shuffle (find-services finder service-desc options))))


(s/fdef async-fn
  :args (s/cat :ch :async/channel
               :finder :crow/service-finder
               :service-desc :crow/service-descriptor
               :call-desc :crow/call-desc
               :options :crow/call-options)
  :ret  :async/channel)

(defn async-fn
  [ch
   {:keys [:async-connect.client/connection-factory]
      :as finder}
   service-desc
   call-desc
   options]
  (debug-pr (str "remote call. service: " (pr-str service-desc) ", fn: " (pr-str call-desc)))
  (if-let [service (find-service finder service-desc options)]
    (invoke ch connection-factory service-desc service call-desc options)
    (throw (IllegalStateException. (format "Service Not Found: service-name=%s, attributes=%s"
                                      (:service-name service-desc)
                                      (pr-str (:attributes service-desc)))))))

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
          {:service-name ~target-ns
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

(defn handle-result
  [finder result]
  (try
    (unbox result)
    (catch Throwable th
      (when-let [[service service-desc] (service-info result)]
        (finder/remove-service finder service-desc service))
      (throw th))))

(defn <!!+
  "read a channel. if the result value is an instance of
   Throwable, then throw the exception. Otherwise returns the
   result.
   This fn is a kind of <!! macro of core.async, so calling
   this fn will block current thread."
  [ch finder]
  (when ch
    (when-let [result (<!! ch)]
      (handle-result finder result))))

(defmacro <!+
  "read a channel. if the result value is an instance of
   Throwable, then throw the exception. Otherwise returns the
   result.
   This macro is a kind of <! macro of core.async, so this macro
   must be called in (go) block. it it why this is a macro, not fn.
   all contents of this macro is expanded into a go block."
  [ch finder]
  `(let [ch# ~ch
         finder# ~finder]
      (when-let [result# (<! ch#)]
        (handle-result finder# result#))))

(s/fdef make-call-fn
  :args (s/cat :ch :async/channel
               :finder :crow/service-finder
               :service-desc :crow/service-descriptor
               :call-desc :crow/call-descriptor
               :options :crow/call-options)
  :ret  (s/fspec :args empty? :ret any?))

(defn- make-call-fn
  [ch finder service-desc call-desc options]
  (fn []
    (try+
      (<!!+ (async-fn ch finder service-desc call-desc options))
      (catch [:type ::connection-error] _
        (:object &throw-context))
      (catch ConnectException ex
        ex))))


(defn- timeout?
  [msg]
  (boolean (when msg (identical? msg :crow.request/timeout))))

(defn- connection-error?
  [msg]
  (when msg
    (and (associative? msg)
         (= ::connection-error (:type msg)))))

(defn- connect-exception?
  [msg]
  (when msg
    (instance? ConnectException msg)))

(defn- need-retry?
  [msg]
  (when msg
    (or (timeout? msg) (connection-error? msg) (connect-exception? msg))))


(s/fdef try-call
  :args (s/cat :ch :async/channel
               :finder :crow/service-finder
               :service-desc :crow/service-descriptor
               :call-desc :crow/call-descriptor
               :call-opts :crow/call-options)
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
          (throw+ {:type ::connection-error, :kind (:type result)})

          (connection-error? result)
          (throw+ result)

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
        (try-call ~ch ~finder service-desc# call-desc# ~opts)))

  ([finder service-namespace attributes call-list opts]
    `(call (chan) ~finder ~service-namespace ~attributes ~call-list ~opts)))

(defmacro with-service
  ([ch factory service call-list opts]
   `(let [[service-desc# call-desc#] (parse-call-list ~call-list)]
      (invoke ~ch ~factory service-desc# ~service call-desc# ~opts)))

  ([factory service call-list opts]
    `(with-service (chan) ~factory ~service ~call-list ~opts)))





