(ns crow.remote
  (:refer-clojure :exclude [send])
  (:require [crow.protocol :refer [remote-call call-result? call-exception? protocol-error?]]
            [crow.request :refer [send] :as request]
            [crow.boxed :refer [box unbox]]
            [manifold.deferred :refer [chain] :as d]
            [clojure.core.async :refer [>!! chan <!! <! close!]]
            [crow.discovery :refer [discover service-finder]]
            [crow.logging :refer [debug-pr]]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [try+ throw+]]
            [schema.core :as s])
  (:import [java.net ConnectException]))

(s/defrecord ServiceDescriptor
  [service-name :- s/Str
   attributes   :- (s/maybe {s/Keyword s/Any})])

(s/defrecord CallDescriptor
  [target-ns      :- s/Str
   fn-name        :- s/Str
   args           :- (s/maybe [s/Any])])

(def DiscoveryOptions {(s/optional-key :timeout-ms) s/Num
                       (s/optional-key :send-retry-count) s/Num
                       (s/optional-key :send-retry-interval-ms) s/Num
                       (s/optional-key :remote-call-retry-count) s/Num
                       (s/optional-key :remote-call-retry-interval-ms) s/Num
                       s/Keyword s/Any})

(def Service {:address  s/Str
              :port     s/Num
              s/Keyword s/Any})

(def CallOptions {(s/optional-key :timeout-ms) s/Num
                  s/Keyword s/Any})

(s/defn invoke
  [ch
   {:keys [address port] :as service} :- Service
   {:keys [target-ns fn-name args]} :- CallDescriptor
   {:keys [timeout-ms send-retry-count send-retry-interval-ms], :or {send-retry-count 3, send-retry-interval-ms 500}} :- DiscoveryOptions]
  (let [msg (remote-call target-ns fn-name args)]
    (-> (send address port msg timeout-ms send-retry-count send-retry-interval-ms)
        (chain
          (fn [msg]
            (cond
              (protocol-error? msg)
              (throw+ {:type :protocol-error, :error-code (:error-code msg), :message (:message msg)})

              (call-exception? msg)
              (let [type-str    (:type msg)
                    stack-trace (:stack-trace msg)]
                (throw+ {:type (keyword type-str)} stack-trace))

              (call-result? msg)
              (:obj msg)

              (identical? request/drained msg)
              (do
                (log/debug "DRAINED!")
                nil)

              (identical? request/timeout msg)
              msg

              :else
              (throw (IllegalStateException. (str "No such message format: " (pr-str msg))))))
          (fn [msg']
            (>!! ch (box msg'))))

        (d/catch
          (fn [th]
            (>!! ch (box th))))
        (d/finally
          (fn [] (close! ch))))
    ch))

(def ^:dynamic *default-finder*)

(defn start-service-finder
  [registrar-source]
  (def ^:dynamic *default-finder* (service-finder registrar-source)))

(defn with-finder-fn
  [finder f]
  (binding [*default-finder* finder]
    (f)))

(defmacro with-finder
  [finder & expr]
  `(with-finder-fn ~finder (fn [] ~@expr)))

(s/defn find-services :- [Service]
  [{:keys [service-name attributes]} :- ServiceDescriptor
   options :- DiscoveryOptions]
  (when-not *default-finder*
    (throw+ {:type :finder-not-found, :message "ServiceFinder doesn't exist! You must start a service finder by start-service-finder at first!"}))
  (discover *default-finder* service-name attributes options))

(s/defn find-service :- Service
  [service-desc :- ServiceDescriptor
   options      :- DiscoveryOptions]
  (first (shuffle (find-services service-desc options))))

(s/defn async-fn :- s/Any
  [ch
   service-desc :- ServiceDescriptor
   call-desc    :- CallDescriptor
   options      :- CallOptions]
  (debug-pr (str "remote call. service: " (pr-str service-desc) ", fn: " (pr-str call-desc)))
  (if-let [service (find-service service-desc options)]
    (invoke ch service call-desc options)
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
          (ServiceDescriptor. ~target-ns {})
          (CallDescriptor. ~target-ns ~fn-name ~args))))

  ([service-namespace attributes call-list]
    (let [service-name (name service-namespace)
          namespace-fn (first call-list)
          namespace-str (str namespace-fn)
          ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
          target-ns (first ns-fn-coll)
          fn-name (last ns-fn-coll)
          args (vec (rest call-list))]
      `(vector
          (ServiceDescriptor. ~service-name ~attributes)
          (CallDescriptor. ~target-ns ~fn-name ~args)))))

(defmacro async
  ([ch call-list options]
   `(let [[service-desc# call-desc#] (parse-call-list ~call-list)]
      (async-fn ~ch service-desc# call-desc# ~options)))

  ([call-list options]
    `(async (chan) ~call-list ~options))

  ([ch service-namespace attributes call-list options]
   `(let [[service-desc# call-desc#] (parse-call-list ~service-namespace ~attributes ~call-list)]
      (async-fn ~ch service-desc# call-desc# ~options)))

  ([service-namespace attributes call-list options]
    `(async (chan) ~service-namespace ~attributes ~call-list ~options)))

(defn <!!+
  "read a channel and if the result value is an instance of
   Throwable, then throw the exception. Otherwise returns the
   result.
   This macro is a kind of <!! macro of core.async, so calling
   this macro will block current thread."
  [ch]
  (when ch
    (let [result (<!! ch)]
      (unbox result))))

(defn <!+
  [ch]
  (when ch
    (let [result (<! ch)]
      (unbox result))))

(s/defn ^:private make-call-fn
  [ch
   service-desc :- ServiceDescriptor
   call-desc :- CallDescriptor
   options :- CallOptions]
  (fn []
    (try+
      (<!!+ (async-fn ch service-desc call-desc options))
      (catch [:type :crow.request/connection-error] _
        (:object &throw-context))
      (catch ConnectException ex
        ex))))


(defn- timeout?
  [msg]
  (when msg (identical? msg request/timeout)))

(defn- connection-error?
  [msg]
  (when msg
    (and (associative? msg)
         (= :crow.request/connection-error (:type msg)))))

(defn- connect-exception?
  [msg]
  (when msg
    (instance? ConnectException msg)))

(defn- need-retry?
  [msg]
  (when msg
    (or (timeout? msg) (connection-error? msg) (connect-exception? msg))))

(s/defn try-call
  [ch
   service-desc :- ServiceDescriptor
   call-desc :- CallDescriptor
   {:keys [remote-call-retry-count remote-call-retry-interval-ms]
    :or {remote-call-retry-count 3 remote-call-retry-interval-ms 500} :as options} :- CallOptions]

  (let [call-fn (make-call-fn ch service-desc call-desc options)]
    (loop [result nil retry 0]
      (if (> retry remote-call-retry-count)
        (cond
          (timeout? result)
          (throw+ {:type :crow.request/connection-error, :kind (:type result)})

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
  ([ch call-list opts]
    `(let [[service-desc# call-desc#] (parse-call-list ~call-list)]
        (try-call ~ch service-desc# call-desc# ~opts)))

  ([call-list opts]
    `(call (chan) ~call-list ~opts))

  ([ch service-namespace attributes call-list opts]
    `(let [[service-desc# call-desc#] (parse-call-list ~service-namespace ~attributes ~call-list)]
        (try-call ~ch service-desc# call-desc# ~opts)))

  ([service-namespace attributes call-list opts]
    `(call (chan) ~service-namespace ~attributes ~call-list ~opts)))

(defn current-finder [] *default-finder*)

(defmacro with-service
  ([ch service call-list opts]
   `(let [[service-desc# call-desc#] (parse-call-list ~call-list)]
      (invoke ~ch ~service call-desc# ~opts)))

  ([service call-list opts]
    `(with-service (chan) ~service ~call-list ~opts)))





