(ns crow.remote
  (:refer-clojure :exclude [send])
  (:require [crow.protocol :refer [remote-call call-result? call-exception? protocol-error?]]
            [crow.request :refer [send] :as request]
            [manifold.deferred :refer [chain] :as d]
            [clojure.core.async :refer [>!! chan <!! close! alts!! timeout thread]]
            [crow.discovery :refer [discover service-finder]]
            [crow.logging :refer [debug-pr]]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [try+ throw+]]
            [schema.core :as s])
  (:import [manifold.deferred Recur]
           [crow.request ConnectionError]))

(def ^:dynamic *retry-interval* 3000)

(s/defrecord ServiceDescriptor
  [service-name :- s/Str
   attributes   :- {s/Keyword s/Any}])

(s/defrecord CallDescriptor
  [target-ns      :- s/Str
   fn-name        :- s/Str
   args           :- (s/maybe [s/Any])])

(def DiscoveryOptions {(s/optional-key :timeout-ms) s/Num
                       s/Keyword s/Any})

(def Service {:address  s/Str
              :port     s/Num
              s/Keyword s/Any})

(def CallOptions {(s/optional-key :timeout-ms) s/Num
                  s/Keyword s/Any})

(s/defn invoke
  [ch
   timeout-ms :- (s/maybe s/Num)
   {:keys [address port] :as service} :- Service
   {:keys [target-ns fn-name args]} :- CallDescriptor]
  (let [msg (remote-call target-ns fn-name args)]
    (-> (send address port msg timeout-ms)
      (chain
        (fn [msg]
          (cond
            (false? msg) ; Could'nt send.
            (do
              (log/debug "Couldn't send. maybe couldn't connnect to pear.")
              request/connect-failed)

            (protocol-error? msg)
            (throw+ {:type :protocol-error, :error-code (:error-code msg), :message (:message msg)})

            (call-exception? msg)
            (let [type-str    (:type msg)
                  stack-trace (:stack-trace msg)]
              (throw+ {:type (keyword type-str)} stack-trace))

            (call-result? msg)
            (:obj msg)

            (= request/timeout msg)
            msg

            (= request/drained msg)
            (do
              (log/debug "DRAINED!")
              nil)

            :else
            (throw (IllegalStateException. (str "No such message format: " (pr-str msg))))))
        (fn [msg']
          (try
            (when-not (nil? msg')
              (>!! ch msg'))
            (finally
              (close! ch)))))

      (d/catch
        (fn [th]
          (>!! ch th))))
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

(s/defn find-service :- [Service]
  [{:keys [service-name attributes]} :- ServiceDescriptor
   options      :- DiscoveryOptions]
  (when-not *default-finder*
    (throw+ {:type :finder-not-found, :message "ServiceFinder doesn't exist! You must start a service finder by start-service-finder at first!"}))
  (discover *default-finder* service-name attributes options))

(s/defn async-fn :- s/Any
  [ch
   service-desc :- ServiceDescriptor
   call-desc    :- CallDescriptor
   options      :- CallOptions]
  (debug-pr (str "remote call. service: " (pr-str service-desc) ", fn: " (pr-str call-desc)))
  (if-let [services (seq (find-service service-desc options))]
    (let [service (first (shuffle services))]
      (invoke ch (:timeout-ms options) service call-desc))
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
    `(async (chan 1) ~call-list ~options))

  ([ch service-namespace attributes call-list options]
   `(let [[service-desc# call-desc#] (parse-call-list ~service-namespace ~attributes ~call-list)]
      (async-fn ~ch service-desc# call-desc# ~options)))

  ([service-namespace attributes call-list options]
    `(async (chan 1) ~service-namespace ~attributes ~call-list ~options)))

(defn <!!+
  "read a channel and if the result value is an instance of
   Throwable, then throw the exception. Otherwise returns the
   result.
   This macro is a kind <!! macro of core.async, so calling
   this macro will block current thread."
  [ch]
  (when ch
    ;; if no data is supplied to ch until 4* msecs of *send-receive-timeout*,
    ;; it should be a bug... (because invoke will retry only 3 times)
    (let [result (<!! ch)]
      (cond
        (instance? Throwable result)
        (throw result)

        (instance? ConnectionError result)
        (throw+ result)

        :else
        result))))

(s/defn call-fn
  [ch
   service-desc :- ServiceDescriptor
   call-desc :- CallDescriptor
   options   :- CallOptions]
   (<!!+ (async-fn ch service-desc call-desc options)))

(s/defn ^:private make-call-fn
  [ch
   service-desc :- ServiceDescriptor
   call-desc :- CallDescriptor
   options :- CallOptions]
  (fn []
    (try+
      (call-fn ch service-desc call-desc options)
      (catch ConnectionError e e))))

(s/defn try-call
  [ch
   service-desc :- ServiceDescriptor
   call-desc :- CallDescriptor
   {:keys [retry-count base-retry-interval-ms]
    :or [retry-count 3 base-retry-interval-ms 2000] :as options} :- CallOptions]
  (let [call-fn (make-call-fn ch service-desc call-desc options)]
    (loop [result (call-fn) retry retry-count interval base-retry-interval-ms]
      (cond
        (and (instance? ConnectionError result)
          (or (= :timeout (:type result)) (= :connect-failed (:type result))))
        (let [new-retry (debug-pr "RETRY! " (dec retry))]
          (if (zero? new-retry)
            (if (instance? ConnectionError result)
              (throw+ {:type (:type result)})
              result)
            (do
              (Thread/sleep interval)
              (recur (call-fn) new-retry (* interval 2)))))

        :else
        result))))

(defmacro call
  ([ch call-list opts]
    `(let [[service-desc# call-desc#] (parse-call-list ~call-list)]
        (try-call ~ch service-desc# call-desc# ~opts)))

  ([call-list opts]
    `(call (chan 1) ~call-list ~opts))

  ([ch service-namespace attributes call-list opts]
    `(let [[service-desc# call-desc#] (parse-call-list ~service-namespace ~attributes ~call-list)]
        (try-call ~ch service-desc# call-desc# ~opts)))

  ([service-namespace attributes call-list opts]
    `(call (chan 1) ~service-namespace ~attributes ~call-list ~opts)))

(defn current-finder [] *default-finder*)

(defmacro with-service
  ([ch service call-list opts]
   `(let [[_ call-desc#] (parse-call-list ~call-list)]
      (invoke ~ch (:timeout-ms ~opts) ~service call-desc#)))

  ([service call-list opts]
    `(with-service (chan 1) ~service ~call-list ~opts)))





