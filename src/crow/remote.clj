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

(defrecord CallDescription [target-ns fn-name args])

(defn invoke
  [ch timeout-ms {:keys [address port] :as service} {:keys [target-ns fn-name args] :as call-description}]
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

(def DiscoveryOptions {(s/optional-key :timeout-ms) s/Num})

(def Service {:address s/Str , :port s/Num, s/Keyword s/Any})
(s/defn find-service :- [Service]
  [service-name :- s/Str
   attrs        :- {s/Keyword s/Any}
   options      :- DiscoveryOptions]
  (when-not *default-finder*
    (throw+ {:type :finder-not-found, :message "ServiceFinder doesn't exist! You must start a service finder by start-service-finder at first!"}))
  (discover *default-finder* service-name attrs options))

(s/defn invoke-with-service-finder :- s/Any
  [ch
   timeout-ms   :- s/Num
   service-name :- s/Str
   attributes   :- {s/Keyword s/Any}
   call-desc    :- CallDescription]
  (if-let [services (seq (find-service service-name attributes {:timeout-ms timeout-ms}))]
    (let [service (first (shuffle services))]
      (invoke ch timeout-ms service call-desc))
    (throw (IllegalStateException. (format "Service Not Found: service-name=%s, attributes=%s"
                                      service-name
                                      (pr-str attributes))))))

(defmacro async
  ([ch timeout-ms call-list]
   (let [namespace-fn (first call-list)
         namespace-str (str namespace-fn)
         ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
         target-ns (first ns-fn-coll)
         fn-name (last ns-fn-coll)
         args (rest call-list)]
     `(do
        (debug-pr "remote call: " ~namespace-str)
        (invoke-with-service-finder ~ch ~timeout-ms ~target-ns {} (CallDescription. ~target-ns ~fn-name [~@args])))))

  ([timeout-ms call-list]
    `(async (chan) ~timeout-ms ~call-list))

  ([ch timeout-ms service-namespace attributes call-list]
   (let [service-name (name service-namespace)
         namespace-fn (first call-list)
         namespace-str (str namespace-fn)
         ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
         target-ns (first ns-fn-coll)
         fn-name (last ns-fn-coll)
         args (rest call-list)]
     `(do
        (debug-pr "remote call: " ~namespace-str)
        (invoke-with-service-finder ~ch ~timeout-ms ~service-name ~attributes (CallDescription. ~target-ns ~fn-name [~@args])))))

  ([timeout-ms service-namespace attributes call-list]
    `(async (chan) ~timeout-ms ~service-namespace ~attributes ~call-list)))

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

(defmacro call-remote
  ([ch timeout-ms call-list]
    `(<!!+ (async ~ch ~timeout-ms ~call-list)))
  ([ch timeout-ms service-namespace attributes call-list]
    `(<!!+ (async ~ch ~timeout-ms ~service-namespace ~attributes ~call-list))))

(defmacro make-call-fn
  ([ch timeout-ms call-list]
    `(fn []
        (try+
          (call-remote ~ch ~timeout-ms ~call-list)
          (catch ConnectionError {:keys [type]}
            type))))

  ([ch timeout-ms service-namespace attributes call-list]
    `(fn []
        (try+
          (call-remote ~ch ~timeout-ms ~service-namespace ~attributes ~call-list)
          (catch ConnectionError {:keys [type]}
            type)))))

(defmacro call
  ([ch call-list {:keys [timeout-ms retry-count base-retry-interval-ms]
                  :or [timeout-ms Long/MAX_VALUE retry-count 3 base-retry-interval-ms 2000]}]
    `(let [call-fn# (make-call-fn ~ch ~timeout-ms ~call-list)]
        (loop [result# (call-fn#) retry# ~retry-count interval# ~base-retry-interval-ms]
          (cond
            (zero? retry#)
            result#

            (or (= :timeout result#) (= :connect-failed result#))
            (do
              (Thread/sleep interval#)
              (recur (call-fn#) (dec retry#) (* interval# 2)))

            :else
            result#))))

  ([call-list {:keys [timeout-ms retry-count base-retry-interval-ms]
               :or [timeout-ms Long/MAX_VALUE retry-count 3 base-retry-interval-ms 2000]}]
    `(call (chan) ~call-list
        {:timeout-ms ~timeout-ms
         :retry-count ~retry-count
         :base-retry-interval-ms ~base-retry-interval-ms}))

  ([ch service-namespace attributes call-list {:keys [timeout-ms retry-count base-retry-interval-ms]
                                               :or [timeout-ms Long/MAX_VALUE retry-count 3 base-retry-interval-ms 2000]}]
    `(let [call-fn# (make-call-fn ~ch ~timeout-ms ~service-namespace ~attributes ~call-list)]
        (loop [result# (call-fn#) retry# ~retry-count interval# ~base-retry-interval-ms]
          (cond
            (zero? retry#)
            result#

            (or (= :timeout result#) (= :connect-failed result#))
            (do
              (Thread/sleep interval#)
              (recur (call-fn#) (dec retry#) (* interval# 2)))

            :else
            result#))))

  ([service-namespace attributes call-list {:keys [timeout-ms retry-count base-retry-interval-ms]
                                            :or [timeout-ms Long/MAX_VALUE retry-count 3 base-retry-interval-ms 2000]}]
    `(call (chan) ~service-namespace ~attributes ~call-list
        {:timeout-ms ~timeout-ms
         :retry-count ~retry-count
         :base-retry-interval-ms ~base-retry-interval-ms})))

(defn current-finder [] *default-finder*)

(defmacro with-service
  ([ch service call-list {:keys [timeout-ms] :or {timeout-ms Long/MAX_VALUE}}]
   (let [namespace-fn (first call-list)
         namespace-str (str namespace-fn)
         ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
         target-ns (first ns-fn-coll)
         fn-name (last ns-fn-coll)
         args (rest call-list)]
     `(do
        (debug-pr "remote call: " ~namespace-str)
        (invoke ~ch ~timeout-ms ~service ~target-ns ~fn-name ~@args))))

  ([service call-list {:keys [timeout-ms] :or {timeout-ms Long/MAX_VALUE}}]
    `(with-service (chan) ~service ~call-list {:timeout-ms ~timeout-ms})))





