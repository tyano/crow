(ns crow.remote
  (:refer-clojure :exclude [send])
  (:require [crow.protocol :refer [remote-call call-result? call-exception?]]
            [crow.request :refer [send] :as request]
            [manifold.deferred :refer [chain] :as d]
            [clojure.core.async :refer [go >! chan <!! close!]]
            [crow.discovery :refer [discover service-finder]]
            [crow.logging :refer [debug-pr]]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+]]))

(defn invoke
  [{:keys [address port] :as service} target-ns fn-name & args]
  (let [ch  (chan)
        msg (remote-call target-ns fn-name args)]
    (-> (send address port msg)
        (chain
          (fn [msg]
            (cond
              (call-exception? msg)
              (let [stack-trace (:stack-trace msg)]
                (go
                  (>! ch (Exception. ^String stack-trace))
                  (close! ch)))

              (call-result? msg)
              (if-let [result (:obj msg)]
                (go
                  (>! ch result)
                  (close! ch))
                (close! ch))

              :else
              (go
                (>! ch (IllegalStateException. (str "No such message format: " (pr-str msg))))
                (close! ch)))))
        (d/catch
          #(go (>! ch %))))
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

(defn with-timeout-fn
  [timeout f]
  (binding [request/*send-recv-timeout* timeout]
    (f)))

(defmacro with-timeout
  [timeout & expr]
  `(with-timeout-fn ~timeout (fn [] ~@expr)))

(defn find-service
  [service-name attrs]
  (when-not *default-finder*
    (throw+ {:type :finder-not-found, :message "ServiceFinder doesn't exist! You must start a service finder by start-service-finder at first!"}))
  (discover *default-finder* service-name attrs))

(defn invoke-with-service-finder
  [service-name attributes target-ns fn-name & args]
  (if-let [services (seq (find-service service-name attributes))]
    (apply invoke (first services) target-ns fn-name args)
    (throw (IllegalStateException. (format "Service Not Found: service-name=%s, attributes=%s"
                                      service-name
                                      (pr-str attributes))))))

(defmacro async
  ([call-list]
   (let [namespace-fn (first call-list)
         namespace-str (str namespace-fn)
         ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
         target-ns (first ns-fn-coll)
         fn-name (last ns-fn-coll)
         args (rest call-list)]
     `(do
        (debug-pr "remote call: " ~namespace-str)
        (invoke-with-service-finder ~target-ns {} ~target-ns ~fn-name ~@args))))
  ([service-namespace attributes call-list]
   (let [service-name (name service-namespace)
         namespace-fn (first call-list)
         namespace-str (str namespace-fn)
         ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
         target-ns (first ns-fn-coll)
         fn-name (last ns-fn-coll)
         args (rest call-list)]
     `(do
        (debug-pr "remote call: " ~namespace-str)
        (invoke-with-service-finder ~service-name ~attributes ~target-ns ~fn-name ~@args)))))

(defn <!!+
  "read a channel and if the result value is an instance of
   Throwable, then throw the exception. Otherwise returns the
   result.
   This macro is a kind <!! macro of core.async, so calling
   this macro will block current thread."
  [ch]
  (when ch
    (when-let [result (<!! ch)]
      (if (instance? Throwable result)
        (throw result)
        result))))


(defmacro call
  ([call-list]
    `(<!!+ (async ~call-list)))
  ([service-namespace attributes call-list]
    `(<!!+ (async ~service-namespace ~attributes ~call-list))))

(defn current-finder [] *default-finder*)

(defmacro with-service
  ([service call-list]
   (let [namespace-fn (first call-list)
         namespace-str (str namespace-fn)
         ns-fn-coll (clojure.string/split (str namespace-fn) #"/")
         target-ns (first ns-fn-coll)
         fn-name (last ns-fn-coll)
         args (rest call-list)]
     `(do
        (debug-pr "remote call: " ~namespace-str)
        (invoke ~service ~target-ns ~fn-name ~@args)))))





