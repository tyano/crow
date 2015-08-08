(ns crow.remote
  (:refer-clojure :exclude [send])
  (:require [crow.protocol :refer [remote-call call-result? call-exception? protocol-error?]]
            [crow.request :refer [send] :as request]
            [manifold.deferred :refer [chain] :as d]
            [clojure.core.async :refer [>!! chan <!! close! alts!! timeout thread]]
            [crow.discovery :refer [discover service-finder]]
            [crow.logging :refer [debug-pr]]
            [clojure.tools.logging :as log]
            [slingshot.slingshot :refer [throw+]])
  (:import [manifold.deferred Recur]))

(def ^:dynamic *retry-interval* 3000)

(defn invoke
  [{:keys [address port] :as service} target-ns fn-name & args]
  (let [ch  (chan)
        msg (remote-call target-ns fn-name args)
        timeout-ms request/*send-recv-timeout*]
    (thread
      (try
        (binding [request/*send-recv-timeout* timeout-ms]
          (let [result
                  (loop [retry-count 3]
                    (if (= retry-count 0)
                      (throw (IllegalStateException. "retry timeout!!"))
                      (let [msg @(send address port msg)]
                        (cond
                          (false? msg) ; Could'nt send. retry
                          (do
                            (Thread/sleep *retry-interval*)
                            (log/debug (str "RETRY! - remaining retry count : " (dec retry-count)))
                            (recur (dec retry-count)))

                          (protocol-error? msg)
                          (throw+ {:type :protocol-error, :error-code (:error-code msg), :message (:message msg)})

                          (call-exception? msg)
                          (let [type-str    (:type msg)
                                stack-trace (:stack-trace msg)]
                            (throw+ {:type (keyword type-str)} stack-trace))

                          (call-result? msg)
                          (:obj msg)

                          :crow.request/timeout
                          (do
                            (Thread/sleep *retry-interval*)
                            (log/debug (str "RETRY! - remaining retry count : " (dec retry-count)))
                            (recur (dec retry-count)))

                          :crow.request/drained
                          (do
                            (log/debug "DRAINED!")
                            nil)

                          :else
                          (throw (IllegalStateException. (str "No such message format: " (pr-str msg))))))))]
          (try
            (when-not (nil? result)
              (>!! ch result))
            (finally
              (close! ch)))))
        (catch Throwable th
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

(defn set-default-timeout-ms!
  [timeout]
  (alter-var-root #'request/*send-recv-timeout* (fn [_] timeout)))

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
    (apply invoke (first (shuffle services)) target-ns fn-name args)
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
    ;; if no data is supplied to ch until 4* msecs of *send-receive-timeout*,
    ;; it should be a bug... (because invoke will retry only 3 times)
    (let [timeout-ch (timeout (+ (* request/*send-recv-timeout* 4) (* *retry-interval* 4)))
          [result c] (alts!! [ch timeout-ch])]
      (if (= c timeout-ch)
        (throw+ {:type :channel-read-timeout})
        (if (instance? Throwable result)
          (throw result)
          result)))))


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





