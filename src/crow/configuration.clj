(ns crow.configuration
  (:require [clojure.java.io :refer [reader]]
            [clojure.edn :as edn])
  (:import [java.io PushbackReader]))


(defn- read-from-file
  [file-path]
  (with-open [rdr (reader file-path)]
    (load-reader rdr)))

(defn- assert-map
  [config]
  (assert (map? config) "A configuration file must return a map containing service configurations."))

(defn- exec-constructor
  "convert a value to a function if the value is a symbol or a string for a function."
  [sym config]
  (if (or (symbol? sym) (string? sym))
    (let [fn-sym (if (symbol? sym) sym (symbol sym))
          fn-ns  (namespace fn-sym)]
      (require (symbol fn-ns))
      (if-let [resolved-fn (find-var fn-sym)]
        (resolved-fn config)
        (throw (IllegalStateException. (str "No such function: " sym)))))
    sym))

(def constructor-fn-keys #{:registrar-source :id-store :object-marshaller})

(defn- resolve-constructor-fn
  "resolve all keys in a configuration which might be a symbol or a string of
  constructor-function. if a fn is resolved, the fn will be executed with 'config' as a argument."
  [config]
  (when config
    (loop [c config ks constructor-fn-keys]
      (if-let [k (first ks)]
        (recur (update c k #(exec-constructor % config)) (rest ks))
        c))))

(defn from-path
  "Create a configuration-map from a source path.
  The file must be a readable clojure source file and must return a map for evaluation.
  The file will be evaluated as a Clojure program, so you must be care about
  the path is trusted for evaluation."
  [file-path]
  (let [config (read-from-file file-path)]
    (assert-map config)
    (resolve-constructor-fn config)))

(defn from-edn
  "read a path containing EDN string.
  Differ than 'from-path', this functions is safe for evaluation, because
  this load the file-contents as a EDN string (not Clojure program).
  The EDN string must be a map containing all configurations."
  [file-path]
  (with-open [stream (PushbackReader. (reader file-path))]
    (let [config (edn/read stream)]
      (assert-map config)
      (resolve-constructor-fn config))))

