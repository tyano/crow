(ns crow.configuration
  (:require [clojure.java.io :refer [reader]]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [integrant.core :as ig])
  (:import [java.io PushbackReader]))


(defn- read-from-file
  [file-path]
  (with-open [rdr (reader file-path)]
    (load-reader rdr)))

(defn- assert-map
  [config]
  (assert (map? config) "A configuration file must return a map containing service configurations.")
  config)

(defn- from-path
  "Create a configuration-map from a source path.
  The file must be a readable clojure source file and must return a map for evaluation.
  The file will be evaluated as a Clojure program, so you must be care about
  the path is trusted for evaluation."
  [file-path]
  (let [config (read-from-file file-path)]
    (assert-map config)))

(defn- from-edn
  "read a path containing EDN string.
  Differ than 'from-path', this functions is safe for evaluation, because
  this load the file-contents as a EDN string (not Clojure program).
  The EDN string must be a map containing all configurations."
  [file-path]
  (with-open [stream (PushbackReader. (reader file-path))]
    (let [config (edn/read stream)]
      (assert-map config))))

(defn- extension
  [path]
  (if (seq path)
    (let [idx (string/last-index-of path ".")]
      (if (and idx (> (count path) (inc idx)))
        (subs path (inc idx))
        ""))
    ""))

(defmulti from
  (fn [file-path] (extension file-path)))


(defmethod from :default
  [file-path]
  (from-path file-path))

(defmethod from "edn"
  [file-path]
  (from-edn file-path))

(defmethod from "ig"
  [file-path]
  (let [config (ig/read-string (slurp file-path))]
    (assert-map config)))


