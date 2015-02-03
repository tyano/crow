(ns crow.logging
  (:require [clojure.tools.logging :as log]))

(defmacro log-output
  ([log-level use-pr? msg value]
    (let [log-fn     (symbol (str 'clojure.tools.logging "/" log-level))
          print-fn   (if use-pr? 'pr-str 'identity)]
      `(let [r# ~value] (~log-fn ~msg (~print-fn r#)) r#)))
  ([log-level use-pr? value]
    (let [log-fn     (symbol (str 'clojure.tools.logging "/" log-level))
          print-fn   (if use-pr? 'pr-str 'identity)]
      `(let [r# ~value] (~log-fn (~print-fn r#)) r#))))

(defmacro trace-pr
  ([msg value] `(log-output ~'trace true ~msg ~value))
  ([value]     `(log-output ~'trace true ~value)))

(defmacro debug-pr
  ([msg value] `(log-output ~'debug true ~msg ~value))
  ([value]     `(log-output ~'debug true ~value)))

(defmacro error-pr
  ([msg value] `(log-output ~'error true ~msg ~value))
  ([value]     `(log-output ~'error true ~value)))
