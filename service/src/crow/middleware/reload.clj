(ns crow.middleware.reload
  (:require [ns-tracker.core :refer [ns-tracker]]
            [clojure.tools.logging :as log]))

(defn wrap-reload
  "Reload namespaces of modified files before the message is passed to the
  service handler.
  Accepts the following options:
  :source-dirs - A list of directories that contain the source files.
                 Defaults to [\"src\"]."
  {:arglists '([options])}
  [& [options]]
  (let [source-dirs (:source-dirs options ["src"])
        modified-namespaces (ns-tracker source-dirs)]
    (fn [handler]
      (fn [msg]
        (doseq [ns-sym (modified-namespaces)]
          (log/trace (str "reload: " ns-sym))
          (require ns-sym :reload))
        (handler msg)))))

