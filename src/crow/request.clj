(ns crow.request
  (:refer-clojure :exclude [send])
  (:require [aleph.tcp :as tcp]
            [manifold.deferred :refer [let-flow] :as d]
            [crow.protocol :refer [send! recv!] :as protocol]))

(defn send
  [address port req]
  (let-flow [stream (tcp/client {:host address, :port port})
             sent?  (send! stream req)]
    (when sent? (recv! stream))))
