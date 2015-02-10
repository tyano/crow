(ns crow.request
  (:refer-clojure :exclude [send])
  (:require [aleph.tcp :as tcp]
            [manifold.stream :refer [try-put! try-take! close!] :as s]
            [manifold.deferred :refer [let-flow chain] :as d]
            [msgpack.core :refer [pack unpack] :as msgpack]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [crow.protocol :refer [restore-ext]]
            [byte-streams :refer [to-byte-array]])
  (:import [msgpack.core Extension]))


(defn unpack-message
  [data]
  (let [msg (unpack data)]
    (if (instance? Extension msg)
      (restore-ext msg)
      msg)))

(def ^:dynamic *send-recv-timeout* 2000)

(defn send!
  "convert object into bytes and send the bytes into stream.
  returns a differed object holding true or false."
  [stream obj]
  (try-put! stream (pack obj) *send-recv-timeout* ::timeout))

(defn read-message
  "convert byte-buffer to byte-array and unpack the byte-array to a message format."
  [data]
  (case data
    ::drained data
    ::timeout data
    (-> data to-byte-array unpack-message)))

(defn recv!
  "read from stream and unpack the received bytes.
  returns a differed object holding an unpacked object."
  [stream]
  (chain (try-take! stream ::drained *send-recv-timeout* ::timeout)
         read-message))


(defn send
  [address port req]
  (chain (tcp/client {:host address, :port port})
    (fn [stream]
      (-> (chain (send! stream req)
            (fn [sent]
              (case sent
                false
                  (do
                    (log/error (str "Couldn't send a message: " (pr-str req)))
                    false)
                :crow.protocol/timeout
                  (do
                    (log/error (str "Timeout: Couldn't send a message: " (pr-str req)))
                    false)
                (recv! stream)))
            (fn [msg]
              (case msg
                false false
                :crow.protocol/timeout
                  (do
                    (log/error (str "Timeout: Couldn't receive a response for a req: " (pr-str req)))
                    false)
                :crow.protocol/drained
                  (do
                    (log/error (str "Drained: Peer closed: req: " (pr-str req)))
                    false)
                msg)))
          (d/finally
            (fn []
              (close! stream)))))))

