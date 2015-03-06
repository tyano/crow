(ns crow.request
  (:refer-clojure :exclude [send])
  (:require [aleph.tcp :as tcp]
            [manifold.stream :refer [try-put! try-take! close!] :as s]
            [manifold.deferred :refer [let-flow chain] :as d]
            [msgpack.core :refer [pack unpack] :as msgpack]
            [msgpack.io :refer [ubytes int->bytes]]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [crow.protocol :refer [restore-ext]]
            [byte-streams :refer [to-byte-array]]
            [clojure.tools.logging :as log])
  (:import [msgpack.core Extension]
           [com.shelf.messagepack MessagePackFrameDecoder]))


(defn frame-decorder
  []
  (MessagePackFrameDecoder.
    (int Integer/MAX_VALUE) ;maxFrameLength
    false)) ;fail-fast


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
    (let [barray (to-byte-array data)] ;TODO this action is not required at newest Aleph, but it is needed yet at 0.4.0-beta3.
      (unpack-message barray))))

(defn recv!
  "read from stream and unpack the received bytes.
  returns a differed object holding an unpacked object."
  [stream]
  (chain (try-take! stream ::drained *send-recv-timeout* ::timeout)
         read-message))

(defn send
  [address port req]
  (log/trace "send-recv-timeout:" *send-recv-timeout*)
  (chain (tcp/client {:host address,
                      :port port,
                      :pipeline-transform #(.addFirst % "framer" (frame-decorder))})
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

