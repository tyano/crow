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
            [byte-streams :refer [to-byte-array]])
  (:import [msgpack.core Extension]
           [io.netty.handler.codec LengthFieldBasedFrameDecoder]))


(defn frame-decorder
  []
  (LengthFieldBasedFrameDecoder.
    (int Integer/MAX_VALUE) ;maxFrameLength
    (int 0)   ;lengthFieldOffset
    (int 4)   ;lengthFieldLength
    (int 0)   ;lengthAdjustment
    (int 4))) ;initialBytesToStrip


(defn unpack-message
  [data]
  (let [msg (unpack data)]
    (if (instance? Extension msg)
      (restore-ext msg)
      msg)))

(def ^:dynamic *send-recv-timeout* 2000)

(defn to-frame
  [^bytes bytes]
  (let [len (count bytes)]
    (ubytes (concat (int->bytes len) bytes))))

(defn send!
  "convert object into bytes and send the bytes into stream.
  returns a differed object holding true or false."
  [stream obj]
  (try-put! stream (to-frame (pack obj)) *send-recv-timeout* ::timeout))

(defn read-message
  "convert byte-buffer to byte-array and unpack the byte-array to a message format."
  [data]
  (case data
    ::drained data
    ::timeout data
    (unpack-message data)))

(defn recv!
  "read from stream and unpack the received bytes.
  returns a differed object holding an unpacked object."
  [stream]
  (chain (try-take! stream ::drained *send-recv-timeout* ::timeout)
         read-message))

(defn send
  [address port req]
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

