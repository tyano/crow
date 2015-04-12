(ns crow.request
  (:refer-clojure :exclude [send])
  (:require [aleph.tcp :as tcp]
            [msgpack.core :as msgpack]
            [manifold.stream :refer [try-put! try-take! close!] :as s]
            [manifold.deferred :refer [let-flow chain] :as d]
            [msgpack.core :refer [pack unpack] :as msgpack]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [crow.protocol :refer [restore-ext]]
            [byte-streams :refer [to-byte-array]]
            [clojure.tools.logging :as log])
  (:import [com.shelf.messagepack MessagePackFrameDecoder]))


(defn frame-decorder
  []
  (MessagePackFrameDecoder.
    (int Integer/MAX_VALUE) ;maxFrameLength
    false)) ;fail-fast

(defn format-stack-trace
  [exception]
  (let [elements (cons (str (.getName (class exception)) ": " (.getMessage exception))
                    (loop [^Throwable ex exception elems []]
                      (if (nil? ex)
                        elems
                        (let [calls (-> (map str (.getStackTrace ex))
                                        (as-> x (if (empty? elems) x (cons "Caused by:" x))))]
                          (recur (.getCause ex) (apply conj elems calls))))))]
    (apply str (map println-str elements))))

(defn unpack-message
  [data]
  (let [msg (unpack data)]
    (restore-ext msg)))

(def ^:dynamic *send-recv-timeout* 2000)

(defn send!
  "convert object into bytes and send the bytes into stream.
  returns a differed object holding true or false."
  [stream obj]
  (try-put! stream obj *send-recv-timeout* ::timeout))

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
  (try-take! stream ::drained *send-recv-timeout* ::timeout))

(defn wrap-duplex-stream
  [stream]
  (let [out (s/stream)
        in  (s/stream)]
    (s/connect
      (s/map pack out)
      stream)
    (s/connect
      (s/map read-message stream)
      in)
    (s/splice out in)))

(defn client
  [address port]
  (chain (tcp/client {:host address,
                      :port port,
                      :pipeline-transform #(.addFirst % "framer" (frame-decorder))})
    #(wrap-duplex-stream %)))

(defn send
  [address port req]
  (log/trace "send-recv-timeout:" *send-recv-timeout*)
  (chain (tcp/client {:host address,
                      :port port,
                      :pipeline-transform #(.addFirst % "framer" (frame-decorder))})
    #(wrap-duplex-stream %)
    (fn [stream]
      (log/trace "sending...")
      (-> (send! stream req)
          (chain
            (fn [sent]
              (log/trace "sent!")
              (case sent
                false
                (do
                  (log/error (str "Couldn't send a message: " (pr-str req)))
                  false)

                ::timeout
                (do
                  (log/error (str "Timeout: Couldn't send a message: " (pr-str req)))
                  ::timeout)

                (d/future @(recv! stream))))
            (fn [msg]
              (log/trace "receive!")
              (case msg
                false
                false

                ::timeout
                (do
                  (log/error (str "Timeout: Couldn't receive a response for a req: " (pr-str req)))
                  ::timeout)

                ::drained
                (do
                  (log/error (str "Drained: Peer closed: req: " (pr-str req)))
                  ::drained)

                msg)))
          (d/catch
            (fn [th]
              (log/error th "send error!")
              (throw th)))
          (d/finally
            (fn []
              (close! stream)))))))
