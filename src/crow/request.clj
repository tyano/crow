(ns crow.request
  (:refer-clojure :exclude [send])
  (:require [aleph.tcp :as tcp]
            [msgpack.core :as msgpack]
            [manifold.stream :refer [try-put! try-take! close! take! put!] :as ms]
            [manifold.deferred :refer [let-flow chain] :as d]
            [msgpack.core :refer [pack unpack refine-ext] :as msgpack]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [byte-streams :refer [to-byte-array]]
            [clojure.tools.logging :as log]
            [schema.core :as s])
  (:import [com.shelf.messagepack MessagePackFrameDecoder]
           [msgpack.core Ext]
           [java.net ConnectException]))


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
    (if (instance? Ext msg)
      (refine-ext msg)
      msg)))

(def ConnectionErrorType (s/enum :drained :timeout :connect-failed))

;;; type describing connection errors.
(s/defrecord ConnectionError [type :- ConnectionErrorType])

(def drained (ConnectionError. :drained))
(def timeout (ConnectionError. :timeout))
(def connect-failed (ConnectionError. :connect-failed))


(defn send!
  "convert object into bytes and send the bytes into stream.
  returns a differed object holding true or false."
  [stream obj timeout-ms]
  (if timeout-ms
    (try-put! stream obj timeout-ms timeout)
    (put! stream obj)))

(defn read-message
  "unpack a byte-array to a message format."
  [data]
  (case data
    drained data
    timeout data
    (unpack-message data)))

(defn recv!
  "read from stream and unpack the received bytes.
  returns a differed object holding an unpacked object."
  [stream timeout-ms]
  (if timeout-ms
    (try-take! stream drained timeout-ms timeout)
    (take! stream drained)))

(defn wrap-duplex-stream
  [stream]
  (let [out (ms/stream)
        in  (ms/stream)]
    (ms/connect
      (ms/map pack out)
      stream)
    (ms/connect
      (ms/map read-message stream)
      in)
    (ms/splice out in)))

(defn client
  [address port]
  (chain (tcp/client {:host address,
                      :port port,
                      :pipeline-transform #(.addFirst % "framer" (frame-decorder))})
    #(wrap-duplex-stream %)))

(defn send*
  [address port req timeout-ms]
  (log/trace "send-recv-timeout:" timeout-ms)
  (chain (tcp/client {:host address,
                      :port port,
                      :pipeline-transform #(.addFirst % "framer" (frame-decorder))})
    #(wrap-duplex-stream %)
    (fn [stream]
      (log/trace "sending...")
      (-> (send! stream req timeout-ms)
          (chain
            (fn [sent]
              (log/trace "sent!")
              (case sent
                false
                (do
                  (log/error (str "Couldn't send a message: " (pr-str req)))
                  false)

                timeout
                (do
                  (log/error (str "Timeout: Couldn't send a message: " (pr-str req)))
                  timeout)

                (recv! stream timeout-ms)))
            (fn [msg]
              (log/trace "receive!")
              (case msg
                false
                false

                timeout
                (do
                  (log/error (str "Timeout: Couldn't receive a response for a req: " (pr-str req)))
                  timeout)

                drained
                (do
                  (log/error (str "Drained: Peer closed: req: " (pr-str req)))
                  drained)

                msg)))
          (d/catch
            (fn [th]
              (log/error th "send error!")
              (throw th)))
          (d/finally
            (fn []
              (log/trace "stream closed.")
              (close! stream)))))))

(defn send
  [address port req timeout-ms retry-count retry-interval-ms stream-handler]
  (d/loop [retry retry-count result nil]
    (if (>= retry 0)
      (try
        @(-> (send* address port req timeout-ms)
             (stream-handler))
        (catch ConnectException ex
          (Thread/sleep retry-interval-ms)
          (log/info "retry! -- remaining " (dec retry) " times.")
          (d/recur (dec retry) ex)))
      (if (instance? Throwable result)
        (throw result)
        result))))

