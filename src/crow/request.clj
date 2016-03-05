(ns crow.request
  (:refer-clojure :exclude [send])
  (:require [aleph.tcp :as tcp]
            [msgpack.core :as msgpack]
            [manifold.stream :refer [try-put! try-take! close! take! put!] :as ms]
            [manifold.deferred :refer [let-flow chain success-deferred error-deferred] :as d]
            [msgpack.core :refer [pack unpack refine-ext] :as msgpack]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [byte-streams :refer [to-byte-array]]
            [schema.core :as s]
            [crow.logging :refer [trace-pr]]
            [slingshot.slingshot :refer [try+ throw+]])
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

(def ConnectionErrorType (s/enum ::drained ::timeout ::connect-failed ::connect-timeout))

;;; type describing connection errors.
(s/defrecord ConnectionError [type :- ConnectionErrorType])

(def drained (ConnectionError. ::drained))
(def timeout (ConnectionError. ::timeout))
(def connect-failed (ConnectionError. ::connect-failed))
(def connect-timeout (ConnectionError. ::connect-timeout))


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
    wrap-duplex-stream))

(defn- try-send
  [address port req timeout-ms send-retry-count send-retry-interval-ms]
  (letfn [(retry-send
            [stream retry-count result]
            (when stream
              (close! stream)
              (log/trace "stream closed."))
            (Thread/sleep (* send-retry-interval-ms retry-count))
            (when (<= retry-count send-retry-count)
              (log/info (format "retry! -- times: %d/%d" retry-count send-retry-count)))
            (d/recur retry-count result))]

    (d/loop [retry 0 result nil]
      (if (> retry send-retry-count)
        (cond
          (instance? ConnectionError result)
          (throw+ {:type ::connection-error, :kind (:type result)})

          (instance? Throwable result)
          (throw result)

          :else
          result)

        (-> (let-flow [stream (client address port)]
              (-> (let-flow [sent (send! stream req timeout-ms)]
                    (case sent
                      false
                      (do
                        (log/error (str "Couldn't send a message: " (pr-str req)))
                        (retry-send stream (inc retry) connect-failed))

                      timeout
                      (do
                        (log/error (str "Timeout: Couldn't send a message: " (pr-str req)))
                        (retry-send stream (inc retry) connect-timeout))

                      stream))
                  (d/catch ConnectException
                    (fn [ex]
                      (retry-send stream (inc retry) ex)))))
            (d/catch ConnectException
              (fn [ex]
                (retry-send nil (inc retry) ex))))))))

(defn send
  ([address port req timeout-ms send-retry-count send-retry-interval-ms]
    (log/trace "send-recv-timeout:" timeout-ms)
    (let-flow [stream (try-send address port req timeout-ms send-retry-count send-retry-interval-ms)]
      (-> (let-flow [msg (recv! stream timeout-ms)]
            (log/trace "receive!")
            (case msg
              timeout
              (do
                (log/error (str "Timeout: Couldn't receive a response for a req: " (pr-str req)))
                timeout)

              drained
              (do
                (log/error (str "Drained: Peer closed: req: " (pr-str req)))
                drained)

              msg))
          (d/catch
            (fn [th]
              (log/error th "send error!")
              (throw th)))
          (d/finally
            (fn []
              (log/trace "stream closed.")
              (close! stream))))))
  ([address port req timeout-ms]
    (send address port req timeout-ms 0 0)))

