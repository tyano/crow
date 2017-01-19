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
            [slingshot.slingshot :refer [try+ throw+]]
            [async-connect.box :as box]
            [clojure.core.async :refer [<! >! <!! >!! go go-loop alt! alts! thread chan] :as async]
            [async-connect.client :refer [connect close] :as async-connect])
  (:import [com.shelf.messagepack MessagePackFrameDecoder]
           [msgpack.core Ext]
           [java.net ConnectException]
           [io.netty.handler.codec.bytes
              ByteArrayDecoder
              ByteArrayEncoder]))


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
    (try-put! stream obj timeout-ms ::timeout)
    (put! stream obj)))

(defn read-message
  "unpack a byte-array to a message format."
  [data]
  (when data (unpack-message data)))

(defn recv!
  "read from stream and unpack the received bytes.
  returns a differed object holding an unpacked object."
  [stream timeout-ms]
  (if timeout-ms
    (try-take! stream drained timeout-ms timeout)
    (take! stream drained)))

(def packer (map #(update % :message pack)))
(def unpacker (map #(box/update % read-message)))

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

(defn- initialize-channel
  [netty-ch config]
  (.. netty-ch
    (pipeline)
    (addLast "messagepack-framedecoder" (frame-decorder))
    (addLast "bytes-decoder" (ByteArrayDecoder.))
    (addLast "bytes-encoder" (ByteArrayEncoder.))))

(def bootstrap
  (async-connect/make-bootstrap
    {:client.config/channel-initializer initialize-channel}))

(defn client
  [address port]
  (let [read-ch  (chan 500 unpacker)
        write-ch (chan 500 packer)]
    (async-connect/connect bootstrap address port read-ch write-ch)))

(defn- try-send
  [address port req timeout-ms send-retry-count send-retry-interval-ms]
  (letfn [(retry-send
            [conn retry-count result]
            (when conn
              (close conn)
              (log/trace "channel closed."))
            (Thread/sleep (* send-retry-interval-ms retry-count))
            (when (<= retry-count send-retry-count)
              (log/info (format "retry! -- times: %d/%d" retry-count send-retry-count)))
            {:type :recur :retry-count retry-count :result result})]

    (go-loop [retry 0 result nil]
      (if (> retry send-retry-count)
        (cond
          (instance? ConnectionError result)
          (throw+ {:type ::connection-error, :kind (:type result)})

          (instance? Throwable result)
          (throw result)

          :else
          result)

        (let [{:keys [read-ch write-ch] :as conn} (client address port)
              {:keys [type] :as c}
                  (try
                    (case (alt!
                            [[write-ch {:message req :flush? true}]]
                            ([v ch] v)

                            [(async/timeout timeout-ms)]
                            ([v ch] timeout))
                      false
                      (do
                        (log/error (str "Couldn't send a message: " (pr-str req)))
                        (retry-send conn (inc retry) connect-failed))

                      timeout
                      (do
                        (log/error (str "Timeout: Couldn't send a message: " (pr-str req)))
                        (retry-send conn (inc retry) connect-timeout))

                      {:type :success :resut conn})
                    (catch ConnectException ex
                      (retry-send nil (inc retry) ex)))]
          (case type
            :recur
            (recur (:retry-count c) (:result c))

            :success
            (:result c)))))))


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

