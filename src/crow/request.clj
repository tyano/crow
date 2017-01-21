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
            [async-connect.client :refer [connect close] :as async-connect]
            [async-connect.box :refer [boxed]])
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

(def connection-errors #{::timeout ::connect-failed ::connect-timeout})
(defn connection-error? [v] (boolean (when v (connection-errors v))))

(defn read-message
  "unpack a byte-array to a message format."
  [data]
  (when data (unpack-message data)))

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
          (connection-error? result)
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
                            ([v ch] ::timeout))
                      false
                      (do
                        (log/error (str "Couldn't send a message: " (pr-str req)))
                        (retry-send conn (inc retry) ::connect-failed))

                      ::timeout
                      (do
                        (log/error (str "Timeout: Couldn't send a message: " (pr-str req)))
                        (retry-send conn (inc retry) ::connect-timeout))

                      {:type :success :result conn})
                    (catch ConnectException ex
                      (retry-send nil (inc retry) ex)))]
          (case type
            :recur
            (recur (:retry-count c) (:result c))

            :success
            (:result c)))))))


(defn send
  ([ch address port req timeout-ms send-retry-count send-retry-interval-ms]
    (log/trace "send-recv-timeout:" timeout-ms)
    (let [{:keys [read-ch]} (try-send address port req timeout-ms send-retry-count send-retry-interval-ms)
          result-ch (or ch (chan))]
      (go
        (try
          (let [msg (alt!
                      [read-ch]
                      ([v ch] @v)

                      [(async/timeout timeout-ms)]
                      ([v ch] ::timeout))]
            (case msg
              ::timeout
              (do
                (log/error (str "Timeout: Couldn't receive a response for a req: " (pr-str req)))
                (>! result-ch (boxed ::timeout)))

              nil
              (do
                (log/error (str "Drained: Peer closed: req: " (pr-str req)))
                (>! result-ch nil))

              (>! result-ch (boxed msg))))
          (catch Throwable th
            (log/error th "send error!")
            (>! result-ch (boxed th)))))
      result-ch))

  ([address port req timeout-ms send-retry-count send-retry-interval-ms]
    (send nil address port req timeout-ms send-retry-count send-retry-interval-ms))

  ([ch address port req timeout-ms]
    (send ch address port req timeout-ms 0 0))

  ([address port req timeout-ms]
    (send nil address port req timeout-ms)))

