(ns crow.request
  (:refer-clojure :exclude [send])
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [clojure.core.async :refer [<! >! <!! >!! go go-loop alt! alts! thread chan close!] :as async]
            [msgpack.core :as msgpack]
            [msgpack.core :refer [pack unpack refine-ext] :as msgpack]
            [crow.logging :refer [trace-pr]]
            [crow.protocol :refer [call-result? sequential-item-start? sequential-item-end? sequential-item?]]
            [async-connect.client :refer [connect] :as async-connect]
            [async-connect.box :refer [boxed] :as box]
            [async-connect.spec :as async-spec])
  (:import [com.shelf.messagepack MessagePackFrameDecoder]
           [msgpack.core Ext]
           [java.net ConnectException]
           [java.util.concurrent TimeoutException]
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

(def connection-errors #{::connect-failed ::connect-timeout})
(defn connection-error? [v] (boolean (when v (connection-errors v))))

(defn read-message
  "unpack a byte-array to a message format."
  [data]
  (when data (unpack-message data)))

(def packer (map #(update % :message pack)))
(def unpacker (map #(box/update % read-message)))

(defn- initialize-channel
  [netty-ch config]
  (.. netty-ch
    (pipeline)
    (addLast "messagepack-framedecoder" (frame-decorder))
    (addLast "bytes-decoder" (ByteArrayDecoder.))
    (addLast "bytes-encoder" (ByteArrayEncoder.))))

(def bootstrap
  (delay
   (async-connect/make-bootstrap
    {::async-connect/channel-initializer initialize-channel})))

(defn client
  [factory address port]
  (let [read-ch  (chan 500 unpacker)
        write-ch (chan 500 packer)]
    (async-connect/connect factory address port read-ch write-ch)))

(defmacro write-with-timeout
  [write-ch data timeout-ms]
  `(let [ch# ~write-ch
         data# ~data
         timeout-ms# ~timeout-ms]
    (if timeout-ms#
      (alt!
        [[ch# data#]]
        ([v# ~'_] v#)

        [(async/timeout timeout-ms#)]
        ([~'_ ~'_] ::timeout))

      (>! ch# data#))))

(defmacro read-with-timeout
  [read-ch timeout-ms]
  `(let [ch# ~read-ch
         timeout-ms# ~timeout-ms]
      (if timeout-ms#
        (alt!
          [ch#]
          ([v# ~'_] (when v# @v#))

          [(async/timeout timeout-ms#)]
          ([~'_ ~'_] ::timeout))

        (when-let [v# (<! ch#)]
          @v#))))



(s/def ::channel (s/nilable ::async-spec/async-channel))
(s/def ::connection-factory ::async-connect/connection-factory)
(s/def ::address string?)
(s/def ::port pos-int?)
(s/def ::req any?)
(s/def ::timeout-ms (s/nilable pos-int?))

(s/def :crow/request
  (s/keys
    :req [::connection-factory
          ::address
          ::port
          ::data]
    :opt [::channel
          ::timeout-ms]))

(s/fdef try-send
  :args (s/cat :request :crow/request)
  :ret  ::async-spec/async-channel)

(defn- try-send
  [{::keys [connection-factory
            address
            port
            data
            timeout-ms] :as send-data}]

  (let [result-ch (chan)]
    (go
      (try
        (let [{::async-connect/keys [write-ch] :as conn} (client connection-factory address port)
              result (case (write-with-timeout write-ch {:message data :flush? true} timeout-ms)
                       false
                       (throw (ConnectException. (str "Couldn't send a message: " (pr-str data))))

                       ::timeout
                       (throw (TimeoutException. (str "Timeout: Couldn't send a message: " (pr-str data))))

                       conn)]

          (>! result-ch (boxed result))
          nil)

        (catch Throwable th
          (>! result-ch (boxed th))
          nil)))

    result-ch))


(s/fdef send
  :args (s/cat :request :crow/request)
  :ret  ::async-spec/async-channel)

(defn send
  [{::keys [channel data timeout-ms]
    :as send-data}]

  (log/trace "send-recv-timeout:" timeout-ms)

  (let [result-ch (or channel (chan))]

    (go
      (try
        (if-let [{::async-connect/keys [read-ch] :as conn} @(<! (try-send send-data))]
          (try
            (loop [msg (read-with-timeout read-ch timeout-ms)]
              (cond
                (= msg ::timeout)
                (do
                  (log/error (str "Timeout: Couldn't receive a response for a data: " (pr-str data)))
                  (async-connect/close conn true)
                  (>! result-ch (boxed ::timeout))
                  (close! result-ch))

                (nil? msg)
                (do
                  (log/error (str "Drained: Peer closed: data: " (pr-str data)))
                  (>! result-ch (boxed nil))
                  (close! result-ch))

                (or (sequential-item-start? msg)
                    (sequential-item? msg))
                (do
                  (>! result-ch (boxed msg))
                  (recur (read-with-timeout read-ch timeout-ms)))

                (sequential-item-end? msg)
                (do
                  (>! result-ch (boxed msg))
                  (close! result-ch))

                (call-result? msg)
                (do
                  (>! result-ch (boxed msg))
                  (close! result-ch))

                :else
                (do
                  (>! result-ch (boxed msg))
                  (close! result-ch))))

            (finally
              (async-connect/close conn)))

          (close! result-ch))

        (catch Throwable th
          (>! result-ch (boxed th))
          (close! result-ch))))

    result-ch))

