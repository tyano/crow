(ns crow.request
  (:refer-clojure :exclude [send])
  (:require [msgpack.core :as msgpack]
            [msgpack.core :refer [pack unpack refine-ext] :as msgpack]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [crow.logging :refer [trace-pr]]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.core.async :refer [<! >! <!! >!! go go-loop alt! alts! thread chan] :as async]
            [async-connect.client :refer [connect] :as async-connect]
            [async-connect.box :refer [boxed] :as box])
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
          ([v# ~'_] @v#)

          [(async/timeout timeout-ms#)]
          ([~'_ ~'_] ::timeout))

        @(<! ch#))))

(defn- try-send
  [connection-factory address port req timeout-ms send-retry-count send-retry-interval-ms]
  (letfn [(retry-send
            [conn retry-count result]
            (when conn
              (async-connect/close conn)
              (log/trace "channel closed."))
            (Thread/sleep (* send-retry-interval-ms retry-count))
            (when (<= retry-count send-retry-count)
              (log/info (format "retry! -- times: %d/%d" retry-count send-retry-count)))
            {:type :recur :retry-count retry-count :result result})]

    (let [result-ch (chan)]
      (go-loop [retry 0 result nil]
        (if (> retry send-retry-count)
          (cond
            (connection-error? result)
            (throw+ {:type ::connection-error, :kind (:type result)})

            (instance? Throwable result)
            (throw result)

            :else
            (throw+ {:type ::retry-count-over, :last-result result}))

          (let [{:keys [:client/write-ch] :as conn} (client connection-factory address port)
                {:keys [type] :as c}
                    (try
                      (case (write-with-timeout write-ch {:message req :flush? true} timeout-ms)
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
              (>! result-ch (:result c))))))
      result-ch)))


(defn send
  ([ch connection-factory address port req timeout-ms send-retry-count send-retry-interval-ms]
    (log/trace "send-recv-timeout:" timeout-ms)
    (let [result-ch (or ch (chan))]

      (go
        (let [{:keys [:client/read-ch] :as conn}
                  (<! (try-send connection-factory address port req timeout-ms send-retry-count send-retry-interval-ms))]
          (go
            (let [result (try
                            (let [msg (read-with-timeout read-ch timeout-ms)]
                              (case msg
                                ::timeout
                                (do
                                  (log/error (str "Timeout: Couldn't receive a response for a req: " (pr-str req)))
                                  ::timeout)

                                nil
                                (do
                                  (log/error (str "Drained: Peer closed: req: " (pr-str req)))
                                  nil)

                                msg))

                            (catch Throwable th
                              (log/error th "send error!")
                              th)

                            (finally
                              (async-connect/close conn)))]
              (>! result-ch (boxed result))))))

      result-ch))

  ([connection-factory address port req timeout-ms send-retry-count send-retry-interval-ms]
    (send nil connection-factory address port req timeout-ms send-retry-count send-retry-interval-ms))

  ([ch connection-factory address port req timeout-ms]
    (send ch connection-factory address port req timeout-ms 0 0))

  ([connection-factory address port req timeout-ms]
    (send nil connection-factory address port req timeout-ms)))

