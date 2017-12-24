(ns crow.request
  (:refer-clojure :exclude [send])
  (:require [msgpack.core :as msgpack]
            [msgpack.core :refer [pack unpack refine-ext] :as msgpack]
            [clojure.tools.logging :as log]
            [crow.logging :refer [trace-pr]]
            [crow.logging :refer [trace-pr]]
            [slingshot.slingshot :refer [try+ throw+]]
            [clojure.core.async :refer [<! >! <!! >!! go go-loop alt! alts! thread chan close!] :as async]
            [async-connect.client :refer [connect] :as async-connect]
            [async-connect.box :refer [boxed] :as box]
            [async-connect.spec :as async-spec]
            [clojure.spec.alpha :as s])
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
    {::async-connect/channel-initializer initialize-channel}))

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
(s/def ::send-retry-count (s/nilable #(or (zero? %) (pos-int? %))))
(s/def ::send-retry-interval-ms (s/nilable #(or (zero? %) (pos-int? %))))

(s/def :crow/request
  (s/keys
    :req [::connection-factory
          ::address
          ::port
          ::data]
    :opt [::channel
          ::timeout-ms
          ::send-retry-count
          ::send-retry-interval-ms]))

(s/fdef try-send
  :args (s/cat :request :crow/request)
  :ret  ::async-spec/async-channel)

(defn- try-send
  [{:keys [::connection-factory
           ::address
           ::port
           ::data
           ::timeout-ms
           ::send-retry-count
           ::send-retry-interval-ms]
    :or {send-retry-count 0
         send-retry-interval-ms 0}
    :as send-data}]

  (letfn [(retry-send
            [conn retry-count result]
            (when conn
              (async-connect/close conn true)
              (log/debug "channel closed."))
            (Thread/sleep (* send-retry-interval-ms retry-count))
            (when (<= retry-count send-retry-count)
              (log/info (format "retry! -- times: %d/%d" retry-count send-retry-count)))
            {:type :recur :retry-count retry-count :result result})]

    (let [result-ch (chan)]
      (go-loop [retry 0 result nil]
        (let [recur-result
                (try
                  (if (> retry send-retry-count)
                    (cond
                      (connection-error? result)
                      (throw+ {:type ::connection-error, :kind (:type result)})

                      (instance? Throwable result)
                      (throw result)

                      :else
                      (throw+ {:type ::retry-count-over, :last-result result}))

                    (let [{::async-connect/keys [write-ch] :as conn} (client connection-factory address port)
                          {:keys [type] :as c}
                              (try
                                (case (write-with-timeout write-ch {:message data :flush? true} timeout-ms)
                                  false
                                  (do
                                    (log/error (str "Couldn't send a message: " (pr-str data)))
                                    (retry-send conn (inc retry) ::connect-failed))

                                  ::timeout
                                  (do
                                    (log/error (str "Timeout: Couldn't send a message: " (pr-str data)))
                                    (retry-send conn (inc retry) ::connect-timeout))

                                  {:type :success :result conn})

                                (catch ConnectException ex
                                  (log/error ex "Can not send data. an exception occurred.")
                                  (retry-send nil (inc retry) ex)))]
                      (case type
                        :recur
                        {:type :recur
                         :retry-count (:retry-count c)
                         :result (:result c)}

                        :success
                        (>! result-ch (boxed (:result c))))))

                  (catch Throwable th
                    (>! result-ch (boxed th))))]

          (when-let [{:keys [retry-count result]} recur-result]
            (recur retry-count result))))

      result-ch)))



(s/fdef send
  :args (s/cat :request :crow/request)
  :ret  ::async-spec/async-channel)

(defn send
  [{:keys [::channel ::data ::timeout-ms]
    :as send-data}]

  (log/trace "send-recv-timeout:" timeout-ms)
  (let [result-ch (or channel (chan))]

    (go
      (try
        (if-let [{::async-connect/keys [read-ch] :as conn} @(<! (try-send send-data))]
          (let [result (try
                          (let [msg (read-with-timeout read-ch timeout-ms)]
                            (case msg
                              ::timeout
                              (do
                                (log/error (str "Timeout: Couldn't receive a response for a data: " (pr-str data)))
                                (async-connect/close conn true)
                                ::timeout)

                              nil
                              (do
                                (log/error (str "Drained: Peer closed: data: " (pr-str data)))
                                nil)

                              msg))

                          (catch Throwable th
                            (log/error th "send error!")
                            th)

                          (finally
                            (async-connect/close conn)))]
            (>! result-ch (boxed result)))

          (close! result-ch))
        (catch Throwable th
          (>! result-ch (boxed th)))))
    result-ch))

