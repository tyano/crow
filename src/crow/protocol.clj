(ns crow.protocol
  (:require [msgpack.core :refer [defext pack unpack] :as msgpack]
            [msgpack.io :refer [ubytes byte-stream next-int next-byte int->bytes byte->bytes] :as io]
            [clj-time.core :refer [year month day hour minute sec date-time]])
  (:import [java.net InetAddress]
           [java.io EOFException]
           [java.util Arrays]
           [msgpack.core Extension]))

(def ^:const separator 0x00)
(def ^:const type-join-request  1)
(def ^:const type-registration  2)
(def ^:const type-heart-beat    3)
(def ^:const type-lease         4)
(def ^:const type-lease-expired 5)
(def ^:const type-invalid-message 6)

(defn- find-separator
  "0x00を探してその位置を返します。先頭は0です。"
  [^bytes data]
  (let [stream (byte-stream data)]
    (try
      (loop [b   (next-byte stream)
             idx 0]
        (if (= b separator)
          idx
          (recur (next-byte stream) (inc idx))))
      (catch EOFException ex
        nil))))

(defn- split-bytes
  "0x00を探してそこでバイト列を２分割して、二つのバイト列のベクタを返します。"
  [^bytes data]
  (if-let [idx (find-separator data)]
    [(Arrays/copyOfRange ^bytes data (int 0) (int idx)) (Arrays/copyOfRange ^bytes data (int (inc idx)) (int (count data)))]
    [data nil]))

(defn date->bytes
  "clj-timeのDateTimeオブジェクトを、年（西暦）、月、日、時、分、秒に分解し、
  それぞれint,byte,byte,byte,byte,byteとしてバイト配列化して結合したバイト列を作ります。"
  [t]
  (let [year-bytes   (int->bytes  (year t))
        month-bytes  (byte->bytes (month t))
        day-bytes    (byte->bytes (day t))
        hour-bytes   (byte->bytes (hour t))
        minute-bytes (byte->bytes (minute t))
        second-bytes (byte->bytes (sec t))]
    (ubytes [year-bytes month-bytes day-bytes hour-bytes minute-bytes second-bytes])))

(defn bytes->date
  "date->bytesによってバイト配列化されたclj-time DateTimeオブジェクトを復元します。"
  [data]
  (let [stream      (byte-stream data)
        year-int    (next-int stream)
        month-byte  (next-byte stream)
        day-byte    (next-byte stream)
        hour-byte   (next-byte stream)
        minute-byte (next-byte stream)
        second-byte (next-byte stream)]
    (date-time year-int month-byte day-byte hour-byte minute-byte second-byte)))


(defmulti restore-ext
  "Extensionオブジェクトから元のrecordを復元します。typeを使ってどのrecordであるかを特定します。
  新しいExtensionを追加したら、そのtypeを使って、defmethodも追加しなければなりません。"
  (fn [ext] (:type ext)))


(defrecord JoinRequest [^String ip-address])

(defext JoinRequest type-join-request [ent]
  (-> (InetAddress/getByName (:ip-address ent))
      (.getAddress)))

(defmethod restore-ext type-join-request
  [ext]
  (let [data    ^bytes (:data ext)
        address ^InetAddress (InetAddress/getByAddress data)]
    (JoinRequest. (.getHostAddress address))))

(defn join-request
  [^String ip-address]
  (JoinRequest. ip-address))



(defrecord Registration [^String service-id expire-at])

(defext Registration type-registration [ent]
  (ubytes (concat (pack (:service-id ent)) [separator] (pack (date->bytes (:expire-at ent))))))

(defmethod restore-ext type-registration
  [ext]
  (let [data ^bytes (:data ext)
        [id-data ms-data] (split-bytes data)
        service-id  (unpack id-data)
        expire-at   (-> (unpack ms-data) (bytes->date))]
    (Registration. service-id expire-at)))

(defn registration [service-id expire-at] (Registration. service-id expire-at))


(defrecord HeartBeat [^String service-id])

(defext HeartBeat type-heart-beat [ent]
  (pack (:service-id ent)))

(defmethod restore-ext type-heart-beat
  [ext]
  (let [data ^bytes (:data ext)
        service-id (unpack data)]
    (HeartBeat. service-id)))

(defn heart-beat [service-id] (HeartBeat. service-id))


(defrecord Lease [expire-at])

(defext Lease type-lease [ent]
  (pack (date->bytes (:expire-at ent))))

(defmethod restore-ext type-lease
  [ext]
  (let [data ^bytes (:data ext)
        expire-at   (-> (unpack data) (bytes->date))]
    (Lease. expire-at)))

(defn lease [expire-at] (Lease. expire-at))



(defrecord LeaseExpired [service-id])

(defext LeaseExpired type-lease-expired [ent]
  (pack (:service-id ent)))

(defmethod restore-ext type-lease-expired
  [ext]
  (let [data ^bytes (:data ext)
        service-id (unpack data)]
    (LeaseExpired. service-id)))

(defn lease-expired [service-id] (LeaseExpired. service-id))


(defrecord InvalidMessage [msg])

(defext InvalidMessage type-invalid-message [ent]
  (pack (:msg ent)))

(defmethod restore-ext type-invalid-message
  [ext]
  (let [data ^bytes (:data ext)]
    (InvalidMessage. (unpack data))))

(defn invalid-message [msg] (InvalidMessage. msg))





(defn unpack-message
  [data]
  (let [msg (unpack data)]
    (if (instance? Extension msg)
      (restore-ext msg)
      msg)))

(defn join-request?
  [msg]
  (instance? JoinRequest msg))

(defn registration?
  [msg]
  (instance? Registration msg))

(defn heart-beat?
  [msg]
  (instance? HeartBeat msg))

(defn lease?
  [msg]
  (instance? Lease msg))

(defn lease-expired?
  [msg]
  (instance? LeaseExpired msg))

(defn invalid-message?
  [msg]
  (instance? InvalidMessage msg))



