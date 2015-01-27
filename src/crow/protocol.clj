(ns crow.protocol
  (:require [msgpack.core :refer [defext pack unpack] :as msgpack]
            [msgpack.io :refer [ubytes byte-stream next-int next-byte int->bytes byte->bytes] :as io]
            [clj-time.core :refer [year month day hour minute sec date-time]])
  (:import [java.net InetAddress]
           [java.io EOFException]
           [java.util Arrays]
           [msgpack.core Extension]))

(def ^:const separator 0x00)
(def ^:const type-join-request    1)
(def ^:const type-registration    2)
(def ^:const type-heart-beat      3)
(def ^:const type-lease           4)
(def ^:const type-lease-expired   5)
(def ^:const type-invalid-message 6)
(def ^:const type-remote-call     7)
(def ^:const type-call-result     8)
(def ^:const type-protocol-error  9)
(def ^:const type-call-exception 10)

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

(defmethod restore-ext :default [ext] ext)


(defrecord JoinRequest [^String service-id])

(defext JoinRequest type-join-request [ent]
  (pack (:service-id ent)))

(defmethod restore-ext type-join-request
  [ext]
  (let [data ^bytes (:data ext)
        service-id (unpack data)]
    (JoinRequest. service-id)))

(defn join-request
  [^String service-id]
  (JoinRequest. service-id))



(defrecord Registration [^String service-id expire-at])

(defext Registration type-registration [ent]
  (pack [(:service-id ent) (date->bytes (:expire-at ent))]))

(defmethod restore-ext type-registration
  [ext]
  (let [data ^bytes (:data ext)
        [service-id date-bytes] (unpack data)
        expire-at (bytes->date date-bytes)]
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



(defrecord RemoteCall [target-ns fn-name args])

(defext RemoteCall type-remote-call [ent]
  (pack [(:target-ns ent) (:fn-name ent) (:args ent)]))

(defmethod restore-ext type-remote-call
  [ext]
  (let [data ^bytes (:data ext)
        entries (unpack data)]
    (apply ->RemoteCall entries)))

(defn remote-call
  [target-ns fn-name args]
  (RemoteCall. target-ns fn-name args))


(defrecord CallResult [obj])

(defext CallResult type-call-result [ent]
  (pack (:obj ent)))

(defmethod restore-ext type-call-result
  [ext]
  (let [data ^bytes (:data ext)
        obj (unpack data)]
    (CallResult. obj)))

(defn call-result
  [obj]
  (CallResult. obj))


(defrecord CallException [stack-trace-str])

(defext CallException type-call-exception [ent]
  (pack (:stack-trace-str ent)))

(defmethod restore-ext type-call-exception
  [ext]
  (let [data ^bytes (:data ext)
        stack-trace-str (unpack data)]
    (CallException. stack-trace-str)))

(defn call-exception
  [stack-trace-str]
  (CallException. stack-trace-str))



(defrecord ProtocolError [error-code message])

(defext ProtocolError type-protocol-error [ent]
  (pack [(:error-code ent) (:message ent)]))

(defmethod restore-ext type-protocol-error
  [ext]
  (let [data ^bytes (:data ext)
        [error-code message] (unpack data)]
    (ProtocolError. error-code message)))

(defn protocol-error
  [error-code message]
  (ProtocolError. error-code message))




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

(defn remote-call?
  [msg]
  (instance? RemoteCall msg))




