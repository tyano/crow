(ns crow.protocol
  (:refer-clojure :exclude [second])
  (:require [msgpack.core :refer [defext pack unpack] :as msgpack]
            [msgpack.io :refer [ubytes byte-stream next-int next-byte int->bytes byte->bytes] :as io]
            [clj-time.core :refer [year month day hour minute second date-time]]
            [clojure.edn :as edn]
            [manifold.stream :refer [put! take!] :as s]
            [manifold.deferred :refer [let-flow chain]]
            [byte-streams :refer [to-byte-array]])
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
(def ^:const type-discovery      11)
(def ^:const type-service-found  12)
(def ^:const type-service-not-found  13)

(defn date->bytes
  "clj-timeのDateTimeオブジェクトを、年（西暦）、月、日、時、分、秒に分解し、
  それぞれint,byte,byte,byte,byte,byteとしてバイト配列化して結合したバイト列を作ります。"
  [t]
  (let [year-bytes   (int->bytes  (year t))
        month-bytes  (byte->bytes (month t))
        day-bytes    (byte->bytes (day t))
        hour-bytes   (byte->bytes (hour t))
        minute-bytes (byte->bytes (minute t))
        second-bytes (byte->bytes (second t))]
    (ubytes (concat year-bytes month-bytes day-bytes hour-bytes minute-bytes second-bytes))))

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


(defrecord JoinRequest [ip-address service-id service-name attributes])

(defext JoinRequest type-join-request [ent]
  (pack [(:ip-address ent) (:service-id ent) (:service-name ent) (:attributes ent)]))

(defmethod restore-ext type-join-request
  [ext]
  (let [data ^bytes (:data ext)
        [ip-address service-id service-name attributes] (unpack data)]
    (JoinRequest. ip-address service-id service-name attributes)))

(defn join-request
  [ip-address service-id service-name attributes]
  (JoinRequest. ip-address service-id service-name attributes))



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
  (pack [(:target-ns ent) (:fn-name ent) (pr-str (:args ent))]))

(defmethod restore-ext type-remote-call
  [ext]
  (let [data ^bytes (:data ext)
        [target-ns fn-name args-edn] (unpack data)
        args (edn/read-string args-edn)]
    (RemoteCall. target-ns fn-name args)))

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


(defrecord Discovery [service-name attributes])

(defext Discovery type-discovery [ent]
  (pack [(:service-name ent) (pr-str (:attributes ent))]))

(defmethod restore-ext type-discovery
  [ext]
  (let [data ^bytes (:data ext)
        [service-name attr-edn] (unpack data)
        attributes (edn/read-string attr-edn)]
    (Discovery. service-name attributes)))

(defn discovery
  [service-name attributes]
  (Discovery. service-name attributes))


;;; services is a coll of maps with keys:
;;; :ip-address :service-name :attributes
(defrecord ServiceFound [services])

(defext ServiceFound type-service-found [ent]
  (pack (vec (mapcat #(vector (:ip-address %) (:service-name %) (pr-str (:attributes %))) (:services ent)))))

(defmethod restore-ext type-service-found
  [ext]
  (let [data ^bytes (:data ext)
        service-data-coll (partition 3 (unpack data))
        services (for [[address service-name attr-edn] service-data-coll]
                    {:ip-address address
                     :service-name service-name
                     :attributes (edn/read-string attr-edn)})]
    (ServiceFound. services)))

(defn service-found
  [services]
  (ServiceFound. services))



(defrecord ServiceNotFound [service-name attributes])

(defext ServiceNotFound type-service-not-found [ent]
  (pack [(:service-name ent) (pr-str (:attributes ent))]))

(defmethod restore-ext type-service-not-found
  [ext]
  (let [data ^bytes (:data ext)
        [service-name attr-edn] (unpack data)
        attributes (edn/read-string attr-edn)]
    (ServiceNotFound. service-name attributes)))

(defn service-not-found
  [service-name attributes]
  (ServiceNotFound. service-name attributes))


(defn ping [] 2r01)
(defn ack  [] 2r10)

(defn unpack-message
  [data]
  (let [msg (unpack data)]
    (if (instance? Extension msg)
      (restore-ext msg)
      msg)))

(defn send!
  "convert object into bytes and send the bytes into stream.
  returns a differed object holding true or false."
  [stream obj]
  (put! stream (pack obj)))

(defn read-message
  "convert byte-buffer to byte-array and unpack the byte-array to a message format."
  [data]
  (-> data to-byte-array unpack-message))

(defn recv!
  "read from stream and unpack the received bytes.
  returns a differed object holding an unpacked object."
  [stream]
  (chain (take! stream)
         read-message))

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

(defn discovery?
  [msg]
  (instance? Discovery msg))

(defn service-found?
  [msg]
  (instance? ServiceFound msg))

(defn service-not-found?
  [msg]
  (instance? ServiceNotFound msg))

(defn ping? [msg] (= msg 2r01))
(defn ack? [msg] (= msg 2r10))

