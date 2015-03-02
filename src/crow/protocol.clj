(ns crow.protocol
  (:refer-clojure :exclude [second])
  (:require [msgpack.core :refer [defext pack unpack] :as msgpack]
            [msgpack.io :refer [ubytes byte-stream next-int next-byte int->bytes byte->bytes] :as io]
            [clj-time.core :refer [year month day hour minute second date-time]]
            [clojure.edn :as edn]
            [crow.marshaller :refer [marshal unmarshal ->EdnObjectMarshaller]]))

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

(def ^:dynamic *object-marshaller* (->EdnObjectMarshaller))

(defn install-default-marshaller
  [marshaller]
  (alter-var-root *object-marshaller* (fn [_] marshaller)))

(defn date->bytes
  "devide an DateTime object of clj-time into year, month, day, hour, minute and seconds,
  convert each element intto byte-arrays, and then combine them into one byte-array."
  [t]
  (let [year-bytes   (int->bytes  (year t))
        month-bytes  (byte->bytes (month t))
        day-bytes    (byte->bytes (day t))
        hour-bytes   (byte->bytes (hour t))
        minute-bytes (byte->bytes (minute t))
        second-bytes (byte->bytes (second t))]
    (ubytes (concat year-bytes month-bytes day-bytes hour-bytes minute-bytes second-bytes))))

(defn bytes->date
  "restore a DateTime object from an byte-array created by date->bytes."
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
  "restore an original record from an Extention record.
  This fn uses the type of Extention for identify the original record.
  If you create a new Extention type and a record for the Extention,
  you must create a new defmethod for the type of Extention."
  (fn [ext] (:type ext)))

(defmethod restore-ext :default [ext] ext)


(defrecord JoinRequest [address port service-id service-name attributes])

(defext JoinRequest type-join-request [ent]
  (pack [(:address ent) (:port ent) (:service-id ent) (:service-name ent) (pr-str (:attributes ent))]))

(defmethod restore-ext type-join-request
  [ext]
  (let [data ^bytes (:data ext)
        [address port service-id service-name attributes-edn] (unpack data)]
    (JoinRequest. address port service-id service-name (edn/read-string attributes-edn))))

(defn join-request
  [address port service-id service-name attributes]
  (JoinRequest. address port service-id service-name attributes))



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
  (pack [(:target-ns ent) (:fn-name ent) (map (partial marshal *object-marshaller*) (:args ent))]))

(defmethod restore-ext type-remote-call
  [ext]
  (let [data ^bytes (:data ext)
        [target-ns fn-name args] (unpack data)
        args (map (partial unmarshal *object-marshaller*) args)]
    (RemoteCall. target-ns fn-name args)))

(defn remote-call
  [target-ns fn-name args]
  (RemoteCall. target-ns fn-name args))


(defrecord CallResult [obj])

(defext CallResult type-call-result [ent]
  (pack (marshal *object-marshaller* (:obj ent))))

(defmethod restore-ext type-call-result
  [ext]
  (let [data ^bytes (:data ext)
        obj-marshalled (unpack data)
        obj (unmarshal *object-marshaller* obj-marshalled)]
    (CallResult. obj)))

(defn call-result
  [obj]
  (CallResult. obj))


(defrecord CallException [stack-trace])

(defext CallException type-call-exception [ent]
  (pack ^String (:stack-trace ent)))

(defmethod restore-ext type-call-exception
  [ext]
  (let [data ^bytes (:data ext)
        stack-trace (unpack data)]
    (CallException. stack-trace)))

(defn call-exception
  [stack-trace]
  (CallException. stack-trace))



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
;;; :address :port :service-name :attributes
(defrecord ServiceFound [services])

(defext ServiceFound type-service-found [ent]
  (pack (vec (mapcat #(vector (:address %) (:port %) (:service-name %) (pr-str (:attributes %))) (:services ent)))))

(defmethod restore-ext type-service-found
  [ext]
  (let [data ^bytes (:data ext)
        service-data-coll (partition 4 (unpack data))
        services (for [[address port service-name attr-edn] service-data-coll]
                    {:address address
                     :port port
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

(defn call-exception?
  [msg]
  (instance? CallException msg))

(defn call-result?
  [msg]
  (instance? CallResult msg))

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

