(ns crow.protocol
  (:refer-clojure :exclude [second])
  (:require [msgpack.core :refer [pack unpack unpack-stream] :as msgpack]
            [msgpack.macros :refer [extend-msgpack]]
            [clj-time.core :refer [year month day hour minute second date-time]]
            [clojure.edn :as edn]
            [crow.marshaller :refer [marshal unmarshal ->EdnObjectMarshaller]]
            [clojure.tools.logging :as log])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream
                    DataOutputStream DataInputStream]))

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
(def ^:const type-call-result-end 14)

(def ^:dynamic *object-marshaller* (->EdnObjectMarshaller))

(defn install-default-marshaller
  [marshaller]
  (alter-var-root #'*object-marshaller* (fn [_] marshaller))
  marshaller)

(defn date->bytes
  "devide an DateTime object of clj-time into year, month, day, hour, minute and seconds,
  convert each element intto byte-arrays, and then combine them into one byte-array."
  [t]
  (with-open [bytearray (ByteArrayOutputStream.)
              s (DataOutputStream. bytearray)]
    (.writeInt s (year t))
    (.writeByte s (month t))
    (.writeByte s (day t))
    (.writeByte s (hour t))
    (.writeByte s (minute t))
    (.writeByte s (second t))
    (.flush s)
    (.toByteArray bytearray)))

(defn bytes->date
  "restore a DateTime object from an byte-array created by date->bytes."
  [data]
  (with-open [s (DataInputStream. (ByteArrayInputStream. data))]
    (let [year-int    (.readInt s)
          month-byte  (.readByte s)
          day-byte    (.readByte s)
          hour-byte   (.readByte s)
          minute-byte (.readByte s)
          second-byte (.readByte s)]
      (date-time year-int month-byte day-byte hour-byte minute-byte second-byte))))

(defn combine-bytes
  [& bytes-coll]
  (when (seq bytes-coll)
    (with-open [bytearray (ByteArrayOutputStream.)
                s (DataOutputStream. bytearray)]
      (doseq [barray bytes-coll]
        (.write s ^bytes barray (int 0) (int (count barray))))
      (.flush s)
      (.toByteArray bytearray))))

(defn pack-and-combine
  [& packable-coll]
  (when (seq packable-coll)
    (apply combine-bytes (map pack packable-coll))))

(defn unpack-n
  [n data]
  (with-open [s (DataInputStream. (ByteArrayInputStream. (byte-array data)))]
    (for [_ (range n)] (unpack-stream s))))


(defrecord JoinRequest [address port service-id service-name attributes])

(extend-msgpack JoinRequest type-join-request
  [ent]
  (pack-and-combine (:address ent) (:port ent) (:service-id ent) (:service-name ent) (pr-str (:attributes ent)))
  [data]
  (let [[address port service-id service-name attributes-edn] (unpack-n 5 data)]
    (try
      (let [attributes (edn/read-string attributes-edn)]
        (JoinRequest. address port service-id service-name attributes))
      (catch Throwable th
        (log/error th (str "address: " address ", port: " port ", service-id: " service-id ", service-name: " service-name ", attributes: " attributes-edn))
        (throw th)))))

(defn join-request
  [address port service-id service-name attributes]
  (JoinRequest. address port service-id service-name attributes))



(defrecord Registration [^String service-id expire-at])

(extend-msgpack Registration type-registration
  [ent]
  (pack-and-combine (:service-id ent) (date->bytes (:expire-at ent)))
  [data]
  (let [[service-id date-bytes] (unpack-n 2 data)
        expire-at (bytes->date date-bytes)]
    (Registration. service-id expire-at)))

(defn registration [service-id expire-at] (Registration. service-id expire-at))


(defrecord HeartBeat [^String service-id])

(extend-msgpack HeartBeat type-heart-beat
  [ent]
  (pack (:service-id ent))
  [data]
  (HeartBeat. (unpack data)))

(defn heart-beat [service-id] (HeartBeat. service-id))


(defrecord Lease [expire-at])

(extend-msgpack Lease type-lease
  [ent]
  (pack (date->bytes (:expire-at ent)))
  [data]
  (let [expire-at   (-> (unpack data) (bytes->date))]
    (Lease. expire-at)))

(defn lease [expire-at] (Lease. expire-at))



(defrecord LeaseExpired [service-id])

(extend-msgpack LeaseExpired type-lease-expired
  [ent]
  (pack (:service-id ent))
  [data]
  (LeaseExpired. (unpack data)))

(defn lease-expired [service-id] (LeaseExpired. service-id))


(defrecord InvalidMessage [msg])

(extend-msgpack InvalidMessage type-invalid-message
  [ent]
  (pack (:msg ent))
  [data]
  (InvalidMessage. (unpack data)))

(defn invalid-message [msg] (InvalidMessage. msg))



(defrecord RemoteCall [target-ns fn-name args])

(extend-msgpack RemoteCall type-remote-call
  [ent]
  (pack-and-combine (:target-ns ent) (:fn-name ent) (map (partial marshal *object-marshaller*) (:args ent)))
  [data]
  (let [[target-ns fn-name args] (unpack-n 3 data)
        args (map (partial unmarshal *object-marshaller*) args)]
    (RemoteCall. target-ns fn-name args)))

(defn remote-call
  [target-ns fn-name args]
  (RemoteCall. target-ns fn-name args))


(defrecord CallResult [obj])

(extend-msgpack CallResult type-call-result
  [ent]
  (pack (marshal *object-marshaller* (:obj ent)))
  [data]
  (let [obj-marshalled (unpack data)
        obj (unmarshal *object-marshaller* obj-marshalled)]
    (CallResult. obj)))

(defn call-result
  [obj]
  (CallResult. obj))


(defrecord CallResultEnd [] type-call-result-end
           [ent]
           (byte-array 0)
           [data]
           (CallResultEnd.))

(defn call-result-end
  []
  (CallResultEnd.))



(defrecord CallException [type stack-trace])

(extend-msgpack CallException type-call-exception
  [ent]
  (pack-and-combine (:type ent) (:stack-trace ent))
  [data]
  (let [[type stack-trace] (unpack-n 2 data)]
    (CallException. type stack-trace)))

(defn call-exception
  [type stack-trace]
  (CallException. (name type) stack-trace))



(defrecord ProtocolError [error-code message])

(extend-msgpack ProtocolError type-protocol-error
  [ent]
  (pack-and-combine (:error-code ent) (:message ent))
  [data]
  (let [[error-code message] (unpack-n 2 data)]
    (ProtocolError. error-code message)))

(defn protocol-error
  [error-code message]
  (ProtocolError. error-code message))


(defrecord Discovery [service-name attributes])

(extend-msgpack Discovery type-discovery
  [ent]
  (pack-and-combine (:service-name ent) (pr-str (:attributes ent)))
  [data]
  (let [[service-name attr-edn] (unpack-n 2 data)]
    (try
      (let [attributes (edn/read-string attr-edn)]
        (Discovery. service-name attributes))
      (catch Throwable th
        (log/error th (str "service-name: " service-name ", attributes: " attr-edn))
        (throw th)))))

(defn discovery
  [service-name attributes]
  (Discovery. service-name attributes))


;;; services is a coll of maps with keys:
;;; :address :port :service-name :attributes
(defrecord ServiceFound [services])

(extend-msgpack ServiceFound type-service-found
  [ent]
  (pack (vec (mapcat #(vector (:address %) (:port %) (:service-name %) (pr-str (:attributes %))) (:services ent))))
  [data]
  (let [service-data-coll (partition 4 (unpack data))
        services (for [[address port service-name attr-edn] service-data-coll]
                    (try
                      (let [attributes (edn/read-string attr-edn)]
                        {:address address
                         :port port
                         :service-name service-name
                         :attributes attributes})
                      (catch Throwable th
                        (log/error th (str "address: " address ", port: " port ", service-name: " service-name ", attributes: " attr-edn))
                        (throw th))))]
    (ServiceFound. services)))

(defn service-found
  [services]
  (ServiceFound. services))


(defrecord ServiceNotFound [service-name attributes])

(extend-msgpack ServiceNotFound type-service-not-found
  [ent]
  (pack-and-combine (:service-name ent) (pr-str (:attributes ent)))
  [data]
  (let [[service-name attr-edn] (unpack-n 2 data)]
    (try
      (let [attributes (edn/read-string attr-edn)]
        (ServiceNotFound. service-name attributes))
      (catch Throwable th
        (log/error th (str "service-name: " service-name ", attributes: " attr-edn))
        (throw th)))))

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

(defn call-result-end?
  [msg]
  (instance? CallResultEnd msg))

(defn discovery?
  [msg]
  (instance? Discovery msg))

(defn service-found?
  [msg]
  (instance? ServiceFound msg))

(defn service-not-found?
  [msg]
  (instance? ServiceNotFound msg))

(defn protocol-error?
  [msg]
  (instance? ProtocolError msg))

(defn ping? [msg] (= msg 2r01))
(defn ack? [msg] (= msg 2r10))

