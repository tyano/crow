(ns crow.protocol
  (:refer-clojure :exclude [second])
  (:require [msgpack.core :refer [pack unpack unpack-stream] :as msgpack]
            [msgpack.macros :refer [extend-msgpack]]
            [clj-time.core :refer [year month day hour minute second date-time]]
            [clojure.edn :as edn]
            [clojure.tools.logging :refer [debug]]
            [crow.marshaller :refer [marshal unmarshal] :as marshal]
            [crow.marshaller.edn :refer [->EdnObjectMarshaller]]
            [crow.marshaller.compact :refer [->CompactObjectMarshaller]]
            [clojure.tools.logging :as log])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream
                    DataOutputStream DataInputStream]))

(def ^:const separator 0x00)
(def ^:const type-join-request    11)
(def ^:const type-registration    12)
(def ^:const type-heart-beat      13)
(def ^:const type-lease           14)
(def ^:const type-lease-expired   15)
(def ^:const type-invalid-message 16)
(def ^:const type-remote-call     17)
(def ^:const type-call-result     18)
(def ^:const type-protocol-error  19)
(def ^:const type-call-exception 20)
(def ^:const type-discovery      21)
(def ^:const type-service-found  22)
(def ^:const type-service-not-found  23)

(def ^:const type-sequential-item-start 24)
(def ^:const type-sequential-item 25)
(def ^:const type-sequential-item-end 26)

(def ^:const type-ping 27)
(def ^:const type-ack  28)

(def ^:dynamic *object-marshaller* (->CompactObjectMarshaller))

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



(defrecord Ping [])
(extend-msgpack Ping type-ping
    [ent]
    (byte-array 0)
    [data]
    (Ping.))

(defn ping
  []
  (Ping.))

(defrecord Ack [])
(extend-msgpack Ack type-ack
    [ent]
    (byte-array 0)
    [data]
    (Ack.))

(defn ack
  []
  (Ack.))


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
  (let [{marshalled-args :data} (reduce
                                 (fn [{marshalling-context :context :as rv} arg]
                                   (let [{::marshal/keys [context data]} (marshal *object-marshaller* marshalling-context arg)]
                                     (-> rv
                                         (assoc :context context)
                                         (update :data concat data))))
                                 {:context {}
                                  :data []}
                                 (:args ent))]
    (pack-and-combine (:target-ns ent)
                      (:fn-name ent)
                      marshalled-args))
  [data]
  (let [[target-ns fn-name args]  (unpack-n 3 data)
        {unmarshalled-args :data} (reduce
                                   (fn [{marshalling-context :context :as rv} arg]
                                     (let [{::marshal/keys [context data]} (unmarshal *object-marshaller* marshalling-context arg)]
                                       (-> rv
                                           (assoc :context context)
                                           (update :data concat data))))
                                   {:context {}
                                    :data []}
                                   args)]
    (RemoteCall. target-ns fn-name unmarshalled-args)))

(defn remote-call
  [target-ns fn-name args]
  (RemoteCall. target-ns fn-name args))


(defn- unmarshal-one
  [current-context marshalled-array]
  (let [{:keys [result context]} (reduce
                                  (fn [{:keys [context] :as rv} v]
                                    (let [{data-array ::marshal/data, new-context ::marshal/context} (unmarshal *object-marshaller* context v)]
                                      (-> rv
                                          (assoc :context new-context)
                                          (update :result concat data-array))))
                                  {:context current-context
                                   :result  []}
                                  marshalled-array)]
    {:context context
     :result  (first result)}))

(defrecord CallResult [obj])

(extend-msgpack CallResult type-call-result
  [ent]
  (let [{marshalled-array ::marshal/data} (marshal *object-marshaller* {} (:obj ent))]
    (pack marshalled-array))
  [data]
  (let [marshalled-array (unpack data)
        {:keys [result]} (unmarshal-one {} marshalled-array)]
    (CallResult. result)))

(defn call-result
  [obj]
  (CallResult. obj))


(def ^:private sequential-context ^ThreadLocal (ThreadLocal.))

(def seq-ctx (atom {}))

(defn- set-sequential-context!
  [ctx]
  (.set sequential-context ctx))

(defn- get-sequential-context
  []
  (let [ctx (.get sequential-context)]
    ctx))

(defn- clear-sequential-context!
  []
  (.remove sequential-context))

(defrecord SequentialItemStart [])

(extend-msgpack SequentialItemStart type-sequential-item-start
  [ent]
  (do
    (set-sequential-context! {})
    (byte-array 0))
  [data]
  (do
    (set-sequential-context! {})
    (SequentialItemStart.)))

(defn sequential-item-start
  []
  (SequentialItemStart.))


(defrecord SequentialItem [obj])

(extend-msgpack SequentialItem type-sequential-item
  [ent]
  (let [context (get-sequential-context)
        {marshalled-array ::marshal/data, new-context ::marshal/context} (marshal *object-marshaller* context (:obj ent))]
    (set-sequential-context! new-context)
    (pack marshalled-array))

  [data]
  (let [marshalled-array (unpack data)

        current-context  (or (get-sequential-context) {})

        {:keys [result], new-context :context}
        (unmarshal-one current-context marshalled-array)]
    (set-sequential-context! new-context)
    (SequentialItem. result)))

(defn sequential-item
  [obj]
  (SequentialItem. obj))

(defrecord SequentialItemEnd [])

(extend-msgpack SequentialItemEnd type-sequential-item-end
  [ent]
  (do
    (clear-sequential-context!)
    (byte-array 0))
  [data]
  (do
    (clear-sequential-context!)
    (SequentialItemEnd.)))

(defn sequential-item-end
  []
  (SequentialItemEnd.))



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
  (pack (vec (mapcat #(vector (:address %) (:port %) (:service-id %) (:service-name %) (pr-str (:attributes %))) (:services ent))))
  [data]
  (let [service-data-coll (partition 5 (unpack data))
        services (for [[address port service-id service-name attr-edn] service-data-coll]
                    (try
                      (let [attributes (edn/read-string attr-edn)]
                        {:address address
                         :port port
                         :service-id service-id
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

(defn sequential-item-start?
  [msg]
  (instance? SequentialItemStart msg))

(defn sequential-item?
  [msg]
  (instance? SequentialItem msg))

(defn sequential-item-end?
  [msg]
  (instance? SequentialItemEnd msg))

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

(defn ping?
  [msg]
  (instance? Ping msg))

(defn ack?
  [msg]
  (instance? Ack msg))
