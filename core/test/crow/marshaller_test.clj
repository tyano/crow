(ns crow.marshaller-test
  (:require [crow.marshaller :as marshaller]
            [crow.marshaller.edn :as edn]
            [crow.marshaller.compact :as compact]
            [clojure.test :refer :all]))

(def edn-marshaller (edn/edn-object-marshaller))
(def compact-marshaller (compact/compact-object-marshaller))


(defn- pack-unpack
  [object-marshaller obj]
  (let [{marshalled ::marshaller/data ::marshaller/keys [context]}
        (marshaller/marshal object-marshaller {} obj)]
    (::marshaller/data (marshaller/unmarshal object-marshaller
                                             {}
                                             marshalled))))

(deftest pack-unpack-test
  (testing "marshal a map and unmarshal the marshalled object"
    (let [obj {:a 1
               :b ["this" "is" "a" "array" 1]
               :c {:name "another-map"}}]

      (testing "with compact object marshaller"
        (is (= obj (pack-unpack compact-marshaller obj))))

      (testing "with edn object marshaller"
        (is (= obj (pack-unpack edn-marshaller obj)))))))
