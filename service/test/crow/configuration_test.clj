(ns crow.configuration-test
  (:require [clojure.test :refer :all]
            [crow.configuration :refer :all :as conf]))


(deftest extension
  (testing "extention fn"
    (is (= (#'conf/extension "test.edn") "edn"))
    (is (= (#'conf/extension "test") ""))
    (is (= (#'conf/extension nil) ""))
    (is (= (#'conf/extension "") ""))))
