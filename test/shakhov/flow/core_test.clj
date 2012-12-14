(ns shakhov.flow.core-test
  (:use clojure.test
        shakhov.flow.core))

(def fnk-1
  (fnk [a b c] (+ a b c)))

(deftest fnk-test
  (testing "Testing fnk"
    (is (fn? fnk-1))
    (is (= #{:a :b :c} (:input-keys (meta fnk-1))))
    (is (= (+ 4 5 6) (fnk-1 {:a 4 :b 5 :c 6})))))