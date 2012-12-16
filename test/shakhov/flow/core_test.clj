(ns shakhov.flow.core-test
  (:use clojure.test
        shakhov.flow.core))

(def fnk-1
  (fnk [a b c] (+ a b c)))

(deftest fnk-test
  (is (fn? fnk-1))
  (is (= #{:a :b :c} (:input-keys (meta fnk-1))))
  (is (= (+ 4 5 6) (fnk-1 {:a 4 :b 5 :c 6}))))

(def flow-1
  (flow {:x (fnk [a] (+ 10 a))
         :y (fnk [b] (/ b 5))
         :z (fnk [x y] (+ x y))}))

(deftest flow-test
  (is (fn? (eager-compile flow-1)))
  (is (= {:a 10 :b 20 :x 20 :y 4 :z 24}
         ((eager-compile flow-1) {:a 10 :b 20})))
  (is (= {:a 0.5 :b -0.6 :x 10.5 :y -0.12 :z 10.38}
         ((eager-compile flow-1) {:a 0.5 :b -0.6}))))