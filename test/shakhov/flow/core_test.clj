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
  (flow {:x ([a] (+ 10 a))
         :y ([b] (/ b 5))
         :z ([x y] (+ x y))}))

(def flow-cube
  (flow {:num-faces ([] 6)
         :num-edges ([] 12)
         :face-area ([a] (* a a))
         :total-area ([face-area num-faces] (* face-area num-faces))
         :volume ([a] (* a a a))}))

(deftest flow-test
  (is (fn? (eager-compile flow-1)))
  (is (= {:a 10 :b 20 :x 20 :y 4 :z 24}
         ((eager-compile flow-1) {:a 10 :b 20})))
  (is (= {:a 0.5 :b -0.6 :x 10.5 :y -0.12 :z 10.38}
         ((eager-compile flow-1) {:a 0.5 :b -0.6})))
  (is (= {:a 2.5 :num-faces 6 :num-edges 12 :face-area 6.25 :total-area 37.5 :volume 15.625}
         ((eager-compile flow-cube) {:a 2.5}))))

(deftest flow-overload
  (is (= {:a 0.5 :b -0.6 :x 999 :y -0.12 :z 998.88}
         ((eager-compile flow-1) {:a 0.5 :b -0.6 :x 999}))))

(deftest parallel-test
  (is (fn? (eager-compile flow-1 :parallel true)))
  (is (= {:a 10 :b 20 :x 20 :y 4 :z 24}
         ((eager-compile flow-1 :parallel true) {:a 10 :b 20})))
  (is (= {:a 0.5 :b -0.6 :x 10.5 :y -0.12 :z 10.38}
         ((eager-compile flow-1 :parallel true) {:a 0.5 :b -0.6})))
  (is (= {:a 2.5 :num-faces 6 :num-edges 12 :face-area 6.25 :total-area 37.5 :volume 15.625}
         ((eager-compile flow-cube :parallel true) {:a 2.5}))))