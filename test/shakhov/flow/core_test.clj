(ns shakhov.flow.core-test
  (:use clojure.test
        shakhov.flow.core))

(def fnk-1
  (fnk [a b c] (+ a b c)))

(deftest fnk-test
  (is (fn? fnk-1))
  (is (= #{:a :b :c} (:shakhov.flow.core/required-keys (meta fnk-1))))
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

(deftest lazy-flow-test
  (is (fn? (lazy-compile flow-1)))
  (is (= 24 (:z ((lazy-compile flow-1) {:a 10 :b 20})))))
  (is (= 15.625 (:volume ((lazy-compile flow-cube) {:a 2.5}))))

(def fnk-2
  (fnk {:keys [a b]
        :strs [str-a str-b]
        :syms [sym-a sym-b]
        key-a :keyA
        key-b :keyB
        :or {a -10 sym-a -20 str-a -30 key-a -40}}
       (+ a b sym-a sym-b str-a str-b key-a key-b)))

(deftest fnk-binding
  (is (= #{:b 'sym-b "str-b" :keyB} (:shakhov.flow.core/required-keys (meta fnk-2))))
  (is (= #{:a 'sym-a "str-a" :keyA} (:shakhov.flow.core/optional-keys (meta fnk-2))))
  (is (= 0 (fnk-2 {:b 10 "str-b" 20 'sym-b 30 :keyB 40})))
  (is (= 200 (fnk-2 {:b 10 "str-b" 20 'sym-b 30 :keyB 40
                     :a 10 "str-a" 20 'sym-a 30 :keyA 40}))))