;; This program and the accompanying materials are made available under the terms
;; of the Eclipse Public License v1.0 which accompanies this distribution,
;; and is available at http://www.eclipse.org/legal/epl-v10.html

(ns shakhov.flow.core-test
  (:use clojure.test
        shakhov.flow.core))

;; Helper function to compare lazy-maps
(defn- lazy-map= [a b]
  (and (= (set (keys a)) (set (keys b)))
       (every? #(= (get a %) (get b %)) (keys b))))

;; Simple fnk defined using plain argument vector
(let [fnk-vector (fnk [a b c d] (+ a b c d))]

  (deftest fnk-vector-test
    ;; fnk is a function
    (is (fn? fnk-vector))
    ;; with metadata
    (is (meta fnk-vector))
    ;; metadata contains set of required keys
    (is (= #{:a :b :c :d} (:shakhov.flow.core/required-keys (meta fnk-vector))))
    ;; fnk is called with single map argument to produce result of its body evaluation
    (is (= 10 (fnk-vector {:a 1 :b 2 :c 3 :d 4})))))

;; More complex fnk definition using clojure's binding map syntax
(let [fnk-map
      (fnk {key-a :key-a key-b :key-b                           ; keys
            :keys [a b] :syms [sym-a sym-b] :strs [str-a str-b] ; special keys
            :or {key-b 100 sym-b 100 str-b 100 b 100} :as args} ; default values
           (+ a b key-a key-b sym-a sym-b str-a str-b))]
  
  (deftest fnk-map-test
    ;; fnk is a function
    (is (fn? fnk-map))
    ;; metadata contains both required and optional keys
    (is (= #{:a :key-a 'sym-a "str-a"} (:shakhov.flow.core/required-keys (meta fnk-map))))
    (is (= #{:b :key-b 'sym-b "str-b"} (:shakhov.flow.core/optional-keys (meta fnk-map))))
    ;; fnk called with required keys only
    (is (= 800 (fnk-map {:a 100 :key-a 100 'sym-a 100 "str-a" 100})))
    ;; fnk called with required and optional keys
    (is (= 400 (fnk-map {:a 100 :key-a 100 'sym-a 100 "str-a" 100
                         :b 0   :key-b 0   'sym-b 0   "str-b" 0})))))
  
;; Flow is just a map from keywords to fnks. 
;; Flow can be created as a regular map {key (fnk [args] body)}
;; or using 'flow' macro with similar syntax {key ([args] body)}
;; Fnk's inputs are used to determine key dependencies and evaluation order.
(let [flow-1
      (flow {:a  (fnk [x] (- x))
             :b  (fnk [y z] (+ y z))
             c  (fnk {:keys [a b alpha]
                      ;; default values for 'a' and 'b' can be defined
                      ;; but these are ignored during flow evaluation
                      :or {a 0 b 0 alpha 1}}
                     (/ b a alpha))
             "d" (fnk {z :z :syms [c]} (* c z))
             :e  (fnk {:keys [a] :strs [d]} (+ a d))})]
  
  (deftest fnk-inputs-test
    ;; default values for fnk keys are ignored since flow contains same keys
    ;; keys :a and :b are required, though default values {a 0 b 0} were defined
    (is (= #{:a :b} (fnk-inputs (flow-1 'c) flow-1))))
  
  ;; Flow can be compiled to 'eager' function which takes input map
  ;; and evaluates all flow key values at once.
  (let [eager-flow (eager-compile flow-1)]
    
    (deftest eager-flow-test
      (is (fn? eager-flow)))
    
    (deftest eager-flow-eval-test
      ;; Given an input map eager function returns map of all inputs and outputs
      (is (= {:x 1 :y 2 :z 3 :a -1 :b 5 'c -5 "d" -15 :e -16}
             (eager-flow {:x 1 :y 2 :z 3})))
      (is (= {:x 4 :y 5 :z 6 :a -4 :b 11 'c -11/4 "d" -33/2 :e -41/2}
             (eager-flow {:x 4 :y 5 :z 6}))))
  
  
    (deftest eager-flow-parallel-eval-test
      ;; Given an input map eager function returns map of all inputs and outputs
      (is (lazy-map= {:x 1 :y 2 :z 3 :a -1 :b 5 'c -5 "d" -15 :e -16}
                   (eager-flow {:x 1 :y 2 :z 3} :parallel)))
      (is (lazy-map= {:x 4 :y 5 :z 6 :a -4 :b 11 'c -11/4 "d" -33/2 :e -41/2}
                     (eager-flow {:x 4 :y 5 :z 6} :parallel))))
  
    (deftest eager-flow-override-test
      ;; Some keys can be overridden by input map keys.
      ;; Overridden value is used to evaluate dependent keys.
      (is (= {:x 1 :y 2 :z 3 :a -1 :b 5 'c 100 "d" 300 :e 299}
             (eager-flow {:x 1 :y 2 :z 3 'c 100}))))))
  

(let [;; Atom for lazy evaluation testing
      flow-log (atom #{})
      
      ;; Each time a key is evaluated, it is pushed into log with this helper function
      log-key (fn [key] (swap! flow-log #(into % #{key})))
      
      ;; Same flow as above with some simple logging
      flow-2 (flow
              {:a  (fnk [x] (log-key :a)
                        (- x))
               :b  (fnk [y z] (log-key :b)
                        (+ y z))
               c  (fnk {:keys [a b] :or {a 0 b 0}}
                       (log-key 'c)
                       (/ b a))
               "d" (fnk {z :z :syms [c]}
                        (log-key "d")
                        (* c z))
               :e  (fnk {:keys [a] :strs [d]}
                        (log-key :e)
                        (+ a d))})
      
      ;; Flow function can be compiled to 'lazy' function returning 'lazy-map'
      ;; 'Lazy' function is called just like an 'eager' one.
      ;; Keys of a lazy map are evaluated only once when the value is needed.
      lazy-flow (lazy-compile flow-2)]
  
  (deftest lazy-flow-test
    (is (fn? lazy-flow)))
      
  ;; Get single keys
  (deftest lazy-flow-keys-test
    (is (lazy-map= {:x 1 :y 2 :z 3 :a -1 :b 5 'c -5 "d" -15 :e -16}
                   (lazy-flow {:x 1 :y 2 :z 3}))))
  
  ;; With parallel evaluation option
  (deftest lazy-flow-keys-parallel-test
    (is (lazy-map= {:x 1 :y 2 :z 3 :a -1 :b 5 'c -5 "d" -15 :e -16}
                   (lazy-flow {:x 1 :y 2 :z 3} :parallel))))
  
  ;; In a lazy style evaluation only required keys are evaluated
  (deftest lazy-flow-log-test
    (reset! flow-log #{})
    ;; evaluate single key
    (is (= 11 ((lazy-flow {:x 4 :y 5 :z 6}) :b)))
    ;; only :b has been evaluated
    (is (= #{:b} @flow-log))
    ;; reset log
    (reset! flow-log #{})
    ;; evaluate 'c key
    (is (= -11/4 ((lazy-flow {:x 4 :y 5 :z 6}) 'c)))
    ;; :a :b 'c are needed to get 'c value
    ;; notice: :b has been evaluated again,
    ;; because call to the lazy-flow created new lazy-map
    (is (= #{:a :b 'c} @flow-log)))
  
  ;; Input map keys can override outputs
  ;; When overridden key acts as a bridge in a flow graph
  ;; keys in a separated component are ignored
  (deftest lazy-flow-override-log-test
    (reset! flow-log #{})
    (is (= #{} @flow-log))
    ;; override 'c
    (is (= 596 ((lazy-flow {:x 4 :y 5 :z 6 'c 100}) :e)))
    ;; :b key is not required, since 'c is overridden
    (is (= #{:a "d" :e} @flow-log)))
  
  ;; Call a compiled lazy flow function to produce lazy map
  ;; None of its keys are evaluated at this time
  (let [lazy-flow-map-1 (lazy-flow {:x 1 :y 2 :z 3})
        lazy-flow-map-2 (lazy-flow {:x 4 :y 5 :z 6})]
  
    ;; Get keys from the same map
    (deftest lazy-flow-map-keys-test
      (is (= 1 (lazy-flow-map-1 :x)))
      (is (= 2 (lazy-flow-map-1 :y)))
      (is (= 3 (lazy-flow-map-1 :z)))
      (is (= -1 (lazy-flow-map-1 :a)))
      (is (= 5 (lazy-flow-map-1 :b)))
      (is (= -5 (lazy-flow-map-1 'c)))
      (is (= -15 (lazy-flow-map-1 "d")))
      (is (= -16 (lazy-flow-map-1 :e))))
    
    ;; Key values in a lazy map are evaluated and memoized first time they're needed.
    (deftest lazy-flow-map-log-test
      (reset! flow-log #{})
      ;; get the :b key value
      (is (= 11 (:b lazy-flow-map-2)))
      ;; only :b key was evaluated
      (is (= #{:b} @flow-log))
      ;; now reset the log
      (reset! flow-log #{})
      ;; get 'c' key value
      (is (= -11/4 ('c lazy-flow-map-2)))
      ;; only :a and 'c keys were evaluated, but not :b
      ;; each key function is called only once
      (is (= #{:a 'c} @flow-log)))))

;; Flow macro accepts some destructuring forms
(let [flow-destructure
      (flow {:a (fnk [] 1)
             ;; All keys in a set depend on the same fnk via gensymed key
             #{:b c "d"} (fnk [] 2)
             ;; Vector and map binding forms are destructured with clojure.core/destructure
             ;; and produce symbol keywords depending on transient gensymed keys created by destructure
             [a1 [a2 a3 :as v1] & ar :as v0]
             (fnk {a :a :keys[b] :syms [c] :strs [d]}
                  [a [b c] d])
             {k :k :keys [l] :syms [m] :strs [n]}
             (fnk [] {:k 1 :l 2 'm 3 "n" 4})})
      eager-flow-destructure (eager-compile flow-destructure)
      lazy-flow-destructure (lazy-compile flow-destructure)]
  
  (deftest flow-destructure-test
    (is (= {:a 1 :b 2 'c 2 "d" 2 :a1 1 :a2 2 :a3 2 :v1 [2 2] :ar [2] :v0 [1 [2 2] 2] :k 1 :l 2 :m 3 :n 4}
           (eager-flow-destructure {})))
    (is (lazy-map= {:a 1 :b 2 'c 2 "d" 2 :a1 1 ':a2 2 :a3 2 :v1 [2 2] :ar [2] :v0 [1 [2 2] 2] :k 1 :l 2 :m 3 :n 4}
                   (lazy-flow-destructure {})))))

;; Nested destructuring
(let [flow-destructure
      (flow {[a [b1 b2] & {:keys [c d] [e & f] :ef}]
             (fnk [] [1 [2 3] :c 4 :d 5 :ef [6 7 8 9]])})]
  (deftest flow-nested-destructuring
    (is (= {:a 1 :b1 2 :b2 3 :c 4 :d 5 :e 6 :f [7 8 9]}
           ((eager-compile flow-destructure) {})))))

;; Flow macro takes optional argument specifying destructured keys type
(let [flow-destr-keys (flow :keys {{:keys [a b]} (fnk [] {:a 1 :b 2})})
      flow-destr-syms (flow :syms {{:keys [a b]} (fnk [] {:a 1 :b 2})})
      flow-destr-strs (flow :strs {{:keys [a b]} (fnk [] {:a 1 :b 2})})]

  (deftest flow-destructured-keys-type
    (is (= #{:a :b}   (set (keys (filter-gensyms flow-destr-keys)))))
    (is (= #{'a 'b}   (set (keys (filter-gensyms flow-destr-syms)))))
    (is (= #{"a" "b"} (set (keys (filter-gensyms flow-destr-strs)))))))

;; Since 0.1.1 version flow macro does not modify flow key values and does destructuring only
;; So fnks can be produced in place by any valid clojure code
(let [subroutine-flow (flow {:d1 (fnk [v1 v2] (+ v1 v2))
                             :d2 (fnk [v3 v4] (- v3 v4))
                             :d3 (fnk [d1 d2] (* d1 d2))})
      ;; Define flow compiling subflow 
      flow-with-lets (flow {:a (fnk [x] (inc x))
                            :b (fnk [y] (dec y))
                            ;; Eager compile, call fnk, destructure result
                            {:keys [d1 d2]}
                            (let [subroutine (eager-compile subroutine-flow)]
                              (fnk [x y a b]
                                   (subroutine {:v1 x :v2 b :v3 y :v4 a})))
                            ;; Lazy compile, call fnk, select desired key 
                            :l (let [subroutine (lazy-compile subroutine-flow)]
                                 (fnk [x y a b]
                                      ((subroutine {:v1 x :v2 b :v3 y :v4 a}) :d2)))
                            ;; Use destructured keys
                            :sum (fnk [d1 d2] (+ d1 d2))})]
  
  (deftest flow-with-lets-test
    (is (= {:x 15.0 :y 73.0 :a 16.0 :b 72.0 :d1 87.0 :d2 57.0 :l 57.0 :sum 144.0}
           ((eager-compile flow-with-lets) {:x 15.0 :y 73.0})))))

