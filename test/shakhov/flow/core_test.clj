;; This program and the accompanying materials are made available under the terms
;; of the Eclipse Public License v1.0 which accompanies this distribution,
;; and is available at http://www.eclipse.org/legal/epl-v10.html

(ns shakhov.flow.core-test
  (:use clojure.test
        shakhov.flow.core))

;; Simple fnk defined using plain argument vector
(def fnk-vector
  (fnk [a b c d] (+ a b c d)))

(deftest fnk-vector-test
  ;; fnk is a function
  (is (fn? fnk-vector))
  ;; with metadata
  (is (meta fnk-vector))
  ;; metadata contains set of required keys
  (is (= #{:a :b :c :d} (:shakhov.flow.core/required-keys (meta fnk-vector))))
  ;; fnk is called with single map argument to produce result of its body evaluation
  (is (= 10 (fnk-vector {:a 1 :b 2 :c 3 :d 4}))))

;; More complex fnk definition using clojure's binding map syntax
(def fnk-map
  (fnk {key-a :key-a key-b :key-b                           ; keys
        :keys [a b] :syms [sym-a sym-b] :strs [str-a str-b] ; special keys
        :or {key-b 100 sym-b 100 str-b 100 b 100} :as args} ; default values
       (+ a b key-a key-b sym-a sym-b str-a str-b)))

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
                       :b 0   :key-b 0   'sym-b 0   "str-b" 0}))))

;; Flow is just a map from keywords to fnks. 
;; Flow can be created as a regular map {key (fnk [args] body)}
;; or using 'flow' macro with similar syntax {key ([args] body)}
;; Fnk's inputs are used to determine key dependencies and evaluation order.
(def flow-1
  (flow
   {:a  ([x] (- x))
    :b  ([y z] (+ y z))
    'c  ({:keys [a b alpha]
          ;; default values for 'a' and 'b' can be defined
          ;; but these are ignored during flow evaluation
          :or {a 0 b 0 alpha 1}}
         (/ b a alpha))
    "d" ({z :z :syms [c]} (* c z))
    :e  ({:keys [a] :strs [d]} (+ a d))}))

(deftest fnk-inputs-test
  ;; defalut values for fnk keys are ignored since flow contains same keys
  ;; keys :a and :b are required, though default values {a 0 b 0} were defined
  (is (= #{:a :b} (fnk-inputs flow-1 (flow-1 'c)))))

;; Flow can be compiled to 'eager' function which takes input map
;; and evaluates all flow key values at once.
(def eager-flow (eager-compile flow-1))

(deftest eager-flow-test
  (is (fn? eager-flow)))

(deftest eager-flow-eval-test
  ;; Given an input map eager function returns map of all inputs and outputs
  (is (= {:x 1 :y 2 :z 3 :a -1 :b 5 'c -5 "d" -15 :e -16}
         (eager-flow {:x 1 :y 2 :z 3})))
  (is (= {:x 4 :y 5 :z 6 :a -4 :b 11 'c -11/4 "d" -33/2 :e -41/2}
         (eager-flow {:x 4 :y 5 :z 6}))))

(deftest eager-flow-eval-override-test
  ;; Some keys can be overridden by input map keys.
  ;; Overridden value is used to evaluate dependent keys.
  (is (= {:x 1 :y 2 :z 3 :a -1 :b 5 'c 100 "d" 300 :e 299}
         (eager-flow {:x 1 :y 2 :z 3 'c 100}))))

;; Atom for lazy evaluation testing
;; FIXME: single atom for multiple tests can probably cause test fails
(def flow-log (atom #{}))

;; Each time a key is evaluated, it is pushed into log with this helper function
(defn- log-key [key]
  (swap! flow-log #(into % #{key})))

;; Same flow as above with some simple logging
(def flow-2
    (flow
     {:a  ([x] (log-key :a)
             (- x))
      :b  ([y z] (log-key :b)
             (+ y z))
      'c  ({:keys [a b] :or {a 0 b 0}}
           (log-key 'c)
           (/ b a))
      "d" ({z :z :syms [c]}
           (log-key "d")
           (* c z))
      :e  ({:keys [a] :strs [d]}
           (log-key :e)
           (+ a d))}))

;; Flow function can be compiled to 'lazy' function returning 'lazy-map'
;; 'Lazy' function is called just like an 'eager' one.
;; Keys of a lazy map are evaluated only once when the value is needed.
(def lazy-flow (lazy-compile flow-2))

(deftest lazy-flow-test
  (is (fn? lazy-flow)))

(deftest lazy-flow-eval-test
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
;; keys in a separated sucomponent are ignored
(deftest lazy-flow-override-test
  (reset! flow-log #{})
  (is (= #{} @flow-log))
  ;; override 'c
  (is (= 596 ((lazy-flow {:x 4 :y 5 :z 6 'c 100}) :e)))
  ;; :b key is not required, since 'c is overridden
  (is (= #{:a "d" :e} @flow-log)))

;; Call a compilled lazy flow function to produce lazy map
;; None of its keys are evaluated at this timex
(def lazy-flow-map (lazy-flow {:x 1 :y 2 :z 3}))

;; Key values in a lazy map are evaluated and memoized first time they're needed.
(deftest lazy-flow-map-test
  (reset! flow-log #{})
  ;; get the :b key value
  (is (= 5 (:b lazy-flow-map)))
  ;; only :b key was evaluated
  (is (= #{:b} @flow-log))
  ;; now reset the log
  (reset! flow-log #{})
  ;; get 'c' key value
  (is (= -5 ('c lazy-flow-map)))
  ;; only :a and 'c keys were evaluated, but not :b
  ;; each key function is called only once since in the lazy-map
  (is (= #{:a 'c} @flow-log)))
