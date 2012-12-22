(ns shakhov.flow.core
  (:use [shakhov.flow.utils])
  (:require [shakhov.flow.graph :as graph]
            [lazymap.core :as lazy-map]
            [clojure.set :as set]))

(defn- destructure-bindings
  [binding-map]
  (let [key-names (set (:keys binding-map))
        sym-names (set (:syms binding-map))
        str-names (set (:strs binding-map))
        rest-bindings  (dissoc binding-map :keys :syms :strs :or :as)
        default-names  (set (keys (:or binding-map)))]

    {:required-keys (set/union
                     (set/difference sym-names default-names)
                     (set (map name (set/difference str-names default-names)))
                     (set (map keyword (set/difference key-names default-names)))
                     (set (vals (apply dissoc rest-bindings default-names))))
                     
     :optional-keys (set/union
                     (set/intersection sym-names default-names)
                     (set (map name (set/intersection str-names default-names)))
                     (set (map keyword (set/intersection key-names default-names)))
                     (set (vals (select-keys rest-bindings default-names))))}))


(defmacro fnk
  "Return fnk - a keyword function. The function takes single map
  argument to destructure. Fnk asserts that all required keys are present
  and evaluates the body form. Set of required and optional keys is stored in fnk's metadata."
  [bindings & body]
  (let [binding-map (if (vector? bindings) {:keys bindings} bindings)
        {:keys [required-keys optional-keys]} (destructure-bindings binding-map)]
    `(with-meta
       (fn [input-map#]
         (assert (every? input-map# '~required-keys))
         (let [~binding-map input-map#]
           ~@body))
       {::required-keys '~required-keys
        ::optional-keys '~optional-keys})))

(defn fnk-inputs
  "Return set of keys required to evaluate fnk in the flow."
  [flow fnk]
  (set/union (::required-keys (meta fnk))
             (set/intersection (::optional-keys (meta fnk))
                               (set (keys flow)))))

(defmacro flow
  "Return new flow. Flow is a map from keys to fnks."
  [flow-map]
  {:pre [(map? flow-map)]}
  (into {} (map (fn [[key decl]]
                  [key `(fnk ~@decl)])
                flow-map)))

(defn flow-graph
  "Return map from flow keys to their dependencies.
   Required keys of each fnk specify flow graph relationships"
  [flow]
  (map-vals (partial fnk-inputs flow) flow))

(defn evaluate-order [flow order input-map inputs]
  "Evaluate flow fnks in the given order using input map values.
   Return a map from keys to values. Output map includes all input keys."
  (assert (every? input-map inputs))
  (reduce (fn [output-map eval-stage]
            (into output-map
                  (map (fn [key]
                         [key (or (input-map key)
                                  ((flow key) output-map))])
                       eval-stage)))
          input-map order))

(def eager-compile
  "Renturn compiled flow function. The function takes map of input values
   and returns the result of evaluating flow fnks in precalculated order.
   Graph ordering and testing graph for loops takes place only once."
  (fn [flow]
    (let [fg (flow-graph flow)
          {:keys [order remains]} (graph/graph-order fg)
           inputs (graph/external-keys fg)]
      (when-not (empty? remains)
        (assert (empty? (graph/graph-loops (select-keys fg remains)))))
      (fn [input-map]
        (evaluate-order flow order input-map inputs)))))

(defn- fnk-memoize [memo k f]
  (with-meta 
    (fn [input-map]
      (or (@memo k)
          (let [output (f input-map)]
            (swap! memo assoc k output)
            output))) 
    (meta f)))

(defn- key-suborders
  [fg order paths inputs]
  (let [overriden (set/intersection
                   (set inputs)
                   (set/difference (graph/internal-keys fg)
                                   (graph/free-internal-keys fg)))
        key-paths (map-vals (partial filter #(not-any? overriden %)) paths)
        key-deps  (graph/graph-transitive-deps fg key-paths)]
    {:orders (map-keys #(graph/filter-order order (key-deps %)) fg)
     :inputs (map-keys #(set/intersection (graph/external-keys fg) (key-deps %)) fg)}))

(def lazy-compile
  "Return lazy compiled flow function. The function takes a map of input values
   and returns a lazy-map of delayed evaluations. Each time map key's value is needed,
   all required flow functions are called in proper order. Each key function evaluates only once."
  (fn [flow]
    (let [fg (flow-graph flow)
          {:keys [order remains]} (graph/graph-order fg)
          flow-paths (graph/graph-paths fg)]
      (when-not (empty? remains)
        (assert (empty? (graph/graph-loops (select-keys fg remains)))))
      ; Function to return
      (fn [input-map]
        (let [output-map (atom input-map)
              flow-memo  (map-map (partial fnk-memoize output-map) flow)
              suborders  (key-suborders fg order flow-paths (keys input-map))]
          (lazy-map/create-lazy-map
           (merge (map-keys (fn [k]
                              (delay (or (k @output-map)
                                         (k (evaluate-order flow-memo   ((:orders suborders) k)
                                                            @output-map ((:inputs suborders) k))))))
                            flow)
                  input-map)))))))