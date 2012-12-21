(ns shakhov.flow.core
  (:use [shakhov.flow.utils])
  (:require [shakhov.flow.graph :as graph]
            [lazymap.core :as lazy-map]
            [clojure.set :as set]))

(defmacro fnk
  "Return fnk - a keyword function. The function takes single map
  argument to destructure with {:keys [...]}. Fnk asserts that all
  required keys are present and evaluates the body form. Set of required
  inputs is stored in fnk's metadata."
  [inputs & body]
  (let [input-keys (set (map keyword inputs))]
    `(with-meta
       (fn [input-map#]
         (assert (every? input-map# ~input-keys))
         (let [{:keys ~inputs} input-map#]
           ~@body))
       {:input-keys ~input-keys})))

(defn fnk-inputs
  "Return set of keys required to evaluate fnk."
  [fnk]
  (:input-keys (meta fnk)))

(defmacro flow
  "Return new flow. Flow is a map from keywords to fnks."
  [flow-map]
  {:pre [(map? flow-map)]}
  (into {} (map (fn [[key decl]]
                  [key `(fnk ~@decl)])
                flow-map)))

(defn flow-graph
  "Return map from flow keys to their dependencies.
   Required inputs of each fnk specify flow graph relationships"
  [flow]
  (map-vals fnk-inputs flow))

(defn evaluate-order [flow order input-map inputs]
  "Evaluate flow fnks in the given order using input map values.
   Return a map from keywords to values. Output map includes all input keywords."
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
           required (graph/external-keys fg)]
      (when-not (empty? remains)
        (assert (empty? (graph/graph-loops (select-keys fg remains)))))
      (fn [input-map]
        (evaluate-order flow order input-map required)))))

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