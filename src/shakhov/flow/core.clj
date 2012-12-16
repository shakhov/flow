(ns shakhov.flow.core
  (:use [shakhov.flow.utils])
  (:require [shakhov.flow.graph :as graph]))

(defmacro fnk
  "Return fnk - a keyword function. The function takes single map
  argument to destructure with {:keys [...]}. Fnk asserts that all
  required keys are present and evaluates the body form. Set of requred
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
  flow-map)

(defn flow-graph
  "Return map from flow keys to their dependencies.
   Required inputs of each fnk specify flow graph relationships"
  [flow]
  (map-vals fnk-inputs flow))

(defn evaluate-order [flow order required-keys input-map]
  "Evaluate flow fnks in the given order using input map values.
   Return a map from keywords to values. Output map includes all input keywords.
   Input map must contain all required keywords."
  (reduce (fn [output-map eval-stage]
            (apply merge output-map
                   (map (fn [key] {key (or (input-map key) ((flow key) output-map))})
                        eval-stage)))
          input-map
          order))

(def eager-compile
  "Renturn compiled flow function. The function takes map of input values
   and returns the result of evaluating flow fnks in precalculated order.
   Graph ordering and testing graph for loops takes place only once."
  (fn [flow]
    (let [fg (flow-graph flow)
          {:keys [order remains]} (graph/graph-order fg)
          required-keys (graph/external-keywords fg)]
      (when-not (empty? remains)
        (assert (empty? (graph/graph-loops (select-keys fg remains)))))
      (fn [input-map]
        (assert (every? input-map required-keys))
        (evaluate-order flow order required-keys input-map)))))
