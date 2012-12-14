(ns shakhov.flow.core
  (:use [shakhov.flow.utils]))

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

