(ns shakhov.flow.core)

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
  