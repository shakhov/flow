;; This program and the accompanying materials are made available under the terms
;; of the Eclipse Public License v1.0 which accompanies this distribution,
;; and is available at http://www.eclipse.org/legal/epl-v10.htmlg

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

(defn assert-inputs
  [input-map required-keys]
  (when-not (every? input-map required-keys)
    (throw (new Exception (str "Missing input keys: "
                               (set/difference required-keys (set (keys input-map))))))))

(defn safe-order [fg & {paths :paths}]
  (let [{:keys [order remains]} (graph/graph-order fg)
        inputs (graph/external-keys fg)]
    (when-not (empty? remains)
      (let [loops (graph/graph-loops (select-keys fg remains) :paths paths)]
        (throw (new Exception (str "Graph contains cycles: " loops)))))
    order))

(defmacro fnk
  "Return fnk - a keyword function. The function takes single map
  argument to destructure. Fnk asserts that all required keys are present
  and evaluates the body form. Set of required and optional keys is stored in fnk's metadata."
  [bindings & body]
  (let [binding-map (if (vector? bindings) {:keys bindings} bindings)
        {:keys [required-keys optional-keys]} (destructure-bindings binding-map)]
    `(with-meta
       (fn [input-map#]
         (assert-inputs input-map# '~required-keys)
         (let [~binding-map input-map#]
           ~@body))
       {::required-keys '~required-keys
        ::optional-keys '~optional-keys})))

(defn fnk-inputs
  "Return set of keys required to evaluate fnk in the flow."
  ([fnk]
     (select-keys (meta fnk) [::required-keys ::optional-keys]))
  ([fnk flow]
     (set/union (::required-keys (meta fnk))
                (set/intersection (::optional-keys (meta fnk))
                                  (set (keys flow))))))

(defn- destructure-set-keys
  [set-subflow]
  (mapcat (fn [[ks form]]
            (let [tmp-key (gensym "key__")]
              (cons `[(quote ~tmp-key) ~form]
                    (map (fn [k] `[(quote ~k) (fnk {:syms [~tmp-key]} ~tmp-key)])
                         ks))))
          set-subflow))

(defn- destructure-destr-keys
  [map-subflow]
  (mapcat (fn [[ds form]]
            (let [[k1 f1 & destr] (destructure [ds form])
                    destr (dissoc (apply hash-map destr) k1)
                    destr-keys (into #{k1} (keys destr))
                    deps  (map-vals (fn [form] 
                                      (let [deps (if (coll? form)
                                                   (set/intersection destr-keys (set form))
                                                   [form])]
                                        (if (empty? deps) [] {:syms (vec deps)})))
                                    destr)]
              (cons `[(quote ~k1) ~f1]
                    (map (fn [[k f]] `[(quote ~k) (fnk ~(deps k) ~f)])
                         destr))))
          map-subflow))

(defmacro flow
  "Return new flow. Flow is a map from keys to fnks."
  [flow-map]
  {:pre [(map? flow-map)]}
  (let [all-keys (keys flow-map)
        single-keys (filter (complement coll?) all-keys)
        set-keys    (filter set? all-keys)
        destr-keys  (filter #(or (map? %) (vector? %)) all-keys)]
    `(merge
      ~(into {} (map (fn [[key form]] `[(quote ~key) ~form])
                     (select-keys flow-map single-keys)))
      ~(into {} (destructure-set-keys (select-keys flow-map set-keys)))
      ~(into {} (destructure-destr-keys (select-keys flow-map destr-keys))))))

(defn flow-graph
  "Return map from flow keys to their dependencies.
   Required keys of each fnk specify flow graph relationships"
  [flow]
  (map-vals #(fnk-inputs % flow) flow))

(defn- gensym? [s]
  (or (.contains (name s) "map__")
      (.contains (name s) "vec__")
      (.contains (name s) "key__")))

(defn filter-gensyms
  "Return map with all 'gensym' keys removed."
  [m]
  (select-keys m (remove gensym? (keys m))))

(defn sorted-map-by-order
  "Return map sorted by given order."
  [m order]
  (let [order (apply concat order)]
    (into (sorted-map-by
           (fn [k1 k2]
             (compare (.indexOf order k1)
                      (.indexOf order k2))))
          m)))

(defn- evaluate-order [flow order input-map & {:keys [parallel inputs]}]
  "Evaluate flow fnks in the given order using input map values.
   Return a map from keys to values. Output map includes all input keys."

  (let [inputs (or inputs (graph/external-keys (flow-graph flow)))]
    (assert-inputs input-map inputs))
  
  (let [create-map (if parallel
                     lazy-map/create-lazy-map
                     identity)
        eval-key (if parallel
                   (fn [key input-map]
                     (let [f (future ((flow key) input-map))]
                       (delay @f)))
                   (fn [key input-map] ((flow key) input-map)))]
    
    (reduce (fn [output-map stage-keys]
              (into output-map
                    (create-map
                     (zipmap stage-keys
                             (map (fn [key]
                                    (or (input-map key)
                                        (eval-key key output-map)))
                                  stage-keys)))))
            (create-map input-map) order)))

(defn eager-compile
  "Renturn compiled flow function. The function takes map of input values
   and returns the result of evaluating flow fnks in precalculated order.
   Graph ordering and testing graph for loops takes place only once."
  [flow]
  (let [fg (flow-graph flow)
        order  (safe-order fg)
        inputs (graph/external-keys fg)]
    (fn [input-map & {:keys [parallel]}]
      (filter-gensyms (evaluate-order flow order input-map :inputs inputs :parallel parallel)))))

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

(defn lazy-compile
  "Return lazy compiled flow function. The function takes a map of input values
   and returns a lazy-map of delayed evaluations. Each time map key's value is needed,
   all required flow functions are called in proper order. Each key function is evaluated only once."
  [flow]
  (let [fg (flow-graph flow)
        flow-paths (graph/graph-paths fg)
        order (safe-order fg :paths flow-paths)]
    ; Function to return
    (fn [input-map & {:keys [parallel]}]
      (let [output-map (atom input-map)
            flow-memo  (map-map (partial fnk-memoize output-map) flow)
            suborders  (key-suborders fg order flow-paths (keys input-map))
            eval-suborder (fn [k] (evaluate-order
                                   flow-memo ((:orders suborders) k) @output-map
                                   :inputs ((:inputs suborders) k) :parallel parallel))
            delayed-flow (map-keys (fn [k] (delay (get @output-map k (get (eval-suborder k) k))))
                                   flow)]
        (lazy-map/create-lazy-map
         (merge (filter-gensyms delayed-flow)
                input-map))))))

(defn flow->dot
  "Print representation of flow in 'dot' format to standard output."
  [flow]
  (let [fg (flow-graph flow)
        all-keys (set/union (graph/internal-keys fg)
                            (graph/external-keys fg))]
    (println "digraph {")
    (doseq [key all-keys]
      (println (str (name key) ";")))
    (doseq [[key inputs] fg]
      (doseq [i inputs]
        (println (str (name i) " -> " (name key)))))
    (println "}")))