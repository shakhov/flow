;; This program and the accompanying materials are made available under the terms
;; of the Eclipse Public License v1.0 which accompanies this distribution,
;; and is available at http://www.eclipse.org/legal/epl-v10.html

(ns shakhov.flow.graph
  (:require [clojure.set :as set])
  (:use shakhov.flow.utils))

(defn internal-keys
  "Return a set of internal keys - keys provided by the graph itself."
  [fg] 
  (set (keys fg)))

(defn external-keys
  "Return a set of external keys - keys required but not provided by graph."
  [fg]
  (set/difference (reduce into #{} (vals fg))
                  (internal-keys fg)))

(defn free-internal-keys
  "Return a set of internal keys with empty dependencies."
  [fg]
  (set (filter #(empty? (fg %)) (keys fg))))

(defn free-keys
  "Return a set of free keys - internal and external."
  [fg]
  (set/union (external-keys fg)
             (free-internal-keys fg)))

(declare extend-path)

(defn key-paths
  "Return set of all paths in graph from given key to external or free keys.
   Paths with loops are terminated with special {:loop [...]} marker."
  [fg base-key]
  (loop [paths [[base-key]]]
    (let [parents (map fg (map peek paths))]
      (if (some (complement empty?) parents)
        (recur (mapcat extend-path paths parents))
        (set paths)))))

(defn graph-paths
  "Return a map from graph keys to all key-paths to external or free keys.
   Paths with loops are terminated with special {:loop [...]} marker."
  ([fg]
     (graph-paths fg (keys fg)))
  ([fg keys]
     (map-keys (partial key-paths fg) (select-keys fg keys))))

(defn key-transitive-deps
  "Return set of all keys the given key depends on."
  ([fg key]
     (key-transitive-deps fg key (key-paths fg key)))
  ([fg key paths]
     (reduce into #{key} paths)))

(defn graph-transitive-deps
  "Return a map from given keys to their dependencies."
  ([fg]
     (graph-transitive-deps fg (graph-paths fg (keys fg))))
  ([fg paths]
     (map-keys (fn[k] (key-transitive-deps fg k (paths k)))
               fg)))

(defn- trim-path
  "Return rest of the path at the first occurance of key"
  ([path key] (trim-path path key 0))
  ([path key i]
     (if (= (path i) key)
       (subvec path i)
       (recur path key (inc i)))))

(defn- extend-path
  "Return all possible extensions of the given path one step towards free keys.
   Paths with loops are terminated with special {:loop [...]} marker."
  [path parents]
  (if (empty? parents)
    [path]
    (for [p parents
          :let [extended-path (conj path p)]]
      (if ((set path) p)
        ;; If loop detected insert ':loop' marker to terminate path
        (conj path {:loop (trim-path extended-path p)})
        extended-path))))

(defn graph-loops
  "Return set of all loops in the graph. Loops are extracted from graph paths."
  [fg & {paths :paths}]
  (let [paths (or paths (graph-paths fg))]
    (->> (vals paths)
         (reduce into [])
         (map peek)
         (filter map?)
         (map :loop)
         (set))))

(defn graph-order
  "Return topological ordering of the graph as a vector of sets of keys with same degree.
   Ordering includes all free (internal and external) keys."
  [fg]
  (let [root-keys (free-keys fg)
        ;; keys with all parents eliminated can be eliminated too
        ready? (fn [order [k req]] (every? (reduce into order) req))]
    (loop [order [root-keys]
           remains (apply dissoc fg root-keys)]
      (if (not (seq remains))
        ;; if all keys are eliminated, return resulting order
        {:order order}
        ;; keys to eliminate
        (let [eliminate (map first (filter (partial ready? order) remains))]
          (if (empty? eliminate)
            ;; nothing can be eliminated - return order and remains
            ;; remaining subgraph probably contains loops
            {:order order
             :remains (keys remains)}
            ;; eliminate and continue
            (recur (conj order (set eliminate))
                   (apply dissoc remains eliminate))))))))

(defn filter-order
  "Return order containing only required keys."
  [order required-keys]
  (map (fn [stage] (set/intersection stage required-keys))
       order))