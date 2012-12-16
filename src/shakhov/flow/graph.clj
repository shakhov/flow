(ns shakhov.flow.graph
  (:require [clojure.set :as set])
  (:use shakhov.flow.utils))

(defn internal-keywords
  "Return a set of internal keywords - keywords provided by the graph itself."
  [fg] 
  (set (keys fg)))

(defn external-keywords
  "Return a set of external keywords - keywords required but not provided by graph."
  [fg]
  (set/difference (reduce into #{} (vals fg))
                  (internal-keywords fg)))

(defn free-internal-keywords
  "Return a set of internal keywords with empty dependencies."
  [fg]
  (set (filter #(empty? (fg %)) (keys fg))))

(defn free-keywords
  "Return a set of free keywords - internal and external."
  [fg]
  (set/union (external-keywords fg)
             (free-internal-keywords fg)))

(declare extend-path)

(defn keyword-paths
  "Return set of all paths in graph from given keyword to external or free keywords.
   Paths with loops are terminated with special {:loop [...]} marker."
  [fg base-key]
  (loop [paths [[base-key]]]
    (let [parents (map fg (map peek paths))]
      (if (some (complement empty?) parents)
        (recur (mapcat extend-path paths parents))
        (set paths)))))

(defn graph-paths
  "Return a map from graph keywords to path from these keywords to free ones.
   Paths with loops are terminated with special {:loop [...]} marker."
  [fg]
  (map-keys (partial keyword-paths fg) fg))

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
  "Return topological ordering of the graph as a vector of sets of keywords with same degree.
   Ordering includes all free (internal and external) keywords."
  [fg]
  (let [root-keywords (free-keywords fg)
        ;; keywords with all parents eliminated can be eliminated too
        ready? (fn [order [k req]] (every? (reduce into order) req))]
    (loop [order [root-keywords]
           remains (apply dissoc fg root-keywords)]
      (if (not (seq remains))
        ;; if all keywords are eliminated, return resulting order
        {:order order}
        ;; keywords to eliminate
        (let [eliminate (map first (filter (partial ready? order) remains))]
          (if (empty? eliminate)
            ;; nothing can be eliminated - return order and remains
            ;; remaining subgraph probably contains loops
            {:order order
             :remains (keys remains)}
            ;; eliminate and continue
            (recur (conj order (set eliminate))
                   (apply dissoc remains eliminate))))))))
