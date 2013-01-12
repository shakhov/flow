;; This program and the accompanying materials are made available under the terms
;; of the Eclipse Public License v1.0 which accompanies this distribution,
;; and is available at http://www.eclipse.org/legal/epl-v10.html

(ns shakhov.flow.utils)

(defn map-vals
  [f m]
  (zipmap (keys m) (map f (vals m))))

(defn map-keys
  [f m]
  (let [k (keys m)]
    (zipmap k (map f k))))

(defn map-map
  [f m]
  (let [k (keys m)]
    (zipmap k (map f k (vals m)))))