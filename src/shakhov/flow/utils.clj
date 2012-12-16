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