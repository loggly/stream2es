(ns loggly.restructure.index
  (:require [clojure.data.priority-map :refer [priority-map]]
            [loggly.restructure.util :refer [map-of]])
  (:import [clojure.lang ExceptionInfo]
           [java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit]
           [java.util PriorityQueue]
           [com.loggly.indexmanager.db.dao AssignmentDAO IndexDAO]))

(defn get-imdb-index [index-name]
  (IndexDAO/queryByName index-name))

(defn get-imdb-assignments [imdb-indexes]
  (mapcat #(AssignmentDAO/queryByIndex (.iid %)) imdb-indexes))

(defn expired-assn? [assn]
  ; XXX
  false)

(defn index-for-assn [assn]
  "someind")

(defn cust-for-assn [assn]
  35)

(defn get-cust-sets [assns]
  (let [inds (set (map index-for-assn assns))]
    (into {}
      (for [ind inds]
        [ind (->> assns
               (filter #(= ind (index-for-assn %)))
               (map cust-for-assn))]))))

(defn get-cust-routes [assns target-count])

(defmacro if-first [[v s] iform eform]
  `(if-let [s# (seq ~s)]
     (let [~v (first s#)]
       ~iform)
     ~eform))

(defn bytes-for-assn [assn]
  ; XXX
  2344556)

(defn expiry-for-assn [assn]
  ; XXX
  134656609)

(defn merge-customers [imdb-assigns]
  (let [cids (set (map cust-for-assn imdb-assigns))]
    (into {}
      (for [cid cids]
        (let [cust-assns (filter #(= cid (cust-for-assn %)) imdb-assigns)
              byte-count (->>
                           cust-assns
                           (map bytes-for-assn)
                           (reduce +))
              max-expire (->>
                          cust-assns
                          (map expiry-for-assn)
                          (reduce max))]
          (map-of cid byte-count max-expire))))))

(defn take-n-bytes [custs byte-limit]
  (loop [rem-custs custs
         bytes-taken 0
         taken-custs []]
    (if-first [cust rem-custs]
      (if (> (+ bytes-taken (/ (:byte-count cust) 2))
             byte-limit)
        [taken-custs bytes-taken rem-custs]
        (recur
          (rest rem-custs)
          (+ bytes-taken (:byte-count cust))
          (conj taken-custs cust)))
      [taken-custs bytes-taken rem-custs])))

(defn make-partitions [custs target-count]
  (loop [rem-custs custs
         partitions-left target-count
         partitions-created []
         bytes-remaining (->> custs
                              (map :byte-count)
                              (reduce +))]
    (if (zero? partitions-left)
      (if (empty? rem-custs)
        partitions-created
        (throw (Exception. "wtf")))
      (let [byte-limit (/ bytes-remaining partitions-left)
            [new-part bytes-still-rem still-rem-custs]
              (take-n-bytes rem-custs byte-limit)]
        (recur
          still-rem-custs
          (dec partitions-left)
          (conj partitions-created new-part)
          bytes-still-rem)))
    ))

(defn get-retention [cid]
  ;XXX
  14)

(defn add-retentions [assns]
  (for [assn assns]
    (assoc assn
      :retention (get-retention (:cid assn)))))

(defn compare-by [ks]
  (fn [x y]
    (or (->>
          ks
          (map (fn [k] (compare (k x) (k y))))
          (remove #(= % 0))
          first)
        0)))

(defn get-src-assns [index-names]
  (->> index-names
       (map get-imdb-index)
       get-imdb-assignments
       merge-customers
       add-retentions
       (sort (compare-by [:retention :stats-event-count]))))

(defn build-dst-assns [source-index-names target-count]
  (let [src-assns (get-src-assns source-index-names)]
    (loop [assns src-assns
           dst-ixs (into (priority-map) (map vector
                                          (range target-count)
                                          (repeat 0)))
           dst-assns {}]
      (let [assn (first assns)
            dst-ix (peek dst-ixs)
            dst-ix-id (first dst-ix)
            dst-ix-count (second dst-ix)]
        (if (empty? assns)
            dst-assns
            (recur (rest assns)
                   (assoc dst-ixs dst-ix-id
                     (+ dst-ix-count (:stats-event-count assn)))
                   (assoc dst-assns (:cid assn) dst-ix-id)))))))

; order by (retention, stats_event_count) descending
(defn get-splitter-policy [{:keys [source-index-names target-count] :as opts}]
  (build-dst-assns source-index-names target-count))
