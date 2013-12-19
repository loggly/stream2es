#_(ns loggly.restructure.index
  (:require [clojure.data.priority-map :refer [priority-map]])
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

(defn get-cust-sets [assns]
  (let [inds (set (map index-for-assn assns))]
    (into {}
      (for [ind inds]
        [ind (->> assns
               (filter #(= ind (ind-for-assn %)))
               (map cust-for-assn))]))))

(future (+ 2 2 ))

(require ['criterium.core :refer ['quick-bench]])

(comment (quick-bench (deref (future (+ 2 2)) 200 nil)))

(defmacro if-first [[v s] iform eform]
  `(if-let [s# (seq ~s)]
     (let [~v (first s#)]
       ~iform)
     ~eform))

(defn merge-customers [imdb-assigns]
  (let [cids (set (map #(.cid %) imdb-assigns))]
    (into {}
      (for [cid cids]
        (let [event-count (->>
                            imdb-assigns
                            (filter #(= cid (.cid %)))
                            (map #(.statsEVentCount %))
                            (reduce +))]
          {:cid cid :stats-event-count event-count})))))

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
