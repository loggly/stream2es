(ns loggly.restructure.index
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
  (when-first [ix imdb-indexes]
    (concat (AssignmentDAO/queryByIndex (.iid ix))
            (get-imdb-assignments (rest imdb-indexes)))))

(defmacro if-first [[v s] iform eform]
  `(if-let [s# (seq ~s)]
     (let [~v (first s#)]
       ~iform)
     ~eform))

(defn merge-customers [imdb-assns]
  (loop [assns imdb-assns
         counts {}
         merged-assns {}]
    (if-first [assn assns]
      (let [cid (.cid assn)
            cid-count (+ (cid counts 0) (.statsEventCount assn))]
        (recur
          (rest assns)
          (assoc counts cid cid-count)
          (assoc merged-assns cid {:cid cid :stats-event-count cid-count})))
      merged-assns)))

(defn get-retention [cid]
  ;XXX
  14)

(defn add-retentions [assns]
  (map (fn [assn]
        {:cid (:cid assn)
         :stats-event-count (:stats-event-count assn)
         :retention (get-retention (:cid assn))})
       assns))

(defn compare-by [ks x y]
  (loop [ks ks]
    (if-first [k ks]
      (let [ret (compare (k x) (k y))]
        (if (= ret 0)
          (recur (rest ks))
          ret))
      0)))

(defn by-retention-events [x y]
  (let [c (compare (:retention y) (:retention x))]
    (if (not= c 0)
      c
      (let [c (compare (:stats-event-count y) (:stats-event-count x))]
        c))))

(defn get-src-assns [index-names]
  (->> index-names
       (map get-imdb-index)
       get-imdb-assignments
       merge-customers
       add-retentions
       (sort by-retention-events)))

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
  (let [dst-assns (build-dst-assns source-index-names target-count)]
    (fn [custid]
      (custid dst-assns))))
