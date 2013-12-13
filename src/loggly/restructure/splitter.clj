(ns loggly.restructure.splitter
  (:require [loggly.restructure.util :refer [in-thread]])
  (:import [java.util.concurrent LinkedBlockingQueue]))

(defn make-doc
  "taken from stream2es. Ask Drew."
  [hit]
  (merge (:_source hit)
         {:_id (:_id hit)
          :_type (:_type hit)})  
  )

(defrecord BulkItem [meta source])
(defn source2item [source]
  "taken from stream2es. Ask Drew."
  (BulkItem.
   {:index
    (merge
     {:_index nil
      :_type (:_type source)}
     (when (:_id source)
       {:_id (str (:_id source))}))}
   (dissoc source :_type)))

(defn start-splitter [policy indexers continue?
                      finish & [transformer]]
  (let [transformer (or transformer
                        #(-> % make-doc source2item))
        q (LinkedBlockingQueue. 10) ;XXX
        ; pack in a vector for quick lookups
        indexers (into [] indexers)
        flush-indexers (fn [] (doseq [indexer indexers]
                                (indexer :stop)))]
    (in-thread "splitter-thread"
      (loop []
        (let [item (.take q)]
          (case item
            :stop (do
                    (flush-indexers)
                    (finish :all))
            :new-index (let [ind-name (.take q)]
                         (if (continue?)
                           (recur)
                           (do
                             (flush-indexers)
                             (finish ind-name))))
            (let [indexer-id (policy item)
                  indexer (nth indexers indexer-id)]
              (indexer (transformer item))
              (recur))))))
    (fn [item] (.put q item))))

