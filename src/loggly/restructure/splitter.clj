(ns loggly.restructure.splitter
  (:require [loggly.restructure.util :refer [resetting-atom
                                             in-thread
                                             parse-int
                                             get-queue]]
            [loggly.restructure.log :refer :all]))

(deflogger logger)

(defn make-doc
  "taken from stream2es. Ask Drew."
  [hit]
  (when-not (or (-> hit :_source :_type)
                (-> hit :_type))
    (error logger (str "no type in hit: " hit)))
  (merge (:_source hit)
         {:_id (:_id hit)
          :_type (:_type hit)}))

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

(def items-sent (resetting-atom 0))
(def items-received (resetting-atom 0))

(def splitter-opts
  [["--splitter-docs-queued" "# of docs to queue after scan"
    :default 10000 :parse-fn parse-int]])

(defn start-splitter [{:keys [policy indexers continue? finish
                              transformer splitter-docs-queued]}]
  (let [transformer (or transformer
                        #(-> % make-doc source2item))
        q (get-queue splitter-docs-queued "splitter")
        ; pack in a vector for quick lookups
        indexers (into [] indexers)
        flush-indexers (fn [] (doseq [indexer indexers]
                                (indexer :stop)))]
    (in-thread "splitter"
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
              (swap! items-sent inc)
              (indexer (transformer item))
              (recur))))))
    (fn [item]
      (swap! items-received inc)
      (.put q item))))

