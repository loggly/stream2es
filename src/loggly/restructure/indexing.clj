(ns loggly.restructure.indexing
  (:require [stream2es.main :refer [make-indexable-bulk]]
            [stream2es.es :as es]
            [stream2es.log :as log]
            [loggly.restructure.util :refer [in-thread make-url]])
  (:import [java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue]) 
  )

(defn do-until-stop [source action]
  (loop []
    (let [x (source)]
      (when-not (= :stop x)
        (action x)
        (recur)))))

(defn start-index-worker-pool
  "takes a number of workers, a number of bulks to queue, a function
   to call on completion, and a function to index a single bulk.
   Returns a function that can be called with a
   list of documents or with :stop to signal done

   stolen with modifications from stream2es main"
  [{:keys [workers-per-index done-notifier do-index]}]
  (let [q (LinkedBlockingQueue.) ; XXX
        latch (CountDownLatch. workers-per-index)
        disp (fn []
               (do-until-stop #(.take q) do-index)
               (log/debug "waiting for POSTs to finish")
               (.countDown latch))
        lifecycle (fn []
                    (.await latch)
                    (log/debug "done indexing")
                    (done-notifier))]
    ;; start index pool
    (dotimes [n workers-per-index]
      (.start (Thread. disp (str "indexer " (inc n)))))
    ;; notify when done
    (.start (Thread. lifecycle "index service"))
    ;; This becomes :indexer above!
    (fn [bulk]
      (if (= bulk :stop)
        (dotimes [n workers-per-index]
          (.put q :stop))
        (.put q bulk)))))

(defn start-indexer [signal-stop bulk-sink
                     {:keys [batch-size index-limit] :as opts}]
  (let [q (LinkedBlockingQueue.) ;XXX
        building-batch (atom [])
        batch-doc-count (atom 0)
        total-doc-count (atom 0)
        do-flush (fn []
                   (bulk-sink @building-batch)
                   (reset! batch-doc-count 0)
                   (reset! building-batch []))]
    (in-thread "indexer-thread-N" ;XXX
      (loop []
        (let [item (.take q)]
          (if (= item :stop)
            (do
              (do-flush)
              (bulk-sink :stop))
            (do
              (swap! building-batch conj item)
              (when (= (swap! batch-doc-count inc)
                       batch-size)
                (do-flush))
              (when (> (swap! total-doc-count inc)
                       index-limit)
                (signal-stop))
              (recur))))))
    (fn [item] (.put q item))))

(defn do-index
  "sends a list of documents to an url

   stolen with modifications from stream2es"
  [target-url bulk]
  (when (and (sequential? bulk) (pos? (count bulk)))
    (let [url (format "%s/%s" target-url "_bulk") ]
      (es/error-capturing-bulk url bulk make-indexable-bulk))))

(defn default-index-fn-fact [iname target-host]
  (let [url (make-url target-host iname)]
    (fn [bulk] (do-index url bulk))))

(defn start-indexers [index-names finish signal-stop
                      {:keys [target-host] :as opts}]
  (let [index-fn-fact (:index-fn-fact opts
                        default-index-fn-fact)]
    (for [iname index-names]
      (start-indexer
        signal-stop
        (start-index-worker-pool
          (assoc opts
            :done-notifier finish
            :do-index (index-fn-fact iname target-host)))
        opts))))
