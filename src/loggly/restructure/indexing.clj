(ns loggly.restructure.indexing
  (:require [stream2es.main :refer [make-indexable-bulk]]
            [stream2es.es :as es]
            [stream2es.log :as log]
            [loggly.restructure.util :refer [in-thread make-url
                                             resetting-atom get-queue]])
  (:import [java.util.concurrent CountDownLatch]))

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
  [{:keys [workers-per-index done-notifier
           do-index bulks-queued pool-name]}]
  (let [q (get-queue bulks-queued pool-name)
        latch (CountDownLatch. workers-per-index)]
    ;; start index pool
    (dotimes [n workers-per-index]
      (in-thread (str pool-name "-" (inc n))
        (do-until-stop #(.take q) do-index)
        (log/debug "waiting for POSTs to finish")
        (.countDown latch)))
    ;; notify when done
    (in-thread (str pool-name "-monitor")
      (.await latch)
      (log/debug "done indexing")
      (done-notifier))
    ;; This becomes :indexer above!
    (fn [bulk]
      (if (= bulk :stop)
        (dotimes [n workers-per-index]
          (.put q :stop))
        (.put q bulk)))))

(defn start-indexer [signal-stop bulk-sink process-name
                     {:keys [batch-size index-limit
                             indexer-docs-queued] :as opts}]
  (let [q (get-queue indexer-docs-queued process-name)
        building-batch (atom [])
        batch-doc-count (atom 0)
        total-doc-count (atom 0)
        do-flush (fn []
                   (bulk-sink @building-batch)
                   (reset! batch-doc-count 0)
                   (reset! building-batch []))]
    (in-thread process-name
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

(def bulks-indexed (resetting-atom 0))
(deref bulks-indexed)

(defn do-index
  "sends a list of documents to an url

   stolen with modifications from stream2es"
  [target-url bulk]
  (when (and (sequential? bulk) (pos? (count bulk)))
    (let [url (format "%s/%s" target-url "_bulk") ]
      (es/error-capturing-bulk url bulk make-indexable-bulk)
      (swap! bulks-indexed inc))))

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
          (merge opts
            {:done-notifier finish
             :do-index (index-fn-fact iname target-host)
             :pool-name (str "pool-" iname)} ))
        (str "indexer-" iname)
        opts))))
