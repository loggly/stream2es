(ns loggly.restructure
  (:require [stream2es.main :as main]
            [stream2es.es :as es]
            [stream2es.log :as log])
  (:import [clojure.lang ExceptionInfo]
           [java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit]))

(defmacro in-thread [thread-name & forms]
  `(.start
     (Thread.
       (fn [] ~@forms)
       ~thread-name)))

(defn get-splitter-policy [opts]
  ; XXX
  (fn [cust-id]
    3))

(defn start-index-worker-pool [& opts]
  ;XXX
  )

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
            (do-flush)
            (do
              (swap! building-batch conj item)
              (when (> (swap! batch-doc-count inc)
                       batch-size)
                (do-flush))
              (when (> (swap! total-doc-count inc)
                       index-limit)
                (signal-stop))
              (recur))))))
    (fn [item] (.put q item)))

  )

(defn start-indexers [index-names finish signal-stop index-fn-fact opts]
  (for [iname index-names]
    (start-indexer
      signal-stop
      (start-index-worker-pool
        finish
        (index-fn-fact iname))
      opts)))

(defn get-target-index-names [opts]
  ; XXX
  ["foo" "bar" "baz"])

(defn get-cust [item] 
  ; XXX
  5)

(defn start-splitter [policy indexers continue? finish]
  (let [q (LinkedBlockingQueue.)
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
            (let [cust (get-cust item)
                  indexer-id (policy cust)
                  indexer (nth indexers indexer-id)]
              (indexer item)
              (recur))))))
    (fn [item] (.put q item))))

(defn create-target-indexes [names opts]
  ; XXX
  )

(defmacro in-daemon [thread-name & forms]
  `(doto (Thread. 
           (fn [] ~@forms)
           ~thread-name)
     (.setDaemon true)
     .start
     ))

(defn get-item-stream [host index-names]
  ; XXX
  )

(defn do-index [target-url bulk]
  (when (and (sequential? bulk) (pos? (count bulk)))
    (let [first-id (-> bulk first :meta :index :_id)
          idxbulk (main/make-indexable-bulk bulk)
          idxbulkbytes (count (.getBytes idxbulk))
          url (format "%s/%s" target-url "_bulk") ]
      (es/error-capturing-bulk url bulk main/make-indexable-bulk))))

(defn make-url [hostname index-name]
  (format "http://%s:9200/%s" hostname index-name))

(defn main [{:keys [source-index-names target-count source-host
                    target-host shard-count]
             :as opts}]
  (let [target-index-names (get-target-index-names opts)
        indexer-done-latch (CountDownLatch. target-count)
        continue-flag      (atom true)
        index-fn-fact      (fn [iname]
                             (let [url (make-url target-host iname)]
                               (fn [bulk (do-index url bulk)])))
        indexers           (start-indexers
                             target-index-names
                             #(.countDown indexer-done-latch)
                             #(reset! continue-flag false)
                             index-fn-fact
                             opts)
        splitter-policy    (get-splitter-policy opts)
        done-reporter      (fn [up-to]
                             (.await indexer-done-latch)
                             (println "done indexing up to " up-to))
        splitter           (start-splitter
                             splitter-policy
                             indexers
                             (fn [] @continue-flag)
                             done-reporter)]
    (create-target-indexes target-index-names opts)
    (in-daemon "scan-thread"
      (doseq [item (get-item-stream source-host source-index-names)]
        (splitter item)))))
