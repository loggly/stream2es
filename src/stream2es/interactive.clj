(ns stream2es.interactive
  (:require [stream2es.main :as main]
            [stream2es.es :as es]
            [stream2es.log :as log])
  (:import [clojure.lang ExceptionInfo]
           [java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue
                                 TimeUnit]))

(def opts
  [["-i" "indexes" "comma-delimited set of index names to restructure"]
   
   ])

(do-index [target-url bulk]
  (when (and (sequential? bulk) (pos? (count bulk)))
    (let [first-id (-> bulk first :meta :index :_id)
          idxbulk (main/make-indexable-bulk bulk)
          idxbulkbytes (count (.getBytes idxbulk))
          url (format "%s/%s" target-url "_bulk") ]
      (es/error-capturing-bulk url bulk main/make-indexable-bulk))))

(defn do-until-stop [source action]
  (loop []
    (let [x (source)]
      (when-not (= :stop x)
        (action x)
        (recur)))))

(defn seq-from-q
  "assumes last message is :stop"
  [q]
  (lazy-seq
    (let [x (.take q)]
      (when-not (= x :stop)
        (cons x (seq-from-q q))))))

(defn start-index-worker-pool
  "takes a number of workers, a number of bulks to queue, a function
   to call on completion, and a function to index a single bulk.
   Returns a function that can be called with a
   list of documents or with :stop to signal done"
  [{:keys [workers queue-size done-notifier do-index]}]
  (let [q (LinkedBlockingQueue. queue-size)
        latch (CountDownLatch. workers)
        disp (fn []
               (do-until-stop #(fn [] (.take q)) do-index)
               (log/debug "waiting for POSTs to finish")
               (.countDown latch))
        lifecycle (fn []
                    (.await latch)
                    (log/debug "done indexing")
                    (done-notifier))]
    ;; start index pool
    (dotimes [n workers]
      (.start (Thread. disp (str "indexer " (inc n)))))
    ;; notify when done
    (.start (Thread. lifecycle "index service"))
    ;; This becomes :indexer above!
    (fn [bulk]
      (if (= bulk :stop)
        (dotimes [n workers]
          (.put q :stop))
        (.put q bulk)))))

