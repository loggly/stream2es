(ns loggly.restructure
  (:require [stream2es.main :as main]
            [stream2es.es :as es]
            [stream2es.log :as log]
            [cheshire.core :as json]
            [clojure.data :as data])
  (:import [java.util.concurrent CountDownLatch
                                 LinkedBlockingQueue]))

(defmacro in-thread [thread-name & forms]
  `(.start
     (Thread.
       (fn [] ~@forms)
       ~thread-name)))

(defn get-splitter-policy [opts]
  ; XXX
  (fn [item]
    3))

(defn do-until-stop [source action]
  (loop []
    (let [x (source)]
      (when-not (= :stop x)
        (action x)
        (recur)))))

(defmacro if-first [[x xs] if-case else-case]
  `(if-let [xs# (seq ~xs)]
     (let [~x (first xs#)]
       ~if-case)
     ~else-case))

(defn compare-by [ks x y]
  (loop [ks ks]
    (if-first [k ks]
      (let [ret (compare (k x) (k y))]
        (if (= ret 0)
          (recur (rest ks))
          ret))
      0)))

(defn start-index-worker-pool
  "takes a number of workers, a number of bulks to queue, a function
   to call on completion, and a function to index a single bulk.
   Returns a function that can be called with a
   list of documents or with :stop to signal done

   stolen with modifications from stream2es main"
  [{:keys [workers-per-index done-notifier do-index]}]
  (let [q (LinkedBlockingQueue. 5) ; XXX
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
   source))

(defn start-indexers [index-names finish signal-stop
                      index-fn-fact opts]
  (for [iname index-names]
    (start-indexer
      signal-stop
      (start-index-worker-pool
        (assoc opts
          :done-notifier finish
          :do-index (index-fn-fact iname)))
      opts)))

(defn get-target-index-names [opts]
  ; XXX
  ["foo" "bar" "baz"])

(defn start-splitter [policy indexers continue?
                      finish transformer]
  (let [q (LinkedBlockingQueue. 10) ;XXX
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

(defn make-url [hostname index-name]
  (format "http://%s:9200/%s" hostname index-name))

; I think this should work?
(def routing-mapping
  {:order
    {:_routing
      {:required true
       :path "_custid"}}})

(defn create-target-indexes [[source-name] target-names
                             {:keys [source-host target-host
                                     num-shards index-tag]}]
  (let [source-url (make-url source-host source-name)
        source-settings (es/settings source-url)
        overrides {:index.routing.allocation.include.tag index-tag
                   :index.number_of_shards num-shards}
        new-settings (merge source-settings overrides)
        creation-json (json/generate-string
                        {:settings new-settings
                         :mapping  routing-mapping})]
    (doseq [iname target-names]
      (let [target-url (make-url target-host iname)]
        (when (es/exists? target-url)
          (throw (Exception. (str "target index " iname
                                  " already exists =/"))))
        (es/post target-url creation-json)
        (let [result-settings (es/settings target-url)]
          (when-not (= result-settings new-settings)
            (println "settings are different -- maybe that's ok? "
                     (data/diff new-settings result-settings))))))))

(defmacro in-daemon [thread-name & forms]
  `(doto (Thread. 
           (fn [] ~@forms)
           ~thread-name)
     (.setDaemon true)
     .start
     ))

(defn do-index
  "sends a list of documents to an url

   stolen with modifications from stream2es"
  [target-url bulk]
  (when (and (sequential? bulk) (pos? (count bulk)))
    (let [first-id (-> bulk first :meta :index :_id)
          idxbulk (main/make-indexable-bulk bulk)
          idxbulkbytes (count (.getBytes idxbulk))
          url (format "%s/%s" target-url "_bulk") ]
      (es/error-capturing-bulk url bulk main/make-indexable-bulk))))

(def match-all
  (json/generate-string
    {"query"
      {"match_all" {}}}))

(defn run-stream [host index-names sink
                  {:keys [scroll-time scroll-size]}]
  (doseq [iname index-names]
    (doseq [hit (es/scan
                  (make-url host iname)
                  match-all
                  scroll-time
                  scroll-size)]
      (sink hit))
    (sink :index)
    (sink :iname))
  (sink :stop))


(comment opts
         workers-per-index
         batch-size
         index-limit
         source-host
         target-host
         num-shards
         index-tag
         scroll-time
         scroll-size
         source-index-names
         target-count
         )
(defn main [{:keys [source-index-names target-count source-host
                    target-host]
             :as opts}]
  (let [target-index-names (get-target-index-names opts)
        indexer-done-latch (CountDownLatch. target-count)
        continue-flag      (atom true)
        index-fn-fact      (fn [iname]
                             (let [url (make-url target-host iname)]
                               (fn [bulk] (do-index url bulk))))
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
                             done-reporter
                             (comp source2item make-doc))]
    (create-target-indexes
      source-index-names
      target-index-names
      opts)
    (in-daemon "scan-thread"
      (run-stream
        source-host
        source-index-names
        splitter
        opts))))
