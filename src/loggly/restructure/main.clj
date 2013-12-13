(ns loggly.restructure.main
  (:require [stream2es.es :as es]
            [cheshire.core :as json]
            [loggly.restructure.util :refer [make-url in-daemon
                                             refresh! resetting-atom]]
            [loggly.restructure.indexing :refer [start-indexers]]
            [loggly.restructure.splitter :refer [start-splitter]]
            [loggly.restructure.setup :refer [create-target-indexes]])
  (:import [java.util.concurrent CountDownLatch]))

(defn get-splitter-policy [opts]
  (constantly 0))

(def index-number (atom 21))
(deref index-number)

(defn get-target-index-names [opts]
  ; XXX
  [(str "testindex-" (swap! index-number inc))])

(def match-all
  (json/generate-string
    {"query"
      {"match_all" {}}}))

(def items-scanned (resetting-atom 0))
(deref items-scanned)

(defn run-stream [host index-names sink
                  {:keys [scroll-time scroll-size]}]
  (doseq [iname index-names]
    (doseq [hit (es/scan
                  (make-url host iname)
                  match-all
                  scroll-time
                  scroll-size)]
      (swap! items-scanned inc)
      (sink hit))
    (sink :index)
    (sink iname))
  (sink :stop))

(defn get-cust [doc]
  (-> doc :_source :_custid))

(comment
  (clojure.pprint/pprint (.take q))
  (use 'loggly.restructure.splitter)
  (-> q .take make-doc source2item clojure.pprint/pprint)
  (def q (java.util.concurrent.LinkedBlockingQueue.))
  (in-daemon "quick-scan"
    (run-stream "ec2-23-20-250-74.compute-1.amazonaws.com"
                ["000101.0000.shared.e4db46"
                 "131210.2338.shared.8ad3e8"
                 "131211.0450.shared.9dd071"]
                #(.put q %)
                {:scroll-time "30s"
                 :scroll-size 100}
                ))
  (refresh!)
  (main
    {:workers-per-index 3
     :batch-size 100
     :index-limit 1000000
     :source-host "ec2-23-20-250-74.compute-1.amazonaws.com"
     :target-host "ec2-23-20-250-74.compute-1.amazonaws.com"
     :num-shards 5
     :index-tag "hot"
     :scroll-time "5m"
     :scroll-size 500
     :splitter-docs-queued
     :indexer-docs-queued
     :bulks-queued
     :source-index-names ["000101.0000.shared.e4db46"
                          "131210.2338.shared.8ad3e8"
                          "131211.0450.shared.9dd071"]
     :target-count 1}))

(defn main [{:keys [source-index-names target-count source-host
                    target-host]
             :as opts}]
  (let [target-index-names (get-target-index-names opts)
        indexer-done-latch (CountDownLatch. target-count)
        continue-flag      (atom true)
        indexers           (start-indexers
                             target-index-names
                             #(.countDown indexer-done-latch)
                             #(reset! continue-flag false)
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
                             opts)]
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
