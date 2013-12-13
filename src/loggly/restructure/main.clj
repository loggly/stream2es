(ns loggly.restructure.main
  (:require [stream2es.es :as es]
            [cheshire.core :as json]
            [loggly.restructure.util :refer [make-url in-daemon]]
            [loggly.restructure.indexing :refer [start-indexers]]
            [loggly.restructure.splitter :refer [start-splitter]]
            [loggly.restructure.setup :refer [create-target-indexes]])
  (:import [java.util.concurrent CountDownLatch]))

(defn get-splitter-policy [opts]
  ; XXX
  (fn [item]
    3))

(defn get-target-index-names [opts]
  ; XXX
  ["foo" "bar" "baz"])

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
    (sink iname))
  (sink :stop))

(defn get-cust [doc]
  (-> doc :_source :_custid))

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
                             done-reporter)]
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
