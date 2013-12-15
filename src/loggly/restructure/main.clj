(ns loggly.restructure.main
  (:require [stream2es.es :as es]
            [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.cli :refer [cli]]
            [loggly.restructure.util :refer [make-url in-daemon parse-int
                                             refresh! resetting-atom]]
            [loggly.restructure.indexing :refer [start-indexers
                                                 index-opts]]
            [loggly.restructure.splitter :refer [start-splitter
                                                 splitter-opts]]
            [loggly.restructure.setup :refer [create-target-indexes]])
  (:import [java.util.concurrent CountDownLatch]))



(defn get-cust [doc]
  (-> doc :_source :_custid))

(defn get-splitter-policy [{:keys [target-count num-shards]}]
  (fn [doc]
    (let [cust (get-cust doc)]
      (mod (quot cust num-shards) target-count))))

(def index-number (atom 60))
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
    (sink :new-index)
    (sink iname))
  (sink :stop))

(def opts
  (concat splitter-opts index-opts
    [["--source-host" "elasticsearch host to scan from"]
     ["--target-host"
      "elasticsearch host to index to (defaults to source-host)"]
     ["--num-shards" "number of shards to build in target indexes"
      :default 12 :parse-fn parse-int]
     ["--index-tag" "tag to apply to target indexes" :default "hot"]
     ["--scroll-time" "time to leave scroll connection open"
      :default "10m"
      ]
     ["--scroll-size" "number of docs to scan at a time"
      :default 1000 :parse-fn parse-int]
     ["--source-index-names"
      "comma-separated list of indexes to pull events from"
      :parse-fn #(remove empty? (string/split % #","))]
     ["--target-count" "number of indexes to index into"
      :default 8 :parse-fn parse-int]]))

(def full-opts
  (cons
    "Deck Chairs: rebuilds indexes to reduce cluster state" opts))

(identity opts)

(in-daemon "hihi"
           (throw (Exception.))
           )

(comment
  (clojure.pprint/pprint (.take q))
  (use 'loggly.restructure.splitter)
  (-> q .take make-doc source2item clojure.pprint/pprint)
  (def q (java.util.concurrent.LinkedBlockingQueue. 3))
  (let [fcount @items-scanned]
    (Thread/sleep 10000)
    (/ (- @items-scanned fcount) 10))
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
    {:workers-per-index 5
     :batch-size 500
     :index-limit 1000000
     :source-host "ec2-23-20-250-74.compute-1.amazonaws.com"
     :target-host "ec2-23-20-250-74.compute-1.amazonaws.com"
     :num-shards 5
     :index-tag "hot"
     :scroll-time "5m"
     :scroll-size 8000
     :splitter-docs-queued 10000
     :indexer-docs-queued 100
     :bulks-queued 10
     :source-index-names ["000101.0000.shared.e4db46"
                          "131210.2338.shared.8ad3e8"
                          "131211.0450.shared.9dd071"]
     :target-count 1}))

(defn main
  "takes a parsed map of args as supplied by tools.cli"
  [{:keys [source-index-names target-count source-host
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

(defn fail [msg]
  (println msg)
  (System/exit -1))

(defn -main
  "when deployed as a bin, this is the entry point"
  [& args]
  (let [[options extra-args banner] (apply cli args full-opts)]
    (when (:help options)
      (println banner)
      (System/exit 0))
    (when-not (seq (:source-index-names options))
      (fail "must specify at least one source index"))
    (when-not (:source-host options)
      (fail "must specify an ES host to index from"))
    ;; if target-host isn't specified, use source-host
    (when (seq extra-args)
      (fail (str "supplied extraneous args " extra-args)))
    (main (merge {:target-host (:source-host options)}
                 options))))
