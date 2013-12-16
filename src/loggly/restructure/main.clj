(ns loggly.restructure.main
  (:require [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.cli :refer [cli]]
            [loggly.restructure.es :as es]
            [loggly.restructure.util :refer [make-url in-daemon
                                             resetting-atom
                                             parse-int]]
            [loggly.restructure.indexing :refer [start-indexers
                                                 index-opts]]
            [loggly.restructure.splitter :refer [start-splitter
                                                 splitter-opts]]
            [loggly.restructure.setup :refer [create-target-indexes]])
  (:import [java.util.concurrent CountDownLatch]))



(defn get-cust [doc]
  (-> doc :_source :_custid))

(def count-by-cust (resetting-atom {}))

(defn get-splitter-policy [{:keys [target-count num-shards]}]
  (fn [doc]
    (let [cust (get-cust doc)]
      (swap! count-by-cust update-in [cust] (fnil inc 0))
      (mod (quot cust num-shards) target-count))))

(def index-number (atom 230))

(defn get-fresh-index-name []
  (str "testindex-" (swap! index-number inc)))

(defn get-target-index-names [{:keys [target-count]}]
  ; XXX
  (repeatedly target-count get-fresh-index-name))

(def match-all
  (json/generate-string
    {"query"
      {"match_all" {}}}))

(def items-scanned (resetting-atom 0))

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
      :default 9 :parse-fn parse-int]
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

(defn main
  "takes a parsed map of args as supplied by tools.cli"
  [{:keys [source-index-names target-count source-host
          target-host]
    :as opts}]
  (let [target-index-names (get-target-index-names opts)
        indexer-done-latch (CountDownLatch. 1)
        continue-flag      (atom true)
        indexers           (start-indexers
                             (merge opts
                               {:index-names target-index-names
                                :finish #(.countDown
                                           indexer-done-latch)
                                :signal-stop #(reset!
                                                continue-flag
                                                false)}))
        splitter-policy    (get-splitter-policy opts)
        done-reporter      (fn [up-to]
                             (.await indexer-done-latch)
                             (println "done indexing up to " up-to))
        splitter           (start-splitter
                             (merge opts
                               {:policy splitter-policy
                                :indexers indexers
                                :continue? (fn [] @continue-flag)
                                :finish done-reporter}))]
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
