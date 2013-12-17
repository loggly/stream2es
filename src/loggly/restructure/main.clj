(ns loggly.restructure.main
  (:require [cheshire.core :as json]
            [clojure.string :as string]
            [clojure.tools.cli :refer [parse-opts]]
            [clojure.pprint :refer [pprint]]
            [loggly.restructure.es :as es]
            [loggly.restructure.log :refer :all]
            [loggly.restructure.util :refer [make-url in-daemon
                                             resetting-atom
                                             parse-int]]
            [loggly.restructure.indexing :refer [start-indexers
                                                 index-opts]]
            [loggly.restructure.splitter :refer [start-splitter
                                                 splitter-opts]]
            [loggly.restructure.setup :refer [create-target-indexes]])
  (:import [java.util.concurrent CountDownLatch])
  (:gen-class))



(defn get-cust [event]
  (-> event :_source :_custid))

(defn get-rec-ts [event]
  (-> event :_source :_rects))

(def count-by-cust (resetting-atom {}))

(defn get-splitter-policy [{:keys [target-count num-shards]}]
  (fn [event]
    (let [cust (get-cust event)]
      (swap! count-by-cust update-in [cust] (fnil inc 0))
      (mod (quot cust num-shards) target-count))))

(def index-number (atom 275))

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

(deflogger logger)

(defn run-stream [host index-names sink
                  {:keys [scroll-time scroll-size]}]
  (doseq [iname index-names]
    (info logger (str "beginning scan for index " iname))
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
    [["-h" "--help" "display this help message"]
     [nil "--source-host eshost01" "elasticsearch host to scan from"]
     [nil "--target-host eshost02"
      "elasticsearch host to index to (defaults to source-host)"]
     [nil "--num-shards NSHARDS" "number of shards to build in target indexes"
      :default 9 :parse-fn parse-int]
     [nil "--index-tag hot|cold" "tag to apply to target indexes"
      :default "cold" :validate [#{"hot" "cold"} "must be one of hot|cold"]]
     [nil "--scroll-time Xm" "time to leave scroll connection open"
      :default "10m"
      ]
     [nil "--scroll-size NEVENTS" "number of events to scan at a time"
      :default 1000 :parse-fn parse-int]
     [nil "--source-index-names ind1,ind2"
      "comma-separated list of indexes to pull events from"
      :parse-fn #(remove empty? (string/split % #","))]
     [nil "--target-count NINDEXES" "number of indexes to index into"
      :default 8 :parse-fn parse-int]
     [nil "--atimeout NSECS" "ES ack timeout seconds" :default 60
      :parse-fn parse-int]
     [nil "--mtimeout NSECS" "ES master timeout seconds" :default 120
      :parse-fn parse-int]
     [nil "--gtimeout NSECS"
      "Seconds to wait for created index to become green"
      :default 900 :parse-fn parse-int]]))

(defn print-usage [opt-summary]
  (println
    "Deck Chairs: rebuilds indexes to reduce cluster state")
  (println)
  (println opt-summary))

(defn get-visitor [opts]
  (let [storage (atom {})]
    {:visit-event
      (fn [event]
        (let [cust (get-cust event)
              ts (get-rec-ts event)]
          (swap! storage update-in [cust]
            (fn [stats]
              (-> stats
                (or {:count 0 :min-rec ts :max-rec ts})
                (update-in [:count] inc)
                (update-in [:min-rec] min ts)
                (update-in [:max-rec] max ts))))))
     :dump-stats #(deref storage)}))

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
        visitor            (get-visitor opts)
        visit-event        (:visit-event visitor)
        done-reporter      (fn [up-to]
                             (.await indexer-done-latch)
                             (println "done indexing up to " up-to)
                             (println "got stats "
                                      ((:dump-stats visitor))))
        splitter           (start-splitter
                             (merge opts
                               {:policy splitter-policy
                                :indexers indexers
                                :visit-event visit-event
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

(defn fail [msg opt-summary]
  (print-usage opt-summary)
  (println)
  (println msg)
  (System/exit -1))

(defn -main
  "when deployed as a bin, this is the entry point"
  [& args]
  (let [{:keys [options arguments errors summary]}
        (parse-opts args opts)]
    (when (:help options)
      (print-usage summary)
      (System/exit 0))
    (when (seq errors)
      (fail (string/join \newline errors) summary))
    (when-not (seq (:source-index-names options))
      (fail "must specify at least one source index" summary))
    (when-not (:source-host options)
      (fail "must specify an ES host to index from" summary))
    (when (seq arguments)
      (fail (str "supplied extraneous args " arguments) summary))
    ;; if target-host isn't specified, use source-host
    (let [parsed-opts (merge
                        {:target-host (:source-host options)}
                        options)]
      (debug logger (str "calling main with opts " parsed-opts))
      (main parsed-opts))))
