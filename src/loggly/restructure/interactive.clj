(ns loggly.restructure.interactive
  (:use [loggly.restructure main indexing splitter util log es setup])
  (:require [cheshire.core :as json]
            [clojure.pprint :refer [pprint]]
            [clj-http.client :as http]))

; For development only, not imported by any package
; This is basically intended for a vim-fireplace/CIDER workflow -- highlight
; forms and evaluate to make things happen =)

(set! *warn-on-reflection* true)

(comment

; COUNT

(-> "http://ec2-23-20-250-74.compute-1.amazonaws.com:9200/testindex-20/log/_count"
  http/get
  :body
  (json/parse-string true)
  :count
  )

; GRAB FIRST FAILED EVEUNT

(-> fails deref first first first pprint)

; SPLITTER STATS

(deref items-sent)
(deref items-received)

; SCAN
 
(def q (java.util.concurrent.LinkedBlockingQueue. 3))
(in-daemon "quick-scan"
  (run-stream "ec2-23-20-250-74.compute-1.amazonaws.com"
              ["000101.0000.shared.e4db46"
               "131210.2338.shared.8ad3e8"
               "131211.0450.shared.9dd071"]
              #(.put q %)
              {:scroll-time "30s"
               :scroll-size 100}
              ))
(clojure.pprint/pprint (.take q))
(-> q .take make-doc source2item clojure.pprint/pprint)

; RATE

(let [fcount @items-scanned]
  (Thread/sleep 10000)
  (/ (- @items-scanned fcount) 10))

; PER CUSTOMER STATS

(deref count-by-cust)

; CHECK INDEX NUMBER

(deref index-number)

; SCAN STATS

(deref items-scanned)

; BULK STATS

(deref bulks-indexed)

; MAIN

(do
  (refresh!)
  (main
    {:workers-per-index 10
     :batch-size 1000
     :index-limit 1000000
     :source-host "ec2-23-20-250-74.compute-1.amazonaws.com"
     :target-host "ec2-23-20-250-74.compute-1.amazonaws.com"
     :num-shards 5
     :index-tag "hot"
     :scroll-time "5m"
     :scroll-size 1000
     :splitter-docs-queued 20000
     :indexer-docs-queued 5000
     :bulks-queued 100
     :source-index-names ["000101.0000.shared.e4db46"
                          "131210.2338.shared.8ad3e8"
                          "131211.0450.shared.9dd071"]
     :target-count 5})
))
