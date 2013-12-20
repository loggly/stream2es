(ns loggly.restructure.interactive
  (:use [loggly.restructure main indexing splitter util log es setup])
  (:require [cheshire.core :as json]
            [clojure.pprint :refer [pprint]]
            [clj-http.client :as http]
            [criterium.core :refer [bench quick-bench]]))

; For development only, not imported by any package
; This is basically intended for a vim-fireplace/CIDER workflow -- highlight
; forms and evaluate to make things happen =)

(set! *warn-on-reflection* true)

(identity *3)

(+ 2 2)

(comment

; COUNT


(identity eshost)

(.contains "foo" "f")

; GRAB FIRST FAILED EVEUNT

(-> fails deref first first first pprint)

; COUNT FAILS

(count fails)

; SPLITTER STATS

(deref items-sent)
(deref items-received)

; SCAN
 
(def q (java.util.concurrent.LinkedBlockingQueue. 3))
(in-daemon "quick-scan"
  (run-stream "ec2-23-20-250-74.compute-1.amazonaws.com"
              ["000101.0000.shared.e4db46"
               "131210.2338.shared.8ad3e8"
;               "131211.0450.shared.9dd071"
               ]
              #(.put q %)
              {:scroll-time "30s"
               :scroll-size 100}
              ))
  (let [o (Object.)] (bench (locking o)))

(def orig-event(first(scan
  "http://ec2-23-20-250-74.compute-1.amazonaws.com:9200/131210.2338.shared.8ad3e8"
  (json/generate-string {:query {:term {:_custid 149}}})
  "1m"
  5)))
(def copied-event(first(scan
  "http://ec2-23-20-250-74.compute-1.amazonaws.com:9200/testindex-245"
  (json/generate-string {:query {:term {:_custid 149}}})
  "1m"
  5)))

  (pprint(clojure.data/diff orig-event copied-event))
(pprint orig-event)
(pprint copied-event)
(def random-event (.take q))
(pprint random-event)
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

; COMMON ARGS

(def eshost "ec2-23-20-250-74.compute-1.amazonaws.com")

(def source-indexes
  [
   "131211.0450.shared.9dd071"])

; SCRATCH
  ;
(reduce + (map (comp :count second) ((:dump-stats @visitor-holder))))

; MAIN
  ;
(deflogger logger)

(do
  (refresh!)
  (try
    (main
      {:num-index-workers 10
       :seq-indexes true
       :batch-size 1000
       :index-limit 1000000
       :source-host eshost
       :target-host eshost
       :num-shards 5
       :index-tag "hot"
       :scroll-time "5m"
       :atimeout 50
       :mtimeout 130
       :gtimeout 900
       :scroll-size 1000
       :splitter-events-queued 20000
       :indexer-events-queued 5000
       :bulks-queued 100
       :source-index-names source-indexes
       :target-count 5})
    (catch Throwable e
      (error logger "oops" e)))
))
