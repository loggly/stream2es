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

