(ns loggly.restructure-tests
  (:use [clojure test]
        [loggly restructure]))

(defn sleep [] (Thread/sleep 10))

(deftest pool-test
  (let [done (atom false)
        collector (atom #{})
        pool (start-index-worker-pool
               {:workers 3
                :queue-size 20
                :done-notifier #(reset! done true)
                :do-index #(swap! collector conj %)})]

    (dotimes [i 20]
      (pool i))

    (sleep)
    (is (= @collector (set (range 20))))  
    (is (not @done))

    (pool :stop)
    (sleep)
    (is @done)
    ))

(deftest indexer-test
  (let [stop-signalled (atom false)
        collector (atom [])
        indexer (start-indexer
                  #(reset! stop-signalled true)
                  #(swap! collector conj %)
                  {:batch-size 3
                   :index-limit 5})]
    (dotimes [i 5]
      (indexer i))
    (sleep)
    (is (not @stop-signalled))
    (doseq [i (range 5 10)]
      (indexer i))
    (sleep)
    (is @stop-signalled)

    (indexer :stop)
    (sleep)
    (is (= @collector
           [[0 1 2]
            [3 4 5]
            [6 7 8]
            [9]
            :stop]))

    ))

(comment (run-tests))
