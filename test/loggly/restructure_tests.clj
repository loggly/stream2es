(ns loggly.restructure-tests
  (:use [clojure test]
        [loggly.restructure indexing splitter]))

(defn sleep [] (Thread/sleep 10))

(deftest pool-test
  (let [done (atom false)
        collector (atom #{})
        pool (start-index-worker-pool
               {:workers-per-index 3
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
            :stop]))))

(defn setup-splitter [continue-fn]
  (let [last-index (atom nil)
        policy {:foo 0 :bar 1 :baz 2}
        collector (atom {})
        finished (atom false)
        indexers (for [i (range 3)]
                   (fn [x]
                     (swap! collector update-in [i] (fnil conj []) x)))
        splitter (start-splitter
                   policy
                   indexers
                   continue-fn
                   (fn [ind]
                     (reset! finished true)
                     (reset! last-index ind))
                   identity)]
    {:last-index last-index
     :collector collector
     :finished finished
     :splitter splitter}
    ))

(deftest splitter-test
  (let [{:keys [last-index
                collector
                finished
                splitter]} (setup-splitter (constantly true))]
    (splitter :foo)
    (splitter :bar)
    (sleep)
    (is (= @collector
           {0 [:foo] 1 [:bar]}))
    (is (not @finished))
    (splitter :new-index)
    (splitter "hello world")
    (splitter :baz)
    (splitter :stop)
    (sleep)
    (is @finished)
    (is (= @collector {0 [:foo :stop] 1 [:bar :stop] 2 [:baz :stop]}))
    (is (= @last-index :all))
    ))

(deftest splitter-check-stop-on-index
  (let [{:keys [last-index
                collector
                finished
                splitter]} (setup-splitter (constantly false))]
    (splitter :foo)
    (splitter :bar)
    (splitter :new-index)
    (splitter "hello world")
    (splitter :baz)
    (sleep)
    (is @finished)
    (is (= @collector {0 [:foo :stop] 1 [:bar :stop] 2 [:stop]}))
    (is (= @last-index "hello world"))))


(comment
  (run-tests))

