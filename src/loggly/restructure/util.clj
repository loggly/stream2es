(ns loggly.restructure.util
  (:require [stream2es.log :as log])
  (:import [java.util.concurrent LinkedBlockingQueue]))

(defmacro in-thread [thread-name & forms]
  `(.start
     (Thread.
       (fn [] ~@forms)
       ~thread-name)))

(defn make-url [hostname index-name]
  (format "http://%s:9200/%s" hostname index-name))

(defmacro in-daemon [thread-name & forms]
  `(doto (Thread. 
           (fn [] ~@forms)
           ~thread-name)
     (.setDaemon true)
     .start
     ))

(defn ^LinkedBlockingQueue get-queue [capacity q-name]
  (let [q (LinkedBlockingQueue. capacity)]
    (in-daemon (str q-name " monitor")
      (loop []
        (Thread/sleep 5000)
        (let [cap (.remainingCapacity q)]
          (when (= cap 0)
            (log/debug (str "queue " q-name " is full")))
          (when (= cap capacity)
            (log/debug (str "queue " q-name " is empty")))
          (recur)
          )))
    q))

(.remainingCapacity (get-queue 5 "foo"))

(set! *warn-on-reflection* true)
