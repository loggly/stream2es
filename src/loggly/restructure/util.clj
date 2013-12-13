(ns loggly.restructure.util
  (:require [stream2es.log :as log])
  (:import [java.util.concurrent LinkedBlockingQueue]))

(defn make-url [hostname index-name]
  (format "http://%s:9200/%s" hostname index-name))

(def refreshes (atom []))

(defn refresh! []
  (doseq [f @refreshes]
    (f))
  (reset! refreshes []))

(defn register-refresh [f]
  (swap! refreshes conj f))

(defn in-thread* [thread-name f daemon]
  (let [t (Thread. f thread-name)]
    (.setDaemon t daemon)
    (register-refresh #(.stop t))
    (.start t)
    nil))

(defmacro in-daemon [thread-name & forms]
  `(in-thread* ~thread-name
               (fn [] ~@forms)
               true))

(defmacro in-thread [thread-name & forms]
  `(in-thread* ~thread-name
               (fn [] ~@forms)
               false))

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
