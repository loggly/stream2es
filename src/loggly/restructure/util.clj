(ns loggly.restructure.util
  (:require [loggly.restructure.log :refer :all])
  (:import [java.util.concurrent LinkedBlockingQueue]))

(deflogger logger)

(defn make-url [hostname index-name]
  (format "http://%s:9200/%s" hostname index-name))

(def refreshes (atom []))
(def perm-refreshes (atom []))

(defn refresh! []
  (doseq [f @refreshes]
    (f))
  (Thread/sleep 200)
  (doseq [f @perm-refreshes]
    (f))
  (reset! refreshes [])
  nil)

(defn register-refresh [f]
  (swap! refreshes conj f))

(defn register-perm-refresh [f]
  (swap! perm-refreshes conj f))

(defn in-thread* [thread-name f daemon]
  (let [t (Thread. f thread-name)]
    (.setDaemon t daemon)
    (.setUncaughtExceptionHandler t
      (reify Thread$UncaughtExceptionHandler
        (uncaughtException [_ t e]
          (fatal
            logger
            (str "thread " t " died unexpectedly")
            e))))
    (register-refresh #(.stop t))
    (.start t)
    nil))

(defmacro in-daemon [thread-name & forms]
  `(in-thread*
     ~thread-name
     (fn [] ~@forms)
     true))

(defmacro in-thread [thread-name & forms]
  `(in-thread*
     ~thread-name
     (fn [] ~@forms)
     false))

(defn ^LinkedBlockingQueue get-queue [capacity q-name]
  (let [q (LinkedBlockingQueue. capacity)]
    (in-daemon (str q-name "-qmonitor")
      (loop []
        (Thread/sleep 5000)
        (let [cap (.remainingCapacity q)]
          (when (= cap 0)
            (debug logger (str "queue " q-name " is full")))
          (when (= cap capacity)
            (debug logger (str "queue " q-name " is empty")))
          (recur)
          )))
    q))

(.remainingCapacity (get-queue 5 "foo"))

(set! *warn-on-reflection* true)

(defn resetting-atom [i]
  (let [a (atom i)]
    (register-perm-refresh #(reset! a i))
    a))
