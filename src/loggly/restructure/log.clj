(ns loggly.restructure.log
  (:import (org.apache.log4j Logger)))

(defmacro deflogger
  "Gets a Log4J Logger for the current namespace and assigns it
   to logger-name"
  [logger-name]
  `(def ~(vary-meta logger-name assoc
           :private true
           :tag Logger)
     (Logger/getLogger (str *ns*))))

(defmacro fatal [logger & args]
  `(.fatal ~logger ~@args))
(defmacro error [logger & args]
  `(.error ~logger ~@args))
(defmacro warn [logger & args]
  `(.warn ~logger ~@args))
(defmacro info [logger & args]
  `(when (.isInfoEnabled ~logger)
     (.info ~logger ~@args)))
(defmacro debug [logger & args]
  `(when (.isDebugEnabled ~logger)
     (.debug ~logger ~@args)))
(defmacro trace [logger & args]
  `(when (.isTraceEnabled ~logger)
     (.trace ~logger ~@args)) )

(defmacro log [logger level & args]
  `(case ~level
     :fatal (fatal ~logger ~@args)
     :error (error ~logger ~@args)
     :warn  (warn ~logger ~@args)
     :info  (info ~logger ~@args)
     :debug (debug ~logger ~@args)
     :trace (trace ~logger ~@args)))
