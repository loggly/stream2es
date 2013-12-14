(ns loggly.restructure.setup
  (:require [loggly.restructure.util :refer [make-url]]
            [loggly.restructure.log :refer :all]
            [loggly.restructure.es :as es]
            [cheshire.core :as json]
            [clojure.pprint :refer [pprint]]
            [clojure.data :refer [diff]]))

(deflogger logger)

; I think this should work?
(def routing-mapping
  {:log
    {:_routing
      {:required true
       :path "_custid"}}})

(defn check-settings [expected target-url]
  (let [result-settings (dissoc
                          (es/settings target-url)
                          ; this gets set by ES, ignore it
                          :index.uuid)]
    (when-not (= result-settings expected)
      (let [[expected-missing unexpected] (diff expected result-settings)]
        (throw
          (Exception.
            (str "settings on newly created index at " target-url
                 " are missing " (or expected-missing {})
                 " and unexpectedly contain " unexpected)))))))

(defn create-target-indexes [[source-name] target-names
                             {:keys [source-host target-host
                                     num-shards index-tag]}]
  (let [source-url (make-url source-host source-name)
        source-settings (es/settings source-url)
        overrides {:index.routing.allocation.include.tag index-tag
                   :index.number_of_shards (str num-shards)}
        new-settings (merge source-settings overrides)
        creation-json (json/generate-string
                        {:settings new-settings
                         :mappings routing-mapping})]
    (doseq [iname target-names]
      (let [target-url (make-url target-host iname)]
        (when (es/exists? target-url)
          (throw (Exception. (str "target index " iname
                                  " already exists =/"))))
        (es/post target-url creation-json)
        (check-settings new-settings target-url)))))
