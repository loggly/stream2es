(ns loggly.restructure.setup
  (:require [loggly.restructure.util :refer [make-url]]
            [stream2es.es :as es]
            [cheshire.core :as json]
            [clojure.data :refer [diff]]))

; I think this should work?
(def routing-mapping
  {:order
    {:_routing
      {:required true
       :path "_custid"}}})

(defn create-target-indexes [[source-name] target-names
                             {:keys [source-host target-host
                                     num-shards index-tag]}]
  (let [source-url (make-url source-host source-name)
        source-settings (es/settings source-url)
        overrides {:index.routing.allocation.include.tag index-tag
                   :index.number_of_shards num-shards}
        new-settings (merge source-settings overrides)
        creation-json (json/generate-string
                        {:settings new-settings
                         :mapping  routing-mapping})]
    (doseq [iname target-names]
      (let [target-url (make-url target-host iname)]
        (when (es/exists? target-url)
          (throw (Exception. (str "target index " iname
                                  " already exists =/"))))
        (es/post target-url creation-json)
        (let [result-settings (es/settings target-url)]
          (when-not (= result-settings new-settings)
            (println "settings are different -- maybe that's ok? "
                     (diff new-settings result-settings))))))))

