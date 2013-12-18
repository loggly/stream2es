(ns loggly.restructure.es
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [loggly.restructure.log :refer :all]))

;;; Lifted wholesale from stream2es

(deflogger logger)

(defn components [url]
  (let [u (java.net.URL. url)
        [_ index type id] (re-find
                           #"/*([^/]+)/?([^/]+)?/?([^/]+)?"
                           (.getPath u))]
    {:proto (.getProtocol u)
     :host (.getHost u)
     :port (.getPort u)
     :index index
     :type type
     :id id}))

(defn base-url [full]
  (let [u (components full)]
    (apply format "%s://%s:%s" ((juxt :proto :host :port) u))))

(defn index-url [url]
  (let [{:keys [proto host port index]} (components url)]
    (format "%s://%s:%s/%s" proto host port index)))

(defn post
  ([url data]
     (trace logger "POSTing" (count (.getBytes data)) "bytes")
     (http/post url {:body data})))

(defn delete [url]
  (let [u (index-url url)]
    (info logger (str "delete index " u))
    (http/delete u {:throw-exceptions false})))

(defn exists? [url]
  (try
    (http/get (format "%s/_mapping" (index-url url)))
    (catch Exception _)))

(defn error-capturing-bulk [url items bulk]
  (let [resp (json/decode (:body (post url bulk)) true)]
    (->> (:items resp)
         (map-indexed (fn [n obj]
                        (when (contains? (val (first obj)) :error)
                          (spit (str "error-"
                                     (:_id (val (first obj))))
                                (with-out-str
                                  (prn obj)
                                  (println)
                                  (prn (nth items n))))
                          obj)))
         (remove nil?)
         count)))

(defn scroll*
  "One set of hits mid-scroll."
  [url id ttl]
  (let [resp (http/get
              (format "%s/_search/scroll" url)
              {:body id
               :query-params {:scroll ttl}})]
    (json/decode (:body resp) true)))

(defn scroll
  "lazy-seq of hits from on originating scroll_id."
  [url id ttl]
  (let [resp (scroll* url id ttl)
        hits (-> resp :hits :hits)
        new-id (:_scroll_id resp)]
    (when (seq hits)
      (concat hits (lazy-seq (scroll url new-id ttl))))))

(defn scan1
  "Set up scroll context."
  [url query ttl size]
  (let [resp (http/get
              (format "%s/_search" url)
              {:body query
               :query-params
               {:search_type "scan"
                :scroll ttl
                :size size}})]
    (json/decode (:body resp) true)))

(defn scan
  "Client entry point. Returns a scrolling lazy-seq of hits."
  [url query ttl size]
  (let [resp (scan1 url query ttl size)]
    (scroll (base-url url) (:_scroll_id resp) ttl)))

(defn mapping [url]
  (let [resp (-> (format "%s/_mapping" url)
                 http/get
                 :body
                 (json/parse-string true))
        index (:index (components url))]
    (if index
      (resp (keyword index))
      resp)))

(defn settings [url]
  (let [resp (-> (format "%s/_settings" url)
                 http/get
                 :body
                 (json/parse-string true))
        index (:index (components url))]
    (if index
      (get-in resp [(keyword index) :settings])
      resp)))

(defn filter-keys [m pred]
  (into {}
    (filter
      (comp pred first)
      m)))

(defn keep-key [cids]
  (let [cids (set cids)]
    (fn [k]
      (try (cids (Integer/parseInt (name k)))
        (catch NumberFormatException _ true)))))

(defn filter-mapping [mapping cids]
  (update-in mapping [:log :properties] filter-keys (keep-key cids)))

(def green-url
  "http://%s:9200/_cluster/health/%s?wait_for_status=green&timeout=%ds")

(defn wait-for-green [host iname timeout]
  (info logger (str "waiting up to " timeout " seconds for index "
                    iname " to become green"))
  ;; http/get will throw on bad status
  (http/get (format green-url host iname timeout)))

(defn index-count [host iname]
  (->
    (format "http://%s:9200/%s/log/_count" host iname)
    http/get
    :body
    (json/parse-string true)
    :count))
