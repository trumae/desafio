(ns desafio.core
  (:require [clojurewerkz.cassaforte.client :as cas]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.query :as q]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.response :refer [not-found? hits-from]]
            [clj-time.core :as clj-time]
            )
  (:import java.util.UUID)
  (:gen-class))


(def ^{:const true} index-name "desafio")
(def ^{:const true} index-type "tweet")

(def mapping-types {"tweet"
                    {:properties {:timeline {:type "string"
                                             :store "yes"}
                                  :text {:type "string"
                                         :store "yes"}
                                  :sync  {:type "boolean" :default false}
                                  :created_at {:type "date" :format "date_hour_minute_second"}}}})

;; Elasticsearch

(defn esCreateIndexDesafio [esconn]
  (esi/create esconn index-name :mappings mapping-types :settings {"number_of_shards" 1}))

(defn esDeleteIndexDesafio[esconn]
  (esi/delete esconn index-name))

(defn putRandomTweets [esconn n]
  (if (pos? n)
    (do
      (esd/put esconn index-name index-type (str (UUID/randomUUID)) {:timeline "vvmaciel"
                                                                     :text (str "texto " n)
                                                                     :sync false})
      (recur esconn (dec n)))))

(defn countTweets [esconn]
  (:count (esd/count esconn index-name index-type)))

(defn deleteAllTweets[esconn]
  (esd/delete-by-query esconn index-name index-type (q/match-all)))

(defn fetch-scroll-results [conn scroll-id results]
  (let [scroll-response (esd/scroll conn scroll-id :scroll "1m")
        hits            (hits-from scroll-response)]
    (if (seq hits)
      (recur conn (:_scroll_id scroll-response) (concat results hits))
      (concat results hits))))

(defn getNotSyncTweets [esconn]
  (let [response     (esd/search esconn index-name index-type
                                 :query (q/term :sync false)
                                 :search_type "query_then_fetch"
                                 :scroll "1m"
                                 :size 10)
        initial-hits (hits-from response)
        scroll-id    (:_scroll_id response)
        all-hits     (fetch-scroll-results esconn scroll-id initial-hits)]
    all-hits))



(defn elasticsearch2cassandra [esconn casconn]
  (let [hits (getNotSyncTweets esconn)]
    (es2cas esconn casconn hits)))
;; Cassandra



;; Enable command-line invocation
(defn -main [& args]
  (let [esconn (esr/connect "http://127.0.0.1:9200")
        casconn (cas/connect ["127.0.0.1"])]  
    (println "Alo Desafio")))


