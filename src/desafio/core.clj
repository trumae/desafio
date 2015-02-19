(ns desafio.core
  (:require [clojurewerkz.cassaforte.client :as cas]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.query :as casq]
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


(def ^{:const true} keyspace-name "desafio")

(def ^{:const true} casdefinitions
  (casq/column-definitions {:id :UUID
                            :timeline :varchar
                            :text  :varchar
                            :created_at :timestamp
                            :sync :boolean
                            :primary-key [:id]}))


;; Elasticsearch

(defn esCreateIndexDesafio [esconn]
  (esi/create esconn index-name :mappings mapping-types :settings {"number_of_shards" 1}))

(defn esDeleteIndexDesafio[esconn]
  (esi/delete esconn index-name))

(defn esPutRandomTweets [esconn n]
  (if (pos? n)
    (do
      (esd/put esconn index-name index-type (str (UUID/randomUUID)) {:timeline "vvmaciel"
                                                                     :text (str "texto orig es " n)
                                                                     :sync false})
      (recur esconn (dec n)))))

(defn esCountTweets [esconn]
  (:count (esd/count esconn index-name index-type)))

(defn esDeleteAllTweets[esconn]
  (esd/delete-by-query esconn index-name index-type (q/match-all)))

(defn fetch-scroll-results [conn scroll-id results]
  (let [scroll-response (esd/scroll conn scroll-id :scroll "1m")
        hits            (hits-from scroll-response)]
    (if (seq hits)
      (recur conn (:_scroll_id scroll-response) (concat results hits))
      (concat results hits))))

(defn esGetNotSyncTweets [esconn]
  (let [response     (esd/search esconn index-name index-type
                                 :query (q/term :sync false)
                                 :search_type "query_then_fetch"
                                 :scroll "1m"
                                 :size 10)
        initial-hits (hits-from response)
        scroll-id    (:_scroll_id response)
        all-hits     (fetch-scroll-results esconn scroll-id initial-hits)]
    all-hits))

(defn es2cas [esconn casconn hits]
  (if (empty? hits)
    nil
    (let [hit (first hits)]
      ;;process hit
      (println hit)
      (recur esconn casconn (rest hits)))))
      

(defn elasticsearch2cassandra [esconn casconn]
  (let [hits (esGetNotSyncTweets esconn)]
    (es2cas esconn casconn hits)))


;; Cassandra
(defn casCreateKeyspaceDesafio [casconn]
  (cql/create-keyspace  casconn
                        keyspace-name
                        (casq/with {:replication
                                    {:class "SimpleStrategy"
                                     :replication_factor 2 }})))

(defn casCreateTableTweet [casconn]
  (cql/use-keyspace casconn keyspace-name)
  (cql/create-table casconn "tweet" casdefinitions)
  (cql/create-index casconn "tweet" "sync")
  (cql/create-index casconn "tweet" "created_at"))

(defn casDropTableTweet [casconn]
  (cql/use-keyspace casconn keyspace-name)
  (cql/drop-table casconn "tweet"))


(defn casDeleteKeyspaceDesafio [casconn]
  (cql/drop-keyspace casconn keyspace-name))


(defn casPutRandomTweets [casconn n]
  (if (pos? n)
    (do
      (cql/insert casconn "tweet" {:timeline "vvmaciel"
                                   :text (str "texto orig cas" n)
                                   :sync false :id (UUID/randomUUID)
                                   :created_at (.getTime (java.util.Date.))})
      (recur casconn (dec n)))))

(defn casGetNotSyncTweets [casconn]
  (cql/select casconn "tweet" (casq/where {:sync false})))

(defn cas2es [esconn casconn hits]
  (if (empty? hits)
    nil
    (let [hit (first hits)]
      ;;process hit
      (println hit)
      (recur esconn casconn (rest hits)))))

(defn cassandra2elasticsearch [esconn casconn]
  (let [hits (casGetNotSyncTweets casconn)]
    (cas2es esconn casconn hits)))

;; Enable command-line invocation
(defn -main [& args]
  (let [esconn (esr/connect "http://127.0.0.1:9200")
        casconn (cas/connect ["127.0.0.1"])]
    (cql/use-keyspace casconn keyspace-name)
    (cassandra2elasticsearch esconn casconn)
    (elasticsearch2cassandra esconn casconn)))


