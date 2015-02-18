(ns desafio.core
  (:require [clojurewerkz.cassaforte.client :as cas]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.query :as q]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clj-time.core :as clj-time]
            )
  (:import java.util.UUID)
  (:gen-class))


(def ^:dynamic esconn nil)
(def ^:dynamic casconn nil)

;;(def ^:dynamic esconn (esr/connect "http://127.0.0.1:9200"))
;;(def ^:dynamic casconn (cas/connect ["127.0.0.1"]))

(def ^{:const true} index-name "desafio")
(def ^{:const true} index-type "tweet")

(def mapping-types {"tweet"
                    {:properties {:timeline {:type "string"
                                             :store "yes"}
                                  :text {:type "string"
                                         :store "yes"}
                                  :sync  {:type "boolean" :default false :boost 10.0 :include_in_all false}
                                  :created_at {:type "date" :format "date_hour_minute_second"}}}})

;; Elasticsearch

(defn esCreateIndexDesafio []
  (esi/create esconn "desafio" :mappings mapping-types :settings {"number_of_shards" 1}))

(defn putRandomTweets [n]
  (if (pos? n)
    (do
      (esd/put esconn index-name index-type (str (UUID/randomUUID)) {:timeline "vvmaciel" :text (str "texto " n)})
      (recur (dec n)))))

(defn countTweets []
  (:count (esd/count esconn index-name index-type)))

(defn deleteAllTweets[]
  (esd/delete-by-query esconn index-name index-type (q/match-all)))


;; Cassandra





  ;; Enable command-line invocation
(defn -main [& args]
  (println "Alo Desafio"))
