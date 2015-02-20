(ns desafio.core-test
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
  (:require [clojure.test :refer :all]
            [desafio.core :refer :all]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; elasticsearch
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn esCreateIndexDesafio [esconn]
  (esi/create esconn index-name :mappings mapping-types :settings {"number_of_shards" 1}))

(defn esDeleteIndexDesafio[esconn]
  (esi/delete esconn index-name))

(defn esPutRandomTweets [esconn n]
  (if (pos? n)
    (do
      (esd/put esconn index-name index-type (str (UUID/randomUUID)) {:timeline "vvmaciel"
                                                                     :text (str "texto orig es " n)
                                                                     :created_at (.getTime (java.util.Date.))
                                                                     :sync false})
      (recur esconn (dec n)))))

(defn esCountTweets [esconn]
  (:count (esd/count esconn index-name index-type)))

(defn esDeleteAllTweets[esconn]
  (esd/delete-by-query esconn index-name index-type (q/match-all)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;    cassandra
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

(defn casCountTweets [casconn]
  (count (cql/select casconn "tweet")))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;          general
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn resetDBs [esconn casconn]
  (esDeleteIndexDesafio esconn)
  (esCreateIndexDesafio esconn)
  (casDropTableTweet casconn)
  (casCreateTableTweet casconn))

(defn data3es2cas [esconn casconn]
  (resetDBs esconn casconn)
  (esPutRandomTweets esconn 3)
  (casPutRandomTweets casconn 2))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;          tests
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(deftest resetDB-test
  (testing "resetDB test"
    (let [esconn (esr/connect "http://127.0.0.1:9200")
          casconn (cas/connect ["127.0.0.1"])]
      (resetDBs esconn casconn)
      (is (= (esCountTweets esconn) 0))
      (is (= (casCountTweets casconn) 0)))))


(deftest data-test
  (testing "data test"
    (let [esconn (esr/connect "http://127.0.0.1:9200")
          casconn (cas/connect ["127.0.0.1"])]
      (data3es2cas esconn casconn)
      (Thread/sleep 1000)
      (is (= (esCountTweets esconn) 3))
      (is (= (casCountTweets casconn) 2)))))


(deftest simple-sync-test-1
  (testing "simple sync tests 1"
    (let [esconn (esr/connect "http://127.0.0.1:9200")
          casconn (cas/connect ["127.0.0.1"])]
      (data3es2cas esconn casconn)
      (Thread/sleep 1000)
      (is (= (esCountTweets esconn) 3))
      (is (= (casCountTweets casconn) 2))
      (cassandra2elasticsearch esconn casconn)
      (Thread/sleep 1000)
      (is (= (esCountTweets esconn) 5))
      (is (= (casCountTweets casconn) 2))
      (elasticsearch2cassandra esconn casconn)
      (Thread/sleep 1000)
      (is (= (esCountTweets esconn) 5))
      (is (= (casCountTweets casconn) 5)))))

(deftest simple-sync-test-2
  (testing "simple sync tests 2"
    (let [esconn (esr/connect "http://127.0.0.1:9200")
          casconn (cas/connect ["127.0.0.1"])]
      (data3es2cas esconn casconn)
      (Thread/sleep 1000)
      (is (= (esCountTweets esconn) 3))
      (is (= (casCountTweets casconn) 2))
      (elasticsearch2cassandra esconn casconn)      
      (Thread/sleep 1000)
      (is (= (esCountTweets esconn) 3))
      (is (= (casCountTweets casconn) 5))
      (cassandra2elasticsearch esconn casconn)
      (Thread/sleep 1000)
      (is (= (esCountTweets esconn) 5))
      (is (= (casCountTweets casconn) 5)))))
