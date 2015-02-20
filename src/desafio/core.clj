(ns desafio.core
  (:require [clojurewerkz.cassaforte.client :as cas]
            [clojurewerkz.cassaforte.cql :as cql]
            [clojurewerkz.cassaforte.query :as casq]
            [clojurewerkz.elastisch.rest :as esr]
            [clojurewerkz.elastisch.rest.index :as esi]
            [clojurewerkz.elastisch.query :as q]
            [clojurewerkz.elastisch.rest.document :as esd]
            [clojurewerkz.elastisch.rest.response :refer [not-found? hits-from]]
            [clj-time.core :as clj-time])
  (:import [org.apache.commons.daemon Daemon DaemonContext])
  (:import java.util.UUID)
  (:gen-class
   :implements [org.apache.commons.daemon.Daemon]))

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;              Elasticsearch
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;


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
      (let [res (cql/select casconn "tweet" (casq/where {:id (UUID/fromString (:_id hit))}))]
        (if (empty? res)
          (do ;; put new cas entry and update es
            (println "new entry on cassandra and update elasticsearch")
            (cql/insert casconn "tweet" {:timeline (:timeline (:_source hit))
                                         :text (:text (:_source hit))
                                         :sync true
                                         :id (UUID/fromString (:_id hit))
                                         :created_at (:created_at (:_source hit))})
            (esd/update-with-partial-doc esconn index-name index-type (:_id hit) {:sync true}))
          (let ;; update entry
              [tes (:created_at (:_source (esGetNotSyncTweets esconn)))
               tcas (.getTime (:created_at (first res)))]
            (println "what the newest?" )
            (if (<= tes tcas)
              (do ;; upgrade cas and es using cas
                (esd/update-with-partial-doc esconn index-name index-type (:_id hit)
                                             {:sync true
                                              :text (:text (first res))
                                              :timeline (:timeline (first res))})
                (cql/update casconn "tweet"
                            {:sync true                             
                             :created_at (:created_at (:_source hit))}
                            (casq/where {:id (UUID/fromString (:_id hit))})))
            (do ;; upgrade cas and es using es
              (esd/update-with-partial-doc esconn index-name index-type (:_id hit) {:sync true})
              (cql/update casconn "tweet"
                          {:timeline (:timeline (:_source hit))
                           :text (:text (:_source hit))
                           :sync true
                           :id (UUID/fromString (:_id hit))
                           :created_at (:created_at (:_source hit))}
                          (casq/where {:id (UUID/fromString (:_id hit))})))))))
      
      
      (recur esconn casconn (rest hits)))))
      

(defn elasticsearch2cassandra [esconn casconn]
  (let [hits (esGetNotSyncTweets esconn)]
    (es2cas esconn casconn hits)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;                Cassandra
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn casGetNotSyncTweets [casconn]
  (cql/select casconn "tweet" (casq/where {:sync false})))

(defn cas2es [esconn casconn hits]
  (if (empty? hits)
    nil
    (let [hit (first hits)]
      ;;process hit
      (println hit)
      (let [res (esd/get esconn index-name index-type (str (:id hit)))]
        (if (nil? res)
          (let ;; put new es entry and update cas
              [timeline (:timeline hit)
               id (:id hit)
               str-id (str (:id hit))
               created (:created_at hit)
               text (:text hit)]
            (println "new entry on elasticsearch and update cassandra")
            (esd/put esconn index-name index-type str-id {:timeline timeline
                                                          :text text
                                                          :created_at (.getTime (java.util.Date.))
                                                          :sync true})
            (cql/update casconn "tweet"
                        {:timeline timeline
                         :text text
                         :sync true
                         :created_at created}
                        (casq/where {:id id})))
          (do ;; update entry
            )))
      (recur esconn casconn (rest hits)))))

(defn cassandra2elasticsearch [esconn casconn]
  (let [hits (casGetNotSyncTweets casconn)]
    (cas2es esconn casconn hits)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  daemon
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(def state (atom {}))

(defn init [args]
  (swap! state assoc :interval (Integer/parseInt (first args)))
  (swap! state assoc :running true))

(defn start []
  (while (:running @state)
    (let [esconn (esr/connect "http://127.0.0.1:9200")
          casconn (cas/connect ["127.0.0.1"])]
      (cql/use-keyspace casconn keyspace-name)
      (println "cassandra2elasticsearch")
      (cassandra2elasticsearch esconn casconn)
      (println "elasticsearch2cassandra")
      (elasticsearch2cassandra esconn casconn)
      (Thread/sleep (:interval @state)))))

(defn stop []
  (swap! state assoc :running false))

;; Daemon implementation

(defn -init [this ^DaemonContext context]
  (init (.getArguments context)))

(defn -start [this]
  (future (start)))

(defn -stop [this]
  (stop))

;; Enable command-line invocation
(defn -main [& args]
  (init args)
  (start))


