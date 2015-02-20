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
      (is (= (esCountTweets esconn) 3))
      (is (= (casCountTweets casconn) 2)))))


