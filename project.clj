(defproject desafio "0.1.0-SNAPSHOT"
  :description "Sincroniza indice cassandra e elasticsearch"
  :url "http://github.com/trumae/desafio"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [clojurewerkz/elastisch "2.1.0"]
                 [clojurewerkz/cassaforte "2.0.0"]
                 [org.apache.commons/commons-daemon "1.0.9"]
                 [clj-time "0.9.0"]
                 [danlentz/clj-uuid "0.1.2-SNAPSHOT"]
                ]
                
  :jvm-opts [;;"-server"
             "-zero"
             "-Xmx256m"
             "-Dlog4j.configuration=log4j.properties.unit"]
  ;;:global-vars {*warn-on-reflection* true}
  ;;:pedantic :warn
  :main desafio.core
  :aot :all)
