/usr/bin/jsvc -java-home "/usr/lib/jvm/java-7-openjdk-amd64" \
     -cp "$(pwd)/target/desafio-0.1.0-SNAPSHOT-standalone.jar" \
     -pidfile "$(pwd)/desafio.pid" \
     -outfile "$(pwd)/desafio.out" \
     -errfile "$(pwd)/desafio.err" \
     -debug \
     desafio.core \
     "5000"
