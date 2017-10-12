#!/bin/sh

while [ $(curl -s -o /dev/null -w "%{http_code}" spark-master:8080) -ne 200 ]
do
    echo "Waiting for spark-master"
    sleep 2
done

export SPARK_SUBMIT_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=4000

/spark-master/spark/bin/spark-submit \
    --files /src/main/resources/log4j.properties \
    --conf 'spark.executor.extraJavaOptions=-Dlog4j.configuration=file:///src/main/resources/log4j.properties' \
    --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///src/main/resources/log4j.properties' \
    --class "$MAIN_CLASS" \
    --master spark://spark-master:7077 \
    --jars $(jarFiles=(/usr/build/libs/*.jar); IFS=,; echo "${jarFiles[*]}")  \
    /usr/build/libs/spark-spikes-1.0S.jar
