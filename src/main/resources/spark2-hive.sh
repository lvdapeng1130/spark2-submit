#!/bin/bash

source ~/.bash_profile

jar=/root/spark-test/spark2-kudu-0.0.1-SNAPSHOT-jar-with-dependencies.jar

mainClass=com.hncy58.spark2_kudu.Spark2HiveApp
sql='select * from default.merge_test limit 1000'

echo "start SparkSQL Job Shell ->"

nohup spark2-submit \
        --master yarn \
        --deploy-mode client \
        --name $1 \
        --num-executors 4 \
        --executor-memory 4G \
        --queue sparksql \
        --driver-memory 4G \
        --conf spark.ui.port=5022 \
        --class $mainClass $jar "$sql" &> $2 &


echo "Finished SparkSQL Job Shell -->|||"