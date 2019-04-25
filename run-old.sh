#!/usr/bin/env bash

MASTER=yarn
DEPLOY_MODE=cluster
#DEPLOY_MODE=client
MAIN_NAME=TxnPlteLogStream
MAIN_CLASS=com.hisun.loganalysis.spark.kafka.TxnPlteDataApp
PROJECT=loganalysis.spark
VERSION=0.0.1-SNAPSHOT

#REDIS_HOST=172.16.49.79
#REDIS_PORT=6379
#REDIS_EXPIREDDATA_CLEAR_KEYS=TXNPLTE_TOTAL_TOP10_BY_TXNCOD,TXNPLTE_DURATION_AVG_TOP10_BY_TXNCOD,TXNPLTE_ERROR_RATIO_TOP10_BY_TXNCOD,TXNPLTE_OVER_VIEW
# 单位
#REDIS_CLEANTASK_EXPIRESECONGDS=43200
# 单位毫秒
#REDIS_CLEANTASK_SLEEPINTERVAL=3600000

REDIS_SERVERTYPE=1
REDIS_MASTERNAME=redis-master
REDIS_SERVERS=172.16.49.79:6379
REDIS_PASSWORD=test

# 单位
BATCH=300

#KAFKA_SERVERS=172.16.49.88:9092,172.16.49.89:9092
#KAFKA_ZK_SERVERS=data1:2181,data2:2181,data3:2181/kafka
#KAFKA_TOPICS=spark-sql
KAFKA_SERVERS=172.16.49.79:9092,172.16.49.78:9092,172.16.49.77:9092
KAFKA_ZK_SERVERS=172.16.49.77:2182,172.16.49.78:2182,172.16.49.79:2182
KAFKA_TOPICS=txnplte
KAFKA_GROUP=sparkSQL-kafka

SQL_INVOKER_PACKAGES=com.hisun.loganalysis.spark.kafka.sql.sqlinvoker
SQL_INVOKER_METHOD_IDS=txnPlte/overview,txnPlte/regionOverview,txnPlte/regionNodeOverview,txnPlte/regionNodeTxnCodOverview,txnPlte/regionNodeTxnCodMsgCdOverview,txnPlte/nodeOverview,txnPlte/txnCodOverview,txnPlte/totalTop10ByTxnCod,txnPlte/errorRatioTop10ByTxnCod,txnPlte/scmErrorRatioTop10ByTxnCod,txnPlte/avgDurationTop10ByTxnCod

cd `dirname $0`

MAIN_JAR="./lib/$PROJECT-$VERSION.jar"
PATH_LIB=./lib
JARS=`ls $PATH_LIB/*.jar | head -1`

for jar in `ls $PATH_LIB/*.jar | grep -v $PROJECT | grep -v $JARS`
do
  JARS="$JARS,""$jar"
done

set -x

appId=`yarn application -list | grep $MAIN_NAME | awk '{print $1}'`
for id in $appId
do
   echo "kill app "$id
   yarn application -kill $id
done

nohup ~/spark2/bin/spark-submit \
--name $MAIN_NAME \
--class $MAIN_CLASS \
--master $MASTER     \
--files ./log4j.properties  \
--driver-java-options "-Dappname=$MAIN_NAME -Dmaster=$MASTER -Dredis.serverType=$REDIS_SERVERTYPE -Dredis.servers=$REDIS_SERVERS -Dredis.masterName=$REDIS_MASTERNAME -Dredis.password=$REDIS_PASSWORD -Dbatch=$BATCH -Dkafka.zkQuorum=$KAFKA_ZK_SERVERS -Dkafka.servers=$KAFKA_SERVERS -Dkafka.topics=$KAFKA_TOPICS -Dkafka.group=$KAFKA_GROUP -Dsql.invoker.packages=$SQL_INVOKER_PACKAGES -Dsql.invoker.method.ids=$SQL_INVOKER_METHOD_IDS -Dlog4j.configuration=./log4j.properties" \
--conf "spark.executor.extraJavaOptions=-Dappname=$MAIN_NAME -Dlog4j.configuration=./log4j.properties"   \
--conf "spark.default.parallelism=36"   \
--conf "spark.streaming.concurrentJobs=10"   \
--conf "spark.sql.shuffle.partitions=100"   \
--conf "spark.yarn.executor.memoryOverhead=1024"   \
--conf "spark.streaming.blockInterval=10000ms"   \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"   \
--conf "spark.scheduler.mode=FAIR"   \
--deploy-mode $DEPLOY_MODE     \
--driver-memory 1g     \
--num-executors 2 \
--executor-cores 4     \
--executor-memory 4g     \
--queue default     \
--jars $JARS    $MAIN_JAR 1>out.log 2>err.log &