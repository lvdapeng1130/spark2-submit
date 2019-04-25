#!/usr/bin/env bash

# 删除hdfs旧文件
hdfs dfs -rm /tmp/spark2-submit-0.1.0.jar
# 上传最新的jar文件
hdfs dfs -put spark2-submit-0.1.0.jar /tmp

# 上面两步也可以使用本地文件方式。代码如下：（目前是这样）
# .setAppResource("./spark2-submit-0.1.0.jar")

# 启动java开始提交spark任务
nohup java -cp .:lib/*:spark2-submit-0.1.0.jar com.hncy58.spark2.submit.SparkSubmitApp "select * from default.customer_all_attrs LIMIT 100" > spark-submit.out 2>&1 &
