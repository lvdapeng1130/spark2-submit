package com.hncy58.spark2.submit.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.apache.spark.launcher.SparkLauncher;

public class Utils {
	private static Configuration configuration = null;

	public static Configuration getConf() {
		if (configuration == null) {

			configuration = new Configuration();

			// configuration.setBoolean("mapreduce.app-submission.cross-platform",
			// true);// 配置使用跨平台提交任务
			// configuration.set("fs.defaultFS", "hdfs://node01:8020");//
			// 指定namenode
			// configuration.set("mapreduce.framework.name", "yarn"); //
			// 指定使用yarn框架
			// configuration.set("yarn.resourcemanager.address", "node01:8032");
			// // 指定resourcemanager
			// configuration.set("yarn.resourcemanager.scheduler.address",
			// "node01:8030");// 指定资源分配器
			// configuration.set("mapreduce.jobhistory.address",
			// "http://node01:18088");// 指定historyserver
			// configuration.set("spark.eventLog.dir",
			// "hdfs://node01:8020/user/spark/applicationHistory");//
			// 指定historyserver
		}

		return configuration;
	}

	/**
	 * 调用Spark
	 * 
	 * @param args
	 * @return
	 */
	public static boolean runSpark(String[] args) {
		try {
			System.setProperty("SPARK_YARN_MODE", "true");
			SparkConf sparkConf = new SparkConf();
			sparkConf.set("spark.yarn.jars",
					"local:/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/jars/*,local:/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/hive/*");
			sparkConf.set("spark.yarn.scheduler.heartbeat.interval-ms", "1000");
//			sparkConf.set("spark.yarn.dist.files", "hdfs://node01:8020/tmp/yarn-site.xml");

//			sparkConf.set(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH,
//					"/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/jars/");
//			sparkConf.set(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH,
//					"/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/jars/");

			sparkConf.set("spark.submit.deployMode", "cluster");
			sparkConf.setMaster("yarn");

			ClientArguments cArgs = new ClientArguments(args);

			Client client = new Client(cArgs, getConf(), sparkConf);
			client.run();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
}
