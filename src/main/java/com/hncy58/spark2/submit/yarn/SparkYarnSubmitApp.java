package com.hncy58.spark2.submit.yarn;

import java.io.IOException;
import java.util.HashMap;

import org.apache.spark.launcher.SparkLauncher;

import com.hncy58.spark2.submit.InputStreamReaderRunnable;

public class SparkYarnSubmitApp {

	public static void main(String[] args) {

		System.setProperty("HADOOP_USER_NAME", "hdfs");

		HashMap<String, String> map = new HashMap<String, String>();

		map.put("HADOOP_CONF_DIR", "/etc/hadoop/conf");

		map.put("YARN_CONF_DIR", "/etc/hadoop/conf");

		map.put("SPARK_CONF_DIR", "/etc/spark/conf");

		map.put("SPARK_HOME", "/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark");

		map.put("JAVA_HOME", "/usr/java/jdk1.8.0_141-cloudera");

		try {
			SparkLauncher spark = new SparkLauncher(map)
					.setAppName("spark2-submit-0.1.0.jar")
					.setMaster("yarn")
					.setDeployMode("cluster")
					.setAppResource("./spark2-submit-0.1.0.jar")
					.setMainClass("com.hncy58.spark2.Spark2HiveQueryApp")
					
					.setConf(SparkLauncher.DRIVER_MEMORY, "1g")
					// .setConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH,
					// "/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/jars/")
					// .setConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH,
					// "/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/jars/")
					// .setConf("spark.yarn.jars",
					// "local:/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/jars/*,local:/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/hive/*")

					.setConf("spark.yarn.preserve.staging.files", "true")
					.setConf("spark.sql.session.timeZone", "Asia/Shanghai")
					.setVerbose(true)

					.addFile("./log4j.properties")
					.addAppArgs(args);

			boolean isRun = true;
			int sleepInterval = 5000;
			while (isRun) {
				// 启动spark任务
				System.out.println("启动spark任务");
				Process process = spark.launch();
				InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(
						process.getInputStream(), "input");
				Thread inputThread = new Thread(inputStreamReaderRunnable, "LogStreamReader input");
				inputThread.start();

				InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(
						process.getErrorStream(), "error");
				Thread errorThread = new Thread(errorStreamReaderRunnable, "LogStreamReader error");
				errorThread.start();

				System.out.println("Waiting for finish...");
				int exitCode = process.waitFor();
				System.out.println("Finished! Exit code:" + exitCode);

				System.out.println("Sleep " + sleepInterval + " ms. Start the next launch.");
				Thread.sleep(sleepInterval);
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
