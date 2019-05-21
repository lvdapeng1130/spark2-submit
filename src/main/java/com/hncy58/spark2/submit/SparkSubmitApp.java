package com.hncy58.spark2.submit;

import java.io.IOException;
import java.util.HashMap;

import org.apache.spark.launcher.SparkLauncher;

/**
 * 应用程序内部提交Spark任务
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2019年5月21日 上午11:43:36
 *
 */
public class SparkSubmitApp {
	/**
	 * 是否启动
	 */
	private static boolean isRun = true;
	/**
	 * 重复部署应用程序间隔（毫秒）
	 */
	private static int sleepInterval = 5000;

	public static void main(String[] args) {

		String execUser = "hdfs";
		String appRes = "./spark2-submit-0.1.0.jar";
		String mainClass = "com.hncy58.spark2.dbscan.DBSCANCluster";
		String deployMode = "cluster";
		String master = "yarn";
		String appName = SparkSubmitApp.class.getSimpleName();
		
		if(args.length > 0) {
			mainClass = args[0].trim();
		}
		
		System.out.println("main class:" + mainClass);
		
		// 设置Spark执行器执行用户
		System.setProperty("HADOOP_USER_NAME", execUser);

		HashMap<String, String> map = new HashMap<String, String>();

		// 通过单独的spark.env文件进行定义
		// map.put("HADOOP_CONF_DIR", "/etc/hadoop/conf");
		// map.put("YARN_CONF_DIR", "/etc/hadoop/conf");
		// map.put("SPARK_CONF_DIR", "/etc/spark/conf");
		// map.put("SPARK_HOME",
		// "/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark");
		// map.put("JAVA_HOME", "/usr/java/jdk1.8.0_141-cloudera");

		// 设置执行器任务执行用户
		map.put("HADOOP_USER_NAME", "tokings");
		
		try {
			SparkLauncher spark = new SparkLauncher(map)
					// 设置应用程序名称
					.setAppName(appName)
					// 设置部署模式
					.setMaster(master)
					// 设置执行模式
					.setDeployMode(deployMode)
					// 设置主应用程序jar路径
					.setAppResource(appRes)
					// 设置主应用程序执行类
					.setMainClass(mainClass)
					// 设置驱动器执行内存
					.setConf(SparkLauncher.DRIVER_MEMORY, "1g")

					// 可不配置（在安装了spark的节点执行时）
					// .setConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH,
					// "/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/jars/")
					// .setConf(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH,
					// "/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/jars/")
					// .setConf("spark.yarn.jars",
					// "local:/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/jars/*,local:/opt/cloudera/parcels/CDH-6.0.1-1.cdh6.0.1.p0.590678/lib/spark/hive/*")

					.setConf("spark.yarn.preserve.staging.files", "true")
					.setConf("spark.sql.session.timeZone", "Asia/Shanghai").setVerbose(true)

					// 添加部署应用所需的依赖jar
					.addJar("./scala-dbscan-0.0.1.jar")
					.addJar("./kudu-spark2_2.11-1.6.0-cdh6.0.1.jar")
					// 添加部署应用所需的相关文件
					.addFile("./log4j.properties")
					// 添加主应用程序参数
					.addAppArgs(args);

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
