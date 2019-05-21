package com.hncy58.spark2.dbscan;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;

import com.hncy58.spark.dbscan.Dbscan;
import com.hncy58.spark.dbscan.DbscanModel;
import com.hncy58.spark.dbscan.DbscanSettings;
import com.hncy58.spark.dbscan.distance.GEODistance;
import com.hncy58.spark.dbscan.spatial.Point;
import com.hncy58.spark.dbscan.spatial.rdd.PartitioningSettings;
import com.hncy58.spark.dbscan.util.io.IOHelper;

import scala.Tuple2;

public class DBSCANCluster {

	// private static String inPath =
	// "C:/dev/workspace/spark2-submit/inf_address_20190516.csv";
	// private static String outPath = "C:/dev/workspace/spark2-submit/out";
	// private static String fileProtocolPrefix = "file:///";
	private static String inPath = "/tmp/inf_address_20190516.csv";
	private static String outPath = "/tmp/out";
	private static String fileProtocolPrefix = "hdfs://node01:8020";
	private static SparkContext sc;

	public static void main(String[] args) {

		System.out.println("DBSCAN algorithm based on spark2 distributed ENV.");
		System.out.printf("Usage: %s inpath outPath fileProtocolPrefix", DBSCANCluster.class.getSimpleName());
		System.out.println();
		System.out.printf("Usage: %s %s %s %s", DBSCANCluster.class.getSimpleName(), inPath, outPath,
				fileProtocolPrefix);
		System.out.println();

		// 接收命令行参数配置
		if (args.length > 0)
			inPath = args[0].trim();
		if (args.length > 1)
			outPath = args[1].trim();

		SparkConf conf = new SparkConf()
				.setAppName("test-dbscan")
//				.setMaster("local[6]")
				.set("spark.driver.userClassPathFirst", "true")
				.set("spark.sql.crossJoin.enabled", "true");

		sc = new SparkContext(conf);

		for (Tuple2<String, String> t : conf.getAll()) {
			System.out.println(t._1 + ":" + t._2);
		}

		System.out.println("HADOOP_HOME ENV => " + System.getenv("HADOOP_HOME"));
		System.out.println("HADOOP_HOME PROP => " + System.getProperty("HADOOP_HOME"));
		System.out.println("hadoop.home.dir ENV => " + System.getenv("hadoop.home.dir"));
		System.out.println("hadoop.home.dir PROP => " + System.getProperty("hadoop.home.dir"));

		// 读取待聚类数据
		RDD<Point> data = IOHelper.readDatasetWithInitId(sc, fileProtocolPrefix + inPath);

		// 聚类算法配置
		DbscanSettings clusteringSettings = new DbscanSettings().withEpsilon(500).withNumberOfPoints(3);
		clusteringSettings.withTreatBorderPointsAsNoise(true);
		clusteringSettings.withDistanceMeasure(new GEODistance()); // 修改为GEO距离计算

		long start = System.currentTimeMillis();
		// 训练数据模型
		DbscanModel model = Dbscan.train(data, clusteringSettings,
				new PartitioningSettings(PartitioningSettings.DefaultNumberOfSplitsAlongEachAxis(),
						PartitioningSettings.DefaultNumberOfLevels(), PartitioningSettings.DefaultNumberOfPointsInBox(),
						PartitioningSettings.DefaultNumberOfSplitsWithinPartition()));
		System.out.println("model trained started, used " + (System.currentTimeMillis() - start)
				+ " ms.========================================");
		
		// 删除输出目录
		IOHelper.deleteOutPath(sc, fileProtocolPrefix, outPath);

		start = System.currentTimeMillis();
		IOHelper.saveClusteringResult(model, fileProtocolPrefix + outPath);
		System.out.println("model saved, used " + (System.currentTimeMillis() - start)
				+ " ms.========================================");
		start = System.currentTimeMillis();
		sc.stop();
		System.out.println("sparkcontext stoped, used " + (System.currentTimeMillis() - start)
				+ " ms.========================================");
	}
}
