package spark2;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.hncy58.spark.dbscan.Dbscan;
import com.hncy58.spark.dbscan.DbscanModel;
import com.hncy58.spark.dbscan.DbscanSettings;
import com.hncy58.spark.dbscan.distance.GEODistance;
import com.hncy58.spark.dbscan.spatial.Point;
import com.hncy58.spark.dbscan.spatial.rdd.PartitioningSettings;

public class DBSCANWithKuduQueryTest {

	// private static String inPath =
	// "C:/dev/workspace/spark2-submit/inf_address_20190516.csv";
	// private static String outPath = "C:/dev/workspace/spark2-submit/out";
	// private static String fileProtocolPrefix = "file:///";
	// private static boolean localMode = true;
	private static String inPath = "/tmp/inf_address_20190516.csv";
	private static String outPath = "/tmp/out";
	private static String fileProtocolPrefix = "hdfs://node01:8020";
	private static boolean localMode = false;

	public static void main(String[] args) {

		System.out.println("HADOOP_HOME ENV => " + System.getenv("HADOOP_HOME"));
		System.out.println("HADOOP_HOME PROP => " + System.getProperty("HADOOP_HOME"));
		System.out.println("hadoop.home.dir ENV => " + System.getenv("hadoop.home.dir"));
		System.out.println("hadoop.home.dir PROP => " + System.getProperty("hadoop.home.dir"));

		System.out.println("DBSCAN algorithm based on spark2 distributed ENV.");
		System.out.printf("Usage: %s inpath outPath fileProtocolPrefix", DBSCANWithKuduQueryTest.class.getSimpleName());
		System.out.println();
		System.out.printf("Usage: %s %s %s %s", DBSCANWithKuduQueryTest.class.getSimpleName(), inPath, outPath, fileProtocolPrefix);
		System.out.println();

		// 接收命令行参数配置
		if (args.length > 0)
			inPath = args[0].trim();
		if (args.length > 1)
			outPath = args[1].trim();
		if (args.length > 2) {
			fileProtocolPrefix = args[2].trim();
			if (!fileProtocolPrefix.startsWith("file:///"))
				localMode = false;
		}

		SparkConf conf = new SparkConf().setAppName("test-dbscan").setMaster("local[6]")
				.set("spark.driver.userClassPathFirst", "true");
		conf.set("spark.sql.crossJoin.enabled", "true");

		SparkContext sc = new SparkContext(conf);

		SparkSession sparkSession = SparkSession.builder().sparkContext(sc).getOrCreate();
		Dataset<Row> ds = sparkSession
				.read()
				.format("org.apache.kudu.spark.kudu")
				.option("kudu.master", "node01:7051")
				.option("kudu.table", "impala::kudu_ods_riskcontrol.inf_address")
				.load();
		
		ds.createOrReplaceTempView("tmp");
		
		sparkSession.sql("select * from tmp").show();
		Function<Row, Point> func = (r) -> {
			return new Point(r.getString(1), new double[] {Double.valueOf(r.getString(r.fieldIndex("lbs_longitude"))), Double.valueOf(r.getString(r.fieldIndex("lbs_latitude"))) });
		};
		JavaRDD<Point> javaRdd = ds.javaRDD().map(func);
		javaRdd.foreach(p -> System.out.println(p));
		RDD<Point> dsPoints = javaRdd.rdd();

		// 聚类算法配置
		DbscanSettings clusteringSettings = new DbscanSettings().withEpsilon(50000000).withNumberOfPoints(2);
		clusteringSettings.withTreatBorderPointsAsNoise(true);
		clusteringSettings.withDistanceMeasure(new GEODistance()); // 修改为GEO距离计算

		long start = System.currentTimeMillis();
		// 训练数据模型
		DbscanModel model = Dbscan.train(dsPoints, clusteringSettings,
				new PartitioningSettings(PartitioningSettings.DefaultNumberOfSplitsAlongEachAxis(),
						PartitioningSettings.DefaultNumberOfLevels(), PartitioningSettings.DefaultNumberOfPointsInBox(),
						PartitioningSettings.DefaultNumberOfSplitsWithinPartition()));
		System.out.println("model trained started, used " + (System.currentTimeMillis() - start)
				+ " ms.========================================");
		
		// 打印结果
		model.allPoints().toJavaRDD().foreach(p -> System.out.println(p.id() + " -> " + p));
		
		sc.stop();
	}
}
