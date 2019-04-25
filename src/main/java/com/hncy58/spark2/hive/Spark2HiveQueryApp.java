package com.hncy58.spark2.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

public class Spark2HiveQueryApp {

	public static void main(String[] args) {
		
		SparkConf conf = new SparkConf()
				.setAppName("test-tdz")
//				.setMaster("local[*]")
				.set("spark.driver.userClassPathFirst",
				"true");
		conf.set("spark.sql.crossJoin.enabled", "true");
		
		SparkContext sparkContext = new SparkContext(conf);
		
		SparkSession sparkSession = SparkSession
				.builder()
				.sparkContext(sparkContext)
				.enableHiveSupport()
				.getOrCreate();

		String sql = "select * from tb_user_1 limit 10";
		
		if(args.length > 0) {
			sql = args[0];
		}
		
		// 查询表前10条数据
		sparkSession.sql(sql).show();
		sparkSession.stop();
	}

}
