package com.hncy58.spark2.submit.yarn;
 
import java.io.IOException;
 
public class RunSpark {
	
	public static void main(String[] args) throws IllegalArgumentException, IOException {
		
		String[] inputArgs= new String[]{
				"SELECT * FROM default.customer_all_attrs LIMIT 10"
		};
		
		String[] runArgs=new String[]{
//                "--name","ALS Model Train ",
                "--class","com.hncy58.spark2.Spark2HiveQueryApp",
                //@TODO 此参数在测试时使用，否则应注释
//                "--driver-memory","512m",
//                "--num-executors", "1",
//                "--executor-memory", "512m",
                "--jar","hdfs://node01:8020/tmp/spark2-submit-0.1.0.jar",//
                //// Spark 在子节点运行driver时，只读取spark-assembly-1.4.1-hadoop2.6.0.jar中的配置文件；
//                "--files","hdfs://node01:8020/tmp/yarn-site.xml",
                "--arg",inputArgs[0]
        };
		
		Utils.runSpark(runArgs);
	}
	
	
}
