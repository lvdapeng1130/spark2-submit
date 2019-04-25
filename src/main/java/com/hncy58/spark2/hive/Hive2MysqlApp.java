package com.hncy58.spark2.hive;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.ds.DSPoolUtil;
import com.hncy58.spark2.hive.SQLBean.SQL;

/**
 * 
 * 测试执行spark sql语句将结果写入Mysql数据库
 * 
 * <br>
 * 注意：需要上传到服务器才能执行成功。
 * 
 * @author tdz
 * @company hncy58 长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2019年1月18日 下午4:55:47
 */
public class Hive2MysqlApp {

	private static final Logger log = LoggerFactory.getLogger(Hive2MysqlApp.class);

	private static SparkConf conf;
	private static SparkContext sparkContext;
	private static SparkSession sparkSession;
	private static int batchSize = 1000;
	
	private transient static int retCode = 0;

	public static void main(String[] args) {
		searchBysparkSql(args);
	}

	public static void searchBysparkSql(String[] args) {

		long start = System.currentTimeMillis();
		try {
			conf = new SparkConf().setAppName("test-hive-to-mysql")
//					.setMaster("local[*]")
					.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
					.set("spark.driver.userClassPathFirst", "true").set("spark.sql.crossJoin.enabled", "true");
			sparkContext = new SparkContext(conf);
			sparkSession = SparkSession.builder().sparkContext(sparkContext).enableHiveSupport().getOrCreate();

			if (args.length > 0) {
				batchSize = Integer.valueOf(args[1].trim());
			}

			List<SQL> sqls = SQLBean.getSQLs();

			if (sqls != null && !sqls.isEmpty()) {

				sqls.forEach(sql -> {
					executeSQL(sql, batchSize);
				});
			} else {
				log.error("sqls is empty.");
			}

			log.info("Total used " + (System.currentTimeMillis() - start) + "ms.");
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			retCode = 1;
		} finally {
			if (sparkSession != null) {
				log.info("start to stop sparkSession...");
				sparkSession.stop();
			}
		}
		
		// 如果执行过程中有异常情况，则直接强制退出JVM进程。
		if(retCode != 0) {
			log.error("异常结束。");
			System.exit(retCode);
		} else {
			log.error("正常结束。");
		}
	}

	public static void executeSQL(SQL sql, int batchSize) {

		if (sql.getQuerySql() == null || "".equals(sql.getQuerySql().trim())) {
			log.error("query sql is empty.");
			return;
		}

		log.info("{} query sql is -> {}", sql.getName(), sql.getQuerySql());

		Dataset<Row> ds = sparkSession.sql(sql.getQuerySql());

		ds.foreachPartition(it -> {

			List<List<Object>> listData = new ArrayList<List<Object>>();
			while (it.hasNext()) {
				Row row = it.next();
				StructField[] fields = row.schema().fields();
				List<Object> rowList = new ArrayList<Object>();
				log.info("schema -> {}", row.schema());
				
				for (int i = 0; i < fields.length; i++) {
					rowList.add(row.getAs(fields[i].name()));
				}
				listData.add(rowList);

				if (listData.size() >= batchSize) {
					try {
						log.info("size:" + listData.size());
						commitBatch(listData, sql);
					} catch (Exception e) {
						log.error(e.getMessage(), e);
						retCode = 1;
						throw e;
					} finally {
						listData.clear();
					}
				}
			}

			if (!listData.isEmpty()) {
				try {
					log.info("size:" + listData.size());
					commitBatch(listData, sql);
				} catch (Exception e) {
					log.error(e.getMessage(), e);
					retCode = 1;
					throw e;
				} finally {
					listData.clear();
				}
			}
		});
	}

	private static void commitBatch(List<List<Object>> list, SQL sql) throws Exception {

		Connection conn = null;
		PreparedStatement ps = null;
		Statement stmt = null;

		try {
			conn = DSPoolUtil.getConnection();

			if (sql.getDeleteSql() != null && !"".equals(sql.getDeleteSql().trim())) {
				stmt = conn.createStatement();
				log.info("{} delete sql is -> {}", sql.getName(), sql.getDeleteSql());
				int ret = stmt.executeUpdate(sql.getDeleteSql());
				log.info("delete rows -> {}", ret);
			}

			if (sql.getInsertSql() == null || "".equals(sql.getInsertSql().trim())) {
				log.error("insert sql is empty.");
				return;
			}

			log.info("{} insert sql is -> {}", sql.getName(), sql.getInsertSql());
			boolean autoCommit = conn.getAutoCommit();
			ps = conn.prepareStatement(sql.getInsertSql());

			for (List<Object> params : list) {
				for (int i = 0; i < params.size(); i++) {
					ps.setObject(i + 1, params.get(i));
				}
				ps.addBatch();
			}

			conn.setAutoCommit(false);
			int[] rets = ps.executeBatch();
			conn.commit();
			conn.setAutoCommit(autoCommit);
			log.info("insert rows -> {}", rets.length);
		} catch (Exception e) {
			conn.rollback();
			throw e;
		} finally {
			release(null, stmt, null);
			release(conn, ps, null);
		}

	}

	private static void release(Connection conn, Statement stmt, ResultSet rs) throws SQLException {

		if (conn != null) {
			conn.close();
		}

		if (stmt != null) {
			stmt.close();
		}

		if (rs != null) {
			rs.close();
		}
	}
}
