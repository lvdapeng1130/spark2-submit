package com.hncy58.spark2.hive;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;

public class SQLBean {

	private static final Logger log = LoggerFactory.getLogger(SQLBean.class);

	private static String jsonStr;

	private static List<SQL> sqls;

	private static boolean inited = false;

	public static void init() {
		
		InputStream is = null;
		ByteArrayOutputStream os = null;
		
		try {
			is = SQLBean.class.getClassLoader().getResourceAsStream("sql.json");
			os = new ByteArrayOutputStream();
			byte[] b = new byte[1024];
			int len = 0;
			while ((len = is.read(b)) > 0) {
				os.write(b, 0, len);
			}
			os.flush();

			jsonStr = new String(os.toByteArray());
			sqls = JSONArray.parseArray(jsonStr, SQL.class);
			
			log.info("jsonStr -> \n {}", jsonStr);
			
			inited = true;
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} finally {
			IOUtils.closeStream(os);
			IOUtils.closeStream(is);
		}
	}

	public static List<SQL> getSQLs() {
		
		if (!inited) {
			init();
		}
		
		return sqls;
	}

	public static void main(String[] args) {

		getSQLs().forEach(sql -> {
			log.info(sql.getName());
		});
	}

	static class SQL implements Serializable {

		private String name;
		private String querySql;
		private String deleteSql;
		private String insertSql;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getQuerySql() {
			return querySql;
		}

		public void setQuerySql(String querySql) {
			this.querySql = querySql;
		}

		public String getDeleteSql() {
			return deleteSql;
		}

		public void setDeleteSql(String deleteSql) {
			this.deleteSql = deleteSql;
		}

		public String getInsertSql() {
			return insertSql;
		}

		public void setInsertSql(String insertSql) {
			this.insertSql = insertSql;
		}

	}
}
