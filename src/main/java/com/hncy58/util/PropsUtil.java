package com.hncy58.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hncy58.ds.DSPoolUtil;

public class PropsUtil {
	
	private static final Logger log = LoggerFactory.getLogger(PropsUtil.class);
	private static final Properties props = new Properties();
	
	static {
		InputStream is = null;
		try {
			is = DSPoolUtil.class.getClassLoader().getResourceAsStream("app.properties");
			props.load(is);
			props.forEach((k, v) -> System.out.println(k + ":" + v));
		} catch (IOException e) {
			log.error(e.getMessage(), e);
		} finally {
			IOUtils.closeStream(is);
		}
	}
	
	public static String getWithDefault(String prefix, String key, String defaultValue) {
		return props.getProperty(prefix + "." + key, defaultValue);
	}
	
	public static String get(String prefix, String key) {
		return props.getProperty(prefix + "." + key);
	}
	
	public static String getWithDefault(String key, String defaultValue) {
		return props.getProperty(key, defaultValue);
	}
	
	public static String get(String key) {
		return props.getProperty(key);
	}
}
