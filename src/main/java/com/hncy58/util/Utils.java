package com.hncy58.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 工具类
 * 
 * @author tokings
 * @company hncy58 湖南长银五八
 * @website http://www.hncy58.com
 * @version 1.0
 * @date 2018年10月31日 下午3:50:58
 *
 */
public class Utils {
	
	private static SimpleDateFormat sdf;

	public static boolean isEmpty(String str) {
		return str == null || "".equals(str.trim());
	}

	public static boolean isEmpty(Object obj) {
		return obj == null || "".equals(obj.toString().trim());
	}
	
	public static boolean toBoolean(Object obj) {
		return obj == null ? null : Boolean.valueOf(obj.toString().trim());
	}
	
	public static String toString(Object obj) {
		return obj == null ? null : obj.toString().trim();
	}
	
	public static int toInt(Object obj) {
		return obj == null ? null : Integer.valueOf(obj.toString().trim());
	}
	
	public static long toLong(Object obj) {
		return obj == null ? null : Long.valueOf(obj.toString().trim());
	}
	
	public static float toFloat(Object obj) {
		return obj == null ? null : Float.valueOf(obj.toString().trim());
	}
	
	public static double toDouble(Object obj) {
		return obj == null ? null : Double.valueOf(obj.toString().trim());
	}
	
	public static Date toDate(Object obj,String pattern) {
		sdf.applyPattern(pattern);
		try {
			return obj == null ? null : sdf.parse(toString(obj));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
		return null;
	}

}
