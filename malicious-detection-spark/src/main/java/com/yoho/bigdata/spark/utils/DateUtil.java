package com.yoho.bigdata.spark.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Date   2017年1月6日 下午2:27:31 
 * @author bingheng.lu 	 
 */
public class DateUtil {


	public static String FORMATTER_HOUR = "yyyyMMddHH";
	public static String FORMATTER_M = "mm";

	public static String DATETIME_PATTERN = "yyyyMMddHHmm";


	public static String getDate(long time,String pattern){
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf.applyPattern(pattern);
		String date = sdf.format(new Date(time * 1000));
		return date;
	}

	public static String getCurrentDateByFormat(String format){
		Date now = new Date();
		DateFormat simpleFormat = new SimpleDateFormat(format);
		return simpleFormat.format(now);
	}


}

