package com.example.elasticsearch.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class DateUtil {

	private static Logger log = Logger.getLogger(DateUtil.class);

	private DateUtil() {

	}

	public static final String DEFAULT_DATE_PATTERN = "yyyy-MM-dd HH:mm:ss";

	public static final String Year_Mouth = "yyyy-MM";

	public static final String Date_Format = "MM-dd";

	public static final String HH_MM = "HH:mm";

	public static final int YEAR_RETURN = 0;

	public static final int MONTH_RETURN = 1;

	public static final int DAY_RETURN = 2;

	public static final int HOUR_RETURN = 3;

	public static final int MINUTE_RETURN = 4;

	public static final int SECOND_RETURN = 5;

	public static final String FIRST_TIME = "0000-01-01 00:00:00";

	/**
	 * 末年时间 9999-12-31 23:59:59
	 */
	public static final String LAST_TIME = "9999-12-31 23:59:59";

	public static Date stringToDate(String dateTime, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date date = null;
		try {
			date = sdf.parse(dateTime);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}
    
	/**
	 * 比对时间
	 * @param startTime
	 * @param endTime
	 * @return
	 */
	public static boolean compareTime(Date startTime, Date endTime) {
		if (startTime.getTime() > endTime.getTime()){
			return true;
		} else {
			return false;
		}
	}

	public static String format(Date date) {
		SimpleDateFormat formatTool = new SimpleDateFormat();
		formatTool.applyPattern(DEFAULT_DATE_PATTERN);
		return formatTool.format(date);
	}

	public static long getBetween(String beginTime, String endTime, String formatPattern, int returnPattern)
			throws ParseException {
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat(formatPattern);
		Date beginDate = simpleDateFormat.parse(beginTime);
		Date endDate = simpleDateFormat.parse(endTime);

		Calendar beginCalendar = Calendar.getInstance();
		Calendar endCalendar = Calendar.getInstance();
		beginCalendar.setTime(beginDate);
		endCalendar.setTime(endDate);
		switch (returnPattern) {
		case YEAR_RETURN:
			return getByField(beginCalendar, endCalendar, Calendar.YEAR);
		case MONTH_RETURN:
			return getByField(beginCalendar, endCalendar, Calendar.YEAR) * 12
					+ getByField(beginCalendar, endCalendar, Calendar.MONTH);
		case DAY_RETURN:
			return getTime(beginDate, endDate) / (24 * 60 * 60 * 1000);
		case HOUR_RETURN:
			return getTime(beginDate, endDate) / (60 * 60 * 1000);
		case MINUTE_RETURN:
			return getTime(beginDate, endDate) / (60 * 1000);
		case SECOND_RETURN:
			return getTime(beginDate, endDate) / 1000;
		default:
			return 0;
		}
	}

	private static long getByField(Calendar beginCalendar, Calendar endCalendar, int calendarField) {
		return endCalendar.get(calendarField) - beginCalendar.get(calendarField);
	}

	private static long getTime(Date beginDate, Date endDate) {
		return endDate.getTime() - beginDate.getTime();
	}

	public static Date parseDate(String str, String patten) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(patten);
		Calendar cd = Calendar.getInstance();
		cd.setTime(sdf.parse(str));
		return cd.getTime();
	}

	public static String format(Date date, String pattern) {
		SimpleDateFormat formatTool = new SimpleDateFormat();
		formatTool.applyPattern(pattern);
		return formatTool.format(date);
	}

	public static String getBeforeMouth() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MONTH, -1);
		String dateString = format(calendar.getTime());
		return dateString;
	}

	public static List<String> getDatesBetweenDays(String beginDay, String endDay, String format) {
		List<String> list = new ArrayList<String>();
		try {
			SimpleDateFormat sdf = new SimpleDateFormat(format);
			Date dBegin = sdf.parse(beginDay);
			Date dEnd = sdf.parse(endDay);
			List<Date> date = getDatesBetweenTwoDate(dBegin, dEnd);
			for (int i = 0; i < date.size(); i++) {
				list.add(sdf.format(date.get(i)));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return list;
	}

	/**
	 * java 根据开始和结束日期得到之间所有日期集合
	 * 
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public static List<Date> getDatesBetweenTwoDate(Date beginDate, Date endDate) {
		List<Date> lDate = new ArrayList<Date>();
		lDate.add(beginDate);// 把开始时间加入集合
		Calendar cal = Calendar.getInstance();
		// 使用给定的 Date 设置此 Calendar 的时间
		cal.setTime(beginDate);
		boolean bContinue = true;
		while (bContinue) {
			// 根据日历的规则，为给定的日历字段添加或减去指定的时间量
			cal.add(Calendar.DAY_OF_MONTH, 1);
			// 测试此日期是否在指定日期之后
			if (endDate.after(cal.getTime())) {
				lDate.add(cal.getTime());
			} else {
				break;
			}
		}
		lDate.add(endDate);// 把结束时间加入集合
		return lDate;
	}

	/**
	 * 获得一年前
	 * 
	 * @return
	 */
	public static String getBeforeYear() {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.YEAR, -1);
		String dateString = format(calendar.getTime());
		return dateString;
	}

	public static Date getNextWeek(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.add(Calendar.DAY_OF_MONTH, -7);
		date = calendar.getTime();
		return date;
	}

	public static List<String> getBetweenMounths(String beginTime, String endTime, String format) {
		List<String> list = new ArrayList<String>();
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		Date d1 = null;
		Date d2 = null;
		try {
			d1 = sdf.parse(beginTime);
			d2 = sdf.parse(endTime);// 定义结束日期
		} catch (ParseException e) {
			e.printStackTrace();
		} // 定义起始日期
		Calendar dd = Calendar.getInstance();// 定义日期实例
		dd.setTime(d1);// 设置日期起始时间
		while (dd.getTime().before(d2)) {// 判断是否到结束日期
			String str = sdf.format(dd.getTime());
			list.add(str);
			dd.add(Calendar.MONTH, 1);// 进行当前日期月份加1
		}
		return list;

	}

	public static String getBeforeMouthByNum(int num) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.MONTH, -num);
		String dateString = format(calendar.getTime());
		return dateString;
	}

	/**
	 * 时间增加分钟
	 * 
	 * @param date
	 * @param n
	 * @return
	 */
	public static Date addMinutes(Date date, int n) {
		Calendar cd = Calendar.getInstance();
		cd.setTime(date);
		cd.add(Calendar.MINUTE, n);// 增加分钟
		return cd.getTime();
	}

	/**
	 * 天数变化
	 * @param date
	 * @param n
	 * @return
	 */
	public static Date addDay(Date date,int n){
		Calendar cd = Calendar.getInstance();
		cd.setTime(date);
		cd.add(Calendar.DATE, n);// 增加小时
		return cd.getTime();
	}
	/**
	 * 时间增加小时
	 * @param date
	 * @param n
	 * @return
	 */
	public static Date addHours(Date date, int n) {
		Calendar cd = Calendar.getInstance();
		cd.setTime(date);
		cd.add(Calendar.HOUR, n);// 增加小时
		return cd.getTime();
	}
	
	/**
	 * 时间增加秒
	 * 
	 * @param date
	 * @param n
	 * @return
	 */
	public static Date addSeconds(Date date, int n) {
		Calendar cd = Calendar.getInstance();
		cd.setTime(date);
		cd.add(Calendar.SECOND, n);// 增加秒
		return cd.getTime();
	}

	/**
	 * 毫秒
	 * 
	 * @param date
	 * @param i
	 * @return
	 */
	public static Date addMillSeconds(Date date, long time) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		if (time > 0) {
			while (time > 0) {
				int span = 0;
				if (time >= (long) Integer.MAX_VALUE) {
					span = Integer.MAX_VALUE;
					time = time - (long) Integer.MAX_VALUE;
				} else {
					span = (int) time + 0;
					time = 0;
				}
				calendar.add(Calendar.MILLISECOND, span);
			}
		} else if (time < 0) {
			while (time < 0) {
				int span = 0;
				if (time <= (long) Integer.MIN_VALUE) {
					span = Integer.MIN_VALUE;
					time = time - (long) Integer.MIN_VALUE;
				} else {
					span = (int) time + 0;
					time = 0;
				}
				calendar.add(Calendar.MILLISECOND, span);
			}
		}
		date = calendar.getTime();

		return date;
	}

	/**
	 * 补全时间分组 ---按月间隔分组(适用于查询3个月，6个月，9个月)
	 * 
	 * @param timeBegin
	 * @param timeEnd
	 * @param timeFormat
	 * @param maps
	 * @return
	 */
	public static Map<String, Object> getTimeFullMapForMonth(String timeBegin, String timeEnd, String timeFormat,
			Map<String, Object> maps) {

		Map<String, Object> result = new LinkedHashMap<String, Object>();
		try {
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM");
			Date sdate = format.parse(timeBegin);
			Calendar sCal = Calendar.getInstance();
			sCal.setTime(sdate);

			Date edate = format.parse(timeEnd);
			Calendar eCel = Calendar.getInstance();
			eCel.setTime(edate);

			for (Calendar c = sCal; c.before(eCel) || c.equals(eCel); c.add(Calendar.MONTH, 1)) {
				String str = DateUtil.format(c.getTime(), timeFormat);
				if (maps.containsKey(str)) {
					result.put(str, maps.get(str));
				} else {
					result.put(str, 0);
				}
			}
			return result;
		} catch (Exception e) {
			log.error("安月时间补全错误：", e);

		}
		return null;
	}

	public static String timeStamp2Date(String seconds, String format) {
		if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
			return "";
		}
		if (format == null || format.isEmpty()) {
			format = "yyyy-MM-dd HH:mm:ss";
		}
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date(Long.valueOf(seconds + "000")));
	}

}
