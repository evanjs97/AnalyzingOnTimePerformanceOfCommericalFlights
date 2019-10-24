package hadoop.util;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Utils {
	private static final String[] days = {"Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
	private static final String[] months = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
	public static boolean isValidEntry(String entry) {
		return !entry.isEmpty() && !entry.equals("NA");
	}


	public static String getDay(int day) {
		return days[day];
	}
	
	public static String reformatString(String s, int length) {
		StringBuilder builder = new StringBuilder(s);
		while(builder.length() < length) {
			builder.append(' ');
		}
		return builder.toString();
	}

	public static String roundTime(int year, int month, int day, String time, int minuteIntervals) {
		Calendar calendar = new GregorianCalendar(year, month, day, Integer.parseInt(time.substring(0,2)), Integer.parseInt(time.substring(2)));
		int minuteRound = calendar.get(Calendar.MINUTE) % minuteIntervals;
		calendar.add(Calendar.MINUTE, minuteRound < 8 ? -minuteRound : (minuteIntervals-minuteRound));

		int hour  = calendar.get(Calendar.HOUR);
		if(calendar.get(Calendar.AM_PM) == Calendar.PM && hour != 12) hour += 12;
		String strHour = hour < 10 ? "0" + hour : "" + hour;
		String minute = calendar.get(Calendar.MINUTE) < 10 ? "0" + calendar.get(Calendar.MINUTE) : ""+calendar.get(Calendar.MINUTE);
		return strHour + minute;
	}

	public static String getMonth(int month) {
		return months[month];
	}
	
	public static boolean isNumber(String num) {
		try {
			Integer.parseInt(num);
		}catch(NumberFormatException nfe) {
			return false;
		}
		return true;
	}
}
