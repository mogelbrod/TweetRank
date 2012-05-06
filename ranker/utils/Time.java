package utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Time {
	private static final int MsPerDay    = 1000*60*60*24;
	private static final int MsPerHour   = 1000*60*60;
	private static final int MsPerMinute = 1000*60;
	private static final int MsPerSecond = 1000;
	long days;
	int hours;
	int minutes;
	int seconds;
	int milliseconds;
	boolean infinity;
	
	public Time() {
		infinity = true;
	}
	
	public Time(long timestamp) {
		days  = timestamp/MsPerDay;
		hours = (int)(timestamp%MsPerDay)/MsPerHour;
		minutes = (int)(timestamp%MsPerHour)/MsPerMinute;
		seconds = (int)(timestamp%MsPerMinute)/MsPerSecond;
		milliseconds = (int)(timestamp%MsPerSecond);
		infinity = false;
	}

	/**
	 * @return the days
	 */
	public long getDays() {
		return days;
	}

	/**
	 * @return the hours
	 */
	public int getHours() {
		return hours;
	}

	/**
	 * @return the minutes
	 */
	public int getMinutes() {
		return minutes;
	}

	/**
	 * @return the seconds
	 */
	public int getSeconds() {
		return seconds;
	}

	/**
	 * @return the milliseconds
	 */
	public int getMilliseconds() {
		return milliseconds;
	}

	/**
	 * @param days the days to set
	 */
	public void setDays(long days) {
		this.days = days;
	}

	/**
	 * @param hours the hours to set
	 */
	public void setHours(int hours) {
		this.hours = hours;
	}

	/**
	 * @param minutes the minutes to set
	 */
	public void setMinutes(int minutes) {
		this.minutes = minutes;
	}

	/**
	 * @param seconds the seconds to set
	 */
	public void setSeconds(int seconds) {
		this.seconds = seconds;
	}

	/**
	 * @param milliseconds the milliseconds to set
	 */
	public void setMilliseconds(int milliseconds) {
		this.milliseconds = milliseconds;
	}
	
	/**
	 * @return the infinity
	 */
	public boolean isInfinity() {
		return infinity;
	}

	/**
	 * @param infinity the infinity to set
	 */
	public void setInfinity(boolean infinity) {
		this.infinity = infinity;
	}	

	@Override
	public String toString() {
		if (infinity)
			return "infinity";
		else if ( days > 0 )
			return days + "d" + hours + "h" + minutes + "m";
		else if ( hours > 0 )
			return hours + "h" + minutes + "m" + seconds + "s";
		else if ( minutes > 0 )
			return minutes + "m" + seconds + "s" + milliseconds + "ms";
		else if ( seconds > 0 )
			return seconds + "s" + milliseconds + "ms";
		else
			return milliseconds + "ms";
	}
	
	public static String formatDate(String format, Date date) {
		DateFormat dateFormat = new SimpleDateFormat(format);
		return dateFormat.format(date);
	}	
}