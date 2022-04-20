
/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.core.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class DateUtil {

	private static final String[] timeFormats = { "HH:mm:ss", "HH:mm" };
	private static final String[] dateSeparators = { "/", "-", " " };

	private static final String DMY_FORMAT = "dd{sep}MM{sep}yyyy";
	private static final String YMD_FORMAT = "yyyy{sep}MM{sep}dd";

	private static final String ymd_template = "\\d{4}{sep}\\d{2}{sep}\\d{2}.*";
	private static final String dmy_template = "\\d{2}{sep}\\d{2}{sep}\\d{4}.*";
	
	public static DateFormat OAIPMH_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'"); // Quoted "Z" to indicate UTC, no timezone offset


	public static Date stringToDate(String input) {
		Date date = null;
		String dateFormat = getDateFormat(input);
		if (dateFormat == null) {
			throw new IllegalArgumentException("Date is not in an accepted format " + input);
		}

		for (String sep : dateSeparators) {
			String actualDateFormat = patternForSeparator(dateFormat, sep);
			// try first with the time
			for (String time : timeFormats) {
				date = tryParse(input, actualDateFormat + " " + time);
				if (date != null) {
					return date;
				}
			}
			// didn't work, try without the time formats
			date = tryParse(input, actualDateFormat);
			if (date != null) {
				return date;
			}
		}

		return date;
	}
	
	public static Date atEndOfDay (Date date){
	    
	    Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			cal.set(Calendar.HOUR_OF_DAY, 23);
			cal.set(Calendar.MINUTE, 59);
			cal.set(Calendar.SECOND, 59);
	    cal.set(Calendar.MILLISECOND, 999);
	    
	    return cal.getTime();
		}

	private static String getDateFormat(String date) {
		for (String sep : dateSeparators) {
			String ymdPattern = patternForSeparator(ymd_template, sep);
			String dmyPattern = patternForSeparator(dmy_template, sep);
			if (date.matches(ymdPattern)) {
				return YMD_FORMAT;
			}
			if (date.matches(dmyPattern)) {
				return DMY_FORMAT;
			}
		}
		return null;
	}

	private static String patternForSeparator(String template, String sep) {
		return template.replace("{sep}", sep);
	}

	private static Date tryParse(String input, String pattern) {
		try {
			return new SimpleDateFormat(pattern).parse(input);
		} catch (ParseException e) {
		}
		return null;
	}

}