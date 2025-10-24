
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

package org.lareferencia.core.util.date;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.Set;

import lombok.Setter;

public class DateHelper {

    @Setter
    private Set<IDateTimeFormatter> dateTimeFormatters;

    static DateTimeFormatter HUMAN_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	static DateTimeFormatter MACHINE_DATE_TIME_FORMATTER =  DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"); // Quoted "Z" to indicate UTC, no timezone offset


    /**
     * Parses a string date and return a LocalDateTime can throw an exception if
     * string date doesn't match any pre-configured dateTimeFormatters
     * 
     * @param strDate
     * @return
     */
    public LocalDateTime parseDate(String strDate) {
        if (strDate == null) {
            return null;
        }
        Optional<DateTimeFormatter> optFormatter = getDateFormatterFromString(strDate);
        if (optFormatter.isPresent()) {
            return LocalDateTime.parse(strDate, optFormatter.get());
        }
        try {
            return LocalDateTime.parse(strDate);
        } catch ( Exception e ) {}
        
        
        throw new DateTimeParseException("Date in an unsupported date format: " + strDate, strDate, 0);
    }

    /**
     * Get Instant string
     * 
     * @param date
     * @return
     */
    public static String getInstantDateString(LocalDateTime date) {
        return DateTimeFormatter.ISO_INSTANT.format(date.toInstant(ZoneOffset.UTC));
    }

    /**
     * From a date, returns a preformatted String with the<br/>
     * pattern: "yyyy-MM-dd'T'HH:mm:ss'Z'"
     * 
     * @param date
     * @return
     */
    public static String getDateTimeMachineString(LocalDateTime date) {
        return MACHINE_DATE_TIME_FORMATTER.format(date);
    }


    /**
     * From a date, returns a preformatted String with the<br/>
     * pattern: "yyyy-MM-dd HH:mm:ss"
     *
     * @param date
     * @param pattern
     *
     * @return
     */
    public static String getDateTimeFormattedString(LocalDateTime date, String pattern) {
        return DateTimeFormatter.ofPattern(pattern).format(date);
    }

    /**
     * From a date, returns a preformatted String with the<br/>
     * pattern derived from granularity "yyyy-MM-ddTHH:mm:ssZ"
     *
     * @param date
     * @param granularity
     *
     * @return
     */
    public static String getDateTimeFormattedStringFromGranularity(LocalDateTime date, String granularity) {
        return getDateTimeFormattedString(date, getPatternFromGranularity(granularity));
    }

    private static String getPatternFromGranularity(String granularity) {
        String pattern = pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'";

        if (granularity != null) {

            switch (granularity) {

            case "yyyy-MM-dd":
                return "yyyy-MM-dd";

            case "YYYY-MM-DD":
                return "yyyy-MM-dd";

            case "yyyy-MM-ddTHH":
                return  "yyyy-MM-dd'T'HH";

            case "yyyy-MM-ddTHH:mm":
                return  "yyyy-MM-dd'T'HH:mm";

            case "yyyy-MM-ddTHH:mm:ss":
                return  "yyyy-MM-dd'T'HH:mm:ss";

            case "yyyy-MM-ddTHH:mm:ssZ":
                return  "yyyy-MM-dd'T'HH:mm:ss'Z'";

            case "yyyy-MM-ddTHH:mm:ss.SSSZ":
                return  "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

            case "yyyy-MM-ddTHH:mm:ss.SSS":
                return  "yyyy-MM-dd'T'HH:mm:ss.SSS";

            case "yyyy-MM-ddTHH:mm:ss.SSSXXX":
                return  "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

            case "yyyy-MM-ddTHH:mm:ss.SSSXXXZ":
                return  "yyyy-MM-dd'T'HH:mm:ss.SSSXXX'Z'";

            }
        }
        return pattern;
    }

    /**
     * From a date, returns a preformatted String with the<br/>
     * pattern: "yyyy-MM-dd HH:mm:ss"
     * 
     * @param date
     * @return
     */
    public static String getDateTimeHumanString(LocalDateTime date) {
        return HUMAN_DATE_TIME_FORMATTER.format(date);
    }

    /**
     * Validates if a string date is a valid date according to configured
     * DateTimeFormatters
     * 
     * @param dateStr
     * @return
     */
    public boolean isValid(String dateStr) {

        for (IDateTimeFormatter dateTimeFormatter : dateTimeFormatters) {
            IDateValidator dateValidator = new DateValidator(dateTimeFormatter.getFormatter());

            // if there is at least one valid date
            if (dateValidator.isValid(dateStr)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the proper DateTimeFromatter that verifies that a string is a date
     * 
     * @param dateStr
     * @return
     */
    public Optional<DateTimeFormatter> getDateFormatterFromString(String dateStr) {

        for (IDateTimeFormatter dateTimeFormatter : dateTimeFormatters) {
            IDateValidator dateValidator = new DateValidator(dateTimeFormatter.getFormatter());

            // if there is at least one valid date
            if (dateValidator.isValid(dateStr)) {
                return Optional.of(dateTimeFormatter.getFormatter());
            }
        }
        return Optional.ofNullable(null);
    }

    /**
     * Validates if a LocalDateTime is a valid date
     *
     * @param dateTime
     * @return
     */
    public boolean isValidLocalDateTime(LocalDateTime dateTime) {
        try {
            dateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            return true;
        } catch (DateTimeException e) {
            return false;
        }
    }

}
