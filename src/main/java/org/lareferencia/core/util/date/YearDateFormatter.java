
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

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * Date formatter for year-only dates (yyyy format).
 * Defaults month and day to 1, and time components to midnight.
 * 
 * @author pgraca
 */
public class YearDateFormatter implements IDateTimeFormatter {

	/**
	 * Constructs a new year date formatter.
	 */
	public YearDateFormatter() {
		// Default constructor
	}

	/**
	 * Returns a DateTimeFormatter configured to parse year-only patterns.
	 * Parses format: yyyy with defaults for month, day, and time.
	 *
	 * @return the configured DateTimeFormatter
	 */
    @Override
    public DateTimeFormatter getFormatter() {
        return new DateTimeFormatterBuilder().appendPattern("yyyy").parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                .parseDefaulting(ChronoField.DAY_OF_MONTH, 1).parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0).parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                .parseDefaulting(ChronoField.MILLI_OF_SECOND, 0).toFormatter();
    }

}
