
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
import java.time.format.DateTimeParseException;

/**
 * Validator for date strings using a specific formatter.
 * <p>
 * Checks if a date string can be parsed using the configured
 * DateTimeFormatter pattern.
 * </p>
 * 
 * @author LA Referencia Team
 * @see IDateValidator
 */
public class DateValidator implements IDateValidator {
    private DateTimeFormatter dateFormatter;

    /**
     * Creates a date validator with the specified formatter.
     * 
     * @param dateFormatter the formatter to use for validation
     */
    public DateValidator(DateTimeFormatter dateFormatter) {
        this.dateFormatter = dateFormatter;
    }

    @Override
    public boolean isValid(String dateStr) {
        try {
            this.dateFormatter.parse(dateStr);
        } catch (DateTimeParseException e) {
            return false;
        }
        return true;
    }

}
