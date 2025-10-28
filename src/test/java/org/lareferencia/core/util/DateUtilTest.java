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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DateUtil Tests")
class DateUtilTest {

    @Test
    @DisplayName("Should parse date in yyyy-MM-dd format")
    void testParseYMDFormat() {
        Date date = DateUtil.stringToDate("2025-10-27");
        
        assertNotNull(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        assertEquals(2025, cal.get(Calendar.YEAR));
        assertEquals(Calendar.OCTOBER, cal.get(Calendar.MONTH));
        assertEquals(27, cal.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    @DisplayName("Should parse date in dd/MM/yyyy format")
    void testParseDMYFormat() {
        Date date = DateUtil.stringToDate("27/10/2025");
        
        assertNotNull(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        assertEquals(2025, cal.get(Calendar.YEAR));
        assertEquals(Calendar.OCTOBER, cal.get(Calendar.MONTH));
        assertEquals(27, cal.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    @DisplayName("Should parse date with time HH:mm:ss")
    void testParseWithTimeSeconds() {
        Date date = DateUtil.stringToDate("2025-10-27 14:30:45");
        
        assertNotNull(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        assertEquals(14, cal.get(Calendar.HOUR_OF_DAY));
        assertEquals(30, cal.get(Calendar.MINUTE));
        assertEquals(45, cal.get(Calendar.SECOND));
    }

    @Test
    @DisplayName("Should parse date with time HH:mm")
    void testParseWithTimeMinutes() {
        Date date = DateUtil.stringToDate("2025-10-27 14:30");
        
        assertNotNull(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        assertEquals(14, cal.get(Calendar.HOUR_OF_DAY));
        assertEquals(30, cal.get(Calendar.MINUTE));
    }

    @Test
    @DisplayName("Should parse date with dash separator")
    void testParseDateDashSeparator() {
        Date date = DateUtil.stringToDate("2025-10-27");
        assertNotNull(date);
    }

    @Test
    @DisplayName("Should parse date with slash separator")
    void testParseDateSlashSeparator() {
        Date date = DateUtil.stringToDate("27/10/2025");
        assertNotNull(date);
    }

    @Test
    @DisplayName("Should parse date with space separator")
    void testParseDateSpaceSeparator() {
        Date date = DateUtil.stringToDate("2025 10 27");
        assertNotNull(date);
    }

    @Test
    @DisplayName("Should throw exception for invalid date format")
    void testInvalidDateFormat() {
        assertThrows(IllegalArgumentException.class, () ->
            DateUtil.stringToDate("not a date"));
    }

    @Test
    @DisplayName("Should throw exception for invalid date pattern")
    void testInvalidDatePattern() {
        assertThrows(IllegalArgumentException.class, () ->
            DateUtil.stringToDate("27-2025-10"));
    }

    @Test
    @DisplayName("Should set time to end of day")
    void testAtEndOfDay() {
        Calendar inputCal = Calendar.getInstance();
        inputCal.set(2025, Calendar.OCTOBER, 27, 10, 30, 45);
        Date input = inputCal.getTime();
        
        Date result = DateUtil.atEndOfDay(input);
        
        assertNotNull(result);
        Calendar cal = Calendar.getInstance();
        cal.setTime(result);
        assertEquals(23, cal.get(Calendar.HOUR_OF_DAY));
        assertEquals(59, cal.get(Calendar.MINUTE));
        assertEquals(59, cal.get(Calendar.SECOND));
        assertEquals(999, cal.get(Calendar.MILLISECOND));
    }

    @Test
    @DisplayName("Should preserve date when setting end of day")
    void testAtEndOfDayPreservesDate() {
        Calendar inputCal = Calendar.getInstance();
        inputCal.set(2025, Calendar.OCTOBER, 27, 10, 30, 45);
        Date input = inputCal.getTime();
        
        Date result = DateUtil.atEndOfDay(input);
        
        Calendar cal = Calendar.getInstance();
        cal.setTime(result);
        assertEquals(2025, cal.get(Calendar.YEAR));
        assertEquals(Calendar.OCTOBER, cal.get(Calendar.MONTH));
        assertEquals(27, cal.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    @DisplayName("Should handle leap year dates")
    void testLeapYear() {
        Date date = DateUtil.stringToDate("2024-02-29");
        
        assertNotNull(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        assertEquals(2024, cal.get(Calendar.YEAR));
        assertEquals(Calendar.FEBRUARY, cal.get(Calendar.MONTH));
        assertEquals(29, cal.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    @DisplayName("Should handle first day of year")
    void testFirstDayOfYear() {
        Date date = DateUtil.stringToDate("2025-01-01");
        
        assertNotNull(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        assertEquals(2025, cal.get(Calendar.YEAR));
        assertEquals(Calendar.JANUARY, cal.get(Calendar.MONTH));
        assertEquals(1, cal.get(Calendar.DAY_OF_MONTH));
    }

    @Test
    @DisplayName("Should handle last day of year")
    void testLastDayOfYear() {
        Date date = DateUtil.stringToDate("2025-12-31");
        
        assertNotNull(date);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        assertEquals(2025, cal.get(Calendar.YEAR));
        assertEquals(Calendar.DECEMBER, cal.get(Calendar.MONTH));
        assertEquals(31, cal.get(Calendar.DAY_OF_MONTH));
    }
}
