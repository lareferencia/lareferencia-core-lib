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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("DateHelper Unit Tests")
class DateHelperTest {

    private DateHelper dateHelper;
    private Set<IDateTimeFormatter> dateTimeFormatters;

    @BeforeEach
    void setUp() {
        dateHelper = new DateHelper();
        dateTimeFormatters = new HashSet<>();
        
        // Add common date formatters
        dateTimeFormatters.add(new YearMonthDayDateFormatter());
        dateTimeFormatters.add(new YearMonthDateFormatter());
        dateTimeFormatters.add(new YearDateFormatter());
        dateTimeFormatters.add(new SystemDateFormatter());
        
        dateHelper.setDateTimeFormatters(dateTimeFormatters);
    }

    @Test
    @DisplayName("Should parse valid date string")
    void testParseDateValid() {
        String validDate = "2023-10-26";
        
        assertDoesNotThrow(() -> {
            LocalDateTime result = dateHelper.parseDate(validDate);
            assertNotNull(result);
        });
    }

    @Test
    @DisplayName("Should return null for null date string")
    void testParseDateNull() {
        LocalDateTime result = dateHelper.parseDate(null);
        assertNull(result);
    }

    @Test
    @DisplayName("Should throw exception for invalid date string")
    void testParseDateInvalid() {
        String invalidDate = "not-a-date";
        
        assertThrows(DateTimeParseException.class, () -> {
            dateHelper.parseDate(invalidDate);
        });
    }

    @Test
    @DisplayName("Should parse ISO format date")
    void testParseDateISO() {
        String isoDate = "2023-10-26T14:30:00";
        
        assertDoesNotThrow(() -> {
            LocalDateTime result = dateHelper.parseDate(isoDate);
            assertNotNull(result);
            assertEquals(2023, result.getYear());
            assertEquals(10, result.getMonthValue());
            assertEquals(26, result.getDayOfMonth());
        });
    }

    @Test
    @DisplayName("Should get instant date string")
    void testGetInstantDateString() {
        LocalDateTime date = LocalDateTime.of(2023, 10, 26, 14, 30, 0);
        
        String result = DateHelper.getInstantDateString(date);
        
        assertNotNull(result);
        assertTrue(result.contains("2023"));
        assertTrue(result.contains("10"));
        assertTrue(result.contains("26"));
    }

    @Test
    @DisplayName("Should get date time machine string")
    void testGetDateTimeMachineString() {
        LocalDateTime date = LocalDateTime.of(2023, 10, 26, 14, 30, 45);
        
        String result = DateHelper.getDateTimeMachineString(date);
        
        assertNotNull(result);
        assertEquals("2023-10-26T14:30:45Z", result);
    }

    @Test
    @DisplayName("Should get date time formatted string")
    void testGetDateTimeFormattedString() {
        LocalDateTime date = LocalDateTime.of(2023, 10, 26, 14, 30, 45);
        String pattern = "dd/MM/yyyy";
        
        String result = DateHelper.getDateTimeFormattedString(date, pattern);
        
        assertNotNull(result);
        assertEquals("26/10/2023", result);
    }

    @Test
    @DisplayName("Should get date time formatted string with custom pattern")
    void testGetDateTimeFormattedStringCustomPattern() {
        LocalDateTime date = LocalDateTime.of(2023, 10, 26, 14, 30, 45);
        String pattern = "yyyy-MM-dd HH:mm";
        
        String result = DateHelper.getDateTimeFormattedString(date, pattern);
        
        assertNotNull(result);
        assertEquals("2023-10-26 14:30", result);
    }

    @Test
    @DisplayName("Should get date time formatted string from granularity yyyy-MM-dd")
    void testGetDateTimeFormattedStringFromGranularityYMD() {
        LocalDateTime date = LocalDateTime.of(2023, 10, 26, 14, 30, 45);
        
        String result = DateHelper.getDateTimeFormattedStringFromGranularity(date, "yyyy-MM-dd");
        
        assertNotNull(result);
        assertEquals("2023-10-26", result);
    }

    @Test
    @DisplayName("Should get date time formatted string from granularity yyyy-MM-ddTHH:mm:ss")
    void testGetDateTimeFormattedStringFromGranularityFull() {
        LocalDateTime date = LocalDateTime.of(2023, 10, 26, 14, 30, 45);
        
        String result = DateHelper.getDateTimeFormattedStringFromGranularity(date, "yyyy-MM-ddTHH:mm:ss");
        
        assertNotNull(result);
        assertEquals("2023-10-26T14:30:45", result);
    }

    @Test
    @DisplayName("Should get date time formatted string from granularity with Z")
    void testGetDateTimeFormattedStringFromGranularityWithZ() {
        LocalDateTime date = LocalDateTime.of(2023, 10, 26, 14, 30, 45);
        
        String result = DateHelper.getDateTimeFormattedStringFromGranularity(date, "yyyy-MM-ddTHH:mm:ssZ");
        
        assertNotNull(result);
        assertEquals("2023-10-26T14:30:45Z", result);
    }

    @Test
    @DisplayName("Should get date time formatted string from null granularity")
    void testGetDateTimeFormattedStringFromGranularityNull() {
        LocalDateTime date = LocalDateTime.of(2023, 10, 26, 14, 30, 45);
        
        String result = DateHelper.getDateTimeFormattedStringFromGranularity(date, null);
        
        assertNotNull(result);
        // Should use default pattern
        assertEquals("2023-10-26T14:30:45Z", result);
    }

    @Test
    @DisplayName("Should get date time human string")
    void testGetDateTimeHumanString() {
        LocalDateTime date = LocalDateTime.of(2023, 10, 26, 14, 30, 45);
        
        String result = DateHelper.getDateTimeHumanString(date);
        
        assertNotNull(result);
        assertEquals("2023-10-26 14:30:45", result);
    }

    @Test
    @DisplayName("Should validate valid date string")
    void testIsValidTrue() {
        String validDate = "2023-10-26";
        
        boolean result = dateHelper.isValid(validDate);
        
        assertTrue(result);
    }

    @Test
    @DisplayName("Should invalidate invalid date string")
    void testIsValidFalse() {
        String invalidDate = "not-a-valid-date";
        
        boolean result = dateHelper.isValid(invalidDate);
        
        assertFalse(result);
    }

    @Test
    @DisplayName("Should get date formatter from valid string")
    void testGetDateFormatterFromString() {
        String validDate = "2023-10-26";
        
        Optional<DateTimeFormatter> result = dateHelper.getDateFormatterFromString(validDate);
        
        assertTrue(result.isPresent());
    }

    @Test
    @DisplayName("Should return empty optional for invalid date string")
    void testGetDateFormatterFromStringInvalid() {
        String invalidDate = "not-a-valid-date";
        
        Optional<DateTimeFormatter> result = dateHelper.getDateFormatterFromString(invalidDate);
        
        assertFalse(result.isPresent());
    }

    @Test
    @DisplayName("Should validate LocalDateTime")
    void testIsValidLocalDateTime() {
        LocalDateTime validDateTime = LocalDateTime.of(2023, 10, 26, 14, 30, 45);
        
        boolean result = dateHelper.isValidLocalDateTime(validDateTime);
        
        assertTrue(result);
    }

    @Test
    @DisplayName("Should handle edge case dates")
    void testEdgeCaseDates() {
        LocalDateTime leapYearDate = LocalDateTime.of(2024, 2, 29, 0, 0, 0);
        String result = DateHelper.getDateTimeMachineString(leapYearDate);
        
        assertNotNull(result);
        assertTrue(result.contains("2024"));
        assertTrue(result.contains("02"));
        assertTrue(result.contains("29"));
    }

    @Test
    @DisplayName("Should handle midnight time")
    void testMidnightTime() {
        LocalDateTime midnight = LocalDateTime.of(2023, 10, 26, 0, 0, 0);
        String result = DateHelper.getDateTimeHumanString(midnight);
        
        assertNotNull(result);
        assertEquals("2023-10-26 00:00:00", result);
    }

    @Test
    @DisplayName("Should handle end of day time")
    void testEndOfDayTime() {
        LocalDateTime endOfDay = LocalDateTime.of(2023, 10, 26, 23, 59, 59);
        String result = DateHelper.getDateTimeHumanString(endOfDay);
        
        assertNotNull(result);
        assertEquals("2023-10-26 23:59:59", result);
    }
}
