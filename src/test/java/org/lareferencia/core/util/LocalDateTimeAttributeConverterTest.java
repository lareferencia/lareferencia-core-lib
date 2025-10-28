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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("LocalDateTimeAttributeConverter Tests")
class LocalDateTimeAttributeConverterTest {

    private LocalDateTimeAttributeConverter converter;

    @BeforeEach
    void setUp() {
        converter = new LocalDateTimeAttributeConverter();
    }

    @Test
    @DisplayName("Should convert LocalDateTime to Timestamp")
    void testConvertToDatabaseColumn() {
        LocalDateTime dateTime = LocalDateTime.of(2025, 10, 27, 12, 30, 45);
        
        Timestamp result = converter.convertToDatabaseColumn(dateTime);
        
        assertNotNull(result);
        assertEquals(Timestamp.valueOf(dateTime), result);
    }

    @Test
    @DisplayName("Should convert null LocalDateTime to null Timestamp")
    void testConvertToDatabaseColumnNull() {
        Timestamp result = converter.convertToDatabaseColumn(null);
        
        assertNull(result);
    }

    @Test
    @DisplayName("Should convert Timestamp to LocalDateTime")
    void testConvertToEntityAttribute() {
        Timestamp timestamp = Timestamp.valueOf("2025-10-27 12:30:45");
        
        LocalDateTime result = converter.convertToEntityAttribute(timestamp);
        
        assertNotNull(result);
        assertEquals(timestamp.toLocalDateTime(), result);
    }

    @Test
    @DisplayName("Should convert null Timestamp to null LocalDateTime")
    void testConvertToEntityAttributeNull() {
        LocalDateTime result = converter.convertToEntityAttribute(null);
        
        assertNull(result);
    }

    @Test
    @DisplayName("Should preserve date and time through round-trip")
    void testRoundTrip() {
        LocalDateTime original = LocalDateTime.of(2025, 10, 27, 12, 30, 45, 123456789);
        
        Timestamp timestamp = converter.convertToDatabaseColumn(original);
        LocalDateTime restored = converter.convertToEntityAttribute(timestamp);
        
        assertEquals(original, restored);
    }

    @Test
    @DisplayName("Should handle midnight")
    void testMidnight() {
        LocalDateTime midnight = LocalDateTime.of(2025, 10, 27, 0, 0, 0);
        
        Timestamp timestamp = converter.convertToDatabaseColumn(midnight);
        LocalDateTime restored = converter.convertToEntityAttribute(timestamp);
        
        assertEquals(midnight, restored);
    }

    @Test
    @DisplayName("Should handle end of day")
    void testEndOfDay() {
        LocalDateTime endOfDay = LocalDateTime.of(2025, 10, 27, 23, 59, 59);
        
        Timestamp timestamp = converter.convertToDatabaseColumn(endOfDay);
        LocalDateTime restored = converter.convertToEntityAttribute(timestamp);
        
        assertEquals(endOfDay, restored);
    }

    @Test
    @DisplayName("Should handle leap year date")
    void testLeapYear() {
        LocalDateTime leapDay = LocalDateTime.of(2024, 2, 29, 12, 0, 0);
        
        Timestamp timestamp = converter.convertToDatabaseColumn(leapDay);
        LocalDateTime restored = converter.convertToEntityAttribute(timestamp);
        
        assertEquals(leapDay, restored);
    }
}
