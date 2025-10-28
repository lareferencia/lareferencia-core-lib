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

package org.lareferencia.core.metadata;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("RecordStatus Enum Tests")
class RecordStatusTest {

    @Test
    @DisplayName("Should have all expected enum values")
    void testEnumValues() {
        RecordStatus[] values = RecordStatus.values();
        
        assertEquals(4, values.length);
        assertTrue(contains(values, RecordStatus.INVALID));
        assertTrue(contains(values, RecordStatus.VALID));
        assertTrue(contains(values, RecordStatus.UNTESTED));
        assertTrue(contains(values, RecordStatus.DELETED));
    }

    @Test
    @DisplayName("Should convert string to enum value")
    void testValueOf() {
        assertEquals(RecordStatus.VALID, RecordStatus.valueOf("VALID"));
        assertEquals(RecordStatus.INVALID, RecordStatus.valueOf("INVALID"));
        assertEquals(RecordStatus.UNTESTED, RecordStatus.valueOf("UNTESTED"));
        assertEquals(RecordStatus.DELETED, RecordStatus.valueOf("DELETED"));
    }

    @Test
    @DisplayName("Should throw exception for invalid enum value")
    void testValueOfInvalid() {
        assertThrows(IllegalArgumentException.class, () -> {
            RecordStatus.valueOf("NONEXISTENT");
        });
    }

    @Test
    @DisplayName("Should maintain enum order")
    void testEnumOrder() {
        RecordStatus[] values = RecordStatus.values();
        
        assertEquals(RecordStatus.INVALID, values[0]);
        assertEquals(RecordStatus.VALID, values[1]);
        assertEquals(RecordStatus.UNTESTED, values[2]);
        assertEquals(RecordStatus.DELETED, values[3]);
    }

    @Test
    @DisplayName("Should compare enum values correctly")
    void testEnumComparison() {
        assertTrue(RecordStatus.INVALID.ordinal() < RecordStatus.VALID.ordinal());
        assertTrue(RecordStatus.VALID.ordinal() < RecordStatus.UNTESTED.ordinal());
        assertTrue(RecordStatus.UNTESTED.ordinal() < RecordStatus.DELETED.ordinal());
    }

    @Test
    @DisplayName("Should return correct name")
    void testEnumName() {
        assertEquals("VALID", RecordStatus.VALID.name());
        assertEquals("INVALID", RecordStatus.INVALID.name());
        assertEquals("UNTESTED", RecordStatus.UNTESTED.name());
        assertEquals("DELETED", RecordStatus.DELETED.name());
    }

    @Test
    @DisplayName("Should work in switch statements")
    void testEnumInSwitch() {
        String result = getStatusDescription(RecordStatus.VALID);
        assertEquals("Record is valid", result);
        
        result = getStatusDescription(RecordStatus.INVALID);
        assertEquals("Record is invalid", result);
        
        result = getStatusDescription(RecordStatus.UNTESTED);
        assertEquals("Record not yet tested", result);
        
        result = getStatusDescription(RecordStatus.DELETED);
        assertEquals("Record is deleted", result);
    }

    @Test
    @DisplayName("Should maintain referential equality")
    void testEnumReferentialEquality() {
        RecordStatus status1 = RecordStatus.VALID;
        RecordStatus status2 = RecordStatus.VALID;
        
        assertSame(status1, status2);
    }

    @Test
    @DisplayName("Should support equality comparison")
    void testEnumEquality() {
        assertEquals(RecordStatus.VALID, RecordStatus.VALID);
        assertNotEquals(RecordStatus.VALID, RecordStatus.INVALID);
        assertNotEquals(RecordStatus.VALID, null);
    }

    @Test
    @DisplayName("Should be serializable")
    void testEnumSerialization() {
        // Enums are inherently serializable in Java
        assertTrue(java.io.Serializable.class.isAssignableFrom(RecordStatus.class));
    }

    // Helper methods

    private boolean contains(RecordStatus[] values, RecordStatus status) {
        for (RecordStatus value : values) {
            if (value == status) {
                return true;
            }
        }
        return false;
    }

    private String getStatusDescription(RecordStatus status) {
        switch (status) {
            case VALID:
                return "Record is valid";
            case INVALID:
                return "Record is invalid";
            case UNTESTED:
                return "Record not yet tested";
            case DELETED:
                return "Record is deleted";
            default:
                return "Unknown status";
        }
    }
}
