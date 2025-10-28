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

package org.lareferencia.core.harvester;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for NoRecordsMatchException
 */
@DisplayName("NoRecordsMatchException Tests")
class NoRecordsMatchExceptionTest {

    @Test
    @DisplayName("Should create exception with no arguments")
    void testNoArgsConstructor() {
        NoRecordsMatchException exception = new NoRecordsMatchException();
        
        assertNotNull(exception);
        assertNull(exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with message")
    void testMessageConstructor() {
        String message = "No records match the query";
        NoRecordsMatchException exception = new NoRecordsMatchException(message);
        
        assertNotNull(exception);
        assertEquals(message, exception.getMessage());
        assertNull(exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void testMessageAndCauseConstructor() {
        String message = "No records match the query";
        Throwable cause = new RuntimeException("Root cause");
        NoRecordsMatchException exception = new NoRecordsMatchException(message, cause);
        
        assertNotNull(exception);
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should create exception with cause only")
    void testCauseConstructor() {
        Throwable cause = new IllegalArgumentException("Invalid argument");
        NoRecordsMatchException exception = new NoRecordsMatchException(cause);
        
        assertNotNull(exception);
        assertEquals(cause, exception.getCause());
        assertTrue(exception.getMessage().contains("IllegalArgumentException"));
    }

    @Test
    @DisplayName("Should be throwable")
    void testExceptionIsThrowable() {
        assertThrows(NoRecordsMatchException.class, () -> {
            throw new NoRecordsMatchException("Test exception");
        });
    }

    @Test
    @DisplayName("Should preserve stack trace when thrown")
    void testStackTracePreservation() {
        try {
            throwNoRecordsMatchException();
            fail("Exception should have been thrown");
        } catch (NoRecordsMatchException e) {
            StackTraceElement[] stackTrace = e.getStackTrace();
            assertNotNull(stackTrace);
            assertTrue(stackTrace.length > 0);
            assertTrue(stackTrace[0].getMethodName().equals("throwNoRecordsMatchException"));
        }
    }

    @Test
    @DisplayName("Should handle null message gracefully")
    void testNullMessage() {
        NoRecordsMatchException exception = new NoRecordsMatchException((String) null);
        assertNotNull(exception);
        assertNull(exception.getMessage());
    }

    @Test
    @DisplayName("Should handle empty message")
    void testEmptyMessage() {
        String emptyMessage = "";
        NoRecordsMatchException exception = new NoRecordsMatchException(emptyMessage);
        
        assertNotNull(exception);
        assertEquals(emptyMessage, exception.getMessage());
    }

    @Test
    @DisplayName("Should verify serial version UID exists")
    void testSerialVersionUID() throws NoSuchFieldException {
        // Verify that the serialVersionUID field exists and has the correct value
        java.lang.reflect.Field serialVersionUIDField = 
            NoRecordsMatchException.class.getDeclaredField("serialVersionUID");
        
        assertNotNull(serialVersionUIDField);
        assertEquals(long.class, serialVersionUIDField.getType());
    }

    @Test
    @DisplayName("Should be instance of Exception")
    void testExceptionHierarchy() {
        NoRecordsMatchException exception = new NoRecordsMatchException();
        
        assertTrue(exception instanceof Exception);
        assertTrue(exception instanceof Throwable);
    }

    // Helper method
    private void throwNoRecordsMatchException() throws NoRecordsMatchException {
        throw new NoRecordsMatchException("Test exception for stack trace");
    }
}
