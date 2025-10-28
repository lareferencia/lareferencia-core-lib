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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for OAIRecordMetadataParseException
 */
@DisplayName("OAIRecordMetadataParseException Tests")
class OAIRecordMetadataParseExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void testExceptionWithMessage() {
        String message = "Error parsing metadata";
        OAIRecordMetadataParseException exception = new OAIRecordMetadataParseException(message);
        
        assertEquals(message, exception.getMessage());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void testExceptionWithMessageAndCause() {
        String message = "Error parsing XML";
        Exception cause = new RuntimeException("Root cause");
        
        OAIRecordMetadataParseException exception = new OAIRecordMetadataParseException(message, cause);
        
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should preserve cause exception")
    void testCausePreservation() {
        RuntimeException cause = new RuntimeException("Original error");
        OAIRecordMetadataParseException exception = new OAIRecordMetadataParseException("Wrapper", cause);
        
        assertNotNull(exception.getCause());
        assertEquals("Original error", exception.getCause().getMessage());
    }

    @Test
    @DisplayName("Should handle null message")
    void testNullMessage() {
        OAIRecordMetadataParseException exception = new OAIRecordMetadataParseException((String)null);
        assertNull(exception.getMessage());
    }

    @Test
    @DisplayName("Should handle empty message")
    void testEmptyMessage() {
        OAIRecordMetadataParseException exception = new OAIRecordMetadataParseException("");
        assertEquals("", exception.getMessage());
    }

    @Test
    @DisplayName("Should be throwable")
    void testThrowable() {
        assertThrows(OAIRecordMetadataParseException.class, () -> {
            throw new OAIRecordMetadataParseException("Test exception");
        });
    }

    @Test
    @DisplayName("Should extend Exception")
    void testExtendsException() {
        OAIRecordMetadataParseException exception = new OAIRecordMetadataParseException("Test");
        assertTrue(exception instanceof Exception);
    }

    @Test
    @DisplayName("Should handle long error messages")
    void testLongErrorMessage() {
        String longMessage = "Error: " + "x".repeat(1000);
        OAIRecordMetadataParseException exception = new OAIRecordMetadataParseException(longMessage);
        
        assertEquals(longMessage, exception.getMessage());
    }

    @Test
    @DisplayName("Should handle special characters in message")
    void testSpecialCharactersInMessage() {
        String specialMessage = "Error parsing XML: <tag> with 'quotes' and \"double quotes\" & special chars";
        OAIRecordMetadataParseException exception = new OAIRecordMetadataParseException(specialMessage);
        
        assertEquals(specialMessage, exception.getMessage());
    }

    @Test
    @DisplayName("Should support exception chaining")
    void testExceptionChaining() {
        Exception rootCause = new IllegalArgumentException("Invalid argument");
        RuntimeException intermediateException = new RuntimeException("Processing error", rootCause);
        OAIRecordMetadataParseException exception = new OAIRecordMetadataParseException("Parse error", intermediateException);
        
        assertEquals(intermediateException, exception.getCause());
        assertEquals(rootCause, exception.getCause().getCause());
    }
}
