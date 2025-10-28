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

/**
 * Unit tests for MetadataRecordStoreException
 */
@DisplayName("MetadataRecordStoreException Tests")
class MetadataRecordStoreExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void testExceptionWithMessage() {
        String message = "Error storing metadata record";
        MetadataRecordStoreException exception = new MetadataRecordStoreException(message);
        
        assertEquals(message, exception.getMessage());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void testExceptionWithMessageAndCause() {
        String message = "Database error";
        Exception cause = new RuntimeException("Connection failed");
        
        MetadataRecordStoreException exception = new MetadataRecordStoreException(message, cause);
        
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should preserve cause exception")
    void testCausePreservation() {
        RuntimeException cause = new RuntimeException("Original error");
        MetadataRecordStoreException exception = new MetadataRecordStoreException("Wrapper", cause);
        
        assertNotNull(exception.getCause());
        assertEquals("Original error", exception.getCause().getMessage());
    }

    @Test
    @DisplayName("Should handle null message")
    void testNullMessage() {
        MetadataRecordStoreException exception = new MetadataRecordStoreException((String)null);
        assertNull(exception.getMessage());
    }

    @Test
    @DisplayName("Should be throwable")
    void testThrowable() {
        assertThrows(MetadataRecordStoreException.class, () -> {
            throw new MetadataRecordStoreException("Test exception");
        });
    }

    @Test
    @DisplayName("Should extend Exception")
    void testExtendsException() {
        MetadataRecordStoreException exception = new MetadataRecordStoreException("Test");
        assertTrue(exception instanceof Exception);
    }

    @Test
    @DisplayName("Should support exception chaining")
    void testExceptionChaining() {
        Exception rootCause = new IllegalStateException("Invalid state");
        RuntimeException intermediateException = new RuntimeException("Store error", rootCause);
        MetadataRecordStoreException exception = new MetadataRecordStoreException("Record error", intermediateException);
        
        assertEquals(intermediateException, exception.getCause());
        assertEquals(rootCause, exception.getCause().getCause());
    }
}
