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
 * Unit tests for MDFormatTranformationException
 */
@DisplayName("MDFormatTranformationException Tests")
class MDFormatTranformationExceptionTest {

    @Test
    @DisplayName("Should create exception with message")
    void testExceptionWithMessage() {
        String message = "Error transforming metadata format";
        MDFormatTranformationException exception = new MDFormatTranformationException(message);
        
        assertEquals(message, exception.getMessage());
    }

    @Test
    @DisplayName("Should create exception with message and cause")
    void testExceptionWithMessageAndCause() {
        String message = "XSLT transformation failed";
        Exception cause = new RuntimeException("Template not found");
        
        MDFormatTranformationException exception = new MDFormatTranformationException(message, cause);
        
        assertEquals(message, exception.getMessage());
        assertEquals(cause, exception.getCause());
    }

    @Test
    @DisplayName("Should preserve cause exception")
    void testCausePreservation() {
        RuntimeException cause = new RuntimeException("XML parse error");
        MDFormatTranformationException exception = new MDFormatTranformationException("Transformation error", cause);
        
        assertNotNull(exception.getCause());
        assertEquals("XML parse error", exception.getCause().getMessage());
    }

    @Test
    @DisplayName("Should handle null message")
    void testNullMessage() {
        MDFormatTranformationException exception = new MDFormatTranformationException((String)null);
        assertNull(exception.getMessage());
    }

    @Test
    @DisplayName("Should be throwable")
    void testThrowable() {
        assertThrows(MDFormatTranformationException.class, () -> {
            throw new MDFormatTranformationException("Test exception");
        });
    }

    @Test
    @DisplayName("Should extend Exception")
    void testExtendsException() {
        MDFormatTranformationException exception = new MDFormatTranformationException("Test");
        assertTrue(exception instanceof Exception);
    }

    @Test
    @DisplayName("Should support exception chaining")
    void testExceptionChaining() {
        Exception rootCause = new IllegalArgumentException("Invalid format");
        RuntimeException intermediateException = new RuntimeException("Processing error", rootCause);
        MDFormatTranformationException exception = new MDFormatTranformationException("Transform error", intermediateException);
        
        assertEquals(intermediateException, exception.getCause());
        assertEquals(rootCause, exception.getCause().getCause());
    }
}
