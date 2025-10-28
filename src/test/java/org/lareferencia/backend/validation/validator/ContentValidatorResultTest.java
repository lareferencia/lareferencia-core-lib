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

package org.lareferencia.backend.validation.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ContentValidatorResult
 */
@DisplayName("ContentValidatorResult Tests")
class ContentValidatorResultTest {

    private ContentValidatorResult result;
    
    @BeforeEach
    void setUp() {
        result = new ContentValidatorResult();
    }
    
    @Test
    @DisplayName("Should create ContentValidatorResult with default constructor")
    void testDefaultConstructor() {
        assertNotNull(result);
        assertFalse(result.isValid());
        assertNull(result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should create ContentValidatorResult with parameterized constructor")
    void testParameterizedConstructor() {
        ContentValidatorResult validResult = new ContentValidatorResult(true, "test value");
        
        assertTrue(validResult.isValid());
        assertEquals("test value", validResult.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should set and get valid flag")
    void testValidProperty() {
        assertFalse(result.isValid());
        
        result.setValid(true);
        assertTrue(result.isValid());
        
        result.setValid(false);
        assertFalse(result.isValid());
    }
    
    @Test
    @DisplayName("Should set and get received value")
    void testReceivedValueProperty() {
        String value = "test content";
        result.setReceivedValue(value);
        
        assertEquals(value, result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle null received value")
    void testNullReceivedValue() {
        result.setReceivedValue(null);
        assertNull(result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle empty received value")
    void testEmptyReceivedValue() {
        result.setReceivedValue("");
        assertEquals("", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should create valid result")
    void testValidResult() {
        result.setValid(true);
        result.setReceivedValue("valid content");
        
        assertTrue(result.isValid());
        assertEquals("valid content", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should create invalid result")
    void testInvalidResult() {
        result.setValid(false);
        result.setReceivedValue("invalid content");
        
        assertFalse(result.isValid());
        assertEquals("invalid content", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle long values")
    void testLongValue() {
        String longValue = "a".repeat(500);
        result.setReceivedValue(longValue);
        
        assertEquals(longValue, result.getReceivedValue());
        assertEquals(500, result.getReceivedValue().length());
    }
    
    @Test
    @DisplayName("Should handle special characters in value")
    void testSpecialCharacters() {
        String specialValue = "Test <>&\"'@#$%^&*()";
        result.setReceivedValue(specialValue);
        
        assertEquals(specialValue, result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle Unicode characters")
    void testUnicodeCharacters() {
        String unicodeValue = "测试 Test Тест";
        result.setReceivedValue(unicodeValue);
        
        assertEquals(unicodeValue, result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should have correct toString representation")
    void testToString() {
        result.setValid(true);
        result.setReceivedValue("test");
        
        String toString = result.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("valid=true"));
        assertTrue(toString.contains("receivedValue=test"));
    }
    
    @Test
    @DisplayName("Should handle multiple property changes")
    void testMultipleChanges() {
        result.setValid(true);
        result.setReceivedValue("first");
        assertTrue(result.isValid());
        assertEquals("first", result.getReceivedValue());
        
        result.setValid(false);
        result.setReceivedValue("second");
        assertFalse(result.isValid());
        assertEquals("second", result.getReceivedValue());
        
        result.setValid(true);
        result.setReceivedValue("third");
        assertTrue(result.isValid());
        assertEquals("third", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should create result for NULL value")
    void testNullValueResult() {
        result.setValid(false);
        result.setReceivedValue("NULL");
        
        assertFalse(result.isValid());
        assertEquals("NULL", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should create result for no occurrences")
    void testNoOccurrencesResult() {
        result.setValid(false);
        result.setReceivedValue("no_occurrences_found");
        
        assertFalse(result.isValid());
        assertEquals("no_occurrences_found", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle whitespace in value")
    void testWhitespaceValue() {
        String whitespaceValue = "  value with spaces  ";
        result.setReceivedValue(whitespaceValue);
        
        assertEquals(whitespaceValue, result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle newlines in value")
    void testNewlinesInValue() {
        String multilineValue = "line1\nline2\nline3";
        result.setReceivedValue(multilineValue);
        
        assertEquals(multilineValue, result.getReceivedValue());
    }
}
