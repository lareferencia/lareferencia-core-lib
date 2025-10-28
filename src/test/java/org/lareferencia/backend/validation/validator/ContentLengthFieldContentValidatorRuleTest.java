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
 * Unit tests for ContentLengthFieldContentValidatorRule
 */
@DisplayName("ContentLengthFieldContentValidatorRule Tests")
class ContentLengthFieldContentValidatorRuleTest {

    private ContentLengthFieldContentValidatorRule rule;
    
    @BeforeEach
    void setUp() {
        rule = new ContentLengthFieldContentValidatorRule();
    }
    
    @Test
    @DisplayName("Should create rule with default values")
    void testDefaultConstructor() {
        assertNotNull(rule);
        assertEquals(0, rule.getMinLength());
        assertEquals(Integer.MAX_VALUE, rule.getMaxLength());
    }
    
    @Test
    @DisplayName("Should validate content within default range")
    void testDefaultRange() {
        ContentValidatorResult result = rule.validate("test");
        
        assertTrue(result.isValid());
        assertTrue(result.getReceivedValue().contains("test"));
        assertTrue(result.getReceivedValue().contains("| 4"));
    }
    
    @Test
    @DisplayName("Should validate exact minimum length")
    void testExactMinLength() {
        rule.setMinLength(5);
        
        ContentValidatorResult result = rule.validate("hello");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should invalidate below minimum length")
    void testBelowMinLength() {
        rule.setMinLength(10);
        
        ContentValidatorResult result = rule.validate("short");
        
        assertFalse(result.isValid());
    }
    
    @Test
    @DisplayName("Should validate exact maximum length")
    void testExactMaxLength() {
        rule.setMaxLength(5);
        
        ContentValidatorResult result = rule.validate("hello");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should invalidate above maximum length")
    void testAboveMaxLength() {
        rule.setMaxLength(5);
        
        ContentValidatorResult result = rule.validate("too long");
        
        assertFalse(result.isValid());
    }
    
    @Test
    @DisplayName("Should validate within range")
    void testWithinRange() {
        rule.setMinLength(5);
        rule.setMaxLength(15);
        
        ContentValidatorResult result = rule.validate("good length");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should handle null content")
    void testNullContent() {
        String nullString = null;
        ContentValidatorResult result = rule.validate(nullString);
        
        assertFalse(result.isValid());
        assertEquals("NULL", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle empty string")
    void testEmptyString() {
        rule.setMinLength(1);
        
        ContentValidatorResult result = rule.validate("");
        
        assertFalse(result.isValid());
    }
    
    @Test
    @DisplayName("Should validate empty string with zero min length")
    void testEmptyStringZeroMin() {
        rule.setMinLength(0);
        
        ContentValidatorResult result = rule.validate("");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should truncate long content in result")
    void testLongContentTruncation() {
        String longContent = "a".repeat(100);
        
        ContentValidatorResult result = rule.validate(longContent);
        
        assertTrue(result.getReceivedValue().contains("..."));
        assertTrue(result.getReceivedValue().length() < longContent.length());
    }
    
    @Test
    @DisplayName("Should include length in received value")
    void testLengthInReceivedValue() {
        ContentValidatorResult result = rule.validate("test");
        
        assertTrue(result.getReceivedValue().contains("| 4"));
    }
    
    @Test
    @DisplayName("Should handle exact boundary values")
    void testBoundaryValues() {
        rule.setMinLength(10);
        rule.setMaxLength(20);
        
        // Exact min
        ContentValidatorResult minResult = rule.validate("1234567890");
        assertTrue(minResult.isValid());
        
        // Exact max
        ContentValidatorResult maxResult = rule.validate("12345678901234567890");
        assertTrue(maxResult.isValid());
        
        // Just below min
        ContentValidatorResult belowResult = rule.validate("123456789");
        assertFalse(belowResult.isValid());
        
        // Just above max
        ContentValidatorResult aboveResult = rule.validate("123456789012345678901");
        assertFalse(aboveResult.isValid());
    }
    
    @Test
    @DisplayName("Should validate single character")
    void testSingleCharacter() {
        rule.setMinLength(1);
        rule.setMaxLength(1);
        
        ContentValidatorResult result = rule.validate("a");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should have correct toString")
    void testToString() {
        rule.setMinLength(5);
        rule.setMaxLength(10);
        
        String toString = rule.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("minLength=5"));
        assertTrue(toString.contains("maxLength=10"));
    }
    
    @Test
    @DisplayName("Should set and get min length")
    void testMinLengthProperty() {
        rule.setMinLength(100);
        assertEquals(100, rule.getMinLength());
    }
    
    @Test
    @DisplayName("Should set and get max length")
    void testMaxLengthProperty() {
        rule.setMaxLength(200);
        assertEquals(200, rule.getMaxLength());
    }
    
    @Test
    @DisplayName("Should handle Unicode characters in length calculation")
    void testUnicodeLength() {
        rule.setMinLength(3);
        rule.setMaxLength(5);
        
        ContentValidatorResult result = rule.validate("测试中");
        
        assertTrue(result.isValid());
    }
}
