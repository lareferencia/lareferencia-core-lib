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
 * Unit tests for RegexFieldContentValidatorRule
 */
@DisplayName("RegexFieldContentValidatorRule Tests")
class RegexFieldContentValidatorRuleTest {

    private RegexFieldContentValidatorRule rule;
    
    @BeforeEach
    void setUp() {
        rule = new RegexFieldContentValidatorRule();
    }
    
    @Test
    @DisplayName("Should validate content matching simple regex")
    void testSimpleRegexMatch() {
        rule.setRegexString("^[0-9]+$");
        
        ContentValidatorResult result = rule.validate("12345");
        
        assertTrue(result.isValid());
        assertEquals("12345", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should reject content not matching regex")
    void testRegexNoMatch() {
        rule.setRegexString("^[0-9]+$");
        
        ContentValidatorResult result = rule.validate("abc123");
        
        assertFalse(result.isValid());
        assertEquals("abc123", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should validate email pattern")
    void testEmailPattern() {
        rule.setRegexString("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$");
        
        ContentValidatorResult validResult = rule.validate("user@example.com");
        assertTrue(validResult.isValid());
        
        ContentValidatorResult invalidResult = rule.validate("invalid-email");
        assertFalse(invalidResult.isValid());
    }
    
    @Test
    @DisplayName("Should validate URL pattern")
    void testURLPattern() {
        rule.setRegexString("^https?://.*$");
        
        ContentValidatorResult httpResult = rule.validate("http://example.com");
        assertTrue(httpResult.isValid());
        
        ContentValidatorResult httpsResult = rule.validate("https://example.com");
        assertTrue(httpsResult.isValid());
        
        ContentValidatorResult invalidResult = rule.validate("ftp://example.com");
        assertFalse(invalidResult.isValid());
    }
    
    @Test
    @DisplayName("Should handle null content")
    void testNullContent() {
        rule.setRegexString("^[0-9]+$");
        
        String nullString = null;
        ContentValidatorResult result = rule.validate(nullString);
        
        assertFalse(result.isValid());
        assertEquals("NULL", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle empty string")
    void testEmptyString() {
        rule.setRegexString("^[a-z]*$");
        
        ContentValidatorResult result = rule.validate("");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should truncate long content in result")
    void testLongContentTruncation() {
        rule.setRegexString(".*");
        
        String longString = "a".repeat(150);
        ContentValidatorResult result = rule.validate(longString);
        
        assertTrue(result.isValid());
        assertTrue(result.getReceivedValue().length() <= 103); // 100 + "..."
        assertTrue(result.getReceivedValue().endsWith("..."));
    }
    
    @Test
    @DisplayName("Should not truncate short content")
    void testShortContentNoTruncation() {
        rule.setRegexString(".*");
        
        String shortString = "short content";
        ContentValidatorResult result = rule.validate(shortString);
        
        assertTrue(result.isValid());
        assertEquals(shortString, result.getReceivedValue());
        assertFalse(result.getReceivedValue().endsWith("..."));
    }
    
    @Test
    @DisplayName("Should validate date pattern YYYY-MM-DD")
    void testDatePattern() {
        rule.setRegexString("^\\d{4}-\\d{2}-\\d{2}$");
        
        ContentValidatorResult validResult = rule.validate("2025-10-27");
        assertTrue(validResult.isValid());
        
        ContentValidatorResult invalidResult = rule.validate("27-10-2025");
        assertFalse(invalidResult.isValid());
    }
    
    @Test
    @DisplayName("Should validate ISBN pattern")
    void testISBNPattern() {
        rule.setRegexString("^ISBN[- ]?(?:\\d{9}[\\dXx]|\\d{13})$");
        
        ContentValidatorResult valid10 = rule.validate("ISBN 1234567890");
        assertTrue(valid10.isValid());
        
        ContentValidatorResult valid13 = rule.validate("ISBN 1234567890123");
        assertTrue(valid13.isValid());
    }
    
    @Test
    @DisplayName("Should validate with complex regex")
    void testComplexRegex() {
        // Pattern for author name: "LastName, FirstName"
        rule.setRegexString("^[A-Z][a-z]+, [A-Z][a-z]+$");
        
        ContentValidatorResult validResult = rule.validate("Smith, John");
        assertTrue(validResult.isValid());
        
        ContentValidatorResult invalidResult = rule.validate("John Smith");
        assertFalse(invalidResult.isValid());
    }
    
    @Test
    @DisplayName("Should set and get regex string")
    void testRegexStringProperty() {
        String regex = "^test.*$";
        rule.setRegexString(regex);
        
        assertEquals(regex, rule.getRegexString());
    }
    
    @Test
    @DisplayName("Should handle special regex characters")
    void testSpecialRegexCharacters() {
        rule.setRegexString("^\\[.*\\]$");
        
        ContentValidatorResult validResult = rule.validate("[content]");
        assertTrue(validResult.isValid());
        
        ContentValidatorResult invalidResult = rule.validate("content");
        assertFalse(invalidResult.isValid());
    }
    
    @Test
    @DisplayName("Should validate DOI pattern")
    void testDOIPattern() {
        rule.setRegexString("^10\\.\\d{4,}/.*$");
        
        ContentValidatorResult validResult = rule.validate("10.1234/example.doi");
        assertTrue(validResult.isValid());
        
        ContentValidatorResult invalidResult = rule.validate("doi:10.1234/example");
        assertFalse(invalidResult.isValid());
    }
    
    @Test
    @DisplayName("Should validate year range pattern")
    void testYearRangePattern() {
        rule.setRegexString("^(19|20)\\d{2}$");
        
        ContentValidatorResult valid1 = rule.validate("2025");
        assertTrue(valid1.isValid());
        
        ContentValidatorResult valid2 = rule.validate("1999");
        assertTrue(valid2.isValid());
        
        ContentValidatorResult invalid = rule.validate("1899");
        assertFalse(invalid.isValid());
    }
}
