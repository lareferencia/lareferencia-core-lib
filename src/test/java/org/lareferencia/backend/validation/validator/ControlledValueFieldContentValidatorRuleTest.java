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

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ControlledValueFieldContentValidatorRule
 */
@DisplayName("ControlledValueFieldContentValidatorRule Tests")
class ControlledValueFieldContentValidatorRuleTest {

    private ControlledValueFieldContentValidatorRule rule;
    
    @BeforeEach
    void setUp() {
        rule = new ControlledValueFieldContentValidatorRule();
    }
    
    @Test
    @DisplayName("Should create rule with empty controlled values")
    void testDefaultConstructor() {
        assertNotNull(rule);
        assertNotNull(rule.getControlledValues());
        assertTrue(rule.getControlledValues().isEmpty());
    }
    
    @Test
    @DisplayName("Should validate value in controlled list")
    void testValueInList() {
        rule.getControlledValues().addAll(Arrays.asList("value1", "value2", "value3"));
        
        ContentValidatorResult result = rule.validate("value2");
        
        assertTrue(result.isValid());
        assertEquals("value2", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should invalidate value not in controlled list")
    void testValueNotInList() {
        rule.getControlledValues().addAll(Arrays.asList("value1", "value2", "value3"));
        
        ContentValidatorResult result = rule.validate("value4");
        
        assertFalse(result.isValid());
        assertEquals("value4", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle null content")
    void testNullContent() {
        rule.getControlledValues().add("valid");
        
        String nullString = null;
        ContentValidatorResult result = rule.validate(nullString);
        
        assertFalse(result.isValid());
        assertEquals("NULL", result.getReceivedValue());
    }
    
    @Test
    @DisplayName("Should handle empty string in controlled values")
    void testEmptyStringInList() {
        rule.getControlledValues().add("");
        
        ContentValidatorResult result = rule.validate("");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should be case sensitive")
    void testCaseSensitive() {
        rule.getControlledValues().add("Value");
        
        ContentValidatorResult upperResult = rule.validate("Value");
        ContentValidatorResult lowerResult = rule.validate("value");
        
        assertTrue(upperResult.isValid());
        assertFalse(lowerResult.isValid());
    }
    
    @Test
    @DisplayName("Should validate with single controlled value")
    void testSingleValue() {
        rule.getControlledValues().add("only");
        
        ContentValidatorResult validResult = rule.validate("only");
        ContentValidatorResult invalidResult = rule.validate("other");
        
        assertTrue(validResult.isValid());
        assertFalse(invalidResult.isValid());
    }
    
    @Test
    @DisplayName("Should handle whitespace in values")
    void testWhitespaceInValues() {
        rule.getControlledValues().add("with spaces");
        
        ContentValidatorResult exactResult = rule.validate("with spaces");
        ContentValidatorResult trimmedResult = rule.validate("withspaces");
        
        assertTrue(exactResult.isValid());
        assertFalse(trimmedResult.isValid());
    }
    
    @Test
    @DisplayName("Should invalidate with empty controlled values list")
    void testEmptyControlledValuesList() {
        ContentValidatorResult result = rule.validate("anything");
        
        assertFalse(result.isValid());
    }
    
    @Test
    @DisplayName("Should truncate long content in result")
    void testLongContentTruncation() {
        rule.getControlledValues().add("short");
        
        String longContent = "a".repeat(150);
        ContentValidatorResult result = rule.validate(longContent);
        
        assertFalse(result.isValid());
        assertTrue(result.getReceivedValue().endsWith("..."));
        assertTrue(result.getReceivedValue().length() <= 103);
    }
    
    @Test
    @DisplayName("Should validate access rights values")
    void testAccessRightsValues() {
        rule.getControlledValues().addAll(Arrays.asList(
            "open access",
            "restricted access",
            "embargoed access",
            "metadata only access"
        ));
        
        ContentValidatorResult openResult = rule.validate("open access");
        ContentValidatorResult restrictedResult = rule.validate("restricted access");
        ContentValidatorResult invalidResult = rule.validate("free access");
        
        assertTrue(openResult.isValid());
        assertTrue(restrictedResult.isValid());
        assertFalse(invalidResult.isValid());
    }
    
    @Test
    @DisplayName("Should validate resource types")
    void testResourceTypes() {
        rule.getControlledValues().addAll(Arrays.asList(
            "article",
            "thesis",
            "book",
            "dataset",
            "software"
        ));
        
        ContentValidatorResult articleResult = rule.validate("article");
        ContentValidatorResult datasetResult = rule.validate("dataset");
        ContentValidatorResult invalidResult = rule.validate("document");
        
        assertTrue(articleResult.isValid());
        assertTrue(datasetResult.isValid());
        assertFalse(invalidResult.isValid());
    }
    
    @Test
    @DisplayName("Should handle special characters")
    void testSpecialCharacters() {
        rule.getControlledValues().add("value@#$%");
        
        ContentValidatorResult result = rule.validate("value@#$%");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should handle Unicode values")
    void testUnicodeValues() {
        rule.getControlledValues().addAll(Arrays.asList("español", "português", "中文"));
        
        ContentValidatorResult result1 = rule.validate("español");
        ContentValidatorResult result2 = rule.validate("português");
        ContentValidatorResult result3 = rule.validate("中文");
        
        assertTrue(result1.isValid());
        assertTrue(result2.isValid());
        assertTrue(result3.isValid());
    }
    
    @Test
    @DisplayName("Should have correct toString")
    void testToString() {
        rule.getControlledValues().addAll(Arrays.asList("val1", "val2"));
        
        String toString = rule.toString();
        
        assertNotNull(toString);
        assertTrue(toString.contains("ControlledValueContentValidationRule"));
        assertTrue(toString.contains("controlledValues="));
    }
    
    @Test
    @DisplayName("Should allow modification of controlled values list")
    void testModifyControlledValues() {
        rule.getControlledValues().add("initial");
        assertTrue(rule.validate("initial").isValid());
        
        rule.getControlledValues().add("added");
        assertTrue(rule.validate("added").isValid());
        
        rule.getControlledValues().remove("initial");
        assertFalse(rule.validate("initial").isValid());
    }
    
    @Test
    @DisplayName("Should handle duplicate values in list")
    void testDuplicateValues() {
        rule.getControlledValues().add("duplicate");
        rule.getControlledValues().add("duplicate");
        
        ContentValidatorResult result = rule.validate("duplicate");
        
        assertTrue(result.isValid());
    }
    
    @Test
    @DisplayName("Should validate language codes")
    void testLanguageCodes() {
        rule.getControlledValues().addAll(Arrays.asList("en", "es", "pt", "fr", "de"));
        
        ContentValidatorResult validResult = rule.validate("es");
        ContentValidatorResult invalidResult = rule.validate("english");
        
        assertTrue(validResult.isValid());
        assertFalse(invalidResult.isValid());
    }
}
