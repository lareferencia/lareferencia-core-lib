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

package org.lareferencia.core.domain;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.worker.validation.QuantifierValues;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ValidatorRule entity
 */
@DisplayName("ValidatorRule Entity Tests")
class ValidatorRuleTest {

    private ValidatorRule rule;
    
    @BeforeEach
    void setUp() {
        rule = new ValidatorRule();
    }
    
    @Test
    @DisplayName("Should create ValidatorRule with default values")
    void testDefaultConstructor() {
        assertNotNull(rule);
        assertNull(rule.getId());
        assertNull(rule.getName());
        assertNull(rule.getDescription());
        assertFalse(rule.getMandatory());
        assertEquals(QuantifierValues.ONE_OR_MORE, rule.getQuantifier());
        assertNull(rule.getJsonserialization());
    }
    
    @Test
    @DisplayName("Should set and get id correctly")
    void testIdProperty() {
        Long id = 789L;
        rule.setId(id);
        assertEquals(id, rule.getId());
    }
    
    @Test
    @DisplayName("Should set and get name correctly")
    void testNameProperty() {
        String name = "Title Validation Rule";
        rule.setName(name);
        assertEquals(name, rule.getName());
    }
    
    @Test
    @DisplayName("Should set and get description correctly")
    void testDescriptionProperty() {
        String description = "Validates title field presence and format";
        rule.setDescription(description);
        assertEquals(description, rule.getDescription());
    }
    
    @Test
    @DisplayName("Should handle null description")
    void testNullDescription() {
        rule.setDescription(null);
        assertNull(rule.getDescription());
    }
    
    @Test
    @DisplayName("Should set and get mandatory flag")
    void testMandatoryProperty() {
        assertFalse(rule.getMandatory());
        
        rule.setMandatory(true);
        assertTrue(rule.getMandatory());
        
        rule.setMandatory(false);
        assertFalse(rule.getMandatory());
    }
    
    @Test
    @DisplayName("Should set and get quantifier")
    void testQuantifierProperty() {
        assertEquals(QuantifierValues.ONE_OR_MORE, rule.getQuantifier());
        
        rule.setQuantifier(QuantifierValues.ONE_ONLY);
        assertEquals(QuantifierValues.ONE_ONLY, rule.getQuantifier());
        
        rule.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        assertEquals(QuantifierValues.ZERO_OR_MORE, rule.getQuantifier());
    }
    
    @Test
    @DisplayName("Should handle all QuantifierValues")
    void testAllQuantifierValues() {
        rule.setQuantifier(QuantifierValues.ZERO_ONLY);
        assertEquals(QuantifierValues.ZERO_ONLY, rule.getQuantifier());
        
        rule.setQuantifier(QuantifierValues.ONE_ONLY);
        assertEquals(QuantifierValues.ONE_ONLY, rule.getQuantifier());
        
        rule.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        assertEquals(QuantifierValues.ZERO_OR_MORE, rule.getQuantifier());
        
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        assertEquals(QuantifierValues.ONE_OR_MORE, rule.getQuantifier());
        
        rule.setQuantifier(QuantifierValues.ALL);
        assertEquals(QuantifierValues.ALL, rule.getQuantifier());
    }
    
    @Test
    @DisplayName("Should set and get JSON serialization")
    void testJsonSerializationProperty() {
        String json = "{\"field\":\"dc.title\",\"xpath\":\"//dc:title\"}";
        rule.setJsonserialization(json);
        assertEquals(json, rule.getJsonserialization());
    }
    
    @Test
    @DisplayName("Should handle null JSON serialization")
    void testNullJsonSerialization() {
        rule.setJsonserialization(null);
        assertNull(rule.getJsonserialization());
    }
    
    @Test
    @DisplayName("Should handle empty JSON serialization")
    void testEmptyJsonSerialization() {
        rule.setJsonserialization("");
        assertEquals("", rule.getJsonserialization());
    }
    
    @Test
    @DisplayName("Should create mandatory rule")
    void testMandatoryRule() {
        rule.setName("Mandatory Title Rule");
        rule.setMandatory(true);
        rule.setQuantifier(QuantifierValues.ONE_ONLY);
        
        assertEquals("Mandatory Title Rule", rule.getName());
        assertTrue(rule.getMandatory());
        assertEquals(QuantifierValues.ONE_ONLY, rule.getQuantifier());
    }
    
    @Test
    @DisplayName("Should create optional rule")
    void testOptionalRule() {
        rule.setName("Optional Subject Rule");
        rule.setMandatory(false);
        rule.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        
        assertEquals("Optional Subject Rule", rule.getName());
        assertFalse(rule.getMandatory());
        assertEquals(QuantifierValues.ZERO_OR_MORE, rule.getQuantifier());
    }
    
    @Test
    @DisplayName("Should create rule with complex JSON configuration")
    void testComplexJsonConfiguration() {
        String complexJson = """
            {
                "field": "dc.contributor.author",
                "xpath": "//dc:contributor[@role='author']",
                "regex": "^[A-Z][a-z]+, [A-Z][a-z]+$",
                "mandatory": true
            }
            """;
        
        rule.setName("Author Validation Rule");
        rule.setDescription("Validates author format");
        rule.setMandatory(true);
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setJsonserialization(complexJson);
        
        assertEquals("Author Validation Rule", rule.getName());
        assertEquals("Validates author format", rule.getDescription());
        assertTrue(rule.getMandatory());
        assertEquals(QuantifierValues.ONE_OR_MORE, rule.getQuantifier());
        assertTrue(rule.getJsonserialization().contains("dc.contributor.author"));
    }
    
    @Test
    @DisplayName("Should handle multiple rules with different configurations")
    void testMultipleRules() {
        ValidatorRule rule1 = new ValidatorRule();
        rule1.setName("Rule 1");
        rule1.setMandatory(true);
        rule1.setQuantifier(QuantifierValues.ONE_ONLY);
        
        ValidatorRule rule2 = new ValidatorRule();
        rule2.setName("Rule 2");
        rule2.setMandatory(false);
        rule2.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        
        assertNotEquals(rule1.getName(), rule2.getName());
        assertNotEquals(rule1.getMandatory(), rule2.getMandatory());
        assertNotEquals(rule1.getQuantifier(), rule2.getQuantifier());
    }
    
    @Test
    @DisplayName("Should handle very long JSON serialization")
    void testLongJsonSerialization() {
        StringBuilder longJson = new StringBuilder("{\"fields\":[");
        for (int i = 0; i < 100; i++) {
            if (i > 0) longJson.append(",");
            longJson.append("\"field").append(i).append("\"");
        }
        longJson.append("]}");
        
        rule.setJsonserialization(longJson.toString());
        
        assertNotNull(rule.getJsonserialization());
        assertTrue(rule.getJsonserialization().length() > 500);
        assertTrue(rule.getJsonserialization().contains("field0"));
        assertTrue(rule.getJsonserialization().contains("field99"));
    }
    
    @Test
    @DisplayName("Should reset id to null")
    void testResetId() {
        rule.setId(100L);
        assertEquals(100L, rule.getId());
        
        rule.setId(null);
        assertNull(rule.getId());
    }
}
