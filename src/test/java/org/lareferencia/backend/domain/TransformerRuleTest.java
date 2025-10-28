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

package org.lareferencia.backend.domain;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TransformerRule entity
 */
@DisplayName("TransformerRule Entity Tests")
class TransformerRuleTest {

    private TransformerRule rule;
    
    @BeforeEach
    void setUp() {
        rule = new TransformerRule();
    }
    
    @Test
    @DisplayName("Should create TransformerRule with default values")
    void testDefaultConstructor() {
        assertNotNull(rule);
        assertNull(rule.getId());
        assertNull(rule.getName());
        assertNull(rule.getDescription());
        assertNull(rule.getRunorder());
        assertNull(rule.getJsonserialization());
    }
    
    @Test
    @DisplayName("Should set and get id correctly")
    void testIdProperty() {
        Long id = 321L;
        rule.setId(id);
        assertEquals(id, rule.getId());
    }
    
    @Test
    @DisplayName("Should set and get name correctly")
    void testNameProperty() {
        String name = "Field Translation Rule";
        rule.setName(name);
        assertEquals(name, rule.getName());
    }
    
    @Test
    @DisplayName("Should set and get description correctly")
    void testDescriptionProperty() {
        String description = "Translates field names from source to target format";
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
    @DisplayName("Should set and get runorder correctly")
    void testRunOrderProperty() {
        Integer runorder = 5;
        rule.setRunorder(runorder);
        assertEquals(runorder, rule.getRunorder());
    }
    
    @Test
    @DisplayName("Should handle different runorder values")
    void testDifferentRunOrders() {
        rule.setRunorder(1);
        assertEquals(1, rule.getRunorder());
        
        rule.setRunorder(10);
        assertEquals(10, rule.getRunorder());
        
        rule.setRunorder(100);
        assertEquals(100, rule.getRunorder());
    }
    
    @Test
    @DisplayName("Should set and get JSON serialization")
    void testJsonSerializationProperty() {
        String json = "{\"sourceField\":\"dc.title\",\"targetField\":\"dcterms.title\"}";
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
    @DisplayName("Should generate correct toString output")
    void testToString() {
        rule.setName("Test Rule");
        rule.setRunorder(5);
        
        String result = rule.toString();
        
        assertNotNull(result);
        assertTrue(result.contains("Test Rule"));
        assertTrue(result.contains("5"));
        assertTrue(result.contains("TransformerRule"));
    }
    
    @Test
    @DisplayName("Should handle toString with null values")
    void testToStringWithNulls() {
        String result = rule.toString();
        
        assertNotNull(result);
        assertTrue(result.contains("TransformerRule"));
    }
    
    @Test
    @DisplayName("Should create rule with complete configuration")
    void testCompleteConfiguration() {
        rule.setId(1L);
        rule.setName("Normalize Author Rule");
        rule.setDescription("Normalizes author names to standard format");
        rule.setRunorder(3);
        rule.setJsonserialization("{\"field\":\"dc.contributor.author\",\"pattern\":\"uppercase\"}");
        
        assertEquals(1L, rule.getId());
        assertEquals("Normalize Author Rule", rule.getName());
        assertEquals("Normalizes author names to standard format", rule.getDescription());
        assertEquals(3, rule.getRunorder());
        assertTrue(rule.getJsonserialization().contains("dc.contributor.author"));
    }
    
    @Test
    @DisplayName("Should create rule with complex JSON configuration")
    void testComplexJsonConfiguration() {
        String complexJson = """
            {
                "transformations": [
                    {"source": "dc.title", "target": "dcterms.title"},
                    {"source": "dc.creator", "target": "dcterms.creator"}
                ],
                "options": {
                    "preserveOriginal": true,
                    "caseSensitive": false
                }
            }
            """;
        
        rule.setName("Field Mapping Rule");
        rule.setDescription("Maps DC to DCTERMS");
        rule.setRunorder(1);
        rule.setJsonserialization(complexJson);
        
        assertEquals("Field Mapping Rule", rule.getName());
        assertEquals(1, rule.getRunorder());
        assertTrue(rule.getJsonserialization().contains("transformations"));
        assertTrue(rule.getJsonserialization().contains("dc.title"));
    }
    
    @Test
    @DisplayName("Should handle multiple rules with different orders")
    void testMultipleRulesWithOrders() {
        TransformerRule rule1 = new TransformerRule();
        rule1.setName("First Rule");
        rule1.setRunorder(1);
        
        TransformerRule rule2 = new TransformerRule();
        rule2.setName("Second Rule");
        rule2.setRunorder(2);
        
        TransformerRule rule3 = new TransformerRule();
        rule3.setName("Third Rule");
        rule3.setRunorder(3);
        
        assertTrue(rule1.getRunorder() < rule2.getRunorder());
        assertTrue(rule2.getRunorder() < rule3.getRunorder());
    }
    
    @Test
    @DisplayName("Should handle very long JSON serialization")
    void testLongJsonSerialization() {
        StringBuilder longJson = new StringBuilder("{\"mappings\":[");
        for (int i = 0; i < 50; i++) {
            if (i > 0) longJson.append(",");
            longJson.append("{\"source\":\"field").append(i).append("\",\"target\":\"mapped").append(i).append("\"}");
        }
        longJson.append("]}");
        
        rule.setJsonserialization(longJson.toString());
        
        assertNotNull(rule.getJsonserialization());
        assertTrue(rule.getJsonserialization().length() > 1000);
        assertTrue(rule.getJsonserialization().contains("field0"));
        assertTrue(rule.getJsonserialization().contains("mapped49"));
    }
    
    @Test
    @DisplayName("Should allow negative runorder values")
    void testNegativeRunOrder() {
        rule.setRunorder(-1);
        assertEquals(-1, rule.getRunorder());
    }
    
    @Test
    @DisplayName("Should allow zero runorder value")
    void testZeroRunOrder() {
        rule.setRunorder(0);
        assertEquals(0, rule.getRunorder());
    }
    
    @Test
    @DisplayName("Should reset id to null")
    void testResetId() {
        rule.setId(200L);
        assertEquals(200L, rule.getId());
        
        rule.setId(null);
        assertNull(rule.getId());
    }
    
    @Test
    @DisplayName("Should handle rules with same runorder")
    void testSameRunOrder() {
        TransformerRule rule1 = new TransformerRule();
        rule1.setName("Rule A");
        rule1.setRunorder(5);
        
        TransformerRule rule2 = new TransformerRule();
        rule2.setName("Rule B");
        rule2.setRunorder(5);
        
        assertEquals(rule1.getRunorder(), rule2.getRunorder());
        assertNotEquals(rule1.getName(), rule2.getName());
    }
}
