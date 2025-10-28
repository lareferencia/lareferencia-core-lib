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
 * Unit tests for Transformer entity
 */
@DisplayName("Transformer Entity Tests")
class TransformerTest {

    private Transformer transformer;
    
    @BeforeEach
    void setUp() {
        transformer = new Transformer();
    }
    
    @Test
    @DisplayName("Should create Transformer with default values")
    void testDefaultConstructor() {
        assertNotNull(transformer);
        assertNull(transformer.getId());
        assertNull(transformer.getName());
        assertNull(transformer.getDescription());
        assertNotNull(transformer.getRules());
        assertTrue(transformer.getRules().isEmpty());
    }
    
    @Test
    @DisplayName("Should set and get id correctly")
    void testIdProperty() {
        Long id = 456L;
        transformer.setId(id);
        assertEquals(id, transformer.getId());
    }
    
    @Test
    @DisplayName("Should set and get name correctly")
    void testNameProperty() {
        String name = "DC Transformer";
        transformer.setName(name);
        assertEquals(name, transformer.getName());
    }
    
    @Test
    @DisplayName("Should set and get description correctly")
    void testDescriptionProperty() {
        String description = "Transforms Dublin Core metadata";
        transformer.setDescription(description);
        assertEquals(description, transformer.getDescription());
    }
    
    @Test
    @DisplayName("Should handle null description")
    void testNullDescription() {
        transformer.setDescription(null);
        assertNull(transformer.getDescription());
    }
    
    @Test
    @DisplayName("Should add rules to transformer")
    void testAddRules() {
        TransformerRule rule1 = new TransformerRule();
        rule1.setName("Rule 1");
        rule1.setRunorder(1);
        
        TransformerRule rule2 = new TransformerRule();
        rule2.setName("Rule 2");
        rule2.setRunorder(2);
        
        transformer.getRules().add(rule1);
        transformer.getRules().add(rule2);
        
        assertEquals(2, transformer.getRules().size());
        assertTrue(transformer.getRules().contains(rule1));
        assertTrue(transformer.getRules().contains(rule2));
    }
    
    @Test
    @DisplayName("Should remove rules from transformer")
    void testRemoveRules() {
        TransformerRule rule = new TransformerRule();
        rule.setName("Test Rule");
        
        transformer.getRules().add(rule);
        assertEquals(1, transformer.getRules().size());
        
        transformer.getRules().remove(rule);
        assertEquals(0, transformer.getRules().size());
    }
    
    @Test
    @DisplayName("Should clear all rules")
    void testClearRules() {
        transformer.getRules().add(new TransformerRule());
        transformer.getRules().add(new TransformerRule());
        transformer.getRules().add(new TransformerRule());
        
        assertEquals(3, transformer.getRules().size());
        
        transformer.getRules().clear();
        assertEquals(0, transformer.getRules().size());
    }
    
    @Test
    @DisplayName("Should reset id to null")
    void testResetId() {
        transformer.setId(200L);
        assertEquals(200L, transformer.getId());
        
        transformer.resetId();
        assertNull(transformer.getId());
    }
    
    @Test
    @DisplayName("Should reset all rule ids when resetting transformer id")
    void testResetIdWithRules() {
        TransformerRule rule1 = new TransformerRule();
        rule1.setId(10L);
        
        TransformerRule rule2 = new TransformerRule();
        rule2.setId(20L);
        
        TransformerRule rule3 = new TransformerRule();
        rule3.setId(30L);
        
        transformer.getRules().add(rule1);
        transformer.getRules().add(rule2);
        transformer.getRules().add(rule3);
        transformer.setId(200L);
        
        transformer.resetId();
        
        assertNull(transformer.getId());
        assertNull(rule1.getId());
        assertNull(rule2.getId());
        assertNull(rule3.getId());
    }
    
    @Test
    @DisplayName("Should handle empty rules list when resetting id")
    void testResetIdWithEmptyRules() {
        transformer.setId(200L);
        transformer.resetId();
        
        assertNull(transformer.getId());
        assertTrue(transformer.getRules().isEmpty());
    }
    
    @Test
    @DisplayName("Should maintain rules order by runorder")
    void testRulesOrder() {
        TransformerRule rule1 = new TransformerRule();
        rule1.setName("First");
        rule1.setRunorder(1);
        
        TransformerRule rule2 = new TransformerRule();
        rule2.setName("Second");
        rule2.setRunorder(2);
        
        TransformerRule rule3 = new TransformerRule();
        rule3.setName("Third");
        rule3.setRunorder(3);
        
        transformer.getRules().add(rule1);
        transformer.getRules().add(rule2);
        transformer.getRules().add(rule3);
        
        assertEquals("First", transformer.getRules().get(0).getName());
        assertEquals(1, transformer.getRules().get(0).getRunorder());
        assertEquals("Second", transformer.getRules().get(1).getName());
        assertEquals(2, transformer.getRules().get(1).getRunorder());
        assertEquals("Third", transformer.getRules().get(2).getName());
        assertEquals(3, transformer.getRules().get(2).getRunorder());
    }
    
    @Test
    @DisplayName("Should create transformer with complex configuration")
    void testComplexTransformer() {
        transformer.setId(1L);
        transformer.setName("OpenAIRE Transformer");
        transformer.setDescription("Transforms metadata according to OpenAIRE guidelines");
        
        TransformerRule normalizeRule = new TransformerRule();
        normalizeRule.setName("Normalize Rule");
        normalizeRule.setRunorder(1);
        normalizeRule.setDescription("Normalizes field values");
        
        TransformerRule translateRule = new TransformerRule();
        translateRule.setName("Translate Rule");
        translateRule.setRunorder(2);
        translateRule.setDescription("Translates field names");
        
        TransformerRule enrichRule = new TransformerRule();
        enrichRule.setName("Enrich Rule");
        enrichRule.setRunorder(3);
        enrichRule.setDescription("Enriches metadata");
        
        transformer.getRules().add(normalizeRule);
        transformer.getRules().add(translateRule);
        transformer.getRules().add(enrichRule);
        
        assertEquals(1L, transformer.getId());
        assertEquals("OpenAIRE Transformer", transformer.getName());
        assertEquals(3, transformer.getRules().size());
        
        // Verify rules order
        assertEquals("Normalize Rule", transformer.getRules().get(0).getName());
        assertEquals(1, transformer.getRules().get(0).getRunorder());
        
        assertEquals("Translate Rule", transformer.getRules().get(1).getName());
        assertEquals(2, transformer.getRules().get(1).getRunorder());
        
        assertEquals("Enrich Rule", transformer.getRules().get(2).getName());
        assertEquals(3, transformer.getRules().get(2).getRunorder());
    }
    
    @Test
    @DisplayName("Should handle rule replacement")
    void testRuleReplacement() {
        TransformerRule oldRule = new TransformerRule();
        oldRule.setName("Old Rule");
        transformer.getRules().add(oldRule);
        
        TransformerRule newRule = new TransformerRule();
        newRule.setName("New Rule");
        
        transformer.getRules().set(0, newRule);
        
        assertEquals(1, transformer.getRules().size());
        assertEquals("New Rule", transformer.getRules().get(0).getName());
    }
    
    @Test
    @DisplayName("Should allow multiple transformers with different configurations")
    void testMultipleTransformers() {
        Transformer transformer1 = new Transformer();
        transformer1.setName("Transformer 1");
        transformer1.getRules().add(new TransformerRule());
        
        Transformer transformer2 = new Transformer();
        transformer2.setName("Transformer 2");
        transformer2.getRules().add(new TransformerRule());
        transformer2.getRules().add(new TransformerRule());
        
        assertEquals(1, transformer1.getRules().size());
        assertEquals(2, transformer2.getRules().size());
        assertNotEquals(transformer1.getName(), transformer2.getName());
    }
    
    @Test
    @DisplayName("Should handle rules with same runorder")
    void testSameRunOrder() {
        TransformerRule rule1 = new TransformerRule();
        rule1.setName("Rule A");
        rule1.setRunorder(1);
        
        TransformerRule rule2 = new TransformerRule();
        rule2.setName("Rule B");
        rule2.setRunorder(1);
        
        transformer.getRules().add(rule1);
        transformer.getRules().add(rule2);
        
        assertEquals(2, transformer.getRules().size());
        assertEquals(1, transformer.getRules().get(0).getRunorder());
        assertEquals(1, transformer.getRules().get(1).getRunorder());
    }
}
