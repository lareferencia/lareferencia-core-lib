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
 * Unit tests for Validator entity
 */
@DisplayName("Validator Entity Tests")
class ValidatorTest {

    private Validator validator;
    
    @BeforeEach
    void setUp() {
        validator = new Validator();
    }
    
    @Test
    @DisplayName("Should create Validator with default values")
    void testDefaultConstructor() {
        assertNotNull(validator);
        assertNull(validator.getId());
        assertNull(validator.getName());
        assertNull(validator.getDescription());
        assertNotNull(validator.getRules());
        assertTrue(validator.getRules().isEmpty());
    }
    
    @Test
    @DisplayName("Should set and get id correctly")
    void testIdProperty() {
        Long id = 123L;
        validator.setId(id);
        assertEquals(id, validator.getId());
    }
    
    @Test
    @DisplayName("Should set and get name correctly")
    void testNameProperty() {
        String name = "DC Validator";
        validator.setName(name);
        assertEquals(name, validator.getName());
    }
    
    @Test
    @DisplayName("Should set and get description correctly")
    void testDescriptionProperty() {
        String description = "Validates Dublin Core metadata";
        validator.setDescription(description);
        assertEquals(description, validator.getDescription());
    }
    
    @Test
    @DisplayName("Should handle null description")
    void testNullDescription() {
        validator.setDescription(null);
        assertNull(validator.getDescription());
    }
    
    @Test
    @DisplayName("Should add rules to validator")
    void testAddRules() {
        ValidatorRule rule1 = new ValidatorRule();
        rule1.setName("Rule 1");
        
        ValidatorRule rule2 = new ValidatorRule();
        rule2.setName("Rule 2");
        
        validator.getRules().add(rule1);
        validator.getRules().add(rule2);
        
        assertEquals(2, validator.getRules().size());
        assertTrue(validator.getRules().contains(rule1));
        assertTrue(validator.getRules().contains(rule2));
    }
    
    @Test
    @DisplayName("Should remove rules from validator")
    void testRemoveRules() {
        ValidatorRule rule = new ValidatorRule();
        rule.setName("Test Rule");
        
        validator.getRules().add(rule);
        assertEquals(1, validator.getRules().size());
        
        validator.getRules().remove(rule);
        assertEquals(0, validator.getRules().size());
    }
    
    @Test
    @DisplayName("Should clear all rules")
    void testClearRules() {
        validator.getRules().add(new ValidatorRule());
        validator.getRules().add(new ValidatorRule());
        validator.getRules().add(new ValidatorRule());
        
        assertEquals(3, validator.getRules().size());
        
        validator.getRules().clear();
        assertEquals(0, validator.getRules().size());
    }
    
    @Test
    @DisplayName("Should reset id to null")
    void testResetId() {
        validator.setId(100L);
        assertEquals(100L, validator.getId());
        
        validator.resetId();
        assertNull(validator.getId());
    }
    
    @Test
    @DisplayName("Should reset all rule ids when resetting validator id")
    void testResetIdWithRules() {
        ValidatorRule rule1 = new ValidatorRule();
        rule1.setId(1L);
        
        ValidatorRule rule2 = new ValidatorRule();
        rule2.setId(2L);
        
        ValidatorRule rule3 = new ValidatorRule();
        rule3.setId(3L);
        
        validator.getRules().add(rule1);
        validator.getRules().add(rule2);
        validator.getRules().add(rule3);
        validator.setId(100L);
        
        validator.resetId();
        
        assertNull(validator.getId());
        assertNull(rule1.getId());
        assertNull(rule2.getId());
        assertNull(rule3.getId());
    }
    
    @Test
    @DisplayName("Should handle empty rules list when resetting id")
    void testResetIdWithEmptyRules() {
        validator.setId(100L);
        validator.resetId();
        
        assertNull(validator.getId());
        assertTrue(validator.getRules().isEmpty());
    }
    
    @Test
    @DisplayName("Should maintain rules order")
    void testRulesOrder() {
        ValidatorRule rule1 = new ValidatorRule();
        rule1.setName("First");
        
        ValidatorRule rule2 = new ValidatorRule();
        rule2.setName("Second");
        
        ValidatorRule rule3 = new ValidatorRule();
        rule3.setName("Third");
        
        validator.getRules().add(rule1);
        validator.getRules().add(rule2);
        validator.getRules().add(rule3);
        
        assertEquals("First", validator.getRules().get(0).getName());
        assertEquals("Second", validator.getRules().get(1).getName());
        assertEquals("Third", validator.getRules().get(2).getName());
    }
    
    @Test
    @DisplayName("Should create validator with complex configuration")
    void testComplexValidator() {
        validator.setId(1L);
        validator.setName("OpenAIRE Validator");
        validator.setDescription("Validates metadata according to OpenAIRE guidelines");
        
        ValidatorRule titleRule = new ValidatorRule();
        titleRule.setName("Title Rule");
        titleRule.setMandatory(true);
        titleRule.setQuantifier(QuantifierValues.ONE_ONLY);
        
        ValidatorRule authorRule = new ValidatorRule();
        authorRule.setName("Author Rule");
        authorRule.setMandatory(true);
        authorRule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        
        ValidatorRule subjectRule = new ValidatorRule();
        subjectRule.setName("Subject Rule");
        subjectRule.setMandatory(false);
        subjectRule.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        
        validator.getRules().add(titleRule);
        validator.getRules().add(authorRule);
        validator.getRules().add(subjectRule);
        
        assertEquals(1L, validator.getId());
        assertEquals("OpenAIRE Validator", validator.getName());
        assertEquals(3, validator.getRules().size());
        
        // Verify first rule
        assertEquals("Title Rule", validator.getRules().get(0).getName());
        assertTrue(validator.getRules().get(0).getMandatory());
        assertEquals(QuantifierValues.ONE_ONLY, validator.getRules().get(0).getQuantifier());
        
        // Verify second rule
        assertEquals("Author Rule", validator.getRules().get(1).getName());
        assertTrue(validator.getRules().get(1).getMandatory());
        assertEquals(QuantifierValues.ONE_OR_MORE, validator.getRules().get(1).getQuantifier());
        
        // Verify third rule
        assertEquals("Subject Rule", validator.getRules().get(2).getName());
        assertFalse(validator.getRules().get(2).getMandatory());
        assertEquals(QuantifierValues.ZERO_OR_MORE, validator.getRules().get(2).getQuantifier());
    }
    
    @Test
    @DisplayName("Should handle rule replacement")
    void testRuleReplacement() {
        ValidatorRule oldRule = new ValidatorRule();
        oldRule.setName("Old Rule");
        validator.getRules().add(oldRule);
        
        ValidatorRule newRule = new ValidatorRule();
        newRule.setName("New Rule");
        
        validator.getRules().set(0, newRule);
        
        assertEquals(1, validator.getRules().size());
        assertEquals("New Rule", validator.getRules().get(0).getName());
    }
    
    @Test
    @DisplayName("Should allow multiple validators with different configurations")
    void testMultipleValidators() {
        Validator validator1 = new Validator();
        validator1.setName("Validator 1");
        validator1.getRules().add(new ValidatorRule());
        
        Validator validator2 = new Validator();
        validator2.setName("Validator 2");
        validator2.getRules().add(new ValidatorRule());
        validator2.getRules().add(new ValidatorRule());
        
        assertEquals(1, validator1.getRules().size());
        assertEquals(2, validator2.getRules().size());
        assertNotEquals(validator1.getName(), validator2.getName());
    }
}
