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

package org.lareferencia.core.service.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.domain.ValidatorRule;
import org.lareferencia.core.worker.validation.validator.RegexFieldContentValidatorRule;
import org.lareferencia.core.worker.validation.validator.ControlledValueFieldContentValidatorRule;
import org.lareferencia.core.worker.validation.QuantifierValues;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Validator Domain to Implementation Integration Tests")
class ValidatorDomainIntegrationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Should create Validator with ValidatorRule and serialize/deserialize RegexFieldContentValidatorRule")
    void testValidatorWithRegexRule() throws Exception {
        // Create domain entities
        Validator validator = new Validator();
        validator.setName("Email Validator");
        validator.setDescription("Validates email format");

        ValidatorRule rule = new ValidatorRule();
        rule.setName("Email Regex");
        rule.setDescription("Regex for email validation");
        rule.setMandatory(true);
        rule.setQuantifier(QuantifierValues.ONE_ONLY);

        // Create implementation
        RegexFieldContentValidatorRule regexRule = new RegexFieldContentValidatorRule();
        regexRule.setFieldname("dc.contributor.author");
        regexRule.setRegexString("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$");

        // Serialize implementation to JSON
        String json = objectMapper.writeValueAsString(regexRule);
        rule.setJsonserialization(json);

        // Add rule to validator
        validator.getRules().add(rule);

        // Verify validator structure
        assertEquals("Email Validator", validator.getName());
        assertEquals(1, validator.getRules().size());
        assertEquals("Email Regex", validator.getRules().get(0).getName());
        assertTrue(validator.getRules().get(0).getMandatory());

        // Verify JSON contains @class
        assertTrue(json.contains("@class"), "JSON should contain @class property: " + json);
        assertTrue(json.contains("RegexFieldContentValidatorRule"), "JSON should contain class name");

        // Deserialize back to concrete class (works because JSON has @class)
        RegexFieldContentValidatorRule deserializedRule = objectMapper.readValue(
            validator.getRules().get(0).getJsonserialization(),
            RegexFieldContentValidatorRule.class
        );

        assertNotNull(deserializedRule);
        assertEquals("dc.contributor.author", deserializedRule.getFieldname());
        assertEquals("^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$", deserializedRule.getRegexString());
    }

    @Test
    @DisplayName("Should handle multiple ValidatorRules in single Validator")
    void testValidatorWithMultipleRules() throws Exception {
        Validator validator = new Validator();
        validator.setName("Publication Validator");

        // Rule 1: Regex
        ValidatorRule rule1 = new ValidatorRule();
        rule1.setName("Year Format");
        rule1.setMandatory(true);
        
        RegexFieldContentValidatorRule regexRule = new RegexFieldContentValidatorRule();
        regexRule.setFieldname("dc.date.issued");
        regexRule.setRegexString("^\\d{4}$");
        rule1.setJsonserialization(objectMapper.writeValueAsString(regexRule));

        // Rule 2: Controlled Value
        ValidatorRule rule2 = new ValidatorRule();
        rule2.setName("Language Check");
        rule2.setMandatory(false);
        
        ControlledValueFieldContentValidatorRule controlledRule = new ControlledValueFieldContentValidatorRule();
        controlledRule.setFieldname("dc.language");
        controlledRule.getControlledValues().add("eng");
        controlledRule.getControlledValues().add("spa");
        controlledRule.getControlledValues().add("por");
        controlledRule.getControlledValues().add("fre");
        rule2.setJsonserialization(objectMapper.writeValueAsString(controlledRule));

        validator.getRules().add(rule1);
        validator.getRules().add(rule2);

        // Verify
        assertEquals(2, validator.getRules().size());
        
        // Deserialize first rule to concrete type
        RegexFieldContentValidatorRule restored1 = objectMapper.readValue(
            validator.getRules().get(0).getJsonserialization(),
            RegexFieldContentValidatorRule.class
        );
        assertNotNull(restored1);
        assertEquals("dc.date.issued", restored1.getFieldname());
        
        // Deserialize second rule to concrete type
        ControlledValueFieldContentValidatorRule restored2 = objectMapper.readValue(
            validator.getRules().get(1).getJsonserialization(),
            ControlledValueFieldContentValidatorRule.class
        );
        assertNotNull(restored2);
        assertEquals("dc.language", restored2.getFieldname());
        assertTrue(restored2.getControlledValues().contains("eng"));
    }

    @Test
    @DisplayName("Should preserve rule order in Validator")
    void testRuleOrder() throws Exception {
        Validator validator = new Validator();
        
        for (int i = 0; i < 5; i++) {
            ValidatorRule rule = new ValidatorRule();
            rule.setName("Rule " + i);
            
            RegexFieldContentValidatorRule regexRule = new RegexFieldContentValidatorRule();
            regexRule.setFieldname("field" + i);
            regexRule.setRegexString(".*");
            
            rule.setJsonserialization(objectMapper.writeValueAsString(regexRule));
            validator.getRules().add(rule);
        }

        assertEquals(5, validator.getRules().size());
        for (int i = 0; i < 5; i++) {
            assertEquals("Rule " + i, validator.getRules().get(i).getName());
            
            RegexFieldContentValidatorRule restored = objectMapper.readValue(
                validator.getRules().get(i).getJsonserialization(),
                RegexFieldContentValidatorRule.class
            );
            assertEquals("field" + i, restored.getFieldname());
        }
    }

    @Test
    @DisplayName("Should handle resetId() for Validator and all rules")
    void testResetId() {
        Validator validator = new Validator();
        validator.setName("Test Validator");
        
        // Simulate persisted entities with IDs
        validator.setId(100L);
        
        ValidatorRule rule1 = new ValidatorRule();
        rule1.setId(1L);
        rule1.setName("Rule 1");
        
        ValidatorRule rule2 = new ValidatorRule();
        rule2.setId(2L);
        rule2.setName("Rule 2");
        
        validator.getRules().add(rule1);
        validator.getRules().add(rule2);

        // Reset IDs
        validator.resetId();

        // Verify all IDs are null
        assertNull(validator.getId());
        assertNull(validator.getRules().get(0).getId());
        assertNull(validator.getRules().get(1).getId());
    }

    @Test
    @DisplayName("Should handle empty rules list in Validator")
    void testEmptyRulesList() {
        Validator validator = new Validator();
        validator.setName("Empty Validator");
        
        assertNotNull(validator.getRules());
        assertEquals(0, validator.getRules().size());
    }

    @Test
    @DisplayName("Should handle ValidatorRule with null jsonserialization")
    void testNullJsonSerialization() {
        ValidatorRule rule = new ValidatorRule();
        rule.setName("Incomplete Rule");
        rule.setMandatory(false);
        
        assertNull(rule.getJsonserialization());
    }

    @Test
    @DisplayName("Should preserve mandatory and quantifier values")
    void testMandatoryAndQuantifier() {
        ValidatorRule rule = new ValidatorRule();
        rule.setMandatory(true);
        rule.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        
        assertTrue(rule.getMandatory());
        assertEquals(QuantifierValues.ZERO_OR_MORE, rule.getQuantifier());
    }

    @Test
    @DisplayName("Should handle default values for ValidatorRule")
    void testDefaultValues() {
        ValidatorRule rule = new ValidatorRule();
        
        assertFalse(rule.getMandatory());
        assertEquals(QuantifierValues.ONE_OR_MORE, rule.getQuantifier());
        assertNull(rule.getId());
        assertNull(rule.getJsonserialization());
    }

    @Test
    @DisplayName("Should serialize and deserialize complex ControlledValueFieldContentValidatorRule")
    void testControlledValueRuleSerialization() throws Exception {
        ValidatorRule rule = new ValidatorRule();
        rule.setName("Access Rights Validator");
        rule.setMandatory(true);
        
        ControlledValueFieldContentValidatorRule controlledRule = new ControlledValueFieldContentValidatorRule();
        controlledRule.setFieldname("dc.rights.accessRights");
        controlledRule.getControlledValues().add("open access");
        controlledRule.getControlledValues().add("embargoed access");
        controlledRule.getControlledValues().add("restricted access");
        controlledRule.getControlledValues().add("metadata only access");
        
        String json = objectMapper.writeValueAsString(controlledRule);
        rule.setJsonserialization(json);

        // Deserialize to concrete type
        ControlledValueFieldContentValidatorRule restored = objectMapper.readValue(
            rule.getJsonserialization(), 
            ControlledValueFieldContentValidatorRule.class
        );
        
        assertNotNull(restored);
        assertEquals("dc.rights.accessRights", restored.getFieldname());
        assertTrue(restored.getControlledValues().contains("open access"));
        assertTrue(restored.getControlledValues().contains("embargoed access"));
    }

    @Test
    @DisplayName("Should handle Validator with description")
    void testValidatorDescription() {
        Validator validator = new Validator();
        validator.setName("Complete Validator");
        validator.setDescription("This validator checks all required metadata fields");
        
        assertEquals("Complete Validator", validator.getName());
        assertEquals("This validator checks all required metadata fields", validator.getDescription());
    }

    @Test
    @DisplayName("Should handle ValidatorRule with description")
    void testValidatorRuleDescription() {
        ValidatorRule rule = new ValidatorRule();
        rule.setName("DOI Check");
        rule.setDescription("Validates DOI format according to DOI handbook");
        
        assertEquals("DOI Check", rule.getName());
        assertEquals("Validates DOI format according to DOI handbook", rule.getDescription());
    }

    @Test
    @DisplayName("Should handle roundtrip serialization of Validator with all rules")
    void testFullValidatorRoundtrip() throws Exception {
        // Create complete validator
        Validator original = new Validator();
        original.setName("Complete Publication Validator");
        original.setDescription("Validates all aspects of publication metadata");

        // Add multiple rule types
        ValidatorRule regexRule = new ValidatorRule();
        regexRule.setName("Year Regex");
        regexRule.setMandatory(true);
        regexRule.setQuantifier(QuantifierValues.ONE_ONLY);
        RegexFieldContentValidatorRule regex = new RegexFieldContentValidatorRule();
        regex.setFieldname("dc.date");
        regex.setRegexString("^\\d{4}$");
        regexRule.setJsonserialization(objectMapper.writeValueAsString(regex));

        ValidatorRule controlledRule = new ValidatorRule();
        controlledRule.setName("Type Check");
        controlledRule.setMandatory(false);
        controlledRule.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        ControlledValueFieldContentValidatorRule controlled = new ControlledValueFieldContentValidatorRule();
        controlled.setFieldname("dc.type");
        controlled.getControlledValues().add("article");
        controlled.getControlledValues().add("book");
        controlled.getControlledValues().add("dataset");
        controlledRule.setJsonserialization(objectMapper.writeValueAsString(controlled));

        original.getRules().add(regexRule);
        original.getRules().add(controlledRule);

        // Serialize entire validator to JSON
        String validatorJson = objectMapper.writeValueAsString(original);

        // Deserialize
        Validator restored = objectMapper.readValue(validatorJson, Validator.class);

        // Verify structure
        assertEquals(original.getName(), restored.getName());
        assertEquals(original.getDescription(), restored.getDescription());
        assertEquals(2, restored.getRules().size());

        // Verify first rule
        ValidatorRule restoredRule1 = restored.getRules().get(0);
        assertEquals("Year Regex", restoredRule1.getName());
        assertTrue(restoredRule1.getMandatory());
        assertEquals(QuantifierValues.ONE_ONLY, restoredRule1.getQuantifier());

        // Verify second rule
        ValidatorRule restoredRule2 = restored.getRules().get(1);
        assertEquals("Type Check", restoredRule2.getName());
        assertFalse(restoredRule2.getMandatory());
        assertEquals(QuantifierValues.ZERO_OR_MORE, restoredRule2.getQuantifier());
    }
}
