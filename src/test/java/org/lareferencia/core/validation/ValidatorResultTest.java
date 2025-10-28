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

package org.lareferencia.core.validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.lareferencia.backend.validation.validator.ContentValidatorResult;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("ValidatorResult Unit Tests")
class ValidatorResultTest {

    private ValidatorResult validatorResult;

    @BeforeEach
    void setUp() {
        validatorResult = new ValidatorResult();
    }

    @Test
    @DisplayName("Should create ValidatorResult with default values")
    void testDefaultConstructor() {
        assertNotNull(validatorResult);
        assertFalse(validatorResult.isValid());
        assertNotNull(validatorResult.getRulesResults());
        assertTrue(validatorResult.getRulesResults().isEmpty());
    }

    @Test
    @DisplayName("Should set and get valid property")
    void testSetAndGetValid() {
        validatorResult.setValid(true);
        assertTrue(validatorResult.isValid());

        validatorResult.setValid(false);
        assertFalse(validatorResult.isValid());
    }

    @Test
    @DisplayName("Should add rule results")
    void testAddRuleResults() {
        ValidatorRuleResult ruleResult1 = new ValidatorRuleResult();
        ValidatorRuleResult ruleResult2 = new ValidatorRuleResult();

        validatorResult.getRulesResults().add(ruleResult1);
        validatorResult.getRulesResults().add(ruleResult2);

        assertEquals(2, validatorResult.getRulesResults().size());
        assertTrue(validatorResult.getRulesResults().contains(ruleResult1));
        assertTrue(validatorResult.getRulesResults().contains(ruleResult2));
    }

    @Test
    @DisplayName("Should reset validator result")
    void testReset() {
        validatorResult.setValid(true);
        ValidatorRuleResult ruleResult = new ValidatorRuleResult();
        validatorResult.getRulesResults().add(ruleResult);

        validatorResult.reset();

        assertFalse(validatorResult.isValid());
        assertTrue(validatorResult.getRulesResults().isEmpty());
    }

    @Test
    @DisplayName("Should generate validation content details")
    void testGetValidationContentDetails() {
        // Create mock rule
        AbstractValidatorRule mockRule = new AbstractValidatorRule() {
            @Override
            public ValidatorRuleResult validate(org.lareferencia.core.metadata.OAIRecordMetadata metadata) {
                return null;
            }
        };
        mockRule.setRuleId(123L);

        ValidatorRuleResult ruleResult = new ValidatorRuleResult();
        ruleResult.setRule(mockRule);

        ContentValidatorResult contentResult1 = new ContentValidatorResult();
        contentResult1.setReceivedValue("value1");
        
        ContentValidatorResult contentResult2 = new ContentValidatorResult();
        contentResult2.setReceivedValue("value2");

        List<ContentValidatorResult> results = new ArrayList<>();
        results.add(contentResult1);
        results.add(contentResult2);
        ruleResult.setResults(results);

        validatorResult.getRulesResults().add(ruleResult);

        String details = validatorResult.getValidationContentDetails();
        assertNotNull(details);
        assertTrue(details.contains("123:value1"));
        assertTrue(details.contains("123:value2"));
    }

    @Test
    @DisplayName("Should generate validation content details without trailing semicolon")
    void testGetValidationContentDetailsNoTrailingSemicolon() {
        AbstractValidatorRule mockRule = new AbstractValidatorRule() {
            @Override
            public ValidatorRuleResult validate(org.lareferencia.core.metadata.OAIRecordMetadata metadata) {
                return null;
            }
        };
        mockRule.setRuleId(1L);

        ValidatorRuleResult ruleResult = new ValidatorRuleResult();
        ruleResult.setRule(mockRule);

        ContentValidatorResult contentResult = new ContentValidatorResult();
        contentResult.setReceivedValue("test");

        List<ContentValidatorResult> results = new ArrayList<>();
        results.add(contentResult);
        ruleResult.setResults(results);

        validatorResult.getRulesResults().add(ruleResult);

        String details = validatorResult.getValidationContentDetails();
        assertFalse(details.endsWith(";"));
    }

    @Test
    @DisplayName("Should generate empty string for validation content details with no results")
    void testGetValidationContentDetailsEmpty() {
        String details = validatorResult.getValidationContentDetails();
        assertNotNull(details);
        assertEquals("", details);
    }

    @Test
    @DisplayName("Should generate toString representation")
    void testToString() {
        validatorResult.setValid(true);
        
        String result = validatorResult.toString();
        assertNotNull(result);
        assertTrue(result.contains("Validation:"));
        assertTrue(result.contains("record valid=true"));
    }

    @Test
    @DisplayName("Should handle multiple rule results in toString")
    void testToStringWithMultipleRules() {
        AbstractValidatorRule mockRule1 = new AbstractValidatorRule() {
            @Override
            public ValidatorRuleResult validate(org.lareferencia.core.metadata.OAIRecordMetadata metadata) {
                return null;
            }
        };
        mockRule1.setRuleId(1L);

        AbstractValidatorRule mockRule2 = new AbstractValidatorRule() {
            @Override
            public ValidatorRuleResult validate(org.lareferencia.core.metadata.OAIRecordMetadata metadata) {
                return null;
            }
        };
        mockRule2.setRuleId(2L);

        ValidatorRuleResult ruleResult1 = new ValidatorRuleResult();
        ruleResult1.setRule(mockRule1);

        ValidatorRuleResult ruleResult2 = new ValidatorRuleResult();
        ruleResult2.setRule(mockRule2);

        validatorResult.getRulesResults().add(ruleResult1);
        validatorResult.getRulesResults().add(ruleResult2);

        String result = validatorResult.toString();
        assertTrue(result.contains("1:"));
        assertTrue(result.contains("2:"));
    }
}
