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

package org.lareferencia.core.worker.validation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AbstractValidatorRule Unit Tests")
class AbstractValidatorRuleTest {

    private TestValidatorRule validatorRule;

    // Concrete implementation for testing
    private static class TestValidatorRule extends AbstractValidatorRule {
        @Override
        public ValidatorRuleResult validate(org.lareferencia.core.metadata.OAIRecordMetadata metadata) {
            return new ValidatorRuleResult();
        }
    }

    @BeforeEach
    void setUp() {
        validatorRule = new TestValidatorRule();
    }

    @Test
    @DisplayName("Should initialize with default values")
    void testDefaultInitialization() {
        assertNotNull(validatorRule);
        assertFalse(validatorRule.getMandatory());
        assertEquals(QuantifierValues.ONE_OR_MORE, validatorRule.getQuantifier());
        assertNull(validatorRule.getRuleId());
    }

    @Test
    @DisplayName("Should set and get ruleId")
    void testSetAndGetRuleId() {
        Long expectedRuleId = 12345L;
        validatorRule.setRuleId(expectedRuleId);
        
        assertEquals(expectedRuleId, validatorRule.getRuleId());
    }

    @Test
    @DisplayName("Should set and get mandatory flag")
    void testSetAndGetMandatory() {
        validatorRule.setMandatory(true);
        assertTrue(validatorRule.getMandatory());

        validatorRule.setMandatory(false);
        assertFalse(validatorRule.getMandatory());
    }

    @Test
    @DisplayName("Should set and get quantifier")
    void testSetAndGetQuantifier() {
        validatorRule.setQuantifier(QuantifierValues.ONE_ONLY);
        assertEquals(QuantifierValues.ONE_ONLY, validatorRule.getQuantifier());

        validatorRule.setQuantifier(QuantifierValues.ZERO_ONLY);
        assertEquals(QuantifierValues.ZERO_ONLY, validatorRule.getQuantifier());

        validatorRule.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        assertEquals(QuantifierValues.ZERO_OR_MORE, validatorRule.getQuantifier());
    }

    @Test
    @DisplayName("Should handle null ruleId")
    void testNullRuleId() {
        validatorRule.setRuleId(null);
        assertNull(validatorRule.getRuleId());
    }

    @Test
    @DisplayName("Should handle null quantifier")
    void testNullQuantifier() {
        validatorRule.setQuantifier(null);
        assertNull(validatorRule.getQuantifier());
    }

    @Test
    @DisplayName("Should maintain state across property changes")
    void testStateConsistency() {
        Long ruleId = 999L;
        Boolean mandatory = true;
        QuantifierValues quantifier = QuantifierValues.ONE_ONLY;

        validatorRule.setRuleId(ruleId);
        validatorRule.setMandatory(mandatory);
        validatorRule.setQuantifier(quantifier);

        assertEquals(ruleId, validatorRule.getRuleId());
        assertEquals(mandatory, validatorRule.getMandatory());
        assertEquals(quantifier, validatorRule.getQuantifier());
    }

    @Test
    @DisplayName("Should implement IValidatorRule interface")
    void testImplementsInterface() {
        assertTrue(validatorRule instanceof IValidatorRule);
    }

    @Test
    @DisplayName("Should be able to call validate method")
    void testValidateMethod() {
        ValidatorRuleResult result = validatorRule.validate(null);
        assertNotNull(result);
    }
}
