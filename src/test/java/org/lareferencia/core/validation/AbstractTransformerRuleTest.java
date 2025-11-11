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
import org.lareferencia.backend.domain.IOAIRecord;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.NetworkRunningContext;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("AbstractTransformerRule Unit Tests")
class AbstractTransformerRuleTest {

    private TestTransformerRule transformerRule;

    // Concrete implementation for testing
    private static class TestTransformerRule extends AbstractTransformerRule {
        private boolean transformCalled = false;
        private boolean shouldReturnTrue = false;

        @Override
        public boolean transform(NetworkRunningContext context, IOAIRecord record, OAIRecordMetadata metadata) throws ValidationException {
            transformCalled = true;
            return shouldReturnTrue;
        }

        public boolean isTransformCalled() {
            return transformCalled;
        }

        public void setShouldReturnTrue(boolean value) {
            this.shouldReturnTrue = value;
        }
    }

    @BeforeEach
    void setUp() {
        transformerRule = new TestTransformerRule();
    }

    @Test
    @DisplayName("Should create transformer rule with default values")
    void testDefaultConstructor() {
        assertNotNull(transformerRule);
        assertNull(transformerRule.getRuleId());
    }

    @Test
    @DisplayName("Should set and get ruleId")
    void testSetAndGetRuleId() {
        Long expectedRuleId = 54321L;
        transformerRule.setRuleId(expectedRuleId);
        
        assertEquals(expectedRuleId, transformerRule.getRuleId());
    }

    @Test
    @DisplayName("Should handle null ruleId")
    void testNullRuleId() {
        transformerRule.setRuleId(null);
        assertNull(transformerRule.getRuleId());
    }

    @Test
    @DisplayName("Should implement ITransformerRule interface")
    void testImplementsInterface() {
        assertTrue(transformerRule instanceof ITransformerRule);
    }

    @Test
    @DisplayName("Should call transform method and return false")
    void testTransformReturnsFalse() throws ValidationException {
        OAIRecord mockRecord = new OAIRecord();
        OAIRecordMetadata mockMetadata = null;
        try {
            mockMetadata = new OAIRecordMetadata("test:identifier");
        } catch (Exception e) {
            fail("Failed to create OAIRecordMetadata: " + e.getMessage());
        }

        transformerRule.setShouldReturnTrue(false);
        boolean result = transformerRule.transform(null, mockRecord, mockMetadata);

        assertFalse(result);
        assertTrue(transformerRule.isTransformCalled());
    }

    @Test
    @DisplayName("Should call transform method and return true")
    void testTransformReturnsTrue() throws ValidationException {
        OAIRecord mockRecord = new OAIRecord();
        OAIRecordMetadata mockMetadata = null;
        try {
            mockMetadata = new OAIRecordMetadata("test:identifier");
        } catch (Exception e) {
            fail("Failed to create OAIRecordMetadata: " + e.getMessage());
        }

        transformerRule.setShouldReturnTrue(true);
        boolean result = transformerRule.transform(null, mockRecord, mockMetadata);

        assertTrue(result);
        assertTrue(transformerRule.isTransformCalled());
    }

    @Test
    @DisplayName("Should handle null record in transform")
    void testTransformWithNullRecord() throws ValidationException {
        OAIRecordMetadata mockMetadata = null;
        try {
            mockMetadata = new OAIRecordMetadata("test:identifier");
        } catch (Exception e) {
            fail("Failed to create OAIRecordMetadata: " + e.getMessage());
        }
        boolean result = transformerRule.transform(null, null, mockMetadata);
        assertFalse(result);
        assertTrue(transformerRule.isTransformCalled());
    }

    @Test
    @DisplayName("Should handle null metadata in transform")
    void testTransformWithNullMetadata() throws ValidationException {
        boolean result = transformerRule.transform(null, new OAIRecord(), null);
        assertFalse(result);
        assertTrue(transformerRule.isTransformCalled());
    }

    @Test
    @DisplayName("Should handle null record and metadata in transform")
    void testTransformWithNullRecordAndMetadata() throws ValidationException {
        boolean result = transformerRule.transform(null, null, null);
        assertFalse(result);
        assertTrue(transformerRule.isTransformCalled());
    }

    @Test
    @DisplayName("Should maintain ruleId after transform")
    void testRuleIdPersistence() throws ValidationException {
        Long expectedRuleId = 999L;
        transformerRule.setRuleId(expectedRuleId);

        try {
            OAIRecordMetadata mockMetadata = new OAIRecordMetadata("test:identifier");
            transformerRule.transform(null, new OAIRecord(), mockMetadata);
        } catch (Exception e) {
            fail("Failed to create OAIRecordMetadata: " + e.getMessage());
        }

        assertEquals(expectedRuleId, transformerRule.getRuleId());
    }
}
