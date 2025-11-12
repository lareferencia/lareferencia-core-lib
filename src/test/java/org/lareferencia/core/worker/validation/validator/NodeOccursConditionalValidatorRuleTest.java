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

package org.lareferencia.core.worker.validation.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.QuantifierValues;
import org.lareferencia.core.worker.validation.ValidatorRuleResult;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("NodeOccursConditionalValidatorRule Tests")
class NodeOccursConditionalValidatorRuleTest {

    private NodeOccursConditionalValidatorRule rule;
    private OAIRecordMetadata metadata;
    private Node mockNode1;
    private Node mockNode2;
    private Node mockNode3;

    @BeforeEach
    void setUp() {
        rule = new NodeOccursConditionalValidatorRule();
        metadata = mock(OAIRecordMetadata.class);
        when(metadata.getIdentifier()).thenReturn("test:123");
        
        mockNode1 = mock(Node.class);
        mockNode2 = mock(Node.class);
        mockNode3 = mock(Node.class);
        when(mockNode1.getNodeName()).thenReturn("dc:title");
        when(mockNode2.getNodeName()).thenReturn("dc:title");
        when(mockNode3.getNodeName()).thenReturn("dc:creator");
    }

    @Test
    @DisplayName("Should validate with ONE_ONLY quantifier - exactly one occurrence")
    void testOneOnlyQuantifierExactlyOne() {
        rule.setQuantifier(QuantifierValues.ONE_ONLY);
        rule.setSourceXPathExpression("//dc:title");
        when(metadata.getFieldNodesByXPath("//dc:title")).thenReturn(Arrays.asList(mockNode1));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        assertEquals(1, result.getResults().size());
        assertTrue(result.getResults().get(0).isValid());
        assertEquals("dc:title", result.getResults().get(0).getReceivedValue());
    }

    @Test
    @DisplayName("Should fail with ONE_ONLY quantifier - multiple occurrences")
    void testOneOnlyQuantifierMultiple() {
        rule.setQuantifier(QuantifierValues.ONE_ONLY);
        rule.setSourceXPathExpression("//dc:title");
        when(metadata.getFieldNodesByXPath("//dc:title")).thenReturn(Arrays.asList(mockNode1, mockNode2));

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
        assertEquals(2, result.getResults().size());
    }

    @Test
    @DisplayName("Should fail with ONE_ONLY quantifier - no occurrences")
    void testOneOnlyQuantifierNone() {
        rule.setQuantifier(QuantifierValues.ONE_ONLY);
        rule.setSourceXPathExpression("//dc:nonexistent");
        when(metadata.getFieldNodesByXPath("//dc:nonexistent")).thenReturn(Collections.emptyList());

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
        assertEquals(1, result.getResults().size());
        assertFalse(result.getResults().get(0).isValid());
        assertEquals("no_occurrences_found", result.getResults().get(0).getReceivedValue());
    }

    @Test
    @DisplayName("Should validate with ONE_OR_MORE quantifier - single occurrence")
    void testOneOrMoreQuantifierSingle() {
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//dc:creator");
        when(metadata.getFieldNodesByXPath("//dc:creator")).thenReturn(Arrays.asList(mockNode3));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        assertEquals(1, result.getResults().size());
    }

    @Test
    @DisplayName("Should validate with ONE_OR_MORE quantifier - multiple occurrences")
    void testOneOrMoreQuantifierMultiple() {
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//dc:title");
        when(metadata.getFieldNodesByXPath("//dc:title")).thenReturn(Arrays.asList(mockNode1, mockNode2));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        assertEquals(2, result.getResults().size());
    }

    @Test
    @DisplayName("Should fail with ONE_OR_MORE quantifier - no occurrences")
    void testOneOrMoreQuantifierNone() {
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//dc:nonexistent");
        when(metadata.getFieldNodesByXPath("//dc:nonexistent")).thenReturn(Collections.emptyList());

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
        assertEquals(1, result.getResults().size());
        assertEquals("no_occurrences_found", result.getResults().get(0).getReceivedValue());
    }

    @Test
    @DisplayName("Should validate with ZERO_OR_MORE quantifier - no occurrences")
    void testZeroOrMoreQuantifierNone() {
        rule.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        rule.setSourceXPathExpression("//dc:optional");
        when(metadata.getFieldNodesByXPath("//dc:optional")).thenReturn(Collections.emptyList());

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid()); // >= 0 always true
    }

    @Test
    @DisplayName("Should validate with ZERO_OR_MORE quantifier - some occurrences")
    void testZeroOrMoreQuantifierSome() {
        rule.setQuantifier(QuantifierValues.ZERO_OR_MORE);
        rule.setSourceXPathExpression("//dc:subject");
        when(metadata.getFieldNodesByXPath("//dc:subject")).thenReturn(Arrays.asList(mockNode1));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should validate with ZERO_ONLY quantifier - no occurrences")
    void testZeroOnlyQuantifierNone() {
        rule.setQuantifier(QuantifierValues.ZERO_ONLY);
        rule.setSourceXPathExpression("//dc:embargo");
        when(metadata.getFieldNodesByXPath("//dc:embargo")).thenReturn(Collections.emptyList());

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        assertEquals(1, result.getResults().size());
        assertEquals("no_occurrences_found", result.getResults().get(0).getReceivedValue());
    }

    @Test
    @DisplayName("Should fail with ZERO_ONLY quantifier - has occurrences")
    void testZeroOnlyQuantifierHasOccurrences() {
        rule.setQuantifier(QuantifierValues.ZERO_ONLY);
        rule.setSourceXPathExpression("//dc:embargo");
        when(metadata.getFieldNodesByXPath("//dc:embargo")).thenReturn(Arrays.asList(mockNode1));

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
    }

    @Test
    @DisplayName("Should validate with ALL quantifier - has occurrences")
    void testAllQuantifierHasOccurrences() {
        rule.setQuantifier(QuantifierValues.ALL);
        rule.setSourceXPathExpression("//dc:identifier");
        when(metadata.getFieldNodesByXPath("//dc:identifier")).thenReturn(Arrays.asList(mockNode1, mockNode2, mockNode3));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid()); // ALL: occurrencesCount > 0
        assertEquals(3, result.getResults().size());
    }

    @Test
    @DisplayName("Should fail with ALL quantifier - no occurrences")
    void testAllQuantifierNoOccurrences() {
        rule.setQuantifier(QuantifierValues.ALL);
        rule.setSourceXPathExpression("//dc:none");
        when(metadata.getFieldNodesByXPath("//dc:none")).thenReturn(Collections.emptyList());

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid()); // ALL: occurrencesCount must be > 0
    }

    @Test
    @DisplayName("Should handle complex XPath expression")
    void testComplexXPath() {
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//dc:relation[@type='isPartOf']");
        when(metadata.getFieldNodesByXPath("//dc:relation[@type='isPartOf']")).thenReturn(Arrays.asList(mockNode1));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        verify(metadata).getFieldNodesByXPath("//dc:relation[@type='isPartOf']");
    }

    @Test
    @DisplayName("Should set rule reference in result")
    void testRuleReferenceInResult() {
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//dc:title");
        when(metadata.getFieldNodesByXPath("//dc:title")).thenReturn(Arrays.asList(mockNode1));

        ValidatorRuleResult result = rule.validate(metadata);

        assertNotNull(result.getRule());
        assertSame(rule, result.getRule());
    }

    @Test
    @DisplayName("Should return all node names in results")
    void testAllNodeNamesInResults() {
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//dc:*");
        when(metadata.getFieldNodesByXPath("//dc:*")).thenReturn(Arrays.asList(mockNode1, mockNode2, mockNode3));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        assertEquals(3, result.getResults().size());
        assertEquals("dc:title", result.getResults().get(0).getReceivedValue());
        assertEquals("dc:title", result.getResults().get(1).getReceivedValue());
        assertEquals("dc:creator", result.getResults().get(2).getReceivedValue());
    }

    @Test
    @DisplayName("Should handle XPath with namespace prefix")
    void testXPathWithNamespace() {
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//oai_dc:dc/dc:title");
        when(metadata.getFieldNodesByXPath("//oai_dc:dc/dc:title")).thenReturn(Arrays.asList(mockNode1));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        verify(metadata).getFieldNodesByXPath("//oai_dc:dc/dc:title");
    }

    @Test
    @DisplayName("Should mark all node occurrences as valid")
    void testAllNodeOccurrencesMarkedValid() {
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//dc:contributor");
        when(metadata.getFieldNodesByXPath("//dc:contributor")).thenReturn(Arrays.asList(mockNode1, mockNode2));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        assertEquals(2, result.getResults().size());
        assertTrue(result.getResults().get(0).isValid());
        assertTrue(result.getResults().get(1).isValid());
    }

    @Test
    @DisplayName("Should handle different node types")
    void testDifferentNodeTypes() {
        Node textNode = mock(Node.class);
        when(textNode.getNodeName()).thenReturn("#text");
        
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//text()");
        when(metadata.getFieldNodesByXPath("//text()")).thenReturn(Arrays.asList(textNode));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        assertEquals("#text", result.getResults().get(0).getReceivedValue());
    }

    @Test
    @DisplayName("Should handle attribute nodes")
    void testAttributeNodes() {
        Node attrNode = mock(Node.class);
        when(attrNode.getNodeName()).thenReturn("type");
        
        rule.setQuantifier(QuantifierValues.ONE_OR_MORE);
        rule.setSourceXPathExpression("//@type");
        when(metadata.getFieldNodesByXPath("//@type")).thenReturn(Arrays.asList(attrNode));

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        assertEquals("type", result.getResults().get(0).getReceivedValue());
    }
}
