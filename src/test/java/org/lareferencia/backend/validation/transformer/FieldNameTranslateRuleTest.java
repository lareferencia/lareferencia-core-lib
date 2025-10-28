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

package org.lareferencia.backend.validation.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("FieldNameTranslateRule Tests")
class FieldNameTranslateRuleTest {

    private FieldNameTranslateRule rule;
    private OAIRecord record;
    private OAIRecordMetadata metadata;
    private Node mockNode1;
    private Node mockNode2;
    private Node mockChildNode1;
    private Node mockChildNode2;

    @BeforeEach
    void setUp() {
        rule = new FieldNameTranslateRule();
        record = mock(OAIRecord.class);
        metadata = mock(OAIRecordMetadata.class);
        
        mockNode1 = mock(Node.class);
        mockNode2 = mock(Node.class);
        mockChildNode1 = mock(Node.class);
        mockChildNode2 = mock(Node.class);
        
        when(mockNode1.getFirstChild()).thenReturn(mockChildNode1);
        when(mockNode2.getFirstChild()).thenReturn(mockChildNode2);
        when(mockChildNode1.getNodeValue()).thenReturn("value1");
        when(mockChildNode2.getNodeValue()).thenReturn("value2");
    }

    @Test
    @DisplayName("Should rename field from source to target")
    void testRenameField() {
        rule.setSourceFieldName("dc.contributor");
        rule.setTargetFieldName("dc.creator");
        when(metadata.getFieldNodes("dc.contributor")).thenReturn(Arrays.asList(mockNode1));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.creator", "value1");
        verify(metadata).removeNode(mockNode1);
    }

    @Test
    @DisplayName("Should handle multiple occurrences")
    void testMultipleOccurrences() {
        rule.setSourceFieldName("dc.subject");
        rule.setTargetFieldName("dc.topic");
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(mockNode1, mockNode2));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.topic", "value1");
        verify(metadata).addFieldOcurrence("dc.topic", "value2");
        verify(metadata).removeNode(mockNode1);
        verify(metadata).removeNode(mockNode2);
    }

    @Test
    @DisplayName("Should return false when source field does not exist")
    void testSourceFieldNotExists() {
        rule.setSourceFieldName("dc.nonexistent");
        rule.setTargetFieldName("dc.target");
        when(metadata.getFieldNodes("dc.nonexistent")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
        verify(metadata, never()).removeNode(any(Node.class));
    }

    @Test
    @DisplayName("Should preserve field values during rename")
    void testPreserveValues() {
        rule.setSourceFieldName("old.field");
        rule.setTargetFieldName("new.field");
        when(mockChildNode1.getNodeValue()).thenReturn("Important Data");
        when(metadata.getFieldNodes("old.field")).thenReturn(Arrays.asList(mockNode1));

        rule.transform(record, metadata);

        verify(metadata).addFieldOcurrence("new.field", "Important Data");
    }

    @Test
    @DisplayName("Should handle empty field values")
    void testEmptyFieldValues() {
        rule.setSourceFieldName("dc.empty");
        rule.setTargetFieldName("dc.target");
        when(mockChildNode1.getNodeValue()).thenReturn("");
        when(metadata.getFieldNodes("dc.empty")).thenReturn(Arrays.asList(mockNode1));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.target", "");
    }

    @Test
    @DisplayName("Should handle Unicode characters in values")
    void testUnicodeCharacters() {
        rule.setSourceFieldName("dc.title");
        rule.setTargetFieldName("dc.titulo");
        when(mockChildNode1.getNodeValue()).thenReturn("Educación en España");
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(mockNode1));

        rule.transform(record, metadata);

        verify(metadata).addFieldOcurrence("dc.titulo", "Educación en España");
    }

    @Test
    @DisplayName("Should handle special characters in values")
    void testSpecialCharacters() {
        rule.setSourceFieldName("dc.rights");
        rule.setTargetFieldName("dc.license");
        when(mockChildNode1.getNodeValue()).thenReturn("CC-BY-NC-SA 4.0 <https://creativecommons.org>");
        when(metadata.getFieldNodes("dc.rights")).thenReturn(Arrays.asList(mockNode1));

        rule.transform(record, metadata);

        verify(metadata).addFieldOcurrence("dc.license", "CC-BY-NC-SA 4.0 <https://creativecommons.org>");
    }

    @Test
    @DisplayName("Should handle whitespace in values")
    void testWhitespaceInValues() {
        rule.setSourceFieldName("dc.description");
        rule.setTargetFieldName("dc.abstract");
        when(mockChildNode1.getNodeValue()).thenReturn("  Text with spaces  ");
        when(metadata.getFieldNodes("dc.description")).thenReturn(Arrays.asList(mockNode1));

        rule.transform(record, metadata);

        verify(metadata).addFieldOcurrence("dc.abstract", "  Text with spaces  ");
    }

    @Test
    @DisplayName("Should respect MAX_NODE_COUNT limit")
    void testMaxNodeCountLimit() {
        rule.setSourceFieldName("dc.many");
        rule.setTargetFieldName("dc.target");
        
        // Create more than MAX_NODE_COUNT nodes
        Node[] manyNodes = new Node[150];
        for (int i = 0; i < 150; i++) {
            Node node = mock(Node.class);
            Node childNode = mock(Node.class);
            when(node.getFirstChild()).thenReturn(childNode);
            when(childNode.getNodeValue()).thenReturn("value" + i);
            manyNodes[i] = node;
        }
        
        when(metadata.getFieldNodes("dc.many")).thenReturn(Arrays.asList(manyNodes));

        rule.transform(record, metadata);

        // Should process at most 101 nodes (i from 0 to 100, then breaks at i=101)
        verify(metadata, atMost(101)).addFieldOcurrence(eq("dc.target"), anyString());
    }

    @Test
    @DisplayName("Should handle field names with namespaces")
    void testFieldNamesWithNamespaces() {
        rule.setSourceFieldName("oai_dc:dc/dc:creator");
        rule.setTargetFieldName("author");
        when(metadata.getFieldNodes("oai_dc:dc/dc:creator")).thenReturn(Arrays.asList(mockNode1));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("author", "value1");
    }

    @Test
    @DisplayName("Should get and set source field name")
    void testGetSetSourceFieldName() {
        rule.setSourceFieldName("test.source");
        assertEquals("test.source", rule.getSourceFieldName());
    }

    @Test
    @DisplayName("Should get and set target field name")
    void testGetSetTargetFieldName() {
        rule.setTargetFieldName("test.target");
        assertEquals("test.target", rule.getTargetFieldName());
    }

    @Test
    @DisplayName("Should handle null child node value gracefully")
    void testNullChildNodeValue() {
        rule.setSourceFieldName("dc.null");
        rule.setTargetFieldName("dc.target");
        when(mockChildNode1.getNodeValue()).thenReturn(null);
        when(metadata.getFieldNodes("dc.null")).thenReturn(Arrays.asList(mockNode1));

        // Should not throw exception
        assertDoesNotThrow(() -> rule.transform(record, metadata));
    }

    @Test
    @DisplayName("Should remove source nodes after transformation")
    void testRemoveSourceNodes() {
        rule.setSourceFieldName("old");
        rule.setTargetFieldName("new");
        when(metadata.getFieldNodes("old")).thenReturn(Arrays.asList(mockNode1, mockNode2));

        rule.transform(record, metadata);

        verify(metadata).removeNode(mockNode1);
        verify(metadata).removeNode(mockNode2);
    }

    @Test
    @DisplayName("Should add occurrences before removing nodes")
    void testOrderOfOperations() {
        rule.setSourceFieldName("source");
        rule.setTargetFieldName("target");
        when(metadata.getFieldNodes("source")).thenReturn(Arrays.asList(mockNode1));

        var inOrder = inOrder(metadata);
        rule.transform(record, metadata);

        inOrder.verify(metadata).addFieldOcurrence("target", "value1");
        inOrder.verify(metadata).removeNode(mockNode1);
    }
}
