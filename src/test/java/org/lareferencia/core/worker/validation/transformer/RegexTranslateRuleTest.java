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

package org.lareferencia.core.worker.validation.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.lareferencia.core.domain.OAIRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("RegexTranslateRule Tests")
class RegexTranslateRuleTest {

    @Mock
    private NetworkRunningContext context;
    private RegexTranslateRule rule;
    private OAIRecord record;
    private OAIRecordMetadata metadata;

    @BeforeEach
    void setUp() {
        rule = new RegexTranslateRule();
        record = mock(OAIRecord.class);
        metadata = mock(OAIRecordMetadata.class);
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node childNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(childNode);
        when(childNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should replace text using regex pattern")
    void testRegexReplace() {
        rule.setSourceFieldName("dc.identifier");
        rule.setTargetFieldName("dc.identifier");
        rule.setRegexSearch("http://");
        rule.setRegexReplace("https://");
        
        Node node = createMockNode("http://example.com");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Arrays.asList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.identifier", "https://example.com");
    }

    @Test
    @DisplayName("Should not add duplicate values")
    void testNoDuplicates() {
        rule.setSourceFieldName("dc.subject");
        rule.setTargetFieldName("dc.subject");
        rule.setRegexSearch("\\s+");
        rule.setRegexReplace("");
        rule.setRemoveMatchingOccurrences(true);
        
        Node node1 = createMockNode("Computer Science");
        Node node2 = createMockNode("Data Science");
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        // Should transform both nodes
        assertTrue(result);
        verify(metadata, times(1)).addFieldOcurrence("dc.subject", "ComputerScience");
        verify(metadata, times(1)).addFieldOcurrence("dc.subject", "DataScience");
        verify(metadata, times(1)).removeNode(node1);
        verify(metadata, times(1)).removeNode(node2);
    }

    @Test
    @DisplayName("Should remove matching occurrences when flag is set")
    void testRemoveMatchingOccurrences() {
        rule.setSourceFieldName("dc.type");
        rule.setTargetFieldName("dc.type");
        rule.setRegexSearch("article");
        rule.setRegexReplace("Article");
        rule.setRemoveMatchingOccurrences(true);
        
        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node));

        rule.transform(context, record, metadata);

        verify(metadata).removeNode(node);
        verify(metadata).addFieldOcurrence("dc.type", "Article");
    }

    @Test
    @DisplayName("Should not remove occurrences when flag is false")
    void testNotRemoveWhenFlagFalse() {
        rule.setSourceFieldName("dc.type");
        rule.setTargetFieldName("dc.type");
        rule.setRegexSearch("article");
        rule.setRegexReplace("Article");
        rule.setRemoveMatchingOccurrences(false);
        
        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node));

        rule.transform(context, record, metadata);

        verify(metadata, never()).removeNode(node);
    }

    @Test
    @DisplayName("Should handle regex with capture groups")
    void testCaptureGroups() {
        rule.setSourceFieldName("dc.date");
        rule.setTargetFieldName("dc.year");
        rule.setRegexSearch("(\\d{4})-\\d{2}-\\d{2}");
        rule.setRegexReplace("$1");
        
        Node node = createMockNode("2023-10-27");
        when(metadata.getFieldNodes("dc.date")).thenReturn(Arrays.asList(node));

        rule.transform(context, record, metadata);

        verify(metadata).addFieldOcurrence("dc.year", "2023");
    }

    @Test
    @DisplayName("Should return false when no fields match")
    void testNoFieldsMatch() {
        rule.setSourceFieldName("dc.nonexistent");
        rule.setTargetFieldName("dc.target");
        rule.setRegexSearch("pattern");
        rule.setRegexReplace("replacement");
        
        when(metadata.getFieldNodes("dc.nonexistent")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should handle multiple occurrences")
    void testMultipleOccurrences() {
        rule.setSourceFieldName("dc.identifier");
        rule.setTargetFieldName("dc.identifier");
        rule.setRegexSearch("^DOI:");
        rule.setRegexReplace("doi:");
        
        Node node1 = createMockNode("DOI:10.1234/abc");
        Node node2 = createMockNode("DOI:10.5678/def");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Arrays.asList(node1, node2));

        rule.transform(context, record, metadata);

        verify(metadata).addFieldOcurrence("dc.identifier", "doi:10.1234/abc");
        verify(metadata).addFieldOcurrence("dc.identifier", "doi:10.5678/def");
    }

    @Test
    @DisplayName("Should handle complex regex patterns")
    void testComplexPattern() {
        rule.setSourceFieldName("dc.identifier");
        rule.setTargetFieldName("dc.clean_id");
        rule.setRegexSearch("^(http://|https://)?(www\\.)?");
        rule.setRegexReplace("");
        
        Node node = createMockNode("https://www.example.com/article");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Arrays.asList(node));

        rule.transform(context, record, metadata);

        verify(metadata).addFieldOcurrence("dc.clean_id", "example.com/article");
    }

    @Test
    @DisplayName("Should use replaceFirst not replaceAll")
    void testReplaceFirst() {
        rule.setSourceFieldName("dc.text");
        rule.setTargetFieldName("dc.text");
        rule.setRegexSearch("test");
        rule.setRegexReplace("TEST");
        
        Node node = createMockNode("test test test");
        when(metadata.getFieldNodes("dc.text")).thenReturn(Arrays.asList(node));

        rule.transform(context, record, metadata);

        verify(metadata).addFieldOcurrence("dc.text", "TEST test test");
    }

    @Test
    @DisplayName("Should get and set properties")
    void testGettersAndSetters() {
        rule.setSourceFieldName("source");
        rule.setTargetFieldName("target");
        rule.setRegexSearch("search");
        rule.setRegexReplace("replace");
        rule.setRemoveMatchingOccurrences(true);

        assertEquals("source", rule.getSourceFieldName());
        assertEquals("target", rule.getTargetFieldName());
        assertEquals("search", rule.getRegexSearch());
        assertEquals("replace", rule.getRegexReplace());
        assertTrue(rule.getRemoveMatchingOccurrences());
    }
}
