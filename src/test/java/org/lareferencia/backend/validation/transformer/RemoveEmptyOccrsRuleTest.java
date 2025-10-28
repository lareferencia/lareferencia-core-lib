package org.lareferencia.backend.validation.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RemoveEmptyOccrsRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RemoveEmptyOccrsRule Tests")
class RemoveEmptyOccrsRuleTest {

    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private RemoveEmptyOccrsRule rule;

    @BeforeEach
    void setUp() {
        rule = new RemoveEmptyOccrsRule();
        rule.setFieldName("dc.description");
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(textNode);
        when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should remove empty string occurrences")
    void testRemoveEmptyString() {
        Node emptyNode = createMockNode("");
        Node validNode = createMockNode("Valid content");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Arrays.asList(emptyNode, validNode));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(emptyNode);
        verify(metadata, never()).removeNode(validNode);
    }

    @Test
    @DisplayName("Should remove whitespace-only occurrences")
    void testRemoveWhitespaceOnly() {
        Node whitespaceNode = createMockNode("   ");
        Node validNode = createMockNode("Content");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Arrays.asList(whitespaceNode, validNode));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(whitespaceNode);
        verify(metadata, never()).removeNode(validNode);
    }

    @Test
    @DisplayName("Should remove tabs and newlines only occurrences")
    void testRemoveTabsAndNewlines() {
        Node tabNode = createMockNode("\t\t");
        Node newlineNode = createMockNode("\n\n");
        Node validNode = createMockNode("Valid");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Arrays.asList(tabNode, newlineNode, validNode));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(tabNode);
        verify(metadata).removeNode(newlineNode);
        verify(metadata, never()).removeNode(validNode);
    }

    @Test
    @DisplayName("Should not remove non-empty occurrences")
    void testDoNotRemoveNonEmpty() {
        Node node1 = createMockNode("Content 1");
        Node node2 = createMockNode("Content 2");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle empty field list")
    void testEmptyFieldList() {
        when(metadata.getFieldNodes("dc.description")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should remove all empty occurrences")
    void testRemoveAllEmptyOccurrences() {
        Node empty1 = createMockNode("");
        Node empty2 = createMockNode("  ");
        Node empty3 = createMockNode("\t");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Arrays.asList(empty1, empty2, empty3));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(empty1);
        verify(metadata).removeNode(empty2);
        verify(metadata).removeNode(empty3);
    }

    @Test
    @DisplayName("Should handle mixed empty and non-empty occurrences")
    void testMixedOccurrences() {
        Node empty = createMockNode("");
        Node whitespace = createMockNode("   ");
        Node valid1 = createMockNode("Content");
        Node valid2 = createMockNode("More content");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Arrays.asList(empty, valid1, whitespace, valid2));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(empty);
        verify(metadata).removeNode(whitespace);
        verify(metadata, never()).removeNode(valid1);
        verify(metadata, never()).removeNode(valid2);
    }

    @Test
    @DisplayName("Should preserve content with leading/trailing whitespace")
    void testPreserveContentWithWhitespace() {
        Node node = createMockNode("  content  ");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should verify getter and setter for fieldName")
    void testFieldNameGetterSetter() {
        rule.setFieldName("dc.subject");
        assertEquals("dc.subject", rule.getFieldName());
    }

    @Test
    @DisplayName("Should handle single empty occurrence")
    void testSingleEmptyOccurrence() {
        Node emptyNode = createMockNode("");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Collections.singletonList(emptyNode));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(emptyNode);
    }

    @Test
    @DisplayName("Should handle mixed whitespace types")
    void testMixedWhitespaceTypes() {
        Node mixedNode = createMockNode(" \t\n\r ");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Collections.singletonList(mixedNode));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(mixedNode);
    }

    @Test
    @DisplayName("Should not remove content with just one character")
    void testSingleCharacterContent() {
        Node node = createMockNode("a");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle zero-width spaces")
    void testZeroWidthSpaces() {
        Node node = createMockNode("\u200B\u200B");
        
        when(metadata.getFieldNodes("dc.description")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(record, metadata);

        assertFalse(result); // Zero-width space is not considered whitespace by trim()
    }
}
