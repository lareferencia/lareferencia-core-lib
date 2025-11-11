package org.lareferencia.backend.validation.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RemoveDuplicateOccrsRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RemoveDuplicateOccrsRule Tests")
class RemoveDuplicateOccrsRuleTest {


    @Mock
    private NetworkRunningContext context;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private RemoveDuplicateOccrsRule rule;

    @BeforeEach
    void setUp() {
        rule = new RemoveDuplicateOccrsRule();
        rule.setFieldName("dc.subject");
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(textNode);
        when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should remove duplicate occurrences")
    void testRemoveDuplicates() {
        Node node1 = createMockNode("Computer Science");
        Node node2 = createMockNode("Computer Science");
        Node node3 = createMockNode("Mathematics");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node2);
        verify(metadata, never()).removeNode(node1);
        verify(metadata, never()).removeNode(node3);
    }

    @Test
    @DisplayName("Should keep first occurrence and remove subsequent duplicates")
    void testKeepFirstOccurrence() {
        Node node1 = createMockNode("Subject");
        Node node2 = createMockNode("Subject");
        Node node3 = createMockNode("Subject");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
        verify(metadata).removeNode(node3);
    }

    @Test
    @DisplayName("Should not transform when no duplicates exist")
    void testNoDuplicates() {
        Node node1 = createMockNode("Subject1");
        Node node2 = createMockNode("Subject2");
        Node node3 = createMockNode("Subject3");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle empty field list")
    void testEmptyFieldList() {
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle single occurrence")
    void testSingleOccurrence() {
        Node node = createMockNode("Single Subject");
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should be case sensitive")
    void testCaseSensitive() {
        Node node1 = createMockNode("Subject");
        Node node2 = createMockNode("subject");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle whitespace differences")
    void testWhitespaceDifferences() {
        Node node1 = createMockNode("Computer Science");
        Node node2 = createMockNode("Computer  Science");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle multiple different duplicates")
    void testMultipleDifferentDuplicates() {
        Node node1 = createMockNode("Science");
        Node node2 = createMockNode("Math");
        Node node3 = createMockNode("Science");
        Node node4 = createMockNode("Math");
        Node node5 = createMockNode("History");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3, node4, node5));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata, never()).removeNode(node2);
        verify(metadata).removeNode(node3);
        verify(metadata).removeNode(node4);
        verify(metadata, never()).removeNode(node5);
    }

    @Test
    @DisplayName("Should handle Unicode characters")
    void testUnicodeCharacters() {
        Node node1 = createMockNode("Español");
        Node node2 = createMockNode("Español");
        Node node3 = createMockNode("中文");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
        verify(metadata, never()).removeNode(node3);
    }

    @Test
    @DisplayName("Should handle empty string duplicates")
    void testEmptyStringDuplicates() {
        Node node1 = createMockNode("");
        Node node2 = createMockNode("");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should verify getter and setter for fieldName")
    void testFieldNameGetterSetter() {
        rule.setFieldName("dc.identifier");
        assertEquals("dc.identifier", rule.getFieldName());
    }

    @Test
    @DisplayName("Should handle special characters in values")
    void testSpecialCharacters() {
        Node node1 = createMockNode("http://example.org/subject");
        Node node2 = createMockNode("http://example.org/subject");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle long duplicate values")
    void testLongDuplicateValues() {
        String longValue = "This is a very long subject description that contains many words and characters to test the handling of longer strings in the duplicate detection logic";
        Node node1 = createMockNode(longValue);
        Node node2 = createMockNode(longValue);
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node2);
    }
}
