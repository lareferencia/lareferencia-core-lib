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
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RemoveAllButFirstOccrRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RemoveAllButFirstOccrRule Tests")
class RemoveAllButFirstOccrRuleTest {


    @Mock
    private NetworkRunningContext context;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private RemoveAllButFirstOccrRule rule;

    @BeforeEach
    void setUp() {
        rule = new RemoveAllButFirstOccrRule();
        rule.setFieldName("dc.title");
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        lenient().when(node.getFirstChild()).thenReturn(textNode);
        lenient().when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should keep only first occurrence")
    void testKeepOnlyFirst() {
        Node node1 = createMockNode("First Title");
        Node node2 = createMockNode("Second Title");
        Node node3 = createMockNode("Third Title");
        
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
        verify(metadata).removeNode(node3);
    }

    @Test
    @DisplayName("Should not transform when only one occurrence exists")
    void testSingleOccurrence() {
        Node node = createMockNode("Only Title");
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle empty field list")
    void testEmptyFieldList() {
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should remove all but first when two occurrences")
    void testTwoOccurrences() {
        Node node1 = createMockNode("Title 1");
        Node node2 = createMockNode("Title 2");
        
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle many occurrences")
    void testManyOccurrences() {
        Node node1 = createMockNode("Title 1");
        Node node2 = createMockNode("Title 2");
        Node node3 = createMockNode("Title 3");
        Node node4 = createMockNode("Title 4");
        Node node5 = createMockNode("Title 5");
        
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(node1, node2, node3, node4, node5));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
        verify(metadata).removeNode(node3);
        verify(metadata).removeNode(node4);
        verify(metadata).removeNode(node5);
    }

    @Test
    @DisplayName("Should preserve first occurrence regardless of value")
    void testPreserveFirstRegardlessOfValue() {
        Node node1 = createMockNode("");
        Node node2 = createMockNode("Valid Title");
        
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle Unicode in first occurrence")
    void testUnicodeInFirstOccurrence() {
        Node node1 = createMockNode("Título en español");
        Node node2 = createMockNode("English Title");
        
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should verify getter and setter for fieldName")
    void testFieldNameGetterSetter() {
        rule.setFieldName("dc.description");
        assertEquals("dc.description", rule.getFieldName());
    }

    @Test
    @DisplayName("Should work with different field names")
    void testDifferentFieldNames() {
        rule.setFieldName("dc.creator");
        
        Node node1 = createMockNode("Author 1");
        Node node2 = createMockNode("Author 2");
        
        when(metadata.getFieldNodes("dc.creator")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle identical values in all occurrences")
    void testIdenticalValues() {
        Node node1 = createMockNode("Same Title");
        Node node2 = createMockNode("Same Title");
        Node node3 = createMockNode("Same Title");
        
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
        verify(metadata).removeNode(node3);
    }

    @Test
    @DisplayName("Should handle special characters in values")
    void testSpecialCharacters() {
        Node node1 = createMockNode("Title: with <special> & characters!");
        Node node2 = createMockNode("Another Title");
        
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle whitespace in values")
    void testWhitespaceInValues() {
        Node node1 = createMockNode("  Title with spaces  ");
        Node node2 = createMockNode("Another Title");
        
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle very long first occurrence")
    void testLongFirstOccurrence() {
        String longTitle = "A".repeat(1000);
        Node node1 = createMockNode(longTitle);
        Node node2 = createMockNode("Short");
        
        when(metadata.getFieldNodes("dc.title")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
    }
}
