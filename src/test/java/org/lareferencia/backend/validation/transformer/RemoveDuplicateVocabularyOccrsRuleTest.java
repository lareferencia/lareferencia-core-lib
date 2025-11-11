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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RemoveDuplicateVocabularyOccrsRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RemoveDuplicateVocabularyOccrsRule Tests")
class RemoveDuplicateVocabularyOccrsRuleTest {


    @Mock
    private NetworkRunningContext context;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private RemoveDuplicateVocabularyOccrsRule rule;

    @BeforeEach
    void setUp() {
        rule = new RemoveDuplicateVocabularyOccrsRule();
        rule.setFieldName("dc.type");
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(textNode);
        when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should keep highest priority occurrence when duplicates exist")
    void testKeepHighestPriority() {
        List<String> vocabulary = Arrays.asList("article", "book", "thesis");
        rule.setVocabulary(vocabulary);

        Node node1 = createMockNode("thesis");
        Node node2 = createMockNode("article");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node1);
        verify(metadata, never()).removeNode(node2);
    }

    @Test
    @DisplayName("Should not transform when no duplicates exist")
    void testNoDuplicates() {
        List<String> vocabulary = Arrays.asList("article", "book", "thesis");
        rule.setVocabulary(vocabulary);

        Node node = createMockNode("article");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle occurrences not in vocabulary")
    void testOccurrencesNotInVocabulary() {
        List<String> vocabulary = Arrays.asList("article", "book");
        rule.setVocabulary(vocabulary);

        Node node1 = createMockNode("report");
        Node node2 = createMockNode("manual");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should remove all but first when multiple duplicates")
    void testMultipleDuplicates() {
        List<String> vocabulary = Arrays.asList("article", "book", "thesis", "conference");
        rule.setVocabulary(vocabulary);

        Node node1 = createMockNode("thesis");
        Node node2 = createMockNode("book");
        Node node3 = createMockNode("article");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node3);
        verify(metadata).removeNode(node2);
        verify(metadata).removeNode(node1);
    }

    @Test
    @DisplayName("Should handle empty vocabulary")
    void testEmptyVocabulary() {
        rule.setVocabulary(Collections.emptyList());

        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle empty field list")
    void testEmptyFieldList() {
        List<String> vocabulary = Arrays.asList("article", "book");
        rule.setVocabulary(vocabulary);

        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should preserve order priority in vocabulary")
    void testVocabularyOrderPriority() {
        List<String> vocabulary = Arrays.asList("primary", "secondary", "tertiary");
        rule.setVocabulary(vocabulary);

        Node node1 = createMockNode("tertiary");
        Node node2 = createMockNode("primary");
        Node node3 = createMockNode("secondary");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node2);
        verify(metadata).removeNode(node3);
        verify(metadata).removeNode(node1);
    }

    @Test
    @DisplayName("Should handle mix of vocabulary and non-vocabulary values")
    void testMixedVocabularyValues() {
        List<String> vocabulary = Arrays.asList("article", "book");
        rule.setVocabulary(vocabulary);

        Node node1 = createMockNode("book");
        Node node2 = createMockNode("report");
        Node node3 = createMockNode("article");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node3);
        verify(metadata).removeNode(node1);
        verify(metadata, never()).removeNode(node2);
    }

    @Test
    @DisplayName("Should verify getter and setter for fieldName")
    void testFieldNameGetterSetter() {
        rule.setFieldName("dc.subject");
        assertEquals("dc.subject", rule.getFieldName());
    }

    @Test
    @DisplayName("Should verify getter and setter for vocabulary")
    void testVocabularyGetterSetter() {
        List<String> vocabulary = Arrays.asList("value1", "value2");
        rule.setVocabulary(vocabulary);
        
        assertEquals(vocabulary, rule.getVocabulary());
        assertEquals(2, rule.getVocabulary().size());
    }

    @Test
    @DisplayName("Should handle same value appearing twice")
    void testSameValueTwice() {
        List<String> vocabulary = Arrays.asList("article", "book");
        rule.setVocabulary(vocabulary);

        Node node1 = createMockNode("article");
        Node node2 = createMockNode("article");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle case sensitivity in vocabulary")
    void testCaseSensitivity() {
        List<String> vocabulary = Arrays.asList("Article", "Book");
        rule.setVocabulary(vocabulary);

        Node node1 = createMockNode("article");
        Node node2 = createMockNode("Article");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should handle Unicode values in vocabulary")
    void testUnicodeInVocabulary() {
        List<String> vocabulary = Arrays.asList("artículo", "libro", "tesis");
        rule.setVocabulary(vocabulary);

        Node node1 = createMockNode("tesis");
        Node node2 = createMockNode("artículo");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node2);
        verify(metadata).removeNode(node1);
    }
}
