package org.lareferencia.backend.validation.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.Translation;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FieldContentPriorityTranslateRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FieldContentPriorityTranslateRule Tests")
class FieldContentPriorityTranslateRuleTest {


    @Mock
    private NetworkRunningContext context;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private FieldContentPriorityTranslateRule rule;

    @BeforeEach
    void setUp() {
        rule = new FieldContentPriorityTranslateRule();
        rule.setTestFieldName("dc.type");
        rule.setWriteFieldName("dc.type");
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(textNode);
        when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should translate using priority order")
    void testPriorityTranslation() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Article"),
            new Translation("book", "Book"),
            new Translation("thesis", "Thesis")
        );
        rule.setTranslationArray(translations);
        rule.setReplaceAllMatchingOccurrences(false);

        Node node1 = createMockNode("article");
        Node node2 = createMockNode("other");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        // Should process article first (first translation in array), then stop because replaceAllMatchingOccurrences=false
        verify(metadata, times(1)).addFieldOcurrence("dc.type", "Article");
        verify(metadata, never()).addFieldOcurrence(eq("dc.type"), eq("Book"));
    }

    @Test
    @DisplayName("Should stop at first match when replaceAllMatchingOccurrences is false")
    void testStopAtFirstMatch() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Article")
        );
        rule.setTranslationArray(translations);
        rule.setReplaceAllMatchingOccurrences(false);

        Node node1 = createMockNode("article");
        Node node2 = createMockNode("article");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, times(1)).addFieldOcurrence("dc.type", "Article");
    }

    @Test
    @DisplayName("Should replace all matching occurrences when flag is true")
    void testReplaceAllMatching() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Article")
        );
        rule.setTranslationArray(translations);
        rule.setReplaceAllMatchingOccurrences(true);

        Node node1 = createMockNode("article");
        Node node2 = createMockNode("article");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, times(1)).addFieldOcurrence("dc.type", "Article");
    }

    @Test
    @DisplayName("Should test value as prefix when flag is true")
    void testValueAsPrefix() {
        List<Translation> translations = Arrays.asList(
            new Translation("art", "Article")
        );
        rule.setTranslationArray(translations);
        rule.setTestValueAsPrefix(true);

        Node node = createMockNode("article-journal");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "Article");
    }

    @Test
    @DisplayName("Should not match prefix when testValueAsPrefix is false")
    void testNotMatchPrefix() {
        List<Translation> translations = Arrays.asList(
            new Translation("art", "Article")
        );
        rule.setTranslationArray(translations);
        rule.setTestValueAsPrefix(false);

        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should not replace occurrence when replaceOccurrence is false")
    void testNoReplaceOccurrence() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Article")
        );
        rule.setTranslationArray(translations);
        rule.setReplaceOccurrence(false);

        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node);
        verify(metadata).addFieldOcurrence("dc.type", "Article");
    }

    @Test
    @DisplayName("Should remove occurrence when replaceOccurrence is true")
    void testReplaceOccurrence() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Article")
        );
        rule.setTranslationArray(translations);
        rule.setReplaceOccurrence(true);

        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node);
        verify(metadata).addFieldOcurrence("dc.type", "Article");
    }

    @Test
    @DisplayName("Should not add duplicate values")
    void testNoDuplicateValues() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Article")
        );
        rule.setTranslationArray(translations);

        Node node1 = createMockNode("article");
        Node node2 = createMockNode("Article");
        
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should handle empty translation array")
    void testEmptyTranslationArray() {
        rule.setTranslationArray(Collections.emptyList());

        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should handle empty field list")
    void testEmptyFieldList() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Article")
        );
        rule.setTranslationArray(translations);

        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should verify getter and setter for testFieldName")
    void testTestFieldNameGetterSetter() {
        rule.setTestFieldName("dc.subject");
        assertEquals("dc.subject", rule.getTestFieldName());
    }

    @Test
    @DisplayName("Should verify getter and setter for writeFieldName")
    void testWriteFieldNameGetterSetter() {
        rule.setWriteFieldName("dc.newfield");
        assertEquals("dc.newfield", rule.getWriteFieldName());
    }

    @Test
    @DisplayName("Should verify getter and setter for replaceOccurrence")
    void testReplaceOccurrenceGetterSetter() {
        rule.setReplaceOccurrence(false);
        assertFalse(rule.getReplaceOccurrence());
    }

    @Test
    @DisplayName("Should verify getter and setter for testValueAsPrefix")
    void testTestValueAsPrefixGetterSetter() {
        rule.setTestValueAsPrefix(true);
        assertTrue(rule.getTestValueAsPrefix());
    }

    @Test
    @DisplayName("Should verify getter and setter for replaceAllMatchingOccurrences")
    void testReplaceAllMatchingOccurrencesGetterSetter() {
        rule.setReplaceAllMatchingOccurrences(false);
        assertFalse(rule.getReplaceAllMatchingOccurrences());
    }

    @Test
    @DisplayName("Should handle Unicode values")
    void testUnicodeValues() {
        List<Translation> translations = Arrays.asList(
            new Translation("artículo", "Article")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("artículo");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "Article");
    }
}
