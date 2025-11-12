package org.lareferencia.core.worker.validation.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.lareferencia.core.domain.OAIRecord;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.Translation;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FieldContentTranslateRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FieldContentTranslateRule Tests")
class FieldContentTranslateRuleTest {


    @Mock
    private NetworkRunningContext context;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private FieldContentTranslateRule rule;

    @BeforeEach
    void setUp() {
        rule = new FieldContentTranslateRule();
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
    @DisplayName("Should translate value using translation array")
    void testBasicTranslation() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Artículo"),
            new Translation("book", "Libro")
        );
        rule.setTranslationArray(translations);
        rule.setReplaceOccurrence(true);

        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node);
        verify(metadata).addFieldOcurrence("dc.type", "Artículo");
    }

    @Test
    @DisplayName("Should be case insensitive in translation")
    void testCaseInsensitiveTranslation() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Artículo")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("ARTICLE");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "Artículo");
    }

    @Test
    @DisplayName("Should not replace occurrence when replaceOccurrence is false")
    void testNoReplaceOccurrence() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Artículo")
        );
        rule.setTranslationArray(translations);
        rule.setReplaceOccurrence(false);

        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(node);
        verify(metadata).addFieldOcurrence("dc.type", "Artículo");
    }

    @Test
    @DisplayName("Should not add duplicate values")
    void testNoDuplicateValues() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Artículo")
        );
        rule.setTranslationArray(translations);

        Node node1 = createMockNode("article");
        Node node2 = createMockNode("Artículo");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should translate to different field when writeFieldName differs")
    void testTranslateToDifferentField() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Artículo")
        );
        rule.setTranslationArray(translations);
        rule.setWriteFieldName("dc.type.translated");

        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type.translated", "Artículo");
    }

    @Test
    @DisplayName("Should handle values not in translation map")
    void testValueNotInTranslationMap() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Artículo")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("thesis");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should handle empty field list")
    void testEmptyFieldList() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Artículo")
        );
        rule.setTranslationArray(translations);

        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should translate multiple different values")
    void testMultipleTranslations() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "Artículo"),
            new Translation("book", "Libro"),
            new Translation("thesis", "Tesis")
        );
        rule.setTranslationArray(translations);

        Node node1 = createMockNode("article");
        Node node2 = createMockNode("book");
        Node node3 = createMockNode("thesis");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "Artículo");
        verify(metadata).addFieldOcurrence("dc.type", "Libro");
        verify(metadata).addFieldOcurrence("dc.type", "Tesis");
    }

    @Test
    @DisplayName("Should handle Unicode characters in translations")
    void testUnicodeTranslations() {
        List<Translation> translations = Arrays.asList(
            new Translation("español", "Spanish"),
            new Translation("中文", "Chinese")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("español");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "Spanish");
    }

    @Test
    @DisplayName("Should return false when value equals translated value")
    void testNoTransformationWhenSameValue() {
        List<Translation> translations = Arrays.asList(
            new Translation("article", "article")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should handle whitespace in values")
    void testWhitespaceInValues() {
        List<Translation> translations = Arrays.asList(
            new Translation("journal article", "Artículo de revista")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("journal article");
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "Artículo de revista");
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
}
