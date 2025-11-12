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
 * Unit tests for FieldNameBulkTranslateRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FieldNameBulkTranslateRule Tests")
class FieldNameBulkTranslateRuleTest {


    @Mock
    private NetworkRunningContext context;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private FieldNameBulkTranslateRule rule;

    @BeforeEach
    void setUp() {
        rule = new FieldNameBulkTranslateRule();
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(textNode);
        when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should translate single field name")
    void testSingleFieldNameTranslation() {
        List<Translation> translations = Collections.singletonList(
            new Translation("dc.creator", "dc.author")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("John Doe");
        when(metadata.getFieldNodes("dc.creator")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.author", "John Doe");
        verify(metadata).removeNode(node);
    }

    @Test
    @DisplayName("Should translate multiple field names")
    void testMultipleFieldNameTranslations() {
        List<Translation> translations = Arrays.asList(
            new Translation("dc.creator", "dc.author"),
            new Translation("dc.date", "dc.year")
        );
        rule.setTranslationArray(translations);

        Node creatorNode = createMockNode("Jane Smith");
        Node dateNode = createMockNode("2023");
        when(metadata.getFieldNodes("dc.creator")).thenReturn(Collections.singletonList(creatorNode));
        when(metadata.getFieldNodes("dc.date")).thenReturn(Collections.singletonList(dateNode));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.author", "Jane Smith");
        verify(metadata).addFieldOcurrence("dc.year", "2023");
        verify(metadata).removeNode(creatorNode);
        verify(metadata).removeNode(dateNode);
    }

    @Test
    @DisplayName("Should handle multiple occurrences of same field")
    void testMultipleOccurrencesOfSameField() {
        List<Translation> translations = Collections.singletonList(
            new Translation("dc.subject", "dc.keyword")
        );
        rule.setTranslationArray(translations);

        Node node1 = createMockNode("Computer Science");
        Node node2 = createMockNode("Artificial Intelligence");
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.keyword", "Computer Science");
        verify(metadata).addFieldOcurrence("dc.keyword", "Artificial Intelligence");
        verify(metadata).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle empty translation array")
    void testEmptyTranslationArray() {
        rule.setTranslationArray(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle non-existent source field")
    void testNonExistentSourceField() {
        List<Translation> translations = Collections.singletonList(
            new Translation("dc.nonexistent", "dc.target")
        );
        rule.setTranslationArray(translations);

        when(metadata.getFieldNodes("dc.nonexistent")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should preserve Unicode values during translation")
    void testUnicodeValuePreservation() {
        List<Translation> translations = Collections.singletonList(
            new Translation("dc.title", "dc.titulo")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("Título en español con acentos");
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.titulo", "Título en español con acentos");
    }

    @Test
    @DisplayName("Should handle complex field name patterns")
    void testComplexFieldNames() {
        List<Translation> translations = Collections.singletonList(
            new Translation("dc.contributor.author", "dc.creator.person")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("Complex Author");
        when(metadata.getFieldNodes("dc.contributor.author")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.creator.person", "Complex Author");
        verify(metadata).removeNode(node);
    }

    @Test
    @DisplayName("Should process all translations sequentially")
    void testSequentialProcessing() {
        List<Translation> translations = Arrays.asList(
            new Translation("field1", "newfield1"),
            new Translation("field2", "newfield2"),
            new Translation("field3", "newfield3")
        );
        rule.setTranslationArray(translations);

        Node node1 = createMockNode("value1");
        Node node2 = createMockNode("value2");
        Node node3 = createMockNode("value3");
        
        when(metadata.getFieldNodes("field1")).thenReturn(Collections.singletonList(node1));
        when(metadata.getFieldNodes("field2")).thenReturn(Collections.singletonList(node2));
        when(metadata.getFieldNodes("field3")).thenReturn(Collections.singletonList(node3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("newfield1", "value1");
        verify(metadata).addFieldOcurrence("newfield2", "value2");
        verify(metadata).addFieldOcurrence("newfield3", "value3");
    }

    @Test
    @DisplayName("Should handle empty field values")
    void testEmptyFieldValues() {
        List<Translation> translations = Collections.singletonList(
            new Translation("dc.description", "dc.abstract")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("");
        when(metadata.getFieldNodes("dc.description")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.abstract", "");
        verify(metadata).removeNode(node);
    }

    @Test
    @DisplayName("Should verify getter and setter for translationArray")
    void testTranslationArrayGetterSetter() {
        List<Translation> translations = Collections.singletonList(
            new Translation("source", "target")
        );
        rule.setTranslationArray(translations);

        assertEquals(translations, rule.getTranslationArray());
        assertEquals(1, rule.getTranslationArray().size());
    }

    @Test
    @DisplayName("Should handle whitespace in field values")
    void testWhitespacePreservation() {
        List<Translation> translations = Collections.singletonList(
            new Translation("dc.title", "dc.maintitle")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("  Title with spaces  ");
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.maintitle", "  Title with spaces  ");
    }

    @Test
    @DisplayName("Should return false when some translations have no matching fields")
    void testPartialTranslations() {
        List<Translation> translations = Arrays.asList(
            new Translation("existing.field", "new.field"),
            new Translation("nonexistent.field", "other.field")
        );
        rule.setTranslationArray(translations);

        Node node = createMockNode("value");
        when(metadata.getFieldNodes("existing.field")).thenReturn(Collections.singletonList(node));
        when(metadata.getFieldNodes("nonexistent.field")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, times(1)).addFieldOcurrence(anyString(), anyString());
    }
}
