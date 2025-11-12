package org.lareferencia.core.worker.validation.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.lareferencia.core.domain.OAIRecord;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FieldContentRemoveWhiteSpacesTranslateRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FieldContentRemoveWhiteSpacesTranslateRule Tests")
class FieldContentRemoveWhiteSpacesTranslateRuleTest {


    @Mock
    private SnapshotMetadata snapshotMetadata;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private FieldContentRemoveWhiteSpacesTranslateRule rule;

    @BeforeEach
    void setUp() {
        rule = new FieldContentRemoveWhiteSpacesTranslateRule();
        rule.setFieldName("dc.identifier");
    }

    private Node createMockNodeWithMutableValue(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(textNode);
        when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should remove all whitespace from field value")
    void testRemoveWhitespace() {
        Node node = createMockNodeWithMutableValue("123 456 789");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(node.getFirstChild()).setNodeValue("123456789");
    }

    @Test
    @DisplayName("Should remove tabs from field value")
    void testRemoveTabs() {
        Node node = createMockNodeWithMutableValue("test\t\tvalue");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(node.getFirstChild()).setNodeValue("testvalue");
    }

    @Test
    @DisplayName("Should remove newlines from field value")
    void testRemoveNewlines() {
        Node node = createMockNodeWithMutableValue("line1\nline2\nline3");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(node.getFirstChild()).setNodeValue("line1line2line3");
    }

    @Test
    @DisplayName("Should handle value with no whitespace")
    void testNoWhitespace() {
        Node node = createMockNodeWithMutableValue("testvalue");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertFalse(result);
        verify(node.getFirstChild()).setNodeValue("testvalue");
    }

    @Test
    @DisplayName("Should remove leading and trailing whitespace")
    void testLeadingTrailingWhitespace() {
        Node node = createMockNodeWithMutableValue("  value  ");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(node.getFirstChild()).setNodeValue("value");
    }

    @Test
    @DisplayName("Should remove multiple consecutive spaces")
    void testMultipleSpaces() {
        Node node = createMockNodeWithMutableValue("a     b     c");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(node.getFirstChild()).setNodeValue("abc");
    }

    @Test
    @DisplayName("Should handle empty field list")
    void testEmptyFieldList() {
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertFalse(result);
    }

    @Test
    @DisplayName("Should process multiple field occurrences")
    void testMultipleOccurrences() {
        Node node1 = createMockNodeWithMutableValue("test 1");
        Node node2 = createMockNodeWithMutableValue("test 2");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(node1.getFirstChild()).setNodeValue("test1");
        verify(node2.getFirstChild()).setNodeValue("test2");
    }

    @Test
    @DisplayName("Should remove all types of whitespace characters")
    void testAllWhitespaceTypes() {
        Node node = createMockNodeWithMutableValue("a b\tc\nd\re\u000Bf");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(node.getFirstChild()).setNodeValue("abcdef");
    }

    @Test
    @DisplayName("Should handle string with only whitespace")
    void testOnlyWhitespace() {
        Node node = createMockNodeWithMutableValue("   \t\n   ");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(node.getFirstChild()).setNodeValue("");
    }

    @Test
    @DisplayName("Should validate toString method")
    void testToString() {
        String result = rule.toString();

        assertNotNull(result);
        assertTrue(result.contains("FieldContentRemoveWhiteSpacesTranslateRule"));
        assertTrue(result.contains("dc.identifier"));
    }

    @Test
    @DisplayName("Should verify getter and setter for fieldName")
    void testFieldNameGetterSetter() {
        rule.setFieldName("dc.title");
        assertEquals("dc.title", rule.getFieldName());
    }

    @Test
    @DisplayName("Should return true when at least one occurrence has whitespace")
    void testMixedOccurrences() {
        Node node1 = createMockNodeWithMutableValue("nowhitespace");
        Node node2 = createMockNodeWithMutableValue("has whitespace");
        when(metadata.getFieldNodes("dc.identifier")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(node1.getFirstChild()).setNodeValue("nowhitespace");
        verify(node2.getFirstChild()).setNodeValue("haswhitespace");
    }
}
