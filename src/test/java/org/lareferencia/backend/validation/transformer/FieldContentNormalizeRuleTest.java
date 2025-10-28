package org.lareferencia.backend.validation.transformer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.validation.validator.ContentValidatorResult;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.IValidatorFieldContentRule;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FieldContentNormalizeRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FieldContentNormalizeRule Tests")
class FieldContentNormalizeRuleTest {

    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    @Mock
    private IValidatorFieldContentRule validationRule;

    private FieldContentNormalizeRule rule;

    @BeforeEach
    void setUp() {
        rule = new FieldContentNormalizeRule();
        rule.setFieldName("dc.subject");
        rule.setValidationRule(validationRule);
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(textNode);
        when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should remove invalid occurrences when removeInvalidOccurrences is true")
    void testRemoveInvalidOccurrences() {
        rule.setRemoveInvalidOccurrences(true);

        Node validNode = createMockNode("Valid Subject");
        Node invalidNode = createMockNode("Invalid");

        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(validNode, invalidNode));
        when(validationRule.validate("Valid Subject")).thenReturn(new ContentValidatorResult(true, "Valid"));
        when(validationRule.validate("Invalid")).thenReturn(new ContentValidatorResult(false, "Too short"));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata, times(1)).removeNode(invalidNode);
        verify(metadata, never()).removeNode(validNode);
    }

    @Test
    @DisplayName("Should not remove any nodes when all are valid")
    void testAllValidOccurrences() {
        rule.setRemoveInvalidOccurrences(true);

        Node node1 = createMockNode("Subject One");
        Node node2 = createMockNode("Subject Two");

        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));
        when(validationRule.validate(anyString())).thenReturn(new ContentValidatorResult(true, "Valid"));

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should remove duplicate occurrences when removeDuplicatedOccurrences is true")
    void testRemoveDuplicateOccurrences() {
        rule.setRemoveDuplicatedOccurrences(true);

        Node node1 = createMockNode("Computer Science");
        Node node2 = createMockNode("Computer Science");
        Node node3 = createMockNode("Mathematics");

        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata, times(1)).removeNode(node2);
        verify(metadata, never()).removeNode(node1);
        verify(metadata, never()).removeNode(node3);
    }

    @Test
    @DisplayName("Should remove both invalid and duplicate occurrences")
    void testRemoveInvalidAndDuplicates() {
        rule.setRemoveInvalidOccurrences(true);
        rule.setRemoveDuplicatedOccurrences(true);

        Node node1 = createMockNode("Valid Subject");
        Node node2 = createMockNode("Invalid");
        Node node3 = createMockNode("Valid Subject");

        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3));
        when(validationRule.validate("Valid Subject")).thenReturn(new ContentValidatorResult(true, "Valid"));
        when(validationRule.validate("Invalid")).thenReturn(new ContentValidatorResult(false, "Too short"));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata, times(1)).removeNode(node2);
        verify(metadata, times(1)).removeNode(node3);
    }

    @Test
    @DisplayName("Should handle empty field list")
    void testEmptyFieldList() {
        rule.setRemoveInvalidOccurrences(true);
        rule.setRemoveDuplicatedOccurrences(true);

        when(metadata.getFieldNodes("dc.subject")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should not transform when both flags are false")
    void testNoTransformationWhenFlagsAreFalse() {
        rule.setRemoveInvalidOccurrences(false);
        rule.setRemoveDuplicatedOccurrences(false);

        Node node = createMockNode("Some Subject");
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should preserve order of unique valid occurrences")
    void testPreserveOrderOfUniqueValidOccurrences() {
        rule.setRemoveDuplicatedOccurrences(true);

        Node node1 = createMockNode("Alpha");
        Node node2 = createMockNode("Beta");
        Node node3 = createMockNode("Gamma");

        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle multiple duplicates of same value")
    void testMultipleDuplicates() {
        rule.setRemoveDuplicatedOccurrences(true);

        Node node1 = createMockNode("Science");
        Node node2 = createMockNode("Science");
        Node node3 = createMockNode("Science");

        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(record, metadata);

        assertTrue(result);
        verify(metadata, times(1)).removeNode(node2);
        verify(metadata, times(1)).removeNode(node3);
        verify(metadata, never()).removeNode(node1);
    }

    @Test
    @DisplayName("Should validate toString method")
    void testToString() {
        rule.setRemoveInvalidOccurrences(true);
        rule.setRemoveDuplicatedOccurrences(true);

        String result = rule.toString();

        assertNotNull(result);
        assertTrue(result.contains("FieldContentNormalizeRule"));
        assertTrue(result.contains("dc.subject"));
        assertTrue(result.contains("removeInvalidOccurrences=true"));
        assertTrue(result.contains("removeDuplicatedOccurrences=true"));
    }

    @Test
    @DisplayName("Should handle single occurrence correctly")
    void testSingleOccurrence() {
        rule.setRemoveInvalidOccurrences(true);
        rule.setRemoveDuplicatedOccurrences(true);

        Node node = createMockNode("Unique Subject");
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Collections.singletonList(node));
        when(validationRule.validate("Unique Subject")).thenReturn(new ContentValidatorResult(true, "Valid"));

        boolean result = rule.transform(record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }
}
