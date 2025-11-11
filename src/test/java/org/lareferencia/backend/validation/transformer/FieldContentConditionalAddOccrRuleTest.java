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
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.w3c.dom.Node;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FieldContentConditionalAddOccrRule
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("FieldContentConditionalAddOccrRule Tests")
class FieldContentConditionalAddOccrRuleTest {


    @Mock
    private NetworkRunningContext context;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private FieldContentConditionalAddOccrRule rule;

    @BeforeEach
    void setUp() {
        rule = new FieldContentConditionalAddOccrRule();
        rule.setFieldName("dc.type");
        rule.setValueToAdd("article");
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(textNode);
        when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should add value when condition is met")
    void testAddValueWhenConditionMet() {
        rule.setConditionalExpression("dc.publisher=%'.*'");
        
        Node publisherNode = createMockNode("Test Publisher");
        when(metadata.getFieldOcurrences("dc.publisher")).thenReturn(Collections.singletonList("Test Publisher"));
        when(metadata.getFieldNodes("dc.publisher")).thenReturn(Collections.singletonList(publisherNode));
        when(metadata.getFieldNodes("dc.type")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "article");
    }

    @Test
    @DisplayName("Should not add value when condition is not met")
    void testDoNotAddValueWhenConditionNotMet() {
        rule.setConditionalExpression("dc.nonexistent");
        
        when(metadata.getFieldNodes("dc.nonexistent")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(context, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
    }

    @Test
    @DisplayName("Should remove duplicates when flag is set")
    void testRemoveDuplicatesWhenFlagSet() {
        rule.setConditionalExpression("dc.title=%'.*'");
        rule.setRemoveDuplicatedOccurrences(true);
        
        Node titleNode = createMockNode("Test Title");
        Node typeNode1 = createMockNode("article");
        Node typeNode2 = createMockNode("article");
        
        when(metadata.getFieldOcurrences("dc.title")).thenReturn(Collections.singletonList("Test Title"));
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(titleNode));
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(typeNode1, typeNode2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "article");
        verify(metadata).removeNode(typeNode2);
    }

    @Test
    @DisplayName("Should not remove duplicates when flag is false")
    void testDoNotRemoveDuplicatesWhenFlagFalse() {
        rule.setConditionalExpression("dc.title=%'.*'");
        rule.setRemoveDuplicatedOccurrences(false);
        
        Node titleNode = createMockNode("Test Title");
        Node typeNode1 = createMockNode("article");
        Node typeNode2 = createMockNode("article");
        
        when(metadata.getFieldOcurrences("dc.title")).thenReturn(Collections.singletonList("Test Title"));
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(titleNode));
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(typeNode1, typeNode2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "article");
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle multiple different values without removal")
    void testMultipleDifferentValues() {
        rule.setConditionalExpression("dc.title=%'.*'");
        rule.setRemoveDuplicatedOccurrences(true);
        
        Node titleNode = createMockNode("Test Title");
        Node typeNode1 = createMockNode("article");
        Node typeNode2 = createMockNode("book");
        
        when(metadata.getFieldOcurrences("dc.title")).thenReturn(Collections.singletonList("Test Title"));
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(titleNode));
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(typeNode1, typeNode2));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "article");
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should add value with Unicode characters")
    void testAddUnicodeValue() {
        rule.setConditionalExpression("dc.title=%'.*'");
        rule.setValueToAdd("artículo científico");
        
        Node titleNode = createMockNode("Test");
        when(metadata.getFieldOcurrences("dc.title")).thenReturn(Collections.singletonList("Test"));
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(titleNode));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "artículo científico");
    }

    @Test
    @DisplayName("Should verify getter and setter for fieldName")
    void testFieldNameGetterSetter() {
        rule.setFieldName("dc.subject");
        assertEquals("dc.subject", rule.getFieldName());
    }

    @Test
    @DisplayName("Should verify getter and setter for valueToAdd")
    void testValueToAddGetterSetter() {
        rule.setValueToAdd("test-value");
        assertEquals("test-value", rule.getValueToAdd());
    }

    @Test
    @DisplayName("Should verify getter and setter for conditionalExpression")
    void testConditionalExpressionGetterSetter() {
        rule.setConditionalExpression("dc.identifier");
        assertEquals("dc.identifier", rule.getConditionalExpression());
    }

    @Test
    @DisplayName("Should verify getter and setter for removeDuplicatedOccurrences")
    void testRemoveDuplicatedOccurrencesGetterSetter() {
        rule.setRemoveDuplicatedOccurrences(true);
        assertTrue(rule.getRemoveDuplicatedOccurrences());
        
        rule.setRemoveDuplicatedOccurrences(false);
        assertFalse(rule.getRemoveDuplicatedOccurrences());
    }

    @Test
    @DisplayName("Should handle empty value to add")
    void testEmptyValueToAdd() {
        rule.setConditionalExpression("dc.title=%'.*'");
        rule.setValueToAdd("");
        
        Node titleNode = createMockNode("Test");
        when(metadata.getFieldOcurrences("dc.title")).thenReturn(Collections.singletonList("Test"));
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(titleNode));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "");
    }

    @Test
    @DisplayName("Should remove multiple duplicates")
    void testRemoveMultipleDuplicates() {
        rule.setConditionalExpression("dc.title=%'.*'");
        rule.setRemoveDuplicatedOccurrences(true);
        
        Node titleNode = createMockNode("Test Title");
        Node typeNode1 = createMockNode("article");
        Node typeNode2 = createMockNode("article");
        Node typeNode3 = createMockNode("article");
        
        when(metadata.getFieldOcurrences("dc.title")).thenReturn(Collections.singletonList("Test Title"));
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(titleNode));
        when(metadata.getFieldNodes("dc.type")).thenReturn(Arrays.asList(typeNode1, typeNode2, typeNode3));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "article");
        verify(metadata).removeNode(typeNode2);
        verify(metadata).removeNode(typeNode3);
        verify(metadata, never()).removeNode(typeNode1);
    }

    @Test
    @DisplayName("Should handle special characters in value")
    void testSpecialCharactersInValue() {
        rule.setConditionalExpression("dc.title=%'.*'");
        rule.setValueToAdd("http://purl.org/dc/dcmitype/Text");
        
        Node titleNode = createMockNode("Test");
        when(metadata.getFieldOcurrences("dc.title")).thenReturn(Collections.singletonList("Test"));
        when(metadata.getFieldNodes("dc.title")).thenReturn(Collections.singletonList(titleNode));

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.type", "http://purl.org/dc/dcmitype/Text");
    }
}
