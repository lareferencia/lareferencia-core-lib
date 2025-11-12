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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FieldNameConditionalTranslateRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FieldNameConditionalTranslateRule Tests")
class FieldNameConditionalTranslateRuleTest {


    @Mock
    private SnapshotMetadata snapshotMetadata;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private FieldNameConditionalTranslateRule rule;

    @BeforeEach
    void setUp() {
        rule = new FieldNameConditionalTranslateRule();
        rule.setTargetFieldName("dc.contributor");
    }

    private Node createMockNode(String value) {
        Node node = mock(Node.class);
        Node textNode = mock(Node.class);
        when(node.getFirstChild()).thenReturn(textNode);
        when(textNode.getNodeValue()).thenReturn(value);
        return node;
    }

    @Test
    @DisplayName("Should translate field name using XPath expression")
    void testBasicXPathTranslation() {
        rule.setSourceXPathExpression("//dc:creator[@role='author']");

        Node node = createMockNode("John Doe");
        when(metadata.getFieldNodesByXPath("//dc:creator[@role='author']"))
            .thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.contributor", "John Doe");
        verify(metadata).removeNode(node);
    }

    @Test
    @DisplayName("Should handle multiple nodes matching XPath")
    void testMultipleNodesMatching() {
        rule.setSourceXPathExpression("//dc:creator");

        Node node1 = createMockNode("Author 1");
        Node node2 = createMockNode("Author 2");
        when(metadata.getFieldNodesByXPath("//dc:creator"))
            .thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.contributor", "Author 1");
        verify(metadata).addFieldOcurrence("dc.contributor", "Author 2");
        verify(metadata).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should not transform when no nodes match XPath")
    void testNoNodesMatching() {
        rule.setSourceXPathExpression("//dc:nonexistent");

        when(metadata.getFieldNodesByXPath("//dc:nonexistent"))
            .thenReturn(Collections.emptyList());

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertFalse(result);
        verify(metadata, never()).addFieldOcurrence(anyString(), anyString());
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle MAX_NODE_COUNT limit")
    void testMaxNodeCountLimit() {
        rule.setSourceXPathExpression("//dc:subject");

        // Create 101 nodes to test the limit
        List<Node> nodes = IntStream.range(0, 101)
            .mapToObj(i -> {
                Node node = mock(Node.class);
                Node textNode = mock(Node.class);
                lenient().when(node.getFirstChild()).thenReturn(textNode);
                lenient().when(textNode.getNodeValue()).thenReturn("Subject " + i);
                return node;
            })
            .collect(Collectors.toList());
        
        when(metadata.getFieldNodesByXPath("//dc:subject")).thenReturn(nodes);

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        // Should process MAX_NODE_COUNT + 1 nodes (101 total)
        verify(metadata, times(101)).addFieldOcurrence(anyString(), anyString());
        verify(metadata, times(101)).removeNode(any());
    }

    @Test
    @DisplayName("Should handle Unicode values")
    void testUnicodeValues() {
        rule.setSourceXPathExpression("//dc:creator");

        Node node = createMockNode("José García");
        when(metadata.getFieldNodesByXPath("//dc:creator"))
            .thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.contributor", "José García");
    }

    @Test
    @DisplayName("Should handle complex XPath expressions")
    void testComplexXPath() {
        rule.setSourceXPathExpression("//metadata/dc:creator[@type='personal' and @role='author']");

        Node node = createMockNode("Complex Author");
        when(metadata.getFieldNodesByXPath("//metadata/dc:creator[@type='personal' and @role='author']"))
            .thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.contributor", "Complex Author");
        verify(metadata).removeNode(node);
    }

    @Test
    @DisplayName("Should verify getter and setter for targetFieldName")
    void testTargetFieldNameGetterSetter() {
        rule.setTargetFieldName("dc.creator");
        assertEquals("dc.creator", rule.getTargetFieldName());
    }

    @Test
    @DisplayName("Should verify getter and setter for sourceXPathExpression")
    void testSourceXPathExpressionGetterSetter() {
        rule.setSourceXPathExpression("//dc:title");
        assertEquals("//dc:title", rule.getSourceXPathExpression());
    }

    @Test
    @DisplayName("Should handle empty string values")
    void testEmptyStringValues() {
        rule.setSourceXPathExpression("//dc:description");

        Node node = createMockNode("");
        when(metadata.getFieldNodesByXPath("//dc:description"))
            .thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.contributor", "");
    }

    @Test
    @DisplayName("Should handle special characters in values")
    void testSpecialCharacters() {
        rule.setSourceXPathExpression("//dc:identifier");

        Node node = createMockNode("http://example.org/resource/123");
        when(metadata.getFieldNodesByXPath("//dc:identifier"))
            .thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.contributor", "http://example.org/resource/123");
    }

    @Test
    @DisplayName("Should handle whitespace in values")
    void testWhitespaceInValues() {
        rule.setSourceXPathExpression("//dc:title");

        Node node = createMockNode("  Title with spaces  ");
        when(metadata.getFieldNodesByXPath("//dc:title"))
            .thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.contributor", "  Title with spaces  ");
    }

    @Test
    @DisplayName("Should handle exactly MAX_NODE_COUNT nodes")
    void testExactlyMaxNodeCount() {
        rule.setSourceXPathExpression("//dc:subject");

        // Create exactly 100 nodes
        List<Node> nodes = IntStream.range(0, 100)
            .mapToObj(i -> createMockNode("Subject " + i))
            .collect(Collectors.toList());
        
        when(metadata.getFieldNodesByXPath("//dc:subject")).thenReturn(nodes);

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata, times(100)).addFieldOcurrence(anyString(), anyString());
        verify(metadata, times(100)).removeNode(any());
    }

    @Test
    @DisplayName("Should translate to different target field")
    void testDifferentTargetField() {
        rule.setTargetFieldName("dc.author");
        rule.setSourceXPathExpression("//dc:creator");

        Node node = createMockNode("Jane Smith");
        when(metadata.getFieldNodesByXPath("//dc:creator"))
            .thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.author", "Jane Smith");
    }

    @Test
    @DisplayName("Should handle XPath with namespaces")
    void testXPathWithNamespaces() {
        rule.setSourceXPathExpression("//oai_dc:dc/dc:creator");

        Node node = createMockNode("Namespace Author");
        when(metadata.getFieldNodesByXPath("//oai_dc:dc/dc:creator"))
            .thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.contributor", "Namespace Author");
    }
}
