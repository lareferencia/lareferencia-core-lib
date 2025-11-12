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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RemoveBlacklistOccrsRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("RemoveBlacklistOccrsRule Tests")
class RemoveBlacklistOccrsRuleTest {


    @Mock
    private SnapshotMetadata snapshotMetadata;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private RemoveBlacklistOccrsRule rule;

    @BeforeEach
    void setUp() {
        rule = new RemoveBlacklistOccrsRule();
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
    @DisplayName("Should remove blacklisted occurrences")
    void testRemoveBlacklistedOccurrences() {
        List<String> blacklist = Arrays.asList("spam", "unwanted", "forbidden");
        rule.setBlacklist(blacklist);

        Node validNode = createMockNode("Computer Science");
        Node blacklistedNode1 = createMockNode("spam");
        Node blacklistedNode2 = createMockNode("unwanted");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(validNode, blacklistedNode1, blacklistedNode2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata, never()).removeNode(validNode);
        verify(metadata).removeNode(blacklistedNode1);
        verify(metadata).removeNode(blacklistedNode2);
    }

    @Test
    @DisplayName("Should not transform when no blacklisted values found")
    void testNoBlacklistedValues() {
        List<String> blacklist = Arrays.asList("spam", "unwanted");
        rule.setBlacklist(blacklist);

        Node node1 = createMockNode("Valid Subject");
        Node node2 = createMockNode("Another Valid");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle empty blacklist")
    void testEmptyBlacklist() {
        rule.setBlacklist(Collections.emptyList());

        Node node = createMockNode("Some Subject");
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Collections.singletonList(node));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should handle empty field list")
    void testEmptyFieldList() {
        List<String> blacklist = Arrays.asList("spam");
        rule.setBlacklist(blacklist);

        when(metadata.getFieldNodes("dc.subject")).thenReturn(Collections.emptyList());

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertFalse(result);
        verify(metadata, never()).removeNode(any());
    }

    @Test
    @DisplayName("Should be case sensitive")
    void testCaseSensitive() {
        List<String> blacklist = Arrays.asList("Spam");
        rule.setBlacklist(blacklist);

        Node node1 = createMockNode("Spam");
        Node node2 = createMockNode("spam");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node1);
        verify(metadata, never()).removeNode(node2);
    }

    @Test
    @DisplayName("Should remove all blacklisted when all match")
    void testRemoveAllWhenAllBlacklisted() {
        List<String> blacklist = Arrays.asList("bad1", "bad2", "bad3");
        rule.setBlacklist(blacklist);

        Node node1 = createMockNode("bad1");
        Node node2 = createMockNode("bad2");
        Node node3 = createMockNode("bad3");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, node3));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node1);
        verify(metadata).removeNode(node2);
        verify(metadata).removeNode(node3);
    }

    @Test
    @DisplayName("Should handle Unicode characters in blacklist")
    void testUnicodeCharacters() {
        List<String> blacklist = Arrays.asList("prohibido", "不要");
        rule.setBlacklist(blacklist);

        Node node1 = createMockNode("prohibido");
        Node node2 = createMockNode("不要");
        Node validNode = createMockNode("Valid");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, validNode, node2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node1);
        verify(metadata).removeNode(node2);
        verify(metadata, never()).removeNode(validNode);
    }

    @Test
    @DisplayName("Should verify getter and setter for fieldName")
    void testFieldNameGetterSetter() {
        rule.setFieldName("dc.title");
        assertEquals("dc.title", rule.getFieldName());
    }

    @Test
    @DisplayName("Should verify getter and setter for blacklist")
    void testBlacklistGetterSetter() {
        List<String> blacklist = Arrays.asList("item1", "item2");
        rule.setBlacklist(blacklist);
        
        assertEquals(blacklist, rule.getBlacklist());
        assertEquals(2, rule.getBlacklist().size());
    }

    @Test
    @DisplayName("Should handle whitespace in blacklisted values")
    void testWhitespaceInValues() {
        List<String> blacklist = Arrays.asList("bad value", "  spaces  ");
        rule.setBlacklist(blacklist);

        Node node1 = createMockNode("bad value");
        Node node2 = createMockNode("  spaces  ");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle special characters in blacklist")
    void testSpecialCharacters() {
        List<String> blacklist = Arrays.asList("test@example.com", "http://spam.com");
        rule.setBlacklist(blacklist);

        Node node1 = createMockNode("test@example.com");
        Node node2 = createMockNode("http://spam.com");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle duplicate blacklisted occurrences")
    void testDuplicateBlacklistedOccurrences() {
        List<String> blacklist = Arrays.asList("spam");
        rule.setBlacklist(blacklist);

        Node node1 = createMockNode("spam");
        Node node2 = createMockNode("spam");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node1);
        verify(metadata).removeNode(node2);
    }

    @Test
    @DisplayName("Should handle empty string in blacklist")
    void testEmptyStringInBlacklist() {
        List<String> blacklist = Arrays.asList("", "spam");
        rule.setBlacklist(blacklist);

        Node node1 = createMockNode("");
        Node node2 = createMockNode("spam");
        Node validNode = createMockNode("Valid");
        
        when(metadata.getFieldNodes("dc.subject")).thenReturn(Arrays.asList(node1, node2, validNode));

        boolean result = rule.transform(snapshotMetadata, record, metadata);

        assertTrue(result);
        verify(metadata).removeNode(node1);
        verify(metadata).removeNode(node2);
        verify(metadata, never()).removeNode(validNode);
    }
}
