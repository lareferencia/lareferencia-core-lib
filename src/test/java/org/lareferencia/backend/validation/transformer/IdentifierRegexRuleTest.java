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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for IdentifierRegexRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("IdentifierRegexRule Tests")
class IdentifierRegexRuleTest {


    @Mock
    private NetworkRunningContext context;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private IdentifierRegexRule rule;

    @BeforeEach
    void setUp() {
        rule = new IdentifierRegexRule();
    }

    @Test
    @DisplayName("Should replace identifier using regex pattern")
    void testBasicRegexReplacement() {
        rule.setRegexSearch("oai:");
        rule.setRegexReplace("");
        
        when(record.getIdentifier()).thenReturn("oai:example.org:123");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("example.org:123");
    }

    @Test
    @DisplayName("Should handle complex regex patterns")
    void testComplexRegexPattern() {
        rule.setRegexSearch("^oai:([^:]+):(.*)$");
        rule.setRegexReplace("$1/$2");
        
        when(record.getIdentifier()).thenReturn("oai:repository.edu:article-123");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("repository.edu/article-123");
    }

    @Test
    @DisplayName("Should remove prefix using regex")
    void testRemovePrefix() {
        rule.setRegexSearch("^prefix-");
        rule.setRegexReplace("");
        
        when(record.getIdentifier()).thenReturn("prefix-identifier-123");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("identifier-123");
    }

    @Test
    @DisplayName("Should remove suffix using regex")
    void testRemoveSuffix() {
        rule.setRegexSearch("-suffix$");
        rule.setRegexReplace("");
        
        when(record.getIdentifier()).thenReturn("identifier-123-suffix");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("identifier-123");
    }

    @Test
    @DisplayName("Should replace all occurrences of pattern")
    void testReplaceAllOccurrences() {
        rule.setRegexSearch("-");
        rule.setRegexReplace("_");
        
        when(record.getIdentifier()).thenReturn("test-id-123-456");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("test_id_123_456");
    }

    @Test
    @DisplayName("Should handle identifier with no match")
    void testNoMatch() {
        rule.setRegexSearch("notfound");
        rule.setRegexReplace("replaced");
        
        when(record.getIdentifier()).thenReturn("identifier-123");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("identifier-123");
    }

    @Test
    @DisplayName("Should handle special regex characters")
    void testSpecialRegexCharacters() {
        rule.setRegexSearch("\\.");
        rule.setRegexReplace("-");
        
        when(record.getIdentifier()).thenReturn("doi.org.10.1234");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("doi-org-10-1234");
    }

    @Test
    @DisplayName("Should use capture groups in replacement")
    void testCaptureGroupsInReplacement() {
        rule.setRegexSearch("(\\d{4})-(\\d{2})-(\\d{2})");
        rule.setRegexReplace("$3/$2/$1");
        
        when(record.getIdentifier()).thenReturn("article-2023-12-25-test");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("article-25/12/2023-test");
    }

    @Test
    @DisplayName("Should handle empty identifier")
    void testEmptyIdentifier() {
        rule.setRegexSearch("test");
        rule.setRegexReplace("replaced");
        
        when(record.getIdentifier()).thenReturn("");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("");
    }

    @Test
    @DisplayName("Should normalize DOI identifiers")
    void testNormalizeDOI() {
        rule.setRegexSearch("^(?:doi:|DOI:|https?://doi\\.org/)");
        rule.setRegexReplace("doi:");
        
        when(record.getIdentifier()).thenReturn("https://doi.org/10.1234/test");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("doi:10.1234/test");
    }

    @Test
    @DisplayName("Should always return true")
    void testAlwaysReturnsTrue() {
        rule.setRegexSearch("anything");
        rule.setRegexReplace("replacement");
        
        when(record.getIdentifier()).thenReturn("test-identifier");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should verify getter and setter for regexSearch")
    void testRegexSearchGetterSetter() {
        rule.setRegexSearch("test-pattern");
        assertEquals("test-pattern", rule.getRegexSearch());
    }

    @Test
    @DisplayName("Should verify getter and setter for regexReplace")
    void testRegexReplaceGetterSetter() {
        rule.setRegexReplace("replacement-value");
        assertEquals("replacement-value", rule.getRegexReplace());
    }

    @Test
    @DisplayName("Should handle Unicode characters in identifier")
    void testUnicodeCharacters() {
        rule.setRegexSearch("título");
        rule.setRegexReplace("title");
        
        when(record.getIdentifier()).thenReturn("article-título-123");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("article-title-123");
    }

    @Test
    @DisplayName("Should remove all whitespace from identifier")
    void testRemoveWhitespace() {
        rule.setRegexSearch("\\s+");
        rule.setRegexReplace("");
        
        when(record.getIdentifier()).thenReturn("test identifier with spaces");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(record).setIdentifier("testidentifierwithspaces");
    }
}
