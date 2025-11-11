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
 * Unit tests for FieldAddRule
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FieldAddRule Tests")
class FieldAddRuleTest {


    @Mock
    private NetworkRunningContext context;
    @Mock
    private OAIRecord record;

    @Mock
    private OAIRecordMetadata metadata;

    private FieldAddRule rule;

    @BeforeEach
    void setUp() {
        rule = new FieldAddRule();
    }

    @Test
    @DisplayName("Should add field with specified value")
    void testAddField() {
        rule.setTargetFieldName("dc.publisher");
        rule.setValue("Test Publisher");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata, times(1)).addFieldOcurrence("dc.publisher", "Test Publisher");
    }

    @Test
    @DisplayName("Should add field with Unicode value")
    void testAddFieldWithUnicode() {
        rule.setTargetFieldName("dc.title");
        rule.setValue("Título en español");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.title", "Título en español");
    }

    @Test
    @DisplayName("Should add field with empty value")
    void testAddFieldWithEmptyValue() {
        rule.setTargetFieldName("dc.description");
        rule.setValue("");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.description", "");
    }

    @Test
    @DisplayName("Should add field with numeric value")
    void testAddFieldWithNumericValue() {
        rule.setTargetFieldName("dc.year");
        rule.setValue("2023");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.year", "2023");
    }

    @Test
    @DisplayName("Should add field with special characters")
    void testAddFieldWithSpecialCharacters() {
        rule.setTargetFieldName("dc.identifier");
        rule.setValue("https://doi.org/10.1234/test-123");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.identifier", "https://doi.org/10.1234/test-123");
    }

    @Test
    @DisplayName("Should add field with whitespace")
    void testAddFieldWithWhitespace() {
        rule.setTargetFieldName("dc.subject");
        rule.setValue("  Computer Science  ");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.subject", "  Computer Science  ");
    }

    @Test
    @DisplayName("Should add field with multiline value")
    void testAddFieldWithMultilineValue() {
        rule.setTargetFieldName("dc.description");
        rule.setValue("Line 1\nLine 2\nLine 3");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.description", "Line 1\nLine 2\nLine 3");
    }

    @Test
    @DisplayName("Should always return true indicating transformation occurred")
    void testAlwaysReturnsTrue() {
        rule.setTargetFieldName("dc.test");
        rule.setValue("test value");

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
    }

    @Test
    @DisplayName("Should handle null value gracefully")
    void testNullValue() {
        rule.setTargetFieldName("dc.test");
        rule.setValue(null);

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.test", null);
    }

    @Test
    @DisplayName("Should add field with very long value")
    void testAddFieldWithLongValue() {
        String longValue = "a".repeat(1000);
        rule.setTargetFieldName("dc.description");
        rule.setValue(longValue);

        boolean result = rule.transform(context, record, metadata);

        assertTrue(result);
        verify(metadata).addFieldOcurrence("dc.description", longValue);
    }

    @Test
    @DisplayName("Should verify getter and setter for targetFieldName")
    void testTargetFieldNameGetterSetter() {
        rule.setTargetFieldName("dc.creator");
        assertEquals("dc.creator", rule.getTargetFieldName());
    }

    @Test
    @DisplayName("Should verify getter and setter for value")
    void testValueGetterSetter() {
        rule.setValue("Test Value");
        assertEquals("Test Value", rule.getValue());
    }
}
