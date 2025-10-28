/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.backend.validation.validator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.validation.QuantifierValues;
import org.lareferencia.core.validation.ValidatorRuleResult;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@DisplayName("FieldExpressionValidatorRule Tests")
class FieldExpressionValidatorRuleTest {

    private OAIRecordMetadata metadata;

    @BeforeEach
    void setUp() {
        metadata = mock(OAIRecordMetadata.class);
        when(metadata.getIdentifier()).thenReturn("test:123");
    }

    private FieldExpressionValidatorRule createRuleWithQuantifier(QuantifierValues quantifier) {
        FieldExpressionValidatorRule rule = new FieldExpressionValidatorRule();
        rule.setQuantifier(quantifier);
        rule.evaluator = new FieldExpressionEvaluator(quantifier);
        return rule;
    }

    @Test
    @DisplayName("Should validate simple equality expression (==) - match")
    void testSimpleEqualityMatch() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("article"));
        rule.setExpression("dc.type=='article'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
        assertNotNull(result.getResults());
    }

    @Test
    @DisplayName("Should validate simple equality expression (==) - no match")
    void testSimpleEqualityNoMatch() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("book"));
        rule.setExpression("dc.type=='article'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
    }

    @Test
    @DisplayName("Should validate regex expression (=%) - match")
    void testRegexMatch() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.identifier")).thenReturn(Arrays.asList("http://example.com/123"));
        rule.setExpression("dc.identifier=%'http://.*'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should validate regex expression (=%) - no match")
    void testRegexNoMatch() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.identifier")).thenReturn(Arrays.asList("ftp://example.com"));
        rule.setExpression("dc.identifier=%'http://.*'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
    }

    @Test
    @DisplayName("Should apply ONE_ONLY quantifier - exactly one match")
    void testQuantifierOneOnlyMatch() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_ONLY);
        when(metadata.getFieldOcurrences("dc.language")).thenReturn(Arrays.asList("en"));
        rule.setExpression("dc.language=='en'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should apply ONE_ONLY quantifier - multiple matches fail")
    void testQuantifierOneOnlyMultipleFails() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_ONLY);
        when(metadata.getFieldOcurrences("dc.language")).thenReturn(Arrays.asList("en", "en"));
        rule.setExpression("dc.language=='en'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
    }

    @Test
    @DisplayName("Should apply ONE_OR_MORE quantifier - single match")
    void testQuantifierOneOrMoreSingle() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.subject")).thenReturn(Arrays.asList("physics"));
        rule.setExpression("dc.subject=='physics'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should apply ONE_OR_MORE quantifier - multiple matches")
    void testQuantifierOneOrMoreMultiple() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.subject")).thenReturn(Arrays.asList("physics", "physics", "chemistry"));
        rule.setExpression("dc.subject=='physics'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should apply ONE_OR_MORE quantifier - no match fails")
    void testQuantifierOneOrMoreNoMatch() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.subject")).thenReturn(Arrays.asList("chemistry"));
        rule.setExpression("dc.subject=='physics'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
    }

    @Test
    @DisplayName("Should apply ZERO_OR_MORE quantifier - always valid")
    void testQuantifierZeroOrMore() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ZERO_OR_MORE);
        when(metadata.getFieldOcurrences("dc.rights")).thenReturn(Arrays.asList("open access"));
        rule.setExpression("dc.rights=='closed'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid()); // ZERO_OR_MORE always returns true (>= 0)
    }

    @Test
    @DisplayName("Should apply ZERO_ONLY quantifier - no matches valid")
    void testQuantifierZeroOnlyValid() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ZERO_ONLY);
        when(metadata.getFieldOcurrences("dc.embargo")).thenReturn(Arrays.asList("none"));
        rule.setExpression("dc.embargo=='active'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should apply ZERO_ONLY quantifier - any match fails")
    void testQuantifierZeroOnlyFails() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ZERO_ONLY);
        when(metadata.getFieldOcurrences("dc.embargo")).thenReturn(Arrays.asList("active"));
        rule.setExpression("dc.embargo=='active'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
    }

    @Test
    @DisplayName("Should apply ALL quantifier - all occurrences match")
    void testQuantifierAllMatch() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ALL);
        when(metadata.getFieldOcurrences("dc.format")).thenReturn(Arrays.asList("pdf", "pdf", "pdf"));
        rule.setExpression("dc.format=='pdf'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should apply ALL quantifier - partial match fails")
    void testQuantifierAllPartialFails() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ALL);
        when(metadata.getFieldOcurrences("dc.format")).thenReturn(Arrays.asList("pdf", "docx", "pdf"));
        rule.setExpression("dc.format=='pdf'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
    }

    @Test
    @DisplayName("Should handle AND operator in expression")
    void testAndOperator() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("article"));
        when(metadata.getFieldOcurrences("dc.language")).thenReturn(Arrays.asList("en"));
        rule.setExpression("dc.type=='article' AND dc.language=='en'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should handle AND operator - one condition fails")
    void testAndOperatorOneFails() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("article"));
        when(metadata.getFieldOcurrences("dc.language")).thenReturn(Arrays.asList("es"));
        rule.setExpression("dc.type=='article' AND dc.language=='en'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
    }

    @Test
    @DisplayName("Should handle OR operator in expression")
    void testOrOperator() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("book"));
        rule.setExpression("dc.type=='article' OR dc.type=='book'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should handle OR operator - both conditions fail")
    void testOrOperatorBothFail() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("thesis"));
        rule.setExpression("dc.type=='article' OR dc.type=='book'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
    }

    @Test
    @DisplayName("Should handle NOT operator in expression")
    void testNotOperator() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("book"));
        rule.setExpression("NOT dc.type=='article'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should handle complex nested expression with parentheses")
    void testComplexNestedExpression() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("article"));
        when(metadata.getFieldOcurrences("dc.language")).thenReturn(Arrays.asList("en"));
        when(metadata.getFieldOcurrences("dc.rights")).thenReturn(Arrays.asList("open"));
        rule.setExpression("(dc.type=='article' AND dc.language=='en') OR dc.rights=='open'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should handle field with no occurrences")
    void testNoOccurrences() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.nonexistent")).thenReturn(Collections.emptyList());
        rule.setExpression("dc.nonexistent=='value'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid());
        assertTrue(result.getResults().size() > 0); // Should have "no_occurrences_found" result
    }

    @Test
    @DisplayName("Should handle multiple occurrences with partial matches")
    void testMultipleOccurrencesPartialMatch() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.contributor")).thenReturn(Arrays.asList("Smith, John", "Doe, Jane", "Smith, Mary"));
        rule.setExpression("dc.contributor=%'Smith.*'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid()); // At least one matches
    }

    @Test
    @DisplayName("Should validate regex with special characters")
    void testRegexWithSpecialCharacters() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.identifier")).thenReturn(Arrays.asList("10.1234/example-2023"));
        rule.setExpression("dc.identifier=%'10\\.\\d+/.*'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should handle empty field value")
    void testEmptyFieldValue() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.description")).thenReturn(Arrays.asList(""));
        rule.setExpression("dc.description==''");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should handle case-sensitive equality")
    void testCaseSensitiveEquality() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("Article"));
        rule.setExpression("dc.type=='article'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertFalse(result.getValid()); // Case mismatch
    }

    @Test
    @DisplayName("Should validate with Unicode characters")
    void testUnicodeCharacters() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.title")).thenReturn(Arrays.asList("Título en español"));
        rule.setExpression("dc.title=='Título en español'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid());
    }

    @Test
    @DisplayName("Should handle expression with whitespace in field values")
    void testWhitespaceInFieldValues() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.publisher")).thenReturn(Arrays.asList("  Springer  "));
        rule.setExpression("dc.publisher=='  Springer  '");

        ValidatorRuleResult result = rule.validate(metadata);

        assertTrue(result.getValid()); // Exact match including whitespace
    }

    @Test
    @DisplayName("Should return evaluation results")
    void testEvaluationResults() {
        FieldExpressionValidatorRule rule = createRuleWithQuantifier(QuantifierValues.ONE_OR_MORE);
        when(metadata.getFieldOcurrences("dc.type")).thenReturn(Arrays.asList("article", "book"));
        rule.setExpression("dc.type=='article'");

        ValidatorRuleResult result = rule.validate(metadata);

        assertNotNull(result.getResults());
        // Should have results from the evaluator
    }
}
