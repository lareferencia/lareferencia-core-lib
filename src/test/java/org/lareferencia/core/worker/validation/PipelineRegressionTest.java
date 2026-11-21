package org.lareferencia.core.worker.validation;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.domain.ValidatorRule;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.service.validation.ValidationService;
import org.lareferencia.core.worker.validation.transformer.*;
import org.lareferencia.core.worker.validation.validator.*;
import org.springframework.test.util.ReflectionTestUtils;

class PipelineRegressionTest {
    private static OAIRecordMetadata record(String... types) throws Exception {
        var xml = new OAIRecordMetadata("fixture");
        for (String type : types) xml.addFieldOcurrence("dc.type", type);
        return xml;
    }

    @Test void vocabularyCleanupPreservesFirstNodeAtBestRankAndUnknownSiblings() throws Exception {
        var xml = new OAIRecordMetadata("fixture", """
                <metadata><element name="a">
                  <field name="value">book</field><field name="value">local</field>
                  <field name="value" lang="es">paper</field><field name="value" lang="en">paper</field>
                  <field name="value">local</field>
                </element></metadata>
                """);
        var rule = new RemoveDuplicateVocabularyOccrsRule();
        rule.setFieldName("a");
        rule.setVocabulary(List.of("paper", "book", "paper"));
        var firstPaper = xml.getFieldNodes("a").get(2);
        assertTrue(rule.transform(null, null, xml));
        assertEquals(List.of("local", "paper", "local"), xml.getFieldOcurrences("a"));
        assertSame(firstPaper, xml.getFieldNodes("a").get(1));
        assertFalse(rule.transform(null, null, xml));
    }

    @Test void vocabularyDiagnosticsKeepOrderDuplicatesAndUnicodeLengthBoundary() throws Exception {
        String boundary = "🌎".repeat(255);
        String tooLong = boundary + "x";
        var rule = new SingleAcceptedVocabularyValidatorRule("dc.type",
                List.of(new VocabularyTerm(boundary, true), new VocabularyTerm(tooLong, true),
                        new VocabularyTerm("book", false)), List.of("ignored"));
        var onlyLong = rule.validate(record(tooLong));
        assertTrue(onlyLong.getValid());
        assertEquals("no_reportable_values_found", onlyLong.getResults().get(0).getReceivedValue());
        var mixed = rule.validate(record("ignored", "book", tooLong, boundary, "book"));
        assertFalse(mixed.getValid());
        assertEquals("book · " + boundary + " · book", mixed.getResults().get(0).getReceivedValue());
        assertEquals("no_vocabulary_occurrences_found",
                rule.validate(record("ignored")).getResults().get(0).getReceivedValue());
        assertEquals("no_reportable_values_found",
                rule.validate(record("ignored", tooLong + "y")).getResults().get(0).getReceivedValue());
        var unknown = rule.validate(record("local", "ignored", "local", "custom"));
        assertEquals(List.of("local", "local", "custom"), unknown.getResults().stream()
                .map(ContentValidatorResult::getReceivedValue).toList());
    }

    @Test void translationIsScopedToDestination() throws Exception {
        var xml = record("paper");
        xml.addFieldOcurrence("dc.subject", "article");
        var rule = new FieldContentTranslateRule();
        rule.setTestFieldName("dc.type");
        rule.setWriteFieldName("dc.type");
        rule.setTranslationArray(List.of(new Translation("paper", "article")));
        assertTrue(rule.transform(null, null, xml));
        assertEquals(List.of("article"), xml.getFieldOcurrences("dc.type"));
        assertEquals(List.of("article"), xml.getFieldOcurrences("dc.subject"));
    }

    @Test void regexTranslationPreservesLaterSourceValuesAndReplacesOnlyTheFirstMatch() throws Exception {
        var xml = record("xx", "Yx", "xxx", "xxx");
        var rule = new RegexTranslateRule();
        rule.setSourceFieldName("dc.type");
        rule.setTargetFieldName("dc.type");
        rule.setRegexSearch("(x)");
        rule.setRegexReplace("Y");
        rule.setRemoveMatchingOccurrences(true);
        assertTrue(rule.transform(null, null, xml));
        assertEquals(List.of("xx", "xxx", "YY", "Yxx"), xml.getFieldOcurrences("dc.type"));
    }

    @Test void absentFieldKeepsItsExpressionDiagnosticEvenWithAnInvalidRegex() throws Exception {
        var rule = new FieldExpressionValidatorRule();
        rule.setQuantifier(QuantifierValues.ALL);
        rule.setExpression("dc.type=%'['");
        var result = rule.validate(record());
        assertTrue(result.getValid());
        assertEquals("no_occurrences_found", result.getResults().get(0).getReceivedValue());
    }

    @Test void priorityReportsPreviousMutation() throws Exception {
        var xml = record("paper");
        var rule = new FieldContentPriorityTranslateRule();
        rule.setTestFieldName("dc.type");
        rule.setWriteFieldName("dc.type");
        rule.setTranslationArray(List.of(new Translation("paper", "article"), new Translation("book", "libro")));
        assertTrue(rule.transform(null, null, xml));
        assertFalse(rule.transform(null, null, xml));
    }

    @Test void conditionalDeduplicationReportsChangeEvenWhenPredicateFails() throws Exception {
        var xml = record("article", "article");
        var rule = new FieldContentConditionalAddOccrRule();
        rule.setFieldName("dc.type");
        rule.setValueToAdd("book");
        rule.setConditionalExpression("dc.type=='report'");
        rule.setRemoveDuplicatedOccurrences(true);
        assertTrue(rule.transform(null, null, xml));
        assertEquals(List.of("article"), xml.getFieldOcurrences("dc.type"));
        assertFalse(rule.transform(null, null, xml));
    }

    @Test void allExpressionQuantifiersComeFromPersistentModel() throws Exception {
        var service = new ValidationService();
        ReflectionTestUtils.setField(service, "serializer", new RuleSerializer());
        var model = new Validator();
        var rule = new ValidatorRule();
        rule.setId(1L);
        rule.setJsonserialization("{\"@class\":\"" + FieldExpressionValidatorRule.class.getName()
                + "\",\"expression\":\"dc.type=='article'\"}");
        model.getRules().add(rule);
        String[][] values = {{}, {"book"}, {"article"}, {"article", "book"}, {"article", "article"}};
        for (var q : QuantifierValues.values()) {
            rule.setQuantifier(q);
            for (var mandatory : List.of(true, false)) {
                rule.setMandatory(mandatory);
                var validator = service.createValidatorFromModel(model);
                for (var types : values) {
                    long matches = List.of(types).stream().filter("article"::equals).count();
                    boolean expected = switch (q) {
                        case ZERO_ONLY -> matches == 0;
                        case ONE_ONLY -> matches == 1;
                        case ONE_OR_MORE -> matches >= 1;
                        case ZERO_OR_MORE -> true;
                        case ALL -> matches == types.length;
                    };
                    var result = validator.validate(record(types), new ValidatorResult());
                    assertEquals(expected, result.getRulesResults().get(0).getValid(), q.name());
                    assertEquals(expected || !mandatory, result.isValid());
                }
            }
        }
    }

    @Test void allPreservesEachRuleFamilysTreatmentOfMissingAndMixedValues() throws Exception {
        var content = new RegexFieldContentValidatorRule();
        content.setFieldname("dc.type");
        content.setRegexString("article");
        var expression = new FieldExpressionValidatorRule();
        expression.setExpression("dc.type=='article'");
        var nodes = new NodeOccursConditionalValidatorRule();
        nodes.setSourceXPathExpression("//*[local-name()='field' and text()='article']");
        for (var rule : List.of(content, expression, nodes)) {
            rule.setQuantifier(QuantifierValues.ALL);
        }

        var missing = record();
        var mixed = record("article", "book");
        var matching = record("article", "article");
        for (var rule : List.of(content, expression)) {
            assertTrue(rule.validate(missing).getValid());
            assertFalse(rule.validate(mixed).getValid());
            assertTrue(rule.validate(matching).getValid());
        }
        assertFalse(nodes.validate(missing).getValid());
        assertFalse(nodes.validate(record("book")).getValid());
        assertTrue(nodes.validate(mixed).getValid());
        assertTrue(nodes.validate(matching).getValid());
    }

    @Test void emptyXmlElementIsRemovedAndItsSiblingSurvives() throws Exception {
        var xml = new OAIRecordMetadata("fixture", """
            <metadata><element name="dc"><field name="title"/><field name="title">  </field>
            <field name="title">Real</field><field name="creator"/></element></metadata>
            """);
        var rule = new RemoveEmptyOccrsRule();
        rule.setFieldName("dc:title");
        assertTrue(rule.transform(null, null, xml));
        assertEquals(2, xml.getDOMDocument().getElementsByTagName("field").getLength());
        assertEquals(List.of("Real"), xml.getFieldOcurrences("dc:title"));
        assertFalse(rule.transform(null, null, xml));
    }
    @Test void byteLimitAndLegacySizeSettings() throws Exception {
        var xml = record("áé漢");
        long size = xml.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        for (long limit : List.of(size + 1, size, size - 1)) {
            var rule = new ReduceRecordsBySizeRule(limit, List.of("dc.type"));
            assertEquals(limit < size, rule.transform(null, null, xml));
        }
        assertTrue(xml.getFieldOcurrences("dc.type").isEmpty());
        assertFalse(new ReduceRecordsBySizeRule(1, List.of("dc.type")).transform(null, null, xml));
        for (String setting : List.of("", ",\"maxRecordSize\":1000000")) {
            String json = "{\"@class\":\"" + ReduceHeavyRecords.class.getName() + "\",\"fieldsToRemove\":[\"dc.type\"]" + setting + "}";
            assertTrue(new RuleSerializer().deserializeTransformerFromJsonString(json).transform(null, null, record("x")));
        }
    }

    @Test void renameSelectsOriginalNodesAndDefaultsToLegacyLimit() throws Exception {
        for (int count : List.of(100, 101, 102)) {
            String fields = "<field name='value'>x</field>".repeat(count);
            String original = "<metadata><element name='a'>" + fields + "</element></metadata>";
            for (boolean unlimited : List.of(false, true)) {
                var simple = new FieldNameTranslateRule();
                simple.setSourceFieldName("a");
                simple.setTargetFieldName("b");
                simple.setProcessAllOccurrences(unlimited);
                var conditional = new FieldNameConditionalTranslateRule();
                conditional.setSourceXPathExpression("//field");
                conditional.setTargetFieldName("b");
                conditional.setProcessAllOccurrences(unlimited);
                for (ITransformerRule rule : List.of(simple, conditional)) {
                    String serialized = new RuleSerializer().serializeTransformerToJsonString(rule);
                    assertEquals(unlimited, serialized.contains("processAllOccurrences"));
                    var xml = new OAIRecordMetadata("fixture", original);
                    assertTrue(rule.transform(null, null, xml));
                    assertEquals(unlimited ? count : Math.min(101, count), xml.getFieldOcurrences("b").size());
                    assertEquals(count, xml.getFieldOcurrences("a").size() + xml.getFieldOcurrences("b").size());
                }
            }
        }
    }
    @Test void yearOnlyAffectsFingerprintsOfPipelinesUsingIt() {
        var fingerprints = new org.lareferencia.core.service.validation.ValidatorFingerprintService();
        var network = new org.lareferencia.core.domain.Network();
        var year = new DynamicYearRangeFieldContentValidatorRule();
        year.setFieldname("dc.date");
        year.useReferenceYear(1999);
        assertTrue(year.validate("1999").isValid());
        assertFalse(year.validate("2000").isValid());
        var serializer = new RuleSerializer();
        var normalize = new FieldContentNormalizeRule();
        normalize.setValidationRule(year);
        var transform = new org.lareferencia.core.domain.Transformer();
        var trule = new org.lareferencia.core.domain.TransformerRule();
        trule.setRunorder(1);
        trule.setJsonserialization(serializer.serializeTransformerToJsonString(normalize));
        transform.getRules().add(trule);
        assertEquals(fingerprints.fingerprintNetwork(network, false, 1999).getHash(),
                fingerprints.fingerprintNetwork(network, false, 2000).getHash());
        network.setSecondaryTransformer(transform);
        assertNotEquals(fingerprints.fingerprintNetwork(network, false, 1999).getHash(),
                fingerprints.fingerprintNetwork(network, false, 2000).getHash());
        network.setSecondaryTransformer(null);
        var model = new Validator();
        var rule = new ValidatorRule();
        rule.setJsonserialization(serializer.serializeValidatorToJsonString(year));
        model.getRules().add(rule);
        network.setValidator(model);
        assertNotEquals(fingerprints.fingerprintNetwork(network, false, 1999).getHash(),
                fingerprints.fingerprintNetwork(network, false, 2000).getHash());
    }

    @Test void destinationQualifiersAndNamedFieldsAreIndependent() throws Exception {
        var xml = record();
        xml.addFieldOcurrence("dc.type.driver", "paper");
        xml.addFieldOcurrence("dc.type.openaire", "article");
        xml.addFieldOcurrence("dc:subject", "translated");
        var rule = new FieldContentTranslateRule();
        rule.setTestFieldName("dc.type.*");
        rule.setWriteFieldName("dc.type.none");
        rule.setTranslationArray(List.of(new Translation("paper", "article")));
        assertTrue(rule.transform(null, null, xml));
        assertEquals(List.of("article"), xml.getFieldOcurrences("dc.type.*"));
        rule.setWriteFieldName("dc:title");
        rule.setReplaceOccurrence(false);
        rule.setTranslationArray(List.of(new Translation("article", "translated")));
        assertTrue(rule.transform(null, null, xml));
        assertEquals(List.of("translated"), xml.getFieldOcurrences("dc:title"));
        assertFalse(rule.transform(null, null, xml));
    }
}
