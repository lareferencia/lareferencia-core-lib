package org.lareferencia.core.worker.validation.validator;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.worker.validation.RuleSerializer;

class LargeControlledValueFieldContentValidatorRuleTest {
    @Test void historicalJsonKeepsCsvAndExactMatches() throws Exception {
        var serializer = new RuleSerializer();
        var json = """
                {"@class":"org.lareferencia.backend.validation.validator.LargeControlledValueFieldContentValidatorRule",
                 "fieldname":"dc.language","controlledValuesCSV":"es;;en;es; pt;中文;"}
                """;
        var rule = (LargeControlledValueFieldContentValidatorRule) serializer.deserializeValidatorFromJsonString(json);
        for (String term : List.of("es", "", "en", " pt", "中文")) {
            assertTrue(rule.validate(term).isValid(), term);
        }
        for (String term : List.of("ES", "pt", "en ")) {
            assertFalse(rule.validate(term).isValid(), term);
        }
        assertFalse(rule.validate((String) null).isValid());
        var export = new ObjectMapper().readTree(serializer.serializeValidatorToJsonString(rule));
        assertEquals("es;;en;es; pt;中文;", export.get("controlledValuesCSV").asText());
        assertFalse(export.has("controlledValues"));
    }

    @Test void lookupsFollowCsvFileAndLiveListEdits(@TempDir Path dir) throws Exception {
        var rule = new LargeControlledValueFieldContentValidatorRule();
        rule.setControlledValuesFromCsvString("es;es;en");
        assertTrue(rule.validate("es").isValid());
        var terms = rule.getControlledValues();
        terms.remove("es");
        assertTrue(rule.validate("es").isValid());
        terms.set(0, "pt");
        assertFalse(rule.validate("es").isValid());
        assertTrue(rule.validate("pt").isValid());
        Path file = Files.writeString(dir.resolve("terms.txt"), "fr\nfr\n");
        rule.setControlledValuesFileName(file.toString());
        assertTrue(rule.validate("fr").isValid());
        assertEquals("pt;en;fr;fr", rule.getControlledValuesCSV());
        terms.subList(2, 4).clear();
        assertFalse(rule.validate("fr").isValid());
        rule.setControlledValuesCSV("de");
        assertTrue(rule.validate("de").isValid());
        assertTrue(rule.validate("en").isValid());

        var replacement = new ArrayList<>(List.of("it"));
        rule.setControlledValues(replacement);
        assertFalse(rule.validate("en").isValid());
        replacement.add("nl");
        assertTrue(rule.validate("nl").isValid());
    }
}
