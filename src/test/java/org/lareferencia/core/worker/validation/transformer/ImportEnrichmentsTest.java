package org.lareferencia.core.worker.validation.transformer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.repository.catalog.OAIRecord;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.worker.validation.SQLiteEnrichmentEdition;
import java.nio.file.*;
import java.sql.DriverManager;
import java.util.List;
import org.lareferencia.core.worker.validation.RuleSerializer;
import static org.junit.jupiter.api.Assertions.*;

class ImportEnrichmentsTest {
    @TempDir Path dir;

    @Test void loadsPreviouslySavedRule() {
        var rule = new RuleSerializer().deserializeTransformerFromJsonString("""
            {"@class":"org.lareferencia.core.worker.validation.transformer.SQLiteEnrichmentRule",
             "filename":"facetas.sqlite","mappings":[{"source":"dc.subject.ods"}]}
            """);
        assertEquals("facetas.sqlite", assertInstanceOf(ImportEnrichments.class, rule).getFilename());
    }

    @Test void rejectsPathsAndNonliteralDestinationsOnDeserialization() {
        String config = """
            {"@class":"org.lareferencia.core.worker.validation.transformer.ImportEnrichments",
             "filename":"FILE","mappings":[{"source":"dc.subject.ods","destination":"DEST"}]}
            """;
        var serializer = new RuleSerializer();
        assertThrows(IllegalArgumentException.class, () -> serializer.deserializeTransformerFromJsonString(
                config.replace("FILE", "../edition.sqlite").replace("DEST", "dc.subject.ods")));
        assertThrows(IllegalArgumentException.class, () -> serializer.deserializeTransformerFromJsonString(
                config.replace("FILE", "edition.sqlite").replace("DEST", "dc.subject.*")));
    }

    @Test void appliesPublishedFieldsByStoredIdentity() throws Exception {
        Path file = dir.resolve("edition.sqlite");
        try (var db = DriverManager.getConnection("jdbc:sqlite:" + file);
             var statement = db.createStatement();
             var fixture = getClass().getResourceAsStream("/enrichment/edition.sql")) {
            for (String sql : new String(fixture.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).split(";"))
                if (!sql.isBlank()) statement.execute(sql);
        }
        var value = new ImportEnrichments.EnrichmentMapping();
        value.setSource("dc.rights.license_l2");
        var country = new ImportEnrichments.EnrichmentMapping();
        country.setSource(value.getSource());
        country.setProperty("jurisdiction");
        country.setProducer("rights");
        country.setDestination("dc.rights.country");
        var rule = new ImportEnrichments();
        rule.setFilename("edition.sqlite");
        rule.setMappings(List.of(value, country));
        rule.validateConfiguration();
        var record = new OAIRecord();
        record.setId("stored-id");
        record.setIdentifier("rewritten-id");
        record.setOriginalMetadataHash("hash");
        var xml = new OAIRecordMetadata("id", "<metadata/>");
        xml.addFieldOcurrence(value.getSource(), "existing");
        try (var edition = new SQLiteEnrichmentEdition(file, "PY")) {
            rule.bind(edition);
            assertTrue(rule.transform(null, record, xml));
            assertEquals(List.of("existing","CC","X","BY"), xml.getFieldOcurrences(value.getSource()));
            assertEquals(List.of("BR"), xml.getFieldOcurrences(country.getDestination()));
            assertFalse(rule.transform(null, record, xml));
            record.setOriginalMetadataHash("changed");
            assertTrue(edition.read(record.getId(), record.getOriginalMetadataHash()).isEmpty());
            assertTrue(edition.read("missing","hash").isEmpty());
        }
        assertThrows(Exception.class, () -> new SQLiteEnrichmentEdition(file, "BR"));
        try (var db = DriverManager.getConnection("jdbc:sqlite:" + file); var sql = db.createStatement()) {
            sql.executeUpdate("UPDATE package SET variant='processed'");
        }
        assertThrows(java.io.IOException.class, () -> new SQLiteEnrichmentEdition(file, "PY"));
        try (var absent = new SQLiteEnrichmentEdition(dir.resolve("missing"), "PY")) {
            assertEquals("absent", absent.digest());
        }
    }
}
