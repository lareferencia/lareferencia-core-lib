package org.lareferencia.core.export;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.metadata.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class MetadataExportServiceTest {
    @TempDir Path temp;
    Path mapping;
    ExportSourceResolver resolver;
    IMetadataStore store;
    MetadataExportService service;
    List<ExportSourceResolver.Source> sources;
    @BeforeEach void setup() throws Exception {
        mapping = temp.resolve("mapping.json");
        Files.writeString(mapping, XoaiExportMappingTest.spec("{\"titles\":{\"forEach\":\"dc.title.*\",\"fields\":{\"value\":{\"select\":\":value\",\"type\":\"string\",\"many\":false}}}}"));
        resolver = mock(ExportSourceResolver.class); store = mock(IMetadataStore.class);
        sources = List.of(source("RCAAP", 1), source("IBICT", 2));
        when(resolver.resolve(any())).thenReturn(new ExportSourceResolver.Selection(sources, ExportJson.JSON.createArrayNode()));
        when(store.getMetadata(any(), eq("original"))).thenReturn(XoaiExportMappingTest.XML);
        when(store.getMetadata(any(), eq("published"))).thenReturn(XoaiExportMappingTest.XML.replace("Outro", "Changed"));
        service = new MetadataExportService(resolver, store);
    }
    private ExportSourceResolver.Source source(String acronym, long id) throws Exception {
        Path directory = temp.resolve(acronym); Files.createDirectories(directory.resolve("catalog")); Files.createDirectories(directory.resolve("validation"));
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("catalog/catalog.db")); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE oai_record(id TEXT PRIMARY KEY,identifier TEXT,datestamp TEXT,original_metadata_hash TEXT,deleted INTEGER)");
            s.execute("INSERT INTO oai_record VALUES ('abc','oai:same','2026-11-05','original',0),('def','oai:other','2026-11-05','original',0),('gone','oai:gone','2026-11-05',NULL,1)");
        }
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + directory.resolve("validation/validation.db")); Statement s = c.createStatement()) {
            s.execute("CREATE TABLE record_validation(identifier_hash TEXT PRIMARY KEY,identifier TEXT,is_valid INTEGER,is_transformed INTEGER,published_metadata_hash TEXT)");
            s.execute("INSERT INTO record_validation VALUES ('abc','oai:same',1,1,'published'),('def','oai:other',0,0,'original')");
        }
        SnapshotMetadata metadata = new SnapshotMetadata(id); var network = new org.lareferencia.core.domain.Network(); network.setAcronym(acronym); metadata.setNetwork(network);
        return new ExportSourceResolver.Source("source-" + id, metadata, directory, ExportJson.object().put("network", acronym), () -> {});
    }
    private ExportRequest request(String variant, String format, String layout, boolean validOnly, String compression, Long maxRecords, Long maxBytes) {
        return new ExportRequest(List.of("RCAAP", "IBICT"), false, variant, format, format.equals("xml") ? null : mapping,
                layout, "none", maxRecords, maxBytes, compression, validOnly, temp.resolve("output"));
    }
    @Test void twoSourcesBothVariantsBatchAndRemapWithoutMutatingDatabases() throws Exception {
        Map<Path,String> before = new HashMap<>();
        for (var source : sources) for (String file : List.of("catalog/catalog.db", "validation/validation.db"))
            before.put(source.directory().resolve(file), ExportJson.hash(source.directory().resolve(file)));
        for (String variant : List.of("original", "processed")) {
            var result = service.export(request(variant, "jsonl", "collection", false, "gzip", 1L, null));
            assertEquals("COMPLETE", result.status()); assertEquals(4, result.exported());
            JsonNode manifest = ExportJson.JSON.readTree(result.directory().resolve("manifest.json").toFile());
            assertEquals(4, manifest.path("counts").path("attempted").asInt());
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + result.directory().resolve("records.sqlite"));
                    Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM records ORDER BY record_key")) {
                int count = 0;
                while (rs.next()) {
                    Path file = result.directory().resolve(rs.getString("file"));
                    String text;
                    try (var input = new GZIPInputStream(Files.newInputStream(file))) { text = new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8); }
                    JsonNode record = ExportJson.JSON.readTree(text);
                    assertEquals(rs.getString("network"), record.path("network").asText());
                    assertEquals(rs.getString("catalog_id"), record.path("catalogId").asText());
                    assertFalse(record.has("recordKey")); assertFalse(record.has("source")); assertFalse(record.has("input"));
                    assertFalse(record.has("exportId")); assertFalse(record.toString().contains("payloadHash"));
                    assertEquals(rs.getString("metadata_hash"), record.path("metadataHash").asText());
                    assertEquals(rs.getString("identifier"), record.path("identifier").asText());
                    assertEquals(1, rs.getInt("position"));
                    String expected = variant.equals("processed") && rs.getString("catalog_id").equals("abc") ? "Changed" : "Outro";
                    assertEquals(expected, record.at("/data/titles/1/value").asText());
                    var source = sources.stream().filter(x -> x.acronym().equals(rsString(record, "network"))).findFirst().orElseThrow();
                    try (Connection original = DriverManager.getConnection("jdbc:sqlite:" + source.directory().resolve("catalog/catalog.db"));
                            PreparedStatement lookup = original.prepareStatement("SELECT identifier FROM oai_record WHERE id=?")) {
                        lookup.setString(1, record.path("catalogId").asText());
                        try (ResultSet match = lookup.executeQuery()) { assertTrue(match.next()); assertEquals(record.path("identifier").asText(), match.getString(1)); }
                    }
                    count++;
                }
                assertEquals(4, count);
            }
            for (JsonNode file : manifest.path("files")) assertEquals(file.path("sha256").asText(), ExportJson.hash(result.directory().resolve(file.path("path").asText())));
            assertFalse(Files.exists(result.directory().resolve("records.sqlite-wal")));
        }
        for (var item : before.entrySet()) assertEquals(item.getValue(), ExportJson.hash(item.getKey()));
    }
    private static String rsString(JsonNode record, String field) { return record.path(field).asText(); }
    @Test void jsonArraysAndXmlLayoutsRemainWellFormedAndIndividuallyLocated() throws Exception {
        for (String format : List.of("json", "xml")) for (String layout : format.equals("xml") ? List.of("collection", "per-record") : List.of("collection")) {
            var result = service.export(request("original", format, layout, false, "none", null, null));
            assertEquals(4, result.exported(), () -> { try { return Files.readString(result.directory().resolve("errors.jsonl")); } catch (Exception e) { return e.toString(); } });
            var manifest = ExportJson.JSON.readTree(result.directory().resolve("manifest.json").toFile());
            for (var file : manifest.path("files")) {
                String path = file.path("path").asText();
                if (!path.startsWith("data/") || !path.endsWith("." + format)) continue;
                String text = Files.readString(result.directory().resolve(path));
                if (format.equals("json")) assertEquals(4, ExportJson.JSON.readTree(text).size());
                else {
                    var document = ExportXml.parse(text);
                    if (layout.equals("collection")) assertEquals(4, document.getElementsByTagName("record").getLength());
                    else assertEquals("metadata", document.getDocumentElement().getLocalName());
                }
            }
        }
    }
    @Test void missingXmlProducesPartialWithErrorsAndFilteredRowsAreIndexed() throws Exception {
        when(store.getMetadata(any(), eq("published"))).thenThrow(new MetadataRecordStoreException("missing"));
        var result = service.export(request("processed", "jsonl", "collection", true, "none", null, null));
        assertEquals("PARTIAL", result.status()); assertEquals(2, result.errors()); assertEquals(2, result.filtered());
        assertEquals(2, Files.readAllLines(result.directory().resolve("errors.jsonl")).size());
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + result.directory().resolve("records.sqlite")); Statement s = c.createStatement();
                ResultSet rows = s.executeQuery("SELECT count(*) FROM records")) { assertTrue(rows.next()); assertEquals(4, rows.getInt(1)); }
    }
    @Test void missingValidationIsFailureAndChangedSourceIsDetected() throws Exception {
        var original = sources.get(0);
        AtomicInteger calls = new AtomicInteger();
        var unstable = new ExportSourceResolver.Source(original.reference(), original.metadata(), original.directory(), original.provenance(), () -> {
            if (calls.incrementAndGet() > 1) throw new IllegalStateException("changed");
        });
        when(resolver.resolve(any())).thenReturn(new ExportSourceResolver.Selection(List.of(unstable), ExportJson.JSON.createArrayNode()));
        var failed = assertThrows(MetadataExportService.ExportFailure.class, () -> service.export(request("processed", "jsonl", "collection", false, "none", null, null)));
        assertEquals("FAILED", ExportJson.JSON.readTree(failed.directory().resolve("manifest.json").toFile()).path("status").asText());
        when(resolver.resolve(any())).thenReturn(new ExportSourceResolver.Selection(List.of(original), ExportJson.JSON.createArrayNode()));
        Files.delete(original.directory().resolve("validation/validation.db"));
        assertThrows(MetadataExportService.ExportFailure.class, () -> service.export(request("processed", "jsonl", "collection", false, "none", null, null)));
    }
    @Test void previewDoesNotWriteAndLimitsAttemptedRecords() throws Exception {
        var preview = service.preview(request("processed", "jsonl", "collection", false, "none", null, null), 1);
        assertEquals(1, preview.path("records").size()); assertFalse(Files.exists(temp.resolve("output")));
    }
    @Test void rejectsDuplicateKeysAndTrailingMappingContent() throws Exception {
        Files.writeString(mapping, Files.readString(mapping) + " {}");
        assertThrows(Exception.class, () -> service.validateMapping(mapping));
        Files.writeString(mapping, "{\"name\":\"a\",\"name\":\"b\"}");
        assertThrows(Exception.class, () -> service.validateMapping(mapping));
    }
    private void changeValidation(String sql) throws Exception {
        for (var source : sources) try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + source.directory().resolve("validation/validation.db")); Statement statement = c.createStatement()) {
            statement.execute(sql);
        }
    }
    @Test void missingValidationRowsAndIdentityConflictsAreRecordErrorsWithoutFallback() throws Exception {
        changeValidation("UPDATE record_validation SET identifier='oai:wrong' WHERE identifier_hash='abc'");
        changeValidation("DELETE FROM record_validation WHERE identifier_hash='def'");
        var result = service.export(request("processed", "jsonl", "collection", false, "none", null, null));
        assertEquals("PARTIAL", result.status()); assertEquals(4, result.errors()); assertEquals(0, result.exported());
        verifyNoInteractions(store);
        String errors = Files.readString(result.directory().resolve("errors.jsonl"));
        assertTrue(errors.contains("does not match")); assertTrue(errors.contains("Validation record missing"));
    }
    @Test void missingPublishedHashesAndMalformedXmlAreReported() throws Exception {
        changeValidation("UPDATE record_validation SET published_metadata_hash=NULL WHERE identifier_hash='abc'");
        when(store.getMetadata(any(), eq("original"))).thenReturn("<broken>");
        var result = service.export(request("processed", "jsonl", "collection", false, "none", null, null));
        assertEquals(4, result.errors());
        String errors = Files.readString(result.directory().resolve("errors.jsonl"));
        assertTrue(errors.contains("Selected metadata hash missing")); assertTrue(errors.contains("SAXParseException"));
    }
    @Test void originalExportWorksWithoutValidationAndEmptySourcesAreComplete() throws Exception {
        for (var source : sources) Files.delete(source.directory().resolve("validation/validation.db"));
        assertEquals(4, service.export(request("original", "jsonl", "collection", false, "none", null, null)).exported());
        for (var source : sources) try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + source.directory().resolve("catalog/catalog.db")); Statement statement = c.createStatement()) {
            statement.execute("DELETE FROM oai_record");
        }
        var empty = service.export(request("original", "jsonl", "collection", false, "none", null, null));
        assertEquals("COMPLETE", empty.status()); assertEquals(0, empty.attempted());
    }
    @Test void interruptionPreservesAFailedManifestAndStandaloneIndex() throws Exception {
        when(store.getMetadata(any(), eq("published"))).thenAnswer(call -> {
            Thread.currentThread().interrupt(); return XoaiExportMappingTest.XML;
        });
        try {
            var failure = assertThrows(MetadataExportService.ExportFailure.class,
                    () -> service.export(request("processed", "jsonl", "collection", false, "none", null, null)));
            assertTrue(Thread.currentThread().isInterrupted());
            Thread.interrupted();
            assertEquals("FAILED", ExportJson.JSON.readTree(failure.directory().resolve("manifest.json").toFile()).path("status").asText());
            assertTrue(Files.isRegularFile(failure.directory().resolve("records.sqlite")));
        } finally { Thread.interrupted(); }
    }
    @Test void networkPartitionsAreSeparatedAndIndexPositionsRecoverJsonAndXml() throws Exception {
        for (String format : List.of("json", "xml")) {
            var request = new ExportRequest(List.of("RCAAP", "IBICT"), false, "original", format,
                    format.equals("xml") ? null : mapping, "collection", "network", 2L, null, "gzip", false, temp.resolve("output"));
            var result = service.export(request);
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + result.directory().resolve("records.sqlite"));
                    Statement statement = c.createStatement(); ResultSet rows = statement.executeQuery("SELECT * FROM records ORDER BY record_key")) {
                while (rows.next()) {
                    String path = rows.getString("file"); assertTrue(path.startsWith(rows.getString("network") + "/"));
                    String text;
                    try (var input = new GZIPInputStream(Files.newInputStream(result.directory().resolve(path)))) {
                        text = new String(input.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                    }
                    int position = rows.getInt("position");
                    String key = format.equals("json") ? ExportJson.JSON.readTree(text).get(position).path("network").asText() + "_" + ExportJson.JSON.readTree(text).get(position).path("catalogId").asText()
                            : ((org.w3c.dom.Element) ExportXml.parse(text).getElementsByTagName("record").item(position)).getAttribute("recordKey");
                    assertEquals(rows.getString("record_key"), key);
                }
            }
        }
    }

}
