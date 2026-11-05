package org.lareferencia.core.export;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.lareferencia.core.metadata.IMetadataStore;
import org.lareferencia.core.util.PathUtils;
import org.springframework.stereotype.Service;

/** Streaming export orchestration; no mutation of snapshots or metadata. */
@Service
public class MetadataExportService {
    public record Result(Path directory, String status, long attempted, long exported, long filtered, long errors) {
        public boolean successful() { return status.equals("COMPLETE"); }
    }
    public static final class ExportFailure extends Exception {
        private final Path directory;
        ExportFailure(Path directory, Exception cause) { super("Export failed at " + directory + ": " + cause.getMessage(), cause); this.directory = directory; }
        public Path directory() { return directory; }
    }
    private final ExportSourceResolver resolver;
    private final IMetadataStore metadata;
    public MetadataExportService(ExportSourceResolver resolver, IMetadataStore metadata) { this.resolver = resolver; this.metadata = metadata; }
    public XoaiExportMapping validateMapping(Path path) throws Exception {
        JsonNode specification;
        try (InputStream input = Files.newInputStream(path)) {
            var reader = ExportJson.JSON.reader().with(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .with(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
            specification = reader.readTree(input);
        }
        if (specification == null) throw new IllegalArgumentException("Empty mapping");
        return new XoaiExportMapping(specification);
    }
    @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    public Result export(ExportRequest request) throws Exception {
        if (request.output() == null) throw new IllegalArgumentException("output is required");
        XoaiExportMapping mapping = request.mapping() == null ? null : validateMapping(request.mapping());
        var selection = resolver.resolve(request);
        String exportId = "exp-" + UUID.randomUUID();
        Path root = request.output().toAbsolutePath().normalize().resolve(exportId);
        Files.createDirectories(root.getParent()); Files.createDirectory(root);
        ObjectNode manifest = manifest(exportId, request, selection);
        long attempted = 0, exported = 0, filtered = 0, errors = 0;
        Exception failure = null;
        ExportBatchWriter batches = new ExportBatchWriter(root, request);
        Path errorFile = root.resolve("errors.jsonl.tmp"), indexFile = root.resolve("records.sqlite.tmp");
        try {
            ExportJson.write(root.resolve("manifest.json"), manifest);
            if (mapping != null) {
                ExportJson.write(root.resolve("mapping.json"), mapping.specification());
                manifest.put("mappingHash", XoaiExportMapping.canonicalHash(mapping.specification()));
            }
            try (ExportRecordIndex index = new ExportRecordIndex(indexFile);
                    BufferedWriter errorWriter = Files.newBufferedWriter(errorFile, StandardCharsets.UTF_8);
                    ExportBatchWriter writer = batches) {
                for (var source : selection.sources()) {
                    source.verification().verify();
                    try (ExportCatalogReader catalog = new ExportCatalogReader(source, needsValidation(request))) {
                        ExportCatalogReader.Row row;
                        while ((row = catalog.next()) != null) {
                            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Export interrupted");
                            attempted++;
                            ObjectNode envelope = envelope(exportId, source, row, request.variant());
                            Prepared prepared;
                            try {
                                validateRow(row, request);
                                if (request.validOnly() && !Boolean.TRUE.equals(row.valid())) {
                                    index.add(envelope, row, "FILTERED", null, null); filtered++; continue;
                                }
                                prepared = prepare(envelope, source, request, mapping);
                            } catch (RecordFailure e) {
                                errors++;
                                ObjectNode error = compact(envelope); error.remove("data");
                                error.put("error", e.getMessage());
                                errorWriter.write(ExportJson.JSON.writeValueAsString(error)); errorWriter.newLine();
                                index.add(envelope, row, "ERROR", null, e.getMessage()); continue;
                            }
                            String partition = request.partitionBy().equals("none") ? "data" : PathUtils.sanitizeNetworkAcronym(source.acronym());
                            var location = writer.write(partition, PathUtils.sanitizeNetworkAcronym(source.acronym()) + "_" + row.id(), prepared.bytes());
                            index.add(envelope, row, "EXPORTED", location, null); exported++;
                        }
                    }
                    source.verification().verify();
                }
            }
            for (var source : selection.sources()) source.verification().verify();
        } catch (Exception e) { failure = e; }
        // Preserve the index and diagnostics even when a package is failed.
        try {
            if (Files.exists(indexFile)) ExportJson.move(indexFile, root.resolve("records.sqlite"));
            if (Files.exists(errorFile)) ExportJson.move(errorFile, root.resolve("errors.jsonl"));
            String status = failure != null ? "FAILED" : errors > 0 ? "PARTIAL" : "COMPLETE";
            manifest.put("status", status).put("finishedAt", Instant.now().toString());
            manifest.putObject("counts").put("attempted", attempted).put("exported", exported).put("filtered", filtered).put("errors", errors);
            manifest.putObject("warnings").put("oversizedRecords", batches.oversizedRecords);
            if (failure != null) manifest.put("failure", failure.toString());
            ExportJson.writeManifest(root, manifest);
        } catch (Exception e) {
            if (failure == null) failure = e; else failure.addSuppressed(e);
            manifest.put("status", "FAILED").put("failure", failure.toString());
            try { ExportJson.write(root.resolve("manifest.json"), manifest); }
            catch (Exception suppressed) { failure.addSuppressed(suppressed); }
        }
        if (failure != null) { if (failure instanceof InterruptedException) Thread.currentThread().interrupt(); throw new ExportFailure(root, failure); }
        return new Result(root, errors > 0 ? "PARTIAL" : "COMPLETE", attempted, exported, filtered, errors);
    }
    /** Preview uses the same identity/projection contract but creates no files. Limit counts attempted rows. */
    @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
    public ObjectNode preview(ExportRequest request, int limit) throws Exception {
        if (limit < 1) throw new IllegalArgumentException("limit must be positive");
        XoaiExportMapping mapping = validateMapping(request.mapping());
        var selection = resolver.resolve(request);
        ObjectNode preview = ExportJson.object(); ArrayNode records = preview.putArray("records"), errors = preview.putArray("errors");
        preview.set("skippedSources", selection.skipped());
        int attempted = 0;
        for (var source : selection.sources()) {
            source.verification().verify();
            try (ExportCatalogReader catalog = new ExportCatalogReader(source, needsValidation(request))) {
                ExportCatalogReader.Row row;
                while (attempted < limit && (row = catalog.next()) != null) {
                    attempted++;
                    ObjectNode envelope = envelope("preview", source, row, request.variant());
                    try {
                        validateRow(row, request);
                        if (request.validOnly() && !Boolean.TRUE.equals(row.valid())) continue;
                        prepare(envelope, source, request, mapping); records.add(compact(envelope));
                    } catch (RecordFailure e) { errors.add(compact(envelope).put("error", e.getMessage())); }
                }
            }
            source.verification().verify();
            if (attempted >= limit) break;
        }
        preview.put("attempted", attempted); return preview;
    }
    private static boolean needsValidation(ExportRequest request) { return request.variant().equals("processed") || request.validOnly(); }
    private static void validateRow(ExportCatalogReader.Row row, ExportRequest request) throws RecordFailure {
        if (row.id() == null || !row.id().matches("[A-Za-z0-9_-]+") || row.identifier() == null)
            throw new RecordFailure("Missing or unsafe catalog identity");
        if (row.originalHash() == null || row.originalHash().isBlank()) throw new RecordFailure("Original metadata hash missing");
        if (row.validationIdentifier() != null && !row.identifier().equals(row.validationIdentifier())) throw new RecordFailure("Validation identifier does not match catalog");
        if (needsValidation(request) && (row.valid() == null || row.validationIdentifier() == null)) throw new RecordFailure("Validation record missing");
        if (Boolean.TRUE.equals(row.validationDeleted())) throw new RecordFailure("Active catalog record has deleted validation result");
    }
    private record Prepared(byte[] bytes) {}
    private Prepared prepare(ObjectNode envelope, ExportSourceResolver.Source source, ExportRequest request, XoaiExportMapping mapping) throws RecordFailure {
        try {
            String hash = envelope.path("input").path("metadataHash").asText(null);
            if (hash == null || hash.isBlank()) throw new IllegalArgumentException("Selected metadata hash missing");
            String xml = metadata.getMetadata(source.metadata(), hash);
            if (xml == null) throw new IllegalArgumentException("Metadata store returned null");
            if (mapping != null) {
                ObjectNode data = mapping.project(xml); envelope.set("data", data);
                return new Prepared(ExportJson.JSON.writeValueAsBytes(compact(envelope)));
            }
            var document = ExportXml.parse(xml);
            if (request.layout().equals("per-record")) return new Prepared(xml.getBytes(StandardCharsets.UTF_8));
            StringBuilder record = new StringBuilder("<record");
            record.append(" exportId=\"").append(ExportXml.attribute(envelope.path("exportId").asText())).append("\" recordKey=\"")
                    .append(ExportXml.attribute(envelope.path("recordKey").asText())).append("\">");
            for (String group : List.of("source", "input")) {
                record.append('<').append(group);
                var fields = envelope.path(group).fields();
                while (fields.hasNext()) { var item = fields.next(); if (!item.getValue().isNull()) record.append(' ').append(item.getKey()).append("=\"").append(ExportXml.attribute(item.getValue().asText())).append('"'); }
                record.append("/>");
            }
            record.append("<metadata>").append(ExportXml.fragment(document)).append("</metadata></record>");
            return new Prepared(record.toString().getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) { throw new RecordFailure(e.getClass().getSimpleName() + ": " + e.getMessage(), e); }
    }
    private static ObjectNode compact(ObjectNode envelope) {
        ObjectNode result = ExportJson.object();
        for (String key : List.of("network", "catalogId", "identifier"))
            result.set(key, envelope.path("source").path(key));
        result.set("metadataHash", envelope.path("input").path("metadataHash"));
        if (envelope.has("data")) result.set("data", envelope.get("data"));
        return result;
    }
    private static ObjectNode envelope(String exportId, ExportSourceResolver.Source source, ExportCatalogReader.Row row, String variant) {
        ObjectNode node = ExportJson.object(); node.put("exportId", exportId).put("recordKey", source.acronym() + "_" + row.id());
        node.putObject("source").put("network", source.acronym()).put("catalogId", row.id()).put("identifier", row.identifier())
                .put("snapshotRef", source.reference()).put("originalMetadataHash", row.originalHash());
        node.putObject("input").put("variant", variant).put("metadataHash", variant.equals("original") ? row.originalHash() : row.publishedHash());
        return node;
    }
    private static ObjectNode manifest(String exportId, ExportRequest request, ExportSourceResolver.Selection selection) {
        ObjectNode node = ExportJson.object();
        node.put("contractVersion", 2).put("exportId", exportId).put("canonicalizer", "export-json-v1")
                .put("status", "RUNNING").put("startedAt", Instant.now().toString()).put("xmlRetention", "references-only");
        ObjectNode options = node.putObject("options");
        options.put("variant", request.variant()).put("format", request.format()).put("layout", request.layout())
                .put("partitionBy", request.partitionBy()).put("maxRecords", request.recordLimit()).put("maxBytes", request.byteLimit())
                .put("compression", request.compression()).put("validOnly", request.validOnly()).put("allNetworks", request.allNetworks());
        options.set("networks", ExportJson.JSON.valueToTree(request.networks()));
        ArrayNode sources = node.putArray("sources"); selection.sources().forEach(source -> sources.add(source.provenance()));
        node.set("skippedSources", selection.skipped()); return node;
    }
    private static final class RecordFailure extends Exception {
        RecordFailure(String message) { super(message); }
        RecordFailure(String message, Throwable cause) { super(message, cause); }
    }
}
