package org.lareferencia.core.worker.validation;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.SnapshotStatus;
import org.lareferencia.core.metadata.*;
import org.lareferencia.core.repository.catalog.*;
import org.lareferencia.core.repository.validation.*;
import org.lareferencia.core.service.management.SnapshotLogService;
import org.lareferencia.core.service.validation.*;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.test.util.ReflectionTestUtils;

class IncrementalValidationLifecycleTest {
    @TempDir Path directory;
    Network network;
    SnapshotMetadata parent, child;
    ISnapshotStore snapshots;
    ValidationDatabaseManager db;
    RecordValidationRepository records;
    OAIRecordCatalogRepository catalog;
    ValidationStatisticsSQLiteService statistics;
    ValidationManifestStore manifests;
    ValidatorFingerprintService fingerprints = new ValidatorFingerprintService();
    Map<Long, SnapshotStatus> states = new HashMap<>();
    ValidationWorker worker;

    @BeforeEach void setup() throws Exception {
        network = new Network(); field(network,"id",10L); network.setName("fixture"); network.setAcronym("FIX");
        parent = metadata(1L); child = metadata(2L);
        snapshots = mock(ISnapshotStore.class);
        when(snapshots.getSnapshotMetadata(1L)).thenReturn(parent);
        when(snapshots.getSnapshotMetadata(2L)).thenReturn(child);
        when(snapshots.findLastHarvestingSnapshot(network)).thenReturn(2L);
        when(snapshots.getPreviousSnapshotId(2L)).thenReturn(1L);
        states.put(1L, SnapshotStatus.VALID); states.put(2L, SnapshotStatus.HARVESTING_FINISHED_VALID);
        when(snapshots.getSnapshotStatus(anyLong())).thenAnswer(c -> states.get(c.getArgument(0)));
        doAnswer(c -> { states.put(2L, SnapshotStatus.VALIDATING); return null; }).when(snapshots).startValidation(2L);
        doAnswer(c -> { states.put(2L, SnapshotStatus.VALID); return null; }).when(snapshots).finishValidation(2L);
        doAnswer(c -> { states.put(2L, SnapshotStatus.VALIDATION_FINISHED_ERROR); return null; }).when(snapshots).markValidationFailed(2L);
        doAnswer(c -> { states.put(2L, SnapshotStatus.VALIDATION_STOPPED); return null; }).when(snapshots).markValidationStopped(2L);
        db = new ValidationDatabaseManager(); field(db,"basePath",directory.toString());
        records = new RecordValidationRepository(); field(records,"dbManager",db); field(records,"batchSize",10);
        RuleOccurrenceRepository occurrences = new RuleOccurrenceRepository(); field(occurrences,"dbManager",db);
        statistics = spy(new ValidationStatisticsSQLiteService());
        field(statistics,"basePath",directory.toString()); field(statistics,"dbManager",db);
        field(statistics,"recordRepository",records); field(statistics,"occurrenceRepository",occurrences);
        field(statistics,"snapshotStore",snapshots);
        statistics.initializeValidationForSnapshot(parent);
        ValidatorResult valid = new ValidatorResult(); valid.setValid(true); valid.setTransformed(true); valid.setMetadataHash("published");
        statistics.addObservation(parent, OAIRecord.create("record", LocalDateTime.of(2026,1,1,0,0),"original",false), valid);
        statistics.finalizeValidationForSnapshot(1L);
        manifests = new ValidationManifestStore(); field(manifests,"basePath",directory.toString());
        var fingerprint = fingerprints.fingerprintNetwork(network); fingerprint.setScope("FULL"); manifests.write(parent,fingerprint);
        var catalogs = new CatalogDatabaseManager(); field(catalogs,"basePath",directory.toString()); field(catalogs,"snapshotStore",snapshots);
        catalog = new OAIRecordCatalogRepository(); field(catalog,"dbManager",catalogs); field(catalog,"batchSize",10);
        catalog.initializeSnapshot(parent,null);
        catalog.upsertRecord(1L,OAIRecord.create("record",LocalDateTime.of(2026,1,1,0,0),"original",false));
        catalog.finalizeSnapshot(1L); catalog.initializeSnapshot(child,1L);
        worker = new ValidationWorker(); worker.setIncremental(true); worker.setRunningContext(new NetworkRunningContext(network));
        field(worker,"snapshotStore",snapshots); field(worker,"catalogRepository",catalog);
        field(worker,"snapshotLogService",mock(SnapshotLogService.class)); field(worker,"validationStatisticsService",statistics);
        field(worker,"validationDatabaseManager",db); field(worker,"validationRecordRepository",records);
        field(worker,"validationManifestStore",manifests); field(worker,"validatorFingerprintService",fingerprints);
        field(worker,"validationManager",mock(ValidationService.class)); field(worker,"metadataStoreService",mock(IMetadataStore.class));
        clearInvocations(statistics,snapshots);
    }
    @Test void emptyDeltaPreservesResultsAndCompletesNewSnapshot() throws Exception {
        worker.run();
        assertEquals(SnapshotStatus.VALID,states.get(2L));
        verify(statistics,never()).deleteValidationStatsObservationsBySnapshotID(2L);
        verify(snapshots).updateValidationCounts(2L,1,1);
        db.openSnapshotForRead(child); assertEquals(1,records.countValid(2L)); assertEquals(1,records.countTransformed(2L));
        assertEquals("INCREMENTAL",manifests.read(child).orElseThrow().getScope());
    }
    @Test void deletionOnlyDeltaOpensCopyBeforeWritingTombstone() throws Exception {
        catalog.upsertRecord(2L,OAIRecord.create("record",LocalDateTime.of(2026,1,2,0,0),null,true));
        worker.run();
        assertEquals(SnapshotStatus.VALID,states.get(2L)); verify(snapshots).updateValidationCounts(2L,0,0);
        db.openSnapshotForRead(child); assertEquals(0,records.count(2L));
        db.openSnapshotForRead(parent); assertEquals(1,records.countValid(1L));
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void recordsEffectiveCapturePolicyEvenWhenThereAreNoValues(boolean detailed) throws Exception {
        worker.setIncremental(false);
        var options = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
        options.put("DETAILED_DIAGNOSE", detailed);
        worker.setRunningContext(new NetworkRunningContext(network, "validate", options));
        var model = new org.lareferencia.core.domain.Validator();
        network.setValidator(model);
        var validator = new ValidatorImpl();
        for (long id : new long[]{7, 8}) {
            child.registerRule(id, "Rule " + id, "", "ALL", false);
            var rule = mock(IValidatorRule.class);
            when(rule.getRuleId()).thenReturn(id);
            when(rule.isStoreOccurrences()).thenReturn(id == 7);
            validator.getRules().add(rule);
        }
        var validation = mock(ValidationService.class);
        when(validation.createValidatorFromModel(model)).thenReturn(validator);
        field(worker, "validationManager", validation);
        catalog.upsertRecord(2L, OAIRecord.create("record", LocalDateTime.of(2026,1,2,0,0), null, true));
        worker.run();

        var frozen = statistics.getSnapshotValidationStats(2L).getSnapshotMetadata();
        assertEquals(detailed, frozen.getRuleDefinition(7L).getOccurrenceCaptureEnabled());
        assertFalse(frozen.getRuleDefinition(8L).getOccurrenceCaptureEnabled());
    }

    @Test void requestedFullValidationDoesNotReuseParent() throws Exception {
        worker.setIncremental(false);
        // A full run must traverse the inherited record; storage failure must not publish VALID.
        worker.run();
        verify(statistics).deleteValidationStatsObservationsBySnapshotID(2L);
        verify(statistics,never()).initializeValidationForSnapshotReusingDatabase(child);
        assertEquals(SnapshotStatus.VALIDATION_FINISHED_ERROR,states.get(2L));
        assertTrue(manifests.read(child).isEmpty());
    }
    @Test void changedRecordReplacesResultAndPreservesUnchangedParent() throws Exception {
        catalog.upsertRecord(2L,OAIRecord.create("record",LocalDateTime.of(2026,1,2,0,0),"changed",false));
        IMetadataStore store = mock(IMetadataStore.class);
        when(store.getMetadata(child,"changed")).thenReturn("<metadata><title>Changed</title></metadata>");
        field(worker,"metadataStoreService",store);
        worker.run();
        assertEquals(SnapshotStatus.VALID,states.get(2L)); verify(snapshots).updateValidationCounts(2L,1,0);
        db.openSnapshotForRead(child); assertEquals(1,records.countValid(2L)); assertEquals(0,records.countTransformed(2L));
        db.openSnapshotForRead(parent); assertEquals(1,records.countTransformed(1L));
    }
    @Test void successfulFullRunRevalidatesInheritedRecordAndPublishesFullScope() throws Exception {
        worker.setIncremental(false);
        IMetadataStore store = mock(IMetadataStore.class);
        when(store.getMetadata(child,"original")).thenReturn("<metadata><title>Original</title></metadata>");
        field(worker,"metadataStoreService",store);
        worker.run(); verify(store).getMetadata(child,"original");
        assertEquals(SnapshotStatus.VALID,states.get(2L)); assertEquals("FULL",manifests.read(child).orElseThrow().getScope());
        verify(snapshots).updateValidationCounts(2L,1,0);
    }
    @Test void replacingEditionReprocessesInheritedCatalogRecord() throws Exception {
        var model = new org.lareferencia.core.domain.Transformer();
        var rule = new org.lareferencia.core.domain.TransformerRule();
        rule.setRunorder(1);
        rule.setJsonserialization("""
            {"@class":"org.lareferencia.core.worker.validation.transformer.ImportEnrichments",
             "filename":"rights.sqlite","mappings":[{"source":"dc.rights.license_l2","producer":"rights"}]}
            """);
        model.getRules().add(rule);
        network.setTransformer(model);
        var service = new ValidationService();
        field(service, "serializer", new RuleSerializer());
        field(worker, "validationManager", service);
        field(worker, "enrichmentBase", directory.toString());
        var store = mock(IMetadataStore.class);
        when(store.getMetadata(child, "original")).thenReturn("<metadata/>");
        when(store.storeAndReturnHash(eq(child), anyString())).thenReturn("enriched");
        field(worker, "metadataStoreService", store);
        Path folder = java.nio.file.Files.createDirectories(directory.resolve("FIX/enrichments"));
        Path target = folder.resolve("rights.sqlite");
        for (String facet : List.of("FIRST", "SECOND")) {
            Path staged = folder.resolve(facet);
            try (var db = java.sql.DriverManager.getConnection("jdbc:sqlite:" + staged);
                 var sql = db.createStatement();
                 var stream = getClass().getResourceAsStream("/enrichment/edition.sql")) {
                String fixture = new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
                        .replace("'PY'", "'FIX'").replace("'hash'", "'original'")
                        .replace("'stored-id'", "'" + OAIRecord.generateIdFromIdentifier("record") + "'")
                        .replace("\"BY\"", "\"" + facet + "\"");
                for (String command : fixture.split(";")) if (!command.isBlank()) sql.execute(command);
            }
            java.nio.file.Files.move(staged, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
            worker.run();
            assertEquals(SnapshotStatus.VALID, states.get(2L));
            verify(store).storeAndReturnHash(eq(child), contains(facet));
            var manifest = manifests.read(child).orElseThrow();
            assertEquals("FULL", manifest.getScope());
            manifests.write(parent, manifest);
        }
    }

    static java.util.stream.Stream<org.junit.jupiter.params.provider.Arguments> publicationCases() {
        return java.util.stream.Stream.of("deduplicate", "conditionalNoOp", "priority", "priorityNoOp",
                "rename", "identifierNoOp").flatMap(operation -> java.util.stream.Stream.of(false, true)
                .map(secondary -> org.junit.jupiter.params.provider.Arguments.of(operation, secondary)));
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.MethodSource("publicationCases")
    void publishedBlobMatchesPersistedRuleDecision(String operation, boolean secondary) throws Exception {
        var service = new ValidationService();
        var serializer = new RuleSerializer();
        field(service, "serializer", serializer);
        field(worker, "validationManager", service);
        var store = new MetadataStoreFSImpl();
        field(store, "basePath", directory.toString());
        field(store, "hashing", new org.lareferencia.core.util.hashing.MD5Hashing());
        field(worker, "metadataStoreService", store);
        var xml = new OAIRecordMetadata("record");
        xml.addFieldOcurrence("dc.type", operation.equals("priority") ? "paper" : "article");
        if (operation.equals("deduplicate")) xml.addFieldOcurrence("dc.type", "article");
        String original = store.storeAndReturnHash(child, xml.toString());
        var harvested = LocalDateTime.of(2020, 1, 1, 0, 0);
        catalog.upsertRecord(2L, OAIRecord.create("record", harvested, original, false));

        ITransformerRule transformation;
        switch (operation) {
            case "deduplicate", "conditionalNoOp" -> {
                var cleanup = new org.lareferencia.core.worker.validation.transformer.FieldContentConditionalAddOccrRule();
                cleanup.setFieldName("dc.type");
                cleanup.setValueToAdd("book");
                cleanup.setConditionalExpression("dc.type=='report'");
                cleanup.setRemoveDuplicatedOccurrences(true);
                transformation = cleanup;
            }
            case "priority", "priorityNoOp" -> {
                var priority = new org.lareferencia.core.worker.validation.transformer.FieldContentPriorityTranslateRule();
                priority.setTestFieldName("dc.type");
                priority.setWriteFieldName("dc.type");
                priority.setTranslationArray(List.of(new Translation("paper", "article"), new Translation("book", "libro")));
                transformation = priority;
            }
            case "rename", "identifierNoOp" -> {
                var rename = new org.lareferencia.core.worker.validation.transformer.IdentifierRegexRule();
                rename.setRegexSearch(operation.equals("rename") ? "^record$" : "^absent$");
                rename.setRegexReplace("new-id");
                transformation = rename;
            }
            default -> throw new AssertionError(operation);
        }
        var transform = new org.lareferencia.core.domain.Transformer();
        var trule = new org.lareferencia.core.domain.TransformerRule();
        trule.setRunorder(1);
        trule.setJsonserialization(serializer.serializeTransformerToJsonString(transformation));
        transform.getRules().add(trule);
        if (secondary) {
            network.setTransformer(new org.lareferencia.core.domain.Transformer());
            network.setSecondaryTransformer(transform);
        } else network.setTransformer(transform);

        var model = new org.lareferencia.core.domain.Validator();
        var vrule = new org.lareferencia.core.domain.ValidatorRule();
        vrule.setId(7L);
        vrule.setMandatory(true);
        vrule.setQuantifier(QuantifierValues.ONE_ONLY);
        var regex = new org.lareferencia.core.worker.validation.validator.RegexFieldContentValidatorRule();
        regex.setFieldname("dc.type");
        regex.setRegexString("article");
        vrule.setJsonserialization(serializer.serializeValidatorToJsonString(regex));
        model.getRules().add(vrule);
        network.setValidator(model);
        child.registerRule(7L, "Type", "", "ONE_ONLY", true);

        worker.run();
        assertEquals(SnapshotStatus.VALID, states.get(2L));
        db.openSnapshotForRead(child);
        records.registerRuleIds(2L, List.of(7L));
        String expectedIdentifier = operation.equals("rename") ? "new-id" : "record";
        var row = records.getByIdentifier(2L, expectedIdentifier);
        assertNotNull(row);
        boolean changed = !operation.endsWith("NoOp");
        assertEquals(changed, row.isTransformed());
        if (changed) assertTrue(row.getDatestamp().isAfter(harvested));
        else {
            assertEquals(harvested, row.getDatestamp());
            assertEquals(original, row.getPublishedMetadataHash());
        }
        verify(snapshots).updateValidationCounts(2L, 1, changed ? 1 : 0);
        var published = new OAIRecordMetadata(row.getIdentifier(), store.getMetadata(child, row.getPublishedMetadataHash()));
        assertEquals(List.of("article"), published.getFieldOcurrences("dc.type"));
        var replay = service.createValidatorFromModel(model).validate(published, new ValidatorResult());
        assertEquals(replay.isValid(), row.isValid());
        assertEquals(replay.getRulesResults().get(0).getValid(), row.getRuleResult(7L));
        assertEquals("FULL", manifests.read(child).orElseThrow().getScope());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {true, false})
    void inheritedUrlIsCheckedOnlyOnFullRun(boolean incremental) throws Exception {
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var http = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        http.createContext("/", exchange -> {
            calls.incrementAndGet();
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        http.start();
        try {
            var url = new org.lareferencia.core.worker.validation.validator.URLExistFieldValidatorRule();
            url.setFieldname("dc.identifier");
            var rule = new org.lareferencia.core.domain.ValidatorRule();
            rule.setId(1L);
            rule.setMandatory(true);
            rule.setQuantifier(QuantifierValues.ONE_ONLY);
            rule.setJsonserialization(new RuleSerializer().serializeValidatorToJsonString(url));
            var validator = new org.lareferencia.core.domain.Validator();
            validator.getRules().add(rule);
            network.setValidator(validator);
            child.registerRule(1L, "URL", "", "ONE_ONLY", true);
            parent.registerRule(1L, "URL", "", "ONE_ONLY", true);
            statistics.deleteValidationStatsObservationsBySnapshotID(1L);
            statistics.initializeValidationForSnapshot(parent);
            var accepted = new ValidatorResult();
            accepted.setValid(true);
            accepted.setMetadataHash("original");
            url.setRuleId(1L);
            accepted.getRulesResults().add(new ValidatorRuleResult(url, true, List.of()));
            statistics.addObservation(parent, OAIRecord.create("record", LocalDateTime.now(), "original", false), accepted);
            statistics.finalizeValidationForSnapshot(1L);
            var stamp = fingerprints.fingerprintNetwork(network);
            stamp.setScope("FULL");
            manifests.write(parent, stamp);
            var service = new ValidationService();
            field(service, "serializer", new RuleSerializer());
            field(worker, "validationManager", service);
            var xml = new OAIRecordMetadata("record");
            xml.addFieldOcurrence("dc.identifier", "http://127.0.0.1:" + http.getAddress().getPort() + "/");
            var store = mock(IMetadataStore.class);
            when(store.getMetadata(child,"original")).thenReturn(xml.toString());
            field(worker, "metadataStoreService", store);
            worker.setIncremental(incremental);
            worker.run();
            assertEquals(SnapshotStatus.VALID, states.get(2L));
            assertEquals(incremental ? 0 : 1, calls.get());
        } finally {
            http.stop(0);
        }
    }

    @Test void runUsesOneYearForFingerprintAndForDirectAndEmbeddedRules() throws Exception {
        var start = java.time.Clock.fixed(java.time.Instant.parse("1999-12-31T23:59:59Z"), java.time.ZoneOffset.UTC);
        field(worker, "clock", start);
        var service = new ValidationService();
        var serializer = new RuleSerializer();
        field(service, "serializer", serializer);
        field(worker, "validationManager", service);
        var year = new org.lareferencia.core.worker.validation.validator.DynamicYearRangeFieldContentValidatorRule();
        year.setFieldname("date");
        var model = new org.lareferencia.core.domain.Validator();
        var rule = new org.lareferencia.core.domain.ValidatorRule();
        rule.setId(3L);
        rule.setMandatory(true);
        rule.setQuantifier(QuantifierValues.ONE_ONLY);
        rule.setJsonserialization(serializer.serializeValidatorToJsonString(year));
        model.getRules().add(rule);
        network.setValidator(model);
        child.registerRule(3L, "Year", "", "ONE_ONLY", true);
        var normalize = new org.lareferencia.core.worker.validation.transformer.FieldContentNormalizeRule();
        normalize.setFieldName("date");
        normalize.setValidationRule(year);
        normalize.setRemoveInvalidOccurrences(true);
        var transformer = new org.lareferencia.core.domain.Transformer();
        var trule = new org.lareferencia.core.domain.TransformerRule();
        trule.setRunorder(1);
        trule.setJsonserialization(serializer.serializeTransformerToJsonString(normalize));
        transformer.getRules().add(trule);
        network.setSecondaryTransformer(transformer);
        var xml = new OAIRecordMetadata("record");
        xml.addFieldOcurrence("date", "1999");
        var store = mock(IMetadataStore.class);
        when(store.getMetadata(child, "original")).thenAnswer(call -> {
            field(worker, "clock", java.time.Clock.offset(start, java.time.Duration.ofSeconds(2)));
            return xml.toString();
        });
        field(worker, "metadataStoreService", store);
        worker.run();
        assertEquals(SnapshotStatus.VALID, states.get(2L));
        verify(snapshots).updateValidationCounts(2L, 1, 0);
        assertEquals(fingerprints.fingerprintNetwork(network, false, 1999).getHash(),
                manifests.read(child).orElseThrow().getHash());
    }

    @Test void previousEngineManifestTriggersFullValidation() throws Exception {
        var old = new ValidatorFingerprint(2, "SHA-256", "validation-pipeline-v2", "old-engine");
        old.setScope("FULL");
        manifests.write(parent, old);
        var store = mock(IMetadataStore.class);
        when(store.getMetadata(child, "original")).thenReturn("<metadata/>");
        field(worker, "metadataStoreService", store);
        worker.run();
        assertEquals("FULL", manifests.read(child).orElseThrow().getScope());
        assertEquals("old-engine", manifests.read(parent).orElseThrow().getHash());
        verify(store).getMetadata(child, "original");
    }

    @Test void incompleteParentCannotBeReused() {
        states.put(1L,SnapshotStatus.VALIDATION_STOPPED);
        worker.run(); verify(statistics,never()).initializeValidationForSnapshotReusingDatabase(child);
        assertEquals(SnapshotStatus.VALIDATION_FINISHED_ERROR,states.get(2L));
    }
    @Test void finalizationFailureNeverPublishesManifestOrValidState() {
        doThrow(new IllegalStateException("disk full")).when(statistics).finalizeValidationForSnapshot(2L);
        assertThrows(IllegalStateException.class,worker::run);
        assertEquals(SnapshotStatus.VALIDATION_FINISHED_ERROR,states.get(2L));
        verify(snapshots,never()).finishValidation(2L); assertTrue(manifests.read(child).isEmpty());
    }
    @Test void cancellationAfterPreparationDoesNotPublishCompletedParent() {
        doAnswer(c -> { worker.stop(); return null; }).when(statistics).setDetailedDiagnose(false);
        worker.run(); assertEquals(SnapshotStatus.VALIDATION_STOPPED,states.get(2L));
        verify(snapshots,never()).finishValidation(2L); assertTrue(manifests.read(child).isEmpty());
    }
    private SnapshotMetadata metadata(long id) {
        var metadata = new SnapshotMetadata(id); metadata.setNetwork(network); metadata.setSize(1); return metadata;
    }
    private static void field(Object object,String name,Object value) { ReflectionTestUtils.setField(object,name,value); }
}
