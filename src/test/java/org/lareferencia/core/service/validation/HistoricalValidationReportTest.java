package org.lareferencia.core.service.validation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.NetworkSnapshot;
import org.lareferencia.core.domain.SnapshotStatus;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.repository.validation.RecordValidationRepository;
import org.lareferencia.core.repository.validation.RuleOccurrenceRepository;
import org.lareferencia.core.repository.validation.ValidationDatabaseManager;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class HistoricalValidationReportTest {
    @TempDir Path directory;
    private static final long ID = 18L;
    private final ValidationDatabaseManager databases = new ValidationDatabaseManager();
    private final ValidationStatisticsSQLiteService statistics = new ValidationStatisticsSQLiteService();
    private final NetworkSnapshot snapshot = mock(NetworkSnapshot.class);
    private final ISnapshotStore store = mock(ISnapshotStore.class);

    @BeforeEach
    void finalizeSnapshotThenReplaceValidator() throws Exception {
        Network network = new Network();
        network.setName("Original source");
        network.setAcronym("FIX");
        SnapshotMetadata archived = new SnapshotMetadata(ID);
        archived.setNetwork(network);
        archived.registerRule(7L, "Original title", "historical description", "ONE_OR_MORE", true);
        archived.getRuleDefinition(7L).setOccurrenceCaptureEnabled(false);
        archived.registerRule(8L, "Original language", "", "ALL", false);
        archived.getRuleDefinition(8L).setOccurrenceCaptureEnabled(true);

        var records = new RecordValidationRepository();
        var occurrences = new RuleOccurrenceRepository();
        ReflectionTestUtils.setField(databases, "basePath", directory.toString());
        ReflectionTestUtils.setField(records, "dbManager", databases);
        ReflectionTestUtils.setField(occurrences, "dbManager", databases);
        ReflectionTestUtils.setField(statistics, "dbManager", databases);
        ReflectionTestUtils.setField(statistics, "recordRepository", records);
        ReflectionTestUtils.setField(statistics, "occurrenceRepository", occurrences);
        ReflectionTestUtils.setField(statistics, "snapshotStore", store);
        ReflectionTestUtils.setField(statistics, "basePath", directory.toString());

        statistics.initializeValidationForSnapshot(archived);
        try (var conn = databases.getDataSource(ID).getConnection(); var sql = conn.createStatement()) {
            sql.executeUpdate("""
                    INSERT INTO record_validation(identifier_hash,identifier,is_valid,is_transformed,rule_7,rule_8)
                    VALUES ('a','oai:a',1,1,1,0), ('b','oai:b',0,0,0,1)
                    """);
            sql.executeUpdate("INSERT INTO rule_occurrences VALUES (1,'a',8,0,'unrecognized')");
        }
        statistics.finalizeValidationForSnapshot(ID);

        network.setName("Renamed source");
        SnapshotMetadata live = new SnapshotMetadata(ID);
        live.setNetwork(network);
        live.registerRule(7L, "Renamed title", "new description", "ZERO_ONLY", false);
        live.registerRule(99L, "Added rule", "", "ALL", true);
        when(snapshot.getId()).thenReturn(ID);
        when(store.getSnapshotMetadata(ID)).thenReturn(live);
        when(store.getSnapshotStatus(ID)).thenReturn(SnapshotStatus.VALID);
    }

    @AfterEach void close() { databases.cleanup(); }

    @Test
    void pagingPreservesGroupTotalsEvenBeyondTheLastValue() throws Exception {
        var selected = List.of("valid_rules@@7");
        var first = statistics.frequencyPage(ID, 8, false, selected, PageRequest.of(0, 1));
        assertEquals(1L, first.totalOccurrences());
        assertEquals(1L, first.totalElements());
        assertEquals("unrecognized", first.content().get(0).getValue());
        var beyond = statistics.frequencyPage(ID, 8, false, selected, PageRequest.of(1, 1));
        assertTrue(beyond.content().isEmpty());
        assertEquals(1L, beyond.totalOccurrences());
        assertEquals(1L, beyond.totalElements());
        assertEquals(0L, statistics.frequencyPage(ID, 8, true, selected, PageRequest.of(0, 1)).totalOccurrences());
        assertEquals(0L, statistics.frequencyPage(ID, 8, false, List.of("invalid_rules@@7"), PageRequest.of(0, 1)).totalElements());
    }

    @Test
    void summaryRecordsAndOccurrencesUseTheCompletedSnapshot() throws Exception {
        var global = statistics.queryValidatorRulesStatsBySnapshot(snapshot, List.of());
        var filters = List.of("valid_rules@@7");
        var selected = statistics.queryValidatorRulesStatsBySnapshot(snapshot, filters);
        assertEquals(2, global.getSize());
        assertEquals(1, selected.getSize());
        for (var report : List.of(global, selected)) {
            assertEquals(Set.of("7", "8"), report.getRulesByID().keySet());
            var rule = report.getRulesByID().get("7");
            assertEquals("Original title", rule.getName());
            assertEquals("historical description", rule.getDescription());
            assertEquals("ONE_OR_MORE", rule.getQuantifier().name());
            assertTrue(rule.getMandatory());
            assertFalse(rule.getOccurrenceCaptureEnabled());
            assertNull(rule.getStoredOccurrencesAvailable());
            assertTrue(report.getRulesByID().get("8").getOccurrenceCaptureEnabled());
        }
        assertEquals(1, global.getRulesByID().get("7").getInvalidCount());
        assertEquals(0, selected.getRulesByID().get("7").getInvalidCount());
        var page = statistics.queryValidationStatsObservationsBySnapshotID(ID, filters, PageRequest.of(0, 10));
        var record = page.getContent().get(0);
        assertEquals(List.of("7"), record.getValidRulesIDList());
        assertEquals(List.of("8"), record.getInvalidRulesIDList());
        assertEquals("Original source", record.getRepositoryName());
        assertNull(record.getValidationDate());
        assertEquals(List.of("unrecognized"), record.getInvalidOccurrencesByRuleID().get("8"));
        var values = statistics.queryValidRuleOccurrencesCountBySnapshotID(ID, 8L, filters);
        assertEquals(1, values.getInvalidRuleOccrs().get(0).getCount());
    }

    @Test
    void historicalIncrementalChildKeepsInheritedRulesAndExcludesDeletions() throws Exception {
        var parent = statistics.getSnapshotValidationStats(ID).getSnapshotMetadata();
        long childId = ID + 1;
        var child = new SnapshotMetadata(childId, 1, 1, 1, parent.getCreatedAt(), parent.getNetwork(),
                parent.getRuleDefinitions());
        databases.copySnapshotDatabase(parent, child);
        statistics.initializeValidationForSnapshotReusingDatabase(child);
        var records = new RecordValidationRepository();
        ReflectionTestUtils.setField(records, "dbManager", databases);
        records.markDeletedRecord(childId, "b", "oai:b", null);
        statistics.finalizeValidationForSnapshot(childId);

        var locator = new SnapshotMetadata(childId);
        Network network = new Network();
        network.setAcronym("FIX");
        locator.setNetwork(network);
        when(store.getSnapshotMetadata(childId)).thenReturn(locator);
        var childSnapshot = mock(NetworkSnapshot.class);
        when(childSnapshot.getId()).thenReturn(childId);
        for (var filters : List.of(List.<String>of(), List.of("invalid_rules@@8"))) {
            var report = statistics.queryValidatorRulesStatsBySnapshot(childSnapshot, filters);
            assertEquals(1, report.getSize());
            assertEquals("Original language", report.getRulesByID().get("8").getName());
            assertEquals(1, report.getRulesByID().get("8").getInvalidCount());
            assertEquals(0, report.getRulesByID().get("8").getValidCount());
        }
        assertEquals(2, statistics.getSnapshotValidationStats(ID).getTotalRecords());
    }

    @Test
    void partialArchivedSummaryRetainsCountsWithoutTheDetailDatabase() throws Exception {
        Path folder = directory.resolve("FIX/snapshots/snapshot_18/validation");
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var json = mapper.readTree(folder.resolve("validation-stats.json").toFile());
        ((com.fasterxml.jackson.databind.node.ObjectNode) json.at("/snapshotMetadata/ruleDefinitions")).remove("7");
        mapper.writeValue(folder.resolve("validation-stats.json").toFile(), json);
        Files.delete(folder.resolve("validation.db"));

        var report = statistics.queryValidatorRulesStatsBySnapshot(snapshot, List.of());
        assertEquals(2, report.getSize());
        assertEquals(1, report.getRulesByID().get("7").getValidCount());
        assertNull(report.getRulesByID().get("7").getMandatory());
        assertNull(report.getRulesByID().get("7").getStoredOccurrencesAvailable());
        assertEquals("Original language", report.getRulesByID().get("8").getName());
    }

    @Test
    void archivedSummaryResolvesAvailabilityWithoutChangingUnknownCaptureFlags() throws Exception {
        Path file = directory.resolve("FIX/snapshots/snapshot_18/validation/validation-stats.json");
        var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        var json = mapper.readTree(file.toFile());
        for (var definition : json.at("/snapshotMetadata/ruleDefinitions")) {
            ((com.fasterxml.jackson.databind.node.ObjectNode) definition).remove("occurrenceCaptureEnabled");
        }
        mapper.writeValue(file.toFile(), json);

        var report = statistics.queryValidatorRulesStatsBySnapshot(snapshot, List.of());
        assertNull(report.getRulesByID().get("7").getOccurrenceCaptureEnabled());
        assertFalse(report.getRulesByID().get("7").getStoredOccurrencesAvailable());
        assertTrue(report.getRulesByID().get("8").getStoredOccurrencesAvailable());
        assertFalse(databases.hasActiveDataSource(ID));
    }

    @Test
    void oldSnapshotWithOnlyTheDatabaseRetainsRuleIdsAndCounts() throws Exception {
        Files.delete(directory.resolve("FIX/snapshots/snapshot_18/validation/validation-stats.json"));
        var global = statistics.queryValidatorRulesStatsBySnapshot(snapshot, List.of());
        var subset = statistics.queryValidatorRulesStatsBySnapshot(snapshot, List.of("valid_rules@@8"));
        assertEquals(2, global.getSize());
        assertEquals(1, subset.getSize());
        for (var report : List.of(global, subset)) {
            assertEquals(Set.of("7", "8"), report.getRulesByID().keySet());
            var rule = report.getRulesByID().get("7");
            assertEquals("7", rule.getName());
            assertNull(rule.getMandatory());
            assertNull(rule.getQuantifier());
            assertNull(rule.getOccurrenceCaptureEnabled());
            assertFalse(rule.getStoredOccurrencesAvailable());
            assertTrue(report.getRulesByID().get("8").getStoredOccurrencesAvailable());
            assertEquals(1, rule.getInvalidCount());
        }
    }
}
