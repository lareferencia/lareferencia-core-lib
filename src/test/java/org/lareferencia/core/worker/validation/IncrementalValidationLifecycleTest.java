package org.lareferencia.core.worker.validation;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Map;
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
