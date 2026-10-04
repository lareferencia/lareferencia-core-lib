package org.lareferencia.core.worker.harvesting;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.SnapshotStatus;
import org.lareferencia.core.metadata.*;
import org.lareferencia.core.repository.catalog.*;
import org.lareferencia.core.service.management.SnapshotLogService;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.test.util.ReflectionTestUtils;

class HarvestingLifecycleTest {
    HarvestingWorker worker;
    ISnapshotStore snapshots;
    OAIRecordCatalogRepository catalog;
    IHarvester harvester;
    Network network;
    @BeforeEach void setup() {
        network = new Network(); network.setName("fixture"); network.setAcronym("FIX");
        network.setOriginURL("https://fixture.invalid/oai"); network.setMetadataPrefix("oai_dc");
        network.setMetadataStoreSchema("oai_dc");
        snapshots = mock(ISnapshotStore.class); catalog = mock(OAIRecordCatalogRepository.class); harvester = mock(IHarvester.class);
        var metadata = new SnapshotMetadata(2L); metadata.setNetwork(network);
        when(snapshots.createSnapshot(network)).thenReturn(2L); when(snapshots.getSnapshotMetadata(2L)).thenReturn(metadata);
        when(snapshots.getSnapshotSize(2L)).thenReturn(10);
        when(catalog.countNotDeleted(2L)).thenReturn(10L);
        when(snapshots.getSnapshotStatus(2L)).thenReturn(SnapshotStatus.HARVESTING);
        when(harvester.identify(network.getOriginURL())).thenReturn(Map.of("granularity","YYYY-MM-DD"));
        worker = new HarvestingWorker(); worker.setRunningContext(new NetworkRunningContext(network)); worker.setHarvester(harvester);
        field("snapshotStore",snapshots); field("catalogRepository",catalog);
        field("metadataStore",mock(IMetadataStore.class)); field("snapshotLogService",mock(SnapshotLogService.class));
        field("harvestingConfigurationStore",mock(HarvestingConfigurationStore.class));
    }
    @Test void finalCountUsesActiveCatalogRatherThanReceivedRecords() {
        when(catalog.countNotDeleted(2L)).thenReturn(7L); worker.run();
        verify(snapshots).updateSnapshotSize(2L,7); verify(snapshots).finishHarvesting(2L);
    }
    @Test void fatalSetStopsRemainingSetsAndCannotBecomeSuccessful() {
        network.setSets(List.of("a","b"));
        doAnswer(c -> { worker.harvestingEventOccurred(event(HarvestingEventStatus.ERROR_FATAL)); return null; })
            .when(harvester).harvest(anyString(),anyString(),anyString(),anyString(),isNull(),isNull(),isNull(),anyInt());
        worker.run(); verify(snapshots,never()).finishHarvesting(2L);
        verify(harvester,times(1)).harvest(anyString(),anyString(),anyString(),anyString(),isNull(),isNull(),isNull(),anyInt());
        assertNotNull(worker.getExecutionFailure());
    }
    @Test void emptySetDoesNotPublishSnapshotBeforeRemainingSets() {
        network.setSets(List.of("a","b"));
        doAnswer(c -> { worker.harvestingEventOccurred(event(HarvestingEventStatus.NO_MATCHING_QUERY));
                verify(snapshots,never()).finishHarvesting(2L); return null; })
            .when(harvester).harvest(anyString(),anyString(),anyString(),anyString(),isNull(),isNull(),isNull(),anyInt());
        worker.run(); verify(harvester,times(2)).harvest(anyString(),anyString(),anyString(),anyString(),isNull(),isNull(),isNull(),anyInt());
        verify(snapshots,times(1)).finishHarvesting(2L);
    }
    @Test void cancellationNeverFinalizesPartialHarvestAsSuccessful() {
        doAnswer(c -> { worker.stop(); return null; }).when(harvester)
            .harvest(anyString(),isNull(),anyString(),anyString(),isNull(),isNull(),isNull(),anyInt());
        worker.run(); verify(snapshots).markHarvestingStopped(2L); verify(snapshots,never()).finishHarvesting(2L);
    }
    @Test void deletionKeepsOriginDatestampAndStorageFailureFailsSnapshot() {
        var stamp = LocalDateTime.of(2025,1,2,3,4,5);
        doThrow(new IllegalStateException("disk full")).when(catalog).upsertBatch(eq(2L),anyList());
        doAnswer(c -> {
            var event = event(HarvestingEventStatus.OK); event.getDeletedRecordsIdentifiers().add("deleted");
            event.getDeletedRecordsDatestamps().put("deleted",stamp); worker.harvestingEventOccurred(event); return null;
        }).when(harvester).harvest(anyString(),isNull(),anyString(),anyString(),isNull(),isNull(),isNull(),anyInt());
        worker.run();
        @SuppressWarnings("unchecked") ArgumentCaptor<List<OAIRecord>> batch = ArgumentCaptor.forClass(List.class);
        verify(catalog).upsertBatch(eq(2L),batch.capture()); assertEquals(stamp,batch.getValue().get(0).getDatestamp());
        verify(snapshots,never()).finishHarvesting(2L); assertNotNull(worker.getExecutionFailure());
    }
    @Test void incrementalUsesProviderDailyGranularityAndPrevalidationRemovesInheritedRecord() throws Exception {
        worker.setIncremental(true);
        var parent = new SnapshotMetadata(1L); parent.setNetwork(network);
        when(snapshots.findLastGoodKnownSnapshot(network)).thenReturn(1L);
        when(snapshots.getSnapshotMetadata(1L)).thenReturn(parent);
        when(snapshots.getSnapshotStartDatestamp(1L)).thenReturn(LocalDateTime.of(2026,1,2,3,4,5));
        var configuration = mock(HarvestingConfigurationStore.class); when(configuration.compatible(parent,network)).thenReturn(true);
        field("harvestingConfigurationStore",configuration);
        var model = new org.lareferencia.core.domain.Validator(); network.setPrevalidator(model);
        var validator = mock(org.lareferencia.core.worker.validation.IValidator.class);
        var rejected = new org.lareferencia.core.worker.validation.ValidatorResult(); rejected.setValid(false);
        when(validator.validate(any(),any())).thenReturn(rejected);
        var validation = mock(org.lareferencia.core.service.validation.ValidationService.class);
        when(validation.createValidatorFromModel(model)).thenReturn(validator); field("validationManager",validation);
        doAnswer(c -> {
            var event = event(HarvestingEventStatus.OK);
            var metadata = new OAIRecordMetadata("record","<metadata><title>Rejected</title></metadata>");
            metadata.setDatestamp(LocalDateTime.of(2026,1,3,0,0)); event.getRecords().add(metadata);
            worker.harvestingEventOccurred(event); return null;
        }).when(harvester).harvest(anyString(),isNull(),anyString(),anyString(),eq("2026-01-02"),isNull(),isNull(),anyInt());
        worker.run();
        @SuppressWarnings("unchecked") ArgumentCaptor<List<OAIRecord>> batch = ArgumentCaptor.forClass(List.class);
        verify(catalog).upsertBatch(eq(2L),batch.capture()); assertTrue(batch.getValue().get(0).isDeleted());
        verify(snapshots).setPreviousSnapshotId(2L,1L); verify(snapshots).finishHarvesting(2L);
    }
    private HarvestingEvent event(HarvestingEventStatus status) { var event = new HarvestingEvent(); event.setStatus(status); event.setMessage("fixture"); return event; }
    private void field(String name,Object value) { ReflectionTestUtils.setField(worker,name,value); }
}
