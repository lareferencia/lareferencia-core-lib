package org.lareferencia.core.metadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.repository.catalog.CatalogDatabaseManager;
import org.lareferencia.core.repository.catalog.OAIRecord;
import org.lareferencia.core.repository.catalog.OAIRecordCatalogRepository;
import org.lareferencia.core.repository.validation.RecordValidationRepository;
import org.lareferencia.core.repository.validation.ValidationDatabaseManager;
import org.springframework.test.util.ReflectionTestUtils;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MetadataOrphanAnalysisServiceTest {
    @TempDir Path directory;
    private IMetadataStore store;
    private CatalogDatabaseManager catalogs;
    private ValidationDatabaseManager validations;
    private OAIRecordCatalogRepository records;
    private RecordValidationRepository validated;
    private MetadataOrphanAnalysisService service;

    @BeforeEach
    void setup() {
        store = mock(IMetadataStore.class);
        catalogs = catalogManager();
        validations = validationManager();
        records = new OAIRecordCatalogRepository();
        ReflectionTestUtils.setField(records, "dbManager", catalogs);
        validated = new RecordValidationRepository();
        ReflectionTestUtils.setField(validated, "dbManager", validations);
        service = new MetadataOrphanAnalysisService(store, records, validated, catalogs, validations);
    }

    @Test
    void opensPreviouslyClosedDatabasesAndPreservesBothKindsOfReference() throws Exception {
        SnapshotMetadata snapshot = createSnapshot(1L);
        assertNull(catalogs.getDataSource(1L));
        assertNull(validations.getDataSource(1L));
        hashes("original-hash", "published-hash", "orphan-hash");
        when(store.deleteMetadata(snapshot, "orphan-hash")).thenReturn(true);
        var result = service.cleanup(List.of(snapshot), 100, 100);
        assertEquals(1, result.oaiReferences());
        assertEquals(1, result.validationReferences());
        assertEquals(1, result.deleted());
        verify(store).deleteMetadata(snapshot, "orphan-hash");
        verify(store, never()).deleteMetadata(snapshot, "original-hash");
        verify(store, never()).deleteMetadata(snapshot, "published-hash");
    }

    @Test
    void missingSecondProtectedCatalogAbortsBeforeScanningOrDeleting() throws Exception {
        SnapshotMetadata first = createSnapshot(1L);
        assertThrows(IOException.class, () -> service.cleanup(List.of(first, snapshot(2L)), 100, 100));
        verifyNoInteractions(store);
    }

    @Test
    void missingValidationDatabaseAbortsBeforeScanningOrDeleting() throws Exception {
        SnapshotMetadata snapshot = createSnapshot(1L);
        Files.delete(directory.resolve("TEST/snapshots/snapshot_1/validation/validation.db"));
        assertThrows(IOException.class, () -> service.cleanup(List.of(snapshot), 100, 100));
        verifyNoInteractions(store);
    }

    @Test
    void unreadableCatalogAbortsInsteadOfTreatingReferencesAsEmpty() throws Exception {
        SnapshotMetadata snapshot = createSnapshot(1L);
        Files.writeString(directory.resolve("TEST/snapshots/snapshot_1/catalog/catalog.db"), "not a SQLite database");
        assertThrows(RuntimeException.class, () -> service.cleanup(List.of(snapshot), 100, 100));
        verifyNoInteractions(store);
    }

    @Test
    void unreadableValidationDatabaseAbortsBeforeScanningOrDeleting() throws Exception {
        SnapshotMetadata snapshot = createSnapshot(1L);
        Files.writeString(directory.resolve("TEST/snapshots/snapshot_1/validation/validation.db"), "invalid SQLite");
        assertThrows(IOException.class, () -> service.cleanup(List.of(snapshot), 100, 100));
        verifyNoInteractions(store);
    }

    @Test
    void allProtectedSnapshotsContributeTheirOriginalAndPublishedHashes() throws Exception {
        SnapshotMetadata first = createSnapshot(1L);
        SnapshotMetadata second = createSnapshot(2L);
        CatalogDatabaseManager writer = catalogManager();
        writer.openSnapshotForRead(second);
        try (Connection connection = writer.getDataSource(2L).getConnection();
             PreparedStatement update = connection.prepareStatement(
                     "UPDATE oai_record SET original_metadata_hash = 'second-original'")) {
            update.executeUpdate();
        }
        ValidationDatabaseManager validationWriter = validationManager();
        validationWriter.openSnapshotForRead(second);
        try (Connection connection = validationWriter.getDataSource(2L).getConnection();
             PreparedStatement update = connection.prepareStatement(
                     "UPDATE record_validation SET published_metadata_hash = 'second-published'")) {
            update.executeUpdate();
        }
        hashes("original-hash", "published-hash", "second-original", "second-published");
        var result = service.cleanup(List.of(first, second), 100, 100);
        assertEquals(2, result.oaiReferences());
        assertEquals(2, result.validationReferences());
        assertEquals(0, result.deleted());
        verify(store, never()).deleteMetadata(any(), anyString());
    }

    @Test
    void readableCatalogsWithoutHashesAbortBeforeScanningOrDeleting() throws Exception {
        SnapshotMetadata snapshot = snapshot(1L);
        catalogs.initializeSnapshot(snapshot, null);
        validations.initializeSnapshot(snapshot, List.of());
        try (Connection connection = catalogs.getDataSource(1L).getConnection();
             PreparedStatement insert = connection.prepareStatement(
                     "INSERT INTO oai_record(id,identifier,datestamp) VALUES ('id','identifier','2026-01-01T00:00:00')")) {
            insert.executeUpdate();
        }

        IOException error = assertThrows(IOException.class, () -> service.cleanup(List.of(snapshot), 100, 100));
        assertTrue(error.getMessage().contains("No metadata references"));
        assertThrows(IOException.class, () -> service.analyze(List.of(snapshot)));
        verifyNoInteractions(store);
    }

    @Test
    void previewAlsoOpensTheProtectedCatalogs() throws Exception {
        SnapshotMetadata snapshot = createSnapshot(1L);
        hashes("original-hash", "published-hash");
        var result = service.analyze(List.of(snapshot));
        assertEquals(0, result.orphanCandidates());
        assertEquals(1, result.oaiReferences());
        assertEquals(1, result.validationReferences());
        verify(store, never()).deleteMetadata(any(), anyString());
    }

    @Test
    void failureWhileScanningTheStoreDoesNotDeleteCollectedCandidates() throws Exception {
        SnapshotMetadata snapshot = createSnapshot(1L);
        doAnswer(invocation -> {
            Consumer<String> consumer = invocation.getArgument(1);
            consumer.accept("orphan-hash");
            throw new MetadataRecordStoreException("read failure");
        }).when(store).forEachHash(eq(snapshot), any());
        assertThrows(MetadataRecordStoreException.class, () -> service.cleanup(List.of(snapshot), 100, 100));
        verify(store, never()).deleteMetadata(any(), anyString());
    }

    @Test
    void strictRepositoryReadsRejectUnopenedDatabases() {
        assertThrows(IllegalStateException.class, () -> records.streamAllRequired(snapshot(1L)));
        assertThrows(IOException.class, () -> validated.streamAllRequired(1L));
    }

    @Test
    void referenceStreamFailureAbortsBeforeMetadataDeletion() throws Exception {
        SnapshotMetadata snapshot = snapshot(1L);
        OAIRecordCatalogRepository broken = mock(OAIRecordCatalogRepository.class);
        int[] reads = {0};
        when(broken.streamAllRequired(snapshot)).thenAnswer(invocation -> {
            if (reads[0]++ == 0) return Stream.of(OAIRecord.create("identifier",
                    java.time.LocalDateTime.of(2026, 1, 1, 0, 0), "original-hash", false));
            return Stream.<OAIRecord>generate(() -> { throw new IllegalStateException("catalog interrupted"); });
        });
        service = new MetadataOrphanAnalysisService(store, broken, validated, catalogs, validations);
        createSnapshot(1L);
        assertThrows(IllegalStateException.class, () -> service.cleanup(List.of(snapshot), 100, 100));
        verifyNoInteractions(store);
    }

    private void hashes(String... values) throws Exception {
        doAnswer(invocation -> {
            Consumer<String> consumer = invocation.getArgument(1);
            for (String value : values) consumer.accept(value);
            return null;
        }).when(store).forEachHash(any(), any());
    }

    private SnapshotMetadata createSnapshot(long id) throws Exception {
        SnapshotMetadata snapshot = snapshot(id);
        CatalogDatabaseManager writer = catalogManager();
        writer.initializeSnapshot(snapshot, null);
        try (Connection connection = writer.getDataSource(id).getConnection();
             PreparedStatement insert = connection.prepareStatement(
                     "INSERT INTO oai_record(id,identifier,datestamp,original_metadata_hash) VALUES ('id','identifier','2026-01-01T00:00:00','original-hash')")) {
            insert.executeUpdate();
        }
        ValidationDatabaseManager validationWriter = validationManager();
        validationWriter.initializeSnapshot(snapshot, List.of());
        try (Connection connection = validationWriter.getDataSource(id).getConnection();
             PreparedStatement insert = connection.prepareStatement(
                     "INSERT INTO record_validation(identifier_hash,identifier,datestamp,is_valid,is_transformed,published_metadata_hash) VALUES ('id','identifier','2026-01-01T00:00:00',1,1,'published-hash')")) {
            insert.executeUpdate();
        }
        return snapshot;
    }

    private SnapshotMetadata snapshot(long id) {
        SnapshotMetadata snapshot = new SnapshotMetadata(id);
        Network network = new Network();
        network.setAcronym("TEST");
        snapshot.setNetwork(network);
        return snapshot;
    }

    private CatalogDatabaseManager catalogManager() {
        CatalogDatabaseManager manager = new CatalogDatabaseManager();
        ReflectionTestUtils.setField(manager, "basePath", directory.toString());
        ReflectionTestUtils.setField(manager, "walMode", false);
        return manager;
    }

    private ValidationDatabaseManager validationManager() {
        ValidationDatabaseManager manager = new ValidationDatabaseManager();
        ReflectionTestUtils.setField(manager, "basePath", directory.toString());
        return manager;
    }
}
