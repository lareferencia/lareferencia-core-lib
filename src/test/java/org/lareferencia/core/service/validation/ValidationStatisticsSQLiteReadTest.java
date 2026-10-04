package org.lareferencia.core.service.validation;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.repository.validation.RecordValidationRepository;
import org.lareferencia.core.repository.validation.ValidationDatabaseManager;
import org.springframework.data.domain.PageRequest;
import org.springframework.test.util.ReflectionTestUtils;

class ValidationStatisticsSQLiteReadTest {
    @TempDir Path directory;

    @Test
    void readsRecordsAndRulesWhenDatabaseWasAlreadyOpenedByAnotherConsumer() throws Exception {
        assertRecordsReadable(true);
    }

    @Test
    void readsRecordsAndRulesWhenDatabaseNeedsToBeReopened() throws Exception {
        assertRecordsReadable(false);
    }

    private void assertRecordsReadable(boolean alreadyOpen) throws Exception {
        Network network = new Network();
        network.setAcronym("UY");
        SnapshotMetadata metadata = new SnapshotMetadata();
        metadata.setSnapshotId(4145L);
        metadata.setNetwork(network);
        metadata.registerRule(7L, "Required title", "", "ONE", true);
        ValidationDatabaseManager databases = new ValidationDatabaseManager();
        ReflectionTestUtils.setField(databases, "basePath", directory.toString());
        try {
            databases.initializeSnapshot(metadata, List.of(7L));
            try (var connection = databases.getDataSource(4145L).getConnection();
                    var statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO record_validation "
                        + "(identifier_hash, identifier, is_valid, is_transformed, deleted, rule_7) "
                        + "VALUES ('hash', 'oai:uy:1', 1, 0, 0, 1)");
            }
            if (!alreadyOpen) databases.closeDataSource(4145L);

            // A fresh repository has no cached rules even if the shared manager
            // already has a DataSource, as happens when indexing opens a snapshot.
            RecordValidationRepository records = new RecordValidationRepository();
            ReflectionTestUtils.setField(records, "dbManager", databases);
            ISnapshotStore snapshots = mock(ISnapshotStore.class);
            when(snapshots.getSnapshotMetadata(4145L)).thenReturn(metadata);
            ValidationStatisticsSQLiteService service = new ValidationStatisticsSQLiteService();
            ReflectionTestUtils.setField(service, "dbManager", databases);
            ReflectionTestUtils.setField(service, "recordRepository", records);
            ReflectionTestUtils.setField(service, "snapshotStore", snapshots);

            var result = service.queryValidationStatsObservationsBySnapshotID(
                    4145L, List.of(), PageRequest.of(0, 10));
            assertEquals(1L, result.getTotalElements());
            assertEquals(1, result.getContent().size());
            var observation = result.getContent().get(0);
            assertEquals("oai:uy:1", observation.getIdentifier());
            assertEquals(List.of("7"), observation.getValidRulesIDList());
            assertTrue(observation.getIsValid());
        } finally {
            databases.cleanup();
        }
    }
}
