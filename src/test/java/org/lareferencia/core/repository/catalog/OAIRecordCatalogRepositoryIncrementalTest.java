package org.lareferencia.core.repository.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.PathUtils;
import org.springframework.test.util.ReflectionTestUtils;

class OAIRecordCatalogRepositoryIncrementalTest {

    @TempDir
    Path tempDir;

    private CatalogDatabaseManager databaseManager;
    private OAIRecordCatalogRepository repository;
    private ISnapshotStore snapshotStore;
    private final Map<Long, SnapshotMetadata> snapshots = new HashMap<>();

    @BeforeEach
    void setUp() {
        snapshotStore = (ISnapshotStore) Proxy.newProxyInstance(
                ISnapshotStore.class.getClassLoader(),
                new Class<?>[] { ISnapshotStore.class },
                (proxy, method, args) -> {
                    if ("getSnapshotMetadata".equals(method.getName())) {
                        return snapshots.get(args[0]);
                    }
                    if (method.getReturnType() == boolean.class) {
                        return false;
                    }
                    if (method.getReturnType().isPrimitive()) {
                        return 0;
                    }
                    return null;
                });
        databaseManager = new CatalogDatabaseManager();
        ReflectionTestUtils.setField(databaseManager, "basePath", tempDir.toString());
        ReflectionTestUtils.setField(databaseManager, "walMode", false);
        ReflectionTestUtils.setField(databaseManager, "snapshotStore", snapshotStore);

        repository = new OAIRecordCatalogRepository();
        ReflectionTestUtils.setField(repository, "dbManager", databaseManager);
        ReflectionTestUtils.setField(repository, "batchSize", 2);
    }

    @Test
    void marksOnlyNewUpdatedAndDeletedRecordsAfterIncrementalCopy() throws Exception {
        SnapshotMetadata first = snapshot(1L);
        repository.initializeSnapshot(first, null);
        repository.upsertBatch(1L, List.of(
                record("a", "hash-a", false),
                record("b", "hash-b", false)));
        repository.finalizeSnapshot(1L);

        snapshots.put(1L, first);
        SnapshotMetadata second = snapshot(2L);
        repository.initializeSnapshot(second, 1L);
        assertEquals(0, repository.countChanged(2L));

        repository.upsertBatch(2L, List.of(
                OAIRecord.create("a", LocalDateTime.of(2026, 1, 2, 0, 0), "hash-a", false),
                record("b", "hash-b-v2", false),
                record("c", "hash-c", false),
                record("d", null, true)));

        Map<String, OAIRecord> records = repository.streamChanged(second)
                .collect(Collectors.toMap(OAIRecord::getIdentifier, record -> record));

        assertEquals("U", records.get("b").getChangeType());
        assertEquals("N", records.get("c").getChangeType());
        assertEquals("D", records.get("d").getChangeType());
        assertNull(records.get("a"));
        assertEquals(3, repository.countChanged(2L));
        repository.closeSnapshot(2L);
    }

    @Test
    void addsChangeTypeWhenCopyingLegacyCatalog() throws Exception {
        SnapshotMetadata first = snapshot(1L);
        Path legacyDb = Path.of(PathUtils.getSnapshotPath(tempDir.toString(), first), "catalog", "catalog.db");
        Files.createDirectories(legacyDb.getParent());

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + legacyDb);
                Statement statement = connection.createStatement()) {
            statement.execute("""
                    CREATE TABLE oai_record (
                        id TEXT PRIMARY KEY,
                        identifier TEXT NOT NULL UNIQUE,
                        datestamp TEXT NOT NULL,
                        original_metadata_hash TEXT,
                        deleted INTEGER NOT NULL DEFAULT 0
                    )
                    """);
            statement.execute("""
                    INSERT INTO oai_record (id, identifier, datestamp, original_metadata_hash, deleted)
                    VALUES ('228c70bfc5589c58c044e03fff0e17eb', 'legacy', '2026-01-01T00:00:00', 'legacy-hash', 0)
                    """);
        }

        snapshots.put(1L, first);
        SnapshotMetadata second = snapshot(2L);
        repository.initializeSnapshot(second, 1L);
        repository.upsertRecord(2L, record("legacy", "legacy-hash", false));

        assertEquals(0, repository.countChanged(2L));
        repository.closeSnapshot(2L);
    }

    private SnapshotMetadata snapshot(long id) {
        SnapshotMetadata metadata = new SnapshotMetadata(id);
        Network network = new Network();
        network.setAcronym("TEST");
        metadata.setNetwork(network);
        return metadata;
    }

    private OAIRecord record(String identifier, String hash, boolean deleted) {
        return OAIRecord.create(identifier, LocalDateTime.of(2026, 1, 1, 0, 0), hash, deleted);
    }
}
