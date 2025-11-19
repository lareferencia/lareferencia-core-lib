/*
 * JUnit test for ValidationRecordManager flush behaviour using flushThreshold
 */
package org.lareferencia.core.repository.parquet;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.metadata.SnapshotMetadata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class ValidationRecordManagerTest {

    private Path tempDir;

    @AfterEach
    public void cleanup() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
    }

    @Test
    public void testFlushThresholdCreatesMultipleBatchFiles() throws Exception {
        tempDir = Files.createTempDirectory("validation-test-");
        String basePath = tempDir.toAbsolutePath().toString();

        SnapshotMetadata snapshotMetadata = new SnapshotMetadata(42L);
        org.lareferencia.core.domain.Network n = new org.lareferencia.core.domain.Network();
        n.setAcronym("TESTNET");
        snapshotMetadata.setNetwork(n);
        Configuration conf = new Configuration();
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        int flushThreshold = 2; // small threshold for test
        try (ValidationRecordManager manager = ValidationRecordManager.forWriting(basePath, snapshotMetadata, conf, flushThreshold)) {
            RecordValidation r1 = new RecordValidation("id-1", LocalDateTime.now(), true, false, "hash1", null);
            RecordValidation r2 = new RecordValidation("id-2", LocalDateTime.now(), false, false, null, null);
            RecordValidation r3 = new RecordValidation("id-3", LocalDateTime.now(), true, true, "hash3", null);

            manager.writeRecord(r1);
            manager.writeRecord(r2);
            manager.writeRecord(r3);
            // Explicit flush and close done by try-with-resources
        }

        String snapshotPath = org.lareferencia.core.util.PathUtils.getSnapshotPath(basePath, snapshotMetadata) + "/validation";
        Path validationDir = Path.of(snapshotPath);
        assertTrue(Files.exists(validationDir), "Validation directory should exist");

        // Check at least two batch files (records_batch_1.parquet and records_batch_2.parquet)
        boolean file1 = Files.exists(validationDir.resolve("records_batch_1.parquet"));
        boolean file2 = Files.exists(validationDir.resolve("records_batch_2.parquet"));

        assertTrue(file1, "Expect records_batch_1.parquet to exist");
        assertTrue(file2, "Expect records_batch_2.parquet to exist");
    }

    @Test
    public void testDeleteParquetFilesPreservesJson() throws Exception {
        tempDir = Files.createTempDirectory("validation-delete-test-");
        String basePath = tempDir.toAbsolutePath().toString();

        SnapshotMetadata snapshotMetadata = new SnapshotMetadata(42L);
        org.lareferencia.core.domain.Network n = new org.lareferencia.core.domain.Network();
        n.setAcronym("TESTNET");
        snapshotMetadata.setNetwork(n);

        Configuration conf = new Configuration();
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        // Create validation files: parquet + JSON
        String snapshotPath = org.lareferencia.core.util.PathUtils.getSnapshotPath(basePath, snapshotMetadata);
        Path validationDir = Path.of(snapshotPath + "/validation");
        Files.createDirectories(validationDir);
        Files.createFile(validationDir.resolve("records_batch_1.parquet"));
        Files.createFile(validationDir.resolve("validation_index.parquet"));
        Files.createFile(validationDir.resolve("validation_stats.json"));

        long initialCount = Files.list(validationDir).count();
        assertEquals(3L, initialCount, "Should have 3 files initially");

        // Test deletion (only parquet)
        try (ValidationRecordManager manager = ValidationRecordManager.forReading(basePath, snapshotMetadata, conf)) {
            manager.deleteParquetFiles();
        }

        long finalCount = Files.list(validationDir).count();
        assertEquals(1L, finalCount, "Should preserve only JSON file");
        assertTrue(Files.exists(validationDir.resolve("validation_stats.json")), "JSON should be preserved");
        assertFalse(Files.exists(validationDir.resolve("records_batch_1.parquet")), "Parquet should be deleted");
    }
}
