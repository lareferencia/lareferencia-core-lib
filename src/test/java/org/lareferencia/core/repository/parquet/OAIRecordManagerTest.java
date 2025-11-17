/*
 * JUnit test for OAIRecordManager flush behaviour using flushThreshold
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

import static org.junit.jupiter.api.Assertions.assertTrue;

public class OAIRecordManagerTest {

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
        tempDir = Files.createTempDirectory("oai-test-");
        String basePath = tempDir.toAbsolutePath().toString();

        SnapshotMetadata snapshotMetadata = new SnapshotMetadata(42L);
        org.lareferencia.core.domain.Network n = new org.lareferencia.core.domain.Network();
        n.setAcronym("TESTNET");
        snapshotMetadata.setNetwork(n);

        Configuration conf = new Configuration();
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        int flushThreshold = 2; // small threshold for test
        try (OAIRecordManager manager = OAIRecordManager.forWriting(basePath, snapshotMetadata, conf, flushThreshold)) {
            OAIRecord r1 = OAIRecord.create("id-1", LocalDateTime.now(), "hash1", false);
            OAIRecord r2 = OAIRecord.create("id-2", LocalDateTime.now(), "hash2", true);
            OAIRecord r3 = OAIRecord.create("id-3", LocalDateTime.now(), "hash3", false);

            manager.writeRecord(r1);
            manager.writeRecord(r2);
            manager.writeRecord(r3);
            // Explicit flush and close done by try-with-resources
        }

        String snapshotPath = org.lareferencia.core.util.PathUtils.getSnapshotPath(basePath, snapshotMetadata) + "/catalog";
        Path catalogDir = Path.of(snapshotPath);
        assertTrue(Files.exists(catalogDir), "Catalog directory should exist");

        boolean file1 = Files.exists(catalogDir.resolve("oai_records_batch_1.parquet"));
        boolean file2 = Files.exists(catalogDir.resolve("oai_records_batch_2.parquet"));

        assertTrue(file1, "Expect oai_records_batch_1.parquet to exist");
        assertTrue(file2, "Expect oai_records_batch_2.parquet to exist");
    }
}
