/*
 * Unit tests for SnapshotLogService
 */
package org.lareferencia.core.service.management;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.util.PathUtils;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotLogServiceTest {

    @Test
    void testAddEntry_removesNewlinesAndWritesSingleLine(@TempDir Path tempDir) throws Exception {
        SnapshotLogService service = new SnapshotLogService();

        // Override basePath using reflection to point to temporary directory
        Field basePathField = SnapshotLogService.class.getDeclaredField("basePath");
        basePathField.setAccessible(true);
        basePathField.set(service, tempDir.toString() + File.separator);

        // Prepare metadata and cache it
        Long snapshotId = 1L;
        SnapshotMetadata metadata = new SnapshotMetadata(snapshotId);
        Network network = new Network();
        network.setAcronym("test-net");
        metadata.setNetwork(network);
        service.cacheMetadata(metadata);

        // Message containing different newlines
        String original = "Line1\nLine2\r\nLine3";
        service.addEntry(snapshotId, original);

        String sanitizedNetwork = PathUtils.sanitizeNetworkAcronym(network.getAcronym());
        Path logPath = Paths.get(tempDir.toString(), sanitizedNetwork, "snapshots", String.format("snapshot_%d", snapshotId), "snapshot.log");

        assertTrue(Files.exists(logPath), "Log file should be created");

        List<String> lines = Files.readAllLines(logPath, StandardCharsets.UTF_8);
        assertEquals(1, lines.size(), "Only one log entry should be present and as a single line");

        String line = lines.get(0);
        assertFalse(line.contains("\n"), "Line should not contain newline characters");
        assertFalse(line.contains("\r"), "Line should not contain carriage return characters");

        // Extract message part after the timestamp bracket and verify sanitized content
        int idx = line.indexOf(']');
        assertTrue(idx > 0);
        String messagePart = line.substring(idx + 1).trim();
        assertEquals("Line1 Line2 Line3", messagePart);
    }
}
