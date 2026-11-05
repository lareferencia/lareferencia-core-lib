package org.lareferencia.core.export;

import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class ExportBatchWriterTest {
    @TempDir Path temp;
    private ExportRequest request(String format, Long records, Long bytes) {
        return new ExportRequest(List.of("A"), false, "original", format, Path.of("mapping.json"), "collection", "none", records, bytes, "none", false, temp);
    }
    @Test void accountsForUtf8FramingAndDoesNotSplitRecords() throws Exception {
        byte[] record = "{\"text\":\"á\"}".getBytes(StandardCharsets.UTF_8);
        long exact = 2L * record.length + 1 + 3; // array prefix, comma, suffix and final newline
        try (var writer = new ExportBatchWriter(temp, request("json", null, exact))) {
            var first = writer.write("data", "a", record);
            var second = writer.write("data", "b", record);
            var third = writer.write("data", "c", record);
            assertEquals(first.file(), second.file()); assertNotEquals(first.file(), third.file());
            assertEquals(0, first.position()); assertEquals(1, second.position());
        }
        assertEquals(exact, Files.size(temp.resolve("data/part-000001.json")));
        assertEquals(2, ExportJson.JSON.readTree(temp.resolve("data/part-000001.json").toFile()).size());
    }
    @Test void oversizedRecordsAreIsolatedAndWarned() throws Exception {
        try (var writer = new ExportBatchWriter(temp, request("jsonl", null, 2L))) {
            var first = writer.write("data", "a", "{\"long\":1}".getBytes(StandardCharsets.UTF_8));
            var second = writer.write("data", "b", "{}".getBytes(StandardCharsets.UTF_8));
            assertNotEquals(first.file(), second.file()); assertEquals(2, writer.oversizedRecords);
        }
    }
    @Test void outputFailureDoesNotPublishATarget() throws Exception {
        Files.writeString(temp.resolve("data"), "blocks directory creation");
        try (var writer = new ExportBatchWriter(temp, request("jsonl", 1L, null))) {
            assertThrows(java.io.IOException.class, () -> writer.write("data", "a", new byte[]{123,125}));
        }
        assertFalse(Files.exists(temp.resolve("data/part-000001.jsonl")));
    }
    @Test void rejectsConflictingOptionsAndParsesByteLimits() {
        assertThrows(IllegalArgumentException.class, () -> new ExportRequest(List.of("A", "A"), false, "original", "jsonl", Path.of("mapping"), "collection", "none", null, null, "gzip", false, temp));
        assertThrows(IllegalArgumentException.class, () -> new ExportRequest(List.of("A"), false, "original", "xml", null, "per-record", "none", 5L, null, "gzip", false, temp));
        assertEquals(128L * 1024 * 1024, ExportRequest.parseBytes("128MiB"));
        assertThrows(IllegalArgumentException.class, () -> ExportRequest.parseBytes("0MiB"));
        assertThrows(ArithmeticException.class, () -> ExportRequest.parseBytes("9223372036854775807GiB"));
    }
    @Test void diskFullDuringARecordLeavesOnlyAnUnpublishedTemporary() throws Exception {
        var writer = new ExportBatchWriter(temp, request("jsonl", null, null), path -> new java.io.FilterOutputStream(Files.newOutputStream(path)) {
            int remaining = 4;
            @Override public void write(int value) throws java.io.IOException {
                if (remaining-- <= 0) throw new java.io.IOException("simulated disk full");
                out.write(value);
            }
            @Override public void write(byte[] value, int offset, int length) throws java.io.IOException {
                for (int i = offset; i < offset + length; i++) write(value[i]);
            }
        });
        try (writer) {
            assertThrows(java.io.IOException.class, () -> writer.write("data", "a", "{\"long\":1}".getBytes(StandardCharsets.UTF_8)));
        }
        assertFalse(Files.exists(temp.resolve("data/part-000001.jsonl")));
        assertTrue(Files.exists(temp.resolve("data/part-000001.jsonl.tmp")));
    }

}
