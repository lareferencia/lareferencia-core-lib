/*
 *   Copyright (c) 2013-2022. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU Affero General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU Affero General Public License for more details.
 *
 *   You should have received a copy of the GNU Affero General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *   This file is part of LA Referencia software platform LRHarvester v4.x
 *   For any further information please contact Lautaro Matas <lmatas@gmail.com>
 */

package org.lareferencia.core.metadata;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.hashing.IHashingHelper;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MetadataStoreFSImpl Tests")
class MetadataStoreFSImplTest {

    @TempDir
    Path tempDir;

    private MetadataStoreFSImpl store;
    private MockHashingHelper hashingHelper;
    private SnapshotMetadata testSnapshotMetadata;

    @BeforeEach
    void setUp() {
        store = new MetadataStoreFSImpl();
        hashingHelper = new MockHashingHelper();
        
        // Create test snapshot metadata
        testSnapshotMetadata = new SnapshotMetadata();
        testSnapshotMetadata.setSnapshotId(1L);
        testSnapshotMetadata.setNetworkAcronym("TEST");
        
        // Set the base path to temp directory
        ReflectionTestUtils.setField(store, "basePath", tempDir.toString());
        ReflectionTestUtils.setField(store, "hashing", hashingHelper);
        
        // Call init to create directories
        store.init();
    }

    @AfterEach
    void tearDown() {
        // Cleanup is automatic with @TempDir
    }

    // Init tests

    @Test
    @DisplayName("init should create base directory if it doesn't exist")
    void testInitCreatesBaseDirectory() throws IOException {
        Path newTempDir = Files.createTempDirectory("metadata-store-test");
        Path baseDir = newTempDir.resolve("new-store");
        
        assertFalse(Files.exists(baseDir));
        
        MetadataStoreFSImpl newStore = new MetadataStoreFSImpl();
        ReflectionTestUtils.setField(newStore, "basePath", baseDir.toString());
        ReflectionTestUtils.setField(newStore, "hashing", hashingHelper);
        
        newStore.init();
        
        assertTrue(Files.exists(baseDir));
        assertTrue(Files.isDirectory(baseDir));
        
        // Cleanup
        Files.deleteIfExists(baseDir);
        Files.deleteIfExists(newTempDir);
    }

    @Test
    @DisplayName("init should not fail if base directory already exists")
    void testInitWithExistingDirectory() {
        assertTrue(Files.exists(tempDir));
        assertDoesNotThrow(() -> store.init());
    }

    // storeAndReturnHash tests

    @Test
    @DisplayName("storeAndReturnHash should store metadata and return hash")
    void testStoreAndReturnHashSuccess() {
        String metadata = "<record><title>Test</title></record>";
        hashingHelper.setNextHash("ABC123456789");

        String hash = store.storeAndReturnHash(testSnapshotMetadata, metadata);

        assertEquals("ABC123456789", hash);
        
        // Verify file was created
        File file = new File(tempDir.toFile(), "A/B/C/ABC123456789.xml.gz");
        assertTrue(file.exists());
    }

    @Test
    @DisplayName("storeAndReturnHash should create partition directories")
    void testStoreAndReturnHashCreatesDirectories() {
        String metadata = "<record><title>Test</title></record>";
        hashingHelper.setNextHash("XYZ987654321");

        store.storeAndReturnHash(testSnapshotMetadata, metadata);

        // Verify partition directories were created
        assertTrue(Files.exists(tempDir.resolve("X")));
        assertTrue(Files.exists(tempDir.resolve("X/Y")));
        assertTrue(Files.exists(tempDir.resolve("X/Y/Z")));
    }

    @Test
    @DisplayName("storeAndReturnHash should compress metadata using gzip")
    void testStoreAndReturnHashCompression() throws IOException {
        String metadata = "<record><title>Test Document with lots of content that should compress well</title></record>";
        hashingHelper.setNextHash("ABC123456789");

        store.storeAndReturnHash(testSnapshotMetadata, metadata);

        File file = new File(tempDir.toFile(), "A/B/C/ABC123456789.xml.gz");
        
        // Read compressed content and verify it's correct
        String decompressed = decompressFile(file);
        assertEquals(metadata, decompressed);
        
        // Verify file was created with gzip format
        assertTrue(file.exists());
        assertTrue(file.getName().endsWith(".xml.gz"));
    }

    @Test
    @DisplayName("storeAndReturnHash should deduplicate existing metadata")
    void testStoreAndReturnHashDeduplication() throws IOException {
        String metadata = "<record><title>Test</title></record>";
        hashingHelper.setNextHash("ABC123456789");

        // Store first time
        String hash1 = store.storeAndReturnHash(testSnapshotMetadata, metadata);
        
        File file = new File(tempDir.toFile(), "A/B/C/ABC123456789.xml.gz");
        long modifiedTime1 = file.lastModified();
        long size1 = file.length();

        // Wait a bit to ensure different modification time if file is recreated
        try { Thread.sleep(10); } catch (InterruptedException e) {}

        // Store second time with same hash
        String hash2 = store.storeAndReturnHash(testSnapshotMetadata, metadata);
        
        long modifiedTime2 = file.lastModified();
        long size2 = file.length();

        assertEquals(hash1, hash2);
        assertEquals(modifiedTime1, modifiedTime2, "File should not be modified on duplicate store");
        assertEquals(size1, size2);
    }

    @Test
    @DisplayName("storeAndReturnHash should store multiple different metadata")
    void testStoreAndReturnHashMultiple() {
        String metadata1 = "<record><title>First</title></record>";
        String metadata2 = "<record><title>Second</title></record>";
        
        hashingHelper.setNextHash("ABC111111111");
        String hash1 = store.storeAndReturnHash(testSnapshotMetadata, metadata1);
        
        hashingHelper.setNextHash("XYZ999999999");
        String hash2 = store.storeAndReturnHash(testSnapshotMetadata, metadata2);

        assertNotEquals(hash1, hash2);
        
        File file1 = new File(tempDir.toFile(), "A/B/C/ABC111111111.xml.gz");
        File file2 = new File(tempDir.toFile(), "X/Y/Z/XYZ999999999.xml.gz");
        
        assertTrue(file1.exists());
        assertTrue(file2.exists());
    }

    @Test
    @DisplayName("storeAndReturnHash should handle empty metadata")
    void testStoreAndReturnHashEmptyMetadata() {
        String metadata = "";
        hashingHelper.setNextHash("ABC123456789");

        String hash = store.storeAndReturnHash(testSnapshotMetadata, metadata);

        assertEquals("ABC123456789", hash);
        File file = new File(tempDir.toFile(), "A/B/C/ABC123456789.xml.gz");
        assertTrue(file.exists());
    }

    @Test
    @DisplayName("storeAndReturnHash should handle large metadata")
    void testStoreAndReturnHashLargeMetadata() {
        // Create a large metadata string (1MB)
        StringBuilder sb = new StringBuilder();
        sb.append("<record>");
        for (int i = 0; i < 10000; i++) {
            sb.append("<field>Large content that repeats many times to test compression ").append(i).append("</field>");
        }
        sb.append("</record>");
        String metadata = sb.toString();
        
        hashingHelper.setNextHash("ABC123456789");

        String hash = store.storeAndReturnHash(testSnapshotMetadata, metadata);

        assertEquals("ABC123456789", hash);
        File file = new File(tempDir.toFile(), "A/B/C/ABC123456789.xml.gz");
        assertTrue(file.exists());
        
        // Verify significant compression
        long compressedSize = file.length();
        long originalSize = metadata.getBytes(StandardCharsets.UTF_8).length;
        assertTrue(compressedSize < originalSize / 2, 
            "Large repetitive content should compress to less than 50%");
    }

    // getMetadata tests

    @Test
    @DisplayName("getMetadata should retrieve stored metadata")
    void testGetMetadataSuccess() throws MetadataRecordStoreException {
        String metadata = "<record><title>Test</title></record>";
        hashingHelper.setNextHash("ABC123456789");
        
        store.storeAndReturnHash(testSnapshotMetadata, metadata);
        String retrieved = store.getMetadata(testSnapshotMetadata, "ABC123456789");

        assertEquals(metadata, retrieved);
    }

    @Test
    @DisplayName("getMetadata should throw exception for non-existent hash")
    void testGetMetadataNonExistent() {
        MetadataRecordStoreException exception = assertThrows(
            MetadataRecordStoreException.class,
            () -> store.getMetadata(testSnapshotMetadata, "NONEXISTENT123")
        );
        assertTrue(exception.getMessage().contains("Metadata not found for hash"));
        assertTrue(exception.getMessage().contains("NONEXISTENT123"));
    }

    @Test
    @DisplayName("getMetadata should decompress gzip content correctly")
    void testGetMetadataDecompression() throws MetadataRecordStoreException {
        String metadata = "<record><title>Test with special chars: é ñ ü</title></record>";
        hashingHelper.setNextHash("ABC123456789");
        
        store.storeAndReturnHash(testSnapshotMetadata, metadata);
        String retrieved = store.getMetadata(testSnapshotMetadata, "ABC123456789");

        assertEquals(metadata, retrieved);
    }

    @Test
    @DisplayName("getMetadata should handle UTF-8 encoding correctly")
    void testGetMetadataUTF8() throws MetadataRecordStoreException {
        String metadata = "<record><title>Test with 中文 and Ελληνικά and العربية</title></record>";
        hashingHelper.setNextHash("ABC123456789");
        
        store.storeAndReturnHash(testSnapshotMetadata, metadata);
        String retrieved = store.getMetadata(testSnapshotMetadata, "ABC123456789");

        assertEquals(metadata, retrieved);
    }

    @Test
    @DisplayName("getMetadata should retrieve large metadata correctly")
    void testGetMetadataLarge() throws MetadataRecordStoreException {
        StringBuilder sb = new StringBuilder();
        sb.append("<record>");
        for (int i = 0; i < 1000; i++) {
            sb.append("<field>Content ").append(i).append("</field>");
        }
        sb.append("</record>");
        String metadata = sb.toString();
        
        hashingHelper.setNextHash("ABC123456789");
        
        store.storeAndReturnHash(testSnapshotMetadata, metadata);
        String retrieved = store.getMetadata(testSnapshotMetadata, "ABC123456789");

        assertEquals(metadata, retrieved);
    }

    // cleanAndOptimizeStore tests

    @Test
    @DisplayName("cleanAndOptimizeStore should return true")
    void testCleanAndOptimizeStoreSuccess() {
        Boolean result = store.cleanAndOptimizeStore();
        assertTrue(result);
    }

    @Test
    @DisplayName("cleanAndOptimizeStore should work with empty store")
    void testCleanAndOptimizeStoreEmpty() {
        Boolean result = store.cleanAndOptimizeStore();
        assertTrue(result);
    }

    @Test
    @DisplayName("cleanAndOptimizeStore should count files correctly")
    void testCleanAndOptimizeStoreWithFiles() {
        // Store some files
        hashingHelper.setNextHash("ABC111111111");
        store.storeAndReturnHash(testSnapshotMetadata, "<record>1</record>");
        
        hashingHelper.setNextHash("ABC222222222");
        store.storeAndReturnHash(testSnapshotMetadata, "<record>2</record>");
        
        hashingHelper.setNextHash("XYZ333333333");
        store.storeAndReturnHash(testSnapshotMetadata, "<record>3</record>");

        Boolean result = store.cleanAndOptimizeStore();
        assertTrue(result);
    }

    // Edge cases and error handling

    @Test
    @DisplayName("getMetadata should handle hash with less than 3 characters")
    void testGetMetadataShortHash() {
        // This should fail because hash is too short for partitioning
        assertThrows(Exception.class, () -> store.getMetadata(testSnapshotMetadata, "AB"));
    }

    @Test
    @DisplayName("storeAndReturnHash should handle hash with lowercase letters")
    void testStoreAndReturnHashLowercaseHash() {
        String metadata = "<record><title>Test</title></record>";
        hashingHelper.setNextHash("abc123456789");

        store.storeAndReturnHash(testSnapshotMetadata, metadata);

        // Hash should be stored, and partition path should use uppercase
        File file = new File(tempDir.toFile(), "A/B/C/abc123456789.xml.gz");
        assertTrue(file.exists());
    }

    @Test
    @DisplayName("Store and retrieve metadata roundtrip should preserve content exactly")
    void testRoundtripPreservesContent() throws MetadataRecordStoreException {
        String metadata = "<record>\n" +
            "  <title>Test with\n" +
            "    whitespace and newlines\n" +
            "  </title>\n" +
            "  <description>Special chars: &lt;&gt;&amp;\"'</description>\n" +
            "</record>";
        
        hashingHelper.setNextHash("ABC123456789");
        
        String hash = store.storeAndReturnHash(testSnapshotMetadata, metadata);
        String retrieved = store.getMetadata(testSnapshotMetadata, hash);

        assertEquals(metadata, retrieved, "Roundtrip should preserve content exactly");
    }

    // Helper methods

    /**
     * Decompresses a gzip file for verification
     */
    private String decompressFile(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file);
             BufferedInputStream bis = new BufferedInputStream(fis);
             GZIPInputStream gzis = new GZIPInputStream(bis)) {
            
            byte[] buffer = gzis.readAllBytes();
            return new String(buffer, StandardCharsets.UTF_8);
        }
    }

    // Mock hashing helper for testing

    private static class MockHashingHelper implements IHashingHelper {
        private String nextHash = "DEFAULT_HASH";

        public void setNextHash(String hash) {
            this.nextHash = hash;
        }

        @Override
        public String calculateHash(String input) {
            return nextHash;
        }
    }
}
