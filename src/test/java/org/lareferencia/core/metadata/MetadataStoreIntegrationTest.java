package org.lareferencia.core.metadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.util.hashing.MD5Hashing;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;
import java.io.FileInputStream;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("MetadataStore Integration Tests - File System Implementation")
class MetadataStoreIntegrationTest {

    @TempDir
    Path tempDir;

    private MetadataStoreFSImpl metadataStore;
    private MD5Hashing hashingHelper;
    private SnapshotMetadata testSnapshotMetadata;

    @BeforeEach
    void setUp() throws Exception {
        metadataStore = new MetadataStoreFSImpl();
        hashingHelper = new MD5Hashing();
        
        // Create a test snapshot metadata
        testSnapshotMetadata = new SnapshotMetadata(1L);
        testSnapshotMetadata.setNetworkAcronym("TEST");
        testSnapshotMetadata.setSize(100L);

        // Inject test configuration using reflection
        var basePathField = MetadataStoreFSImpl.class.getDeclaredField("basePath");
        basePathField.setAccessible(true);
        basePathField.set(metadataStore, tempDir.toString());

        var hashingField = MetadataStoreFSImpl.class.getDeclaredField("hashing");
        hashingField.setAccessible(true);
        hashingField.set(metadataStore, hashingHelper);

        metadataStore.init();
    }

    @Test
    @DisplayName("Should store and retrieve single metadata record")
    void testStoreSingleMetadata() throws Exception {
        // Given
        String xmlContent = "<metadata><title>Test Record</title><author>John Doe</author></metadata>";

        // When
        String hash = metadataStore.storeAndReturnHash(testSnapshotMetadata, xmlContent);
        String retrieved = metadataStore.getMetadata(testSnapshotMetadata, hash);

        // Then
        assertNotNull(hash);
        assertNotNull(retrieved);
        assertEquals(xmlContent, retrieved);
        assertTrue(retrieved.contains("Test Record"));
        assertTrue(retrieved.contains("John Doe"));
    }

    @Test
    @DisplayName("Should handle multiple metadata records")
    void testStoreMultipleMetadata() throws Exception {
        // Given
        List<String> xmlContents = new ArrayList<>();
        List<String> hashes = new ArrayList<>();
        
        for (int i = 0; i < 10; i++) {
            String xml = String.format("<metadata><id>%d</id><title>Record %d</title></metadata>", i, i);
            xmlContents.add(xml);
        }

        // When - Store all records
        for (String xml : xmlContents) {
            String hash = metadataStore.storeAndReturnHash(testSnapshotMetadata, xml);
            hashes.add(hash);
        }

        // Then - Retrieve and verify all records
        for (int i = 0; i < 10; i++) {
            String retrieved = metadataStore.getMetadata(testSnapshotMetadata, hashes.get(i));
            assertNotNull(retrieved);
            assertEquals(xmlContents.get(i), retrieved);
            assertTrue(retrieved.contains(String.format("Record %d", i)));
        }
    }

    @Test
    @DisplayName("Should create partitioned directory structure")
    void testPartitionedStructure() throws Exception {
        // Given
        String xmlContent = "<metadata><title>Partition Test</title></metadata>";

        // When
        String hash = metadataStore.storeAndReturnHash(testSnapshotMetadata, xmlContent);

        // Then - Verify 3-level partitioning (A/B/C/hash.xml.gz)
        String level1 = hash.substring(0, 1).toUpperCase();
        String level2 = hash.substring(1, 2).toUpperCase();
        String level3 = hash.substring(2, 3).toUpperCase();
        
        Path expectedPath = tempDir.resolve(level1).resolve(level2).resolve(level3).resolve(hash + ".xml.gz");
        assertTrue(Files.exists(expectedPath), "File should exist at: " + expectedPath);
        assertTrue(Files.isRegularFile(expectedPath), "Should be a regular file");
    }

    @Test
    @DisplayName("Should handle duplicate content (same hash)")
    void testDuplicateContent() throws Exception {
        // Given
        String xmlContent = "<metadata><title>Duplicate Test</title></metadata>";

        // When - Store same content twice
        String hash1 = metadataStore.storeAndReturnHash(testSnapshotMetadata, xmlContent);
        String hash2 = metadataStore.storeAndReturnHash(testSnapshotMetadata, xmlContent);

        // Then
        assertEquals(hash1, hash2, "Same content should produce same hash");
        
        // Verify only one file exists
        String level1 = hash1.substring(0, 1).toUpperCase();
        String level2 = hash1.substring(1, 2).toUpperCase();
        String level3 = hash1.substring(2, 3).toUpperCase();
        Path filePath = tempDir.resolve(level1).resolve(level2).resolve(level3).resolve(hash1 + ".xml.gz");
        
        assertTrue(Files.exists(filePath), "File should exist");
    }

    @Test
    @DisplayName("Should handle metadata with special characters")
    void testSpecialCharactersInMetadata() throws Exception {
        // Given
        String xmlContent = "<metadata>" +
                "<title>Test with Special: &lt;chars&gt; &amp; \"quotes\"</title>" +
                "<description>Símbolos especiales: ñ, á, é, í, ó, ú, ü</description>" +
                "<path>/some/path/with/slashes</path>" +
                "</metadata>";

        // When
        String hash = metadataStore.storeAndReturnHash(testSnapshotMetadata, xmlContent);
        String retrieved = metadataStore.getMetadata(testSnapshotMetadata, hash);

        // Then
        assertNotNull(retrieved);
        assertEquals(xmlContent, retrieved);
        assertTrue(retrieved.contains("&lt;chars&gt;"));
        assertTrue(retrieved.contains("ñ, á, é"));
        assertTrue(retrieved.contains("/some/path/with/slashes"));
    }

    @Test
    @DisplayName("Should verify gzip compression is used")
    void testCompressionIsUsed() throws Exception {
        // Given
        String xmlContent = "<metadata>" +
                "<title>Large Metadata for Compression Test</title>" +
                "<description>This is a longer description to test compression efficiency.</description>" +
                "</metadata>";

        // When
        String hash = metadataStore.storeAndReturnHash(testSnapshotMetadata, xmlContent);

        // Then - Verify file is gzipped
        String level1 = hash.substring(0, 1).toUpperCase();
        String level2 = hash.substring(1, 2).toUpperCase();
        String level3 = hash.substring(2, 3).toUpperCase();
        Path filePath = tempDir.resolve(level1).resolve(level2).resolve(level3).resolve(hash + ".xml.gz");

        assertTrue(Files.exists(filePath), "Compressed file should exist");
        assertTrue(filePath.toString().endsWith(".xml.gz"), "File should have .xml.gz extension");

        // Verify it's actually gzipped by reading with GZIPInputStream
        try (FileInputStream fis = new FileInputStream(filePath.toFile());
             GZIPInputStream gzis = new GZIPInputStream(fis)) {
            byte[] decompressed = gzis.readAllBytes();
            String content = new String(decompressed);
            assertEquals(xmlContent, content);
        }
    }

    @Test
    @DisplayName("Should handle empty metadata")
    void testEmptyMetadata() throws Exception {
        // Given
        String xmlContent = "<metadata></metadata>";

        // When
        String hash = metadataStore.storeAndReturnHash(testSnapshotMetadata, xmlContent);
        String retrieved = metadataStore.getMetadata(testSnapshotMetadata, hash);

        // Then
        assertNotNull(hash);
        assertNotNull(retrieved);
        assertEquals(xmlContent, retrieved);
    }

    @Test
    @DisplayName("Should handle large metadata")
    void testLargeMetadata() throws Exception {
        // Given - Create large XML with many fields
        StringBuilder xmlBuilder = new StringBuilder("<metadata>");
        for (int i = 0; i < 1000; i++) {
            xmlBuilder.append(String.format("<field%d>Value %d</field%d>", i, i, i));
        }
        xmlBuilder.append("</metadata>");
        String xmlContent = xmlBuilder.toString();

        // When
        String hash = metadataStore.storeAndReturnHash(testSnapshotMetadata, xmlContent);
        String retrieved = metadataStore.getMetadata(testSnapshotMetadata, hash);

        // Then
        assertNotNull(hash);
        assertNotNull(retrieved);
        assertEquals(xmlContent, retrieved);
        assertTrue(retrieved.contains("field999"));
        assertTrue(retrieved.contains("Value 999"));
    }

    @Test
    @DisplayName("Should throw exception when retrieving non-existent metadata")
    void testRetrieveNonExistentMetadata() {
        // Given
        String nonExistentHash = "nonexistenthash123456789";

        // When/Then
        assertThrows(MetadataRecordStoreException.class, () -> {
            metadataStore.getMetadata(testSnapshotMetadata, nonExistentHash);
        });
    }

    @Test
    @DisplayName("Should handle concurrent access")
    void testConcurrentAccess() throws Exception {
        // Given
        int threadCount = 5;
        int recordsPerThread = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Future<List<String>>> futures = new ArrayList<>();

        // When - Multiple threads storing metadata concurrently
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            Future<List<String>> future = executor.submit(() -> {
                List<String> hashes = new ArrayList<>();
                try {
                    for (int i = 0; i < recordsPerThread; i++) {
                        String xml = String.format("<metadata><thread>%d</thread><record>%d</record></metadata>", 
                                                  threadId, i);
                        String hash = metadataStore.storeAndReturnHash(testSnapshotMetadata, xml);
                        hashes.add(hash);
                    }
                } finally {
                    latch.countDown();
                }
                return hashes;
            });
            futures.add(future);
        }

        // Wait for all threads
        latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        // Then - Verify all records were stored correctly
        for (int t = 0; t < threadCount; t++) {
            List<String> hashes = futures.get(t).get();
            assertEquals(recordsPerThread, hashes.size());
            
            for (int i = 0; i < recordsPerThread; i++) {
                String retrieved = metadataStore.getMetadata(testSnapshotMetadata, hashes.get(i));
                assertNotNull(retrieved);
                assertTrue(retrieved.contains(String.format("<thread>%d</thread>", t)));
                assertTrue(retrieved.contains(String.format("<record>%d</record>", i)));
            }
        }
    }
}
