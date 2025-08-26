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

package org.lareferencia.backend.repositories.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;
import org.lareferencia.backend.repositories.parquet.ValidationStatParquetQueryEngine.AggregationFilter;
import org.lareferencia.backend.repositories.parquet.ValidationStatParquetQueryEngine.AggregationResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Repositorio para datos de validación en Parquet con persistencia real.
 * Utiliza Apache Parquet para almacenamiento eficiente en disco.
 * 
 * ARCHITECTURE OVERVIEW:
 * - NEW: Multiple parquet files per snapshot for optimal performance
 *   Structure: /base-path/snapshot-{id}/data-{index}.parquet
 *   Each file contains configurable number of records (default: 10000)
 *   Enables streaming processing for millions of records without memory overload
 * 
 * - LEGACY: Single file per snapshot (compatibility mode)
 *   Structure: /base-path/snapshot_{id}.parquet  
 *   Used for backward compatibility with existing data
 * 
 * MEMORY MANAGEMENT:
 * - Intelligent buffering system accumulates records until recordsPerFile limit
 * - Automatic flush when buffer reaches capacity or validation completes
 * - Multi-file streaming with offset/limit for efficient pagination
 * 
 * PERFORMANCE FEATURES:
 * - Row group pruning for fast filtering
 * - Aggregated statistics without full record loading
 * - Memory-efficient processing of datasets with millions of records
 */
@Repository
public class ValidationStatParquetRepository {

    private static final Logger logger = LogManager.getLogger(ValidationStatParquetRepository.class);

    @Value("${validation.stats.parquet.path:/tmp/validation-stats-parquet}")
    private String parquetBasePath;
    
    @Value("${parquet.validation.records-per-file:10000}")
    private int recordsPerFile;

    @Autowired
    private ValidationStatParquetQueryEngine queryEngine;

    private Schema avroSchema;
    private Configuration hadoopConf;
    
    // Counter to track next file index per snapshot
    private final Map<Long, Integer> snapshotFileCounters = new HashMap<>();
    
    // BUFFER SYSTEM: Accumulate records until reaching recordsPerFile limit
    private final Map<Long, List<ValidationStatObservationParquet>> snapshotBuffers = new HashMap<>();
    
    @PostConstruct
    public void init() throws IOException {
        logger.info("Initializing ValidationStatParquetRepository with {} records per file", recordsPerFile);
        
        // Initialize Hadoop configuration
        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        
        // Create base directory if it doesn't exist
        Files.createDirectories(Paths.get(parquetBasePath));
        
        // Initialize Avro schema for validation data
        initializeAvroSchema();
        
        // Initialize file counters for existing snapshots
        initializeFileCounters();
        
        logger.info("ValidationStatParquetRepository initialized - ready to use at: {}", parquetBasePath);
    }
    
    /**
     * CLEANUP: Flush any remaining buffered data before shutdown
     */
    @PreDestroy
    public void shutdown() {
        try {
            logger.info("SHUTDOWN: Flushing remaining buffers before shutdown");
            flushAllBuffers();
        } catch (IOException e) {
            logger.error("SHUTDOWN: Error flushing buffers during shutdown", e);
        }
    }
    
    /**
     * Initialize file counters for existing snapshots to avoid file conflicts
     */
    private void initializeFileCounters() throws IOException {
        File baseDir = new File(parquetBasePath);
        if (!baseDir.exists()) {
            return;
        }
        
        File[] snapshotDirs = baseDir.listFiles(file -> file.isDirectory() && file.getName().startsWith("snapshot-"));
        if (snapshotDirs != null) {
            for (File snapshotDir : snapshotDirs) {
                try {
                    String dirName = snapshotDir.getName();
                    Long snapshotId = Long.parseLong(dirName.substring("snapshot-".length()));
                    
                    // Count existing data files in this snapshot directory
                    File[] dataFiles = snapshotDir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
                    int maxIndex = -1;
                    if (dataFiles != null) {
                        for (File dataFile : dataFiles) {
                            String fileName = dataFile.getName();
                            String indexStr = fileName.substring("data-".length(), fileName.indexOf(".parquet"));
                            try {
                                int index = Integer.parseInt(indexStr);
                                maxIndex = Math.max(maxIndex, index);
                            } catch (NumberFormatException e) {
                                logger.warn("Invalid data file name format: {}", fileName);
                            }
                        }
                    }
                    
                    snapshotFileCounters.put(snapshotId, maxIndex + 1);
                    logger.debug("Initialized file counter for snapshot {}: next index = {}", snapshotId, maxIndex + 1);
                } catch (NumberFormatException e) {
                    logger.warn("Invalid snapshot directory name: {}", snapshotDir.getName());
                }
            }
        }
    }
    
    private void initializeAvroSchema() {
        // Define Avro schema for ValidationStatObservation
        String schemaJson = "{\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"ValidationStatObservation\",\n" +
            "  \"namespace\": \"org.lareferencia.backend.domain.parquet\",\n" +
            "  \"fields\": [\n" +
            "    {\"name\": \"id\", \"type\": \"string\"},\n" +
            "    {\"name\": \"identifier\", \"type\": \"string\"},\n" +
            "    {\"name\": \"snapshotID\", \"type\": \"long\"},\n" +
            "    {\"name\": \"origin\", \"type\": \"string\"},\n" +
            "    {\"name\": \"setSpec\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"metadataPrefix\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"networkAcronym\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"repositoryName\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"institutionName\", \"type\": [\"null\", \"string\"], \"default\": null},\n" +
            "    {\"name\": \"isValid\", \"type\": \"boolean\"},\n" +
            "    {\"name\": \"isTransformed\", \"type\": \"boolean\"},\n" +
            "    {\"name\": \"validOccurrencesByRuleID\", \"type\": [\"null\", {\"type\": \"map\", \"values\": {\"type\": \"array\", \"items\": \"string\"}}], \"default\": null},\n" +
            "    {\"name\": \"invalidOccurrencesByRuleID\", \"type\": [\"null\", {\"type\": \"map\", \"values\": {\"type\": \"array\", \"items\": \"string\"}}], \"default\": null},\n" +
            "    {\"name\": \"validRulesIDList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}], \"default\": null},\n" +
            "    {\"name\": \"invalidRulesIDList\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}], \"default\": null}\n" +
            "  ]\n" +
            "}";
        
        avroSchema = new Schema.Parser().parse(schemaJson);
    }
    
    /**
     * NEW: Get snapshot directory path
     */
    private String getSnapshotDirectoryPath(Long snapshotId) {
        return parquetBasePath + "/snapshot-" + snapshotId;
    }
    
    /**
     * NEW: Get specific data file path within snapshot directory
     */
    private String getDataFilePath(Long snapshotId, int fileIndex) {
        return getSnapshotDirectoryPath(snapshotId) + "/data-" + fileIndex + ".parquet";
    }
    
    /**
     * NEW: Get all existing data file paths for a snapshot
     */
    private List<String> getAllDataFilePaths(Long snapshotId) throws IOException {
        List<String> filePaths = new ArrayList<>();
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists() || !dir.isDirectory()) {
            logger.debug("Snapshot directory does not exist: {}", snapshotDir);
            return filePaths;
        }
        
        File[] dataFiles = dir.listFiles(file -> 
            file.isFile() && file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles != null) {
            // Sort files by index to ensure consistent ordering
            Arrays.sort(dataFiles, (f1, f2) -> {
                try {
                    String name1 = f1.getName();
                    String name2 = f2.getName();
                    int index1 = Integer.parseInt(name1.substring("data-".length(), name1.indexOf(".parquet")));
                    int index2 = Integer.parseInt(name2.substring("data-".length(), name2.indexOf(".parquet")));
                    return Integer.compare(index1, index2);
                } catch (NumberFormatException e) {
                    return f1.getName().compareTo(f2.getName());
                }
            });
            
            for (File file : dataFiles) {
                filePaths.add(file.getAbsolutePath());
            }
        }
        
        logger.debug("Found {} data files for snapshot {}", filePaths.size(), snapshotId);
        return filePaths;
    }
    
    /**
     * LEGACY PATH SYSTEM: Single file per snapshot (backward compatibility)
     * Returns: /base-path/snapshot_{id}.parquet
     * 
     * Used by methods that still need compatibility with pre-existing single-file data:
     * - findBySnapshotId() for legacy data access
     * - deleteBySnapshotId() for cleanup operations  
     * - existsBySnapshotId() for existence checks
     * 
     * @deprecated in favor of multi-file architecture, but maintained for compatibility
     */
    private String getParquetFilePath(Long snapshotId) {
        return parquetBasePath + "/snapshot_" + snapshotId + ".parquet";
    }
    
    private GenericRecord toGenericRecord(ValidationStatObservationParquet observation) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", observation.getId());
        record.put("identifier", observation.getIdentifier());
        record.put("snapshotID", observation.getSnapshotID());
        record.put("origin", observation.getOrigin());
        record.put("setSpec", observation.getSetSpec());
        record.put("metadataPrefix", observation.getMetadataPrefix());
        record.put("networkAcronym", observation.getNetworkAcronym());
        record.put("repositoryName", observation.getRepositoryName());
        record.put("institutionName", observation.getInstitutionName());
        record.put("isValid", observation.getIsValid());
        record.put("isTransformed", observation.getIsTransformed());
        record.put("validOccurrencesByRuleID", observation.getValidOccurrencesByRuleID());
        record.put("invalidOccurrencesByRuleID", observation.getInvalidOccurrencesByRuleID());
        record.put("validRulesIDList", observation.getValidRulesIDList());
        record.put("invalidRulesIDList", observation.getInvalidRulesIDList());
        return record;
    }
    
    /**
     * Convierte un objeto Avro a String (maneja Utf8 y String)
     */
    private String avroToString(Object obj) {
        if (obj == null) {
            return null;
        }
        return obj.toString(); // Funciona tanto para String como para Utf8
    }
    
    /**
     * Convierte una lista de Avro (con posibles elementos Utf8) a Lista de String
     */
    @SuppressWarnings("unchecked")
    private List<String> convertStringList(Object obj) {
        if (obj == null) {
            return null;
        }
        List<Object> avroList = (List<Object>) obj;
        return avroList.stream()
                .map(this::avroToString)
                .collect(Collectors.toList());
    }
    
    /**
     * Convierte un Map de Avro (con keys y valores Utf8) a Map<String, List<String>>
     */
    @SuppressWarnings("unchecked")
    private Map<String, List<String>> convertMapWithStringLists(Object obj) {
        if (obj == null) {
            return null;
        }
        Map<Object, Object> avroMap = (Map<Object, Object>) obj;
        Map<String, List<String>> result = new HashMap<>();
        
        for (Map.Entry<Object, Object> entry : avroMap.entrySet()) {
            String key = avroToString(entry.getKey());
            List<String> value = convertStringList(entry.getValue());
            result.put(key, value);
        }
        
        return result;
    }
    
    @SuppressWarnings("unchecked")
    private ValidationStatObservationParquet fromGenericRecord(GenericRecord record) {
        ValidationStatObservationParquet observation = new ValidationStatObservationParquet();
        observation.setId(avroToString(record.get("id")));
        observation.setIdentifier(avroToString(record.get("identifier")));
        observation.setSnapshotID((Long) record.get("snapshotID"));
        observation.setOrigin(avroToString(record.get("origin")));
        observation.setSetSpec(avroToString(record.get("setSpec")));
        observation.setMetadataPrefix(avroToString(record.get("metadataPrefix")));
        observation.setNetworkAcronym(avroToString(record.get("networkAcronym")));
        observation.setRepositoryName(avroToString(record.get("repositoryName")));
        observation.setInstitutionName(avroToString(record.get("institutionName")));
        observation.setIsValid((Boolean) record.get("isValid"));
        observation.setIsTransformed((Boolean) record.get("isTransformed"));
        observation.setValidOccurrencesByRuleID(convertMapWithStringLists(record.get("validOccurrencesByRuleID")));
        observation.setInvalidOccurrencesByRuleID(convertMapWithStringLists(record.get("invalidOccurrencesByRuleID")));
        observation.setValidRulesIDList(convertStringList(record.get("validRulesIDList")));
        observation.setInvalidRulesIDList(convertStringList(record.get("invalidRulesIDList")));
        return observation;
    }

    /**
     * NEW: Find all observations by snapshot ID - supports multi-file architecture
     * Reads from all data files in snapshot directory
     */
    public List<ValidationStatObservationParquet> findBySnapshotId(Long snapshotId) throws IOException {
        logger.debug("MULTI-FILE: Finding observations for snapshot {}", snapshotId);
        
        List<String> filePaths = getAllDataFilePaths(snapshotId);
        if (filePaths.isEmpty()) {
            logger.debug("MULTI-FILE: No data files found for snapshot {}", snapshotId);
            return Collections.emptyList();
        }
        
        List<ValidationStatObservationParquet> allResults = new ArrayList<>();
        
        // Read from all data files for this snapshot
        for (String filePath : filePaths) {
            File file = new File(filePath);
            if (!file.exists()) {
                logger.warn("MULTI-FILE: Data file does not exist: {}", filePath);
                continue;
            }
            
            logger.debug("MULTI-FILE: Reading from file: {}", filePath);
            
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath))
                    .withConf(hadoopConf)
                    .build()) {
                
                GenericRecord record;
                int fileRecordCount = 0;
                while ((record = reader.read()) != null) {
                    ValidationStatObservationParquet observation = fromGenericRecord(record);
                    allResults.add(observation);
                    fileRecordCount++;
                }
                
                logger.debug("MULTI-FILE: Read {} records from file: {}", fileRecordCount, filePath);
            }
        }
        
        logger.info("MULTI-FILE: Found {} total observations for snapshot {} across {} files", 
                   allResults.size(), snapshotId, filePaths.size());
        return allResults;
    }

    /**
     * Implementa paginación simple
     */
    public List<ValidationStatObservationParquet> findBySnapshotIdWithPagination(Long snapshotId, int page, int size) throws IOException {
        List<ValidationStatObservationParquet> observations = findBySnapshotId(snapshotId);
        
        int start = page * size;
        int end = Math.min(start + size, observations.size());
        
        if (start >= observations.size()) {
            return Collections.emptyList();
        }
        
        return observations.subList(start, end);
    }

    /**
     * Cuenta observaciones por snapshot ID
     */
    public long countBySnapshotId(Long snapshotId) throws IOException {
        return findBySnapshotId(snapshotId).size();
    }

    /**
     * OPTIMIZED: Saves all observations in Parquet files by snapshot using streaming and batch processing
     */
    public void saveAll(List<ValidationStatObservationParquet> observations) throws IOException {
        if (observations == null || observations.isEmpty()) {
            return;
        }
        
        logger.debug("Starting OPTIMIZED batch save of {} observations", observations.size());
        
        // Group by snapshot ID for batch processing
        Map<Long, List<ValidationStatObservationParquet>> groupedBySnapshot = observations.stream()
            .collect(Collectors.groupingBy(ValidationStatObservationParquet::getSnapshotID));
        
        // Process each snapshot group with optimized streaming approach
        for (Map.Entry<Long, List<ValidationStatObservationParquet>> entry : groupedBySnapshot.entrySet()) {
            Long snapshotId = entry.getKey();
            List<ValidationStatObservationParquet> snapshotObservations = entry.getValue();
            
            saveObservationsToParquetOptimized(snapshotId, snapshotObservations);
        }
        
        logger.info("OPTIMIZED: Successfully saved {} observations across {} snapshots", 
                   observations.size(), groupedBySnapshot.size());
    }
    
    /**
     * OPTIMIZED: High-performance append-mode saving with streaming and deduplication
     * Uses Set-based deduplication and batch writing to minimize memory usage and CPU load
     */
    private void saveObservationsToParquetOptimized(Long snapshotId, List<ValidationStatObservationParquet> newObservations) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        File parquetFile = new File(filePath);
        
        logger.debug("OPTIMIZED save for snapshot {}: {} new observations", snapshotId, newObservations.size());
        
        if (!parquetFile.exists()) {
            // No existing file - direct write (most efficient case)
            writeObservationsToParquetStreamOptimized(filePath, newObservations);
            logger.debug("OPTIMIZED: Direct write for new snapshot {} - {} observations", snapshotId, newObservations.size());
            return;
        }
        
        // File exists - need to merge efficiently without loading all data into memory
        Set<String> newObservationIds = newObservations.stream()
            .map(ValidationStatObservationParquet::getId)
            .collect(Collectors.toSet());
        
        logger.debug("OPTIMIZED: Merging with existing file, {} new unique IDs", newObservationIds.size());
        
        // Create temporary file for merge operation
        String tempFilePath = filePath + ".tmp";
        List<ValidationStatObservationParquet> mergedBatch = new ArrayList<>();
        int batchSize = 10000; // Process in batches to control memory usage
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath))
                .withConf(hadoopConf)
                .build();
             ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(tempFilePath))
                .withSchema(avroSchema)
                .withConf(hadoopConf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
            
            // Copy existing observations in batches, excluding duplicates
            GenericRecord record;
            int processedCount = 0;
            int skippedDuplicates = 0;
            
            while ((record = reader.read()) != null) {
                ValidationStatObservationParquet obs = fromGenericRecord(record);
                
                // Skip if this observation will be replaced by new data
                if (!newObservationIds.contains(obs.getId())) {
                    mergedBatch.add(obs);
                }else {
                    skippedDuplicates++;
                }
                
                // Write batch when it reaches size limit
                if (mergedBatch.size() >= batchSize) {
                    for (ValidationStatObservationParquet batchObs : mergedBatch) {
                        writer.write(toGenericRecord(batchObs));
                    }
                    processedCount += mergedBatch.size();
                    mergedBatch.clear(); // Free memory
                    
                    if (processedCount % 50000 == 0) {
                        logger.debug("OPTIMIZED: Processed {} existing observations", processedCount);
                    }
                }
            }
            
            // Write remaining existing observations
            for (ValidationStatObservationParquet batchObs : mergedBatch) {
                writer.write(toGenericRecord(batchObs));
            }
            processedCount += mergedBatch.size();
            
            // Append all new observations
            for (ValidationStatObservationParquet newObs : newObservations) {
                writer.write(toGenericRecord(newObs));
            }
            
            logger.debug("OPTIMIZED merge completed: {} existing (skipped {} duplicates), {} new, {} total", 
                        processedCount, skippedDuplicates, newObservations.size(), processedCount + newObservations.size());
        }
        
        // Replace original file with merged file atomically
        File originalFile = new File(filePath);
        File tempFile = new File(tempFilePath);
        
        if (!originalFile.delete()) {
            logger.warn("Could not delete original file: {}", filePath);
        }
        
        if (!tempFile.renameTo(originalFile)) {
            throw new IOException("Could not replace original file with merged data");
        }
        
        logger.debug("OPTIMIZED: File replacement completed for snapshot {}", snapshotId);
    }
    
    /**
     * OPTIMIZED: Stream-based writing with minimal memory footprint
     */
    private void writeObservationsToParquetStreamOptimized(String filePath, List<ValidationStatObservationParquet> observations) throws IOException {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                .withSchema(avroSchema)
                .withConf(hadoopConf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
            
            int written = 0;
            for (ValidationStatObservationParquet observation : observations) {
                GenericRecord record = toGenericRecord(observation);
                writer.write(record);
                written++;
                
                // Log progress for large batches
                if (written % 10000 == 0) {
                    logger.debug("OPTIMIZED: Written {} of {} observations", written, observations.size());
                }
            }
            
            logger.debug("OPTIMIZED: Stream write completed - {} observations written", written);
        }
    }
    
    /**
     * ULTRA-OPTIMIZED: Massive batch processing with memory monitoring and backpressure control
     * For datasets with millions of observations
     */
    public void saveAllMassiveOptimized(List<ValidationStatObservationParquet> observations) throws IOException {
        if (observations == null || observations.isEmpty()) {
            return;
        }
        
        logger.debug("Starting ULTRA-OPTIMIZED massive save of {} observations", observations.size());
        
        // Monitor memory usage
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        logger.debug("Available memory: {} MB", maxMemory / (1024 * 1024));
        
        // Group by snapshot ID
        Map<Long, List<ValidationStatObservationParquet>> groupedBySnapshot = observations.stream()
            .collect(Collectors.groupingBy(ValidationStatObservationParquet::getSnapshotID));
        
        int processedSnapshots = 0;
        for (Map.Entry<Long, List<ValidationStatObservationParquet>> entry : groupedBySnapshot.entrySet()) {
            Long snapshotId = entry.getKey();
            List<ValidationStatObservationParquet> snapshotObservations = entry.getValue();
            
            // Memory check before processing each snapshot
            long usedMemory = runtime.totalMemory() - runtime.freeMemory();
            double memoryUsagePercent = (double) usedMemory / maxMemory * 100;
            
            if (memoryUsagePercent > 80) {
                logger.warn("High memory usage detected: {:.2f}% - forcing garbage collection", memoryUsagePercent);
                System.gc();
                Thread.yield(); // Allow GC to run
            }
            
            // Process large snapshots in chunks
            if (snapshotObservations.size() > 50000) {
                saveObservationsInChunks(snapshotId, snapshotObservations);
            } else {
                saveObservationsToParquetOptimized(snapshotId, snapshotObservations);
            }
            
            processedSnapshots++;
            if (processedSnapshots % 10 == 0) {
                logger.info("ULTRA-OPTIMIZED: Processed {}/{} snapshots", processedSnapshots, groupedBySnapshot.size());
            }
        }
        
        logger.info("ULTRA-OPTIMIZED: Successfully completed massive save - {} observations across {} snapshots", 
                   observations.size(), groupedBySnapshot.size());
    }
    
    /**
     * CHUNK PROCESSING: Handles massive datasets by processing in memory-safe chunks
     */
    private void saveObservationsInChunks(Long snapshotId, List<ValidationStatObservationParquet> observations) throws IOException {
        int chunkSize = 25000; // Optimal chunk size for memory management
        int totalChunks = (observations.size() + chunkSize - 1) / chunkSize;
        
        logger.debug("Processing snapshot {} in {} chunks of max {} observations", snapshotId, totalChunks, chunkSize);
        
        for (int i = 0; i < totalChunks; i++) {
            int start = i * chunkSize;
            int end = Math.min(start + chunkSize, observations.size());
            List<ValidationStatObservationParquet> chunk = observations.subList(start, end);
            
            logger.debug("Processing chunk {}/{} for snapshot {} ({} observations)", i + 1, totalChunks, snapshotId, chunk.size());
            
            if (i == 0) {
                // First chunk - use normal optimized method
                saveObservationsToParquetOptimized(snapshotId, chunk);
            } else {
                // Subsequent chunks - append to existing file
                appendObservationsToParquet(snapshotId, chunk);
            }
            
            // Clear chunk reference to help GC
            chunk = null;
        }
        
        logger.debug("Completed chunked processing for snapshot {}", snapshotId);
    }
    
    /**
     * APPEND MODE: Efficiently appends observations to existing Parquet file
     */
    private void appendObservationsToParquet(Long snapshotId, List<ValidationStatObservationParquet> newObservations) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        String tempFilePath = filePath + ".append.tmp";
        
        // Read existing file and append new observations
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath))
                .withConf(hadoopConf)
                .build();
             ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(tempFilePath))
                .withSchema(avroSchema)
                .withConf(hadoopConf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .build()) {
            
            // Copy existing data
            GenericRecord record;
            int existingCount = 0;
            while ((record = reader.read()) != null) {
                writer.write(record);
                existingCount++;
            }
            
            // Append new observations
            for (ValidationStatObservationParquet obs : newObservations) {
                writer.write(toGenericRecord(obs));
            }
            
            logger.debug("Appended {} new observations to {} existing for snapshot {}", 
                        newObservations.size(), existingCount, snapshotId);
        }
        
        // Replace original file atomically
        File originalFile = new File(filePath);
        File tempFile = new File(tempFilePath);
        
        if (!originalFile.delete()) {
            logger.warn("Could not delete original file for append: {}", filePath);
        }
        
        if (!tempFile.renameTo(originalFile)) {
            throw new IOException("Could not replace file after append operation");
        }
    }

    /**
     * NEW: Deletes all observations for a snapshot (removes entire snapshot directory)
     * This works with the new multi-file architecture
     */
    public void deleteBySnapshotId(Long snapshotId) throws IOException {
        logger.info("CLEANUP: Deleting all data for snapshot {}", snapshotId);
        
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.info("CLEANUP: Snapshot directory does not exist: {}", snapshotDir);
            return;
        }
        
        // Find and delete all data files
        File[] dataFiles = dir.listFiles(file -> 
            file.isFile() && file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        int deletedCount = 0;
        if (dataFiles != null) {
            for (File file : dataFiles) {
                if (file.delete()) {
                    deletedCount++;
                    logger.debug("CLEANUP: Deleted data file: {}", file.getName());
                } else {
                    logger.warn("CLEANUP: Failed to delete data file: {}", file.getAbsolutePath());
                }
            }
        }
        
        // Try to remove the directory if it's empty
        if (dir.list() == null || dir.list().length == 0) {
            if (dir.delete()) {
                logger.info("CLEANUP: Deleted empty snapshot directory: {}", snapshotDir);
            } else {
                logger.warn("CLEANUP: Failed to delete snapshot directory: {}", snapshotDir);
            }
        }
        
        // Clear any buffered data and reset counters
        List<ValidationStatObservationParquet> buffer = snapshotBuffers.get(snapshotId);
        if (buffer != null && !buffer.isEmpty()) {
            logger.info("CLEANUP: Clearing {} buffered records for snapshot {}", buffer.size(), snapshotId);
            buffer.clear();
        }
        snapshotBuffers.remove(snapshotId);
        snapshotFileCounters.remove(snapshotId);
        
        logger.info("CLEANUP: Successfully deleted {} data files for snapshot {}", deletedCount, snapshotId);
    }

    /**
     * Elimina observación específica por ID
     */
    public void deleteById(String id, Long snapshotId) throws IOException {
        List<ValidationStatObservationParquet> observations = findBySnapshotId(snapshotId);
        
        List<ValidationStatObservationParquet> filteredObservations = observations.stream()
            .filter(obs -> !id.equals(obs.getId()))
            .collect(Collectors.toList());
        
        if (filteredObservations.size() < observations.size()) {
            // Re-escribir el archivo sin la observación eliminada
            String filePath = getParquetFilePath(snapshotId);
            writeObservationsToParquet(filePath, filteredObservations);
            logger.info("Eliminada observación {} del snapshot {}", id, snapshotId);
        }
    }

    /**
     * Copia datos de un snapshot a otro
     */
    public void copySnapshotData(Long originalSnapshotId, Long newSnapshotId) throws IOException {
        List<ValidationStatObservationParquet> originalData = findBySnapshotId(originalSnapshotId);
        
        if (!originalData.isEmpty()) {
            List<ValidationStatObservationParquet> copiedData = new ArrayList<>();
            for (ValidationStatObservationParquet obs : originalData) {
                ValidationStatObservationParquet copy = new ValidationStatObservationParquet();
                copyObservation(obs, copy);
                copy.setSnapshotID(newSnapshotId);
                copiedData.add(copy);
            }
            
            saveObservationsToParquetOptimized(newSnapshotId, copiedData);
            logger.info("Copiados datos del snapshot {} al snapshot {}", originalSnapshotId, newSnapshotId);
        }
    }
    
    private void writeObservationsToParquet(String filePath, List<ValidationStatObservationParquet> observations) throws IOException {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                .withSchema(avroSchema)
                .withConf(hadoopConf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            
            for (ValidationStatObservationParquet observation : observations) {
                GenericRecord record = toGenericRecord(observation);
                writer.write(record);
            }
        }
    }

    /**
     * Obtiene estadísticas agregadas por snapshot ID (VERSIÓN OPTIMIZADA MULTI-ARCHIVO)
     * Utiliza el query engine para evitar cargar todos los registros en memoria
     */
    public Map<String, Object> getAggregatedStats(Long snapshotId) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("MULTI-FILE STATS: Snapshot directory does not exist: {}", snapshotId);
            // Return empty statistics
            Map<String, Object> emptyStats = new HashMap<>();
            emptyStats.put("totalCount", 0L);
            emptyStats.put("validCount", 0L);
            emptyStats.put("transformedCount", 0L);
            emptyStats.put("validRuleCounts", new HashMap<String, Long>());
            emptyStats.put("invalidRuleCounts", new HashMap<String, Long>());
            return emptyStats;
        }
        
        // Get all data files in the snapshot directory
        File[] dataFiles = dir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles == null || dataFiles.length == 0) {
            logger.debug("MULTI-FILE STATS: No data files found for snapshot: {}", snapshotId);
            // Return empty statistics
            Map<String, Object> emptyStats = new HashMap<>();
            emptyStats.put("totalCount", 0L);
            emptyStats.put("validCount", 0L);
            emptyStats.put("transformedCount", 0L);
            emptyStats.put("validRuleCounts", new HashMap<String, Long>());
            emptyStats.put("invalidRuleCounts", new HashMap<String, Long>());
            return emptyStats;
        }
        
        // Aggregate statistics from all files
        AggregationFilter filter = new AggregationFilter(); // No filters, process all records
        
        // Initialize combined results
        long totalCount = 0;
        long validCount = 0;
        long transformedCount = 0;
        Map<String, Long> combinedValidRuleCounts = new HashMap<>();
        Map<String, Long> combinedInvalidRuleCounts = new HashMap<>();
        
        logger.debug("MULTI-FILE STATS: Processing {} data files for snapshot {}", dataFiles.length, snapshotId);
        
        // Process each data file and combine results
        for (File dataFile : dataFiles) {
            try {
                AggregationResult result = queryEngine.getAggregatedStatsFullyOptimized(dataFile.getAbsolutePath(), filter);
                
                // Combine counts
                totalCount += result.getTotalCount();
                validCount += result.getValidCount();
                transformedCount += result.getTransformedCount();
                
                // Combine rule counts
                for (Map.Entry<String, Long> entry : result.getValidRuleCounts().entrySet()) {
                    combinedValidRuleCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
                }
                
                for (Map.Entry<String, Long> entry : result.getInvalidRuleCounts().entrySet()) {
                    combinedInvalidRuleCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
                }
                
                logger.debug("MULTI-FILE STATS: Processed {} - {} records, {} valid", 
                           dataFile.getName(), result.getTotalCount(), result.getValidCount());
                           
            } catch (Exception e) {
                logger.error("MULTI-FILE STATS: Error processing file {} for snapshot {}", dataFile.getName(), snapshotId, e);
                // Continue with other files, don't fail completely
            }
        }
        
        // Convert result to compatible format
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalCount", totalCount);
        stats.put("validCount", validCount);
        stats.put("transformedCount", transformedCount);
        stats.put("validRuleCounts", combinedValidRuleCounts);
        stats.put("invalidRuleCounts", combinedInvalidRuleCounts);
        
        logger.info("MULTI-FILE STATS: Combined statistics for snapshot {}: {} total records, {} valid, {} transformed from {} files", 
                   snapshotId, totalCount, validCount, transformedCount, dataFiles.length);
        
        return stats;
    }

    /**
     * Obtiene estadísticas agregadas con filtros específicos (MULTI-ARCHIVO OPTIMIZADO)
     * @param snapshotId ID del snapshot
     * @param filter Filtros a aplicar (isValid, isTransformed, ruleIds, etc.)
     * @return Estadísticas agregadas filtradas
     */
    public Map<String, Object> getAggregatedStatsWithFilter(Long snapshotId, AggregationFilter filter) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("MULTI-FILE FILTER: Snapshot directory does not exist: {}", snapshotId);
            // Return empty statistics
            Map<String, Object> emptyStats = new HashMap<>();
            emptyStats.put("totalCount", 0L);
            emptyStats.put("validCount", 0L);
            emptyStats.put("transformedCount", 0L);
            emptyStats.put("validRuleCounts", new HashMap<String, Long>());
            emptyStats.put("invalidRuleCounts", new HashMap<String, Long>());
            return emptyStats;
        }
        
        // Get all data files in the snapshot directory
        File[] dataFiles = dir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles == null || dataFiles.length == 0) {
            logger.debug("MULTI-FILE FILTER: No data files found for snapshot: {}", snapshotId);
            // Return empty statistics
            Map<String, Object> emptyStats = new HashMap<>();
            emptyStats.put("totalCount", 0L);
            emptyStats.put("validCount", 0L);
            emptyStats.put("transformedCount", 0L);
            emptyStats.put("validRuleCounts", new HashMap<String, Long>());
            emptyStats.put("invalidRuleCounts", new HashMap<String, Long>());
            return emptyStats;
        }
        
        // Aggregate filtered statistics from all files
        long totalCount = 0;
        long validCount = 0;
        long transformedCount = 0;
        Map<String, Long> combinedValidRuleCounts = new HashMap<>();
        Map<String, Long> combinedInvalidRuleCounts = new HashMap<>();
        
        logger.debug("MULTI-FILE FILTER: Processing {} data files for snapshot {} with filters", dataFiles.length, snapshotId);
        
        // Process each data file and combine filtered results
        for (File dataFile : dataFiles) {
            try {
                AggregationResult result = queryEngine.getAggregatedStatsFullyOptimized(dataFile.getAbsolutePath(), filter);
                
                // Combine counts
                totalCount += result.getTotalCount();
                validCount += result.getValidCount();
                transformedCount += result.getTransformedCount();
                
                // Combine rule counts
                for (Map.Entry<String, Long> entry : result.getValidRuleCounts().entrySet()) {
                    combinedValidRuleCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
                }
                
                for (Map.Entry<String, Long> entry : result.getInvalidRuleCounts().entrySet()) {
                    combinedInvalidRuleCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
                }
                
            } catch (Exception e) {
                logger.error("MULTI-FILE FILTER: Error processing file {} for snapshot {}", dataFile.getName(), snapshotId, e);
                // Continue with other files, don't fail completely
            }
        }
        
        // Convert result to compatible format
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalCount", totalCount);
        stats.put("validCount", validCount);
        stats.put("transformedCount", transformedCount);
        stats.put("validRuleCounts", combinedValidRuleCounts);
        stats.put("invalidRuleCounts", combinedInvalidRuleCounts);
        
        logger.debug("MULTI-FILE FILTER: Combined filtered statistics for snapshot {}: {} records matched criteria from {} files", 
                   snapshotId, totalCount, dataFiles.length);
        
        return stats;
    }

    /**
     * Cuenta registros con filtros específicos (MULTI-ARCHIVO STREAMING - Sin cargar en memoria)
     * @param snapshotId ID del snapshot
     * @param filter Filtros a aplicar
     * @return Número de registros que cumplen los criterios
     */
    public long countRecordsWithFilter(Long snapshotId, AggregationFilter filter) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("MULTI-FILE STREAMING COUNT: Snapshot directory does not exist: {}", snapshotId);
            return 0L;
        }
        
        // Get all data files in the snapshot directory
        File[] dataFiles = dir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles == null || dataFiles.length == 0) {
            logger.debug("MULTI-FILE STREAMING COUNT: No data files found for snapshot: {}", snapshotId);
            return 0L;
        }
        
        logger.debug("MULTI-FILE STREAMING COUNT: Counting in {} data files for snapshot {}", dataFiles.length, snapshotId);
        
        long totalCount = 0;
        
        // Count records from all data files using streaming
        for (File dataFile : dataFiles) {
            try {
                long fileCount = countFileWithFilter(dataFile.getAbsolutePath(), filter);
                totalCount += fileCount;
                logger.debug("MULTI-FILE STREAMING COUNT: File {} has {} matching records", dataFile.getName(), fileCount);
            } catch (Exception e) {
                logger.error("MULTI-FILE STREAMING COUNT: Error counting records in file {} for snapshot {}", 
                           dataFile.getName(), snapshotId, e);
                // Continue with other files
            }
        }
        
        logger.debug("MULTI-FILE STREAMING COUNT: Total matching records for snapshot {}: {}", snapshotId, totalCount);
        return totalCount;
    }
    
    /**
     * Count records in a single file that match the filter, without loading all data in memory
     */
    private long countFileWithFilter(String filePath, AggregationFilter filter) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            logger.debug("COUNT FILE: File does not exist: {}", filePath);
            return 0L;
        }
        
        logger.debug("COUNT FILE: Counting matches in {}", file.getName());
        
        long count = 0;
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath))
                .withConf(hadoopConf)
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                ValidationStatObservationParquet observation = fromGenericRecord(record);
                
                // Apply filter and count matches
                if (matchesAggregationFilter(observation, filter)) {
                    count++;
                }
            }
        }
        
        logger.debug("COUNT FILE: File {} has {} matching records", file.getName(), count);
        return count;
    }

    /**
     * Búsqueda paginada con filtros (MULTI-ARCHIVO STREAMING - Sin cargar en memoria)
     * @param snapshotId ID del snapshot
     * @param filter Filtros a aplicar
     * @param page Número de página (base 0)
     * @param size Tamaño de página
     * @return Lista de observaciones que cumplen los criterios
     */
    public List<ValidationStatObservationParquet> findWithFilterAndPagination(Long snapshotId, 
                                                                              AggregationFilter filter, 
                                                                              int page, int size) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("MULTI-FILE STREAMING: Snapshot directory does not exist: {}", snapshotId);
            return Collections.emptyList();
        }
        
        // Get all data files in the snapshot directory
        File[] dataFiles = dir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles == null || dataFiles.length == 0) {
            logger.debug("MULTI-FILE STREAMING: No data files found for snapshot: {}", snapshotId);
            return Collections.emptyList();
        }
        
        // Sort files to ensure consistent ordering
        Arrays.sort(dataFiles, (f1, f2) -> f1.getName().compareTo(f2.getName()));
        
        logger.debug("MULTI-FILE STREAMING: Processing {} data files for snapshot {} with pagination streaming", 
                   dataFiles.length, snapshotId);
        
        // First, let's count total records that match the filter for debugging
        long totalMatchingRecords = countRecordsWithFilter(snapshotId, filter);
        logger.debug("PAGINATION DEBUG: Total records matching filter for snapshot {}: {}", snapshotId, totalMatchingRecords);
        
        List<ValidationStatObservationParquet> results = new ArrayList<>();
        int currentOffset = page * size;
        int collected = 0;
        
        logger.debug("PAGINATION DEBUG: Starting pagination - page: {}, size: {}, currentOffset: {}, totalMatching: {}", 
                   page, size, currentOffset, totalMatchingRecords);
        
        // Process each file with streaming until we have enough results
        for (File dataFile : dataFiles) {
            if (collected >= size) {
                logger.debug("PAGINATION DEBUG: Breaking early - collected {} >= size {}", collected, size);
                break; // We have enough results
            }
            
            try {
                logger.debug("PAGINATION DEBUG: Processing file {} (collected: {}, needed: {}, currentOffset: {})", 
                           dataFile.getName(), collected, size, currentOffset);
                
                // Stream through this file with filters applied
                List<ValidationStatObservationParquet> fileResults = streamFileWithFilter(
                    dataFile.getAbsolutePath(), filter, currentOffset, size - collected);
                
                // Add the results we got
                results.addAll(fileResults);
                collected = results.size();
                
                // Update offset for next file (subtract what we used from this file)
                int recordsFromThisFile = fileResults.size();
                if (currentOffset > 0) {
                    // We were still in offset mode, now reduce the offset
                    currentOffset = Math.max(0, currentOffset - recordsFromThisFile);
                    logger.debug("PAGINATION DEBUG: Used {} records from {}, new currentOffset: {}", 
                               recordsFromThisFile, dataFile.getName(), currentOffset);
                } else {
                    // We were in collection mode
                    logger.debug("PAGINATION DEBUG: Collected {} records from {}, total collected: {}", 
                               recordsFromThisFile, dataFile.getName(), collected);
                }
                
                logger.debug("MULTI-FILE STREAMING: File {} contributed {} results (total collected: {})", 
                           dataFile.getName(), fileResults.size(), collected);
                           
            } catch (Exception e) {
                logger.error("MULTI-FILE STREAMING: Error processing file {} for snapshot {}", 
                           dataFile.getName(), snapshotId, e);
                // Continue with other files
            }
        }
        
        logger.debug("MULTI-FILE STREAMING: Final results - {} records from page {} for snapshot {}", 
                   results.size(), page, snapshotId);
        
        return results;
    }
    
    /**
     * Helper method to check if an observation matches the aggregation filter
     */
    private boolean matchesAggregationFilter(ValidationStatObservationParquet obs, AggregationFilter filter) {
        if (filter == null) {
            return true;
        }
        
        // Check isValid filter
        if (filter.getIsValid() != null && !filter.getIsValid().equals(obs.getIsValid())) {
            return false;
        }
        
        // Check isTransformed filter
        if (filter.getIsTransformed() != null && !filter.getIsTransformed().equals(obs.getIsTransformed())) {
            return false;
        }
        
        // Check identifier filter
        if (filter.getRecordOAIId() != null && !filter.getRecordOAIId().equals(obs.getIdentifier())) {
            return false;
        }
        
        // Check valid rules filter
        if (filter.getValidRulesFilter() != null) {
            List<String> validRules = obs.getValidRulesIDList();
            if (validRules == null || !validRules.contains(filter.getValidRulesFilter())) {
                return false;
            }
        }
        
        // Check invalid rules filter
        if (filter.getInvalidRulesFilter() != null) {
            List<String> invalidRules = obs.getInvalidRulesIDList();
            if (invalidRules == null || !invalidRules.contains(filter.getInvalidRulesFilter())) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Stream a single file with filters applied, with efficient offset/limit handling
     * Only reads what's needed - no memory overload for millions of records
     */
    private List<ValidationStatObservationParquet> streamFileWithFilter(String filePath, AggregationFilter filter, 
                                                                       int offset, int limit) throws IOException {
        List<ValidationStatObservationParquet> results = new ArrayList<>();
        
        File file = new File(filePath);
        if (!file.exists()) {
            logger.debug("STREAM FILE: File does not exist: {}", filePath);
            return results;
        }
        
        logger.debug("STREAM FILE: Processing {} with offset={}, limit={}", file.getName(), offset, limit);
        
        int filteredCount = 0; // Count of records that match the filter
        int collected = 0;     // Count of records collected for results
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath))
                .withConf(hadoopConf)
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null && collected < limit) {
                ValidationStatObservationParquet observation = fromGenericRecord(record);
                
                // Apply filter first
                if (matchesAggregationFilter(observation, filter)) {
                    // This record matches the filter
                    if (filteredCount >= offset) {
                        // We've skipped enough records, start collecting
                        results.add(observation);
                        collected++;
                    }
                    filteredCount++;
                }
            }
        }
        
        logger.debug("STREAM FILE: File {} returned {} results (filtered {} total, skipped {} for offset)", 
                   file.getName(), results.size(), filteredCount, offset);
        
        return results;
    }

    /**
     * Obtiene conteos de ocurrencias de reglas (MULTI-ARCHIVO OPTIMIZADO)
     */
    public Map<String, Long> getRuleOccurrenceCounts(Long snapshotId, String ruleId, boolean valid) throws IOException {
        List<ValidationStatObservationParquet> observations = findBySnapshotId(snapshotId);
        Map<String, Long> occurrenceCounts = new HashMap<>();
        
        logger.debug("RULE OCCURRENCES: Processing {} observations for rule {} (valid: {})", observations.size(), ruleId, valid);
        
        for (ValidationStatObservationParquet obs : observations) {
            Map<String, List<String>> occurrenceMap = valid ? 
                obs.getValidOccurrencesByRuleID() : 
                obs.getInvalidOccurrencesByRuleID();
                
            if (occurrenceMap != null && occurrenceMap.containsKey(ruleId)) {
                List<String> occurrences = occurrenceMap.get(ruleId);
                if (occurrences != null) {
                    for (String occurrence : occurrences) {
                        occurrenceCounts.put(occurrence, occurrenceCounts.getOrDefault(occurrence, 0L) + 1);
                    }
                }
            }
        }
        
        logger.debug("RULE OCCURRENCES: Found {} unique occurrences for rule {} (valid: {})", occurrenceCounts.size(), ruleId, valid);
        
        return occurrenceCounts;
    }

    private void copyObservation(ValidationStatObservationParquet source, ValidationStatObservationParquet target) {
        target.setId(source.getId());
        target.setIdentifier(source.getIdentifier());
        target.setSnapshotID(source.getSnapshotID());
        target.setOrigin(source.getOrigin());
        target.setSetSpec(source.getSetSpec());
        target.setMetadataPrefix(source.getMetadataPrefix());
        target.setNetworkAcronym(source.getNetworkAcronym());
        target.setRepositoryName(source.getRepositoryName());
        target.setInstitutionName(source.getInstitutionName());
        target.setIsValid(source.getIsValid());
        target.setIsTransformed(source.getIsTransformed());
        target.setValidOccurrencesByRuleID(source.getValidOccurrencesByRuleID());
        target.setInvalidOccurrencesByRuleID(source.getInvalidOccurrencesByRuleID());
        target.setValidRulesIDList(source.getValidRulesIDList());
        target.setInvalidRulesIDList(source.getInvalidRulesIDList());
    }
    
    /**
     * Obtiene la ruta del archivo Parquet para un snapshot
     */
    public String getSnapshotFilePath(Long snapshotId) {
        return parquetBasePath + "/snapshot_" + snapshotId + ".parquet";
    }
    
    /**
     * BUFFERED: Multi-file streaming with intelligent buffering
     * Accumulates records until reaching recordsPerFile limit (e.g., 10000)
     * Only writes when buffer is full or explicitly flushed
     * 
     * CORRECTED BEHAVIOR:
     * - 1000 records arrive → stored in buffer
     * - 1000 more arrive → buffer has 2000, still waiting
     * - ... continues until buffer reaches 10000
     * - At 10000 → writes one file and clears buffer
     */
    public void saveAllImmediate(List<ValidationStatObservationParquet> observations) throws IOException {
        if (observations == null || observations.isEmpty()) {
            logger.debug("SAVE: No observations to save (null or empty)");
            return;
        }
        
        logger.debug("SAVE: Processing {} observations with intelligent buffering", observations.size());
        
        // Group by snapshot ID to handle multiple snapshots in one batch
        Map<Long, List<ValidationStatObservationParquet>> groupedBySnapshot = observations.stream()
            .collect(Collectors.groupingBy(ValidationStatObservationParquet::getSnapshotID));
        
        logger.debug("SAVE: Grouped observations into {} snapshots: {}", groupedBySnapshot.size(), groupedBySnapshot.keySet());
        
        // Process each snapshot with buffering approach
        for (Map.Entry<Long, List<ValidationStatObservationParquet>> entry : groupedBySnapshot.entrySet()) {
            Long snapshotId = entry.getKey();
            List<ValidationStatObservationParquet> snapshotObservations = entry.getValue();
            
            logger.debug("SAVE: Processing {} observations for snapshot {}", snapshotObservations.size(), snapshotId);
            addToBufferAndFlushIfNeeded(snapshotId, snapshotObservations);
        }
        
        logger.debug("SAVE: Completed processing {} observations", observations.size());
    }

    /**
     * BUFFER MANAGEMENT: Adds observations to buffer and flushes when reaching recordsPerFile limit
     * This ensures exactly recordsPerFile records per file (e.g., 10000)
     */
    private synchronized void addToBufferAndFlushIfNeeded(Long snapshotId, List<ValidationStatObservationParquet> newObservations) throws IOException {
        // Get or create buffer for this snapshot
        List<ValidationStatObservationParquet> buffer = snapshotBuffers.computeIfAbsent(snapshotId, k -> new ArrayList<>());
        
        // Add new observations to buffer
        buffer.addAll(newObservations);
        
        logger.debug("BUFFER: Added {} observations to snapshot {} buffer (now {} total)", 
                   newObservations.size(), snapshotId, buffer.size());
        
        // Check if buffer should be flushed
        while (buffer.size() >= recordsPerFile) {
            // Extract exactly recordsPerFile records for writing
            List<ValidationStatObservationParquet> recordsToWrite = new ArrayList<>(buffer.subList(0, recordsPerFile));
            
            // Remove these records from buffer
            buffer.subList(0, recordsPerFile).clear();
            
            // Write the file
            writeBufferToFile(snapshotId, recordsToWrite);
            
            logger.info("BUFFER: Flushed {} records for snapshot {} (buffer remaining: {})", 
                       recordsToWrite.size(), snapshotId, buffer.size());
        }
        
        // Log current buffer state
        if (buffer.size() > 0) {
            logger.debug("BUFFER: Snapshot {} has {} records waiting for flush threshold ({})", 
                       snapshotId, buffer.size(), recordsPerFile);
        }
    }

    /**
     * CORE: Write exactly recordsPerFile records to a new file
     * This creates files with exactly the configured number of records
     */
    private void writeBufferToFile(Long snapshotId, List<ValidationStatObservationParquet> observations) throws IOException {
        if (observations == null || observations.isEmpty()) {
            return;
        }
        
        // Ensure snapshot directory exists
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        Files.createDirectories(Paths.get(snapshotDir));
        
        // Get next file index for this snapshot
        int nextFileIndex = snapshotFileCounters.getOrDefault(snapshotId, 0);
        String filePath = getDataFilePath(snapshotId, nextFileIndex);
        
        logger.info("BUFFER: Writing {} records to file: {}", observations.size(), filePath);
        
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                new org.apache.hadoop.fs.Path(filePath))
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(hadoopConf)
                .withSchema(avroSchema)
                .build()) {
            
            // Write all observations in this batch
            for (ValidationStatObservationParquet obs : observations) {
                writer.write(toGenericRecord(obs));
            }
            
            logger.info("BUFFER: Successfully wrote {} records to {}", observations.size(), filePath);
        }
        
        // Update file counter for this snapshot
        snapshotFileCounters.put(snapshotId, nextFileIndex + 1);
    }
    
    /**
     * CLEANUP: Clean snapshot directory when starting a new collection
     * Removes all existing data files to prevent mixing old and new data
     */
    private void cleanSnapshotDirectory(Long snapshotId) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("CLEANUP: Snapshot directory does not exist, creating: {}", snapshotDir);
            Files.createDirectories(Paths.get(snapshotDir));
            return;
        }
        
        // Find and remove all existing data files
        File[] existingFiles = dir.listFiles(file -> 
            file.isFile() && file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (existingFiles != null && existingFiles.length > 0) {
            logger.info("CLEANUP: Removing {} existing data files from snapshot {} directory", 
                       existingFiles.length, snapshotId);
            
            for (File file : existingFiles) {
                if (file.delete()) {
                    logger.debug("CLEANUP: Deleted existing file: {}", file.getName());
                } else {
                    logger.warn("CLEANUP: Failed to delete file: {}", file.getAbsolutePath());
                }
            }
        } else {
            logger.debug("CLEANUP: No existing data files found in snapshot {} directory", snapshotId);
        }
        
        // Reset file counter for this snapshot
        snapshotFileCounters.put(snapshotId, 0);
        logger.info("CLEANUP: Cleaned snapshot {} directory and reset file counter", snapshotId);
    }
    
    /**
     * FORCE FLUSH: Write any remaining buffered records (useful for shutdown or testing)
     */
    public synchronized void flushAllBuffers() throws IOException {
        logger.info("BUFFER: Force flushing all snapshot buffers");
        
        for (Map.Entry<Long, List<ValidationStatObservationParquet>> entry : snapshotBuffers.entrySet()) {
            Long snapshotId = entry.getKey();
            List<ValidationStatObservationParquet> buffer = entry.getValue();
            
            if (!buffer.isEmpty()) {
                logger.info("BUFFER: Force flushing {} remaining records for snapshot {}", buffer.size(), snapshotId);
                writeBufferToFile(snapshotId, new ArrayList<>(buffer));
                buffer.clear();
            }
        }
        
        logger.info("BUFFER: All buffers flushed");
    }
    
    /**
     * PUBLIC API: Manually clean a snapshot directory (useful for starting new validation)
     * This will remove all existing data files and reset counters for the specified snapshot
     */
    public synchronized void cleanSnapshot(Long snapshotId) throws IOException {
        logger.info("MANUAL CLEANUP: Cleaning snapshot {} by user request", snapshotId);
        
        // Clear any buffered data for this snapshot
        List<ValidationStatObservationParquet> buffer = snapshotBuffers.get(snapshotId);
        if (buffer != null && !buffer.isEmpty()) {
            logger.warn("MANUAL CLEANUP: Discarding {} buffered records for snapshot {}", buffer.size(), snapshotId);
            buffer.clear();
        }
        
        // Clean the directory
        cleanSnapshotDirectory(snapshotId);
        
        logger.info("MANUAL CLEANUP: Completed cleaning snapshot {}", snapshotId);
    }
    
}
