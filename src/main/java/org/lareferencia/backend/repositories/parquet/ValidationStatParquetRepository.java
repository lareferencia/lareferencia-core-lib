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
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;
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
import java.util.concurrent.ConcurrentHashMap;

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

    private Schema avroSchema;
    private Configuration hadoopConf;
    
    // Counter to track next file index per snapshot
    private final Map<Long, Integer> snapshotFileCounters = new ConcurrentHashMap<>();
    
    // BUFFER SYSTEM: Accumulate records until reaching recordsPerFile limit
    private final Map<Long, List<ValidationStatObservationParquet>> snapshotBuffers = new ConcurrentHashMap<>();
    
    private final Map<String, AggregationResult> queryCache = new ConcurrentHashMap<>();
    
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
     * Convierte un objeto Avro a String (maneja Utf8, CharSequence y String)
     * Optimizado para manejar todos los tipos comunes en Avro/Parquet
     */
    private String avroToString(Object obj) {
        if (obj == null) {
            return null;
        }
        
        // Handle Avro Utf8 objects specifically
        if (obj instanceof org.apache.avro.util.Utf8) {
            return obj.toString();
        }
        
        // Handle CharSequence (includes String and other string-like objects)
        if (obj instanceof CharSequence) {
            return obj.toString();
        }
        
        // Fallback for any other type
        return obj.toString();
    }
    
    /**
     * Helper method para buscar una clave en un mapa que puede contener claves Utf8 o String
     * Maneja las diferencias de tipos entre Java String y Avro Utf8
     */
    private Object findValueInAvroMap(Map<Object, Object> map, String searchKey) {
        if (map == null || searchKey == null) {
            return null;
        }
        
        // Try direct lookup first (String key)
        Object value = map.get(searchKey);
        if (value != null) {
            return value;
        }
        
        // Try with Utf8 key
        try {
            org.apache.avro.util.Utf8 utf8Key = new org.apache.avro.util.Utf8(searchKey);
            value = map.get(utf8Key);
            if (value != null) {
                return value;
            }
        } catch (Exception e) {
            // Ignore errors creating Utf8 key
        }
        
        // Fallback: iterate and compare string representations
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            if (searchKey.equals(avroToString(entry.getKey()))) {
                return entry.getValue();
            }
        }
        
        return null;
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
     * 
     * WARNING: This method loads ALL records into memory - use with caution for large datasets!
     * For large datasets (millions of records), prefer using:
     * - findBySnapshotIdWithPagination() for paginated access
     * - findWithFilterAndPagination() for filtered access
     * - countBySnapshotId() for just counting records
     * - getAggregatedStats() for statistics without loading records
     */
    public List<ValidationStatObservationParquet> findBySnapshotId(Long snapshotId) throws IOException {
        logger.warn("MEMORY WARNING: findBySnapshotId loads ALL records into memory - consider using pagination for large datasets");
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
            
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
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
     * STREAMING ALTERNATIVE: Procesa todos los registros de un snapshot con un callback
     * Evita cargar millones de registros en memoria de una vez
     * @param snapshotId ID del snapshot
     * @param processor Función que procesa cada registro individualmente
     * @return Número total de registros procesados
     */
    public long processAllRecords(Long snapshotId, java.util.function.Consumer<ValidationStatObservationParquet> processor) throws IOException {
        logger.debug("STREAMING PROCESSOR: Processing all records for snapshot {} with callback", snapshotId);
        
        List<String> filePaths = getAllDataFilePaths(snapshotId);
        if (filePaths.isEmpty()) {
            logger.debug("STREAMING PROCESSOR: No data files found for snapshot {}", snapshotId);
            return 0;
        }
        
        long totalProcessed = 0;
        
        // Process each file with streaming
        for (String filePath : filePaths) {
            File file = new File(filePath);
            if (!file.exists()) {
                logger.warn("STREAMING PROCESSOR: Data file does not exist: {}", filePath);
                continue;
            }
            
            logger.debug("STREAMING PROCESSOR: Processing file: {}", filePath);
            
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
                    .withConf(hadoopConf)
                    .build()) {
                
                GenericRecord record;
                long fileRecordCount = 0;
                while ((record = reader.read()) != null) {
                    ValidationStatObservationParquet observation = fromGenericRecord(record);
                    processor.accept(observation); // Process each record individually
                    fileRecordCount++;
                    totalProcessed++;
                }
                
                logger.debug("STREAMING PROCESSOR: Processed {} records from file: {}", fileRecordCount, filePath);
            }
        }
        
        logger.info("STREAMING PROCESSOR: Processed {} total records for snapshot {} across {} files", 
                   totalProcessed, snapshotId, filePaths.size());
        return totalProcessed;
    }

    /**
     * STREAMING: Implementa paginación eficiente sin cargar datos en memoria
     * Utiliza el método de streaming ya implementado para evitar memory overflow
     */
    public List<ValidationStatObservationParquet> findBySnapshotIdWithPagination(Long snapshotId, int page, int size) throws IOException {
        // Use the existing streaming pagination method with no filters
        AggregationFilter noFilter = new AggregationFilter();
        return findWithFilterAndPagination(snapshotId, noFilter, page, size);
    }

    /**
     * STREAMING: Cuenta observaciones por snapshot ID sin cargar datos en memoria
     * Utiliza el método de streaming ya implementado para evitar memory overflow
     */
    public long countBySnapshotId(Long snapshotId) throws IOException {
        // Use the existing streaming count method with no filters
        AggregationFilter noFilter = new AggregationFilter();
        return countRecordsWithFilter(snapshotId, noFilter);
    }

    /**
     * MULTI-FILE: Saves all observations using the new multi-file architecture with intelligent buffering
     * This method delegates to saveAllImmediate() which uses the modern multi-file approach
     */
    public void saveAll(List<ValidationStatObservationParquet> observations) throws IOException {
        // Delegate to the multi-file implementation
        saveAllImmediate(observations);
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
     * STREAMING: Elimina observación específica por ID usando arquitectura multi-archivo
     * Procesa archivo por archivo sin cargar todos los datos en memoria
     */
    public void deleteById(String id, Long snapshotId) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("DELETE BY ID: Snapshot directory does not exist: {}", snapshotId);
            return;
        }
        
        // Get all data files in the snapshot directory
        File[] dataFiles = dir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles == null || dataFiles.length == 0) {
            logger.debug("DELETE BY ID: No data files found for snapshot: {}", snapshotId);
            return;
        }
        
        boolean recordFound = false;
        List<String> tempFilePaths = new ArrayList<>();
        
        logger.info("DELETE BY ID: Searching and filtering record {} in {} files for snapshot {}", 
                   id, dataFiles.length, snapshotId);
        
        // First pass: Create filtered temporary files
        for (File dataFile : dataFiles) {
            try {
                String tempFilePath = dataFile.getAbsolutePath() + ".tmp";
                tempFilePaths.add(tempFilePath);
                
                boolean foundInThisFile = filterRecordFromFile(dataFile.getAbsolutePath(), tempFilePath, id);
                if (foundInThisFile) {
                    recordFound = true;
                    logger.debug("DELETE BY ID: Found and filtered record {} from file {}", id, dataFile.getName());
                }
                
            } catch (Exception e) {
                logger.error("DELETE BY ID: Error processing file {} for snapshot {}", 
                           dataFile.getName(), snapshotId, e);
                // Clean up temp files and abort
                cleanupTempFiles(tempFilePaths);
                throw new IOException("Failed to delete record " + id + " from snapshot " + snapshotId, e);
            }
        }
        
        if (recordFound) {
            // Second pass: Replace original files with filtered ones
            for (int i = 0; i < dataFiles.length; i++) {
                File originalFile = dataFiles[i];
                String tempFilePath = tempFilePaths.get(i);
                File tempFile = new File(tempFilePath);
                
                if (tempFile.exists()) {
                    // Replace original with filtered version
                    if (!originalFile.delete()) {
                        logger.error("DELETE BY ID: Failed to delete original file: {}", originalFile.getAbsolutePath());
                        cleanupTempFiles(tempFilePaths);
                        throw new IOException("Failed to delete original file during record deletion");
                    }
                    
                    if (!tempFile.renameTo(originalFile)) {
                        logger.error("DELETE BY ID: Failed to rename temp file: {}", tempFilePath);
                        throw new IOException("Failed to rename temp file during record deletion");
                    }
                    
                    logger.debug("DELETE BY ID: Replaced file {} with filtered version", originalFile.getName());
                }
            }
            
            logger.info("DELETE BY ID: Successfully deleted record {} from snapshot {} using streaming approach", id, snapshotId);
        } else {
            // Clean up temp files if record was not found
            cleanupTempFiles(tempFilePaths);
            logger.info("DELETE BY ID: Record {} not found in snapshot {}", id, snapshotId);
        }
    }
    
    /**
     * STREAMING HELPER: Filtra un registro específico de un archivo sin cargar todo en memoria
     * @param sourceFilePath Archivo fuente
     * @param targetFilePath Archivo destino filtrado
     * @param recordIdToDelete ID del registro a eliminar
     * @return true si se encontró y filtró el registro
     */
    private boolean filterRecordFromFile(String sourceFilePath, String targetFilePath, String recordIdToDelete) throws IOException {
        boolean recordFound = false;
        int totalRecords = 0;
        int writtenRecords = 0;
        
        File sourceFile = new File(sourceFilePath);
        if (!sourceFile.exists()) {
            logger.debug("FILTER RECORD: Source file does not exist: {}", sourceFilePath);
            return false;
        }
        
        logger.debug("FILTER RECORD: Filtering record {} from {} to {}", 
                   recordIdToDelete, sourceFile.getName(), targetFilePath);
        
        org.apache.hadoop.fs.Path targetPath = new org.apache.hadoop.fs.Path(targetFilePath);
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(new Path(sourceFilePath), hadoopConf))
                .withConf(hadoopConf)
                .build();
             ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(targetPath, hadoopConf))
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(hadoopConf)
                .withSchema(avroSchema)
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                totalRecords++;
                
                String recordId = avroToString(record.get("id"));
                if (recordIdToDelete.equals(recordId)) {
                    recordFound = true;
                    logger.debug("FILTER RECORD: Found target record {} to delete", recordIdToDelete);
                    // Skip this record (don't write it)
                } else {
                    // Keep this record
                    writer.write(record);
                    writtenRecords++;
                }
            }
        }
        
        logger.debug("FILTER RECORD: Processed {} records, wrote {} records, found target: {}", 
                   totalRecords, writtenRecords, recordFound);
        
        return recordFound;
    }
    
    /**
     * CLEANUP HELPER: Limpia archivos temporales en caso de error
     */
    private void cleanupTempFiles(List<String> tempFilePaths) {
        for (String tempFilePath : tempFilePaths) {
            File tempFile = new File(tempFilePath);
            if (tempFile.exists()) {
                if (tempFile.delete()) {
                    logger.debug("CLEANUP: Deleted temp file: {}", tempFilePath);
                } else {
                    logger.warn("CLEANUP: Failed to delete temp file: {}", tempFilePath);
                }
            }
        }
    }

    /**
     * STREAMING: Copia datos de un snapshot a otro usando la arquitectura multi-archivo
     * Procesa archivo por archivo sin cargar todos los datos en memoria
     */
    public void copySnapshotData(Long originalSnapshotId, Long newSnapshotId) throws IOException {
        String originalSnapshotDir = getSnapshotDirectoryPath(originalSnapshotId);
        File originalDir = new File(originalSnapshotDir);
        
        if (!originalDir.exists()) {
            logger.debug("COPY SNAPSHOT: Original snapshot directory does not exist: {}", originalSnapshotId);
            return;
        }
        
        // Get all data files in the original snapshot directory
        File[] originalDataFiles = originalDir.listFiles(file -> 
            file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (originalDataFiles == null || originalDataFiles.length == 0) {
            logger.debug("COPY SNAPSHOT: No data files found in original snapshot: {}", originalSnapshotId);
            return;
        }
        
        // Ensure target snapshot directory exists
        String newSnapshotDir = getSnapshotDirectoryPath(newSnapshotId);
        Files.createDirectories(Paths.get(newSnapshotDir));
        
        // Clear any existing data in target snapshot
        cleanSnapshot(newSnapshotId);
        
        logger.info("COPY SNAPSHOT: Copying {} data files from snapshot {} to snapshot {} using streaming", 
                   originalDataFiles.length, originalSnapshotId, newSnapshotId);
        
        int processedFiles = 0;
        long totalCopiedRecords = 0;
        
        // Process each file individually with streaming
        for (File originalFile : originalDataFiles) {
            try {
                long copiedRecords = copyFileWithSnapshotUpdate(
                    originalFile.getAbsolutePath(), newSnapshotId, processedFiles);
                
                totalCopiedRecords += copiedRecords;
                processedFiles++;
                
                logger.debug("COPY SNAPSHOT: Copied {} records from file {} ({}/{})", 
                           copiedRecords, originalFile.getName(), processedFiles, originalDataFiles.length);
                           
            } catch (Exception e) {
                logger.error("COPY SNAPSHOT: Error copying file {} from snapshot {} to {}", 
                           originalFile.getName(), originalSnapshotId, newSnapshotId, e);
                throw new IOException("Failed to copy snapshot data", e);
            }
        }
        
        logger.info("COPY SNAPSHOT: Successfully copied {} records from snapshot {} to {} across {} files using streaming", 
                   totalCopiedRecords, originalSnapshotId, newSnapshotId, processedFiles);
    }
    
    /**
     * STREAMING HELPER: Copia un archivo actualizando el snapshot ID sin cargar todo en memoria
     * @param sourceFilePath Archivo fuente
     * @param newSnapshotId Nuevo snapshot ID
     * @param fileIndex Índice del archivo en el nuevo snapshot
     * @return Número de registros copiados
     */
    private long copyFileWithSnapshotUpdate(String sourceFilePath, Long newSnapshotId, int fileIndex) throws IOException {
        File sourceFile = new File(sourceFilePath);
        if (!sourceFile.exists()) {
            logger.debug("COPY FILE: Source file does not exist: {}", sourceFilePath);
            return 0;
        }
        
        String targetFilePath = getDataFilePath(newSnapshotId, fileIndex);
        org.apache.hadoop.fs.Path targetPath = new org.apache.hadoop.fs.Path(targetFilePath);
        
        logger.debug("COPY FILE: Streaming copy from {} to {} with snapshot ID update", 
                   sourceFile.getName(), targetFilePath);
        
        long copiedRecords = 0;
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(new Path(sourceFilePath), hadoopConf))
                .withConf(hadoopConf)
                .build();
             ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(targetPath, hadoopConf))
                .withWriteMode(ParquetFileWriter.Mode.CREATE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(hadoopConf)
                .withSchema(avroSchema)
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                // Create a new record with updated snapshot ID
                GenericRecord newRecord = new GenericData.Record(avroSchema);
                
                // Copy all fields except snapshotID
                for (Schema.Field field : avroSchema.getFields()) {
                    if ("snapshotID".equals(field.name())) {
                        newRecord.put("snapshotID", newSnapshotId);
                    } else {
                        newRecord.put(field.name(), record.get(field.name()));
                    }
                }
                
                writer.write(newRecord);
                copiedRecords++;
            }
        }
        
        // Update file counter for the new snapshot
        snapshotFileCounters.put(newSnapshotId, fileIndex + 1);
        
        logger.debug("COPY FILE: Copied {} records from {} to {}", 
                   copiedRecords, sourceFile.getName(), targetFilePath);
        
        return copiedRecords;
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
                AggregationResult result = getAggregatedStatsOptimized(dataFile.getAbsolutePath(), filter);
                
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
                AggregationResult result = getAggregatedStatsOptimized(dataFile.getAbsolutePath(), filter);
                
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
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
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
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
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
     * STREAMING: Obtiene conteos de ocurrencias de reglas sin cargar en memoria
     * Procesa archivo por archivo de forma secuencial para millones de registros
     */
    public Map<String, Long> getRuleOccurrenceCounts(Long snapshotId, String ruleId, boolean valid) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("RULE OCCURRENCES STREAMING: Snapshot directory does not exist: {}", snapshotId);
            return new HashMap<>();
        }
        
        // Get all data files in the snapshot directory
        File[] dataFiles = dir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles == null || dataFiles.length == 0) {
            logger.debug("RULE OCCURRENCES STREAMING: No data files found for snapshot: {}", snapshotId);
            return new HashMap<>();
        }
        
        Map<String, Long> occurrenceCounts = new HashMap<>();
        
        logger.debug("RULE OCCURRENCES STREAMING: Processing {} data files for rule {} (valid: {})", 
                   dataFiles.length, ruleId, valid);
        
        // Process each file sequentially without loading all data in memory
        for (File dataFile : dataFiles) {
            try {
                Map<String, Long> fileOccurrences = getRuleOccurrenceCountsFromFile(
                    dataFile.getAbsolutePath(), ruleId, valid);
                
                // Merge counts from this file
                for (Map.Entry<String, Long> entry : fileOccurrences.entrySet()) {
                    occurrenceCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
                }
                
                logger.debug("RULE OCCURRENCES STREAMING: File {} contributed {} unique occurrences", 
                           dataFile.getName(), fileOccurrences.size());
                           
            } catch (Exception e) {
                logger.error("RULE OCCURRENCES STREAMING: Error processing file {} for snapshot {}", 
                           dataFile.getName(), snapshotId, e);
                // Continue with other files
            }
        }
        
        logger.info("RULE OCCURRENCES STREAMING: Found {} unique occurrences for rule {} (valid: {}) across {} files", 
                   occurrenceCounts.size(), ruleId, valid, dataFiles.length);
        
        return occurrenceCounts;
    }
    
    /**
     * STREAMING HELPER: Procesa conteos de ocurrencias de reglas de un solo archivo
     * Sin cargar todos los registros en memoria
     */
    private Map<String, Long> getRuleOccurrenceCountsFromFile(String filePath, String ruleId, boolean valid) throws IOException {
        Map<String, Long> fileOccurrenceCounts = new HashMap<>();
        
        File file = new File(filePath);
        if (!file.exists()) {
            logger.debug("RULE OCCURRENCES FILE: File does not exist: {}", filePath);
            return fileOccurrenceCounts;
        }
        
        logger.debug("RULE OCCURRENCES FILE: Processing {} for rule {} (valid: {})", 
                   file.getName(), ruleId, valid);
        
        int processedRecords = 0;
        int recordsWithOccurrences = 0;
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
                .withConf(hadoopConf)
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                processedRecords++;
                
                // Get the appropriate occurrences map based on valid flag
                String fieldName = valid ? "validOccurrencesByRuleID" : "invalidOccurrencesByRuleID";
                Object occurrenceMapObj = record.get(fieldName);
                
                if (occurrenceMapObj != null) {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> occurrenceMap = (Map<Object, Object>) occurrenceMapObj;
                    
                    // Use helper method to find the rule regardless of String/Utf8 type
                    Object occurrencesObj = findValueInAvroMap(occurrenceMap, ruleId);
                    
                    if (occurrencesObj != null) {
                        recordsWithOccurrences++;
                        @SuppressWarnings("unchecked")
                        List<Object> occurrences = (List<Object>) occurrencesObj;
                        
                        // Count each occurrence
                        for (Object occurrence : occurrences) {
                            String occurrenceStr = avroToString(occurrence);
                            if (occurrenceStr != null) {
                                fileOccurrenceCounts.merge(occurrenceStr, 1L, Long::sum);
                            }
                        }
                    }
                }
            }
        }
        
        logger.debug("RULE OCCURRENCES FILE: Processed {} records, {} had occurrences, found {} unique occurrences in {}", 
                   processedRecords, recordsWithOccurrences, fileOccurrenceCounts.size(), file.getName());
        
        return fileOccurrenceCounts;
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
        
        org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(filePath);
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(hadoopPath, hadoopConf))
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
    
    // ==================== QUERY OPTIMIZATION METHODS ====================
    // (Moved from ValidationStatParquetQueryEngine for consolidated architecture)
    
    /**
     * STREAMING OPTIMIZED: Consulta optimizada que procesa registros de forma streaming
     * Lee registros uno por uno sin cargar todo el archivo en memoria
     * Utiliza caché para evitar re-procesar archivos con los mismos filtros
     */
    private AggregationResult getAggregatedStatsOptimized(String filePath, AggregationFilter filter) throws IOException {
        logger.debug("STREAMING STATS: Processing aggregated stats for: {} (streaming mode)", filePath);
        
        String cacheKey = filePath + "_" + filter.hashCode();
        if (queryCache.containsKey(cacheKey)) {
            logger.debug("Resultado desde cache");
            return queryCache.get(cacheKey);
        }
        
        AggregationResult result = new AggregationResult();
        
        // CRÍTICO: Para obtener estadísticas de reglas correctas, necesitamos leer registros reales
        // Los metadatos no contienen información de validRuleCounts/invalidRuleCounts
        logger.debug("CORRECCIÓN: Usando lectura real de registros para obtener estadísticas de reglas precisas");
        
        try (org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader = 
                org.apache.parquet.avro.AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(new Path(filePath), new Configuration()))
                .build()) {
            
            GenericRecord record;
            int totalRecords = 0;
            int filteredRecords = 0;
            
            while ((record = reader.read()) != null) {
                totalRecords++;
                
                // Aplicar filtros si los hay
                if (filter == null || matchesSpecificFilter(record, filter)) {
                    filteredRecords++;
                    result.incrementTotalCount();
                    
                    // Procesar estadísticas básicas
                    Object isValidObj = record.get("isValid");
                    Boolean isValid = (Boolean) isValidObj;
                    if (isValid != null && isValid) {
                        result.incrementValidCount();
                    } else {
                        result.incrementInvalidCount();
                    }
                    
                    Object isTransformedObj = record.get("isTransformed");
                    Boolean isTransformed = (Boolean) isTransformedObj;
                    if (isTransformed != null && isTransformed) {
                        result.incrementTransformedCount();
                    }
                    
                    // CRÍTICO: Procesar estadísticas de reglas
                    processRuleStatistics(record, result);
                }
            }
            
            logger.debug("FILTER DEBUG: File: {}, Total records: {}, Filtered records: {}, Filter: isValid={}, isTransformed={}", 
                        filePath, totalRecords, filteredRecords, 
                        filter != null ? filter.getIsValid() : "null",
                        filter != null ? filter.getIsTransformed() : "null");
        }
        
        queryCache.put(cacheKey, result);
        logger.debug("Consulta híbrida completada: {} registros, {} reglas válidas, {} reglas inválidas", 
                    result.getTotalCount(), result.getValidRuleCounts().size(), result.getInvalidRuleCounts().size());
        return result;
    }
    
    /**
     * CORRECCIÓN: Procesa estadísticas de reglas desde registros reales
     */
    private void processRuleStatistics(GenericRecord record, AggregationResult result) {
        try {
            // Procesar reglas válidas
            Object validRulesObj = record.get("validRulesIDList");
            if (validRulesObj != null) {
                @SuppressWarnings("unchecked")
                java.util.List<Object> validRulesList = (java.util.List<Object>) validRulesObj;
                for (Object ruleObj : validRulesList) {
                    String rule = avroToString(ruleObj);
                    if (rule != null && !rule.isEmpty()) {
                        result.addValidRuleCount(rule);
                    }
                }
            }
            
            // Procesar reglas inválidas
            Object invalidRulesObj = record.get("invalidRulesIDList");
            if (invalidRulesObj != null) {
                @SuppressWarnings("unchecked")
                java.util.List<Object> invalidRulesList = (java.util.List<Object>) invalidRulesObj;
                for (Object ruleObj : invalidRulesList) {
                    String rule = avroToString(ruleObj);
                    if (rule != null && !rule.isEmpty()) {
                        result.addInvalidRuleCount(rule);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Error procesando estadísticas de reglas: {}", e.getMessage());
        }
    }
    
    /**
     * Verificación específica de registros individuales
     */
    private boolean matchesSpecificFilter(GenericRecord record, AggregationFilter filter) {
        try {
            // Aplicar filtros específicos
            if (filter.getSnapshotId() != null) {
                Object snapshotIdObj = record.get("snapshotID");
                String snapshotId = avroToString(snapshotIdObj);
                if (!filter.getSnapshotId().toString().equals(snapshotId)) {
                    return false;
                }
            }
            
            if (filter.getRecordOAIId() != null) {
                Object identifierObj = record.get("identifier");
                String identifier = avroToString(identifierObj);
                if (!filter.getRecordOAIId().equals(identifier)) {
                    return false;
                }
            }
            
            if (filter.getIsValid() != null) {
                Object isValidObj = record.get("isValid");
                Boolean isValid = (Boolean) isValidObj;
                if (!filter.getIsValid().equals(isValid)) {
                    return false;
                }
            }
            
            if (filter.getIsTransformed() != null) {
                Object isTransformedObj = record.get("isTransformed");
                Boolean isTransformed = (Boolean) isTransformedObj;
                if (!filter.getIsTransformed().equals(isTransformed)) {
                    return false;
                }
            }
            
            // Filtro de reglas válidas
            if (filter.getValidRulesFilter() != null) {
                Object validRulesObj = record.get("validRulesIDList");
                if (validRulesObj != null) {
                    @SuppressWarnings("unchecked")
                    java.util.List<Object> validRulesList = (java.util.List<Object>) validRulesObj;
                    String targetRule = filter.getValidRulesFilter();
                    boolean found = false;
                    for (Object ruleObj : validRulesList) {
                        String rule = avroToString(ruleObj);
                        if (targetRule.equals(rule)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        return false;
                    }
                }
            }
            
            // Filtro de reglas inválidas
            if (filter.getInvalidRulesFilter() != null) {
                Object invalidRulesObj = record.get("invalidRulesIDList");
                if (invalidRulesObj != null) {
                    @SuppressWarnings("unchecked")
                    java.util.List<Object> invalidRulesList = (java.util.List<Object>) invalidRulesObj;
                    String targetRule = filter.getInvalidRulesFilter();
                    boolean found = false;
                    for (Object ruleObj : invalidRulesList) {
                        String rule = avroToString(ruleObj);
                        if (targetRule.equals(rule)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        return false;
                    }
                }
            }
            
            return true;
        } catch (Exception e) {
            logger.warn("Error al verificar filtro específico: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Obtener estadísticas de cache
     */
    public Map<String, Object> getCacheStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("size", queryCache.size());
        stats.put("keys", new ArrayList<>(queryCache.keySet()));
        return stats;
    }

    /**
     * Limpiar cache
     */
    public void clearCache() {
        queryCache.clear();
        logger.info("Query cache cleared");
    }
    
    // ==================== QUERY OPTIMIZATION CLASSES ====================
    // (Moved from ValidationStatParquetQueryEngine for consolidated architecture)
    
    /**
     * Clase para filtros de agregación
     */
    public static class AggregationFilter {
        private Boolean isValid;
        private Boolean isTransformed;
        private String recordOAIId;
        private List<String> ruleIds;
        private Long snapshotId;
        private String validRulesFilter;
        private String invalidRulesFilter;

        // Getters y setters
        public Boolean getIsValid() { return isValid; }
        public void setIsValid(Boolean isValid) { this.isValid = isValid; }

        public Boolean getIsTransformed() { return isTransformed; }
        public void setIsTransformed(Boolean isTransformed) { this.isTransformed = isTransformed; }

        public String getRecordOAIId() { return recordOAIId; }
        public void setRecordOAIId(String recordOAIId) { this.recordOAIId = recordOAIId; }

        public List<String> getRuleIds() { return ruleIds; }
        public void setRuleIds(List<String> ruleIds) { this.ruleIds = ruleIds; }

        public Long getSnapshotId() { return snapshotId; }
        public void setSnapshotId(Long snapshotId) { this.snapshotId = snapshotId; }

        public String getValidRulesFilter() { return validRulesFilter; }
        public void setValidRulesFilter(String validRulesFilter) { this.validRulesFilter = validRulesFilter; }

        public String getInvalidRulesFilter() { return invalidRulesFilter; }
        public void setInvalidRulesFilter(String invalidRulesFilter) { this.invalidRulesFilter = invalidRulesFilter; }

        // Métodos adicionales para compatibilidad
        public Long getMinSnapshotId() { return snapshotId; }
        public void setMinSnapshotId(Long minSnapshotId) { this.snapshotId = minSnapshotId; }

        public Long getMaxSnapshotId() { return snapshotId; }
        public void setMaxSnapshotId(Long maxSnapshotId) { this.snapshotId = maxSnapshotId; }

        @Override
        public int hashCode() {
            return Objects.hash(isValid, isTransformed, recordOAIId, ruleIds, snapshotId, validRulesFilter, invalidRulesFilter);
        }
    }

    /**
     * Clase para resultados de agregación
     */
    public static class AggregationResult {
        private long totalCount = 0;
        private long validCount = 0;
        private long invalidCount = 0;
        private long transformedCount = 0;
        private Map<String, Long> validRuleCounts = new HashMap<>();
        private Map<String, Long> invalidRuleCounts = new HashMap<>();

        // Getters y setters
        public long getTotalCount() { return totalCount; }
        public void setTotalCount(long totalCount) { this.totalCount = totalCount; }

        public long getValidCount() { return validCount; }
        public void setValidCount(long validCount) { this.validCount = validCount; }
        public void incrementValidCount() { this.validCount++; }

        public long getInvalidCount() { return invalidCount; }
        public void setInvalidCount(long invalidCount) { this.invalidCount = invalidCount; }
        public void incrementInvalidCount() { this.invalidCount++; }

        public long getTransformedCount() { return transformedCount; }
        public void setTransformedCount(long transformedCount) { this.transformedCount = transformedCount; }
        public void incrementTransformedCount() { this.transformedCount++; }

        public void incrementTotalCount() { this.totalCount++; }

        public Map<String, Long> getValidRuleCounts() { return validRuleCounts; }
        public Map<String, Long> getInvalidRuleCounts() { return invalidRuleCounts; }

        public void addValidRuleCount(String rule) {
            validRuleCounts.merge(rule, 1L, Long::sum);
        }

        public void addInvalidRuleCount(String rule) {
            invalidRuleCounts.merge(rule, 1L, Long::sum);
        }
    }

}
