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
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.io.api.Binary;
import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

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

    // ==================== MEMORY CACHE CONFIGURATION ====================
    
    @Value("${validation.stats.memory-cache.enabled:true}")
    private boolean memoryCacheEnabled;
    
    @Value("${validation.stats.memory-cache.max-snapshots:10}")
    private int maxCachedSnapshots;
    
    @Value("${validation.stats.memory-cache.max-records-per-snapshot:5000000}")
    private long maxRecordsPerSnapshot;

    private Schema avroSchema;
    private Configuration hadoopConf;
    
    // Counter to track next file index per snapshot
    private final Map<Long, Integer> snapshotFileCounters = new ConcurrentHashMap<>();
    
    // BUFFER SYSTEM: Accumulate records until reaching recordsPerFile limit
    private final Map<Long, List<ValidationStatObservationParquet>> snapshotBuffers = new ConcurrentHashMap<>();
    
    // ==================== MEMORY CACHE SYSTEM ====================
    
    /**
     * CACHE DATA STRUCTURE: Holds all data for a snapshot in memory
     */
    private static class SnapshotCache {
        private final List<ValidationStatObservationParquet> allRecords;
        private final Map<String, Object> precomputedStats;
        private final long loadedTimestamp;
        private final long recordCount;
        
        public SnapshotCache(List<ValidationStatObservationParquet> records, Map<String, Object> stats) {
            this.allRecords = Collections.unmodifiableList(new ArrayList<>(records));
            this.precomputedStats = new HashMap<>(stats);
            this.loadedTimestamp = System.currentTimeMillis();
            this.recordCount = records.size();
        }
        
        public List<ValidationStatObservationParquet> getAllRecords() { return allRecords; }
        public Map<String, Object> getPrecomputedStats() { return precomputedStats; }
        public long getLoadedTimestamp() { return loadedTimestamp; }
        public long getRecordCount() { return recordCount; }
    }
    
    // MEMORY CACHE STORAGE
    private final Map<Long, SnapshotCache> snapshotMemoryCache = new ConcurrentHashMap<>();
    private final Map<Long, Long> cacheLoadTimestamp = new ConcurrentHashMap<>();
    private final Map<Long, Boolean> cacheLoadingInProgress = new ConcurrentHashMap<>();
    
    // CACHE STATISTICS
    private final AtomicLong cacheHits = new AtomicLong(0);
    private final AtomicLong cacheMisses = new AtomicLong(0);
    private final AtomicLong cacheLoads = new AtomicLong(0);
    
    private final Map<String, AggregationResult> queryCache = new ConcurrentHashMap<>();
    
    // CACHE: Para evitar recontar registros en cada paginación
    private final Map<String, Long> countCache = new ConcurrentHashMap<>();
    private final Map<String, Long> countCacheTimestamp = new ConcurrentHashMap<>();
    private final long COUNT_CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutos de TTL
    
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
            
            // Limpiar caches
            queryCache.clear();
            countCache.clear();
            countCacheTimestamp.clear();
            
            // Limpiar memory cache
            if (memoryCacheEnabled) {
                clearMemoryCache();
            }
            
            logger.debug("SHUTDOWN: Cleared all caches");
            
        } catch (IOException e) {
            logger.error("SHUTDOWN: Error flushing buffers during shutdown", e);
        }
    }
    
    /**
     * CACHE HELPER: Invalida cache de conteo para un snapshot específico
     */
    private void invalidateCountCache(Long snapshotId) {
        // Buscar todas las claves de cache que empiecen con el snapshot ID
        countCache.entrySet().removeIf(entry -> entry.getKey().startsWith("count_" + snapshotId + "_"));
        countCacheTimestamp.entrySet().removeIf(entry -> entry.getKey().startsWith("count_" + snapshotId + "_"));
        logger.debug("CACHE: Invalidated count cache for snapshot {}", snapshotId);
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
     * PREDICATE PUSHDOWN: Construye filtros optimizados para consultas Parquet
     * Permite filtrar datos directamente en el nivel de columnas sin cargar registros
     */
    private FilterPredicate buildFilterPredicate(AggregationFilter filter) {
        if (filter == null) {
            return null;
        }
        
        FilterPredicate predicate = null;
        
        try {
            // Filtro por identifier (String)
            if (filter.getRecordOAIId() != null && !filter.getRecordOAIId().trim().isEmpty()) {
                FilterPredicate identifierFilter = FilterApi.eq(
                    FilterApi.binaryColumn("identifier"), 
                    Binary.fromString(filter.getRecordOAIId())
                );
                predicate = combineWithAnd(predicate, identifierFilter);
                logger.debug("PREDICATE: Added identifier filter: {}", filter.getRecordOAIId());
            }
            
            // Filtro por isValid (Boolean)
            if (filter.getIsValid() != null) {
                FilterPredicate validFilter = FilterApi.eq(
                    FilterApi.booleanColumn("isValid"), 
                    filter.getIsValid()
                );
                predicate = combineWithAnd(predicate, validFilter);
                logger.debug("PREDICATE: Added isValid filter: {}", filter.getIsValid());
            }
            
            // Filtro por isTransformed (Boolean)
            if (filter.getIsTransformed() != null) {
                FilterPredicate transformedFilter = FilterApi.eq(
                    FilterApi.booleanColumn("isTransformed"), 
                    filter.getIsTransformed()
                );
                predicate = combineWithAnd(predicate, transformedFilter);
                logger.debug("PREDICATE: Added isTransformed filter: {}", filter.getIsTransformed());
            }
            
            // NOTA: Los campos validRulesIDList e invalidRulesIDList son arrays complejos
            // Parquet predicate pushdown no soporta directamente filtros en arrays
            // Estos se manejarán en post-procesamiento después del pushdown básico
            if (filter.getValidRulesFilter() != null || filter.getInvalidRulesFilter() != null) {
                logger.debug("PREDICATE: Complex array filters (validRules/invalidRules) will be applied in post-processing");
            }
            
        } catch (Exception e) {
            logger.warn("PREDICATE: Error building filter predicate, falling back to full scan: {}", e.getMessage());
            return null;
        }
        
        if (predicate != null) {
            logger.info("PREDICATE: Built optimized filter predicate with {} conditions", 
                       countPredicateConditions(predicate));
        }
        
        return predicate;
    }

    /**
     * HELPER: Combina predicados con operador AND
     */
    private FilterPredicate combineWithAnd(FilterPredicate existing, FilterPredicate newPredicate) {
        if (existing == null) {
            return newPredicate;
        }
        return FilterApi.and(existing, newPredicate);
    }

    /**
     * HELPER: Cuenta condiciones en un predicado (para logging)
     */
    private int countPredicateConditions(FilterPredicate predicate) {
        // Implementación simple para contar condiciones
        String predicateStr = predicate.toString();
        return (int) predicateStr.chars().filter(ch -> ch == '=').count();
    }

    /**
     * POST-PROCESSING: Aplica filtros complejos que no se pueden hacer con predicate pushdown
     * Específicamente para arrays como validRulesIDList e invalidRulesIDList
     */
    private boolean matchesComplexFilters(ValidationStatObservationParquet obs, AggregationFilter filter) {
        if (filter == null) {
            return true;
        }
        
        // Filtro de reglas válidas (array complex filter)
        if (filter.getValidRulesFilter() != null) {
            List<String> validRules = obs.getValidRulesIDList();
            if (validRules == null || !validRules.contains(filter.getValidRulesFilter())) {
                return false;
            }
        }
        
        // Filtro de reglas inválidas (array complex filter)
        if (filter.getInvalidRulesFilter() != null) {
            List<String> invalidRules = obs.getInvalidRulesIDList();
            if (invalidRules == null || !invalidRules.contains(filter.getInvalidRulesFilter())) {
                return false;
            }
        }
        
        // Otros filtros complejos se pueden agregar aquí
        // Por ejemplo: filtros de rango en arrays, filtros de texto parcial, etc.
        
        return true;
    }

    /**
     * OPTIMIZED: Stream file with predicate pushdown for maximum performance
     * Utiliza filtros a nivel de columna para evitar lectura de registros innecesarios
     */
    private List<ValidationStatObservationParquet> streamFileWithPredicatePushdown(String filePath, 
                                                                                   AggregationFilter filter, 
                                                                                   int offset, int limit) throws IOException {
        List<ValidationStatObservationParquet> results = new ArrayList<>();
        
        File file = new File(filePath);
        if (!file.exists()) {
            logger.debug("PREDICATE STREAM: File does not exist: {}", filePath);
            return results;
        }
        
        // Construir predicado para pushdown
        FilterPredicate predicate = buildFilterPredicate(filter);
        
        logger.debug("PREDICATE STREAM: Processing {} with predicate pushdown enabled, offset={}, limit={}", 
                   file.getName(), offset, limit);
        
        int filteredCount = 0; // Registros que pasan el filtro completo
        int collected = 0;     // Registros recolectados para resultado
        int pushedDownCount = 0; // Registros que pasan el predicate pushdown
        
        try {
            // Crear reader con predicate pushdown
            ParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
                .withConf(hadoopConf);
            
            // Aplicar predicate pushdown si está disponible
            if (predicate != null) {
                readerBuilder = readerBuilder.withFilter(FilterCompat.get(predicate));
                logger.debug("PREDICATE STREAM: Applied predicate pushdown filter to reader");
            }
            
            try (ParquetReader<GenericRecord> reader = readerBuilder.build()) {
                GenericRecord record;
                while ((record = reader.read()) != null && collected < limit) {
                    pushedDownCount++;
                    
                    // Post-procesamiento para filtros complejos (arrays) que no se pueden hacer con pushdown
                    ValidationStatObservationParquet observation = fromGenericRecord(record);
                    if (matchesComplexFilters(observation, filter)) {
                        // Este registro pasa todos los filtros
                        if (filteredCount >= offset) {
                            // Ya saltamos suficientes registros, empezar a recolectar
                            results.add(observation);
                            collected++;
                        }
                        filteredCount++;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("PREDICATE STREAM: Error with predicate pushdown for file {}", file.getName(), e);
            // Re-throw exception since we removed fallback methods
            throw new IOException("Failed to process file with predicate pushdown: " + file.getName(), e);
        }
        
        logger.debug("PREDICATE STREAM: File {} - Pushdown filtered to {} records, complex filters to {} records, returned {} results", 
                   file.getName(), pushedDownCount, filteredCount, results.size());
        
        return results;
    }

    /**
     * OPTIMIZED: Count with predicate pushdown for maximum performance
     * Cuenta registros utilizando filtros a nivel de columna
     */
    private long countFileWithPredicatePushdown(String filePath, AggregationFilter filter) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            logger.debug("PREDICATE COUNT: File does not exist: {}", filePath);
            return 0L;
        }
        
        // Construir predicado para pushdown
        FilterPredicate predicate = buildFilterPredicate(filter);
        
        logger.debug("PREDICATE COUNT: Counting with predicate pushdown in {}", file.getName());
        
        long count = 0;
        long pushedDownCount = 0;
        
        try {
            // Crear reader con predicate pushdown
            ParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
                .withConf(hadoopConf);
            
            // Aplicar predicate pushdown si está disponible
            if (predicate != null) {
                readerBuilder = readerBuilder.withFilter(FilterCompat.get(predicate));
                logger.debug("PREDICATE COUNT: Applied predicate pushdown filter for counting");
            }
            
            try (ParquetReader<GenericRecord> reader = readerBuilder.build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    pushedDownCount++;
                    
                    // Post-procesamiento para filtros complejos
                    ValidationStatObservationParquet observation = fromGenericRecord(record);
                    if (matchesComplexFilters(observation, filter)) {
                        count++;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("PREDICATE COUNT: Error with predicate pushdown for file {}", file.getName(), e);
            // Re-throw exception since we removed fallback methods
            throw new IOException("Failed to count with predicate pushdown: " + file.getName(), e);
        }
        
        logger.debug("PREDICATE COUNT: File {} - Pushdown to {} records, complex filters to {} final count", 
                   file.getName(), pushedDownCount, count);
        
        return count;
    }

    /**
     * OPTIMIZED: Aggregated stats with predicate pushdown
     * Genera estadísticas utilizando filtros optimizados a nivel de columna
     */
    private AggregationResult getAggregatedStatsWithPredicatePushdown(String filePath, AggregationFilter filter) throws IOException {
        logger.debug("PREDICATE STATS: Processing aggregated stats with pushdown for: {}", filePath);
        
        String cacheKey = filePath + "_predicate_" + filter.hashCode();
        if (queryCache.containsKey(cacheKey)) {
            logger.debug("PREDICATE STATS: Result from cache");
            return queryCache.get(cacheKey);
        }
        
        // Construir predicado para pushdown
        FilterPredicate predicate = buildFilterPredicate(filter);
        
        AggregationResult result = new AggregationResult();
        
        try {
            // Crear reader con predicate pushdown
            ParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
                .withConf(hadoopConf);
            
            // Aplicar predicate pushdown si está disponible
            if (predicate != null) {
                readerBuilder = readerBuilder.withFilter(FilterCompat.get(predicate));
                logger.debug("PREDICATE STATS: Applied predicate pushdown filter for aggregation");
            }
            
            try (ParquetReader<GenericRecord> reader = readerBuilder.build()) {
                GenericRecord record;
                int totalRecords = 0;
                int pushedDownRecords = 0;
                int filteredRecords = 0;
                
                while ((record = reader.read()) != null) {
                    totalRecords++;
                    pushedDownRecords++;
                    
                    // Post-procesamiento para filtros complejos
                    ValidationStatObservationParquet observation = fromGenericRecord(record);
                    if (matchesComplexFilters(observation, filter)) {
                        filteredRecords++;
                        result.incrementTotalCount();
                        
                        // Procesar estadísticas básicas
                        Boolean isValid = observation.getIsValid();
                        if (isValid != null && isValid) {
                            result.incrementValidCount();
                        } else {
                            result.incrementInvalidCount();
                        }
                        
                        Boolean isTransformed = observation.getIsTransformed();
                        if (isTransformed != null && isTransformed) {
                            result.incrementTransformedCount();
                        }
                        
                        // Procesar estadísticas de reglas
                        processRuleStatisticsFromObservation(observation, result);
                    }
                }
                
                logger.debug("PREDICATE STATS: File processed - Total: {}, Pushdown: {}, Final: {}", 
                            totalRecords, pushedDownRecords, filteredRecords);
            }
        } catch (Exception e) {
            logger.error("PREDICATE STATS: Error with predicate pushdown for file {}", filePath, e);
            // Re-throw exception since we removed fallback methods  
            throw new IOException("Failed to get aggregated stats with predicate pushdown: " + filePath, e);
        }
        
        queryCache.put(cacheKey, result);
        logger.debug("PREDICATE STATS: Aggregation completed with pushdown optimization");
        return result;
    }

    /**
     * HELPER: Procesa estadísticas de reglas desde observación convertida
     */
    private void processRuleStatisticsFromObservation(ValidationStatObservationParquet observation, AggregationResult result) {
        try {
            // Procesar reglas válidas
            List<String> validRules = observation.getValidRulesIDList();
            if (validRules != null) {
                for (String rule : validRules) {
                    if (rule != null && !rule.isEmpty()) {
                        result.addValidRuleCount(rule);
                    }
                }
            }
            
            // Procesar reglas inválidas
            List<String> invalidRules = observation.getInvalidRulesIDList();
            if (invalidRules != null) {
                for (String rule : invalidRules) {
                    if (rule != null && !rule.isEmpty()) {
                        result.addInvalidRuleCount(rule);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("PREDICATE STATS: Error processing rule statistics: {}", e.getMessage());
        }
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
        
        // Invalidar cache de conteo para este snapshot
        invalidateCountCache(snapshotId);
        
        // CACHE INVALIDATION: Automatically invalidate memory cache when data is deleted
        if (memoryCacheEnabled) {
            evictFromCache(snapshotId);
            logger.debug("CACHE INVALIDATION: Evicted snapshot {} due to deletion", snapshotId);
        }
        
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
     * Obtiene estadísticas agregadas con filtros específicos (OPTIMIZADO CON PREDICATE PUSHDOWN)
     * @param snapshotId ID del snapshot
     * @param filter Filtros a aplicar (isValid, isTransformed, ruleIds, etc.)
     * @return Estadísticas agregadas filtradas
     */
    public Map<String, Object> getAggregatedStatsWithFilter(Long snapshotId, AggregationFilter filter) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("PREDICATE AGGREGATION API: Snapshot directory does not exist: {}", snapshotId);
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
            logger.debug("PREDICATE AGGREGATION API: No data files found for snapshot: {}", snapshotId);
            // Return empty statistics
            Map<String, Object> emptyStats = new HashMap<>();
            emptyStats.put("totalCount", 0L);
            emptyStats.put("validCount", 0L);
            emptyStats.put("transformedCount", 0L);
            emptyStats.put("validRuleCounts", new HashMap<String, Long>());
            emptyStats.put("invalidRuleCounts", new HashMap<String, Long>());
            return emptyStats;
        }
        
        // Aggregate filtered statistics from all files using predicate pushdown
        long totalCount = 0;
        long validCount = 0;
        long transformedCount = 0;
        Map<String, Long> combinedValidRuleCounts = new HashMap<>();
        Map<String, Long> combinedInvalidRuleCounts = new HashMap<>();
        
        logger.info("PREDICATE AGGREGATION API: Processing {} data files for snapshot {} with PREDICATE PUSHDOWN filters", 
                   dataFiles.length, snapshotId);
        
        // Process each data file and combine filtered results using pushdown
        for (File dataFile : dataFiles) {
            try {
                AggregationResult result = getAggregatedStatsWithPredicatePushdown(dataFile.getAbsolutePath(), filter);
                
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
                logger.error("PREDICATE AGGREGATION API: Error processing file {} for snapshot {}", 
                           dataFile.getName(), snapshotId, e);
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
        
        logger.info("PREDICATE AGGREGATION API: OPTIMIZED combined statistics for snapshot {}: {} records matched criteria from {} files using PREDICATE PUSHDOWN", 
                   snapshotId, totalCount, dataFiles.length);
        
        return stats;
    }

    /**
     * CACHE HELPER: Obtiene clave de cache para conteo
     */
    private String getCountCacheKey(Long snapshotId, AggregationFilter filter) {
        return "count_" + snapshotId + "_" + (filter != null ? filter.hashCode() : "nofilter");
    }
    
    /**
     * CACHE HELPER: Verifica si el cache está vigente
     */
    private boolean isCountCacheValid(String cacheKey) {
        Long timestamp = countCacheTimestamp.get(cacheKey);
        if (timestamp == null) {
            return false;
        }
        return (System.currentTimeMillis() - timestamp) < COUNT_CACHE_TTL_MS;
    }
    
    /**
     * OPTIMIZED: Cuenta registros con cache inteligente para evitar recálculos frecuentes
     * @param snapshotId ID del snapshot
     * @param filter Filtros a aplicar
     * @return Número de registros que cumplen los criterios
     */
    public long countRecordsWithFilter(Long snapshotId, AggregationFilter filter) throws IOException {
        String cacheKey = getCountCacheKey(snapshotId, filter);
        
        // Verificar cache primero
        if (isCountCacheValid(cacheKey)) {
            Long cachedCount = countCache.get(cacheKey);
            if (cachedCount != null) {
                logger.debug("PREDICATE COUNT CACHE: Returning cached count {} for snapshot {} (cache hit)", 
                           cachedCount, snapshotId);
                return cachedCount;
            }
        }
        
        // No hay cache válido, calcular el conteo
        logger.info("PREDICATE COUNT CACHE: Cache miss or expired, calculating count for snapshot {} with PREDICATE PUSHDOWN", 
                   snapshotId);
        
        long count = calculateCountWithPredicate(snapshotId, filter);
        
        // Guardar en cache
        countCache.put(cacheKey, count);
        countCacheTimestamp.put(cacheKey, System.currentTimeMillis());
        
        logger.info("PREDICATE COUNT CACHE: Calculated and cached count {} for snapshot {} (cache stored)", 
                   count, snapshotId);
        
        return count;
    }
    
    /**
     * FORCE: Cuenta registros sin usar cache (para casos específicos)
     * @param snapshotId ID del snapshot
     * @param filter Filtros a aplicar
     * @return Número de registros que cumplen los criterios
     */
    public long countRecordsWithFilterNoCache(Long snapshotId, AggregationFilter filter) throws IOException {
        logger.info("PREDICATE COUNT FORCE: Bypassing cache, calculating count for snapshot {} with PREDICATE PUSHDOWN", 
                   snapshotId);
        return calculateCountWithPredicate(snapshotId, filter);
    }
    
    /**
     * CACHE MANAGEMENT: Limpia cache de conteo para un snapshot específico
     */
    public void clearCountCache(Long snapshotId) {
        invalidateCountCache(snapshotId);
        logger.info("CACHE: Manually cleared count cache for snapshot {}", snapshotId);
    }
    
    /**
     * CACHE MANAGEMENT: Limpia todo el cache de conteo
     */
    public void clearAllCountCache() {
        countCache.clear();
        countCacheTimestamp.clear();
        logger.info("CACHE: Manually cleared all count cache");
    }
    
    /**
     * INTERNAL: Realiza el cálculo real del conteo con predicate pushdown
     */
    private long calculateCountWithPredicate(Long snapshotId, AggregationFilter filter) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("PREDICATE COUNT API: Snapshot directory does not exist: {}", snapshotId);
            return 0L;
        }
        
        // Get all data files in the snapshot directory
        File[] dataFiles = dir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles == null || dataFiles.length == 0) {
            logger.debug("PREDICATE COUNT API: No data files found for snapshot: {}", snapshotId);
            return 0L;
        }
        
        logger.info("PREDICATE COUNT API: Counting in {} data files for snapshot {} with PREDICATE PUSHDOWN", 
                   dataFiles.length, snapshotId);
        
        long totalCount = 0;
        int processedFiles = 0;
        
        // Count records from all data files using predicate pushdown
        for (File dataFile : dataFiles) {
            processedFiles++;
            try {
                long fileCount = countFileWithPredicatePushdown(dataFile.getAbsolutePath(), filter);
                totalCount += fileCount;
                logger.debug("PREDICATE COUNT API: File {} ({}/{}) has {} matching records with pushdown", 
                           dataFile.getName(), processedFiles, dataFiles.length, fileCount);
            } catch (Exception e) {
                logger.error("PREDICATE COUNT API: Error counting records in file {} for snapshot {}", 
                           dataFile.getName(), snapshotId, e);
                // Continue with other files
            }
        }
        
        logger.info("PREDICATE COUNT API: OPTIMIZED total matching records for snapshot {}: {} using PREDICATE PUSHDOWN across {} files", 
                   snapshotId, totalCount, processedFiles);
        return totalCount;
    }

    /**
     * Búsqueda paginada con filtros (OPTIMIZADA CON PREDICATE PUSHDOWN)
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
            logger.debug("PREDICATE API: Snapshot directory does not exist: {}", snapshotId);
            return Collections.emptyList();
        }
        
        // Get all data files in the snapshot directory
        File[] dataFiles = dir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles == null || dataFiles.length == 0) {
            logger.debug("PREDICATE API: No data files found for snapshot: {}", snapshotId);
            return Collections.emptyList();
        }
        
        // Sort files to ensure consistent ordering
        Arrays.sort(dataFiles, (f1, f2) -> f1.getName().compareTo(f2.getName()));
        
        logger.info("PREDICATE API: Processing up to {} data files for snapshot {} with PREDICATE PUSHDOWN optimization", 
                   dataFiles.length, snapshotId);
        
        List<ValidationStatObservationParquet> results = new ArrayList<>();
        int totalOffset = page * size; // Total de registros a saltar
        int remainingToSkip = totalOffset; // Registros que aún necesitamos saltar
        int targetSize = size; // Número de registros que queremos recolectar
        int processedFiles = 0;
        
        // Process each file with predicate pushdown until we have enough results
        for (File dataFile : dataFiles) {
            processedFiles++;
            
            // Si ya tenemos suficientes resultados, no procesar más archivos
            if (results.size() >= targetSize) {
                logger.debug("PREDICATE API: EARLY STOP - Already collected {} records, skipping remaining {} files", 
                           results.size(), dataFiles.length - processedFiles + 1);
                break;
            }
            
            try {
                logger.debug("PREDICATE API: Processing file {} ({}/{}) with pushdown optimization, remaining to skip: {}, need: {}", 
                           dataFile.getName(), processedFiles, dataFiles.length, remainingToSkip, targetSize - results.size());
                
                // Calcular cuántos registros necesitamos de este archivo
                int remainingNeeded = targetSize - results.size();
                
                // Stream through this file with predicate pushdown
                List<ValidationStatObservationParquet> fileResults = streamFileWithPredicatePushdown(
                    dataFile.getAbsolutePath(), filter, remainingToSkip, remainingNeeded);
                
                // Si obtuvimos resultados, significa que hemos terminado de saltar registros
                if (!fileResults.isEmpty()) {
                    results.addAll(fileResults);
                    remainingToSkip = 0; // Ya no necesitamos saltar más registros
                    
                    logger.debug("PREDICATE API: File {} contributed {} results (total collected: {}/{})", 
                               dataFile.getName(), fileResults.size(), results.size(), targetSize);
                    
                    // Si ya tenemos suficientes resultados, terminar
                    if (results.size() >= targetSize) {
                        logger.debug("PREDICATE API: TARGET REACHED - Collected {} records after processing {} of {} files", 
                                   results.size(), processedFiles, dataFiles.length);
                        break;
                    }
                } else {
                    // No obtuvimos resultados, actualizar cuántos registros saltamos en este archivo
                    // Necesitamos contar cuántos registros había que cumplían el filtro en este archivo
                    long filteredInThisFile = countFileWithPredicatePushdown(dataFile.getAbsolutePath(), filter);
                    remainingToSkip = Math.max(0, remainingToSkip - (int)filteredInThisFile);
                    
                    logger.debug("PREDICATE API: File {} had {} filtered records, remaining to skip: {}", 
                               dataFile.getName(), filteredInThisFile, remainingToSkip);
                }
                           
            } catch (Exception e) {
                logger.error("PREDICATE API: Error processing file {} for snapshot {}", 
                           dataFile.getName(), snapshotId, e);
                // Continue with other files
            }
        }
        
        logger.info("PREDICATE API: OPTIMIZED results - {} records from page {} for snapshot {} using PREDICATE PUSHDOWN (processed {}/{} files)", 
                   results.size(), page, snapshotId, processedFiles, dataFiles.length);
        
        return results;
    }
    
    /**
     * OPTIMIZED: Obtiene conteos de ocurrencias de reglas con predicate pushdown
     * Procesa archivo por archivo con filtros optimizados a nivel de columna
     */
    public Map<String, Long> getRuleOccurrenceCounts(Long snapshotId, String ruleId, boolean valid) throws IOException {
        String snapshotDir = getSnapshotDirectoryPath(snapshotId);
        File dir = new File(snapshotDir);
        
        if (!dir.exists()) {
            logger.debug("RULE OCCURRENCES PREDICATE: Snapshot directory does not exist: {}", snapshotId);
            return new HashMap<>();
        }
        
        // Get all data files in the snapshot directory
        File[] dataFiles = dir.listFiles(file -> file.getName().startsWith("data-") && file.getName().endsWith(".parquet"));
        
        if (dataFiles == null || dataFiles.length == 0) {
            logger.debug("RULE OCCURRENCES PREDICATE: No data files found for snapshot: {}", snapshotId);
            return new HashMap<>();
        }
        
        Map<String, Long> occurrenceCounts = new HashMap<>();
        
        logger.debug("RULE OCCURRENCES PREDICATE: Processing {} data files for rule {} (valid: {}) with predicate pushdown", 
                   dataFiles.length, ruleId, valid);
        
        // Process each file sequentially with predicate pushdown optimization
        for (File dataFile : dataFiles) {
            try {
                Map<String, Long> fileOccurrences = getRuleOccurrenceCountsFromFile(
                    dataFile.getAbsolutePath(), ruleId, valid);
                
                // Merge counts from this file
                for (Map.Entry<String, Long> entry : fileOccurrences.entrySet()) {
                    occurrenceCounts.merge(entry.getKey(), entry.getValue(), Long::sum);
                }
                
                logger.debug("RULE OCCURRENCES PREDICATE: File {} contributed {} unique occurrences", 
                           dataFile.getName(), fileOccurrences.size());
                           
            } catch (Exception e) {
                logger.error("RULE OCCURRENCES PREDICATE: Error processing file {} for snapshot {}", 
                           dataFile.getName(), snapshotId, e);
                // Continue with other files
            }
        }
        
        logger.info("RULE OCCURRENCES PREDICATE: OPTIMIZED results - Found {} unique occurrences for rule {} (valid: {}) across {} files using PREDICATE PUSHDOWN", 
                   occurrenceCounts.size(), ruleId, valid, dataFiles.length);
        
        return occurrenceCounts;
    }
    
    /**
     * OPTIMIZED: Procesa conteos de ocurrencias de reglas con predicate pushdown
     * Utiliza filtros optimizados a nivel de columna para maximizar rendimiento
     */
    private Map<String, Long> getRuleOccurrenceCountsFromFile(String filePath, String ruleId, boolean valid) throws IOException {
        Map<String, Long> fileOccurrenceCounts = new HashMap<>();
        
        File file = new File(filePath);
        if (!file.exists()) {
            logger.debug("RULE OCCURRENCES PREDICATE: File does not exist: {}", filePath);
            return fileOccurrenceCounts;
        }
        
        // Crear filtro para predicate pushdown usando isValid
        AggregationFilter filter = new AggregationFilter();
        filter.setIsValid(valid); // Filtrar por validez para optimizar
        
        FilterPredicate predicate = buildFilterPredicate(filter);
        
        logger.debug("RULE OCCURRENCES PREDICATE: Processing {} for rule {} (valid: {}) with predicate pushdown", 
                   file.getName(), ruleId, valid);
        
        int totalRecords = 0;
        int pushedDownRecords = 0;
        int recordsWithOccurrences = 0;
        
        try {
            // Crear reader con predicate pushdown
            ParquetReader.Builder<GenericRecord> readerBuilder = AvroParquetReader.<GenericRecord>builder(
                HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
                .withConf(hadoopConf);
            
            // Aplicar predicate pushdown si está disponible
            if (predicate != null) {
                readerBuilder = readerBuilder.withFilter(FilterCompat.get(predicate));
                logger.debug("RULE OCCURRENCES PREDICATE: Applied isValid filter for rule {} (valid: {})", ruleId, valid);
            }
            
            try (ParquetReader<GenericRecord> reader = readerBuilder.build()) {
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    totalRecords++;
                    pushedDownRecords++;
                    
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
        } catch (Exception e) {
            logger.error("RULE OCCURRENCES PREDICATE: Error with predicate pushdown for file {}", file.getName(), e);
            throw new IOException("Failed to process rule occurrences with predicate pushdown: " + file.getName(), e);
        }
        
        logger.debug("RULE OCCURRENCES PREDICATE: File {} - Total: {}, Pushdown: {}, WithOccurrences: {}, UniqueOccurrences: {}", 
                   file.getName(), totalRecords, pushedDownRecords, recordsWithOccurrences, fileOccurrenceCounts.size());
        
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
        
        // CACHE INVALIDATION: Automatically invalidate cache when data changes
        if (memoryCacheEnabled && observations != null) {
            Set<Long> affectedSnapshots = observations.stream()
                .map(ValidationStatObservationParquet::getSnapshotID)
                .collect(Collectors.toSet());
            
            for (Long snapshotId : affectedSnapshots) {
                evictFromCache(snapshotId);
                logger.debug("CACHE INVALIDATION: Evicted snapshot {} due to data changes", snapshotId);
            }
        }
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
            
            // Invalidar cache de conteo ya que se agregaron nuevos datos
            invalidateCountCache(snapshotId);
            
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
    
    // ==================== MEMORY CACHE CORE METHODS ====================
    
    /**
     * SMART CACHE LOADER: Loads snapshot data into memory cache if not already loaded
     * Thread-safe with loading indicators to prevent multiple concurrent loads
     */
    private SnapshotCache ensureSnapshotInCache(Long snapshotId) throws IOException {
        if (!memoryCacheEnabled) {
            throw new UnsupportedOperationException("Memory cache is disabled");
        }
        
        // Check if already in cache
        SnapshotCache cached = snapshotMemoryCache.get(snapshotId);
        if (cached != null) {
            cacheHits.incrementAndGet();
            logger.debug("MEMORY CACHE HIT: Snapshot {} found in cache ({} records)", 
                       snapshotId, cached.getRecordCount());
            return cached;
        }
        
        // Check if loading is in progress
        synchronized (cacheLoadingInProgress) {
            if (cacheLoadingInProgress.getOrDefault(snapshotId, false)) {
                logger.debug("MEMORY CACHE WAIT: Snapshot {} is being loaded by another thread, waiting...", snapshotId);
                
                // Wait for loading to complete
                while (cacheLoadingInProgress.getOrDefault(snapshotId, false)) {
                    try {
                        Thread.sleep(100); // Wait 100ms and check again
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted while waiting for cache load", e);
                    }
                }
                
                // Check cache again after waiting
                cached = snapshotMemoryCache.get(snapshotId);
                if (cached != null) {
                    cacheHits.incrementAndGet();
                    logger.debug("MEMORY CACHE HIT AFTER WAIT: Snapshot {} now available in cache", snapshotId);
                    return cached;
                }
            }
            
            // Mark as loading in progress
            cacheLoadingInProgress.put(snapshotId, true);
        }
        
        try {
            cacheMisses.incrementAndGet();
            logger.info("MEMORY CACHE MISS: Loading snapshot {} into memory cache...", snapshotId);
            
            // Check if we need to evict old snapshots (LRU eviction)
            evictOldSnapshotsIfNeeded();
            
            // Load all data from parquet files
            long startTime = System.currentTimeMillis();
            List<ValidationStatObservationParquet> allRecords = loadAllRecordsFromDisk(snapshotId);
            
            // Check record count limit
            if (allRecords.size() > maxRecordsPerSnapshot) {
                logger.warn("MEMORY CACHE LIMIT: Snapshot {} has {} records, exceeds limit of {}. Skipping cache.", 
                           snapshotId, allRecords.size(), maxRecordsPerSnapshot);
                return null; // Don't cache, fall back to disk queries
            }
            
            // Precompute basic statistics for ultra-fast stats queries
            Map<String, Object> precomputedStats = precomputeStatistics(allRecords);
            
            // Create cache entry
            SnapshotCache newCache = new SnapshotCache(allRecords, precomputedStats);
            
            // Store in cache
            snapshotMemoryCache.put(snapshotId, newCache);
            cacheLoadTimestamp.put(snapshotId, System.currentTimeMillis());
            cacheLoads.incrementAndGet();
            
            long loadTime = System.currentTimeMillis() - startTime;
            logger.info("MEMORY CACHE LOADED: Snapshot {} cached in memory - {} records in {}ms ({}MB estimated)", 
                       snapshotId, allRecords.size(), loadTime, estimateMemoryUsage(allRecords));
            
            return newCache;
            
        } finally {
            // Always remove loading indicator
            synchronized (cacheLoadingInProgress) {
                cacheLoadingInProgress.remove(snapshotId);
            }
        }
    }
    
    /**
     * DISK LOADER: Loads all records from parquet files (used for cache loading)
     */
    private List<ValidationStatObservationParquet> loadAllRecordsFromDisk(Long snapshotId) throws IOException {
        logger.debug("DISK LOAD: Loading all records for snapshot {} from parquet files", snapshotId);
        
        List<String> filePaths = getAllDataFilePaths(snapshotId);
        if (filePaths.isEmpty()) {
            logger.debug("DISK LOAD: No data files found for snapshot {}", snapshotId);
            return Collections.emptyList();
        }
        
        List<ValidationStatObservationParquet> allRecords = new ArrayList<>();
        
        // Load from all parquet files
        for (String filePath : filePaths) {
            File file = new File(filePath);
            if (!file.exists()) {
                logger.warn("DISK LOAD: File does not exist: {}", filePath);
                continue;
            }
            
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                    HadoopInputFile.fromPath(new Path(filePath), hadoopConf))
                    .withConf(hadoopConf)
                    .build()) {
                
                GenericRecord record;
                int fileRecordCount = 0;
                while ((record = reader.read()) != null) {
                    ValidationStatObservationParquet observation = fromGenericRecord(record);
                    allRecords.add(observation);
                    fileRecordCount++;
                }
                
                logger.debug("DISK LOAD: Loaded {} records from {}", fileRecordCount, file.getName());
            }
        }
        
        logger.info("DISK LOAD: Loaded {} total records for snapshot {} from {} files", 
                   allRecords.size(), snapshotId, filePaths.size());
        return allRecords;
    }
    
    /**
     * PRECOMPUTE: Calculate basic statistics during cache load for ultra-fast stats queries
     */
    private Map<String, Object> precomputeStatistics(List<ValidationStatObservationParquet> records) {
        logger.debug("PRECOMPUTE: Calculating statistics for {} records", records.size());
        
        long totalCount = records.size();
        long validCount = 0;
        long transformedCount = 0;
        Map<String, Long> validRuleCounts = new HashMap<>();
        Map<String, Long> invalidRuleCounts = new HashMap<>();
        
        for (ValidationStatObservationParquet obs : records) {
            // Count valid/invalid
            if (Boolean.TRUE.equals(obs.getIsValid())) {
                validCount++;
            }
            
            // Count transformed
            if (Boolean.TRUE.equals(obs.getIsTransformed())) {
                transformedCount++;
            }
            
            // Count rules
            if (obs.getValidRulesIDList() != null) {
                for (String rule : obs.getValidRulesIDList()) {
                    if (rule != null && !rule.isEmpty()) {
                        validRuleCounts.merge(rule, 1L, Long::sum);
                    }
                }
            }
            
            if (obs.getInvalidRulesIDList() != null) {
                for (String rule : obs.getInvalidRulesIDList()) {
                    if (rule != null && !rule.isEmpty()) {
                        invalidRuleCounts.merge(rule, 1L, Long::sum);
                    }
                }
            }
        }
        
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalCount", totalCount);
        stats.put("validCount", validCount);
        stats.put("invalidCount", totalCount - validCount);
        stats.put("transformedCount", transformedCount);
        stats.put("validRuleCounts", validRuleCounts);
        stats.put("invalidRuleCounts", invalidRuleCounts);
        
        logger.debug("PRECOMPUTE: Statistics calculated - {} total, {} valid, {} rules", 
                    totalCount, validCount, validRuleCounts.size() + invalidRuleCounts.size());
        return stats;
    }
    
    /**
     * MEMORY MANAGEMENT: Evict old snapshots using LRU policy
     */
    private void evictOldSnapshotsIfNeeded() {
        if (snapshotMemoryCache.size() >= maxCachedSnapshots) {
            // Find oldest snapshot by load timestamp
            Long oldestSnapshotId = cacheLoadTimestamp.entrySet().stream()
                .min(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
            
            if (oldestSnapshotId != null) {
                SnapshotCache evicted = snapshotMemoryCache.remove(oldestSnapshotId);
                cacheLoadTimestamp.remove(oldestSnapshotId);
                
                if (evicted != null) {
                    logger.info("MEMORY CACHE EVICT: Removed snapshot {} from cache ({} records freed)", 
                               oldestSnapshotId, evicted.getRecordCount());
                }
            }
        }
    }
    
    /**
     * MEMORY ESTIMATION: Estimate memory usage of cached records
     */
    private long estimateMemoryUsage(List<ValidationStatObservationParquet> records) {
        if (records.isEmpty()) return 0;
        
        // Rough estimation: ~500 bytes per record (strings, lists, etc.)
        return records.size() * 500L / (1024 * 1024); // Return in MB
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
    
    // ==================== MEMORY CACHE API METHODS ====================
    
    /**
     * ULTRA-FAST: Get aggregated stats from memory cache (millisecond response)
     */
    public Map<String, Object> getAggregatedStatsFromCache(Long snapshotId) throws IOException {
        if (!memoryCacheEnabled) {
            logger.info("CACHE DISABLED: Falling back to disk-based aggregation for snapshot {}", snapshotId);
            return getAggregatedStats(snapshotId);
        }
        
        SnapshotCache cache = ensureSnapshotInCache(snapshotId);
        if (cache == null) {
            logger.warn("CACHE FALLBACK: Snapshot {} too large for cache, using disk queries", snapshotId);
            return getAggregatedStats(snapshotId);
        }
        
        logger.debug("ULTRA-FAST STATS: Returning precomputed statistics for snapshot {} from memory", snapshotId);
        return new HashMap<>(cache.getPrecomputedStats());
    }
    
    /**
     * ULTRA-FAST: Get aggregated stats with filter from memory cache
     */
    public Map<String, Object> getAggregatedStatsWithFilterFromCache(Long snapshotId, AggregationFilter filter) throws IOException {
        if (!memoryCacheEnabled) {
            logger.info("CACHE DISABLED: Falling back to disk-based filtered aggregation for snapshot {}", snapshotId);
            return getAggregatedStatsWithFilter(snapshotId, filter);
        }
        
        SnapshotCache cache = ensureSnapshotInCache(snapshotId);
        if (cache == null) {
            logger.warn("CACHE FALLBACK: Snapshot {} too large for cache, using disk queries", snapshotId);
            return getAggregatedStatsWithFilter(snapshotId, filter);
        }
        
        logger.debug("ULTRA-FAST FILTERED STATS: Processing filter on {} cached records", cache.getRecordCount());
        
        // Apply filter in memory - ultra fast!
        List<ValidationStatObservationParquet> filteredRecords = cache.getAllRecords().stream()
            .filter(record -> matchesFilter(record, filter))
            .collect(Collectors.toList());
        
        // Compute stats on filtered results
        Map<String, Object> filteredStats = precomputeStatistics(filteredRecords);
        
        logger.debug("ULTRA-FAST FILTERED STATS: {} records matched filter criteria", filteredRecords.size());
        return filteredStats;
    }
    
    /**
     * ULTRA-FAST: Count records with filter from memory cache
     */
    public long countRecordsWithFilterFromCache(Long snapshotId, AggregationFilter filter) throws IOException {
        if (!memoryCacheEnabled) {
            logger.info("CACHE DISABLED: Falling back to disk-based count for snapshot {}", snapshotId);
            return countRecordsWithFilter(snapshotId, filter);
        }
        
        SnapshotCache cache = ensureSnapshotInCache(snapshotId);
        if (cache == null) {
            logger.warn("CACHE FALLBACK: Snapshot {} too large for cache, using disk queries", snapshotId);
            return countRecordsWithFilter(snapshotId, filter);
        }
        
        logger.debug("ULTRA-FAST COUNT: Counting on {} cached records with filter", cache.getRecordCount());
        
        // Count in memory - ultra fast!
        long count = cache.getAllRecords().stream()
            .filter(record -> matchesFilter(record, filter))
            .count();
        
        logger.debug("ULTRA-FAST COUNT: {} records matched filter criteria", count);
        return count;
    }
    
    /**
     * ULTRA-FAST: Find records with filter and pagination from memory cache
     */
    public List<ValidationStatObservationParquet> findWithFilterAndPaginationFromCache(Long snapshotId, 
                                                                                       AggregationFilter filter, 
                                                                                       int page, int size) throws IOException {
        if (!memoryCacheEnabled) {
            logger.info("CACHE DISABLED: Falling back to disk-based pagination for snapshot {}", snapshotId);
            return findWithFilterAndPagination(snapshotId, filter, page, size);
        }
        
        SnapshotCache cache = ensureSnapshotInCache(snapshotId);
        if (cache == null) {
            logger.warn("CACHE FALLBACK: Snapshot {} too large for cache, using disk queries", snapshotId);
            return findWithFilterAndPagination(snapshotId, filter, page, size);
        }
        
        logger.debug("ULTRA-FAST PAGINATION: Processing page {} (size {}) on {} cached records", 
                   page, size, cache.getRecordCount());
        
        // Filter and paginate in memory - ultra fast!
        List<ValidationStatObservationParquet> results = cache.getAllRecords().stream()
            .filter(record -> matchesFilter(record, filter))
            .skip((long) page * size)
            .limit(size)
            .collect(Collectors.toList());
        
        logger.debug("ULTRA-FAST PAGINATION: Returning {} results for page {}", results.size(), page);
        return results;
    }
    
    /**
     * IN-MEMORY FILTER: Apply filter to individual record (ultra-fast)
     */
    private boolean matchesFilter(ValidationStatObservationParquet record, AggregationFilter filter) {
        if (filter == null) {
            return true;
        }
        
        // Apply filters
        if (filter.getRecordOAIId() != null && !filter.getRecordOAIId().equals(record.getIdentifier())) {
            return false;
        }
        
        if (filter.getIsValid() != null && !filter.getIsValid().equals(record.getIsValid())) {
            return false;
        }
        
        if (filter.getIsTransformed() != null && !filter.getIsTransformed().equals(record.getIsTransformed())) {
            return false;
        }
        
        if (filter.getValidRulesFilter() != null) {
            List<String> validRules = record.getValidRulesIDList();
            if (validRules == null || !validRules.contains(filter.getValidRulesFilter())) {
                return false;
            }
        }
        
        if (filter.getInvalidRulesFilter() != null) {
            List<String> invalidRules = record.getInvalidRulesIDList();
            if (invalidRules == null || !invalidRules.contains(filter.getInvalidRulesFilter())) {
                return false;
            }
        }
        
        return true;
    }
    
    // ==================== CACHE MANAGEMENT API ====================
    
    /**
     * CACHE INFO: Get cache statistics and status
     */
    public Map<String, Object> getMemoryCacheInfo() {
        Map<String, Object> info = new HashMap<>();
        info.put("enabled", memoryCacheEnabled);
        info.put("cachedSnapshots", snapshotMemoryCache.size());
        info.put("maxSnapshots", maxCachedSnapshots);
        info.put("maxRecordsPerSnapshot", maxRecordsPerSnapshot);
        info.put("cacheHits", cacheHits.get());
        info.put("cacheMisses", cacheMisses.get());
        info.put("cacheLoads", cacheLoads.get());
        
        // Hit ratio
        long totalRequests = cacheHits.get() + cacheMisses.get();
        double hitRatio = totalRequests > 0 ? (double) cacheHits.get() / totalRequests * 100 : 0;
        info.put("hitRatio", String.format("%.2f%%", hitRatio));
        
        // Snapshot details
        Map<Long, Map<String, Object>> snapshotDetails = new HashMap<>();
        for (Map.Entry<Long, SnapshotCache> entry : snapshotMemoryCache.entrySet()) {
            Long snapshotId = entry.getKey();
            SnapshotCache cache = entry.getValue();
            Map<String, Object> details = new HashMap<>();
            details.put("recordCount", cache.getRecordCount());
            details.put("loadedTimestamp", cache.getLoadedTimestamp());
            details.put("estimatedSizeMB", estimateMemoryUsage(Collections.emptyList()));
            snapshotDetails.put(snapshotId, details);
        }
        info.put("snapshotDetails", snapshotDetails);
        
        return info;
    }
    
    /**
     * CACHE CONTROL: Manually warm up cache for a snapshot
     */
    public void warmUpCache(Long snapshotId) throws IOException {
        if (!memoryCacheEnabled) {
            logger.warn("CACHE WARMUP: Memory cache is disabled");
            return;
        }
        
        logger.info("CACHE WARMUP: Manually warming up cache for snapshot {}", snapshotId);
        SnapshotCache cache = ensureSnapshotInCache(snapshotId);
        if (cache != null) {
            logger.info("CACHE WARMUP: Successfully warmed up snapshot {} ({} records)", 
                       snapshotId, cache.getRecordCount());
        } else {
            logger.warn("CACHE WARMUP: Failed to warm up snapshot {} (too large?)", snapshotId);
        }
    }
    
    /**
     * CACHE CONTROL: Clear specific snapshot from cache
     */
    public void evictFromCache(Long snapshotId) {
        SnapshotCache evicted = snapshotMemoryCache.remove(snapshotId);
        cacheLoadTimestamp.remove(snapshotId);
        
        if (evicted != null) {
            logger.info("CACHE EVICT: Manually evicted snapshot {} from cache ({} records freed)", 
                       snapshotId, evicted.getRecordCount());
        } else {
            logger.debug("CACHE EVICT: Snapshot {} was not in cache", snapshotId);
        }
    }
    
    /**
     * CACHE CONTROL: Clear entire cache
     */
    public void clearMemoryCache() {
        int evictedCount = snapshotMemoryCache.size();
        snapshotMemoryCache.clear();
        cacheLoadTimestamp.clear();
        
        // Reset statistics
        cacheHits.set(0);
        cacheMisses.set(0);
        cacheLoads.set(0);
        
        logger.info("CACHE CLEAR: Cleared entire memory cache ({} snapshots freed)", evictedCount);
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
