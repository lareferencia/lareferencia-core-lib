/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
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

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.lareferencia.backend.domain.parquet.FactOccurrence;
import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;
import org.lareferencia.backend.repositories.parquet.fact.FactOccurrencesReader;
import org.lareferencia.backend.repositories.parquet.fact.FactOccurrencesWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * FACT TABLE REPOSITORY: Repositorio basado en tabla de hechos para estadísticas de validación.
 * 
 * ARQUITECTURA REVOLUCIONARIA:
 * ================================
 * 
 * REPRESENTACIÓN 1 (FACT TABLE - ESTA IMPLEMENTACIÓN):
 * - 1 fila por cada ocurrencia de regla
 * - Esquema: (id, snapshot_id, rule_id, value, is_valid, ...)
 * - Particionado por: snapshot_id / network / is_valid
 * - Compresión ZSTD + Dictionary encoding
 * 
 * VENTAJAS:
 * ✓ Queries ultra-rápidos con predicate pushdown
 * ✓ Agregaciones nativas sin cargar registros
 * ✓ Escalabilidad lineal (millones de registros)
 * ✓ Storage eficiente (10-20x compresión típica)
 * ✓ Compatible con herramientas analíticas (Spark, Presto, etc.)
 * 
 * LAYOUT EN DISCO:
 * /base-path/
 *   snapshot_id=3577/
 *     network=CR/
 *       is_valid=true/
 *         part-00000.parquet
 *         part-00001.parquet
 *       is_valid=false/
 *         part-00000.parquet
 *     network=AR/
 *       ...
 * 
 * OPERACIONES PRINCIPALES:
 * - saveAll(): Convierte observaciones → filas fact con particionamiento
 * - getAggregatedStats(): Estadísticas sin cargar datos (predicate pushdown)
 * - findWithFilter(): Búsqueda optimizada con filtros columnares
 * - countRecords(): Conteo eficiente usando row group metadata
 */
@Repository
public class ValidationStatParquetRepository {

    private static final Logger logger = LogManager.getLogger(ValidationStatParquetRepository.class);

    @Value("${validation.stats.parquet.path:/tmp/validation-stats-parquet}")
    private String factBasePath;

    @Value("${parquet.validation.records-per-file:100000}")
    private int recordsPerFile;
    
    @Value("${parquet.validation.enable-dynamic-sizing:true}")
    private boolean enableDynamicSizing;

    private Configuration hadoopConf;

    // PARTITION MANAGEMENT: Track files per partition
    private final Map<String, Integer> partitionFileCounters = new ConcurrentHashMap<>();
    
    // BUFFER SYSTEM: Accumulate facts before writing
    private final Map<String, List<FactOccurrence>> partitionBuffers = new ConcurrentHashMap<>();
    
    // DYNAMIC SIZING: Track total records per snapshot for optimal file sizing
    private final Map<Long, Integer> snapshotRecordCounts = new ConcurrentHashMap<>();

    /**
     * Initializes Hadoop configuration and base directory structure
     */
    @PostConstruct
    public void init() throws IOException {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        // Create base directory
        Files.createDirectories(Paths.get(factBasePath));
        
        logger.info("========================================");
        logger.info("FACT REPOSITORY INITIALIZED");
        logger.info("Base path: {}", factBasePath);
        logger.info("Records per file (base): {}", recordsPerFile);
        logger.info("Dynamic sizing: {}", enableDynamicSizing ? "ENABLED" : "DISABLED");
        logger.info("========================================");
    }

    /**
     * DYNAMIC SIZING: Calcula el tamaño óptimo de archivo basado en el total de registros del snapshot
     * 
     * ESTRATEGIA:
     * - Snapshots pequeños (<100K): archivos ~50K registros (~3-5 MB)
     * - Snapshots medianos (100K-1M): archivos ~500K registros (~30-40 MB) 
     * - Snapshots grandes (1M-10M): archivos ~1M registros (~60-80 MB)
     * - Snapshots muy grandes (>10M): archivos ~2M registros (~120-150 MB)
     * 
     * BENEFICIOS:
     * - Evita file explosion en snapshots pequeños
     * - Optimiza row groups en snapshots grandes
     * - Balance automático entre I/O y metadata overhead
     * 
     * @param snapshotId ID del snapshot
     * @return número óptimo de registros por archivo
     */
    private int getOptimalRecordsPerFile(Long snapshotId) {
        if (!enableDynamicSizing) {
            return recordsPerFile; // Usar configuración fija si dynamic sizing está deshabilitado
        }
        
        Integer totalRecords = snapshotRecordCounts.get(snapshotId);
        if (totalRecords == null) {
            // Si no tenemos información, usar configuración base
            return recordsPerFile;
        }
        
        int optimal;
        if (totalRecords < 100_000) {
            optimal = 50_000;      // Snapshots pequeños: archivos ~3-5 MB
        } else if (totalRecords < 1_000_000) {
            optimal = 500_000;     // Snapshots medianos: archivos ~30-40 MB
        } else if (totalRecords < 10_000_000) {
            optimal = 1_000_000;   // Snapshots grandes: archivos ~60-80 MB
        } else {
            optimal = 2_000_000;   // Snapshots muy grandes: archivos ~120-150 MB
        }
        
        logger.debug("DYNAMIC SIZING: Snapshot {} has {} total records, using {} records/file", 
                    snapshotId, totalRecords, optimal);
        
        return optimal;
    }
    
    /**
     * Registra el conteo de registros para un snapshot (usado para dynamic sizing)
     * 
     * @param snapshotId ID del snapshot
     * @param totalRecords total de registros en el snapshot
     */
    public void registerSnapshotSize(Long snapshotId, int totalRecords) {
        snapshotRecordCounts.put(snapshotId, totalRecords);
        logger.info("DYNAMIC SIZING: Registered snapshot {} with {} total records", snapshotId, totalRecords);
    }

    /**
     * Cleanup: Flush remaining buffers before shutdown
     */
    @PreDestroy
    public void shutdown() {
        try {
            logger.info("FACT REPO: Shutting down - flushing remaining buffers");
            flushAllBuffers();
        } catch (Exception e) {
            logger.error("FACT REPO: Error during shutdown", e);
        }
    }

    /**
     * CORE SAVE: Converts ValidationStatObservationParquet to fact rows with partitioning
     * 
     * PROCESS:
     * 1. Convert each observation → N fact rows (explosion)
     * 2. Group rows by partition key (snapshot_id/network/is_valid)
     * 3. Buffer rows until reaching recordsPerFile
     * 4. Flush buffers to partitioned Parquet files
     * 
     * @param observations source observations to explode
     * @throws IOException if write fails
     */
    public void saveAll(List<ValidationStatObservationParquet> observations) throws IOException {
        if (observations == null || observations.isEmpty()) {
            logger.debug("FACT REPO: No observations to save");
            return;
        }

        logger.info("FACT REPO: Converting {} observations to fact table", observations.size());
        long startTime = System.currentTimeMillis();
        
        // Convert each observation to fact rows and buffer by partition
        for (ValidationStatObservationParquet obs : observations) {
            explodeAndBufferObservation(obs);
        }
        
        // Flush any remaining buffered data
        flushAllBuffers();
        
        long elapsed = System.currentTimeMillis() - startTime;
        logger.info("FACT REPO: Conversion completed in {}ms", elapsed);
    }

    /**
     * EXPLOSION LOGIC: Converts one observation to multiple fact rows
     * Groups rows by partition and buffers them
     */
    private void explodeAndBufferObservation(ValidationStatObservationParquet obs) throws IOException {
        // Validate required fields
        if (obs.getId() == null || obs.getSnapshotID() == null) {
            logger.warn("FACT REPO: Skipping observation with null id or snapshotID");
            return;
        }

        String network = obs.getNetworkAcronym() != null ? obs.getNetworkAcronym() : "UNKNOWN";
        
        // Process valid occurrences
        if (obs.getValidOccurrencesByRuleID() != null) {
            for (Map.Entry<String, List<String>> entry : obs.getValidOccurrencesByRuleID().entrySet()) {
                explodeRuleOccurrences(obs, entry.getKey(), entry.getValue(), true, network);
            }
        }
        
        // Process invalid occurrences
        if (obs.getInvalidOccurrencesByRuleID() != null) {
            for (Map.Entry<String, List<String>> entry : obs.getInvalidOccurrencesByRuleID().entrySet()) {
                explodeRuleOccurrences(obs, entry.getKey(), entry.getValue(), false, network);
            }
        }
    }

    /**
     * Explodes occurrences of a single rule into fact rows
     */
    private void explodeRuleOccurrences(ValidationStatObservationParquet obs, 
                                       String ruleIdStr, 
                                       List<String> values, 
                                       boolean isValid,
                                       String network) throws IOException {
        Integer ruleId = FactOccurrence.tryParseRuleId(ruleIdStr, obs.getId());
        if (ruleId == null) {
            return; // Skip invalid rule IDs
        }

        if (values == null || values.isEmpty()) {
            return; // No values to emit
        }

        // Get partition key for this combination
        String partitionKey = getPartitionKey(obs.getSnapshotID(), network, isValid);
        List<FactOccurrence> buffer = partitionBuffers.computeIfAbsent(partitionKey, k -> new ArrayList<>());

        // Create one row per unique value
        Set<String> dedupSet = new HashSet<>();
        for (String rawValue : values) {
            String normalizedValue = FactOccurrence.normalize(rawValue);
            String dedupKey = ruleId + "\u0001" + String.valueOf(normalizedValue);
            
            if (!dedupSet.add(dedupKey)) {
                continue; // Already processed this (rule_id, value)
            }

            FactOccurrence fact = FactOccurrence.builder()
                .id(obs.getId())
                .identifier(obs.getIdentifier())
                .snapshotId(obs.getSnapshotID())
                .origin(obs.getOrigin())
                .network(network)
                .repository(obs.getRepositoryName())
                .institution(obs.getInstitutionName())
                .ruleId(ruleId)
                .value(normalizedValue)
                .isValid(isValid)
                .recordIsValid(obs.getIsValid()) // Estado de validación del record completo
                .isTransformed(obs.getIsTransformed())
                .metadataPrefix(obs.getMetadataPrefix())
                .setSpec(obs.getSetSpec())
                .build();

            buffer.add(fact);

            // Flush buffer if it reaches the dynamic limit
            int optimalSize = getOptimalRecordsPerFile(obs.getSnapshotID());
            if (buffer.size() >= optimalSize) {
                flushPartitionBuffer(partitionKey);
            }
        }
    }

    /**
     * Generates partition key for grouping
     * Format: "snapshot_id={id}/network={net}/is_valid={valid}"
     */
    private String getPartitionKey(Long snapshotId, String network, Boolean isValid) {
        return String.format("snapshot_id=%d/network=%s/is_valid=%s", snapshotId, network, isValid);
    }

    /**
     * Flushes a specific partition buffer to disk
     */
    private synchronized void flushPartitionBuffer(String partitionKey) throws IOException {
        List<FactOccurrence> buffer = partitionBuffers.get(partitionKey);
        if (buffer == null || buffer.isEmpty()) {
            return;
        }

        // Get next file index for this partition
        int fileIndex = partitionFileCounters.compute(partitionKey, (k, v) -> v == null ? 0 : v + 1);
        
        // Build file path
        String filePath = factBasePath + "/" + partitionKey + "/part-" + String.format("%05d", fileIndex) + ".parquet";
        
        // Create parent directories
        File file = new File(filePath);
        file.getParentFile().mkdirs();

        logger.debug("FACT REPO: Flushing {} rows to {}", buffer.size(), filePath);

        // Write buffered rows
        try (FactOccurrencesWriter writer = FactOccurrencesWriter.newWriter(filePath, hadoopConf)) {
            for (FactOccurrence fact : buffer) {
                writer.writeRecord(convertToParquetObservation(fact));
            }
        }

        // Clear buffer
        buffer.clear();
        
        logger.info("FACT REPO: Wrote partition file {} with {} records", filePath, recordsPerFile);
    }

    /**
     * Flushes all partition buffers
     */
    public synchronized void flushAllBuffers() throws IOException {
        logger.info("FACT REPO: Flushing {} partition buffers", partitionBuffers.size());
        
        List<String> partitionKeys = new ArrayList<>(partitionBuffers.keySet());
        for (String partitionKey : partitionKeys) {
            flushPartitionBuffer(partitionKey);
        }
        
        logger.info("FACT REPO: All buffers flushed");
    }

    /**
     * HELPER: Converts FactOccurrence back to ValidationStatObservationParquet for writer
     * This is a temporary conversion - writer will re-explode it
     */
    private ValidationStatObservationParquet convertToParquetObservation(FactOccurrence fact) {
        Map<String, List<String>> occurrences = new HashMap<>();
        occurrences.put(String.valueOf(fact.getRuleId()), 
                       fact.getValue() != null ? Arrays.asList(fact.getValue()) : new ArrayList<>());

        Map<String, List<String>> validOccurrences = fact.getIsValid() ? occurrences : null;
        Map<String, List<String>> invalidOccurrences = !fact.getIsValid() ? occurrences : null;

        return new ValidationStatObservationParquet(
            fact.getId(),
            fact.getIdentifier(),
            fact.getSnapshotId(),
            fact.getOrigin(),
            fact.getSetSpec(),
            fact.getMetadataPrefix(),
            fact.getNetwork(),
            fact.getRepository(),
            fact.getInstitution(),
            fact.getIsValid(),
            fact.getIsTransformed(),
            validOccurrences,
            invalidOccurrences,
            fact.getIsValid() ? Arrays.asList(String.valueOf(fact.getRuleId())) : new ArrayList<>(),
            !fact.getIsValid() ? Arrays.asList(String.valueOf(fact.getRuleId())) : new ArrayList<>()
        );
    }

    /**
     * Gets all partition paths for a snapshot
     */
    private List<String> getAllPartitionPaths(Long snapshotId) throws IOException {
        String snapshotPath = factBasePath + "/snapshot_id=" + snapshotId;
        File snapshotDir = new File(snapshotPath);
        
        if (!snapshotDir.exists() || !snapshotDir.isDirectory()) {
            logger.debug("FACT REPO: No data found for snapshot {}", snapshotId);
            return new ArrayList<>();
        }

        List<String> paths = new ArrayList<>();
        
        // Walk through network partitions
        File[] networkDirs = snapshotDir.listFiles(File::isDirectory);
        if (networkDirs != null) {
            for (File networkDir : networkDirs) {
                // Walk through is_valid partitions
                File[] isValidDirs = networkDir.listFiles(File::isDirectory);
                if (isValidDirs != null) {
                    for (File isValidDir : isValidDirs) {
                        paths.add(isValidDir.getAbsolutePath());
                    }
                }
            }
        }

        logger.debug("FACT REPO: Found {} partition paths for snapshot {}", paths.size(), snapshotId);
        return paths;
    }

    /**
     * Gets all Parquet files within partition directories
     */
    private List<String> getAllParquetFiles(List<String> partitionPaths) {
        List<String> parquetFiles = new ArrayList<>();
        
        for (String partitionPath : partitionPaths) {
            File dir = new File(partitionPath);
            File[] files = dir.listFiles((d, name) -> name.endsWith(".parquet"));
            
            if (files != null) {
                for (File file : files) {
                    parquetFiles.add(file.getAbsolutePath());
                }
            }
        }
        
        logger.debug("FACT REPO: Found {} parquet files across partitions", parquetFiles.size());
        return parquetFiles;
    }

    /**
     * AGGREGATED STATS: Computes statistics without loading full data
     * Uses predicate pushdown and row group metadata
     */
    public Map<String, Object> getAggregatedStats(Long snapshotId) throws IOException {
        logger.debug("FACT REPO: Computing aggregated stats for snapshot {}", snapshotId);
        
        List<String> partitionPaths = getAllPartitionPaths(snapshotId);
        List<String> parquetFiles = getAllParquetFiles(partitionPaths);
        
        if (parquetFiles.isEmpty()) {
            return buildEmptyStats();
        }

        // Aggregation accumulators - using arrays to make them effectively final
        final long[] totalCount = {0};
        final long[] validCount = {0};
        final long[] transformedCount = {0};
        Map<String, Long> validRuleCounts = new HashMap<>();
        Map<String, Long> invalidRuleCounts = new HashMap<>();

        // Process each file with aggregation-only (no object materialization)
        FilterPredicate filter = FactOccurrencesReader.snapshotIdEquals(snapshotId);
        
        for (String filePath : parquetFiles) {
            try (FactOccurrencesReader reader = FactOccurrencesReader.newReader(filePath, hadoopConf, filter)) {
                reader.aggregateFromGroups(group -> {
                    // Extract fields directly from Group (no object creation)
                    boolean isValid = group.getBoolean("is_valid", 0);
                    boolean isTransformed = group.getBoolean("is_transformed", 0);
                    int ruleId = group.getInteger("rule_id", 0);
                    
                    // Update counters
                    synchronized (validRuleCounts) {
                        totalCount[0]++;
                        if (isValid) validCount[0]++;
                        if (isTransformed) transformedCount[0]++;
                        
                        String ruleIdStr = String.valueOf(ruleId);
                        if (isValid) {
                            validRuleCounts.merge(ruleIdStr, 1L, Long::sum);
                        } else {
                            invalidRuleCounts.merge(ruleIdStr, 1L, Long::sum);
                        }
                    }
                });
            }
        }

        Map<String, Object> stats = new HashMap<>();
        stats.put("totalCount", totalCount[0]);
        stats.put("validCount", validCount[0]);
        stats.put("transformedCount", transformedCount[0]);
        stats.put("validRuleCounts", validRuleCounts);
        stats.put("invalidRuleCounts", invalidRuleCounts);
        
        logger.debug("FACT REPO: Stats computed - total: {}, valid: {}, transformed: {}", 
                   totalCount[0], validCount[0], transformedCount[0]);
        
        return stats;
    }

    private Map<String, Object> buildEmptyStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalCount", 0L);
        stats.put("validCount", 0L);
        stats.put("transformedCount", 0L);
        stats.put("validRuleCounts", new HashMap<>());
        stats.put("invalidRuleCounts", new HashMap<>());
        return stats;
    }

    /**
     * Deletes all data for a snapshot by removing partition directories
     */
    public void deleteBySnapshotId(Long snapshotId) throws IOException {
        String snapshotPath = factBasePath + "/snapshot_id=" + snapshotId;
        File snapshotDir = new File(snapshotPath);
        
        if (!snapshotDir.exists()) {
            logger.debug("FACT REPO: No data to delete for snapshot {}", snapshotId);
            return;
        }

        logger.info("FACT REPO: Deleting all data for snapshot {}", snapshotId);
        deleteRecursively(snapshotDir);
        
        // Clear any buffers for this snapshot
        List<String> keysToRemove = partitionBuffers.keySet().stream()
            .filter(key -> key.startsWith("snapshot_id=" + snapshotId + "/"))
            .collect(Collectors.toList());
        keysToRemove.forEach(partitionBuffers::remove);
        keysToRemove.forEach(partitionFileCounters::remove);
        
        logger.info("FACT REPO: Snapshot {} deleted successfully", snapshotId);
    }

    private void deleteRecursively(File file) throws IOException {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        if (!file.delete()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath());
        }
    }

    /**
     * Counts records matching filter without loading data
     */
    public long countRecords(Long snapshotId, FilterPredicate filter) throws IOException {
        List<String> partitionPaths = getAllPartitionPaths(snapshotId);
        List<String> parquetFiles = getAllParquetFiles(partitionPaths);
        
        long totalCount = 0;
        
        for (String filePath : parquetFiles) {
            try (FactOccurrencesReader reader = FactOccurrencesReader.newReader(filePath, hadoopConf, filter)) {
                totalCount += reader.count();
            }
        }
        
        logger.debug("FACT REPO: Counted {} records for snapshot {} with filter", totalCount, snapshotId);
        return totalCount;
    }

    /**
     * Finds records with pagination and filtering
     */
    public List<FactOccurrence> findWithPagination(Long snapshotId, 
                                                    FilterPredicate filter,
                                                    int page, 
                                                    int size) throws IOException {
        List<String> partitionPaths = getAllPartitionPaths(snapshotId);
        List<String> parquetFiles = getAllParquetFiles(partitionPaths);
        
        int offset = page * size;
        int remaining = size;
        int currentOffset = offset;
        List<FactOccurrence> results = new ArrayList<>();

        for (String filePath : parquetFiles) {
            if (remaining <= 0) {
                break;
            }

            try (FactOccurrencesReader reader = FactOccurrencesReader.newReader(filePath, hadoopConf, filter)) {
                // Count records in this file first to determine if we can skip it
                long fileCount = reader.count();
                
                if (currentOffset >= fileCount) {
                    // Skip this entire file
                    currentOffset -= fileCount;
                    continue;
                }

                // Re-open reader to actually read records
                try (FactOccurrencesReader dataReader = FactOccurrencesReader.newReader(filePath, hadoopConf, filter)) {
                    List<FactOccurrence> pageResults = dataReader.readWithLimit((int) currentOffset, remaining);
                    results.addAll(pageResults);
                    remaining -= pageResults.size();
                    currentOffset = 0; // Reset offset after first file
                }
            }
        }

        logger.debug("FACT REPO: Retrieved {} records (page {}, size {}) for snapshot {}", 
                   results.size(), page, size, snapshotId);
        
        return results;
    }

    /**
     * Cleans a snapshot directory in preparation for new data
     */
    public synchronized void cleanSnapshot(Long snapshotId) throws IOException {
        deleteBySnapshotId(snapshotId);
        // Clear snapshot size tracking for dynamic sizing
        snapshotRecordCounts.remove(snapshotId);
        logger.info("FACT REPO: Snapshot {} cleaned and ready for new data", snapshotId);
    }

    // ==================== AGREGATION FILTER ====================
    
    /**
     * Filter class for aggregation and query operations
     */
    public static class AggregationFilter {
        private Long snapshotId;
        private Boolean isValid;
        private Boolean isTransformed;
        private String recordOAIId;
        private String validRulesFilter;
        private String invalidRulesFilter;

        public Long getSnapshotId() { return snapshotId; }
        public void setSnapshotId(Long snapshotId) { this.snapshotId = snapshotId; }
        public Boolean getIsValid() { return isValid; }
        public void setIsValid(Boolean isValid) { this.isValid = isValid; }
        public Boolean getIsTransformed() { return isTransformed; }
        public void setIsTransformed(Boolean isTransformed) { this.isTransformed = isTransformed; }
        public String getRecordOAIId() { return recordOAIId; }
        public void setRecordOAIId(String recordOAIId) { this.recordOAIId = recordOAIId; }
        public String getValidRulesFilter() { return validRulesFilter; }
        public void setValidRulesFilter(String validRulesFilter) { this.validRulesFilter = validRulesFilter; }
        public String getInvalidRulesFilter() { return invalidRulesFilter; }
        public void setInvalidRulesFilter(String invalidRulesFilter) { this.invalidRulesFilter = invalidRulesFilter; }
    }

    // ==================== MÉTODOS ADICIONALES PARA COMPATIBILIDAD ====================

    /**
     * Alias de saveAll para compatibilidad
     */
    public void saveAllImmediate(List<ValidationStatObservationParquet> observations) throws IOException {
        saveAll(observations);
    }

    /**
     * Get aggregated stats with filters
     */
    public Map<String, Object> getAggregatedStatsWithFilter(Long snapshotId, AggregationFilter filter) throws IOException {
        // Build predicate from filter
        FilterPredicate predicate = buildPredicateFromFilter(filter);
        
        // Use the same aggregation logic but with filter
        return getAggregatedStatsWithPredicate(snapshotId, predicate);
    }

    /**
     * Helper: Build predicate from AggregationFilter
     */
    private FilterPredicate buildPredicateFromFilter(AggregationFilter filter) {
        logger.debug("BUILD PREDICATE: Starting with filter -> snapshotId={}, isValid={}, isTransformed={}, recordOAIId={}", 
                    filter.getSnapshotId(), filter.getIsValid(), filter.getIsTransformed(), filter.getRecordOAIId());
        
        FilterPredicate predicate = null;

        if (filter.getSnapshotId() != null) {
            predicate = FactOccurrencesReader.snapshotIdEquals(filter.getSnapshotId());
            logger.debug("BUILD PREDICATE: Added snapshotId predicate: {}", filter.getSnapshotId());
        }

        if (filter.getIsValid() != null) {
            // IMPORTANTE: Filtramos por record_is_valid (estado del record completo), no por is_valid (estado de la ocurrencia)
            FilterPredicate validPredicate = FactOccurrencesReader.recordIsValidEquals(filter.getIsValid());
            predicate = predicate == null ? validPredicate : FactOccurrencesReader.and(predicate, validPredicate);
            logger.debug("BUILD PREDICATE: Added recordIsValid predicate: {}", filter.getIsValid());
        }

        if (filter.getIsTransformed() != null) {
            FilterPredicate transformedPredicate = FactOccurrencesReader.isTransformedEquals(filter.getIsTransformed());
            predicate = predicate == null ? transformedPredicate : FactOccurrencesReader.and(predicate, transformedPredicate);
            logger.debug("BUILD PREDICATE: Added isTransformed predicate: {}", filter.getIsTransformed());
        }

        if (filter.getRecordOAIId() != null) {
            FilterPredicate idPredicate = FactOccurrencesReader.identifierEquals(filter.getRecordOAIId());
            predicate = predicate == null ? idPredicate : FactOccurrencesReader.and(predicate, idPredicate);
            logger.debug("BUILD PREDICATE: Added identifier predicate: {}", filter.getRecordOAIId());
        }

        // Filtro por reglas válidas: busca ocurrencias donde rule_id = X AND is_valid = true
        if (filter.getValidRulesFilter() != null) {
            try {
                Integer ruleId = Integer.parseInt(filter.getValidRulesFilter());
                FilterPredicate ruleIdPredicate = FactOccurrencesReader.ruleIdEquals(ruleId);
                FilterPredicate isValidTrue = FactOccurrencesReader.isValidEquals(true);
                FilterPredicate validRulePredicate = FactOccurrencesReader.and(ruleIdPredicate, isValidTrue);
                predicate = predicate == null ? validRulePredicate : FactOccurrencesReader.and(predicate, validRulePredicate);
                logger.debug("BUILD PREDICATE: Added valid_rules predicate for rule_id: {}", ruleId);
            } catch (NumberFormatException e) {
                logger.warn("Invalid valid_rules filter value: {}", filter.getValidRulesFilter());
            }
        }

        // Filtro por reglas inválidas: busca ocurrencias donde rule_id = X AND is_valid = false
        if (filter.getInvalidRulesFilter() != null) {
            try {
                Integer ruleId = Integer.parseInt(filter.getInvalidRulesFilter());
                FilterPredicate ruleIdPredicate = FactOccurrencesReader.ruleIdEquals(ruleId);
                FilterPredicate isValidFalse = FactOccurrencesReader.isValidEquals(false);
                FilterPredicate invalidRulePredicate = FactOccurrencesReader.and(ruleIdPredicate, isValidFalse);
                predicate = predicate == null ? invalidRulePredicate : FactOccurrencesReader.and(predicate, invalidRulePredicate);
                logger.debug("BUILD PREDICATE: Added invalid_rules predicate for rule_id: {}", ruleId);
            } catch (NumberFormatException e) {
                logger.warn("Invalid invalid_rules filter value: {}", filter.getInvalidRulesFilter());
            }
        }
        
        logger.debug("BUILD PREDICATE: Final predicate built: {}", predicate != null ? predicate.toString() : "NULL");

        return predicate;
    }

    /**
     * Get aggregated stats with predicate
     */
    private Map<String, Object> getAggregatedStatsWithPredicate(Long snapshotId, FilterPredicate filter) throws IOException {
        List<String> partitionPaths = getAllPartitionPaths(snapshotId);
        List<String> parquetFiles = getAllParquetFiles(partitionPaths);
        
        if (parquetFiles.isEmpty()) {
            return buildEmptyStats();
        }

        final long[] totalCount = {0};
        final long[] validCount = {0};
        final long[] transformedCount = {0};
        Map<String, Long> validRuleCounts = new HashMap<>();
        Map<String, Long> invalidRuleCounts = new HashMap<>();

        for (String filePath : parquetFiles) {
            try (FactOccurrencesReader reader = FactOccurrencesReader.newReader(filePath, hadoopConf, filter)) {
                reader.aggregateFromGroups(group -> {
                    boolean isValid = group.getBoolean("is_valid", 0);
                    boolean isTransformed = group.getBoolean("is_transformed", 0);
                    int ruleId = group.getInteger("rule_id", 0);
                    
                    synchronized (validRuleCounts) {
                        totalCount[0]++;
                        if (isValid) validCount[0]++;
                        if (isTransformed) transformedCount[0]++;
                        
                        String ruleIdStr = String.valueOf(ruleId);
                        if (isValid) {
                            validRuleCounts.merge(ruleIdStr, 1L, Long::sum);
                        } else {
                            invalidRuleCounts.merge(ruleIdStr, 1L, Long::sum);
                        }
                    }
                });
            }
        }

        Map<String, Object> stats = new HashMap<>();
        stats.put("totalCount", totalCount[0]);
        stats.put("validCount", validCount[0]);
        stats.put("transformedCount", transformedCount[0]);
        stats.put("validRuleCounts", validRuleCounts);
        stats.put("invalidRuleCounts", invalidRuleCounts);
        
        return stats;
    }

    /**
     * Find with filter and pagination (returns fact occurrences, not observations)
     */
    public List<ValidationStatObservationParquet> findWithFilterAndPagination(Long snapshotId, 
                                                                             AggregationFilter filter,
                                                                             int page, 
                                                                             int size) throws IOException {
        FilterPredicate predicate = buildPredicateFromFilter(filter);
        List<FactOccurrence> facts = findWithPagination(snapshotId, predicate, page, size);
        
        // Convert facts to observations (simplified - may need grouping by id)
        return convertFactsToObservations(facts);
    }

    /**
     * Find from cache (stub - fact table is already fast)
     */
    public List<ValidationStatObservationParquet> findWithFilterAndPaginationFromCache(Long snapshotId,
                                                                                      AggregationFilter filter,
                                                                                      int page,
                                                                                      int size) throws IOException {
        // In fact table architecture, queries are already optimized
        // No need for complex caching - predicate pushdown is the "cache"
        return findWithFilterAndPagination(snapshotId, filter, page, size);
    }

    /**
     * Count with filter
     */
    public long countRecordsWithFilter(Long snapshotId, AggregationFilter filter) throws IOException {
        FilterPredicate predicate = buildPredicateFromFilter(filter);
        return countRecords(snapshotId, predicate);
    }

    /**
     * Count from cache (stub)
     */
    public long countRecordsWithFilterFromCache(Long snapshotId, AggregationFilter filter) throws IOException {
        return countRecordsWithFilter(snapshotId, filter);
    }

    /**
     * Find by snapshot with pagination (simplified - returns grouped observations)
     */
    public List<ValidationStatObservationParquet> findBySnapshotIdWithPagination(Long snapshotId, 
                                                                                 int page, 
                                                                                 int size) throws IOException {
        List<FactOccurrence> facts = findWithPagination(snapshotId, null, page, size);
        return convertFactsToObservations(facts);
    }

    /**
     * Count by snapshot
     */
    public long countBySnapshotId(Long snapshotId) throws IOException {
        return countRecords(snapshotId, FactOccurrencesReader.snapshotIdEquals(snapshotId));
    }

    /**
     * Get rule occurrence counts
     */
    public Map<String, Long> getRuleOccurrenceCounts(Long snapshotId, 
                                                     String ruleId, 
                                                     boolean valid,
                                                     Map<String, Object> filters) throws IOException {
        // Build filter predicate
        FilterPredicate predicate = FactOccurrencesReader.and(
            FactOccurrencesReader.snapshotIdEquals(snapshotId),
            FactOccurrencesReader.ruleIdEquals(Integer.parseInt(ruleId)),
            FactOccurrencesReader.isValidEquals(valid)
        );

        // Count occurrences by value
        Map<String, Long> occurrenceCounts = new HashMap<>();
        List<String> partitionPaths = getAllPartitionPaths(snapshotId);
        List<String> parquetFiles = getAllParquetFiles(partitionPaths);

        for (String filePath : parquetFiles) {
            try (FactOccurrencesReader reader = FactOccurrencesReader.newReader(filePath, hadoopConf, predicate)) {
                reader.stream(fact -> {
                    String value = fact.getValue();
                    if (value != null) {
                        occurrenceCounts.merge(value, 1L, Long::sum);
                    }
                });
            }
        }

        return occurrenceCounts;
    }

    /**
     * Delete by ID (requires scanning to find matching fact rows)
     */
    public void deleteById(String id, Long snapshotId) throws IOException {
        logger.warn("FACT REPO: deleteById() is expensive in fact table architecture - consider batch operations");
        // TODO: Implement if really needed - requires rewriting files without matching id
        throw new UnsupportedOperationException("deleteById not yet implemented for fact table");
    }

    /**
     * Copy snapshot data
     */
    public void copySnapshotData(Long originalSnapshotId, Long newSnapshotId) throws IOException {
        logger.warn("FACT REPO: copySnapshotData() is expensive - consider alternative approaches");
        // TODO: Implement if really needed - requires reading all partitions and rewriting with new snapshot_id
        throw new UnsupportedOperationException("copySnapshotData not yet implemented for fact table");
    }

    // ==================== CONVERSION HELPERS ====================

    /**
     * Convert FactOccurrences to ValidationStatObservationParquet
     * Groups facts by id to reconstruct observations
     */
    private List<ValidationStatObservationParquet> convertFactsToObservations(List<FactOccurrence> facts) {
        // Group facts by id
        Map<String, List<FactOccurrence>> factsById = facts.stream()
            .collect(Collectors.groupingBy(FactOccurrence::getId));

        // Convert each group to an observation
        List<ValidationStatObservationParquet> observations = new ArrayList<>();
        
        for (Map.Entry<String, List<FactOccurrence>> entry : factsById.entrySet()) {
            ValidationStatObservationParquet obs = convertFactGroupToObservation(entry.getValue());
            observations.add(obs);
        }

        return observations;
    }

    /**
     * Convert a group of facts (same id) to a single observation
     */
    private ValidationStatObservationParquet convertFactGroupToObservation(List<FactOccurrence> facts) {
        if (facts.isEmpty()) {
            return null;
        }

        // Use first fact for common fields
        FactOccurrence first = facts.get(0);

        // Rebuild occurrence maps
        Map<String, List<String>> validOccurrences = new HashMap<>();
        Map<String, List<String>> invalidOccurrences = new HashMap<>();
        List<String> validRules = new ArrayList<>();
        List<String> invalidRules = new ArrayList<>();

        for (FactOccurrence fact : facts) {
            String ruleId = String.valueOf(fact.getRuleId());
            String value = fact.getValue();

            if (fact.getIsValid()) {
                validOccurrences.computeIfAbsent(ruleId, k -> new ArrayList<>()).add(value);
                if (!validRules.contains(ruleId)) {
                    validRules.add(ruleId);
                }
            } else {
                invalidOccurrences.computeIfAbsent(ruleId, k -> new ArrayList<>()).add(value);
                if (!invalidRules.contains(ruleId)) {
                    invalidRules.add(ruleId);
                }
            }
        }

        return new ValidationStatObservationParquet(
            first.getId(),
            first.getIdentifier(),
            first.getSnapshotId(),
            first.getOrigin(),
            first.getSetSpec(),
            first.getMetadataPrefix(),
            first.getNetwork(),
            first.getRepository(),
            first.getInstitution(),
            first.getRecordIsValid(), // Usar el campo recordIsValid del registro original
            first.getIsTransformed(),
            validOccurrences,
            invalidOccurrences,
            validRules,
            invalidRules
        );
    }
}

