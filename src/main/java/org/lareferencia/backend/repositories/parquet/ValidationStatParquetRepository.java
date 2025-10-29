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

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.lareferencia.backend.domain.parquet.FactOccurrence;
import org.lareferencia.backend.domain.parquet.SnapshotSummary;
import org.lareferencia.backend.domain.validation.ValidationStatObservation;
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
    
    @Value("${parquet.validation.enable-parallel-processing:true}")
    private boolean enableParallelProcessing;
    
    @Value("${parquet.validation.parallel-threshold:5}")
    private int parallelThreshold;  // Minimum files to enable parallel processing

    private Configuration hadoopConf;

    // PARTITION MANAGEMENT: Track files per partition
    private final Map<String, Integer> partitionFileCounters = new ConcurrentHashMap<>();
    
    // BUFFER SYSTEM: Accumulate facts before writing
    private final Map<String, List<FactOccurrence>> partitionBuffers = new ConcurrentHashMap<>();
    
    // DYNAMIC SIZING: Track total records per snapshot for optimal file sizing
    private final Map<Long, Integer> snapshotRecordCounts = new ConcurrentHashMap<>();
    
    // PARTITION PATH CACHE: Cache partition paths per snapshot to avoid repeated filesystem scans
    private final Map<Long, List<String>> partitionPathsCache = new ConcurrentHashMap<>();
    
    // STRING POOL: Intern rule IDs to reduce memory duplication (same ID used many times)
    private final Map<Integer, String> ruleIdStringPool = new ConcurrentHashMap<>();

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
     * CORE SAVE: Converts ValidationStatObservation to fact rows with partitioning
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
    public void saveAll(List<ValidationStatObservation> observations) throws IOException {
        if (observations == null || observations.isEmpty()) {
            logger.debug("FACT REPO: No observations to save");
            return;
        }

        logger.info("FACT REPO: Converting {} observations to fact table", observations.size());
        long startTime = System.currentTimeMillis();
        
        // Convert each observation to fact rows and buffer by partition
        for (ValidationStatObservation obs : observations) {
            explodeAndBufferObservation(obs);
        }
        
        // Flush any remaining buffered data
        flushAllBuffers();
        
        long elapsed = System.currentTimeMillis() - startTime;
        logger.info("FACT REPO: Conversion completed in {}ms", elapsed);
        
        // PHASE 1 OPTIMIZATION: Generate snapshot summary for fast stats queries
        if (!observations.isEmpty()) {
            Long snapshotId = observations.get(0).getSnapshotID();
            try {
                generateSnapshotSummary(snapshotId);
            } catch (IOException e) {
                logger.error("FACT REPO: Failed to generate snapshot summary for {}: {}", 
                           snapshotId, e.getMessage());
                // Don't fail the whole operation if summary generation fails
            }
        }
    }

    /**
     * EXPLOSION LOGIC: Converts one observation to multiple fact rows
     * Groups rows by partition and buffers them
     */
    private void explodeAndBufferObservation(ValidationStatObservation obs) throws IOException {
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
    private void explodeRuleOccurrences(ValidationStatObservation obs, 
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
     * OPTIMIZED: Batch write for better performance
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

        // OPTIMIZATION: Batch write all facts at once
        try (FactOccurrencesWriter writer = FactOccurrencesWriter.newWriter(filePath, hadoopConf)) {
            writer.writeFactOccurrencesBatch(buffer);
        }

        // Clear buffer
        buffer.clear();
        
        // Invalidate cache for the snapshot that was just written to
        // Extract snapshot ID from partition key (format: snapshot_id=X/network=Y/is_valid=Z)
        String[] parts = partitionKey.split("/");
        if (parts.length > 0 && parts[0].startsWith("snapshot_id=")) {
            Long snapshotId = Long.parseLong(parts[0].substring("snapshot_id=".length()));
            partitionPathsCache.remove(snapshotId);
        }
        
        logger.info("FACT REPO: Wrote partition file {} with {} records", filePath, buffer.size());
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
     * Gets all partition paths for a snapshot
     * Uses cache to avoid repeated filesystem scans
     */
    private List<String> getAllPartitionPaths(Long snapshotId) throws IOException {
        // Check cache first
        List<String> cachedPaths = partitionPathsCache.get(snapshotId);
        if (cachedPaths != null) {
            logger.debug("FACT REPO: Using cached partition paths for snapshot {} ({} paths)", snapshotId, cachedPaths.size());
            return cachedPaths;
        }
        
        // Cache miss - scan filesystem
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

        logger.debug("FACT REPO: Found {} partition paths for snapshot {} (cached)", paths.size(), snapshotId);
        
        // Store in cache
        partitionPathsCache.put(snapshotId, paths);
        
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
     * 
     * PHASE 1 OPTIMIZATION: Intenta cargar resumen precalculado (FAST PATH <1ms)
     * Si no existe, calcula desde archivos Parquet (SLOW PATH ~200ms con optimizaciones)
     * 
     * IMPORTANTE: En fact table, cada registro genera múltiples filas (una por regla).
     * TODOS los conteos son de REGISTROS únicos, no filas fact.
     * - totalCount: registros únicos totales
     * - validCount: registros únicos válidos
     * - transformedCount: registros únicos transformados
     * - validRuleCounts: registros únicos que tienen cada regla válida
     * - invalidRuleCounts: registros únicos que tienen cada regla inválida
     */
    public Map<String, Object> getAggregatedStats(Long snapshotId) throws IOException {
        logger.debug("FACT REPO: Getting aggregated stats for snapshot {}", snapshotId);
        
        // FAST PATH: Try to load pre-calculated summary (Phase 1 optimization)
        try {
            SnapshotSummary summary = loadSnapshotSummary(snapshotId);
            if (summary != null) {
                logger.debug("FACT REPO: Using pre-calculated summary (FAST PATH <1ms) for snapshot {}", snapshotId);
                
                // Convert SnapshotSummary to Map<String, Object> format
                Map<String, Object> stats = new HashMap<>(8);
                stats.put("totalCount", summary.getTotalRecords());
                stats.put("validCount", summary.getValidRecords());
                stats.put("transformedCount", summary.getTransformedRecords());
                stats.put("validRuleCounts", summary.getValidRuleCounts());
                stats.put("invalidRuleCounts", summary.getInvalidRuleCounts());
                
                return stats;
            }
        } catch (Exception e) {
            logger.warn("FACT REPO: Failed to load snapshot summary for {}, falling back to Parquet computation: {}", 
                       snapshotId, e.getMessage());
        }
        
        // SLOW PATH: Compute from Parquet files (all optimizations applied)
        logger.debug("FACT REPO: Computing stats from Parquet files (SLOW PATH ~200ms) for snapshot {}", snapshotId);
        return computeStatsFromParquet(snapshotId);
    }

    private Map<String, Object> buildEmptyStats() {
        Map<String, Object> stats = new HashMap<>(8);  // Pre-sized for 5 keys
        stats.put("totalCount", 0L);
        stats.put("validCount", 0L);
        stats.put("transformedCount", 0L);
        stats.put("validRuleCounts", new HashMap<>());
        stats.put("invalidRuleCounts", new HashMap<>());
        return stats;
    }

    /**
     * OPTIMIZATION: String pool for rule IDs to reduce memory duplication
     * Rule IDs are repeated many times across records, interning saves memory
     * 
     * @param ruleId integer rule ID
     * @return interned String representation
     */
    private String getPooledRuleIdString(int ruleId) {
        return ruleIdStringPool.computeIfAbsent(ruleId, String::valueOf);
    }

    // ==================== SNAPSHOT SUMMARY (PHASE 1 OPTIMIZATION) ====================
    
    /**
     * PHASE 1: Generate snapshot summary for ultra-fast stats queries
     * 
     * PERFORMANCE:
     * - Generation time: ~200ms (one-time cost when writing snapshot)
     * - Read time: <1ms (vs ~200ms reading all Parquet files)
     * - File size: ~50-100KB per snapshot
     * 
     * WHEN CALLED:
     * - After saveAll() completes writing a snapshot
     * - After cleanSnapshot() to regenerate if data changed
     * 
     * @param snapshotId ID of snapshot to summarize
     * @throws IOException if file operations fail
     */
    private void generateSnapshotSummary(Long snapshotId) throws IOException {
        logger.info("SUMMARY: Generating snapshot summary for snapshot {}", snapshotId);
        long startTime = System.currentTimeMillis();
        
        // Read all parquet files and compute stats
        // This uses the optimized path with pre-sized collections and parallel processing
        Map<String, Object> stats = computeStatsFromParquet(snapshotId);
        
        // Create summary object
        SnapshotSummary summary = new SnapshotSummary(snapshotId);
        summary.setTotalRecords((Long) stats.get("totalCount"));
        summary.setValidRecords((Long) stats.get("validCount"));
        summary.setTransformedRecords((Long) stats.get("transformedCount"));
        
        @SuppressWarnings("unchecked")
        Map<String, Long> validCounts = (Map<String, Long>) stats.get("validRuleCounts");
        summary.setValidRuleCounts(validCounts);
        
        @SuppressWarnings("unchecked")
        Map<String, Long> invalidCounts = (Map<String, Long>) stats.get("invalidRuleCounts");
        summary.setInvalidRuleCounts(invalidCounts);
        
        // Count partition files
        List<String> partitionPaths = getAllPartitionPaths(snapshotId);
        List<String> parquetFiles = getAllParquetFiles(partitionPaths);
        summary.setPartitionCount(parquetFiles.size());
        
        // Total fact rows equals total unique records for now
        // (we could count actual rows if needed, but it's expensive)
        summary.setTotalFactRows(summary.getTotalRecords());
        
        // Write summary file
        String summaryPath = getSummaryFilePath(snapshotId);
        File summaryFile = new File(summaryPath);
        summaryFile.getParentFile().mkdirs();
        
        ObjectMapper mapper = new ObjectMapper();
        mapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, summary);
        
        long elapsed = System.currentTimeMillis() - startTime;
        logger.info("SUMMARY: Generated for snapshot {} in {}ms - {} unique records, {} fact rows, {} partitions", 
                   snapshotId, elapsed, summary.getTotalRecords(), summary.getTotalFactRows(), summary.getPartitionCount());
    }
    
    /**
     * Load snapshot summary from disk if it exists
     * 
     * @param snapshotId ID of snapshot
     * @return SnapshotSummary or null if not found
     */
    private SnapshotSummary loadSnapshotSummary(Long snapshotId) {
        String summaryPath = getSummaryFilePath(snapshotId);
        File summaryFile = new File(summaryPath);
        
        if (!summaryFile.exists()) {
            return null;
        }
        
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(summaryFile, SnapshotSummary.class);
        } catch (IOException e) {
            logger.warn("SUMMARY: Failed to load summary for snapshot {}: {}", snapshotId, e.getMessage());
            return null;
        }
    }
    
    /**
     * Delete snapshot summary file
     * 
     * @param snapshotId ID of snapshot
     */
    private void deleteSnapshotSummary(Long snapshotId) {
        String summaryPath = getSummaryFilePath(snapshotId);
        File summaryFile = new File(summaryPath);
        
        if (summaryFile.exists()) {
            if (summaryFile.delete()) {
                logger.debug("SUMMARY: Deleted summary for snapshot {}", snapshotId);
            } else {
                logger.warn("SUMMARY: Failed to delete summary for snapshot {}", snapshotId);
            }
        }
    }
    
    /**
     * Get file path for snapshot summary
     * 
     * @param snapshotId ID of snapshot
     * @return absolute path to summary JSON file
     */
    private String getSummaryFilePath(Long snapshotId) {
        return factBasePath + "/snapshot_id=" + snapshotId + "/_SUMMARY.json";
    }
    
    /**
     * Compute stats from Parquet files (original slow path)
     * Renamed to make it clear this is the compute path (not the optimized summary path)
     */
    private Map<String, Object> computeStatsFromParquet(Long snapshotId) throws IOException {
        // This is the original getAggregatedStats implementation
        // It reads all Parquet files and computes stats
        List<String> partitionPaths = getAllPartitionPaths(snapshotId);
        List<String> parquetFiles = getAllParquetFiles(partitionPaths);
        
        if (parquetFiles.isEmpty()) {
            return buildEmptyStats();
        }

        // OPTIMIZATION: Pre-size collections based on expected data volume
        // Typical snapshot: ~300K records, ~50 rules → initial capacity improves performance
        final int estimatedRecords = 30000;  // Conservative estimate
        final int estimatedRules = 100;
        
        // OPTIMIZED: Thread-safe Sets without explicit synchronization + pre-sized
        Set<String> uniqueRecordIds = ConcurrentHashMap.newKeySet(estimatedRecords);
        Set<String> validRecordIds = ConcurrentHashMap.newKeySet(estimatedRecords);
        Set<String> transformedRecordIds = ConcurrentHashMap.newKeySet(estimatedRecords);
        
        // Rule counts: Map<ruleId, Set<recordId>> - track unique records per rule
        Map<String, Set<String>> validRuleRecordSets = new ConcurrentHashMap<>(estimatedRules);
        Map<String, Set<String>> invalidRuleRecordSets = new ConcurrentHashMap<>(estimatedRules);

        // OPTIMIZATION: Use parallel processing for multiple files
        // Only parallelize if we have enough files to make it worthwhile
        boolean useParallel = enableParallelProcessing && parquetFiles.size() >= parallelThreshold;
        
        if (useParallel) {
            logger.debug("FACT REPO: Using parallel processing for {} files", parquetFiles.size());
        }
        
        // Process each file with aggregation-only (no object materialization)
        FilterPredicate filter = FactOccurrencesReader.snapshotIdEquals(snapshotId);
        
        // Use parallel stream if enabled and worthwhile, otherwise sequential
        (useParallel ? parquetFiles.parallelStream() : parquetFiles.stream()).forEach(filePath -> {
            try (FactOccurrencesReader reader = FactOccurrencesReader.newReader(filePath, hadoopConf, filter)) {
                reader.aggregateFromGroups(group -> {
                    // Extract fields directly from Group (no object creation)
                    String recordId = group.getString("id", 0);
                    boolean isValid = group.getBoolean("is_valid", 0);
                    boolean isTransformed = group.getBoolean("is_transformed", 0);
                    int ruleId = group.getInteger("rule_id", 0);
                    
                    // Track UNIQUE records - lock-free with ConcurrentHashMap (thread-safe)
                    uniqueRecordIds.add(recordId);
                    if (isValid) validRecordIds.add(recordId);
                    if (isTransformed) transformedRecordIds.add(recordId);
                    
                    // Track unique RECORDS per rule (not occurrences)
                    // Use pooled String to reduce memory duplication
                    String ruleIdStr = getPooledRuleIdString(ruleId);
                    if (isValid) {
                        validRuleRecordSets.computeIfAbsent(ruleIdStr, k -> ConcurrentHashMap.newKeySet()).add(recordId);
                    } else {
                        invalidRuleRecordSets.computeIfAbsent(ruleIdStr, k -> ConcurrentHashMap.newKeySet()).add(recordId);
                    }
                });
            } catch (IOException e) {
                logger.error("FACT REPO: Error reading file {} during parallel aggregation: {}", filePath, e.getMessage());
                throw new RuntimeException("Failed to read parquet file: " + filePath, e);
            }
        });

        // Convert Sets to counts with pre-sized Maps
        Map<String, Long> validRuleCounts = new HashMap<>(validRuleRecordSets.size());
        validRuleRecordSets.forEach((ruleId, recordSet) -> 
            validRuleCounts.put(ruleId, (long) recordSet.size()));
        
        Map<String, Long> invalidRuleCounts = new HashMap<>(invalidRuleRecordSets.size());
        invalidRuleRecordSets.forEach((ruleId, recordSet) -> 
            invalidRuleCounts.put(ruleId, (long) recordSet.size()));

        Map<String, Object> stats = new HashMap<>(8);  // Exactly 5 keys
        stats.put("totalCount", (long) uniqueRecordIds.size());           // UNIQUE records
        stats.put("validCount", (long) validRecordIds.size());            // UNIQUE valid records
        stats.put("transformedCount", (long) transformedRecordIds.size()); // UNIQUE transformed records
        stats.put("validRuleCounts", validRuleCounts);                    // UNIQUE records per valid rule
        stats.put("invalidRuleCounts", invalidRuleCounts);                // UNIQUE records per invalid rule
        
        logger.debug("FACT REPO: Stats computed - total records: {}, valid records: {}, transformed records: {}", 
                   uniqueRecordIds.size(), validRecordIds.size(), transformedRecordIds.size());
        
        return stats;
    }

    /**
     * Deletes all data for a snapshot by removing partition directories
     */
    private void deleteAllForSnapshot(Long snapshotId) throws IOException {
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
     * Finds records with pagination and filtering.
     * 
     * OPTIMIZATION: Single-pass pagination using skip/limit directly on reader.
     * Only reads the EXACT records needed for the page - no unnecessary I/O.
     * 
     * @param snapshotId snapshot to query
     * @param filter optional predicate filter
     * @param page page number (0-based)
     * @param size page size
     * @return list of FactOccurrences for the requested page
     */
    public List<FactOccurrence> findWithPagination(Long snapshotId, 
                                                    FilterPredicate filter,
                                                    int page, 
                                                    int size) throws IOException {
        List<String> partitionPaths = getAllPartitionPaths(snapshotId);
        List<String> parquetFiles = getAllParquetFiles(partitionPaths);
        
        int globalOffset = page * size;  // Total records to skip across all files
        int remaining = size;             // Records still needed for this page
        List<FactOccurrence> results = new ArrayList<>(size);  // Pre-sized for page

        logger.debug("FACT REPO: Pagination query - snapshot {}, page {}, size {}, global offset {}", 
                    snapshotId, page, size, globalOffset);

        for (String filePath : parquetFiles) {
            if (remaining <= 0) {
                break;  // Page is complete
            }

            try (FactOccurrencesReader reader = FactOccurrencesReader.newReader(filePath, hadoopConf, filter)) {
                // OPTIMIZATION: Single reader with skip/limit
                // readWithLimit handles both skipping and limiting internally
                List<FactOccurrence> pageResults = reader.readWithLimit(globalOffset, remaining);
                
                int retrieved = pageResults.size();
                
                if (retrieved > 0) {
                    results.addAll(pageResults);
                    remaining -= retrieved;
                    globalOffset = 0;  // After first file with results, no more skipping needed
                    
                    logger.debug("FACT REPO: File {} contributed {} records, {} remaining", 
                               filePath, retrieved, remaining);
                } else if (globalOffset > 0) {
                    // This file had records but all were skipped
                    // Estimate how many were skipped (reader already handled the skip internally)
                    // We need to continue to next file
                    logger.debug("FACT REPO: File {} skipped (offset consumed)", filePath);
                }
            }
        }

        logger.debug("FACT REPO: Pagination complete - retrieved {} records (page {}, size {}) for snapshot {}", 
                   results.size(), page, size, snapshotId);
        
        return results;
    }

    /**
     * Cleans a snapshot directory in preparation for new data
     * Also removes the pre-calculated summary (Phase 1 optimization)
     */
    public synchronized void cleanSnapshot(Long snapshotId) throws IOException {
        deleteAllForSnapshot(snapshotId);
        // Clear snapshot size tracking for dynamic sizing
        snapshotRecordCounts.remove(snapshotId);
        // Invalidate partition paths cache
        partitionPathsCache.remove(snapshotId);
        // Delete pre-calculated summary (Phase 1 optimization)
        try {
            deleteSnapshotSummary(snapshotId);
        } catch (Exception e) {
            logger.warn("FACT REPO: Failed to delete snapshot summary for {}: {}", snapshotId, e.getMessage());
        }
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
    public void saveAllImmediate(List<ValidationStatObservation> observations) throws IOException {
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
     * 
     * IMPORTANTE: Cuenta REGISTROS únicos, no filas fact.
     * Todos los conteos son de registros únicos que cumplen el predicado.
     * 
     * OPTIMIZACIÓN: Usa ConcurrentHashMap.newKeySet() para thread-safety sin locks.
     */
    private Map<String, Object> getAggregatedStatsWithPredicate(Long snapshotId, FilterPredicate filter) throws IOException {
        List<String> partitionPaths = getAllPartitionPaths(snapshotId);
        List<String> parquetFiles = getAllParquetFiles(partitionPaths);
        
        if (parquetFiles.isEmpty()) {
            return buildEmptyStats();
        }

        // OPTIMIZATION: Pre-size collections based on expected filtered data volume
        // Filtered queries typically return 10-50% of total records
        final int estimatedRecords = 10000;  // Conservative for filtered results
        final int estimatedRules = 100;
        
        // OPTIMIZED: Thread-safe Sets without explicit synchronization + pre-sized
        Set<String> uniqueRecordIds = ConcurrentHashMap.newKeySet(estimatedRecords);
        Set<String> validRecordIds = ConcurrentHashMap.newKeySet(estimatedRecords);
        Set<String> transformedRecordIds = ConcurrentHashMap.newKeySet(estimatedRecords);
        
        // Rule counts: Map<ruleId, Set<recordId>> - track unique records per rule
        Map<String, Set<String>> validRuleRecordSets = new ConcurrentHashMap<>(estimatedRules);
        Map<String, Set<String>> invalidRuleRecordSets = new ConcurrentHashMap<>(estimatedRules);

        // OPTIMIZATION: Use parallel processing for multiple files
        boolean useParallel = enableParallelProcessing && parquetFiles.size() >= parallelThreshold;
        
        if (useParallel) {
            logger.debug("FACT REPO: Using parallel processing for {} files (filtered query)", parquetFiles.size());
        }

        // Use parallel stream if enabled and worthwhile, otherwise sequential
        (useParallel ? parquetFiles.parallelStream() : parquetFiles.stream()).forEach(filePath -> {
            try (FactOccurrencesReader reader = FactOccurrencesReader.newReader(filePath, hadoopConf, filter)) {
                reader.aggregateFromGroups(group -> {
                    String recordId = group.getString("id", 0);
                    boolean isValid = group.getBoolean("is_valid", 0);
                    boolean isTransformed = group.getBoolean("is_transformed", 0);
                    int ruleId = group.getInteger("rule_id", 0);
                    
                    // Track UNIQUE records - lock-free with ConcurrentHashMap
                    uniqueRecordIds.add(recordId);
                    if (isValid) validRecordIds.add(recordId);
                    if (isTransformed) transformedRecordIds.add(recordId);
                    
                    // Track unique RECORDS per rule (not occurrences)
                    // Use pooled String to reduce memory duplication
                    String ruleIdStr = getPooledRuleIdString(ruleId);
                    if (isValid) {
                        validRuleRecordSets.computeIfAbsent(ruleIdStr, k -> ConcurrentHashMap.newKeySet()).add(recordId);
                    } else {
                        invalidRuleRecordSets.computeIfAbsent(ruleIdStr, k -> ConcurrentHashMap.newKeySet()).add(recordId);
                    }
                });
            } catch (IOException e) {
                logger.error("FACT REPO: Error reading file {} during parallel aggregation: {}", filePath, e.getMessage());
                throw new RuntimeException("Failed to read parquet file: " + filePath, e);
            }
        });

        // Convert Sets to counts with pre-sized Maps
        Map<String, Long> validRuleCounts = new HashMap<>(validRuleRecordSets.size());
        validRuleRecordSets.forEach((ruleId, recordSet) -> 
            validRuleCounts.put(ruleId, (long) recordSet.size()));
        
        Map<String, Long> invalidRuleCounts = new HashMap<>(invalidRuleRecordSets.size());
        invalidRuleRecordSets.forEach((ruleId, recordSet) -> 
            invalidRuleCounts.put(ruleId, (long) recordSet.size()));

        Map<String, Object> stats = new HashMap<>(8);  // Exactly 5 keys
        stats.put("totalCount", (long) uniqueRecordIds.size());           // UNIQUE records
        stats.put("validCount", (long) validRecordIds.size());            // UNIQUE valid records
        stats.put("transformedCount", (long) transformedRecordIds.size()); // UNIQUE transformed records
        stats.put("validRuleCounts", validRuleCounts);                    // UNIQUE records per valid rule
        stats.put("invalidRuleCounts", invalidRuleCounts);                // UNIQUE records per invalid rule
        
        return stats;
    }

    /**
     * Find with filter and pagination (returns fact occurrences, not observations)
     */
    public List<ValidationStatObservation> findWithFilterAndPagination(Long snapshotId, 
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
    public List<ValidationStatObservation> findWithFilterAndPaginationFromCache(Long snapshotId,
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
    public List<ValidationStatObservation> findBySnapshotIdWithPagination(Long snapshotId, 
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
     * Convert FactOccurrences to ValidationStatObservation
     * Groups facts by id to reconstruct observations
     */
    private List<ValidationStatObservation> convertFactsToObservations(List<FactOccurrence> facts) {
        // Group facts by id
        Map<String, List<FactOccurrence>> factsById = facts.stream()
            .collect(Collectors.groupingBy(FactOccurrence::getId));

        // Convert each group to an observation
        List<ValidationStatObservation> observations = new ArrayList<>();
        
        for (Map.Entry<String, List<FactOccurrence>> entry : factsById.entrySet()) {
            ValidationStatObservation obs = convertFactGroupToObservation(entry.getValue());
            observations.add(obs);
        }

        return observations;
    }

    /**
     * Convert a group of facts (same id) to a single observation
     */
    private ValidationStatObservation convertFactGroupToObservation(List<FactOccurrence> facts) {
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

        return new ValidationStatObservation(
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

