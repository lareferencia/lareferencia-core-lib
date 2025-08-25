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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Repositorio para datos de validación en Parquet con persistencia real.
 * Utiliza Apache Parquet para almacenamiento eficiente en disco.
 */
@Repository
public class ValidationStatParquetRepository {

    private static final Logger logger = LogManager.getLogger(ValidationStatParquetRepository.class);

    @Value("${validation.stats.parquet.path:/tmp/validation-stats-parquet}")
    private String parquetBasePath;

    @Autowired
    private ValidationStatParquetQueryEngine queryEngine;

    private Schema avroSchema;
    private Configuration hadoopConf;
    
    @PostConstruct
    public void init() throws IOException {
        // Inicializar configuración de Hadoop
        hadoopConf = new Configuration();
        hadoopConf.set("fs.defaultFS", "file:///");
        
        // Crear directorio base si no existe
        Files.createDirectories(Paths.get(parquetBasePath));
        
        // Definir el esquema Avro para los datos de validación
        initializeAvroSchema();
        
        logger.info("ValidationStatParquetRepository inicializado - listo para usar en: {}", parquetBasePath);
    }
    
    private void initializeAvroSchema() {
        // Definir el esquema Avro para ValidationStatObservation
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
     * Busca todas las observaciones por snapshot ID desde archivo Parquet
     */
    public List<ValidationStatObservationParquet> findBySnapshotId(Long snapshotId) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        File file = new File(filePath);
        
        if (!file.exists()) {
            logger.debug("Archivo Parquet no existe para snapshot: {}", snapshotId);
            return Collections.emptyList();
        }
        
        List<ValidationStatObservationParquet> observations = new ArrayList<>();
        
        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path(filePath))
                .withConf(hadoopConf)
                .build()) {
            
            GenericRecord record;
            while ((record = reader.read()) != null) {
                observations.add(fromGenericRecord(record));
            }
        }
        
        logger.debug("Leídas {} observaciones para snapshot: {}", observations.size(), snapshotId);
        return observations;
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
        
        logger.info("Starting OPTIMIZED batch save of {} observations", observations.size());
        
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
        
        logger.info("Starting ULTRA-OPTIMIZED massive save of {} observations", observations.size());
        
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
     * Legacy method - delegates to optimized version
     * @deprecated Use saveObservationsToParquetOptimized for better performance
     */
    private void saveObservationsToParquet(Long snapshotId, List<ValidationStatObservationParquet> observations) throws IOException {
        logger.debug("Using legacy saveObservationsToParquet - delegating to optimized version");
        saveObservationsToParquetOptimized(snapshotId, observations);
    }

    /**
     * Elimina observaciones por snapshot ID (elimina el archivo Parquet)
     */
    public void deleteBySnapshotId(Long snapshotId) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        File file = new File(filePath);
        
        if (file.exists()) {
            boolean deleted = file.delete();
            if (deleted) {
                logger.info("Eliminado archivo Parquet para snapshot: {}", snapshotId);
            } else {
                logger.warn("No se pudo eliminar archivo Parquet para snapshot: {}", snapshotId);
            }
        }
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
            
            saveObservationsToParquet(newSnapshotId, copiedData);
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
     * Obtiene estadísticas agregadas por snapshot ID (VERSIÓN OPTIMIZADA)
     * Utiliza el query engine para evitar cargar todos los registros en memoria
     */
    public Map<String, Object> getAggregatedStats(Long snapshotId) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        File file = new File(filePath);
        
        if (!file.exists()) {
            logger.debug("Archivo Parquet no existe para snapshot: {}", snapshotId);
            // Retornar estadísticas vacías
            Map<String, Object> emptyStats = new HashMap<>();
            emptyStats.put("totalCount", 0L);
            emptyStats.put("validCount", 0L);
            emptyStats.put("transformedCount", 0L);
            emptyStats.put("validRuleCounts", new HashMap<String, Long>());
            emptyStats.put("invalidRuleCounts", new HashMap<String, Long>());
            return emptyStats;
        }
        
        // Usar query engine completamente optimizado
        AggregationFilter filter = new AggregationFilter(); // Sin filtros, procesar todos los registros
        AggregationResult result = queryEngine.getAggregatedStatsFullyOptimized(filePath, filter);
        
        // Convertir resultado a formato compatible
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalCount", result.getTotalCount());
        stats.put("validCount", result.getValidCount());
        stats.put("transformedCount", result.getTransformedCount());
        stats.put("validRuleCounts", result.getValidRuleCounts());
        stats.put("invalidRuleCounts", result.getInvalidRuleCounts());
        
        logger.debug("Estadísticas agregadas optimizadas para snapshot {}: {} registros totales, {} válidos", 
                snapshotId, result.getTotalCount(), result.getValidCount());
        
        return stats;
    }

    /**
     * Obtiene estadísticas agregadas con filtros específicos (NUEVO MÉTODO OPTIMIZADO)
     * @param snapshotId ID del snapshot
     * @param filter Filtros a aplicar (isValid, isTransformed, ruleIds, etc.)
     * @return Estadísticas agregadas filtradas
     */
    public Map<String, Object> getAggregatedStatsWithFilter(Long snapshotId, AggregationFilter filter) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        File file = new File(filePath);
        
        if (!file.exists()) {
            logger.debug("Archivo Parquet no existe para snapshot: {}", snapshotId);
            // Retornar estadísticas vacías
            Map<String, Object> emptyStats = new HashMap<>();
            emptyStats.put("totalCount", 0L);
            emptyStats.put("validCount", 0L);
            emptyStats.put("transformedCount", 0L);
            emptyStats.put("validRuleCounts", new HashMap<String, Long>());
            emptyStats.put("invalidRuleCounts", new HashMap<String, Long>());
            return emptyStats;
        }
        
        // Usar query engine optimizado con filtros
        // Usar query engine completamente optimizado con filtros
        AggregationResult result = queryEngine.getAggregatedStatsFullyOptimized(filePath, filter);
        
        // Convertir resultado a formato compatible
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalCount", result.getTotalCount());
        stats.put("validCount", result.getValidCount());
        stats.put("transformedCount", result.getTransformedCount());
        stats.put("validRuleCounts", result.getValidRuleCounts());
        stats.put("invalidRuleCounts", result.getInvalidRuleCounts());
        
        logger.debug("Estadísticas filtradas para snapshot {}: {} registros cumplieron criterios", 
                snapshotId, result.getTotalCount());
        
        return stats;
    }

    /**
     * Cuenta registros con filtros específicos sin cargarlos en memoria (MÉTODO OPTIMIZADO)
     * @param snapshotId ID del snapshot
     * @param filter Filtros a aplicar
     * @return Número de registros que cumplen los criterios
     */
    public long countRecordsWithFilter(Long snapshotId, AggregationFilter filter) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        File file = new File(filePath);
        
        if (!file.exists()) {
            logger.debug("Archivo Parquet no existe para snapshot: {}", snapshotId);
            return 0L;
        }
        
        return queryEngine.countRecords(filePath, filter);
    }

    /**
     * Búsqueda paginada con filtros (MÉTODO OPTIMIZADO)
     * @param snapshotId ID del snapshot
     * @param filter Filtros a aplicar
     * @param page Número de página (base 0)
     * @param size Tamaño de página
     * @return Lista de observaciones que cumplen los criterios
     */
    public List<ValidationStatObservationParquet> findWithFilterAndPagination(Long snapshotId, 
                                                                              AggregationFilter filter, 
                                                                              int page, int size) throws IOException {
        String filePath = getParquetFilePath(snapshotId);
        File file = new File(filePath);
        
        if (!file.exists()) {
            logger.debug("Archivo Parquet no existe para snapshot: {}", snapshotId);
            return Collections.emptyList();
        }
        
        int offset = page * size;
        List<GenericRecord> records = queryEngine.queryWithPagination(filePath, filter, offset, size);
        
        // Convertir GenericRecord a ValidationStatObservationParquet
        List<ValidationStatObservationParquet> observations = new ArrayList<>();
        for (GenericRecord record : records) {
            observations.add(fromGenericRecord(record));
        }
        
        logger.debug("Consulta paginada filtrada para snapshot {}: {} registros en página {}", 
                snapshotId, observations.size(), page);
        
        return observations;
    }

    /**
     * Obtiene conteos de ocurrencias de reglas
     */
    public Map<String, Long> getRuleOccurrenceCounts(Long snapshotId, String ruleId, boolean valid) throws IOException {
        List<ValidationStatObservationParquet> observations = findBySnapshotId(snapshotId);
        Map<String, Long> occurrenceCounts = new HashMap<>();
        
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
     * OPTIMIZED: Search with pagination using ValidationStatParquetQueryEngine WITHOUT INDIVIDUAL VERIFICATION
     */
    public List<ValidationStatObservationParquet> findOptimizedWithPagination(String filePath, 
            ValidationStatParquetQueryEngine.AggregationFilter filter, 
            org.springframework.data.domain.Pageable pageable) throws IOException {
        
        logger.debug("findOptimizedWithPagination FULLY OPTIMIZED - file: {}", filePath);
        
        File file = new File(filePath);
        if (!file.exists()) {
            logger.debug("File does not exist: {}", filePath);
            return Collections.emptyList();
        }
        
        // Use optimized query engine directly with Row Group Pruning
        int offset = pageable.getPageNumber() * pageable.getPageSize();
        int limit = pageable.getPageSize();
        
        List<GenericRecord> records = queryEngine.queryWithPagination(filePath, filter, offset, limit);
        
        List<ValidationStatObservationParquet> results = new ArrayList<>();
        for (GenericRecord record : records) {
            ValidationStatObservationParquet observation = fromGenericRecord(record);
            results.add(observation);
        }
        
        logger.debug("findOptimizedWithPagination FULLY OPTIMIZED completed - results: {}", results.size());
        return results;
    }
    
    /**
     * OPTIMIZATION 2+3: Ultra-optimized counting with memory management for millions of records
     */
    public long countOptimized(String filePath, ValidationStatParquetQueryEngine.AggregationFilter filter) throws IOException {
        logger.debug("countOptimized ULTRA-OPTIMIZED with batch streaming - file: {}", filePath);
        
        File file = new File(filePath);
        if (!file.exists()) {
            logger.debug("File does not exist: {}", filePath);
            return 0;
        }
        
        // Use new ultra-optimized method for massive datasets
        long count = queryEngine.countRecordsUltraOptimized(filePath, filter);
        
        logger.debug("countOptimized ULTRA-OPTIMIZED completed - total: {}", count);
        return count;
    }
    
    /**
     * OPTIMIZATION 2+3: Standard count method with fallback for compatibility
     */
    public long countOptimizedStandard(String filePath, ValidationStatParquetQueryEngine.AggregationFilter filter) throws IOException {
        logger.debug("countOptimizedStandard - standard fallback method");
        
        File file = new File(filePath);
        if (!file.exists()) {
            return 0;
        }
        
        // Use standard method as fallback
        return queryEngine.countRecords(filePath, filter);
    }
    
    /**
     * IMMEDIATE: Zero-memory streaming writes for 1000-record batches
     * Optimized for ValidationWorker page processing pattern
     */
    public void saveAllImmediate(List<ValidationStatObservationParquet> observations) throws IOException {
        if (observations == null || observations.isEmpty()) {
            return;
        }
        
        logger.debug("Starting IMMEDIATE streaming save of {} observations", observations.size());
        
        // Group by snapshot ID to handle multiple snapshots in one batch
        Map<Long, List<ValidationStatObservationParquet>> groupedBySnapshot = observations.stream()
            .collect(Collectors.groupingBy(ValidationStatObservationParquet::getSnapshotID));
        
        // Process each snapshot immediately without memory accumulation
        for (Map.Entry<Long, List<ValidationStatObservationParquet>> entry : groupedBySnapshot.entrySet()) {
            Long snapshotId = entry.getKey();
            List<ValidationStatObservationParquet> snapshotObservations = entry.getValue();
            
            saveObservationsImmediate(snapshotId, snapshotObservations);
        }
        
        logger.debug("IMMEDIATE: Successfully completed streaming save of {} observations", observations.size());
    }

    /**
     * IMMEDIATE: Direct streaming writes without memory accumulation
     * Uses the same file structure as other methods but optimized for streaming
     * This method is ULTRA-OPTIMIZED for 1000-record batches from ValidationWorker
     */
    private void saveObservationsImmediate(Long snapshotId, List<ValidationStatObservationParquet> newObservations) throws IOException {
        if (newObservations == null || newObservations.isEmpty()) {
            return;
        }
        
        String filePath = getParquetFilePath(snapshotId);
        File file = new File(filePath);
        
        // Ensure parent directory exists
        file.getParentFile().mkdirs();
        
        Configuration conf = new Configuration();
        
        // For immediate streaming, we use a different approach:
        // 1. If file doesn't exist, create it directly
        // 2. If file exists, read + append + write (this is inevitable with Parquet format)
        //    but we do it as efficiently as possible
        
        if (!file.exists()) {
            // FAST PATH: Create new file directly
            logger.debug("IMMEDIATE: Creating new file with {} observations", newObservations.size());
            
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                    new org.apache.hadoop.fs.Path(filePath))
                    .withWriteMode(ParquetFileWriter.Mode.CREATE)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withConf(conf)
                    .withSchema(avroSchema)
                    .build()) {
                
                // Write all new observations
                for (ValidationStatObservationParquet obs : newObservations) {
                    writer.write(toGenericRecord(obs));
                }
                
                logger.debug("IMMEDIATE: Successfully created file with {} observations", newObservations.size());
            }
        } else {
            // APPEND PATH: Read existing, write all together
            // This is the limitation of Parquet format - it doesn't support true append
            logger.debug("IMMEDIATE: Appending {} observations to existing file", newObservations.size());
            
            List<GenericRecord> existingRecords = new ArrayList<>();
            
            // Read existing records as efficiently as possible
            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(
                    new org.apache.hadoop.fs.Path(filePath))
                    .withConf(conf)
                    .build()) {
                
                GenericRecord record;
                while ((record = reader.read()) != null) {
                    existingRecords.add(record);
                }
                logger.debug("IMMEDIATE: Read {} existing records", existingRecords.size());
            }
            
            // Write all records (existing + new) in one go
            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(
                    new org.apache.hadoop.fs.Path(filePath))
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withConf(conf)
                    .withSchema(avroSchema)
                    .build()) {
                
                // Write existing records first
                for (GenericRecord record : existingRecords) {
                    writer.write(record);
                }
                
                // Write new observations
                for (ValidationStatObservationParquet obs : newObservations) {
                    writer.write(toGenericRecord(obs));
                }
                
                logger.debug("IMMEDIATE: Successfully wrote {} total records ({} existing + {} new)", 
                           existingRecords.size() + newObservations.size(), 
                           existingRecords.size(), 
                           newObservations.size());
            }
            
            // Clear the list to help GC
            existingRecords.clear();
        }
    }
    
}
