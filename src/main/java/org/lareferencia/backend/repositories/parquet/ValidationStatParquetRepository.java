/*
 *   Copyright (c) 2013-2025. LA Referencia / Red CLARA and others
 *
 *   This program is free software: you can redistribu    private void writeRecordsAndFacts(Long snapshotId, List<ValidationStatObservation> observations) 
            throws IOException {
        
        String snapshotDir = String.format("%s/snapshot_%d", basePath, snapshotId);
        
        // Write records (Layer 2) - estructura por snapshot, sin particiones
        try (RecordsWriter writer = RecordsWriter.newWriter(basePath, snapshotId, hadoopConf)) {
            for (ValidationStatObservation obs : observations) {
                writer.writeRecord(convertToRecordValidation(obs));
            }
        }ify
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
import org.lareferencia.backend.domain.parquet.RecordValidation;
import org.lareferencia.backend.domain.parquet.RuleFact;
import org.lareferencia.backend.domain.parquet.SnapshotValidationStats;
import org.lareferencia.core.metadata.RecordStatus;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * NUEVA ARQUITECTURA DE 3 CAPAS - REPOSITORIO DE VALIDACIÓN PARQUET
 * 
 * ARQUITECTURA REVOLUCIONARIA - ELIMINA EXPLOSIÓN DE FILAS:
 * ==========================================================
 * 
 * LAYER 1 - METADATA (JSON):
 * - metadata.json: Datos de snapshot guardados UNA SOLA VEZ
 * - Consultas ultra-rápidas (<1ms) para estadísticas agregadas
 * 
 * LAYER 2 - RECORDS (PARQUET):
 * - 1 fila por RECORD (no por occurrence)
 * - Arrays: validRuleIds[], invalidRuleIds[]
 * - Paginación CORRECTA: 20 filas = 20 records
 * - Reducción ~88% de espacio vs fact table
 * - Particionado: snapshot_id / record_is_valid
 * 
 * LAYER 3 - RULE FACTS (PARQUET):
 * - 1 fila por (record_id, rule_id) con array de occurrences
 * - Análisis detallado por regla específica
 * - Particionado: snapshot_id / is_valid
 * 
 * COMPATIBILIDAD:
 * - repositoryName e institutionName retornan null (eliminados de storage)
 * - API externa 100% compatible
 */
@Repository
public class ValidationStatParquetRepository {

    private static final Logger logger = LogManager.getLogger(ValidationStatParquetRepository.class);

    @Value("${validation.stats.parquet.path:/tmp/validation-stats-parquet}")
    private String basePath;

    private Configuration hadoopConf;
    private final Map<Long, SnapshotValidationStats> snapshotStatsCache = new ConcurrentHashMap<>();
    
    // MANAGER PERSISTENTE: Se reutiliza entre llamadas para aprovechar buffer de 10k
    private final Map<Long, ValidationRecordManager> recordsManagers = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        
        try {
            Files.createDirectories(Paths.get(basePath));
            logger.info("NEW ARCHITECTURE INITIALIZED: basePath={}", basePath);
        } catch (IOException e) {
            logger.error("Failed to create base path: {}", basePath, e);
        }
    }

    @PreDestroy
    public void cleanup() {
        // Cerrar todos los managers persistentes
        logger.info("REPOSITORY SHUTDOWN: Closing {} active managers", recordsManagers.size());
        
        recordsManagers.forEach((snapshotId, manager) -> {
            try {
                manager.close();
                logger.debug("Closed RecordsManager for snapshot {}", snapshotId);
            } catch (IOException e) {
                logger.error("Error closing RecordsManager for snapshot {}", snapshotId, e);
            }
        });
        
        recordsManagers.clear();
        snapshotStatsCache.clear();
        logger.info("REPOSITORY SHUTDOWN COMPLETE");
    }

    /**
    /**
     * Inicializa un snapshot para escritura.
     * 
     * CICLO DE VIDA:
     * 1. initializeSnapshot() → Crea metadata inicial, directorio y managers
     * 2. saveRecordAndFacts() × N → Escritura incremental record por record
     * 3. finalizeSnapshot() → Cierra managers y persiste metadata final
     * 
     * @param snapshotId ID del snapshot
     * @param networkAcronym acrónimo de la red
     * @throws IOException si hay error
     */
    public void initializeSnapshot(SnapshotMetadata snapshotMetadata) throws IOException {
        Long snapshotId = snapshotMetadata.getSnapshotId();
        logger.info("INITIALIZE SNAPSHOT: snapshot={}", snapshotId);
        
        try {
            // Crear directorio del snapshot
            String snapshotDir = String.format("%s/snapshot_%d", basePath, snapshotId);
            Files.createDirectories(Paths.get(snapshotDir));

            // Crear manager persistente para escritura
            ValidationRecordManager recordsManager = ValidationRecordManager.forWriting(basePath, snapshotId, hadoopConf);
            recordsManagers.put(snapshotId, recordsManager);
            logger.info("Created RecordsManager for snapshot {}", snapshotId);

            // create Snapshot Validation Stats
            SnapshotValidationStats snapshotStats = new SnapshotValidationStats(snapshotMetadata);
            
            SnapshotMetadataManager.writeValidationStats(snapshotDir, snapshotStats);
            snapshotStatsCache.put(snapshotId, snapshotStats);

            logger.info("Snapshot {} initialized and ready for writes", snapshotId);
            
        } catch (IOException e) {
            // Limpiar managers parcialmente creados en caso de error
            recordsManagers.remove(snapshotId);
            snapshotStatsCache.remove(snapshotId);
            logger.error("Failed to initialize snapshot {}", snapshotId, e);
            throw e;
        }
    }

    /**
     * Guarda un record y sus facts (LLAMADA INDIVIDUAL - TRANSPARENTE)
     * 
     * THREAD-SAFE: Múltiples threads pueden llamar concurrentemente.
     * Los writers internos manejan sincronización con synchronized.
     * 
     * BUFFERING AUTOMÁTICO:
     * - Acumula en memoria hasta 10k registros
     * - Auto-flush al alcanzar umbral
     * - Crea archivos batch_N.parquet automáticamente
     * 
     * @param snapshotId ID del snapshot
     * @param networkAcronym acrónimo de la red (para metadata)
     * @param record datos del record con facts integrados
     * @throws IOException si hay error
     */
    public void saveRecordAndFacts(Long snapshotId, String networkAcronym, RecordValidation record) throws IOException {
        
        if (record == null) {
            logger.warn("SAVE: Null record for snapshot {}", snapshotId);
            return;
        }
        
        // Actualizar metadata acumulativa
        updateStoredStats(snapshotId, record);
        
        // Escribir record con facts integrados
        writeRecord(snapshotId, record);
    }
    
    /**
     * Actualiza metadata de forma acumulativa (thread-safe)
     */
    /**
     * Actualiza metadata de forma acumulativa (thread-safe)
     */
    private synchronized void updateStoredStats(Long snapshotId, RecordValidation record) {
        
        // Obtener metadata del cache (debe existir porque initializeSnapshot() la crea)
        SnapshotValidationStats stats = snapshotStatsCache.get(snapshotId);
        if (stats == null) {
            throw new IllegalStateException(
                "Snapshot " + snapshotId + " not initialized. Call initializeSnapshot() first."
            );
        }
        
        updateStats(stats, record);
    }

    private synchronized void updateStats(SnapshotValidationStats snapshotStats, RecordValidation record) {
        
        // update total count
        snapshotStats.incrementTotalRecords();

        if (record.getRecordIsValid() != null && record.getRecordIsValid()) {
            snapshotStats.incrementValidRecords();
        }

        if (record.getIsTransformed() != null && record.getIsTransformed()) {
            snapshotStats.incrementTransformedRecords();
        }

        // Update facets
        // Facet: record_is_valid
        if (record.getRecordIsValid() != null) {
            snapshotStats.updateFacet("record_is_valid", record.getRecordIsValid().toString());
        }
        
        // Facet: record_is_transformed
        if (record.getIsTransformed() != null) {
            snapshotStats.updateFacet("record_is_transformed", record.getIsTransformed().toString());
        }

        // update rule stats and facets
        if (record.getRuleFacts() != null) {
            for (RuleFact fact : record.getRuleFacts()) {
                
                Long ruleID = Long.valueOf(fact.getRuleId());
                String ruleIdStr = ruleID.toString();

                if (fact.getIsValid() != null && fact.getIsValid()) {
                    snapshotStats.incrementRuleValid(ruleID);
                    // Facet: valid_rules
                    snapshotStats.updateFacet("valid_rules", ruleIdStr);
                } else {
                    snapshotStats.incrementRuleInvalid(ruleID);
                    // Facet: invalid_rules
                    snapshotStats.updateFacet("invalid_rules", ruleIdStr);
                }     
            }
        }
    }   

    // iterate over all records of a snapshot, create an empty snapshotValidationStats and update with each record
    public SnapshotValidationStats buildStats(SnapshotMetadata metadata, List<String> fq) throws IOException {
        
        SnapshotValidationStats stats = new SnapshotValidationStats(metadata);
        Long snapshotId = metadata.getSnapshotId();

        logger.debug("BUILD STATS: snapshot={}, filters={}", snapshotId, fq);

        // Parsear filtros UNA SOLA VEZ (optimización: pre-parsea todos los strings/integers)
        ParsedFilters filters = parseFilters(fq);
        logger.debug("BUILD STATS: Parsed filters -> isValid={}, isTransformed={}, invalidRules={}, validRules={}", 
                    filters.isValid, filters.isTransformed, filters.invalidRuleIds, filters.validRuleIds);

        // ITERACIÓN LAZY: Procesar records sin cargar todo en memoria
        try (ValidationRecordManager recordsManager = ValidationRecordManager.forReading(basePath, snapshotId, hadoopConf)) {
            
            long totalRecords = 0;
            long filteredRecords = 0;
            
            // Iterar sobre todos los records de forma lazy
            for (RecordValidation record : recordsManager) {
                totalRecords++;
                
                // Aplicar filtros y actualizar stats solo si cumple criterios
                if (matchesFilters(record, filters)) {
                    updateStats(stats, record);
                    filteredRecords++;
                }
                
                // Log de progreso cada 10k records
                if (totalRecords % 10000 == 0) {
                    logger.debug("BUILD STATS: Processed {} records, {} matched filters", 
                               totalRecords, filteredRecords);
                }
            }
            
            logger.info("BUILD STATS: Processed {} total records, {} matched filters (lazy iteration)", 
                       totalRecords, filteredRecords);
        }

        return stats;
    }

    /**
     * Clase interna para representar filtros pre-parseados.
     * Optimiza comparaciones evitando parseo repetido en cada record.
     */
    private static class ParsedFilters {
        Boolean isValid;
        Boolean isTransformed;
        Set<Integer> invalidRuleIds;
        Set<Integer> validRuleIds;
        
        boolean isEmpty() {
            return isValid == null && isTransformed == null 
                   && invalidRuleIds == null && validRuleIds == null;
        }
    }
    
    /**
     * Parsea filtros UNA SOLA VEZ convirtiendo strings a tipos nativos.
     * OPTIMIZACIÓN: Pre-parsea rule IDs a Sets de Integers para lookups O(1).
     */
    private ParsedFilters parseFilters(List<String> fq) {
        ParsedFilters filters = new ParsedFilters();
        
        if (fq == null || fq.isEmpty()) {
            return filters;
        }
        
        for (String filter : fq) {
            if (filter == null || filter.trim().isEmpty()) {
                continue;
            }
            
            // Formato esperado: "campo:valor"
            String[] parts = filter.split(":|@@", 2);
            if (parts.length != 2) {
                logger.warn("BUILD STATS: Invalid filter format (expected field:value or field@@value): {}", filter);
                continue;
            }
            
            String key = parts[0].trim();
            String value = parts[1].trim().replaceAll("^\"|\"$|^'|'$", "");
            
            switch (key) {
                case "record_is_valid":
                    filters.isValid = Boolean.parseBoolean(value);
                    break;
                    
                case "record_is_transformed":
                    filters.isTransformed = Boolean.parseBoolean(value);
                    break;
                    
                case "invalid_rules":
                    if (filters.invalidRuleIds == null) {
                        filters.invalidRuleIds = new HashSet<>();
                    }
                    parseRuleIds(value, filters.invalidRuleIds);
                    break;
                    
                case "valid_rules":
                    if (filters.validRuleIds == null) {
                        filters.validRuleIds = new HashSet<>();
                    }
                    parseRuleIds(value, filters.validRuleIds);
                    break;
                    
                default:
                    logger.warn("BUILD STATS: Unknown filter key: {}", key);
            }
        }
        
        return filters;
    }
    
    /**
     * Helper para parsear IDs de reglas (soporta múltiples valores separados por coma)
     */
    private void parseRuleIds(String value, Set<Integer> targetSet) {
        String[] ruleIds = value.split(",");
        for (String ruleIdStr : ruleIds) {
            try {
                targetSet.add(Integer.parseInt(ruleIdStr.trim()));
            } catch (NumberFormatException e) {
                logger.warn("BUILD STATS: Invalid rule ID: {}", ruleIdStr);
            }
        }
    }

    /**
     * Verifica si un record cumple con TODOS los filtros (lógica AND).
     * 
     * OPTIMIZACIONES:
     * - Filtros pre-parseados (no más split/parseInt por record)
     * - Verificación directa sin crear Sets intermedios innecesarios
     * - Short-circuit: retorna false en cuanto falla un criterio
     * - anyMatch() para chequeos de intersección eficientes
     */
    private boolean matchesFilters(RecordValidation record, ParsedFilters filters) {
        if (filters.isEmpty()) {
            return true;
        }
        
        if (filters.isValid != null) {
            if (!record.getRecordIsValid().equals(filters.isValid)) {
                return false;
            }
        }
        
        if (filters.isTransformed != null) {
            if (!record.getIsTransformed().equals(filters.isTransformed)) {
                return false; 
            }
        }
        
        // Si no hay filtros de reglas, ya pasó todos los criterios
        if (filters.invalidRuleIds == null && filters.validRuleIds == null) {
            return true;
        }
        
        // Verificar que el record tenga RuleFacts
        if (record.getRuleFacts() == null || record.getRuleFacts().isEmpty()) {
            // Sin facts, no puede cumplir filtros de reglas → falla criterios 3 y/o 4
            return false;
        }
        
        // CRITERIO 3: invalid_rules (AND - debe tener AL MENOS UNA regla inválida especificada)
        if (filters.invalidRuleIds != null) {
            // Verificar intersección: ¿el record tiene alguna de las reglas inválidas requeridas?
            boolean hasInvalidMatch = filters.invalidRuleIds.stream()
                .anyMatch(requiredRuleId -> 
                    record.getRuleFacts().stream()
                        .anyMatch(fact -> 
                            fact.getRuleId() != null 
                            && fact.getRuleId().equals(requiredRuleId)
                            && fact.getIsValid() != null 
                            && !fact.getIsValid() // Debe ser inválida
                        )
                );
            
            if (!hasInvalidMatch) {
                return false; // Falla criterio 3 → short-circuit
            }
        }
        
        // CRITERIO 4: valid_rules (AND - debe tener AL MENOS UNA regla válida especificada)
        if (filters.validRuleIds != null) {
            // Verificar intersección: ¿el record tiene alguna de las reglas válidas requeridas?
            boolean hasValidMatch = filters.validRuleIds.stream()
                .anyMatch(requiredRuleId -> 
                    record.getRuleFacts().stream()
                        .anyMatch(fact -> 
                            fact.getRuleId() != null 
                            && fact.getRuleId().equals(requiredRuleId)
                            && fact.getIsValid() != null 
                            && fact.getIsValid() // Debe ser válida
                        )
                );
            
            if (!hasValidMatch) {
                return false; // Falla criterio 4 → short-circuit
            }
        }
        
        // Pasó TODOS los criterios (AND completo)
        return true;
    }

    /**
     * Escribe un record usando el manager persistente
     */
    private void writeRecord(Long snapshotId, RecordValidation record) 
            throws IOException {
        
        // Obtener RecordsManager persistente (creado en initializeSnapshot)
        ValidationRecordManager recordsManager = recordsManagers.get(snapshotId);
        if (recordsManager == null) {
            throw new IllegalStateException("RecordsManager not found for snapshot " + snapshotId + ". Call initializeSnapshot() first.");
        }
        
        // Escribir record (ahora con facts integrados - synchronized internamente)
        recordsManager.writeRecord(record);
    }


    /**
     * Deletes all data for a snapshot (metadata, records, rule facts).
     * Removes the snapshot directory and clears cache.
     *
     * @param snapshotId the snapshot ID to delete
     * @throws IOException if deletion fails
     */
    public void deleteSnapshot(Long snapshotId) throws IOException {
        logger.info("DELETE SNAPSHOT: {}", snapshotId);

        // Finalize snapshot to close writers and persist metadata
        finalizeSnapshot(snapshotId);

        // Delete snapshot directory (removes metadata.json, records, rule facts)
        String snapshotDir = String.format("%s/snapshot_%d", basePath, snapshotId);
        if (Files.exists(Paths.get(snapshotDir))) {
            Files.walk(Paths.get(snapshotDir))
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            logger.error("Failed to delete {}", path, e);
                        }
                    });
        }
        snapshotStatsCache.remove(snapshotId);
        logger.info("SNAPSHOT DELETED: {}", snapshotId);
    }
    
    /**
     * Cierra los writers de un snapshot específico y libera recursos.
     * 
     * CICLO DE VIDA:
     * 1. initializeSnapshot() → Crea metadata inicial
     * 2. saveRecordAndFacts() × N → Escritura incremental
     * 3. finalizeSnapshot() → Cierra writers, flush final y persiste metadata
     * 
     * Debe llamarse cuando se termina de procesar completamente un snapshot.
     * 
     * IMPORTANTE: Después de cerrar, cualquier nueva escritura creará nuevos writers.
     * 
     * @param snapshotId el ID del snapshot a finalizar
     * @throws IOException si hay error al cerrar los writers
     */
    public void finalizeSnapshot(Long snapshotId) throws IOException {
        logger.info("FINALIZE SNAPSHOT: Closing writers for snapshot {}", snapshotId);
        
        // Escribir metadata final (última versión con totales exactos)
        SnapshotValidationStats stats = snapshotStatsCache.get(snapshotId);
        if (stats != null) {
            
            SnapshotMetadataManager.writeValidationStats(basePath, stats);
            logger.info("Final metadata written: total={}, valid={} ",  
                       stats.getTotalRecords(), stats.getValidRecords());
        }
        
        // Cerrar manager
        ValidationRecordManager recordsManager = recordsManagers.remove(snapshotId);
        if (recordsManager != null) {
            recordsManager.close();
            logger.info("RecordsManager closed: {} total records in {} batches", 
                        recordsManager.getTotalRecordsWritten(), recordsManager.getBatchCount());
        }
        
        logger.info("SNAPSHOT FINALIZED: {} (manager closed and data persisted)", snapshotId);
    }
    
   
    /**
     * Fuerza el flush de los buffers de escritura para un snapshot específico.
     * 
     * Los managers mantienen buffers de 10k registros para eficiencia.
     * Este método fuerza la escritura de cualquier dato pendiente en los buffers.
     * 
     * CUÁNDO USAR:
     * - Antes de leer datos que acabas de escribir
     * - Para garantizar durabilidad en puntos críticos
     * - Al finalizar un lote de procesamiento
     * 
     * NOTA: finalizeSnapshot() ya hace flush automáticamente al cerrar
     * 
     * @param snapshotId el ID del snapshot a hacer flush
     * @throws IOException si hay error en la operación de flush
     */
    public void flush(Long snapshotId) throws IOException {
        logger.debug("FLUSH requested for snapshot {}", snapshotId);
        
        ValidationRecordManager recordsManager = recordsManagers.get(snapshotId);
        if (recordsManager != null) {
            recordsManager.flush();
            logger.debug("Flushed RecordsManager for snapshot {}", snapshotId);
        }
        
        logger.info("FLUSH COMPLETE for snapshot {}", snapshotId);
    }
    
    
    /**
     * Obtiene las estadísticas de validación en formato interno para un snapshot.
     * 
     * Primero intenta obtener desde caché en memoria. Si no está disponible,
     * lee desde el archivo JSON en disco y actualiza el caché.
     * 
     * @param snapshotId el ID del snapshot
     * @return SnapshotValidationStats con las estadísticas, o null si no existen
     * @throws IOException si hay error al leer el archivo
     */
    public SnapshotValidationStats getSnapshotValidationStats(Long snapshotId) throws IOException {
        logger.debug("GET SNAPSHOT VALIDATION STATS: snapshot={}", snapshotId);
        
        // Intentar obtener desde caché primero
        SnapshotValidationStats snapshotStats = snapshotStatsCache.get(snapshotId);
        
        if (snapshotStats != null) {
            logger.debug("SNAPSHOT VALIDATION STATS FROM CACHE: snapshot={}, totalRecords={}", 
                        snapshotId, snapshotStats.getTotalRecords());
            return snapshotStats;
        }
        
        // No está en caché, leer desde disco
        snapshotStats = SnapshotMetadataManager.readValidationStats(basePath, snapshotId);
        
        if (snapshotStats == null) {
            logger.warn("SNAPSHOT VALIDATION STATS NOT FOUND: snapshot={}", snapshotId);
            return null;
        }
        
        // Actualizar caché para futuras consultas
        snapshotStatsCache.put(snapshotId, snapshotStats);
        
        logger.debug("SNAPSHOT VALIDATION STATS LOADED FROM DISK: snapshot={}, totalRecords={}", 
                    snapshotId, snapshotStats.getTotalRecords());
        return snapshotStats;
    }
    
    /**
     * Calcula las ocurrencias (válidas e inválidas) para una regla específica en un snapshot.
     * 
     * IMPLEMENTACIÓN:
     * - Lee TODOS los archivos batch del snapshot usando ValidationRecordManager
     * - Filtra records que tienen la regla especificada
     * - Agrega occurrences por valor (contando duplicados)
     * - Aplica filtros opcionales (fq) si se proporcionan
     * 
     * PERFORMANCE:
     * - Streaming: No carga todos los records en memoria
     * - Lazy iteration: Procesa record por record
     * - Solo lee campos necesarios: ruleFacts con occurrences
     * 
     * @param snapshotId ID del snapshot
     * @param ruleId ID de la regla a analizar
     * @param fq filtros opcionales (formato: "field@@value")
     * @return mapa con dos entradas: "valid" y "invalid", cada una con Map<String, Integer> de valores a conteos
     * @throws IOException si hay error leyendo los archivos Parquet
     */
    public Map<String, Map<String, Integer>> calculateRuleOccurrences(Long snapshotId, Integer ruleId, List<String> fq) 
            throws IOException {
        logger.info("CALCULATE RULE OCCURRENCES: snapshot={}, rule={}, filters={}", snapshotId, ruleId, fq);
        
        // Parsear filtros si existen
        ParsedFilters filters = parseFilters(fq != null ? fq : Collections.emptyList());
        logger.info("PARSED FILTERS: isValid={}, isTransformed={}, invalidRuleIds={}, validRuleIds={}", 
                   filters.isValid, filters.isTransformed, filters.invalidRuleIds, filters.validRuleIds);
        
        // Mapas para acumular occurrences: valor -> count
        Map<String, Integer> validOccurrences = new HashMap<>();
        Map<String, Integer> invalidOccurrences = new HashMap<>();
        
        long totalRecordsProcessed = 0;
        long recordsWithRule = 0;
        long recordsFilteredOut = 0;
        
        // Leer todos los records usando ValidationRecordManager (streaming)
        try (ValidationRecordManager reader = ValidationRecordManager.forReading(basePath, snapshotId, hadoopConf)) {
            
            for (RecordValidation record : reader) {
                totalRecordsProcessed++;
                
                // Aplicar filtros generales si existen
                if (!matchesFilters(record, filters)) {
                    recordsFilteredOut++;
                    continue;
                }
                
                // Buscar la regla específica en los RuleFacts del record
                if (record.getRuleFacts() == null) {
                    continue;
                }
                
                for (RuleFact fact : record.getRuleFacts()) {
                    if (fact.getRuleId() == null || !fact.getRuleId().equals(ruleId)) {
                        continue;
                    }
                    
                    recordsWithRule++;
                    
                    // Agregar valid occurrences
                    if (fact.getValidOccurrences() != null) {
                        for (String value : fact.getValidOccurrences()) {
                            validOccurrences.merge(value, 1, Integer::sum);
                        }
                    }
                    
                    // Agregar invalid occurrences
                    if (fact.getInvalidOccurrences() != null) {
                        for (String value : fact.getInvalidOccurrences()) {
                            invalidOccurrences.merge(value, 1, Integer::sum);
                        }
                    }
                }
            }
        }
        
        logger.info("RULE OCCURRENCES CALCULATED: processed={} records, filteredOut={}, found={} with rule {}, valid occurrences={}, invalid occurrences={}", 
                   totalRecordsProcessed, recordsFilteredOut, recordsWithRule, ruleId, validOccurrences.size(), invalidOccurrences.size());
        
        // Retornar resultado estructurado
        Map<String, Map<String, Integer>> result = new HashMap<>();
        result.put("valid", validOccurrences);
        result.put("invalid", invalidOccurrences);
        
        return result;
    }
    
    /**
     * Carga el índice ligero completo de un snapshot en memoria.
     * 
     * ÍNDICE LIGERO:
     * - RecordValidation sin ruleFacts (solo campos esenciales)
     * - ~35 bytes/record comprimido
     * - Carga rápida desde archivo validation_index.parquet
     * - Filtrado opcional por RecordStatus
     * 
     * USO RECOMENDADO:
     * - Queries rápidas en memoria
     * - Filtrados por identifier, recordId, isValid, isTransformed
     * - Estadísticas sin necesidad de cargar ruleFacts completos
     * 
     * @param snapshotId ID del snapshot
     * @param status Filtro por estado (VALID, INVALID, UNTESTED=todos, DELETED ignorado)
     * @return lista de RecordValidation ligeros filtrados
     * @throws IOException si hay error leyendo el índice
     */
    public List<RecordValidation> getRecordValidationListBySnapshotAndStatus(Long snapshotId, RecordStatus status) throws IOException {
        logger.info("GET RECORD VALIDATION LIST: snapshot={}, status={}", snapshotId, status);
        
        try (ValidationRecordManager reader = ValidationRecordManager.forReading(basePath, snapshotId, hadoopConf)) {
            List<RecordValidation> indexRecords = reader.loadLightweightIndex(status);
            logger.info("RECORD VALIDATION LIST LOADED: {} records for snapshot {} with status {}", 
                       indexRecords.size(), snapshotId, status);
            return indexRecords;
        }
    }
    
    /**
     * Consulta observaciones con paginación y filtrado.
     * 
     * ESTRATEGIA DE PAGINACIÓN CON STREAMING OPTIMIZADO:
     * - Lee records de forma lazy usando ValidationRecordManager
     * - Aplica filtros a cada record (misma lógica que buildStats)
     * - Acumula SOLO los records filtrados que corresponden a la página
     * - OPTIMIZACIÓN: Detiene iteración cuando completa la página (no recorre todo)
     * 
     * ALGORITMO:
     * 1. Iterar sobre records del snapshot (streaming)
     * 2. Aplicar filtros a cada record
     * 3. Si pasa filtros: asignar índice (0-based)
     * 4. Si índice está en rango [offset, limit): agregar a página
     * 5. Si página completa (size records): DETENER iteración
     * 6. Retornar página + total procesados hasta el momento
     * 
     * EJEMPLOS:
     * - Página 0, size 20: offset=0, limit=20 → índices [0-19] → detiene en record filtrado #20
     * - Página 1, size 20: offset=20, limit=40 → índices [20-39] → detiene en record filtrado #40
     * - Página 2, size 20: offset=40, limit=60 → índices [40-59] → detiene en record filtrado #60
     * 
     * PERFORMANCE:
     * - Página 0: procesa ~20 records (optimal)
     * - Página 100: procesa ~2020 records (skip 2000 + read 20)
     * - SIN recorrer los 13,400 records completos
     * 
     * @param snapshotId ID del snapshot
     * @param fq filtros opcionales
     * @param offset número de records filtrados a saltar (índice inicio, 0-based)
     * @param limit número total de records filtrados para completar página (offset + pageSize)
     * @return Map con "records" (List<RecordValidation>) y "totalFiltered" (Long = records procesados)
     * @throws IOException si hay error
     */
    public Map<String, Object> queryObservationsWithPagination(Long snapshotId, List<String> fq, int offset, int limit) 
            throws IOException {
        logger.info("QUERY OBSERVATIONS WITH PAGINATION: snapshot={}, offset={}, limit={}, filters={}", 
                   snapshotId, offset, limit, fq);
        
        // Parsear filtros UNA SOLA VEZ
        ParsedFilters filters = parseFilters(fq != null ? fq : Collections.emptyList());
        logger.debug("PAGINATION: Parsed filters -> isValid={}, isTransformed={}, invalidRules={}, validRules={}", 
                    filters.isValid, filters.isTransformed, filters.invalidRuleIds, filters.validRuleIds);
        
        List<RecordValidation> pageRecords = new ArrayList<>();
        long totalRecordsProcessed = 0;
        long totalFilteredRecords = 0;
        
        // STREAMING: Leer records de forma lazy
        // OPTIMIZACIÓN DE MEMORIA: Solo guardamos (add) los records de la página actual
        // PERO seguimos contando TODOS los filtrados para tener totalFiltered correcto
        try (ValidationRecordManager reader = ValidationRecordManager.forReading(basePath, snapshotId, hadoopConf)) {
            
            for (RecordValidation record : reader) {
                totalRecordsProcessed++;
                
                // Aplicar filtros
                if (!matchesFilters(record, filters)) {
                    continue; // No pasa filtros, siguiente record
                }
                
                // Record filtrado - incrementar contador (índice base 0: primer record filtrado = 0)
                long filteredIndex = totalFilteredRecords;
                totalFilteredRecords++;
                
                // OPTIMIZACIÓN: Solo agregamos a la lista si está en el rango de la página actual
                // Esto ahorra memoria al no almacenar records fuera del rango [offset, limit)
                // Página 0: offset=0, limit=20 → índices [0-19]
                // Página 1: offset=20, limit=40 → índices [20-39]
                if (filteredIndex >= offset && filteredIndex < limit) {
                    pageRecords.add(record);
                }
                // Si filteredIndex < offset → estamos ANTES de la página, solo contamos
                // Si filteredIndex >= limit → estamos DESPUÉS de la página, solo contamos
                // En ambos casos seguimos iterando para tener el total correcto
            }
        }
        
        logger.info("PAGINATION COMPLETED: processed={} records, filtered={}, returned={} for page", 
                   totalRecordsProcessed, totalFilteredRecords, pageRecords.size());
        
        // Retornar resultado con página y total REAL
        Map<String, Object> result = new HashMap<>();
        result.put("records", pageRecords);
        result.put("totalFiltered", totalFilteredRecords);
        
        return result;
    }
}