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

package org.lareferencia.core.repository.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.RecordStatus;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Servicio de consultas y cálculo de estadísticas sobre snapshots de validación.
 * 
 * RESPONSABILIDADES:
 * - Construcción de estadísticas con filtros (buildStats)
 * - Cálculo de ocurrencias por regla (calculateRuleOccurrences)
 * - Consultas paginadas (queryObservationsWithPagination)
 * - Obtención de estadísticas de snapshot
 * - Acceso a listas de records por estado
 * - Iteradores para procesamiento streaming
 * 
 * OPTIMIZACIONES:
 * - Usa SnapshotRecordsCache para evitar relecturas de Parquet
 * - Filtros pre-parseados para evitar parseo repetido por record
 * - Short-circuit en evaluación de filtros
 */
@Service
public class ValidationStatsQueryService {

    private static final Logger logger = LogManager.getLogger(ValidationStatsQueryService.class);

    @Value("${store.basepath:/tmp/data/}")
    private String basePath;

    @Autowired
    private ISnapshotStore snapshotStore;

    @Autowired
    private SnapshotRecordsCache recordsCache;

    private Configuration hadoopConf;

    @PostConstruct
    public void init() {
        hadoopConf = new Configuration();
        hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        logger.info("QUERY SERVICE: Initialized | BasePath: {}", basePath);
    }

    // ========================================
    // CLASES INTERNAS
    // ========================================

    /**
     * Clase interna para representar filtros pre-parseados.
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

    // ========================================
    // MÉTODOS DE ESTADÍSTICAS
    // ========================================

    /**
     * Construye estadísticas iterando sobre records con filtros opcionales.
     * 
     * @param metadata metadata del snapshot
     * @param fq       filtros opcionales
     * @return estadísticas calculadas
     * @throws IOException si hay error
     */
    public SnapshotValidationStats buildStats(SnapshotMetadata metadata, List<String> fq) throws IOException {
        SnapshotValidationStats stats = new SnapshotValidationStats(metadata);
        Long snapshotId = metadata.getSnapshotId();

        logger.debug("BUILD STATS: snapshot={}, filters={}", snapshotId, fq);

        ParsedFilters filters = parseFilters(fq);
        logger.debug("BUILD STATS: Parsed filters -> isValid={}, isTransformed={}, invalidRules={}, validRules={}",
                filters.isValid, filters.isTransformed, filters.invalidRuleIds, filters.validRuleIds);

        List<RecordValidation> cachedRecords = recordsCache.getRecords(snapshotId, metadata);
        
        long totalRecords = cachedRecords.size();
        long filteredRecords = 0;

        for (RecordValidation record : cachedRecords) {
            if (matchesFilters(record, filters)) {
                updateStats(stats, record);
                filteredRecords++;
            }
        }

        logger.debug("BUILD STATS: Processed {} total records, {} matched filters (from cache)",
                totalRecords, filteredRecords);

        return stats;
    }

    /**
     * Obtiene estadísticas de validación de un snapshot.
     * 
     * Siempre lee desde disco para evitar problemas de inconsistencia con versiones
     * anteriores cacheadas. El archivo JSON de stats es pequeño y la lectura es rápida.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @return estadísticas o null si no existen
     * @throws IOException si hay error
     */
    public SnapshotValidationStats getSnapshotValidationStats(SnapshotMetadata snapshotMetadata) throws IOException {
        if (snapshotMetadata == null || snapshotMetadata.getSnapshotId() == null) {
            throw new IllegalArgumentException("snapshotMetadata y snapshotId no pueden ser null");
        }

        Long snapshotId = snapshotMetadata.getSnapshotId();
        logger.debug("GET SNAPSHOT VALIDATION STATS: snapshot={}, network={}",
                snapshotId, snapshotMetadata.getNetwork().getAcronym());

        // Siempre leer desde disco para evitar inconsistencias con cache desactualizado
        SnapshotValidationStats snapshotStats = SnapshotMetadataManager.readValidationStats(basePath, snapshotMetadata);
        if (snapshotStats == null) {
            logger.warn("SNAPSHOT VALIDATION STATS NOT FOUND: snapshot={}, network={}",
                    snapshotId, snapshotMetadata.getNetwork().getAcronym());
            return null;
        }

        logger.debug("SNAPSHOT VALIDATION STATS LOADED FROM DISK: snapshot={}, network={}, totalRecords={}",
                snapshotId, snapshotMetadata.getNetwork().getAcronym(), snapshotStats.getTotalRecords());
        return snapshotStats;
    }

    /**
     * Calcula las ocurrencias para una regla específica.
     * 
     * @param snapshotId ID del snapshot
     * @param ruleId     ID de la regla
     * @param fq         filtros opcionales
     * @return mapa con "valid" e "invalid" ocurrencias
     * @throws IOException si hay error
     */
    public Map<String, Map<String, Integer>> calculateRuleOccurrences(Long snapshotId, Integer ruleId, List<String> fq)
            throws IOException {
        logger.debug("CALCULATE RULE OCCURRENCES: snapshot={}, rule={}, filters={}", snapshotId, ruleId, fq);

        ParsedFilters filters = parseFilters(fq != null ? fq : Collections.emptyList());
        
        Map<String, Integer> validOccurrences = new HashMap<>();
        Map<String, Integer> invalidOccurrences = new HashMap<>();

        long totalRecordsProcessed = 0;
        long recordsWithRule = 0;
        long recordsFilteredOut = 0;

        SnapshotMetadata snapshotMetadata = getSnapshotMetadata(snapshotId);
        List<RecordValidation> cachedRecords = recordsCache.getRecords(snapshotId, snapshotMetadata);

        for (RecordValidation record : cachedRecords) {
            totalRecordsProcessed++;

            if (!matchesFilters(record, filters)) {
                recordsFilteredOut++;
                continue;
            }

            if (record.getRuleFacts() == null) {
                continue;
            }

            for (RuleFact fact : record.getRuleFacts()) {
                if (fact.getRuleId() == null || !fact.getRuleId().equals(ruleId)) {
                    continue;
                }

                recordsWithRule++;

                if (fact.getValidOccurrences() != null) {
                    for (String value : fact.getValidOccurrences()) {
                        validOccurrences.merge(value, 1, Integer::sum);
                    }
                }

                if (fact.getInvalidOccurrences() != null) {
                    for (String value : fact.getInvalidOccurrences()) {
                        invalidOccurrences.merge(value, 1, Integer::sum);
                    }
                }
            }
        }

        logger.debug("RULE OCCURRENCES CALCULATED: processed={}, filteredOut={}, found={} with rule {}, valid={}, invalid={} (from cache)",
                totalRecordsProcessed, recordsFilteredOut, recordsWithRule, ruleId, 
                validOccurrences.size(), invalidOccurrences.size());

        Map<String, Map<String, Integer>> result = new HashMap<>();
        result.put("valid", validOccurrences);
        result.put("invalid", invalidOccurrences);
        return result;
    }

    // ========================================
    // MÉTODOS DE CONSULTA
    // ========================================

    /**
     * Consulta observaciones con paginación y filtrado.
     * 
     * @param snapshotId ID del snapshot
     * @param fq         filtros opcionales
     * @param offset     registros a saltar
     * @param limit      límite total
     * @return mapa con "records" y "totalFiltered"
     * @throws IOException si hay error
     */
    public Map<String, Object> queryObservationsWithPagination(Long snapshotId, List<String> fq, int offset, int limit)
            throws IOException {
        logger.debug("QUERY OBSERVATIONS WITH PAGINATION: snapshot={}, offset={}, limit={}, filters={}",
                snapshotId, offset, limit, fq);

        ParsedFilters filters = parseFilters(fq != null ? fq : Collections.emptyList());
        SnapshotMetadata snapshotMetadata = getSnapshotMetadata(snapshotId);
        List<RecordValidation> cachedRecords = recordsCache.getRecords(snapshotId, snapshotMetadata);

        int pageSize = limit - offset;

        List<RecordValidation> pageRecords = cachedRecords.stream()
                .filter(record -> matchesFilters(record, filters))
                .skip(offset)
                .limit(pageSize)
                .collect(Collectors.toList());

        long totalFilteredRecords = cachedRecords.stream()
                .filter(record -> matchesFilters(record, filters))
                .count();

        logger.debug("PAGINATION COMPLETED: total={} records, filtered={}, returned={} for page (from cache)",
                cachedRecords.size(), totalFilteredRecords, pageRecords.size());

        Map<String, Object> result = new HashMap<>();
        result.put("records", pageRecords);
        result.put("totalFiltered", totalFilteredRecords);
        return result;
    }

    /**
     * Obtiene lista de records por estado (usando índice ligero).
     * 
     * @param snapshotId ID del snapshot
     * @param status     estado a filtrar
     * @return lista de RecordValidation ligeros
     * @throws IOException si hay error
     */
    public List<RecordValidation> getRecordValidationListBySnapshotAndStatus(Long snapshotId, RecordStatus status)
            throws IOException {
        logger.debug("GET RECORD VALIDATION LIST: snapshot={}, status={}", snapshotId, status);

        SnapshotMetadata snapshotMetadata = getSnapshotMetadata(snapshotId);

        try (ValidationRecordManager reader = ValidationRecordManager.forReading(basePath, snapshotMetadata, hadoopConf)) {
            List<RecordValidation> indexRecords = reader.loadLightweightIndex(status);
            logger.debug("RECORD VALIDATION LIST LOADED: {} records for snapshot {} with status {}",
                    indexRecords.size(), snapshotId, status);
            return indexRecords;
        }
    }

    /**
     * Obtiene un record por snapshot e identifier.
     * 
     * OPTIMIZACIÓN: Usa el cache de records en lugar de leer desde disco,
     * aprovechando que el cache ya puede estar poblado por otras consultas.
     * 
     * @param snapshotId ID del snapshot
     * @param identifier identificador del record
     * @return RecordValidation o null
     */
    public RecordValidation getRecordValidationBySnapshotAndIdentifier(Long snapshotId, String identifier) {
        logger.debug("GET RECORD VALIDATION BY IDENTIFIER: snapshot={}, identifier={}", snapshotId, identifier);

        try {
            // Usar cache para evitar lecturas repetidas desde disco
            SnapshotMetadata metadata = getSnapshotMetadata(snapshotId);
            List<RecordValidation> cachedRecords = recordsCache.getRecords(snapshotId, metadata);
            
            return cachedRecords.stream()
                    .filter(record -> identifier.equals(record.getIdentifier()))
                    .findFirst()
                    .orElse(null);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error retrieving record validation for snapshot " + snapshotId + " and identifier " + identifier, e);
        }
    }

    /**
     * Retorna iterator completo para streaming sobre records.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @return iterator lazy
     * @throws IOException si hay error
     */
    public Iterator<RecordValidation> getIterator(SnapshotMetadata snapshotMetadata) throws IOException {
        logger.debug("GET ITERATOR: snapshot={}, network={} (FULL MODE)",
                snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetwork().getAcronym());
        return ValidationRecordManager.iterate(basePath, snapshotMetadata, hadoopConf);
    }

    /**
     * Retorna iterator ligero para streaming sobre records sin ruleFacts.
     * 
     * @param snapshotMetadata metadata del snapshot
     * @param status           estado a filtrar
     * @return iterator lazy ligero
     * @throws IOException si hay error
     */
    public Iterator<RecordValidation> getLightweightIterator(SnapshotMetadata snapshotMetadata, RecordStatus status)
            throws IOException {
        logger.debug("GET LIGHTWEIGHT ITERATOR: snapshot={}, network={} (LIGHT MODE)",
                snapshotMetadata.getSnapshotId(), snapshotMetadata.getNetwork().getAcronym());
        return ValidationRecordManager.iterateLightweight(basePath, snapshotMetadata, status, hadoopConf);
    }

    // ========================================
    // MÉTODOS PRIVADOS DE FILTRADO
    // ========================================

    private SnapshotMetadata getSnapshotMetadata(Long snapshotId) throws IOException {
        return snapshotStore.getSnapshotMetadata(snapshotId);
    }

    private ParsedFilters parseFilters(List<String> fq) {
        ParsedFilters filters = new ParsedFilters();

        if (fq == null || fq.isEmpty()) {
            return filters;
        }

        for (String filter : fq) {
            if (filter == null || filter.trim().isEmpty()) {
                continue;
            }

            String[] parts = filter.split(":|@@", 2);
            if (parts.length != 2) {
                logger.warn("Invalid filter format: {}", filter);
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
                    logger.warn("Unknown filter key: {}", key);
            }
        }

        return filters;
    }

    private void parseRuleIds(String value, Set<Integer> targetSet) {
        String[] ruleIds = value.split(",");
        for (String ruleIdStr : ruleIds) {
            try {
                targetSet.add(Integer.parseInt(ruleIdStr.trim()));
            } catch (NumberFormatException e) {
                logger.warn("Invalid rule ID: {}", ruleIdStr);
            }
        }
    }

    private boolean matchesFilters(RecordValidation record, ParsedFilters filters) {
        if (filters.isEmpty()) {
            return true;
        }

        if (filters.isValid != null && !record.getRecordIsValid().equals(filters.isValid)) {
            return false;
        }

        if (filters.isTransformed != null && !record.getIsTransformed().equals(filters.isTransformed)) {
            return false;
        }

        if (filters.invalidRuleIds == null && filters.validRuleIds == null) {
            return true;
        }

        if (record.getRuleFacts() == null || record.getRuleFacts().isEmpty()) {
            return false;
        }

        if (filters.invalidRuleIds != null && !hasRuleMatch(record, filters.invalidRuleIds, false)) {
            return false;
        }

        if (filters.validRuleIds != null && !hasRuleMatch(record, filters.validRuleIds, true)) {
            return false;
        }

        return true;
    }

    private boolean hasRuleMatch(RecordValidation record, Set<Integer> ruleIds, boolean expectedValidity) {
        return ruleIds.stream()
                .anyMatch(requiredRuleId -> record.getRuleFacts().stream()
                        .anyMatch(fact -> fact.getRuleId() != null
                                && fact.getRuleId().equals(requiredRuleId)
                                && fact.getIsValid() != null
                                && fact.getIsValid() == expectedValidity
                        ));
    }

    private void updateStats(SnapshotValidationStats stats, RecordValidation record) {
        stats.incrementTotalRecords();

        if (record.getRecordIsValid() != null && record.getRecordIsValid()) {
            stats.incrementValidRecords();
        }

        if (record.getIsTransformed() != null && record.getIsTransformed()) {
            stats.incrementTransformedRecords();
        }

        if (record.getRecordIsValid() != null) {
            stats.updateFacet("record_is_valid", record.getRecordIsValid().toString());
        }

        if (record.getIsTransformed() != null) {
            stats.updateFacet("record_is_transformed", record.getIsTransformed().toString());
        }

        if (record.getRuleFacts() != null) {
            for (RuleFact fact : record.getRuleFacts()) {
                Long ruleID = Long.valueOf(fact.getRuleId());
                String ruleIdStr = ruleID.toString();

                if (fact.getIsValid() != null && fact.getIsValid()) {
                    stats.incrementRuleValid(ruleID);
                    stats.updateFacet("valid_rules", ruleIdStr);
                } else {
                    stats.incrementRuleInvalid(ruleID);
                    stats.updateFacet("invalid_rules", ruleIdStr);
                }
            }
        }
    }
}
