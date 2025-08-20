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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Repositorio simplificado para datos de validación en Parquet.
 * La lógica de filtrado se maneja completamente en el servicio.
 */
@Repository
public class ValidationStatParquetRepository {

    private static final Logger logger = LogManager.getLogger(ValidationStatParquetRepository.class);

    @Value("${validation.stats.parquet.path:/tmp/validation-stats-parquet}")
    private String parquetBasePath;

    // Cache simple en memoria para demostración
    private Map<Long, List<ValidationStatObservationParquet>> dataCache = new HashMap<>();
    
    // Constructor sin datos de prueba
    public ValidationStatParquetRepository() {
        // Sin inicialización de datos de prueba
        logger.info("ValidationStatParquetRepository inicializado sin datos de prueba");
    }
    
    /**
     * Busca todas las observaciones por snapshot ID
     */
    public List<ValidationStatObservationParquet> findBySnapshotId(Long snapshotId) throws IOException {
        logger.debug("Buscando observaciones para snapshot: {}", snapshotId);
        
        // Retornar datos de cache o lista vacía
        return dataCache.getOrDefault(snapshotId, Collections.emptyList());
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
     * Guarda todas las observaciones
     */
    public void saveAll(List<ValidationStatObservationParquet> observations) throws IOException {
        if (observations == null || observations.isEmpty()) {
            return;
        }
        
        // Agrupar por snapshot ID
        Map<Long, List<ValidationStatObservationParquet>> groupedBySnapshot = observations.stream()
            .collect(Collectors.groupingBy(ValidationStatObservationParquet::getSnapshotID));
        
        // Agregar al cache
        for (Map.Entry<Long, List<ValidationStatObservationParquet>> entry : groupedBySnapshot.entrySet()) {
            Long snapshotId = entry.getKey();
            List<ValidationStatObservationParquet> snapshotObservations = entry.getValue();
            
            dataCache.computeIfAbsent(snapshotId, k -> new ArrayList<>()).addAll(snapshotObservations);
        }
        
        logger.info("Guardadas {} observaciones en repositorio", observations.size());
    }

    /**
     * Elimina observaciones por snapshot ID
     */
    public void deleteBySnapshotId(Long snapshotId) throws IOException {
        dataCache.remove(snapshotId);
        logger.info("Eliminadas observaciones del snapshot: {}", snapshotId);
    }

    /**
     * Elimina observación específica por ID
     */
    public void deleteById(String id, Long snapshotId) throws IOException {
        List<ValidationStatObservationParquet> observations = dataCache.get(snapshotId);
        if (observations != null) {
            observations.removeIf(obs -> id.equals(obs.getId()));
        }
    }

    /**
     * Copia datos de un snapshot a otro
     */
    public void copySnapshotData(Long originalSnapshotId, Long newSnapshotId) throws IOException {
        List<ValidationStatObservationParquet> originalData = dataCache.get(originalSnapshotId);
        if (originalData != null) {
            List<ValidationStatObservationParquet> copiedData = new ArrayList<>();
            for (ValidationStatObservationParquet obs : originalData) {
                ValidationStatObservationParquet copy = new ValidationStatObservationParquet();
                copyObservation(obs, copy);
                copy.setSnapshotID(newSnapshotId);
                copiedData.add(copy);
            }
            dataCache.put(newSnapshotId, copiedData);
        }
    }

    /**
     * Obtiene estadísticas agregadas por snapshot ID
     */
    public Map<String, Object> getAggregatedStats(Long snapshotId) throws IOException {
        List<ValidationStatObservationParquet> observations = findBySnapshotId(snapshotId);
        
        Map<String, Object> stats = new HashMap<>();
        
        long totalCount = observations.size();
        long validCount = observations.stream().mapToLong(obs -> obs.getIsValid() ? 1L : 0L).sum();
        long transformedCount = observations.stream().mapToLong(obs -> obs.getIsTransformed() ? 1L : 0L).sum();
        
        // Contar reglas válidas e inválidas
        Map<String, Long> validRuleCounts = new HashMap<>();
        Map<String, Long> invalidRuleCounts = new HashMap<>();
        
        for (ValidationStatObservationParquet obs : observations) {
            if (obs.getValidRulesIDList() != null) {
                for (String ruleId : obs.getValidRulesIDList()) {
                    validRuleCounts.put(ruleId, validRuleCounts.getOrDefault(ruleId, 0L) + 1);
                }
            }
            if (obs.getInvalidRulesIDList() != null) {
                for (String ruleId : obs.getInvalidRulesIDList()) {
                    invalidRuleCounts.put(ruleId, invalidRuleCounts.getOrDefault(ruleId, 0L) + 1);
                }
            }
        }
        
        stats.put("totalCount", totalCount);
        stats.put("validCount", validCount);
        stats.put("transformedCount", transformedCount);
        stats.put("validRuleCounts", validRuleCounts);
        stats.put("invalidRuleCounts", invalidRuleCounts);
        
        return stats;
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
}
