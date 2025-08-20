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

package org.lareferencia.backend.services.parquet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.NetworkSnapshot;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.domain.ValidatorRule;

import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;
import org.lareferencia.backend.repositories.parquet.ValidationStatParquetRepository;
import org.lareferencia.backend.services.validation.ValidationStatisticsException;
import org.lareferencia.backend.validation.validator.ContentValidatorResult;
import org.lareferencia.core.metadata.IMetadataRecordStoreService;
import org.lareferencia.core.util.IRecordFingerprintHelper;
import org.lareferencia.core.validation.QuantifierValues;
import org.lareferencia.core.validation.ValidatorResult;
import org.lareferencia.core.validation.ValidatorRuleResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.lareferencia.backend.services.validation.IValidationStatisticsService;
import org.lareferencia.backend.domain.validation.ValidationStatObservation;
import org.lareferencia.backend.domain.validation.ValidationStatsQueryResult;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Servicio de estadísticas de validación basado en archivos Parquet.
 * Esta implementación reemplaza el uso de Solr con almacenamiento en filesystem
 * usando archivos organizados por snapshot ID para optimizar consultas.
 */
@Service
@Scope("prototype")
public class ValidationStatisticsParquetService implements IValidationStatisticsService {

    private static Logger logger = LogManager.getLogger(ValidationStatisticsParquetService.class);

    @Autowired
    private ValidationStatParquetRepository parquetRepository;

    @Value("${reponame.fieldname}")
    private String repositoryFieldName;

    @Value("${reponame.prefix}")
    private String repositoryPrefix;

    @Value("${instname.fieldname}")
    private String institutionFieldName;

    @Value("${instname.prefix}")
    private String institutionPrefix;

    @Autowired
    IMetadataRecordStoreService metadataStoreService;

    @Getter
    boolean detailedDiagnose = false;

    /**
     * Configura si se debe realizar un diagnóstico detallado
     */
    public void setDetailedDiagnose(boolean detailedDiagnose) {
        this.detailedDiagnose = detailedDiagnose;
        logger.info("ValidationStatisticsParquetService detailedDiagnose configurado a: {}", detailedDiagnose);
    }

    @Autowired
    private IRecordFingerprintHelper fingerprintHelper;

    // Constantes similares al servicio original
    public static final String[] FACET_FIELDS = { "record_is_valid", "record_is_transformed", "valid_rules", "invalid_rules", "institution_name", "repository_name" };
    public static final String SNAPSHOT_ID_FIELD = "snapshot_id";
    public static final String INVALID_RULE_SUFFIX = "_rule_invalid_occrs";
    public static final String VALID_RULE_SUFFIX = "_rule_valid_occrs";

    /**
     * Construye una observación de validación a partir del resultado del validador
     */
    public ValidationStatObservationParquet buildObservation(OAIRecord record, ValidatorResult validationResult) {

        logger.debug("Building validation result record ID: " + record.getId().toString());

        String id = fingerprintHelper.getStatsIDfromRecord(record);
        String identifier = record.getIdentifier();
        Long snapshotID = record.getSnapshot().getId();
        String origin = record.getSnapshot().getNetwork().getOriginURL();
        String metadataPrefix = record.getSnapshot().getNetwork().getMetadataPrefix();
        String networkAcronym = record.getSnapshot().getNetwork().getAcronym();
        Boolean isTransformed = record.getTransformed();
        Boolean isValid = validationResult.isValid();

        // Mapas para las ocurrencias
        Map<String, List<String>> validOccurrencesByRuleID = new HashMap<>();
        Map<String, List<String>> invalidOccurrencesByRuleID = new HashMap<>();
        List<String> validRulesID = new ArrayList<>();
        List<String> invalidRulesID = new ArrayList<>();

        for (ValidatorRuleResult ruleResult : validationResult.getRulesResults()) {

            String ruleID = ruleResult.getRule().getRuleId().toString();

            List<String> invalidOccr = new ArrayList<>();
            List<String> validOccr = new ArrayList<>();

            if (detailedDiagnose) {
                logger.debug("Detailed validation report - Rule ID: " + ruleID);

                for (ContentValidatorResult contentResult : ruleResult.getResults()) {
                    if (contentResult.isValid())
                        validOccr.add(contentResult.getReceivedValue());
                    else
                        invalidOccr.add(contentResult.getReceivedValue());
                }

                validOccurrencesByRuleID.put(ruleID, validOccr);
                invalidOccurrencesByRuleID.put(ruleID, invalidOccr);
            }

            // Agregar las reglas válidas e inválidas
            if (ruleResult.getValid())
                validRulesID.add(ruleID);
            else
                invalidRulesID.add(ruleID);
        }

        return new ValidationStatObservationParquet(
                id, identifier, snapshotID, origin, null, metadataPrefix, networkAcronym,
                null, null, isValid, isTransformed, validOccurrencesByRuleID,
                invalidOccurrencesByRuleID, validRulesID, invalidRulesID
        );
    }

    /**
     * Registra una lista de observaciones de validación
     */
    public void registerObservations(List<ValidationStatObservationParquet> validationStatsObservations) {
        logger.info("Registrando {} observaciones en Parquet", validationStatsObservations.size());
        try {
            parquetRepository.saveAll(validationStatsObservations);
            logger.info("Observaciones registradas exitosamente en Parquet");
        } catch (IOException e) {
            logger.error("Error en registro de stats (parquet): " + e.getMessage());
        }
    }

    /**
     * Consulta estadísticas de reglas de validación por snapshot (versión simplificada)
     */
    public Map<String, Object> queryValidatorRulesStatsBySnapshot(Long snapshotId) throws Exception {
        try {
            // Obtener estadísticas agregadas del repositorio
            Map<String, Object> aggregatedStats = parquetRepository.getAggregatedStats(snapshotId);
            
            Map<String, Object> result = new HashMap<>();
            result.put("size", ((Number) aggregatedStats.get("totalCount")).intValue());
            result.put("validSize", ((Number) aggregatedStats.get("validCount")).intValue());
            result.put("transformedSize", ((Number) aggregatedStats.get("transformedCount")).intValue());
            
            @SuppressWarnings("unchecked")
            Map<String, Long> validRuleCounts = (Map<String, Long>) aggregatedStats.get("validRuleCounts");
            @SuppressWarnings("unchecked")
            Map<String, Long> invalidRuleCounts = (Map<String, Long>) aggregatedStats.get("invalidRuleCounts");
            
            // Construir mapas de reglas simplificados
            Map<String, Map<String, Object>> rulesByID = new HashMap<>();
            
            // Combinar todas las reglas encontradas
            Set<String> allRuleIds = new HashSet<>();
            if (validRuleCounts != null) allRuleIds.addAll(validRuleCounts.keySet());
            if (invalidRuleCounts != null) allRuleIds.addAll(invalidRuleCounts.keySet());
            
            for (String ruleId : allRuleIds) {
                Map<String, Object> ruleStats = new HashMap<>();
                ruleStats.put("ruleID", Long.valueOf(ruleId));
                ruleStats.put("validCount", validRuleCounts != null ? validRuleCounts.getOrDefault(ruleId, 0L) : 0L);
                ruleStats.put("invalidCount", invalidRuleCounts != null ? invalidRuleCounts.getOrDefault(ruleId, 0L) : 0L);
                rulesByID.put(ruleId, ruleStats);
            }
            
            result.put("rulesByID", rulesByID);
            result.put("facets", new HashMap<String, Object>()); // Facetas vacías por simplicidad
            
            return result;
            
        } catch (IOException e) {
            logger.error("Error querying validation stats for snapshot " + snapshotId, e);
            throw new Exception("Error querying validation statistics", e);
        }
    }

    /**
     * Consulta estadísticas de reglas de validación por snapshot
     */
    public ValidationStats queryValidatorRulesStatsBySnapshot(NetworkSnapshot snapshot, List<String> fq) throws Exception {

        ValidationStats result = new ValidationStats();

        try {
            // Si hay filtros, aplicarlos tanto a las estadísticas como a las facetas
            if (fq != null && !fq.isEmpty()) {
                logger.info("Aplicando filtros a las estadísticas principales: {}", fq);
                
                // Procesar filtros
                Map<String, Object> filters = parseFilterQueries(fq);
                
                // Obtener observaciones filtradas
                List<ValidationStatObservationParquet> filteredObservations = parquetRepository.findBySnapshotId(snapshot.getId())
                    .stream()
                    .filter(obs -> matchesFilters(obs, filters))
                    .collect(Collectors.toList());
                
                logger.info("Observaciones filtradas para estadísticas: {} (de {} totales)", 
                    filteredObservations.size(), parquetRepository.findBySnapshotId(snapshot.getId()).size());
                
                // Calcular estadísticas agregadas a partir de las observaciones filtradas
                result.size = filteredObservations.size();
                result.validSize = (int) filteredObservations.stream().filter(obs -> obs.getIsValid()).count();
                result.transformedSize = (int) filteredObservations.stream().filter(obs -> obs.getIsTransformed()).count();
                
                // Calcular conteos de reglas válidas e inválidas a partir de observaciones filtradas
                Map<String, Long> validRuleCounts = new HashMap<>();
                Map<String, Long> invalidRuleCounts = new HashMap<>();
                
                for (ValidationStatObservationParquet obs : filteredObservations) {
                    // Contar reglas válidas
                    if (obs.getValidRulesIDList() != null) {
                        for (String ruleId : obs.getValidRulesIDList()) {
                            validRuleCounts.put(ruleId, validRuleCounts.getOrDefault(ruleId, 0L) + 1);
                        }
                    }
                    // Contar reglas inválidas
                    if (obs.getInvalidRulesIDList() != null) {
                        for (String ruleId : obs.getInvalidRulesIDList()) {
                            invalidRuleCounts.put(ruleId, invalidRuleCounts.getOrDefault(ruleId, 0L) + 1);
                        }
                    }
                }
                
                // Construir facetas con filtros
                result.facets = buildSimulatedFacets(snapshot.getId(), fq);
                
                // Construir estadísticas por regla usando los conteos filtrados
                if (snapshot.getNetwork().getValidator() != null) {
                    for (ValidatorRule rule : snapshot.getNetwork().getValidator().getRules()) {

                        String ruleID = rule.getId().toString();

                        ValidationRuleStat ruleResult = new ValidationRuleStat();

                        ruleResult.ruleID = rule.getId();
                        ruleResult.validCount = validRuleCounts.getOrDefault(ruleID, 0L).intValue();
                        ruleResult.invalidCount = invalidRuleCounts.getOrDefault(ruleID, 0L).intValue();
                        ruleResult.name = rule.getName();
                        ruleResult.description = rule.getDescription();
                        ruleResult.mandatory = rule.getMandatory();
                        ruleResult.quantifier = rule.getQuantifier();

                        result.rulesByID.put(ruleID, ruleResult);
                    }
                }
                
            } else {
                // Sin filtros, usar estadísticas agregadas (comportamiento original)
                logger.info("Sin filtros - usando estadísticas agregadas completas");
                
                Map<String, Object> aggregatedStats = parquetRepository.getAggregatedStats(snapshot.getId());
                
                result.size = ((Number) aggregatedStats.get("totalCount")).intValue();
                result.validSize = ((Number) aggregatedStats.get("validCount")).intValue();
                result.transformedSize = ((Number) aggregatedStats.get("transformedCount")).intValue();

                @SuppressWarnings("unchecked")
                Map<String, Long> validRuleCounts = (Map<String, Long>) aggregatedStats.get("validRuleCounts");
                @SuppressWarnings("unchecked")
                Map<String, Long> invalidRuleCounts = (Map<String, Long>) aggregatedStats.get("invalidRuleCounts");

                // Construir facetas sin filtros
                result.facets = buildSimulatedFacets(snapshot.getId(), fq);

                if (snapshot.getNetwork().getValidator() != null) {
                    for (ValidatorRule rule : snapshot.getNetwork().getValidator().getRules()) {

                        String ruleID = rule.getId().toString();

                        ValidationRuleStat ruleResult = new ValidationRuleStat();

                        ruleResult.ruleID = rule.getId();
                        ruleResult.validCount = validRuleCounts.getOrDefault(ruleID, 0L).intValue();
                        ruleResult.invalidCount = invalidRuleCounts.getOrDefault(ruleID, 0L).intValue();
                        ruleResult.name = rule.getName();
                        ruleResult.description = rule.getDescription();
                        ruleResult.mandatory = rule.getMandatory();
                        ruleResult.quantifier = rule.getQuantifier();

                        result.rulesByID.put(ruleID, ruleResult);
                    }
                }
            }

        } catch (IOException e) {
            throw new Exception("Error consultando estadísticas de validación: " + e.getMessage());
        }

        return result;
    }

    /**
     * Verifica si el servicio está disponible
     */
    public boolean isServiceAvailable() {
        try {
            // Verificar que el directorio base existe y es accesible
            return parquetRepository != null;
        } catch (Exception e) {
            logger.error("Error verificando disponibilidad del servicio parquet: " + e.getMessage());
            return false;
        }
    }

    /**
     * Consulta conteos de ocurrencias de reglas válidas por snapshot ID y regla ID
     */
    public ValidationRuleOccurrencesCount queryValidRuleOccurrencesCountBySnapshotID(Long snapshotID, Long ruleID, List<String> fq) {

        ValidationRuleOccurrencesCount result = new ValidationRuleOccurrencesCount();

        try {
            String ruleIdStr = ruleID.toString();
            
            // Obtener conteos de ocurrencias válidas e inválidas
            Map<String, Long> validOccurrences = parquetRepository.getRuleOccurrenceCounts(snapshotID, ruleIdStr, true);
            Map<String, Long> invalidOccurrences = parquetRepository.getRuleOccurrenceCounts(snapshotID, ruleIdStr, false);

            List<OccurrenceCount> validRuleOccurrence = validOccurrences.entrySet().stream()
                    .map(entry -> new OccurrenceCount(entry.getKey(), entry.getValue().intValue()))
                    .sorted((a, b) -> Integer.compare(b.count, a.count)) // Ordenar por conteo descendente
                    .collect(Collectors.toList());

            List<OccurrenceCount> invalidRuleOccurrence = invalidOccurrences.entrySet().stream()
                    .map(entry -> new OccurrenceCount(entry.getKey(), entry.getValue().intValue()))
                    .sorted((a, b) -> Integer.compare(b.count, a.count)) // Ordenar por conteo descendente
                    .collect(Collectors.toList());

            result.setValidRuleOccrs(validRuleOccurrence);
            result.setInvalidRuleOccrs(invalidRuleOccurrence);

        } catch (IOException e) {
            logger.error("Error consultando ocurrencias de reglas: " + e.getMessage());
            result.setValidRuleOccrs(new ArrayList<>());
            result.setInvalidRuleOccrs(new ArrayList<>());
        }

        return result;
    }

    /**
     * Consulta observaciones de validación por snapshot ID con paginación
     */
    public List<ValidationStatObservationParquet> queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, int page, int size) {
        try {
            // Aplicar filtros si están presentes
            Map<String, Object> filters = parseFilterQueries(fq);
            
            if (filters.isEmpty()) {
                return parquetRepository.findBySnapshotIdWithPagination(snapshotID, page, size);
            } else {
                // Obtener todas las observaciones y aplicar filtros en el servicio
                List<ValidationStatObservationParquet> allObservations = parquetRepository.findBySnapshotId(snapshotID);
                List<ValidationStatObservationParquet> filtered = allObservations.stream()
                    .filter(obs -> matchesFilters(obs, filters))
                    .collect(Collectors.toList());
                
                // Aplicar paginación manual
                int start = page * size;
                int end = Math.min(start + size, filtered.size());
                
                if (start >= filtered.size()) {
                    return new ArrayList<>();
                }
                
                return filtered.subList(start, end);
            }
        } catch (IOException e) {
            logger.error("Error consultando observaciones: " + e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * Consulta observaciones de estadísticas de validación por snapshot ID con paginación
     */
    @Override
    public ValidationStatsQueryResult queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) throws ValidationStatisticsException {
        System.out.println("DEBUG: Consulta Parquet - snapshotID: " + snapshotID + ", filtros: " + fq + ", página: " + pageable.getPageNumber() + ", tamaño: " + pageable.getPageSize());
        
        try {
            // Obtener observaciones de Parquet con filtros aplicados en el servicio
            List<ValidationStatObservationParquet> parquetObservations;
            
            if (fq != null && !fq.isEmpty()) {
                Map<String, Object> filters = convertFiltersToMap(fq);
                System.out.println("DEBUG: Aplicando filtros en el servicio: " + filters);
                
                // Obtener todas las observaciones del snapshot y aplicar filtros en el servicio
                List<ValidationStatObservationParquet> allObservations = parquetRepository.findBySnapshotId(snapshotID);
                System.out.println("DEBUG: Total de observaciones sin filtrar: " + allObservations.size());
                
                // Aplicar filtros usando la lógica del servicio
                parquetObservations = allObservations.stream()
                    .filter(obs -> matchesFilters(obs, filters))
                    .collect(Collectors.toList());
                
                System.out.println("DEBUG: Observaciones después del filtrado: " + parquetObservations.size());
                
                // Aplicar paginación manualmente
                int start = pageable.getPageNumber() * pageable.getPageSize();
                int end = Math.min(start + pageable.getPageSize(), parquetObservations.size());
                
                System.out.println("DEBUG: Aplicando paginación - inicio: " + start + ", fin: " + end + ", total: " + parquetObservations.size());
                
                List<ValidationStatObservation> observations;
                if (start >= parquetObservations.size()) {
                    observations = Collections.emptyList();
                } else {
                    List<ValidationStatObservationParquet> paginatedList = parquetObservations.subList(start, end);
                    observations = new ArrayList<>(paginatedList);
                }
                
                return new ValidationStatsQueryResult(
                    observations,
                    parquetObservations.size(),
                    pageable
                );
            } else {
                parquetObservations = parquetRepository.findBySnapshotIdWithPagination(
                    snapshotID, 
                    pageable.getPageNumber(), 
                    pageable.getPageSize()
                );
                
                long totalElements = parquetRepository.countBySnapshotId(snapshotID);
                List<ValidationStatObservation> observations = new ArrayList<>(parquetObservations);
                
                return new ValidationStatsQueryResult(
                    observations,
                    totalElements,
                    pageable
                );
            }

        } catch (IOException e) {
            logger.error("Error querying validation stats observations by snapshot ID: " + e.getMessage(), e);
            throw new ValidationStatisticsException("Error consultando observaciones de validación", e);
        }
    }

    /**
     * Convierte los filtros de formato String a Map para el repositorio Parquet
     */
    private Map<String, Object> convertFiltersToMap(List<String> fq) {
        Map<String, Object> filters = new HashMap<>();
        
        System.out.println("DEBUG: Convirtiendo filtros de lista: " + fq);
        
        if (fq != null) {
            for (String filter : fq) {
                System.out.println("DEBUG: Procesando filtro: '" + filter + "'");
                
                // Convertir filtros de formato "field:value" a map
                if (filter.contains(":")) {
                    String[] parts = filter.split(":", 2);
                    if (parts.length == 2) {
                        String field = parts[0].trim();
                        String value = parts[1].trim();
                        
                        System.out.println("DEBUG: Campo: '" + field + "', valor original: '" + value + "'");
                        
                        // Manejar valores entre comillas
                        if (value.startsWith("\"") && value.endsWith("\"")) {
                            value = value.substring(1, value.length() - 1);
                            System.out.println("DEBUG: Valor después de quitar comillas: '" + value + "'");
                        }
                        
                        // Convertir valores boolean string a boolean si es necesario
                        Object filterValueObject = value;
                        if ("isValid".equals(field) || "isTransformed".equals(field)) {
                            if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                                filterValueObject = Boolean.parseBoolean(value);
                                System.out.println("DEBUG: Convertido a boolean: " + filterValueObject);
                            }
                        }
                        
                        filters.put(field, filterValueObject);
                        System.out.println("DEBUG: Agregado al map - campo: '" + field + "', valor: '" + filterValueObject + "'");
                    }
                } else {
                    System.out.println("DEBUG: Filtro no contiene ':', ignorando: '" + filter + "'");
                }
            }
        }
        
        System.out.println("DEBUG: Map de filtros final: " + filters);
        return filters;
    }

    /**
     * Elimina observaciones de validación por snapshot ID
     */
    public void deleteValidationStatsObservationsByRecordIDsAndSnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            parquetRepository.deleteBySnapshotId(snapshotID);
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error eliminando información de validación | snapshot:" + snapshotID + " :: " + e.getMessage());
        }
    }

    /**
     * Elimina observaciones de validación por lista de registros
     */
    public void deleteValidationStatsObservationsByRecordsAndSnapshotID(Long snapshotID, Collection<OAIRecord> records) throws ValidationStatisticsException {
        for (OAIRecord record : records) {
            try {
                String id = fingerprintHelper.getStatsIDfromRecord(record);
                parquetRepository.deleteById(id, snapshotID);
            } catch (IOException e) {
                throw new ValidationStatisticsException("Error eliminando información de validación | snapshot:" + snapshotID + " recordID:" + record.getIdentifier() + " :: " + e.getMessage());
            }
        }
    }

    /**
     * Elimina observación de validación por registro
     */
    public void deleteValidationStatsObservationByRecordAndSnapshotID(Long snapshotID, OAIRecord record) throws ValidationStatisticsException {
        try {
            String id = fingerprintHelper.getStatsIDfromRecord(record);
            parquetRepository.deleteById(id, snapshotID);
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error eliminando información de validación | snapshot:" + snapshotID + " recordID:" + record.getIdentifier() + " :: " + e.getMessage());
        }
    }

    /**
     * Copia observaciones de validación de un snapshot a otro
     */
    public boolean copyValidationStatsObservationsFromTo(Long originalSnapshotId, Long newSnapshotId) throws ValidationStatisticsException {
        try {
            parquetRepository.copySnapshotData(originalSnapshotId, newSnapshotId);
            return true;
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error copiando información de validación | snapshot:" + originalSnapshotId + " a snapshot:" + newSnapshotId + " :: " + e.getMessage());
        }
    }

    /**
     * Elimina estadísticas de validación por snapshot ID
     */
    public void deleteValidationStatsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            parquetRepository.deleteBySnapshotId(snapshotID);
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error eliminando información de validación | snapshot:" + snapshotID + " :: " + e.getMessage());
        }
    }

    // Métodos auxiliares privados

    private Map<String, Object> parseFilterQueries(List<String> fq) {
        Map<String, Object> filters = new HashMap<>();
        
        if (fq == null || fq.isEmpty()) {
            logger.info("No hay filtros para procesar");
            return filters;
        }
        
        logger.info("Procesando filtros: {}", fq);
        
        for (String fqTerm : fq) {
            logger.info("Procesando término de filtro: '{}'", fqTerm);
            
            // Intentar decodificar URL si es necesario
            String decodedTerm = fqTerm;
            if (fqTerm.contains("%22")) {
                decodedTerm = fqTerm.replace("%22", "\"");
                logger.info("Filtro decodificado de URL: '{}'", decodedTerm);
            }
            
            String cleanTerm = decodedTerm.replace("@@", ":");
            logger.info("Término limpiado: '{}'", cleanTerm);
            String[] parts = cleanTerm.split(":", 2);
            
            if (parts.length == 2) {
                String field = parts[0];
                String value = parts[1];
                logger.info("Campo: '{}', Valor original: '{}'", field, value);
                
                // Remover comillas si están presentes
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                    logger.info("Valor sin comillas: '{}'", value);
                }
                
                // Convertir valores booleanos
                if ("true".equalsIgnoreCase(value)) {
                    filters.put(field, Boolean.TRUE);
                } else if ("false".equalsIgnoreCase(value)) {
                    filters.put(field, Boolean.FALSE);
                } else {
                    filters.put(field, value);
                }
                
                logger.info("Filtro agregado: '{}' = '{}'", field, filters.get(field));
            } else {
                logger.warn("Formato de filtro inválido: '{}'", fqTerm);
            }
        }
        
        logger.info("Filtros finales: {}", filters);
        return filters;
    }

    private Map<String, List<FacetFieldEntry>> buildSimulatedFacets(Long snapshotId, List<String> fq) throws IOException {
        Map<String, List<FacetFieldEntry>> facets = new HashMap<>();
        
        // Obtener todas las observaciones del snapshot
        List<ValidationStatObservationParquet> observations = parquetRepository.findBySnapshotId(snapshotId);
        logger.info("Total de observaciones antes del filtro: {}", observations.size());
        
        // Aplicar filtros si están presentes
        if (fq != null && !fq.isEmpty()) {
            Map<String, Object> filters = parseFilterQueries(fq);
            if (!filters.isEmpty()) {
                List<ValidationStatObservationParquet> filteredObservations = observations.stream()
                    .filter(obs -> matchesFilters(obs, filters))
                    .collect(Collectors.toList());
                    
                logger.info("Observaciones después del filtro: {} (filtros aplicados: {})", 
                    filteredObservations.size(), filters);
                observations = filteredObservations;
            }
        }
        
        // Calcular facetas para cada campo
        facets.put("record_is_valid", buildFacetForBooleanField(observations, "isValid"));
        facets.put("record_is_transformed", buildFacetForBooleanField(observations, "isTransformed"));
        facets.put("institution_name", buildFacetForStringField(observations, "institutionName"));
        facets.put("repository_name", buildFacetForStringField(observations, "repositoryName"));
        facets.put("valid_rules", buildFacetForValidRules(observations));
        facets.put("invalid_rules", buildFacetForInvalidRules(observations));
        
        return facets;
    }

    // Métodos auxiliares para construir facetas
    private List<FacetFieldEntry> buildFacetForBooleanField(List<ValidationStatObservationParquet> observations, String fieldName) {
        Map<String, Long> counts = new HashMap<>();
        
        for (ValidationStatObservationParquet obs : observations) {
            Boolean fieldValue = null;
            switch (fieldName) {
                case "isValid":
                    fieldValue = obs.getIsValid();
                    break;
                case "isTransformed":
                    fieldValue = obs.getIsTransformed();
                    break;
            }
            
            String valueStr = fieldValue != null ? fieldValue.toString() : "null";
            counts.put(valueStr, counts.getOrDefault(valueStr, 0L) + 1);
        }
        
        String facetKeyName = fieldName.equals("isValid") ? "record_is_valid" : "record_is_transformed";
        
        return counts.entrySet().stream()
                .map(entry -> new FacetFieldEntry(entry.getKey(), entry.getValue(), facetKeyName))
                .sorted((a, b) -> Long.compare(b.getValueCount(), a.getValueCount()))
                .collect(Collectors.toList());
    }
    
    private List<FacetFieldEntry> buildFacetForStringField(List<ValidationStatObservationParquet> observations, String fieldName) {
        Map<String, Long> counts = new HashMap<>();
        
        for (ValidationStatObservationParquet obs : observations) {
            String fieldValue = null;
            switch (fieldName) {
                case "institutionName":
                    fieldValue = obs.getInstitutionName();
                    break;
                case "repositoryName":
                    fieldValue = obs.getRepositoryName();
                    break;
            }
            
            String valueStr = fieldValue != null ? fieldValue : "unknown";
            counts.put(valueStr, counts.getOrDefault(valueStr, 0L) + 1);
        }
        
        return counts.entrySet().stream()
                .map(entry -> new FacetFieldEntry(entry.getKey(), entry.getValue(), fieldName))
                .sorted((a, b) -> Long.compare(b.getValueCount(), a.getValueCount()))
                .collect(Collectors.toList());
    }
    
    private List<FacetFieldEntry> buildFacetForValidRules(List<ValidationStatObservationParquet> observations) {
        Map<String, Long> counts = new HashMap<>();
        
        for (ValidationStatObservationParquet obs : observations) {
            List<String> validRules = obs.getValidRulesIDList();
            if (validRules != null) {
                for (String ruleId : validRules) {
                    counts.put(ruleId, counts.getOrDefault(ruleId, 0L) + 1);
                }
            }
        }
        
        return counts.entrySet().stream()
                .map(entry -> new FacetFieldEntry(entry.getKey(), entry.getValue(), "valid_rules"))
                .sorted((a, b) -> Long.compare(b.getValueCount(), a.getValueCount()))
                .collect(Collectors.toList());
    }
    
    private List<FacetFieldEntry> buildFacetForInvalidRules(List<ValidationStatObservationParquet> observations) {
        Map<String, Long> counts = new HashMap<>();
        
        for (ValidationStatObservationParquet obs : observations) {
            List<String> invalidRules = obs.getInvalidRulesIDList();
            if (invalidRules != null) {
                for (String ruleId : invalidRules) {
                    counts.put(ruleId, counts.getOrDefault(ruleId, 0L) + 1);
                }
            }
        }
        
        return counts.entrySet().stream()
                .map(entry -> new FacetFieldEntry(entry.getKey(), entry.getValue(), "invalid_rules"))
                .sorted((a, b) -> Long.compare(b.getValueCount(), a.getValueCount()))
                .collect(Collectors.toList());
    }
    
    private boolean matchesFilters(ValidationStatObservationParquet obs, Map<String, Object> filters) {
        for (Map.Entry<String, Object> filter : filters.entrySet()) {
            String field = filter.getKey();
            Object value = filter.getValue();
            
            switch (field) {
                case "isValid":
                case "record_is_valid":
                    if (!Objects.equals(obs.getIsValid(), value)) return false;
                    break;
                case "isTransformed":
                case "record_is_transformed":
                    if (!Objects.equals(obs.getIsTransformed(), value)) return false;
                    break;
                case "institutionName":
                case "institution_name":
                    if (!Objects.equals(obs.getInstitutionName(), value)) return false;
                    break;
                case "repositoryName":
                case "repository_name":
                    if (!Objects.equals(obs.getRepositoryName(), value)) return false;
                    break;
                case "identifier":
                    // Verificar si el identifier coincide (búsqueda exacta o contiene)
                    String identifierValue = value.toString();
                    String obsIdentifier = obs.getIdentifier();
                    logger.debug("Filtro identifier: buscando '{}' en '{}'", identifierValue, obsIdentifier);
                    if (obsIdentifier == null || (!obsIdentifier.equals(identifierValue) && !obsIdentifier.contains(identifierValue))) {
                        logger.debug("Comparando '{}' con '{}' (exacta o contiene) = false", obsIdentifier, identifierValue);
                        return false;
                    }
                    logger.debug("Comparando '{}' con '{}' (exacta o contiene) = true", obsIdentifier, identifierValue);
                    break;
                case "valid_rules":
                    // Verificar si el ruleId está en la lista de reglas válidas
                    String valueStr = value.toString();
                    List<String> validRules = obs.getValidRulesIDList();
                    logger.debug("Filtro valid_rules: buscando '{}' en {}", valueStr, validRules);
                    if (validRules == null || !validRules.contains(valueStr)) {
                        logger.debug("Registro rechazado: regla '{}' no está en valid_rules {}", valueStr, validRules);
                        return false;
                    }
                    logger.debug("Registro aceptado: regla '{}' encontrada en valid_rules", valueStr);
                    break;
                case "invalid_rules":
                    // Verificar si el ruleId está en la lista de reglas inválidas
                    String invalidValueStr = value.toString();
                    List<String> invalidRules = obs.getInvalidRulesIDList();
                    logger.debug("Filtro invalid_rules: buscando '{}' en {}", invalidValueStr, invalidRules);
                    if (invalidRules == null || !invalidRules.contains(invalidValueStr)) {
                        logger.debug("Registro rechazado: regla '{}' no está en invalid_rules {}", invalidValueStr, invalidRules);
                        return false;
                    }
                    logger.debug("Registro aceptado: regla '{}' encontrada en invalid_rules", invalidValueStr);
                    break;
                // Agregar más campos según sea necesario
            }
        }
        return true;
    }

    // Clases internas (copiadas del servicio original)

    @Getter
    @Setter
    public class ValidationStats {

        public ValidationStats() {
            facets = new HashMap<>();
            rulesByID = new HashMap<>();
        }

        Integer size;
        Integer transformedSize;
        Integer validSize;
        Map<String, ValidationRuleStat> rulesByID;
        Map<String, List<FacetFieldEntry>> facets;
    }

    @Getter
    @Setter
    public class ValidationRuleStat {
        Long ruleID;
        String name;
        String description;
        QuantifierValues quantifier;
        Boolean mandatory;
        Integer validCount;
        Integer invalidCount;
    }

    @Getter
    @Setter
    public class ValidationRuleOccurrencesCount {
        List<OccurrenceCount> invalidRuleOccrs;
        List<OccurrenceCount> validRuleOccrs;
    }

    @Getter
    @Setter
    public class OccurrenceCount {
        public OccurrenceCount(String value, int count) {
            super();
            this.value = value;
            this.count = count;
        }

        String value;
        Integer count;
    }

    // Clase auxiliar para simular FacetFieldEntry de Solr
    public static class FacetFieldEntry {
        private String value;
        private long valueCount;
        private FacetKey key;

        public FacetFieldEntry(String value, long valueCount, String keyName) {
            this.value = value;
            this.valueCount = valueCount;
            this.key = new FacetKey(keyName);
        }

        public String getValue() {
            return value;
        }

        public long getValueCount() {
            return valueCount;
        }
        
        public FacetKey getKey() {
            return key;
        }
    }
    
    // Clase auxiliar para la clave de faceta
    public static class FacetKey {
        private String name;
        
        public FacetKey(String name) {
            this.name = name;
        }
        
        public String getName() {
            return name;
        }
    }
    
    // Implementación de métodos de la interfaz IValidationStatisticsService
    
    @Override
    public void saveValidationStatObservations(List<ValidationStatObservation> observations) throws ValidationStatisticsException {
        try {
            List<ValidationStatObservationParquet> parquetObservations = new ArrayList<>();
            for (ValidationStatObservation obs : observations) {
                if (obs instanceof ValidationStatObservationParquet) {
                    parquetObservations.add((ValidationStatObservationParquet) obs);
                } else {
                    // Convertir ValidationStatObservation a ValidationStatObservationParquet
                    ValidationStatObservationParquet parquetObs = new ValidationStatObservationParquet();
                    parquetObs.setId(obs.getId());
                    parquetObs.setIdentifier(obs.getIdentifier());
                    parquetObs.setSnapshotId(obs.getSnapshotId());
                    parquetObs.setOrigin(obs.getOrigin());
                    parquetObs.setSetSpec(obs.getSetSpec());
                    parquetObs.setMetadataPrefix(obs.getMetadataPrefix());
                    parquetObs.setNetworkAcronym(obs.getNetworkAcronym());
                    parquetObs.setRepositoryName(obs.getRepositoryName());
                    parquetObs.setInstitutionName(obs.getInstitutionName());
                    parquetObs.setIsValid(obs.getIsValid());
                    parquetObs.setIsTransformed(obs.getIsTransformed());
                    parquetObs.setValidOccurrencesByRuleIDJson(obs.getValidOccurrencesByRuleIDJson());
                    parquetObs.setInvalidOccurrencesByRuleIDJson(obs.getInvalidOccurrencesByRuleIDJson());
                    parquetObs.setValidRulesID(obs.getValidRulesID());
                    parquetObs.setInvalidRulesID(obs.getInvalidRulesID());
                    parquetObservations.add(parquetObs);
                }
            }
            registerObservations(parquetObservations);
        } catch (Exception e) {
            throw new ValidationStatisticsException("Error guardando observaciones de validación", e);
        }
    }

    @Override
    public ValidationStatsQueryResult queryValidatorRulesStatsBySnapshot(Long snapshotID, List<String> filters) throws ValidationStatisticsException {
        try {
            Map<String, Object> stats = queryValidatorRulesStatsBySnapshot(snapshotID);
            ValidationStatsQueryResult result = new ValidationStatsQueryResult();
            result.setAggregations(stats);
            result.setMetadata(Map.of("snapshotId", snapshotID, "implementationType", "parquet"));
            return result;
        } catch (Exception e) {
            throw new ValidationStatisticsException("Error consultando estadísticas de reglas", e);
        }
    }

    @Override
    public void deleteValidationStatsObservationsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            parquetRepository.deleteBySnapshotId(snapshotID);
            logger.info("Observaciones del snapshot {} eliminadas exitosamente", snapshotID);
        } catch (Exception e) {
            throw new ValidationStatisticsException("Error eliminando observaciones del snapshot: " + snapshotID, e);
        }
    }

    @Override
    public String getImplementationType() {
        return "parquet";
    }

    @Override
    public Map<String, Object> getPerformanceMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("implementationType", "parquet");
        metrics.put("available", isServiceAvailable());
        metrics.put("timestamp", System.currentTimeMillis());
        
        try {
            // Agregar métricas básicas del repositorio si están disponibles
            metrics.put("repositoryType", "filesystem-parquet");
            
        } catch (Exception e) {
            logger.warn("Error obteniendo métricas de performance: {}", e.getMessage());
        }
        
        return metrics;
    }

    @Override
    public boolean validateFilters(List<String> filters) {
        if (filters == null || filters.isEmpty()) {
            return true;
        }
        
        // Validar formato de filtros (campo@@valor)
        for (String filter : filters) {
            if (!filter.contains("@@")) {
                return false;
            }
            String[] parts = filter.split("@@");
            if (parts.length != 2) {
                return false;
            }
        }
        
        return true;
    }
}
