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
import org.lareferencia.backend.repositories.parquet.ValidationStatParquetQueryEngine;
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

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.Setter;

/**
 * Validation statistics service based on Parquet files.
 * This implementation replaces Solr usage with filesystem storage
 * using files organized by snapshot ID to optimize queries.
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
     * Configures whether detailed diagnosis should be performed
     */
    public void setDetailedDiagnose(boolean detailedDiagnose) {
        this.detailedDiagnose = detailedDiagnose;
        logger.info("ValidationStatisticsParquetService detailedDiagnose set to: {}", detailedDiagnose);
    }

    @Autowired
    private IRecordFingerprintHelper fingerprintHelper;

    // Constants similar to the original service
    public static final String[] FACET_FIELDS = { "record_is_valid", "record_is_transformed", "valid_rules", "invalid_rules", "institution_name", "repository_name" };
    public static final String SNAPSHOT_ID_FIELD = "snapshot_id";
    public static final String INVALID_RULE_SUFFIX = "_rule_invalid_occrs";
    public static final String VALID_RULE_SUFFIX = "_rule_valid_occrs";

    /**
     * INITIALIZATION: Initialize a new validation for a snapshot
     * This method should be called when starting a new validation process
     * to ensure clean state and remove any previous validation data
     */
    public void initializeValidationForSnapshot(Long snapshotId) {
        try {
            logger.info("INIT VALIDATION: Starting new validation for snapshot {}", snapshotId);
            
            // Clean the snapshot directory and reset all state
            parquetRepository.cleanSnapshot(snapshotId);
            
            logger.info("INIT VALIDATION: Successfully initialized validation for snapshot {}", snapshotId);
            
        } catch (Exception e) {
            logger.error("INIT VALIDATION: Error initializing validation for snapshot {}", snapshotId, e);
            throw new RuntimeException("Failed to initialize validation for snapshot: " + snapshotId, e);
        }
    }

    /**
     * Builds a validation observation from validator result
     */
    public ValidationStatObservationParquet buildObservation(OAIRecord record, ValidatorResult validationResult) {

        logger.debug("Building validation result record ID: {}", record.getId().toString());

        String id = fingerprintHelper.getStatsIDfromRecord(record);
        String identifier = record.getIdentifier();
        Long snapshotID = record.getSnapshot().getId();
        String origin = record.getSnapshot().getNetwork().getOriginURL();
        String metadataPrefix = record.getSnapshot().getNetwork().getMetadataPrefix();
        String networkAcronym = record.getSnapshot().getNetwork().getAcronym();
        Boolean isTransformed = record.getTransformed();
        Boolean isValid = validationResult.isValid();

        // Maps for occurrences
        Map<String, List<String>> validOccurrencesByRuleID = new HashMap<>();
        Map<String, List<String>> invalidOccurrencesByRuleID = new HashMap<>();
        List<String> validRulesID = new ArrayList<>();
        List<String> invalidRulesID = new ArrayList<>();

        for (ValidatorRuleResult ruleResult : validationResult.getRulesResults()) {

            String ruleID = ruleResult.getRule().getRuleId().toString();

            List<String> invalidOccr = new ArrayList<>();
            List<String> validOccr = new ArrayList<>();

            if (detailedDiagnose) {
                logger.debug("Detailed validation report - Rule ID: {}", ruleID);

                for (ContentValidatorResult contentResult : ruleResult.getResults()) {
                    if (contentResult.isValid())
                        validOccr.add(contentResult.getReceivedValue());
                    else
                        invalidOccr.add(contentResult.getReceivedValue());
                }

                validOccurrencesByRuleID.put(ruleID, validOccr);
                invalidOccurrencesByRuleID.put(ruleID, invalidOccr);
            }

            // Add valid and invalid rules
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
     * Registers a list of validation observations with IMMEDIATE streaming writes
     * 
     * OPTIMIZED FOR ValidationWorker pattern:
     * - ValidationWorker calls prePage() -> processItem() (1000x) -> postPage()
     * - postPage() calls this method with exactly 1000 observations per batch
     * - We use immediate streaming writes to minimize memory accumulation
     * - No size-based optimization needed - all batches use the same fast path
     * 
     * PERFORMANCE CHARACTERISTICS:
     * - For new files: Direct write (fastest path)
     * - For existing files: Read + merge + write (limitation of Parquet format)
     * - Memory usage: O(existing_file_size + 1000) instead of O(massive_dataset)
     * - CPU usage: Optimized for frequent small writes vs. rare large writes
     */
    public void registerObservations(List<ValidationStatObservationParquet> validationStatsObservations) {
        if (validationStatsObservations == null || validationStatsObservations.isEmpty()) {
            return;
        }
        
        try {
            int observationCount = validationStatsObservations.size();
            logger.debug("Processing {} observations with immediate streaming", observationCount);
            
            // Always use immediate streaming writes for all batch sizes
            // This is optimized for frequent 1000-record batches from ValidationWorker
            parquetRepository.saveAllImmediate(validationStatsObservations);
            
            logger.debug("Successfully streamed {} observations to Parquet", observationCount);
            
        } catch (Exception e) {
            logger.error("Error streaming observations: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to stream validation observations", e);
        }
    }    /**
     * Query validation rule statistics by snapshot (simplified version)
     */
    public Map<String, Object> queryValidatorRulesStatsBySnapshot(Long snapshotId) throws Exception {
        try {
            // Get aggregated statistics from repository
            Map<String, Object> aggregatedStats = parquetRepository.getAggregatedStats(snapshotId);
            
            Map<String, Object> result = new HashMap<>();
            result.put("size", ((Number) aggregatedStats.get("totalCount")).intValue());
            result.put("validSize", ((Number) aggregatedStats.get("validCount")).intValue());
            result.put("transformedSize", ((Number) aggregatedStats.get("transformedCount")).intValue());
            
            @SuppressWarnings("unchecked")
            Map<String, Long> validRuleCounts = (Map<String, Long>) aggregatedStats.get("validRuleCounts");
            @SuppressWarnings("unchecked")
            Map<String, Long> invalidRuleCounts = (Map<String, Long>) aggregatedStats.get("invalidRuleCounts");
            
            // Build simplified rule maps
            Map<String, Map<String, Object>> rulesByID = new HashMap<>();
            
            // Combine all found rules
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
            result.put("facets", new HashMap<String, Object>()); // Empty facets for simplicity
            
            return result;
            
        } catch (IOException e) {
            logger.error("Error querying validation stats for snapshot {}", snapshotId, e);
            throw new Exception("Error querying validation statistics", e);
        }
    }

    /**
     * Query validation rule statistics by snapshot
     */
    public ValidationStats queryValidatorRulesStatsBySnapshot(NetworkSnapshot snapshot, List<String> fq) throws Exception {

        ValidationStats result = new ValidationStats();

        try {
            // If there are filters, apply them to both statistics and facets
            if (fq != null && !fq.isEmpty()) {
                logger.debug("Applying filters to main statistics: {}", fq);
                
                // Process filters
                Map<String, Object> filters = parseFilterQueries(fq);
                ValidationStatParquetQueryEngine.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshot.getId());
                
                // Use OPTIMIZED repository methods - NO memory loading
                Map<String, Object> filteredStats = parquetRepository.getAggregatedStatsWithFilter(snapshot.getId(), aggregationFilter);
                
                result.size = ((Number) filteredStats.getOrDefault("totalCount", 0L)).intValue();
                result.validSize = ((Number) filteredStats.getOrDefault("validCount", 0L)).intValue();
                result.transformedSize = ((Number) filteredStats.getOrDefault("transformedCount", 0L)).intValue();
                
                logger.debug("OPTIMIZED filtered statistics: {} total, {} valid, {} transformed", 
                    result.size, result.validSize, result.transformedSize);
                
                // Build facets with filters using optimized method
                result.facets = buildSimulatedFacets(snapshot.getId(), fq);
                
                // Get rule counts from filtered stats for building rule statistics
                @SuppressWarnings("unchecked")
                Map<String, Long> validRuleCounts = (Map<String, Long>) filteredStats.getOrDefault("validRuleCounts", new HashMap<>());
                @SuppressWarnings("unchecked")
                Map<String, Long> invalidRuleCounts = (Map<String, Long>) filteredStats.getOrDefault("invalidRuleCounts", new HashMap<>());
                
                // Build rule statistics using filtered counts
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
                // Without filters, use aggregated statistics (original behavior)
                logger.info("No filters - using complete aggregated statistics");
                
                Map<String, Object> aggregatedStats = parquetRepository.getAggregatedStats(snapshot.getId());
                
                result.size = ((Number) aggregatedStats.get("totalCount")).intValue();
                result.validSize = ((Number) aggregatedStats.get("validCount")).intValue();
                result.transformedSize = ((Number) aggregatedStats.get("transformedCount")).intValue();

                @SuppressWarnings("unchecked")
                Map<String, Long> validRuleCounts = (Map<String, Long>) aggregatedStats.get("validRuleCounts");
                @SuppressWarnings("unchecked")
                Map<String, Long> invalidRuleCounts = (Map<String, Long>) aggregatedStats.get("invalidRuleCounts");

                // Build facets without filters
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
            throw new Exception("Error querying validation statistics: " + e.getMessage());
        }

        return result;
    }

    /**
     * Checks if the service is available
     */
    public boolean isServiceAvailable() {
        try {
            // Check that the base directory exists and is accessible
            return parquetRepository != null;
        } catch (Exception e) {
            logger.error("Error checking parquet service availability", e);
            return false;
        }
    }

    /**
     * Query valid rule occurrences count by snapshot ID and rule ID
     */
    public ValidationRuleOccurrencesCount queryValidRuleOccurrencesCountBySnapshotID(Long snapshotID, Long ruleID, List<String> fq) {

        ValidationRuleOccurrencesCount result = new ValidationRuleOccurrencesCount();

        try {
            String ruleIdStr = ruleID.toString();
            
            // Get valid and invalid occurrence counts
            Map<String, Long> validOccurrences = parquetRepository.getRuleOccurrenceCounts(snapshotID, ruleIdStr, true);
            Map<String, Long> invalidOccurrences = parquetRepository.getRuleOccurrenceCounts(snapshotID, ruleIdStr, false);

            List<OccurrenceCount> validRuleOccurrence = validOccurrences.entrySet().stream()
                    .map(entry -> new OccurrenceCount(entry.getKey(), entry.getValue().intValue()))
                    .sorted((a, b) -> Integer.compare(b.count, a.count)) // Sort by count descending
                    .collect(Collectors.toList());

            List<OccurrenceCount> invalidRuleOccurrence = invalidOccurrences.entrySet().stream()
                    .map(entry -> new OccurrenceCount(entry.getKey(), entry.getValue().intValue()))
                    .sorted((a, b) -> Integer.compare(b.count, a.count)) // Sort by count descending
                    .collect(Collectors.toList());

            result.setValidRuleOccrs(validRuleOccurrence);
            result.setInvalidRuleOccrs(invalidRuleOccurrence);

        } catch (IOException e) {
            logger.error("Error querying rule occurrences", e);
            result.setValidRuleOccrs(new ArrayList<>());
            result.setInvalidRuleOccrs(new ArrayList<>());
        }

        return result;
    }

    /**
     * Query validation observations by snapshot ID with OPTIMIZED pagination (never loads all records in memory)
     */
    public List<ValidationStatObservationParquet> queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, int page, int size) {
        logger.debug("OPTIMIZED Parquet query - snapshotID: {}, filters: {}, page: {}, size: {}", snapshotID, fq, page, size);
        
        try {
            // Apply filters if present
            Map<String, Object> filters = parseFilterQueries(fq);
            
            if (filters.isEmpty()) {
                return parquetRepository.findBySnapshotIdWithPagination(snapshotID, page, size);
            } else {
                logger.debug("Applying optimized filters: {}", filters);
                
                // **USE ADVANCED OPTIMIZATIONS**: Convert filters to AggregationFilter
                ValidationStatParquetQueryEngine.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshotID);
                
                return parquetRepository.findWithFilterAndPagination(snapshotID, aggregationFilter, page, size);
            }
        } catch (IOException e) {
            logger.error("Error querying optimized observations", e);
            return new ArrayList<>();
        }
    }

    /**
     * Query validation statistics observations by snapshot ID with pagination (never loads all records in memory)
     */
    @Override
    public ValidationStatsQueryResult queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) throws ValidationStatisticsException {
        logger.debug("OPTIMIZED Parquet query - snapshotID: {}, filters: {}, page: {}, size: {}", snapshotID, fq, pageable.getPageNumber(), pageable.getPageSize());
        
        try {
            if (fq != null && !fq.isEmpty()) {
                Map<String, Object> filters = parseFilterQueries(fq);
                logger.debug("Applying optimized filters: {}", filters);
                
                // **USE ADVANCED OPTIMIZATIONS**: Convert filters to AggregationFilter
                ValidationStatParquetQueryEngine.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshotID);
                
                // Use optimized methods with pagination - never loads all data
                List<ValidationStatObservationParquet> pageResults = 
                    parquetRepository.findWithFilterAndPagination(snapshotID, aggregationFilter, pageable.getPageNumber(), pageable.getPageSize());
                
                // Get total count using optimizations (without loading all data)
                long totalElements = parquetRepository.countRecordsWithFilter(snapshotID, aggregationFilter);
                
                logger.debug("OPTIMIZED results - found: {} total, {} in page", totalElements, pageResults.size());
                
                // Return Parquet objects directly - JSON serialization will work correctly
                return new ValidationStatsQueryResult(
                    pageResults,
                    totalElements,
                    pageable
                );
            } else {
                // Without filters, use direct pagination
                List<ValidationStatObservationParquet> parquetObservations = parquetRepository.findBySnapshotIdWithPagination(
                    snapshotID, 
                    pageable.getPageNumber(), 
                    pageable.getPageSize()
                );
                
                long totalElements = parquetRepository.countBySnapshotId(snapshotID);
                
                // Return Parquet objects directly
                return new ValidationStatsQueryResult(
                    parquetObservations,
                    totalElements,
                    pageable
                );
            }

        } catch (IOException e) {
            logger.error("Error querying Parquet with pagination for snapshot {}", snapshotID, e);
            throw new ValidationStatisticsException("Error querying validation observations", e);
        }
    }

    /**
     * Converts filters from String format to Map for Parquet repository (no System.out)
     */
    private Map<String, Object> parseFilterQueries(List<String> fq) {
        Map<String, Object> filters = new HashMap<>();
        
        if (fq == null || fq.isEmpty()) {
            return filters;
        }
        
        logger.debug("Processing filters: {}", fq);
        
        for (String fqTerm : fq) {
            if (fqTerm.contains(":")) {
                String[] parts = fqTerm.split(":", 2);
                String key = parts[0].trim();
                String value = parts[1].trim();
                
                if (value.equals("true") || value.equals("false")) {
                    filters.put(key, Boolean.parseBoolean(value));
                } else {
                    filters.put(key, value);
                }
            }
        }
        
        logger.debug("Final filter map: {}", filters);
        return filters;
    }

    /**
     * Deletes validation observations by snapshot ID
     */
    public void deleteValidationStatsObservationsByRecordIDsAndSnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            parquetRepository.deleteBySnapshotId(snapshotID);
            logger.info("Deleted validation observations for snapshot {}", snapshotID);
        } catch (IOException e) {
            logger.error("Error deleting observations for snapshot {}", snapshotID, e);
            throw new ValidationStatisticsException("Error deleting validation information | snapshot:" + snapshotID + " :: " + e.getMessage());
        }
    }

    /**
     * Deletes validation observations by record list
     */
    public void deleteValidationStatsObservationsByRecordsAndSnapshotID(Long snapshotID, Collection<OAIRecord> records) throws ValidationStatisticsException {
        for (OAIRecord record : records) {
            try {
                String id = fingerprintHelper.getStatsIDfromRecord(record);
                parquetRepository.deleteById(id, snapshotID);
            } catch (IOException e) {
                throw new ValidationStatisticsException("Error deleting validation information | snapshot:" + snapshotID + " recordID:" + record.getIdentifier() + " :: " + e.getMessage());
            }
        }
    }

    /**
     * Deletes validation observation by record
     */
    public void deleteValidationStatsObservationByRecordAndSnapshotID(Long snapshotID, OAIRecord record) throws ValidationStatisticsException {
        try {
            String id = fingerprintHelper.getStatsIDfromRecord(record);
            parquetRepository.deleteById(id, snapshotID);
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error deleting validation information | snapshot:" + snapshotID + " recordID:" + record.getIdentifier() + " :: " + e.getMessage());
        }
    }

    /**
     * Copies validation observations from one snapshot to another
     */
    public boolean copyValidationStatsObservationsFromTo(Long originalSnapshotId, Long newSnapshotId) throws ValidationStatisticsException {
        try {
            parquetRepository.copySnapshotData(originalSnapshotId, newSnapshotId);
            return true;
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error copying validation information | snapshot:" + originalSnapshotId + " to snapshot:" + newSnapshotId + " :: " + e.getMessage());
        }
    }

    /**
     * Deletes validation statistics by snapshot ID
     */
    public void deleteValidationStatsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            parquetRepository.deleteBySnapshotId(snapshotID);
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error deleting validation information | snapshot:" + snapshotID + " :: " + e.getMessage());
        }
    }

    // Private helper methods

    private ValidationStatObservation convertToValidationStatObservation(ValidationStatObservationParquet parquetObs) {
        ValidationStatObservation obs = new ValidationStatObservation();
        obs.setId(parquetObs.getId());
        obs.setIdentifier(parquetObs.getIdentifier());
        // Note: ValidationStatObservation may not have all setters - using compatible ones
        obs.setOrigin(parquetObs.getOrigin());
        obs.setSetSpec(parquetObs.getSetSpec());
        obs.setMetadataPrefix(parquetObs.getMetadataPrefix());
        obs.setNetworkAcronym(parquetObs.getNetworkAcronym());
        obs.setRepositoryName(parquetObs.getRepositoryName());
        obs.setInstitutionName(parquetObs.getInstitutionName());
        obs.setIsValid(parquetObs.getIsValid());
        obs.setIsTransformed(parquetObs.getIsTransformed());
        // Note: Some fields may not be compatible - skipping them for now
        return obs;
    }

    /**
     * Converts filters from fq format to optimized AggregationFilter
     */
    private ValidationStatParquetQueryEngine.AggregationFilter convertToAggregationFilter(Map<String, Object> filters, Long snapshotId) {
        ValidationStatParquetQueryEngine.AggregationFilter aggFilter = new ValidationStatParquetQueryEngine.AggregationFilter();
        aggFilter.setSnapshotId(snapshotId);
        
        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            switch (key) {
                case "record_is_valid":
                    if (value instanceof Boolean) {
                        aggFilter.setIsValid((Boolean) value);
                    }
                    break;
                case "record_is_transformed":
                    if (value instanceof Boolean) {
                        aggFilter.setIsTransformed((Boolean) value);
                    }
                    break;
                case "record_oai_id":
                    if (value instanceof String) {
                        aggFilter.setRecordOAIId((String) value);
                    }
                    break;
                case "valid_rules":
                    if (value instanceof String) {
                        aggFilter.setValidRulesFilter((String) value);
                    }
                    break;
                case "invalid_rules":
                    if (value instanceof String) {
                        aggFilter.setInvalidRulesFilter((String) value);
                    }
                    break;
            }
        }
        
        return aggFilter;
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
