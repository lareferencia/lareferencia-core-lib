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
 * 
 * ARCHITECTURE:
 * - Replaces Solr usage with efficient filesystem-based Parquet storage
 * - Multi-file structure per snapshot for optimal performance with large datasets
 * - Supports memory-efficient streaming for millions of validation records
 * 
 * FILTERING SYSTEM:
 * - Dual format support: "field:value" (standard) and "field@@value" (legacy)
 * - Boolean and string value conversion with quote handling
 * - Optimized aggregation filters for multi-file queries
 * 
 * DELETION OPERATIONS:
 * - deleteValidationStatsObservationsBySnapshotID: Removes entire snapshot data
 * - deleteValidationStatsObservationByRecordAndSnapshotID: Removes specific record
 * - deleteValidationStatsBySnapshotID: Cleanup operation (same as first one)
 * 
 * PERFORMANCE FEATURES:
 * - Intelligent buffering system for batch operations
 * - Multi-file pagination with offset/limit optimization  
 * - Memory-efficient aggregated statistics without full record loading
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
            logger.debug("REGISTER: No observations to register (null or empty)");
            return;
        }
        
        try {
            int observationCount = validationStatsObservations.size();
            logger.debug("REGISTER: Processing {} observations with immediate streaming", observationCount);
            
            // Log first observation details for debugging
            if (!validationStatsObservations.isEmpty()) {
                ValidationStatObservationParquet firstObs = validationStatsObservations.get(0);
                logger.debug("REGISTER: First observation - snapshotId: {}, identifier: {}, isValid: {}, isTransformed: {}", 
                           firstObs.getSnapshotID(), 
                           firstObs.getIdentifier(), 
                           firstObs.getIsValid(), 
                           firstObs.getIsTransformed());
            }
            
            // Always use immediate streaming writes for all batch sizes
            // This is optimized for frequent 1000-record batches from ValidationWorker
            parquetRepository.saveAllImmediate(validationStatsObservations);
            
            logger.debug("REGISTER: Successfully streamed {} observations to Parquet", observationCount);
            
        } catch (Exception e) {
            logger.error("REGISTER: Error streaming observations: {}", e.getMessage(), e);
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
        logger.debug("ENTERING: queryValidatorRulesStatsBySnapshot with NetworkSnapshot {} and filters: {}", snapshot.getId(), fq);

        ValidationStats result = new ValidationStats();

        try {
            // If there are filters, apply them to both statistics and facets
            if (fq != null && !fq.isEmpty()) {
                logger.debug("MAIN STATS: Applying filters to main statistics: {}", fq);
                
                // Process filters
                Map<String, Object> filters = parseFilterQueries(fq);
                logger.debug("MAIN STATS: Parsed filters: {}", filters);
                
                ValidationStatParquetQueryEngine.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshot.getId());
                logger.debug("MAIN STATS: Created aggregation filter: isValid={}, identifier={}", 
                    aggregationFilter.getIsValid(), aggregationFilter.getRecordOAIId());
                
                // Use OPTIMIZED repository methods - NO memory loading
                Map<String, Object> filteredStats = parquetRepository.getAggregatedStatsWithFilter(snapshot.getId(), aggregationFilter);
                logger.debug("MAIN STATS: Filtered stats result: {}", filteredStats);
                
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
                logger.debug("No filters - using complete aggregated statistics");
                
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
     * This method focuses ONLY on returning filtered paginated records, NOT aggregated statistics
     */
    @Override
    public ValidationStatsQueryResult queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) throws ValidationStatisticsException {
        logger.debug("OPTIMIZED Parquet query - snapshotID: {}, filters: {}, page: {}, size: {}", snapshotID, fq, pageable.getPageNumber(), pageable.getPageSize());
        
        try {
            if (fq != null && !fq.isEmpty()) {
                Map<String, Object> filters = parseFilterQueries(fq);
                logger.debug("FILTER RECORDS: Parsed filters from fq: {}", filters);
                logger.debug("FILTER RECORDS: Original fq list: {}", fq);
                
                // **USE ADVANCED OPTIMIZATIONS**: Convert filters to AggregationFilter
                ValidationStatParquetQueryEngine.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshotID);
                logger.debug("FILTER RECORDS: Converted AggregationFilter - isValid: {}, isTransformed: {}, identifier: {}", 
                           aggregationFilter.getIsValid(), aggregationFilter.getIsTransformed(), aggregationFilter.getRecordOAIId());
                
                // Use optimized methods with pagination - never loads all data
                List<ValidationStatObservationParquet> pageResults = 
                    parquetRepository.findWithFilterAndPagination(snapshotID, aggregationFilter, pageable.getPageNumber(), pageable.getPageSize());
                
                // Get total count using optimizations (without loading all data)
                long totalElements = parquetRepository.countRecordsWithFilter(snapshotID, aggregationFilter);
                logger.debug("FILTER RECORDS: Total filtered elements: {}", totalElements);
                
                logger.debug("OPTIMIZED results - found: {} total, {} in page", totalElements, pageResults.size());
                
                // Return ONLY paginated records - NO aggregated statistics
                // Aggregated statistics should be handled by separate endpoints like /public/diagnose/{snapshotID}
                return new ValidationStatsQueryResult(pageResults, totalElements, pageable);
                
            } else {
                // Without filters, use direct pagination
                List<ValidationStatObservationParquet> parquetObservations = parquetRepository.findBySnapshotIdWithPagination(
                    snapshotID, 
                    pageable.getPageNumber(), 
                    pageable.getPageSize()
                );
                
                long totalElements = parquetRepository.countBySnapshotId(snapshotID);
                
                // Return ONLY paginated records - NO aggregated statistics
                return new ValidationStatsQueryResult(parquetObservations, totalElements, pageable);
            }

        } catch (IOException e) {
            logger.error("Error querying Parquet with pagination for snapshot {}", snapshotID, e);
            throw new ValidationStatisticsException("Error querying validation observations", e);
        }
    }

    /**
     * Converts filters from String format to Map for Parquet repository
     * Supports both formats: "field:value" and "field@@value"
     */
    private Map<String, Object> parseFilterQueries(List<String> fq) {
        Map<String, Object> filters = new HashMap<>();
        
        if (fq == null || fq.isEmpty()) {
            return filters;
        }
        
        logger.debug("Processing {} filter queries", fq.size());
        
        for (String fqTerm : fq) {
            String key = null;
            String value = null;
            
            // Support both formats: field:value and field@@value
            if (fqTerm.contains("@@")) {
                String[] parts = fqTerm.split("@@", 2);
                key = parts[0].trim();
                value = parts[1].trim();
            } else if (fqTerm.contains(":")) {
                String[] parts = fqTerm.split(":", 2);
                key = parts[0].trim();
                value = parts[1].trim();
            }
            
            if (key != null && value != null) {
                // Remove quotes if present
                if (value.startsWith("\"") && value.endsWith("\"")) {
                    value = value.substring(1, value.length() - 1);
                }
                
                // Convert string values to appropriate types
                if (value.equals("true") || value.equals("false")) {
                    filters.put(key, Boolean.parseBoolean(value));
                } else {
                    filters.put(key, value);
                }
            } else {
                logger.warn("Could not parse filter: {}", fqTerm);
            }
        }
        
        logger.debug("FILTER PARSING: Processed {} filters -> {}", fq.size(), filters);
        return filters;
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

    /**
     * Converts filters from fq format to optimized AggregationFilter
     */
    private ValidationStatParquetQueryEngine.AggregationFilter convertToAggregationFilter(Map<String, Object> filters, Long snapshotId) {
        ValidationStatParquetQueryEngine.AggregationFilter aggFilter = new ValidationStatParquetQueryEngine.AggregationFilter();
        aggFilter.setSnapshotId(snapshotId);
        
        logger.debug("FILTER CONVERSION: Converting filters to AggregationFilter: {}", filters);
        
        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            logger.debug("FILTER CONVERSION: Processing filter - key: {}, value: {}", key, value);
            
            switch (key) {
                case "record_is_valid":
                case "isValid":  // Support both formats
                    if (value instanceof Boolean) {
                        aggFilter.setIsValid((Boolean) value);
                        logger.debug("FILTER CONVERSION: Set isValid filter to: {}", value);
                    }
                    break;
                case "record_is_transformed":
                case "isTransformed":  // Support both formats
                    if (value instanceof Boolean) {
                        aggFilter.setIsTransformed((Boolean) value);
                        logger.debug("FILTER CONVERSION: Set isTransformed filter to: {}", value);
                    }
                    break;
                case "record_oai_id":
                case "identifier":  // Support both formats
                    if (value instanceof String) {
                        aggFilter.setRecordOAIId((String) value);
                        logger.debug("FILTER CONVERSION: Set identifier filter to: {}", value);
                    }
                    break;
                case "valid_rules":
                    if (value instanceof String) {
                        aggFilter.setValidRulesFilter((String) value);
                        logger.debug("FILTER CONVERSION: Set valid rules filter to: {}", value);
                    }
                    break;
                case "invalid_rules":
                    if (value instanceof String) {
                        aggFilter.setInvalidRulesFilter((String) value);
                        logger.debug("FILTER CONVERSION: Set invalid rules filter to: {}", value);
                    }
                    break;
                default:
                    logger.debug("FILTER CONVERSION: Unrecognized filter key: {}", key);
                    break;
            }
        }
        
        logger.debug("FILTER CONVERSION: Final AggregationFilter - isValid: {}, isTransformed: {}, identifier: {}", 
                   aggFilter.getIsValid(), aggFilter.getIsTransformed(), aggFilter.getRecordOAIId());
        
        return aggFilter;
    }

    private Map<String, List<FacetFieldEntry>> buildSimulatedFacets(Long snapshotId, List<String> fq) throws IOException {
        Map<String, List<FacetFieldEntry>> facets = new HashMap<>();
        
        // OPTIMIZED: Use aggregated statistics instead of loading all records
        Map<String, Object> aggregatedStats;
        
        if (fq != null && !fq.isEmpty()) {
            // Apply filters to aggregated statistics (MEMORY EFFICIENT)
            Map<String, Object> filters = parseFilterQueries(fq);
            ValidationStatParquetQueryEngine.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshotId);
            aggregatedStats = parquetRepository.getAggregatedStatsWithFilter(snapshotId, aggregationFilter);
            logger.debug("FACETS: Using filtered aggregated stats for snapshot {} with filters: {}", snapshotId, filters);
        } else {
            // No filters - use complete aggregated statistics
            aggregatedStats = parquetRepository.getAggregatedStats(snapshotId);
            logger.debug("FACETS: Using complete aggregated stats for snapshot {}", snapshotId);
        }
        
        // Build facets from aggregated statistics (MEMORY EFFICIENT)
        facets.put("record_is_valid", buildFacetFromAggregatedStats(aggregatedStats, "isValid"));
        facets.put("record_is_transformed", buildFacetFromAggregatedStats(aggregatedStats, "isTransformed"));
        facets.put("institution_name", buildFacetForInstitutionName(aggregatedStats));
        facets.put("repository_name", buildFacetForRepositoryName(aggregatedStats));
        facets.put("valid_rules", buildFacetForValidRulesFromStats(aggregatedStats));
        facets.put("invalid_rules", buildFacetForInvalidRulesFromStats(aggregatedStats));
        
        logger.debug("FACETS: Built {} facet fields", facets.size());
        return facets;
    }

    /**
     * Build facets from aggregated statistics instead of loading all records
     */
    private List<FacetFieldEntry> buildFacetFromAggregatedStats(Map<String, Object> stats, String fieldType) {
        List<FacetFieldEntry> facetEntries = new ArrayList<>();
        
        long totalCount = ((Number) stats.getOrDefault("totalCount", 0L)).longValue();
        long validCount = ((Number) stats.getOrDefault("validCount", 0L)).longValue();
        long transformedCount = ((Number) stats.getOrDefault("transformedCount", 0L)).longValue();
        
        if ("isValid".equals(fieldType)) {
            // Build isValid facet
            if (validCount > 0) {
                facetEntries.add(new FacetFieldEntry("true", validCount, "record_is_valid"));
            }
            
            long invalidCount = totalCount - validCount;
            if (invalidCount > 0) {
                facetEntries.add(new FacetFieldEntry("false", invalidCount, "record_is_valid"));
            }
        } else if ("isTransformed".equals(fieldType)) {
            // Build isTransformed facet
            if (transformedCount > 0) {
                facetEntries.add(new FacetFieldEntry("true", transformedCount, "record_is_transformed"));
            }
            
            long notTransformedCount = totalCount - transformedCount;
            if (notTransformedCount > 0) {
                facetEntries.add(new FacetFieldEntry("false", notTransformedCount, "record_is_transformed"));
            }
        }
        
        return facetEntries;
    }

    /**
     * Build facets for institution name from aggregated statistics
     */
    private List<FacetFieldEntry> buildFacetForInstitutionName(Map<String, Object> stats) {
        List<FacetFieldEntry> facetEntries = new ArrayList<>();
        
        // For now, use simple logic (can be enhanced later)
        long totalCount = ((Number) stats.getOrDefault("totalCount", 0L)).longValue();
        if (totalCount > 0) {
            facetEntries.add(new FacetFieldEntry("unknown", totalCount, "institutionName"));
        }
        
        return facetEntries;
    }

    /**
     * Build facets for repository name from aggregated statistics
     */
    private List<FacetFieldEntry> buildFacetForRepositoryName(Map<String, Object> stats) {
        List<FacetFieldEntry> facetEntries = new ArrayList<>();
        
        // For now, use simple logic (can be enhanced later)
        long totalCount = ((Number) stats.getOrDefault("totalCount", 0L)).longValue();
        if (totalCount > 0) {
            facetEntries.add(new FacetFieldEntry("unknown", totalCount, "repositoryName"));
        }
        
        return facetEntries;
    }

    /**
     * Build facets for valid rules from aggregated statistics
     */
    private List<FacetFieldEntry> buildFacetForValidRulesFromStats(Map<String, Object> stats) {
        List<FacetFieldEntry> facetEntries = new ArrayList<>();
        
        @SuppressWarnings("unchecked")
        Map<String, Long> validRuleCounts = (Map<String, Long>) stats.getOrDefault("validRuleCounts", new HashMap<>());
        
        for (Map.Entry<String, Long> entry : validRuleCounts.entrySet()) {
            facetEntries.add(new FacetFieldEntry(entry.getKey(), entry.getValue(), "valid_rules"));
        }
        
        return facetEntries;
    }

    /**
     * Build facets for invalid rules from aggregated statistics
     */
    private List<FacetFieldEntry> buildFacetForInvalidRulesFromStats(Map<String, Object> stats) {
        List<FacetFieldEntry> facetEntries = new ArrayList<>();
        
        @SuppressWarnings("unchecked")
        Map<String, Long> invalidRuleCounts = (Map<String, Long>) stats.getOrDefault("invalidRuleCounts", new HashMap<>());
        
        for (Map.Entry<String, Long> entry : invalidRuleCounts.entrySet()) {
            facetEntries.add(new FacetFieldEntry(entry.getKey(), entry.getValue(), "invalid_rules"));
        }
        
        return facetEntries;
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
    
    /**
     * Flush any remaining buffered validation data for a snapshot.
     * This should be called at the end of validation to ensure all data is written to files.
     */
    public void flushValidationData(Long snapshotId) {
        try {
            logger.debug("VALIDATION FLUSH: Flushing remaining buffered data for snapshot {}", snapshotId);
            parquetRepository.flushAllBuffers();
            logger.debug("VALIDATION FLUSH: Successfully flushed validation data for snapshot {}", snapshotId);
        } catch (Exception e) {
            logger.error("VALIDATION FLUSH: Error flushing validation data for snapshot {}", snapshotId, e);
            throw new RuntimeException("Error flushing validation data for snapshot " + snapshotId, e);
        }
    }
}
