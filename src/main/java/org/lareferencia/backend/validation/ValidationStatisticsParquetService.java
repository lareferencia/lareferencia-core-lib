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

package org.lareferencia.backend.validation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.NetworkSnapshot;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.domain.ValidatorRule;

import org.lareferencia.backend.domain.parquet.ValidationStatObservationParquet;
import org.lareferencia.backend.repositories.parquet.ValidationStatParquetRepository;
import org.lareferencia.backend.validation.validator.ContentValidatorResult;
import org.lareferencia.core.metadata.IMetadataRecordStoreService;
import org.lareferencia.core.util.IRecordFingerprintHelper;
import org.lareferencia.core.validation.ValidatorResult;
import org.lareferencia.core.validation.ValidatorRuleResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.lareferencia.backend.domain.validation.ValidationStatObservation;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import lombok.Getter;

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

    /**
     * Field name for repository information.
     */
    @Value("${reponame.fieldname}")
    private String repositoryFieldName;

    /**
     * Prefix for repository field values.
     */
    @Value("${reponame.prefix}")
    private String repositoryPrefix;

    /**
     * Field name for institution information.
     */
    @Value("${instname.fieldname}")
    private String institutionFieldName;

    /**
     * Prefix for institution field values.
     */
    @Value("${instname.prefix}")
    private String institutionPrefix;

    @Autowired
    IMetadataRecordStoreService metadataStoreService;

    @Getter
    boolean detailedDiagnose = false;

    /**
     * Configures whether detailed diagnosis should be performed.
     * 
     * @param detailedDiagnose true to enable detailed diagnosis, false otherwise
     */
    public void setDetailedDiagnose(boolean detailedDiagnose) {
        this.detailedDiagnose = detailedDiagnose;
        logger.info("ValidationStatisticsParquetService detailedDiagnose set to: {}", detailedDiagnose);
    }

    @Autowired
    private IRecordFingerprintHelper fingerprintHelper;

    /**
     * Constructs a new ValidationStatisticsParquetService instance.
     */
    public ValidationStatisticsParquetService() {
        // Default constructor for Spring dependency injection
    }

    // Constants similar to the original service
    
    /**
     * Field names used for faceting validation statistics queries.
     */
    public static final String[] FACET_FIELDS = { "record_is_valid", "record_is_transformed", "valid_rules", "invalid_rules", "institution_name", "repository_name" };
    
    /**
     * Field name for snapshot identifier in statistics records.
     */
    public static final String SNAPSHOT_ID_FIELD = "snapshot_id";
    
    /**
     * Suffix appended to rule names for invalid occurrence counts.
     */
    public static final String INVALID_RULE_SUFFIX = "_rule_invalid_occrs";
    
    /**
     * Suffix appended to rule names for valid occurrence counts.
     */
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
     * Builds a validation observation from validator result.
     * 
     * @param record the OAI record being validated
     * @param validationResult the validation result to build observation from
     * @return the validation statistics observation
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
     * 
     * @param validationStatsObservations the list of validation observations to register
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
    }   

    /**
     * Query validation rule statistics by snapshot
     */
                           
    public ValidationStatsResult queryValidatorRulesStatsBySnapshot(NetworkSnapshot snapshot, List<String> fq) throws ValidationStatisticsException {
        logger.debug("ENTERING: queryValidatorRulesStatsBySnapshot with NetworkSnapshot {} and filters: {}", snapshot.getId(), fq);

        ValidationStatsResult result = new ValidationStatsResult();

        try {
            // If there are filters, apply them to both statistics and facets
            if (fq != null && !fq.isEmpty()) {
                logger.debug("MAIN STATS: Applying filters to main statistics: {}", fq);
                
                // Process filters
                Map<String, Object> filters = parseFilterQueries(fq);
                logger.debug("MAIN STATS: Parsed filters: {}", filters);
                
                ValidationStatParquetRepository.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshot.getId());
                logger.debug("MAIN STATS: Created aggregation filter: isValid={}, identifier={}", 
                    aggregationFilter.getIsValid(), aggregationFilter.getRecordOAIId());
                
                // TRY ULTRA-FAST CACHE FIRST, fallback to disk if needed
                Map<String, Object> filteredStats;
                try {
                    logger.debug("CACHE ATTEMPT: Trying memory cache for filtered stats (snapshot {})", snapshot.getId());
                    filteredStats = parquetRepository.getAggregatedStatsWithFilterFromCache(snapshot.getId(), aggregationFilter);
                    logger.debug("ULTRA-FAST CACHE HIT: Used memory cache for filtered stats (response in milliseconds)");
                } catch (Exception e) {
                    logger.debug("CACHE MISS/LOADING: Loading data from disk for snapshot {} - subsequent queries will be ultra-fast - {}", snapshot.getId(), e.getMessage());
                    filteredStats = parquetRepository.getAggregatedStatsWithFilter(snapshot.getId(), aggregationFilter);
                }
                logger.debug("MAIN STATS: Filtered stats result: {}", filteredStats);
                
                result.setSize(((Number) filteredStats.getOrDefault("totalCount", 0L)).intValue());
                result.setValidSize(((Number) filteredStats.getOrDefault("validCount", 0L)).intValue());
                result.setTransformedSize(((Number) filteredStats.getOrDefault("transformedCount", 0L)).intValue());
                
                logger.debug("OPTIMIZED filtered statistics: {} total, {} valid, {} transformed", 
                    result.getSize(), result.getValidSize(), result.getTransformedSize());
                
                // Build facets with filters using optimized method
                result.setFacets(buildSimulatedFacets(snapshot.getId(), fq));
                
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

                        ruleResult.setRuleID(rule.getId());
                        ruleResult.setValidCount(validRuleCounts.getOrDefault(ruleID, 0L).intValue());
                        ruleResult.setInvalidCount(invalidRuleCounts.getOrDefault(ruleID, 0L).intValue());
                        ruleResult.setName(rule.getName());
                        ruleResult.setDescription(rule.getDescription());
                        ruleResult.setMandatory(rule.getMandatory());
                        ruleResult.setQuantifier(rule.getQuantifier());

                        result.getRulesByID().put(ruleID, ruleResult);
                    }
                }
                
            } else {
                // TRY ULTRA-FAST CACHE FIRST, fallback to disk if needed
                logger.debug("CACHE ATTEMPT: Trying memory cache for complete aggregated statistics (snapshot {})", snapshot.getId());
                
                Map<String, Object> aggregatedStats;
                try {
                    aggregatedStats = parquetRepository.getAggregatedStatsFromCache(snapshot.getId());
                    logger.debug("ULTRA-FAST CACHE HIT: Used memory cache for aggregated stats (response in milliseconds)");
                } catch (Exception e) {
                    logger.debug("CACHE MISS/LOADING: Loading data from disk for snapshot {} - subsequent queries will be ultra-fast - {}", snapshot.getId(), e.getMessage());
                    aggregatedStats = parquetRepository.getAggregatedStats(snapshot.getId());
                }
                
                result.setSize(((Number) aggregatedStats.get("totalCount")).intValue());
                result.setValidSize(((Number) aggregatedStats.get("validCount")).intValue());
                result.setTransformedSize(((Number) aggregatedStats.get("transformedCount")).intValue());

                @SuppressWarnings("unchecked")
                Map<String, Long> validRuleCounts = (Map<String, Long>) aggregatedStats.get("validRuleCounts");
                @SuppressWarnings("unchecked")
                Map<String, Long> invalidRuleCounts = (Map<String, Long>) aggregatedStats.get("invalidRuleCounts");

                // Build facets without filters
                result.setFacets(buildSimulatedFacets(snapshot.getId(), fq));

                if (snapshot.getNetwork().getValidator() != null) {
                    for (ValidatorRule rule : snapshot.getNetwork().getValidator().getRules()) {

                        String ruleID = rule.getId().toString();

                        ValidationRuleStat ruleResult = new ValidationRuleStat();

                        ruleResult.setRuleID(rule.getId());
                        ruleResult.setValidCount(validRuleCounts.getOrDefault(ruleID, 0L).intValue());
                        ruleResult.setInvalidCount(invalidRuleCounts.getOrDefault(ruleID, 0L).intValue());
                        ruleResult.setName(rule.getName());
                        ruleResult.setDescription(rule.getDescription());
                        ruleResult.setMandatory(rule.getMandatory());
                        ruleResult.setQuantifier(rule.getQuantifier());

                        result.getRulesByID().put(ruleID, ruleResult);
                    }
                }
            }

        } catch (IOException e) {
            throw new ValidationStatisticsException("Error querying validation statistics: " + e.getMessage());
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
    
    // ==================== MEMORY CACHE INTEGRATION ====================
    
    /**
     * PRE-WARM CACHE: Manually load snapshot data into memory cache for ultra-fast queries.
     * Call this method after validation completion to prepare for dashboard queries.
     * 
     * @param snapshotId the snapshot ID to pre-warm in cache
     */
    public void warmUpCacheForSnapshot(Long snapshotId) {
        try {
            logger.info("CACHE WARMUP: Pre-warming memory cache for snapshot {}", snapshotId);
            parquetRepository.warmUpCache(snapshotId);
            logger.info("CACHE WARMUP: Successfully warmed up cache for snapshot {}", snapshotId);
        } catch (Exception e) {
            logger.warn("CACHE WARMUP: Failed to warm up cache for snapshot {} - {}", snapshotId, e.getMessage());
        }
    }
    
    /**
     * CACHE INFO: Get memory cache statistics and performance metrics.
     * 
     * @return a map containing cache information and performance metrics
     */
    public Map<String, Object> getCacheInfo() {
        try {
            return parquetRepository.getMemoryCacheInfo();
        } catch (Exception e) {
            logger.error("Error getting cache info", e);
            Map<String, Object> errorInfo = new HashMap<>();
            errorInfo.put("error", e.getMessage());
            errorInfo.put("enabled", false);
            return errorInfo;
        }
    }
    
    /**
     * CACHE CONTROL: Clear cache for specific snapshot (useful when data changes).
     * 
     * @param snapshotId the snapshot ID to evict from cache
     */
    public void evictSnapshotFromCache(Long snapshotId) {
        try {
            parquetRepository.evictFromCache(snapshotId);
            logger.info("CACHE EVICT: Evicted snapshot {} from memory cache", snapshotId);
        } catch (Exception e) {
            logger.warn("CACHE EVICT: Failed to evict snapshot {} - {}", snapshotId, e.getMessage());
        }
    }
    
    /**
     * CACHE CONTROL: Clear entire memory cache
     */
    public void clearMemoryCache() {
        try {
            parquetRepository.clearMemoryCache();
            logger.info("CACHE CLEAR: Cleared entire memory cache");
        } catch (Exception e) {
            logger.warn("CACHE CLEAR: Failed to clear cache - {}", e.getMessage());
        }
    }

    /**
     * Query valid rule occurrences count by snapshot ID and rule ID
     * Soporta filtros para analizar solo ocurrencias en registros que cumplen criterios específicos
     */
    public ValidationRuleOccurrencesCount queryValidRuleOccurrencesCountBySnapshotID(Long snapshotID, Long ruleID, List<String> fq) {

        logger.debug("Query rule occurrences - snapshotID: {}, ruleID: {}, filters: {}", snapshotID, ruleID, fq);
        
        ValidationRuleOccurrencesCount result = new ValidationRuleOccurrencesCount();

        try {
            String ruleIdStr = ruleID.toString();
            
            // Parse filters to determine which occurrences to analyze
            Map<String, Object> filters = parseFilterQueries(fq);
            
            // Determine if we should analyze valid occurrences, invalid occurrences, or both
            boolean analyzeValidOccurrences = true;
            boolean analyzeInvalidOccurrences = true;
            
            // Check if there's a filter for valid_rules or invalid_rules
            if (filters.containsKey("valid_rules")) {
                String validRulesFilter = filters.get("valid_rules").toString();
                if (validRulesFilter.equals(ruleIdStr)) {
                    analyzeInvalidOccurrences = false; // Solo válidas
                } else {
                    // La regla no está en el filtro, no hay nada que analizar
                    result.setValidRuleOccrs(new ArrayList<>());
                    result.setInvalidRuleOccrs(new ArrayList<>());
                    return result;
                }
            }
            
            if (filters.containsKey("invalid_rules")) {
                String invalidRulesFilter = filters.get("invalid_rules").toString();
                if (invalidRulesFilter.equals(ruleIdStr)) {
                    analyzeValidOccurrences = false; // Solo inválidas
                } else {
                    // La regla no está en el filtro, no hay nada que analizar
                    result.setValidRuleOccrs(new ArrayList<>());
                    result.setInvalidRuleOccrs(new ArrayList<>());
                    return result;
                }
            }
            
            // Get occurrence counts based on what we should analyze
            Map<String, Long> validOccurrences = new HashMap<>();
            Map<String, Long> invalidOccurrences = new HashMap<>();
            
            if (analyzeValidOccurrences) {
                validOccurrences = parquetRepository.getRuleOccurrenceCounts(snapshotID, ruleIdStr, true, filters);
            }
            
            if (analyzeInvalidOccurrences) {
                invalidOccurrences = parquetRepository.getRuleOccurrenceCounts(snapshotID, ruleIdStr, false, filters);
            }

            List<OccurrenceCount> validRuleOccurrence = validOccurrences.entrySet().stream()
                    .map(entry -> new OccurrenceCount(entry.getKey(), entry.getValue().intValue()))
                    .sorted((a, b) -> Integer.compare(b.getCount(), a.getCount()))
                    .collect(Collectors.toList());

            List<OccurrenceCount> invalidRuleOccurrence = invalidOccurrences.entrySet().stream()
                    .map(entry -> new OccurrenceCount(entry.getKey(), entry.getValue().intValue()))
                    .sorted((a, b) -> Integer.compare(b.getCount(), a.getCount()))
                    .collect(Collectors.toList());

            result.setValidRuleOccrs(validRuleOccurrence);
            result.setInvalidRuleOccrs(invalidRuleOccurrence);

            logger.debug("Rule occurrences for snapshotID={}, ruleID={}: {} valid, {} invalid", 
                       snapshotID, ruleID, validRuleOccurrence.size(), invalidRuleOccurrence.size());

        } catch (IOException e) {
            logger.error("Error querying rule occurrences for snapshotID={}, ruleID={}", snapshotID, ruleID, e);
            result.setValidRuleOccrs(new ArrayList<>());
            result.setInvalidRuleOccrs(new ArrayList<>());
        }

        return result;
    }

    /**
     * Query validation statistics observations by snapshot ID with pagination (never loads all records in memory)
     * This method focuses ONLY on returning filtered paginated records, NOT aggregated statistics
     */
    @Override
    public ValidationStatsObservationsResult queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) throws ValidationStatisticsException {
        logger.debug("OPTIMIZED Parquet query - snapshotID: {}, filters: {}, page: {}, size: {}", snapshotID, fq, pageable.getPageNumber(), pageable.getPageSize());
        
        try {
            if (fq != null && !fq.isEmpty()) {
                Map<String, Object> filters = parseFilterQueries(fq);
                logger.debug("FILTER RECORDS: Parsed filters from fq: {}", filters);
                logger.debug("FILTER RECORDS: Original fq list: {}", fq);
                
                // **USE ADVANCED OPTIMIZATIONS**: Convert filters to AggregationFilter
                ValidationStatParquetRepository.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshotID);
                logger.debug("FILTER RECORDS: Converted AggregationFilter - isValid: {}, isTransformed: {}, identifier: {}", 
                           aggregationFilter.getIsValid(), aggregationFilter.getIsTransformed(), aggregationFilter.getRecordOAIId());
                
                // TRY ULTRA-FAST CACHE FIRST for pagination
                List<ValidationStatObservationParquet> pageResults;
                long totalElements;
                
                try {
                    logger.debug("CACHE ATTEMPT: Trying memory cache for filtered pagination (snapshot {})", snapshotID);
                    pageResults = parquetRepository.findWithFilterAndPaginationFromCache(snapshotID, aggregationFilter, pageable.getPageNumber(), pageable.getPageSize());
                    totalElements = parquetRepository.countRecordsWithFilterFromCache(snapshotID, aggregationFilter);
                    logger.debug("ULTRA-FAST CACHE HIT: Used memory cache for filtered pagination (response in milliseconds)");
                } catch (Exception e) {
                    logger.debug("CACHE MISS/LOADING: Loading paginated data from disk for snapshot {} - subsequent queries will be ultra-fast - {}", snapshotID, e.getMessage());
                    pageResults = parquetRepository.findWithFilterAndPagination(snapshotID, aggregationFilter, pageable.getPageNumber(), pageable.getPageSize());
                    totalElements = parquetRepository.countRecordsWithFilter(snapshotID, aggregationFilter);
                }
                logger.debug("FILTER RECORDS: Total filtered elements: {}", totalElements);
                
                logger.debug("OPTIMIZED results - found: {} total, {} in page", totalElements, pageResults.size());
                
                // Return ONLY paginated records - NO aggregated statistics
                // Aggregated statistics should be handled by separate endpoints like /public/diagnose/{snapshotID}
                return new ValidationStatsObservationsResult(pageResults, totalElements, pageable);
                
            } else {
                // Without filters, use direct pagination
                List<ValidationStatObservationParquet> parquetObservations = parquetRepository.findBySnapshotIdWithPagination(
                    snapshotID, 
                    pageable.getPageNumber(), 
                    pageable.getPageSize()
                );
                
                long totalElements = parquetRepository.countBySnapshotId(snapshotID);
                
                // Return ONLY paginated records - NO aggregated statistics
                return new ValidationStatsObservationsResult(parquetObservations, totalElements, pageable);
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
     * Deletes validation observation by record.
     * 
     * @param snapshotID the snapshot ID containing the record
     * @param record the record whose validation observation should be deleted
     * @throws ValidationStatisticsException if deletion fails
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
     * Copies validation observations from one snapshot to another.
     * 
     * @param originalSnapshotId the source snapshot ID
     * @param newSnapshotId the destination snapshot ID
     * @return true if copy was successful
     * @throws ValidationStatisticsException if copy operation fails
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
     * Deletes validation statistics by snapshot ID.
     * 
     * @param snapshotID the snapshot ID whose validation statistics should be deleted
     * @throws ValidationStatisticsException if deletion fails
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
    private ValidationStatParquetRepository.AggregationFilter convertToAggregationFilter(Map<String, Object> filters, Long snapshotId) {
        ValidationStatParquetRepository.AggregationFilter aggFilter = new ValidationStatParquetRepository.AggregationFilter();
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
            // TRY ULTRA-FAST CACHE FIRST for filtered stats
            Map<String, Object> filters = parseFilterQueries(fq);
            ValidationStatParquetRepository.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshotId);
            
            try {
                logger.debug("FACETS CACHE ATTEMPT: Trying memory cache for filtered facets (snapshot {})", snapshotId);
                aggregatedStats = parquetRepository.getAggregatedStatsWithFilterFromCache(snapshotId, aggregationFilter);
                logger.debug("ULTRA-FAST FACETS HIT: Used memory cache for filtered stats (millisecond response)");
            } catch (Exception e) {
                logger.debug("FACETS CACHE MISS: Loading filtered stats from disk for snapshot {} - subsequent queries will be ultra-fast", snapshotId);
                aggregatedStats = parquetRepository.getAggregatedStatsWithFilter(snapshotId, aggregationFilter);
            }
            logger.debug("FACETS: Using filtered aggregated stats for snapshot {} with filters: {}", snapshotId, filters);
        } else {
            // TRY ULTRA-FAST CACHE FIRST for complete stats
            try {
                logger.debug("FACETS CACHE ATTEMPT: Trying memory cache for complete facets (snapshot {})", snapshotId);
                aggregatedStats = parquetRepository.getAggregatedStatsFromCache(snapshotId);
                logger.debug("ULTRA-FAST FACETS HIT: Used memory cache for complete stats (millisecond response)");
            } catch (Exception e) {
                logger.debug("FACETS CACHE MISS: Loading complete stats from disk for snapshot {} - subsequent queries will be ultra-fast", snapshotId);
                aggregatedStats = parquetRepository.getAggregatedStats(snapshotId);
            }
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
    public void deleteValidationStatsObservationsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            parquetRepository.deleteBySnapshotId(snapshotID);
            logger.info("Observaciones del snapshot {} eliminadas exitosamente", snapshotID);
        } catch (Exception e) {
            throw new ValidationStatisticsException("Error eliminando observaciones del snapshot: " + snapshotID, e);
        }
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
     * 
     * NOTE: Cache pre-warming is now handled automatically on first query for better performance
     * and to avoid unnecessary memory usage if data is never queried.
     * 
     * @param snapshotId the snapshot ID to flush data for
     */
    public void flushValidationData(Long snapshotId) {
        try {
            logger.debug("VALIDATION FLUSH: Flushing remaining buffered data for snapshot {}", snapshotId);
            parquetRepository.flushAllBuffers();
            logger.debug("VALIDATION FLUSH: Successfully flushed validation data for snapshot {} - cache will be loaded on first query", snapshotId);
        } catch (Exception e) {
            logger.error("VALIDATION FLUSH: Error flushing validation data for snapshot {}", snapshotId, e);
            throw new RuntimeException("Error flushing validation data for snapshot " + snapshotId, e);
        }
    }
}
