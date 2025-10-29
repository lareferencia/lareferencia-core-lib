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

import org.lareferencia.backend.domain.validation.ValidationStatObservation;
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
 * Validation statistics service based on Fact Table Parquet architecture.
 * 
 * ARCHITECTURE:
 * - Fact table: 1 row per rule occurrence (optimized for analytics)
 * - Partitioning: snapshot_id / network / is_valid
 * - Compression: ZSTD with dictionary encoding
 * - Queries: Predicate pushdown for columnar filtering
 * 
 * RESPONSIBILITIES:
 * - Transform validation results to observations
 * - Parse and convert filter queries
 * - Build result objects for API responses
 * - Delegate storage operations to repository
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
     * Initialize a new validation for a snapshot
     */
    public void initializeValidationForSnapshot(Long snapshotId) {
        try {
            logger.info("Initializing validation for snapshot {}", snapshotId);
            parquetRepository.cleanSnapshot(snapshotId);
            
            // DYNAMIC SIZING: Register snapshot size for optimal file sizing
            Integer snapshotSize = metadataStoreService.getSnapshotSize(snapshotId);
            if (snapshotSize != null && snapshotSize > 0) {
                parquetRepository.registerSnapshotSize(snapshotId, snapshotSize);
            }
            
            logger.info("Successfully initialized validation for snapshot {}", snapshotId);
        } catch (Exception e) {
            logger.error("Error initializing validation for snapshot {}", snapshotId, e);
            throw new RuntimeException("Failed to initialize validation for snapshot: " + snapshotId, e);
        }
    }
    
    /**
     * Registers the total size of a snapshot for dynamic file sizing optimization.
     * This method is called automatically during initialization, but can be called
     * explicitly if needed for recalculation.
     * 
     * @param snapshotId the snapshot ID
     * @param totalRecords total number of records in the snapshot
     */
    public void registerSnapshotSize(Long snapshotId, int totalRecords) {
        try {
            parquetRepository.registerSnapshotSize(snapshotId, totalRecords);
            logger.info("Registered snapshot {} size: {} records for dynamic sizing", snapshotId, totalRecords);
        } catch (Exception e) {
            logger.warn("Failed to register snapshot size (will use default sizing): {}", e.getMessage());
        }
    }

    /**
     * Builds a validation observation from validator result.
     * 
     * @param record the OAI record being validated
     * @param validationResult the validation result to build observation from
     * @return the validation statistics observation
     */
    public ValidationStatObservation buildObservation(OAIRecord record, ValidatorResult validationResult) {

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

        return new ValidationStatObservation(
                id, identifier, snapshotID, origin, null, metadataPrefix, networkAcronym,
                null, null, isValid, isTransformed, validOccurrencesByRuleID,
                invalidOccurrencesByRuleID, validRulesID, invalidRulesID
        );
    }

    /**
     * Registers a list of validation observations.
     * 
     * @param validationStatsObservations the list of validation observations to register
     */
    public void registerObservations(List<ValidationStatObservation> validationStatsObservations) {
        if (validationStatsObservations == null || validationStatsObservations.isEmpty()) {
            logger.debug("No observations to register");
            return;
        }
        
        try {
            logger.debug("Registering {} observations", validationStatsObservations.size());
            parquetRepository.saveAll(validationStatsObservations);
            logger.debug("Successfully registered {} observations", validationStatsObservations.size());
        } catch (Exception e) {
            logger.error("Error registering observations: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to register validation observations", e);
        }
    }   

    /**
     * Query validation rule statistics by snapshot
     */
    public ValidationStatsResult queryValidatorRulesStatsBySnapshot(NetworkSnapshot snapshot, List<String> fq) throws ValidationStatisticsException {
        logger.debug("Querying validation statistics for snapshot {} with filters: {}", snapshot.getId(), fq);

        ValidationStatsResult result = new ValidationStatsResult();

        try {
            Map<String, Object> aggregatedStats;
            
            if (fq != null && !fq.isEmpty()) {
                Map<String, Object> filters = parseFilterQueries(fq);
                ValidationStatParquetRepository.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshot.getId());
                aggregatedStats = parquetRepository.getAggregatedStatsWithFilter(snapshot.getId(), aggregationFilter);
            } else {
                aggregatedStats = parquetRepository.getAggregatedStats(snapshot.getId());
            }
            
            // Build result from aggregated stats
            result.setSize(((Number) aggregatedStats.getOrDefault("totalCount", 0L)).intValue());
            result.setValidSize(((Number) aggregatedStats.getOrDefault("validCount", 0L)).intValue());
            result.setTransformedSize(((Number) aggregatedStats.getOrDefault("transformedCount", 0L)).intValue());
            
            @SuppressWarnings("unchecked")
            Map<String, Long> validRuleCounts = (Map<String, Long>) aggregatedStats.getOrDefault("validRuleCounts", new HashMap<>());
            @SuppressWarnings("unchecked")
            Map<String, Long> invalidRuleCounts = (Map<String, Long>) aggregatedStats.getOrDefault("invalidRuleCounts", new HashMap<>());
            
            // Build facets
            result.setFacets(buildSimulatedFacets(snapshot.getId(), fq));
            
            // Build rule statistics
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
     * Query validation statistics observations by snapshot ID with pagination
     */
    @Override
    public ValidationStatsObservationsResult queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) throws ValidationStatisticsException {
        logger.debug("Querying observations for snapshot {} with filters: {}, page: {}, size: {}", 
                    snapshotID, fq, pageable.getPageNumber(), pageable.getPageSize());
        
        try {
            if (fq != null && !fq.isEmpty()) {
                Map<String, Object> filters = parseFilterQueries(fq);
                ValidationStatParquetRepository.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshotID);
                
                List<ValidationStatObservation> pageResults = parquetRepository.findWithFilterAndPagination(
                    snapshotID, aggregationFilter, pageable.getPageNumber(), pageable.getPageSize());
                long totalElements = parquetRepository.countRecordsWithFilter(snapshotID, aggregationFilter);
                
                return new ValidationStatsObservationsResult(pageResults, totalElements, pageable);
            } else {
                List<ValidationStatObservation> parquetObservations = parquetRepository.findBySnapshotIdWithPagination(
                    snapshotID, pageable.getPageNumber(), pageable.getPageSize());
                long totalElements = parquetRepository.countBySnapshotId(snapshotID);
                
                return new ValidationStatsObservationsResult(parquetObservations, totalElements, pageable);
            }
        } catch (IOException e) {
            logger.error("Error querying observations for snapshot {}", snapshotID, e);
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
     * Converts filters from fq format to AggregationFilter
     */
    private ValidationStatParquetRepository.AggregationFilter convertToAggregationFilter(Map<String, Object> filters, Long snapshotId) {
        ValidationStatParquetRepository.AggregationFilter aggFilter = new ValidationStatParquetRepository.AggregationFilter();
        aggFilter.setSnapshotId(snapshotId);
        
        logger.debug("CONVERT TO AGG FILTER: Starting conversion with filters: {}", filters);
        
        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            logger.debug("CONVERT TO AGG FILTER: Processing key={}, value={}, valueType={}", key, value, value.getClass().getSimpleName());
            
            switch (key) {
                case "record_is_valid":
                case "isValid":
                    if (value instanceof Boolean) {
                        aggFilter.setIsValid((Boolean) value);
                        logger.debug("CONVERT TO AGG FILTER: Set isValid to {}", value);
                    } else {
                        logger.warn("CONVERT TO AGG FILTER: isValid value is not Boolean: {} ({})", value, value.getClass());
                    }
                    break;
                case "record_is_transformed":
                case "isTransformed":
                    if (value instanceof Boolean) {
                        aggFilter.setIsTransformed((Boolean) value);
                        logger.debug("CONVERT TO AGG FILTER: Set isTransformed to {}", value);
                    } else {
                        logger.warn("CONVERT TO AGG FILTER: isTransformed value is not Boolean: {} ({})", value, value.getClass());
                    }
                    break;
                case "record_oai_id":
                case "identifier":
                    if (value instanceof String) {
                        aggFilter.setRecordOAIId((String) value);
                        logger.debug("CONVERT TO AGG FILTER: Set recordOAIId to {}", value);
                    }
                    break;
                case "valid_rules":
                    if (value instanceof String) {
                        aggFilter.setValidRulesFilter((String) value);
                        logger.debug("CONVERT TO AGG FILTER: Set validRulesFilter to {}", value);
                    }
                    break;
                case "invalid_rules":
                    if (value instanceof String) {
                        aggFilter.setInvalidRulesFilter((String) value);
                        logger.debug("CONVERT TO AGG FILTER: Set invalidRulesFilter to {}", value);
                    }
                    break;
                default:
                    logger.warn("CONVERT TO AGG FILTER: Unknown filter key: {}", key);
                    break;
            }
        }
        
        logger.debug("CONVERT TO AGG FILTER: Result -> isValid={}, isTransformed={}, recordOAIId={}", 
                    aggFilter.getIsValid(), aggFilter.getIsTransformed(), aggFilter.getRecordOAIId());
        
        return aggFilter;
    }

    private Map<String, List<FacetFieldEntry>> buildSimulatedFacets(Long snapshotId, List<String> fq) throws IOException {
        Map<String, List<FacetFieldEntry>> facets = new HashMap<>();
        
        Map<String, Object> aggregatedStats;
        
        if (fq != null && !fq.isEmpty()) {
            Map<String, Object> filters = parseFilterQueries(fq);
            ValidationStatParquetRepository.AggregationFilter aggregationFilter = convertToAggregationFilter(filters, snapshotId);
            aggregatedStats = parquetRepository.getAggregatedStatsWithFilter(snapshotId, aggregationFilter);
        } else {
            aggregatedStats = parquetRepository.getAggregatedStats(snapshotId);
        }
        
        // Build facets from aggregated statistics
        facets.put("record_is_valid", buildFacetFromAggregatedStats(aggregatedStats, "isValid"));
        facets.put("record_is_transformed", buildFacetFromAggregatedStats(aggregatedStats, "isTransformed"));
        facets.put("institution_name", buildFacetForInstitutionName(aggregatedStats));
        facets.put("repository_name", buildFacetForRepositoryName(aggregatedStats));
        facets.put("valid_rules", buildFacetForValidRulesFromStats(aggregatedStats));
        facets.put("invalid_rules", buildFacetForInvalidRulesFromStats(aggregatedStats));
        
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
            // Ya no necesitamos conversión - todos son ValidationStatObservation
            registerObservations(observations);
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
     * 
     * @param snapshotId the snapshot ID to flush data for
     */
    public void flushValidationData(Long snapshotId) {
        try {
            logger.debug("Flushing buffered data for snapshot {}", snapshotId);
            parquetRepository.flushAllBuffers();
            logger.debug("Successfully flushed validation data for snapshot {}", snapshotId);
        } catch (Exception e) {
            logger.error("Error flushing validation data for snapshot {}", snapshotId, e);
            throw new RuntimeException("Error flushing validation data for snapshot " + snapshotId, e);
        }
    }
}
