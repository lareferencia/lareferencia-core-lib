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

package org.lareferencia.core.service.validation;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.domain.NetworkSnapshot;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.repository.catalog.OAIRecord;
import org.lareferencia.core.repository.validation.*;
import org.lareferencia.core.util.PathUtils;
import org.lareferencia.core.worker.validation.ValidatorResult;
import org.lareferencia.core.worker.validation.ValidatorRuleResult;
import org.lareferencia.core.worker.validation.validator.ContentValidatorResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * SQLite-based implementation of IValidationStatisticsService.
 * 
 * ARCHITECTURE:
 * - Replaces Parquet-based validation storage
 * - Dynamic schema: rule columns created based on validator
 * - Maintains JSON stats file for precomputed statistics
 * 
 * RESPONSIBILITIES:
 * - Initialize validation database with dynamic schema
 * - Store validation observations as records
 * - Query validation statistics with filters
 * - Generate and persist statistics JSON
 */
@Service("validationStatisticsSQLiteService")
@Primary
@Scope("prototype")
public class ValidationStatisticsSQLiteService implements IValidationStatisticsService {

    private static final Logger logger = LogManager.getLogger(ValidationStatisticsSQLiteService.class);

    private static final String VALIDATION_SUBDIR = "validation";
    private static final String STATS_FILENAME = "validation-stats.json";

    @Value("${store.basepath:/tmp/data/}")
    private String basePath;

    @Autowired
    private ValidationDatabaseManager dbManager;

    @Autowired
    private RecordValidationRepository recordRepository;

    @Autowired
    private RuleOccurrenceRepository occurrenceRepository;

    @Autowired
    private ISnapshotStore snapshotStore;

    private final ObjectMapper objectMapper = new ObjectMapper();

    // Runtime state
    private boolean detailedDiagnose = false;
    private SnapshotMetadata currentMetadata;
    private List<Long> currentRuleIds;

    // Batch buffer for records
    private List<ValidationRecord> recordBuffer = new ArrayList<>();
    private List<RuleOccurrence> occurrenceBuffer = new ArrayList<>();
    private static final int BUFFER_SIZE = 1000;

    // In-memory stats accumulator
    private SnapshotValidationStats currentStats;

    // ========================================
    // CONFIGURATION
    // ========================================

    @Override
    public void setDetailedDiagnose(Boolean detailedDiagnose) {
        this.detailedDiagnose = detailedDiagnose != null && detailedDiagnose;
        logger.debug("ValidationStatisticsSQLiteService detailedDiagnose set to: {}", this.detailedDiagnose);
    }

    public boolean isDetailedDiagnose() {
        return detailedDiagnose;
    }

    // ========================================
    // LIFECYCLE
    // ========================================

    @Override
    public void initializeValidationForSnapshot(SnapshotMetadata snapshotMetadata) {
        logger.info("SQLITE: Initializing validation for snapshot {}", snapshotMetadata.getSnapshotId());

        this.currentMetadata = snapshotMetadata;

        // Extract rule IDs from metadata
        this.currentRuleIds = snapshotMetadata.getRuleDefinitions().keySet()
                .stream()
                .sorted()
                .collect(Collectors.toList());

        try {
            // Create database with dynamic schema
            dbManager.initializeSnapshot(snapshotMetadata, currentRuleIds);

            // Register rule IDs in repository for dynamic column binding
            recordRepository.registerRuleIds(snapshotMetadata.getSnapshotId(), currentRuleIds);

            // Initialize in-memory stats accumulator
            currentStats = new SnapshotValidationStats(snapshotMetadata);

            // Clear buffers
            recordBuffer.clear();
            occurrenceBuffer.clear();

            logger.info("SQLITE: Initialized with {} rules for snapshot {}",
                    currentRuleIds.size(), snapshotMetadata.getSnapshotId());

        } catch (IOException e) {
            logger.error("SQLITE: Failed to initialize snapshot {}: {}",
                    snapshotMetadata.getSnapshotId(), e.getMessage(), e);
            throw new RuntimeException("Failed to initialize validation for snapshot " +
                    snapshotMetadata.getSnapshotId(), e);
        }
    }

    @Override
    public void addObservation(SnapshotMetadata snapshotMetadata, IOAIRecord record, ValidatorResult validationResult) {
        Long snapshotId = snapshotMetadata.getSnapshotId();
        logger.debug("SQLITE: Adding observation for record {} in snapshot {}",
                record.getIdentifier(), snapshotId);

        // Build ValidationRecord
        ValidationRecord validationRecord = new ValidationRecord();
        validationRecord.setIdentifierHash(OAIRecord.generateIdFromIdentifier(record.getIdentifier()));
        validationRecord.setIdentifier(record.getIdentifier());
        validationRecord.setDatestamp(record.getDatestamp());
        validationRecord.setValid(validationResult.isValid());
        validationRecord.setTransformed(validationResult.isTransformed());
        validationRecord.setPublishedMetadataHash(validationResult.getMetadataHash());

        // Build rule results map
        Map<Long, Boolean> ruleResults = new HashMap<>();
        for (ValidatorRuleResult ruleResult : validationResult.getRulesResults()) {
            Long ruleId = ruleResult.getRule().getRuleId();
            ruleResults.put(ruleId, ruleResult.getValid());

            // Update in-memory stats and facets
            if (ruleResult.getValid()) {
                currentStats.incrementRuleValid(ruleId);
                currentStats.updateFacet("valid_rules", ruleId.toString());
            } else {
                currentStats.incrementRuleInvalid(ruleId);
                currentStats.updateFacet("invalid_rules", ruleId.toString());
            }

            // Collect occurrences if detailedDiagnose enabled
            if (detailedDiagnose && ruleResult.getRule().isStoreOccurrences()) {
                for (ContentValidatorResult contentResult : ruleResult.getResults()) {
                    RuleOccurrence occ = new RuleOccurrence(
                            validationRecord.getIdentifierHash(),
                            ruleId,
                            contentResult.isValid(),
                            contentResult.getReceivedValue());
                    occurrenceBuffer.add(occ);
                }
            }
        }
        validationRecord.setRuleResults(ruleResults);

        // Update global stats and facets
        currentStats.incrementTotalRecords();
        if (validationResult.isValid()) {
            currentStats.incrementValidRecords();
        }
        if (validationResult.isTransformed()) {
            currentStats.incrementTransformedRecords();
        }

        // Update record-level facets
        currentStats.updateFacet("record_is_valid", String.valueOf(validationResult.isValid()));
        currentStats.updateFacet("record_is_transformed", String.valueOf(validationResult.isTransformed()));

        // Buffer the record
        recordBuffer.add(validationRecord);

        // Flush if buffer full
        if (recordBuffer.size() >= BUFFER_SIZE) {
            flushBuffers(snapshotId);
        }
    }

    @Override
    public void finalizeValidationForSnapshot(Long snapshotId) {
        logger.info("SQLITE: Finalizing validation for snapshot {}", snapshotId);

        try {
            // Flush remaining records
            flushBuffers(snapshotId);

            // Write stats JSON
            writeStatsJson(snapshotId);

            // Close database
            dbManager.closeDataSource(snapshotId);
            recordRepository.clearCache(snapshotId);

            logger.info("SQLITE: Finalized validation for snapshot {} - {} total records",
                    snapshotId, currentStats.getTotalRecords());

        } catch (IOException e) {
            logger.error("SQLITE: Failed to finalize snapshot {}: {}", snapshotId, e.getMessage(), e);
            throw new RuntimeException("Failed to finalize validation for snapshot " + snapshotId, e);
        }
    }

    // ========================================
    // QUERIES
    // ========================================

    @Override
    public ValidationStatsResult queryValidatorRulesStatsBySnapshot(NetworkSnapshot snapshot, List<String> fq)
            throws ValidationStatisticsException {

        logger.debug("SQLITE: Querying stats for snapshot {}, filters={}", snapshot.getId(), fq);

        try {
            SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshot.getId());
            if (metadata == null) {
                throw new ValidationStatisticsException("Snapshot metadata not found: " + snapshot.getId());
            }

            SnapshotValidationStats stats;

            if (fq != null && !fq.isEmpty()) {
                // Compute filtered stats from database
                stats = buildFilteredStats(metadata, fq);
            } else {
                // Read from JSON (precomputed)
                stats = getSnapshotValidationStats(snapshot.getId());
            }

            return ValidationStatsResult.fromSnapshotValidationStats(stats);

        } catch (IOException e) {
            throw new ValidationStatisticsException("Error querying stats: " + e.getMessage(), e);
        }
    }

    @Override
    public ValidationStatsObservationsResult queryValidationStatsObservationsBySnapshotID(
            Long snapshotID, List<String> filters, Pageable pageable) throws ValidationStatisticsException {

        logger.debug("SQLITE: Querying observations for snapshot {}, page={}, size={}",
                snapshotID, pageable.getPageNumber(), pageable.getPageSize());

        try {
            SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotID);
            if (metadata == null) {
                throw new ValidationStatisticsException("Snapshot metadata not found: " + snapshotID);
            }

            // Open database if not already open
            if (!dbManager.hasActiveDataSource(snapshotID)) {
                dbManager.openSnapshotForRead(metadata);
                // Register rule IDs for reading
                List<Long> ruleIds = metadata.getRuleDefinitions().keySet()
                        .stream().sorted().collect(Collectors.toList());
                recordRepository.registerRuleIds(snapshotID, ruleIds);
            }

            int offset = pageable.getPageNumber() * pageable.getPageSize();
            int limit = pageable.getPageSize();

            // Query records
            List<ValidationRecord> records = recordRepository.queryWithPagination(
                    snapshotID, filters, offset, offset + limit);

            // Convert to observations
            List<ValidationStatObservation> observations = records.stream()
                    .map(r -> convertToObservation(r, metadata))
                    .collect(Collectors.toList());

            // Get total count
            long totalFiltered = recordRepository.countWithFilters(snapshotID, filters);

            return new ValidationStatsObservationsResult(observations, totalFiltered, pageable);

        } catch (IOException e) {
            throw new ValidationStatisticsException("Error querying observations: " + e.getMessage(), e);
        }
    }

    @Override
    public ValidationRuleOccurrencesCount queryValidRuleOccurrencesCountBySnapshotID(
            Long snapshotID, Long ruleID, List<String> fq) throws ValidationStatisticsException {

        logger.debug("SQLITE: Querying rule occurrences for snapshot {}, rule {}", snapshotID, ruleID);

        try {
            SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotID);
            if (metadata == null) {
                throw new ValidationStatisticsException("Snapshot metadata not found: " + snapshotID);
            }

            // Open database if needed
            if (!dbManager.hasActiveDataSource(snapshotID)) {
                dbManager.openSnapshotForRead(metadata);
            }

            // Get occurrences from rule_occurrences table
            Map<String, Map<String, Integer>> occurrences = occurrenceRepository.getOccurrencesByRule(snapshotID,
                    ruleID);

            // Convert to result format
            ValidationRuleOccurrencesCount result = new ValidationRuleOccurrencesCount();

            List<OccurrenceCount> validOccrs = occurrences.getOrDefault("valid", Collections.emptyMap())
                    .entrySet().stream()
                    .map(e -> new OccurrenceCount(e.getKey(), e.getValue()))
                    .sorted((a, b) -> b.getCount().compareTo(a.getCount()))
                    .collect(Collectors.toList());

            List<OccurrenceCount> invalidOccrs = occurrences.getOrDefault("invalid", Collections.emptyMap())
                    .entrySet().stream()
                    .map(e -> new OccurrenceCount(e.getKey(), e.getValue()))
                    .sorted((a, b) -> b.getCount().compareTo(a.getCount()))
                    .collect(Collectors.toList());

            result.setValidRuleOccrs(validOccrs);
            result.setInvalidRuleOccrs(invalidOccrs);

            return result;

        } catch (IOException e) {
            throw new ValidationStatisticsException("Error querying occurrences: " + e.getMessage(), e);
        }
    }

    @Override
    public RecordValidation getRecordValidationListBySnapshotAndIdentifier(Long snapshotID, String identifier)
            throws ValidationStatisticsException {

        try {
            SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotID);
            if (metadata == null) {
                throw new ValidationStatisticsException("Snapshot metadata not found: " + snapshotID);
            }

            // Open database if needed
            if (!dbManager.hasActiveDataSource(snapshotID)) {
                dbManager.openSnapshotForRead(metadata);
                List<Long> ruleIds = metadata.getRuleDefinitions().keySet()
                        .stream().sorted().collect(Collectors.toList());
                recordRepository.registerRuleIds(snapshotID, ruleIds);
            }

            ValidationRecord record = recordRepository.getByIdentifier(snapshotID, identifier);
            if (record == null) {
                throw new ValidationStatisticsException(
                        "No record validation found for snapshot " + snapshotID + " and identifier " + identifier);
            }

            // Convert to RecordValidation for API compatibility
            return convertToRecordValidation(record, snapshotID);

        } catch (IOException e) {
            throw new ValidationStatisticsException("Error getting record: " + e.getMessage(), e);
        }
    }

    @Override
    public SnapshotValidationStats getSnapshotValidationStats(Long snapshotID) throws ValidationStatisticsException {
        try {
            SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotID);
            if (metadata == null) {
                throw new ValidationStatisticsException("Snapshot metadata not found: " + snapshotID);
            }

            return readStatsJson(metadata);

        } catch (IOException e) {
            throw new ValidationStatisticsException("Error reading stats: " + e.getMessage(), e);
        }
    }

    // ========================================
    // DELETE OPERATIONS
    // ========================================

    @Override
    public void deleteValidationStatsObservationsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotID);
            if (metadata != null) {
                dbManager.deleteDatabase(metadata);
                recordRepository.clearCache(snapshotID);
                logger.info("SQLITE: Deleted validation data for snapshot {}", snapshotID);
            }
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error deleting observations: " + e.getMessage(), e);
        }
    }

    // ========================================
    // UTILITY
    // ========================================

    @Override
    public boolean isServiceAvailable() {
        return dbManager != null;
    }

    @Override
    public boolean validateFilters(List<String> filters) {
        if (filters == null || filters.isEmpty()) {
            return true;
        }
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

    // ========================================
    // PRIVATE HELPERS
    // ========================================

    private void flushBuffers(Long snapshotId) {
        if (!recordBuffer.isEmpty()) {
            try {
                recordRepository.insertBatch(snapshotId, recordBuffer);
                logger.debug("SQLITE: Flushed {} records", recordBuffer.size());
                recordBuffer.clear();
            } catch (IOException e) {
                logger.error("SQLITE: Failed to flush records: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to flush validation records", e);
            }
        }

        if (!occurrenceBuffer.isEmpty()) {
            try {
                occurrenceRepository.insertBatch(snapshotId, occurrenceBuffer);
                logger.debug("SQLITE: Flushed {} occurrences", occurrenceBuffer.size());
                occurrenceBuffer.clear();
            } catch (IOException e) {
                logger.error("SQLITE: Failed to flush occurrences: {}", e.getMessage(), e);
                throw new RuntimeException("Failed to flush occurrences", e);
            }
        }
    }

    private void writeStatsJson(Long snapshotId) throws IOException {
        String snapshotPath = PathUtils.getSnapshotPath(basePath, currentMetadata);
        Path statsPath = Paths.get(snapshotPath, VALIDATION_SUBDIR, STATS_FILENAME);

        Files.createDirectories(statsPath.getParent());
        objectMapper.writeValue(statsPath.toFile(), currentStats);

        logger.debug("SQLITE: Wrote stats JSON to {}", statsPath);
    }

    private SnapshotValidationStats readStatsJson(SnapshotMetadata metadata) throws IOException {
        String snapshotPath = PathUtils.getSnapshotPath(basePath, metadata);
        Path statsPath = Paths.get(snapshotPath, VALIDATION_SUBDIR, STATS_FILENAME);

        if (!Files.exists(statsPath)) {
            throw new IOException("Stats file not found: " + statsPath);
        }

        return objectMapper.readValue(statsPath.toFile(), SnapshotValidationStats.class);
    }

    private SnapshotValidationStats buildFilteredStats(SnapshotMetadata metadata, List<String> filters)
            throws IOException {

        Long snapshotId = metadata.getSnapshotId();

        // Open database if needed
        if (!dbManager.hasActiveDataSource(snapshotId)) {
            dbManager.openSnapshotForRead(metadata);
            List<Long> ruleIds = metadata.getRuleDefinitions().keySet()
                    .stream().sorted().collect(Collectors.toList());
            recordRepository.registerRuleIds(snapshotId, ruleIds);
        }

        SnapshotValidationStats stats = new SnapshotValidationStats(metadata);

        // Use optimized aggregation query
        List<Long> ruleIds = new ArrayList<>(metadata.getRuleDefinitions().keySet());
        RecordValidationRepository.AggregatedStats aggregated = recordRepository.getAggregatedStats(snapshotId, filters,
                ruleIds);

        // Populate stats from aggregation result
        stats.setTotalRecords((int) aggregated.getTotalRecords());
        stats.setValidRecords((int) aggregated.getValidRecords());
        stats.setTransformedRecords((int) aggregated.getTransformedRecords());

        // Process rule stats and facets
        for (Long ruleId : ruleIds) {
            long validCount = aggregated.getValidRuleCounts().getOrDefault(ruleId, 0L);
            long invalidCount = aggregated.getInvalidRuleCounts().getOrDefault(ruleId, 0L);

            // Register rule stats container
            stats.registerRule(ruleId);
            SnapshotValidationStats.RuleStats ruleStats = stats.getRuleStats(ruleId);

            // Increment logic is simulated since we have raw counts
            // Ideally RuleStats would have setters, but we loop for compatibility with
            // current API
            for (int i = 0; i < validCount; i++)
                ruleStats.incrementValid();
            for (int i = 0; i < invalidCount; i++)
                ruleStats.incrementInvalid();

            // Update facets for rules
            if (validCount > 0) {
                stats.updateFacet("valid_rules", ruleId.toString(), validCount);
            }
            if (invalidCount > 0) {
                stats.updateFacet("invalid_rules", ruleId.toString(), invalidCount);
            }
        }

        // Update record-level facets
        stats.updateFacet("record_is_valid", "true", aggregated.getValidRecords());
        stats.updateFacet("record_is_valid", "false", aggregated.getTotalRecords() - aggregated.getValidRecords());
        stats.updateFacet("record_is_transformed", "true", aggregated.getTransformedRecords());
        stats.updateFacet("record_is_transformed", "false",
                aggregated.getTotalRecords() - aggregated.getTransformedRecords());

        logger.debug("SQLITE: Built filtered stats using SQL Aggregation - total={}, valid={}",
                aggregated.getTotalRecords(), aggregated.getValidRecords());

        return stats;
    }

    private ValidationStatObservation convertToObservation(ValidationRecord record, SnapshotMetadata metadata) {
        List<String> validRulesID = new ArrayList<>();
        List<String> invalidRulesID = new ArrayList<>();

        for (Map.Entry<Long, Boolean> entry : record.getRuleResults().entrySet()) {
            if (entry.getValue()) {
                validRulesID.add(entry.getKey().toString());
            } else {
                invalidRulesID.add(entry.getKey().toString());
            }
        }

        return new ValidationStatObservation(
                record.getIdentifierHash(),
                record.getIdentifier(),
                metadata.getSnapshotId(),
                metadata.getNetwork().getOriginURL(),
                null, // repositoryName
                metadata.getNetwork().getMetadataPrefix(),
                metadata.getNetwork().getAcronym(),
                null, // institutionName
                null, // setSpec
                record.isValid(),
                record.isTransformed(),
                new HashMap<>(), // validOccurrencesByRuleID - empty, loaded on demand
                new HashMap<>(), // invalidOccurrencesByRuleID - empty
                validRulesID,
                invalidRulesID);
    }

    private RecordValidation convertToRecordValidation(ValidationRecord record, Long snapshotId) throws IOException {
        RecordValidation rv = new RecordValidation(
                record.getIdentifier(),
                record.getDatestamp(),
                record.isValid(),
                record.isTransformed(),
                record.getPublishedMetadataHash(),
                new ArrayList<>());

        // Convert rule results to RuleFacts
        for (Map.Entry<Long, Boolean> entry : record.getRuleResults().entrySet()) {
            RuleFact fact = new RuleFact();
            fact.setRuleId(entry.getKey().intValue());
            fact.setIsValid(entry.getValue());

            // Load occurrences if available
            if (detailedDiagnose) {
                List<RuleOccurrence> occurrences = occurrenceRepository.getOccurrencesByRecord(
                        snapshotId, record.getIdentifierHash());

                List<String> validOccs = new ArrayList<>();
                List<String> invalidOccs = new ArrayList<>();

                for (RuleOccurrence occ : occurrences) {
                    if (occ.getRuleId().equals(entry.getKey())) {
                        if (occ.isValid()) {
                            validOccs.add(occ.getOccurrenceValue());
                        } else {
                            invalidOccs.add(occ.getOccurrenceValue());
                        }
                    }
                }

                fact.setValidOccurrences(validOccs);
                fact.setInvalidOccurrences(invalidOccs);
            }

            rv.addRuleFact(fact);
        }

        return rv;
    }
}
