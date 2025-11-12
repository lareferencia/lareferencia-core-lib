package org.lareferencia.core.service.validation;

import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.domain.NetworkSnapshot;
import org.lareferencia.core.repository.parquet.RecordValidation;
import org.lareferencia.core.repository.parquet.SnapshotValidationStats;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.worker.validation.ValidatorResult;
import org.springframework.data.domain.Pageable;


import java.util.List;

/**
 * Main interface for validation statistics services
 * Allows abstraction of the implementation (Parquet, Solr, DB, etc.)
 */
public interface IValidationStatisticsService {
    
      
    /**
     * Query observations by snapshot ID with filters and pagination
     * @param snapshotID Snapshot ID
     * @param filters List of filters in "field@@value" format
     * @param pageable Pagination configuration
     * @return Query result with paginated data
     * @throws ValidationStatisticsException If an error occurs during the query
     */
    ValidationStatsObservationsResult queryValidationStatsObservationsBySnapshotID(
        Long snapshotID, 
        List<String> filters, 
        Pageable pageable
    ) throws ValidationStatisticsException;
    
    /**
     * Query validation rules statistics by snapshot
     * @param snapshot The snapshot for which to query statistics
     * @param filters List of applied filters
     * @return Statistics aggregated by rules
     * @throws ValidationStatisticsException If an error occurs during the query
     */
    ValidationStatsResult queryValidatorRulesStatsBySnapshot(
        NetworkSnapshot snapshot, 
        List<String> filters
    ) throws ValidationStatisticsException;


    /**
     * Query the count of valid rule occurrences by snapshot
     * @param snapshotID Snapshot ID
     * @param ruleID Rule ID
     * @param fq Additional filters
     * @return Count of valid rule occurrences
     * @throws ValidationStatisticsException If an error occurs during the query
     */
    ValidationRuleOccurrencesCount queryValidRuleOccurrencesCountBySnapshotID(Long snapshotID, Long ruleID, List<String> fq) throws ValidationStatisticsException;



    /**
     * Delete observations by snapshot ID
     * @param snapshotID Snapshot ID to delete
     * @throws ValidationStatisticsException If an error occurs during deletion
     */
    void deleteValidationStatsObservationsBySnapshotID(Long snapshotID) throws ValidationStatisticsException;

    
    
    /**
     * Initialize a new validation for a specific snapshot
     * This method should be called when starting a new validation process
     * to ensure a clean state and remove previous validation data
     * @param metadata Snapsphot metadata
     * @throws ValidationStatisticsException If an error occurs during initialization
     */
    void initializeValidationForSnapshot(SnapshotMetadata metadata);
    
    /**
     * Check if the service is available and working
     * @return true if the service is available
     */
    boolean isServiceAvailable();
           
    /**
     * Validate the provided filters
     * @param filters List of filters to validate
     * @return true if all filters are valid
     */
    boolean validateFilters(List<String> filters);

    void setDetailedDiagnose(Boolean booleanPropertyValue);

    void addObservation(SnapshotMetadata snapshotMetadata, IOAIRecord record, ValidatorResult reusableValidationResult);

    RecordValidation getRecordValidationListBySnapshotAndIdentifier(Long snapshotID, String identifier) throws ValidationStatisticsException;   


    void finalizeValidationForSnapshot(Long snapshotId);

    SnapshotValidationStats getSnapshotValidationStats(Long snapshotID) throws ValidationStatisticsException;
}
