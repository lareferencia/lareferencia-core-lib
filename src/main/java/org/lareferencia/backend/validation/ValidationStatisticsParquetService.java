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
import org.lareferencia.backend.domain.parquet.RecordValidation;
import org.lareferencia.backend.domain.parquet.RuleFact;
import org.lareferencia.backend.domain.parquet.SnapshotValidationStats;
import org.lareferencia.backend.repositories.parquet.ValidationStatParquetRepository;
import org.lareferencia.backend.validation.validator.ContentValidatorResult;
import org.lareferencia.core.metadata.IMetadataRecordStoreService;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.IRecordFingerprintHelper;
import org.lareferencia.core.validation.ValidatorResult;
import org.lareferencia.core.validation.ValidatorRuleResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;


import java.io.IOException;
import java.util.*;


/**
 * Validation statistics service based on Fact Table Parquet architecture.
 * 
 * ARCHITECTURE:
 * - Fact table: 1 row per rule occurrence (optimized for analytics)
 * - Partitioning: snapshot_id / network / record_is_valid (validez del registro completo)
 * - Compression: ZSTD with dictionary encoding
 * - Queries: Predicate pushdown for columnar filtering + partition pruning
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

    @Autowired
    IMetadataRecordStoreService metadataStoreService;

    private boolean detailedDiagnose = false;

    /**
     * Configures whether detailed diagnosis should be performed.
     * 
     * @param detailedDiagnose true to enable detailed diagnosis, false otherwise
     */
    public void setDetailedDiagnose(Boolean detailedDiagnose) {
        this.detailedDiagnose = detailedDiagnose;
        logger.info("ValidationStatisticsParquetService detailedDiagnose set to: {}", detailedDiagnose);
    }
    
    public boolean isDetailedDiagnose() {
        return detailedDiagnose;
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
     * Initialize validation for snapshot (SIMPLIFIED)
     */
    public void initializeValidationForSnapshot(SnapshotMetadata snapshotMetadata) {
        logger.debug("SIMPLIFIED: Initialize validation for snapshot {} (no pre-cleanup needed)", snapshotMetadata.getSnapshotId());
        try {
            parquetRepository.initializeSnapshot( snapshotMetadata );
        } catch (IOException e) {
            logger.error("Error initializing validation for snapshot {}: {}", snapshotMetadata.getSnapshotId(), e.getMessage(), e);
            throw new RuntimeException("Failed to initialize validation for snapshot " + snapshotMetadata.getSnapshotId(), e);
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

    public void addObservation(Long snapshotId, OAIRecord record, ValidatorResult validationResult) {
                
        String id = fingerprintHelper.getStatsIDfromRecord(record);
        String networkAcronym = record.getSnapshot().getNetwork().getAcronym();
        logger.debug("Adding observation for record ID: {} in snapshot {} network: {}", id, snapshotId, networkAcronym);

        // Create RecordValidation object
        RecordValidation recordValidation = new RecordValidation();
        recordValidation.setId(id);
        recordValidation.setIdentifier(record.getIdentifier());
        recordValidation.setRecordIsValid(validationResult.isValid());
        recordValidation.setIsTransformed(record.getTransformed());

        // Create and add RuleFact objects directly to RecordValidation
        for (ValidatorRuleResult ruleResult : validationResult.getRulesResults()) {
            String ruleID = ruleResult.getRule().getRuleId().toString();
            RuleFact ruleFact = new RuleFact();
            ruleFact.setRuleId(Integer.parseInt(ruleID));
            ruleFact.setIsValid(ruleResult.getValid());


            if ( detailedDiagnose )
            { // detailed diagnose enabled

                // for validation of each occurrence
                for (ContentValidatorResult contentResult : ruleResult.getResults()) {
                
                    if (contentResult.isValid()) {
                        ruleFact.setValidOccurrences(
                            Collections.singletonList(contentResult.getReceivedValue())
                        );
                    } else {
                        ruleFact.setInvalidOccurrences(
                            Collections.singletonList(contentResult.getReceivedValue())
                        );
                    }
                }
            }
            
            recordValidation.addRuleFact(ruleFact);
        }

        try {
            parquetRepository.saveRecordAndFacts(snapshotId, networkAcronym, recordValidation);

            logger.debug("Successfully added observation for record ID: {} in snapshot {}", id, snapshotId);
        } catch (IOException e) {
            logger.error("Error adding observation for record ID: {} in snapshot {}: {}", id, snapshotId, e.getMessage(), e);
            throw new RuntimeException("Failed to add validation observation", e);
        }
       
        
    }


    /**
     * Query validation rule statistics by snapshot
     */
    public ValidationStatsResult queryValidatorRulesStatsBySnapshot(NetworkSnapshot snapshot, List<String> fq) throws ValidationStatisticsException {

        logger.debug("Querying stats for snapshot {}, filters={}", snapshot.getId(), fq);
        
        ValidationStatsResult result = new ValidationStatsResult();

        if (fq != null && !fq.isEmpty()) {
            // we need calculate the stats based on filters
            logger.info("FILTERS APPLIED: Calculating filtered snapshot statistics from Parquet for snapshot {}", snapshot.getId());
            try {
                // Obtener SnapshotMetadata desde el record store service
                SnapshotMetadata metadata = metadataStoreService.getSnapshotMetadata(snapshot.getId());
                if (metadata == null) {
                    throw new ValidationStatisticsException("Snapshot metadata not found for snapshot: " + snapshot.getId());
                }
                
                SnapshotValidationStats filteredStats = parquetRepository.buildStats(metadata, fq);
                result = ValidationStatsResult.fromSnapshotValidationStats(filteredStats);
                logger.info("FILTERED STATS: snapshot={}, total={}, valid={}, filters={}", 
                           snapshot.getId(), filteredStats.getTotalRecords(), filteredStats.getValidRecords(), fq);
            } catch (IOException e) {
                throw new ValidationStatisticsException("Error building filtered statistics for snapshot " + snapshot.getId() + ": " + e.getMessage());
            }


        } else {
            logger.debug("No filters applied, retrieving full snapshot statistics from Parquet for snapshot {}", snapshot.getId());
            
            // if validation stats exist in cache return otherwise read from parquet
            SnapshotValidationStats snapshotStats;
            try {
                snapshotStats = parquetRepository.getSnapshotValidationStats(snapshot.getId());
            } catch (IOException e) {
                throw new ValidationStatisticsException("Error querying validation statistics from Parquet: " + e.getMessage());
            }
            
            if (snapshotStats != null) {
                result = ValidationStatsResult.fromSnapshotValidationStats(snapshotStats);
                logger.debug("Validation statistics retrieved from Parquet for snapshot {}", snapshot.getId());
            } else {
                logger.warn("No validation statistics found for snapshot {} in Parquet", snapshot.getId());
            }
        }

        return result;
    }

    
    /**
     * Checks if the service is available
     */
    public boolean isServiceAvailable() {
        try {
            return parquetRepository != null;
        } catch (Exception e) {
            logger.error("Error checking parquet service availability", e);
            return false;
        }
    }

    /**
     * Query valid rule occurrences count by snapshot ID and rule ID.
     * 
     * IMPLEMENTACIÓN:
     * - Calcula SIEMPRE desde archivos Parquet (no hay datos precomputados)
     * - Lee todos los records del snapshot usando streaming
     * - Agrega occurrences por valor para la regla específica
     * - Aplica filtros opcionales si se proporcionan
     * 
     * @param snapshotID ID del snapshot
     * @param ruleID ID de la regla
     * @param fq filtros opcionales
     * @return objeto con listas de OccurrenceCount para valid e invalid occurrences
     */
    public ValidationRuleOccurrencesCount queryValidRuleOccurrencesCountBySnapshotID(Long snapshotID, Long ruleID, List<String> fq) {
        logger.info("QUERY RULE OCCURRENCES: snapshot={}, rule={}, filters={}", snapshotID, ruleID, fq);
        
        ValidationRuleOccurrencesCount result = new ValidationRuleOccurrencesCount();
        
        try {
            // Calcular occurrences desde Parquet (streaming)
            Map<String, Map<String, Integer>> occurrences = parquetRepository.calculateRuleOccurrences(
                snapshotID, 
                ruleID.intValue(), 
                fq
            );
            
            // Convertir Map<String, Integer> a List<OccurrenceCount> para valid occurrences
            Map<String, Integer> validMap = occurrences.get("valid");
            List<OccurrenceCount> validOccrs = new ArrayList<>();
            if (validMap != null) {
                validMap.forEach((value, count) -> {
                    validOccrs.add(new OccurrenceCount(value, count));
                });
            }
            
            // Convertir Map<String, Integer> a List<OccurrenceCount> para invalid occurrences
            Map<String, Integer> invalidMap = occurrences.get("invalid");
            List<OccurrenceCount> invalidOccrs = new ArrayList<>();
            if (invalidMap != null) {
                invalidMap.forEach((value, count) -> {
                    invalidOccrs.add(new OccurrenceCount(value, count));
                });
            }
            
            // Ordenar por count descendente (más frecuentes primero)
            validOccrs.sort((a, b) -> b.getCount().compareTo(a.getCount()));
            invalidOccrs.sort((a, b) -> b.getCount().compareTo(a.getCount()));
            
            result.setValidRuleOccrs(validOccrs);
            result.setInvalidRuleOccrs(invalidOccrs);
            
            logger.info("RULE OCCURRENCES QUERY COMPLETED: snapshot={}, rule={}, valid={}, invalid={}", 
                       snapshotID, ruleID, validOccrs.size(), invalidOccrs.size());
            
        } catch (IOException e) {
            logger.error("Error calculating rule occurrences for snapshot={}, rule={}: {}", 
                        snapshotID, ruleID, e.getMessage(), e);
            // Retornar listas vacías en caso de error
            result.setValidRuleOccrs(new ArrayList<>());
            result.setInvalidRuleOccrs(new ArrayList<>());
        }
        
        return result;
    }

    /**
     * Query validation statistics observations by snapshot ID with pagination (SIMPLIFIED)
     */
    @Override
    public ValidationStatsObservationsResult queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) throws ValidationStatisticsException {
        logger.debug("SIMPLIFIED: Querying observations for snapshot {}, page={}, size={}", 
                    snapshotID, pageable.getPageNumber(), pageable.getPageSize());
        
            // NUEVA ARQUITECTURA: Sin filtros complejos, paginación directa desde Layer 2
            // fq se ignora temporalmente - filtrado se puede agregar después si es necesario
            Boolean recordIsValid = null; // null = todos los records
            
            // List<ValidationStatObservation> observations = parquetRepository.findBySnapshotIdWithPagination(
            //     snapshotID, recordIsValid, pageable.getPageNumber(), pageable.getPageSize());
            // long totalElements = parquetRepository.countRecords(snapshotID);
            
            // return new ValidationStatsObservationsResult(observations, totalElements, pageable);
            return null;
            
      
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
     * Deletes validation observation by record (SIMPLIFIED - NOT IMPLEMENTED)
     */
    public void deleteValidationStatsObservationByRecordAndSnapshotID(Long snapshotID, OAIRecord record)  {
        // TODO: Implementar eliminación individual cuando sea necesario
        logger.warn("Individual record deletion not yet implemented in new architecture");
    }

    /**
     * Copies validation observations from one snapshot to another (SIMPLIFIED - NOT IMPLEMENTED)
     */
    public Boolean copyValidationStatsObservationsFromTo(Long originalSnapshotId, Long newSnapshotId)  {
        // TODO: Implementar copia cuando sea necesario
        logger.warn("Snapshot copy not yet implemented in new architecture");
        return false;
    }

    /**
     * Deletes validation statistics by snapshot ID (SIMPLIFIED)
     */
    public void deleteValidationStatsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            parquetRepository.deleteSnapshot(snapshotID);
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error deleting validation information | snapshot:" + snapshotID + " :: " + e.getMessage());
        }
    }

    // Private helper methods



   

    // Implementación de métodos de la interfaz IValidationStatisticsService
    
    @Override
    public void deleteValidationStatsObservationsBySnapshotID(Long snapshotID) throws ValidationStatisticsException {
        try {
            parquetRepository.deleteSnapshot(snapshotID);
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
     * Finaliza la validación de un snapshot.
     * 
     * CICLO DE VIDA COMPLETO:
     * 1. initializeValidationForSnapshot(id) → Preparación inicial
     * 2. registerObservations() × N veces → Escritura incremental con auto-flush
     * 3. finalizeValidationForSnapshot(id) → Cierre de writers y flush final
     * 
     * Este método debe ser llamado por el worker cuando termina completamente
     * la validación de un snapshot. Garantiza:
     * - Flush final de buffers pendientes (si hay < 10k registros sin flush)
     * - Cierre de archivos Parquet
     * - Liberación de memoria
     * - Datos 100% persistidos en disco
     * 
     * THREAD-SAFE: Puede ser llamado desde cualquier thread del worker
     * 
     * @param snapshotId el ID del snapshot que terminó validación
     * @throws IOException si hay error al finalizar
     */
    public void finalizeValidationForSnapshot(Long snapshotId)  {
        logger.info("FINALIZE VALIDATION: Snapshot {} - closing writers and flushing data", snapshotId);
        try {
            parquetRepository.finalizeSnapshot(snapshotId);
        } catch (IOException e) {
            // logger.error("Error finalizing validation for snapshot {}: {}", snapshotId, e.getMessage(), e);
            throw new RuntimeException("Error finalizing validation for snapshot " + snapshotId + ": " + e.getMessage(), e);
        }
        logger.info("Validation finalized for snapshot {} - all data persisted", snapshotId);
    }

 

 
    
  
    

}
