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

package org.lareferencia.core.service.validation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.IOAIRecord;
import org.lareferencia.core.domain.NetworkSnapshot;
import org.lareferencia.core.repository.parquet.RecordValidation;
import org.lareferencia.core.repository.parquet.RuleFact;
import org.lareferencia.core.repository.parquet.SnapshotValidationStats;
import org.lareferencia.core.repository.parquet.ValidationStatParquetRepository;
import org.lareferencia.core.worker.validation.validator.ContentValidatorResult;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.IRecordFingerprintHelper;
import org.lareferencia.core.worker.validation.ValidatorResult;
import org.lareferencia.core.worker.validation.ValidatorRuleResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
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
    private ISnapshotStore snapshotStore;

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


    /**
     * Constructs a new ValidationStatisticsParquetService instance.
     */
    public ValidationStatisticsParquetService() {
        // Default constructor for Spring dependency injection
    }

    
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
    
 
    public void addObservation(SnapshotMetadata snapshotMetadata, IOAIRecord record, ValidatorResult validationResult) {
        
        Long snapshotId = snapshotMetadata.getSnapshotId();
        String networkAcronym = snapshotMetadata.getNetwork().getAcronym();
        logger.debug("Adding observation for record identifier: {} in snapshot {} network: {}", 
                    record.getIdentifier(), snapshotId, networkAcronym);

        // Create RecordValidation object usando constructor que calcula recordId automáticamente
        RecordValidation recordValidation = new RecordValidation(
            record.getIdentifier(),
            validationResult.isValid(),
            validationResult.isTransformed()
        );


        // Set published metadata hash
        recordValidation.setPublishedMetadataHash(validationResult.getMetadataHash());

        // set datestamp
        recordValidation.setDatestamp(record.getDatestamp());

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
            parquetRepository.saveRecordAndFacts(snapshotMetadata, recordValidation);

            logger.debug("Successfully added observation for record identifier: {} in snapshot {}", 
                        record.getIdentifier(), snapshotMetadata.getSnapshotId());
        } catch (IOException e) {
            logger.error("Error adding observation for record identifier: {} in snapshot {}: {}", 
                        record.getIdentifier(), snapshotId, e.getMessage(), e);
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
                // Obtener SnapshotMetadata desde snapshotStore
                SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshot.getId());
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
                // Obtener SnapshotMetadata desde snapshotStore
                SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshot.getId());
                if (metadata == null) {
                    throw new ValidationStatisticsException("Snapshot metadata not found for snapshot: " + snapshot.getId());
                }
                
                snapshotStats = parquetRepository.getSnapshotValidationStats(metadata);
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
     * Query validation statistics observations by snapshot ID with pagination.
     * 
     * IMPLEMENTACIÓN CON STREAMING Y FILTRADO:
     * - Usa ValidationRecordManager para lectura lazy (no carga todo en memoria)
     * - Aplica filtros usando ParsedFilters (misma técnica que buildStats)
     * - Acumula records filtrados hasta completar offset + size
     * - Retorna solo la página solicitada
     * 
     * EJEMPLO DE PAGINACIÓN:
     * - Página 0, size 20: Acumula 20 records filtrados, retorna [0-19]
     * - Página 1, size 20: Acumula 40 records filtrados, retorna [20-39]
     * - Página 2, size 20: Acumula 60 records filtrados, retorna [40-59]
     * 
     * @param snapshotID ID del snapshot
     * @param fq filtros opcionales (formato "field@@value")
     * @param pageable información de paginación (page, size)
     * @return resultado con observaciones de la página y total de elementos
     * @throws ValidationStatisticsException si hay error
     */
    @Override
    public ValidationStatsObservationsResult queryValidationStatsObservationsBySnapshotID(Long snapshotID, List<String> fq, Pageable pageable) throws ValidationStatisticsException {
        logger.info("QUERY OBSERVATIONS: snapshot={}, page={}, size={}, filters={}", 
                   snapshotID, pageable.getPageNumber(), pageable.getPageSize(), fq);
        
        try {
            // Calcular offset y límite para paginación
            int pageNumber = pageable.getPageNumber();
            int pageSize = pageable.getPageSize();
            int offset = pageNumber * pageSize;
            int limit = offset + pageSize;
            
            logger.debug("PAGINATION: offset={}, limit={}", offset, limit);
            
            // Lista para acumular observaciones de la página actual
            List<ValidationStatObservation> pageObservations = new ArrayList<>();
            
            // Delegar a método del repositorio que hace el filtrado y paginación
            Map<String, Object> result = parquetRepository.queryObservationsWithPagination(
                snapshotID, fq, offset, limit
            );

            // Obtener SnapshotMetadata desde snapshotStore
            SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotID);
            if (metadata == null) {
                throw new ValidationStatisticsException("Snapshot metadata not found for snapshot: " + snapshotID);
            }

            
            @SuppressWarnings("unchecked")
            List<RecordValidation> pageRecords = (List<RecordValidation>) result.get("records");
            Long totalFiltered = (Long) result.get("totalFiltered");
            
            // Convertir RecordValidation a ValidationStatObservation
            for (RecordValidation record : pageRecords) {
                ValidationStatObservation obs = convertRecordToObservation(record, metadata);
                pageObservations.add(obs);
            }
            
            logger.info("OBSERVATIONS QUERY COMPLETED: snapshot={}, page={}, returned={}, totalFiltered={}", 
                       snapshotID, pageNumber, pageObservations.size(), totalFiltered);
            
            return new ValidationStatsObservationsResult(pageObservations, totalFiltered, pageable);
            
        } catch (IOException e) {
            logger.error("Error querying observations for snapshot {}: {}", snapshotID, e.getMessage(), e);
            throw new ValidationStatisticsException("Error querying observations: " + e.getMessage(), e);
        }
    }
    
    /**
     * Convierte RecordValidation (Parquet) a ValidationStatObservation (API)
     */
    private ValidationStatObservation convertRecordToObservation(RecordValidation record, SnapshotMetadata metadata) {
        
        // Campos básicos (recordId es la PK única)
        String recordId = record.getRecordId();
        String identifier = record.getIdentifier();
        Boolean isValid = record.getRecordIsValid();
        Boolean isTransformed = record.getIsTransformed();
        
        // Listas de reglas válidas e inválidas
        List<String> validRulesID = new ArrayList<>();
        List<String> invalidRulesID = new ArrayList<>();
        
        // Maps para occurrences (solo si detailedDiagnose está activo)
        Map<String, List<String>> validOccurrencesByRuleID = new HashMap<>();
        Map<String, List<String>> invalidOccurrencesByRuleID = new HashMap<>();
        
        // Extraer información de RuleFacts
        if (record.getRuleFacts() != null) {
            for (RuleFact fact : record.getRuleFacts()) {
                String ruleID = String.valueOf(fact.getRuleId());
                
                if (fact.getIsValid() != null && fact.getIsValid()) {
                    validRulesID.add(ruleID);
                    
                    // Agregar occurrences válidas si existen
                    if (fact.getValidOccurrences() != null && !fact.getValidOccurrences().isEmpty()) {
                        validOccurrencesByRuleID.put(ruleID, new ArrayList<>(fact.getValidOccurrences()));
                    }
                } else {
                    invalidRulesID.add(ruleID);
                    
                    // Agregar occurrences inválidas si existen
                    if (fact.getInvalidOccurrences() != null && !fact.getInvalidOccurrences().isEmpty()) {
                        invalidOccurrencesByRuleID.put(ruleID, new ArrayList<>(fact.getInvalidOccurrences()));
                    }
                }
            }
        }

        
        // Crear observación (repositoryName e institutionName son null en nueva arquitectura)
        return new ValidationStatObservation(
            recordId, identifier, metadata.getSnapshotId(), 
            metadata.getNetwork().getOriginURL(), 
            null, // repositoryName (eliminado en nueva arquitectura)
            metadata.getNetwork().getMetadataPrefix(), 
            metadata.getNetwork().getAcronym(), 
            null, // institutionName (eliminado en nueva arquitectura)
            null, // setSpec (no disponible en RecordValidation)
            isValid, isTransformed,
            validOccurrencesByRuleID,
            invalidOccurrencesByRuleID,
            validRulesID,
            invalidRulesID
        );
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

    @Override
    public RecordValidation getRecordValidationListBySnapshotAndIdentifier(Long snapshotID, String identifier) throws ValidationStatisticsException {
            RecordValidation recordValidation = parquetRepository.getRecordValidationBySnapshotAndIdentifier(snapshotID, identifier);
            if (recordValidation == null) {
                throw new ValidationStatisticsException("No record validation found for snapshot " + snapshotID + " and identifier " + identifier);
            }
            return recordValidation;
      

    }

    @Override
    public SnapshotValidationStats getSnapshotValidationStats(Long snapshotID) throws ValidationStatisticsException {
        try {
            // Obtener SnapshotMetadata desde snapshotStore
            SnapshotMetadata metadata = snapshotStore.getSnapshotMetadata(snapshotID);
            if (metadata == null) {
                throw new ValidationStatisticsException("Snapshot metadata not found for snapshot: " + snapshotID);
            }
            
            SnapshotValidationStats snapshotStats = parquetRepository.getSnapshotValidationStats(metadata);
            if (snapshotStats == null) {
                throw new ValidationStatisticsException("No validation statistics found for snapshot " + snapshotID);
            }
            return snapshotStats;
        } catch (IOException e) {
            throw new ValidationStatisticsException("Error querying validation statistics from Parquet: " + e.getMessage());
        }
    }
    
 

}
