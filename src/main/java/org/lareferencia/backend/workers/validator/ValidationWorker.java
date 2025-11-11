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

package org.lareferencia.backend.workers.validator;

import java.text.NumberFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.SnapshotStatus;
import org.lareferencia.backend.domain.parquet.OAIRecord;
import org.lareferencia.backend.repositories.jpa.NetworkRepository;
import org.lareferencia.backend.services.SnapshotLogService;
import org.lareferencia.backend.services.ValidationService;
import org.lareferencia.backend.validation.IValidationStatisticsService;
import org.lareferencia.backend.validation.ValidationStatisticsException;
import org.lareferencia.core.metadata.IMetadataStore;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadataParseException;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.validation.ITransformer;
import org.lareferencia.core.validation.IValidator;
import org.lareferencia.core.validation.ValidationException;
import org.lareferencia.core.validation.ValidatorResult;
import org.lareferencia.core.worker.OAIRecordParquetWorker;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Worker that performs validation and transformation of harvested OAI records using Parquet storage.
 * <p>
 * This is the NEW Parquet-based implementation that uses:
 * - {@link org.lareferencia.backend.repositories.parquet.OAIRecordParquetRepository} for OAI records (Parquet)
 * - {@link ISnapshotStore} for snapshot metadata (SQL)
 * <p>
 * For the legacy SQL-based implementation, see {@link ValidationWorkerLegacy}.
 * <p>
 * This worker processes OAI records from Parquet storage in a network snapshot,
 * applying validation rules and metadata transformations. It supports both full and
 * incremental validation modes.
 * <p>
 * The worker performs the following operations:
 * </p>
 * <ul>
 *   <li>Reads OAI records from Parquet storage (streaming)</li>
 *   <li>Validates metadata records against configured validation rules</li>
 *   <li>Applies primary and secondary transformations to valid records</li>
 *   <li>Tracks validation statistics and observations</li>
 *   <li>Updates record status (stored in Parquet)</li>
 *   <li>Writes validated/transformed records back to Parquet</li>
 * </ul>
 * <p>
 * In incremental mode, only new (UNTESTED) records are processed. In full mode,
 * all non-deleted records are revalidated, allowing rule changes to be applied
 * retroactively.
 * </p>
 * 
 * @author LA Referencia Team
 * @see OAIRecord
 * @see IValidator
 * @see ITransformer
 * @see ValidationService
 * @see OAIRecordParquetWorker
 */
public class ValidationWorker extends OAIRecordParquetWorker {
	
	private static Logger logger = LogManager.getLogger(ValidationWorker.class);

	NumberFormat percentajeFormat = NumberFormat.getPercentInstance();

	
	@Autowired
	NetworkRepository networkRepository;
	
	@Autowired
	private SnapshotLogService snapshotLogService;
	
	@Autowired
	private ISnapshotStore snapshotStore;

	@Autowired
	private IMetadataStore metadataStoreService;
	
	
	private ITransformer transformer;
	private ITransformer secondaryTransformer;
	private IValidator validator;

	@Autowired
	private ValidationService validationManager;

	private SnapshotMetadata snapshotMetadata;
	private Long processedRecordsCount;
	

	//private ArrayList<ValidationStatObservation> validationStatsObservations = new ArrayList<ValidationStatObservation>();
	
	@Autowired
	IValidationStatisticsService validationStatisticsService;
	
	
	// reusable objects
	private ValidatorResult reusableValidationResult;
	private Boolean wasTransformed;
	
	/**
	 * Constructs a new validation worker.
	 */
	public ValidationWorker()  {
		super();
		
	}
	
	@Override
	public void preRun() {

		processedRecordsCount = 0L;

		// check if validation statistics service is available
		if ( ! validationStatisticsService.isServiceAvailable() ) {
			logError("Validation Statistics Service is not available, can't run validation");
			this.stop();
			return;
		}
	
		// new reusable validation result
		reusableValidationResult = new ValidatorResult();
		
		// trata de conseguir la ultima cosecha válida o el revalida el lgk 
		this.snapshotId = snapshotStore.findLastHarvestingSnapshot( runningContext.getNetwork() );


		if ( snapshotId != null ) {

		
			snapshotMetadata = snapshotStore.getSnapshotMetadata( snapshotId );

			try {
				validationStatisticsService.deleteValidationStatsObservationsBySnapshotID(snapshotId);
			} catch (ValidationStatisticsException e) {
				logError("Error deleting previous validation results: " + e.getMessage());
				this.stop();
			}

			
			// INITIALIZE: Create fresh writers AFTER cleanup
			logInfo("Initializing validation statistics for snapshot: " + snapshotId);
			validationStatisticsService.initializeValidationForSnapshot( snapshotStore.getSnapshotMetadata(snapshotId)  );

		
			logger.debug("Detailed diagnose: " + runningContext.getNetwork().getBooleanPropertyValue("DETAILED_DIAGNOSE") );
			validationStatisticsService.setDetailedDiagnose( runningContext.getNetwork().getBooleanPropertyValue("DETAILED_DIAGNOSE") );
			
	
			try {
					
				if ( runningContext.getNetwork().getValidator() != null ) {
					validator = validationManager.createValidatorFromModel(runningContext.getNetwork().getValidator());
				} else {
					logInfo("No Validator was found for " + runningContext.toString() + "!!!");
				}

				if ( runningContext.getNetwork().getTransformer() != null ) {
					transformer = validationManager.createTransformerFromModel(runningContext.getNetwork().getTransformer());
				}

				if ( runningContext.getNetwork().getSecondaryTransformer() != null ) {
					secondaryTransformer = validationManager.createTransformerFromModel(runningContext.getNetwork().getSecondaryTransformer());
				}

				if ( transformer == null || secondaryTransformer == null )
					logInfo("No transformers for "+  runningContext.toString() +"!!!");
				
	
			} catch (ValidationException  e) {
				logError(runningContext.toString() + ": " + e.getMessage());
				this.stop();
				return;
			}
						
		
		} else {
			logger.error("There is not a suitable snapshot for validation");
			this.stop();
		}
		
	}



	
	@Override
	public void processItem(OAIRecord record) {
		
		
		try {
		    // reset validation result
            reusableValidationResult.reset();

			
			logger.debug( "Validating record: " + record.getId() + " :: "+ record.getIdentifier() );
			logger.debug("Initial status: "  + record.getId() + " :: "+ record.getIdentifier() + "::" );
			wasTransformed = false;
			
			// carga la metadata original sin transformar
			logger.debug("Load metadata: "  + record.getId() + " :: "+ record.getIdentifier() );
			String metadataStr = metadataStoreService.getMetadata( record.getOriginalMetadataHash() );
			OAIRecordMetadata metadata = new OAIRecordMetadata(record.getIdentifier(), metadataStr );

			
			// si corresponde lo transforma

			logger.debug("Starting transformations: " + record.getId() + " :: "+ record.getIdentifier() );
			
			// transforma
			
			if ( transformer != null ) {
				logger.debug("Primary transformer: "  + record.getId() + " :: "+ record.getIdentifier() );
				wasTransformed |= transformer.transform(this.runningContext, record, metadata);
			}
			
			if ( secondaryTransformer != null ) {
				logger.debug("Secondary transformer: "  + record.getId() + " :: "+ record.getIdentifier() );
				wasTransformed |= secondaryTransformer.transform(this.runningContext, record, metadata);
			}
				
			logger.debug( record.getId() + " :: "+ record.getIdentifier() + "  Transformed: " + wasTransformed);
			
			// if validator is defined
			if ( validator != null ) {

				logger.debug("Validating: "  + record.getId() + " :: "+ record.getIdentifier() );
				
				// validación
				reusableValidationResult = validator.validate(metadata, reusableValidationResult);
				reusableValidationResult.setTransformed(wasTransformed);
				
				// update record validation status
				//metadataStoreService.updateRecordStatus(record, reusableValidationResult.isValid() ? RecordStatus.VALID : RecordStatus.INVALID, wasTransformed);
					
				
			}
			else { // if no validator is set, then record is consired valid and the validation results are set to true
				
			    reusableValidationResult.setValid(true);

	            // update record validation status
				//metadataStoreService.updateRecordStatus(record, RecordStatus.VALID, wasTransformed);

			}
			
			
			logger.debug( record.getId() + " :: "+ record.getIdentifier() + " final status: " );

			// store metadata if needed and set publishedMetadataHash
			String publishedMetadataHash = record.getOriginalMetadataHash();
			// Store metadata if transformed
			if ( wasTransformed ) {
				publishedMetadataHash = metadataStoreService.storeAndReturnHash(metadata.toString());
			}
			// store publishedMetadataHash in validation result
			reusableValidationResult.setMetadataHash( publishedMetadataHash );

			// Se almacenan las estadísticas de cosecha
            logger.debug("Storing diagnose "  + record.getId() + " :: "+ record.getIdentifier() );
            // validationStatsObservations.add( validationStatisticsService.buildObservation(record, reusableValidationResult) );
			validationStatisticsService.addObservation(snapshotMetadata, record, reusableValidationResult);



			
		} catch (OAIRecordMetadataParseException e) {
		
			logError("Metadata parsing record ID: " + record.getId() + " oai_id: " + record.getIdentifier() + " :: "  +  e.getMessage() );
			//logger.debug( record.getOriginalXML());
			setSnapshotStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
			this.stop();
			
		} catch (ValidationException e) {
			logError("Validation error:" + runningContext.toString() + ": " + e.getMessage() );
			setSnapshotStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
			this.stop();
		
		} catch (Exception e) {
			logError("Unknown validation error:" + runningContext.toString() + ": " + e.getMessage());
			setSnapshotStatus(SnapshotStatus.HARVESTING_FINISHED_VALID);
			this.stop();
		}

		processedRecordsCount++;
		// log info every 1000 records
		if ( processedRecordsCount % 1000 == 0 ) {
			snapshotStore.saveSnapshot(snapshotId);
		}
		
	}


	@Override
	public void postRun() {
		// CRITICAL: Flush any remaining buffered validation data before marking as complete
		try {
			validationStatisticsService.finalizeValidationForSnapshot(snapshotId);
		} catch (Exception e) {
			logger.error("ERROR: Failed to flush validation data for snapshot {}", snapshotId, e);
		}
		
		setSnapshotStatus(SnapshotStatus.VALID);
		logInfo("Finishing Validation/Transformation of " + runningContext.toString() );

	}

	@Override
	public String toString() {
		return "Transform/Validate(" + percentajeFormat.format(this.getCompletionRate()) + ")" + (isIncremental() ? "(I)" : "(F)") ;
	}
	
	private void setSnapshotStatus(SnapshotStatus status) {
		// metodo auxiliar para centralizar la forma de actualizar status
		snapshotStore.updateSnapshotStatus(snapshotId, status);
		snapshotStore.saveSnapshot(snapshotId);

	}
	
	private void logError(String message) {
		logger.error(message);
		snapshotLogService.addEntry(snapshotId, "ERROR: " + message);		
	}

	private void logInfo(String message) {
		logger.info(message);
		snapshotLogService.addEntry(snapshotId, "INFO: " + message);		
	}

	
}
