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
import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.SnapshotStatus;
import org.lareferencia.backend.domain.ValidationStatObservation;
import org.lareferencia.backend.repositories.jpa.NetworkRepository;
import org.lareferencia.backend.services.SnapshotLogService;
import org.lareferencia.backend.services.ValidationService;
import org.lareferencia.backend.services.ValidationStatisticsException;
import org.lareferencia.backend.services.ValidationStatisticsService;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.IMetadataRecordStoreService;
import org.lareferencia.core.metadata.MetadataRecordStoreException;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadataParseException;
import org.lareferencia.core.metadata.RecordStatus;
import org.lareferencia.core.validation.ITransformer;
import org.lareferencia.core.validation.IValidator;
import org.lareferencia.core.validation.ValidationException;
import org.lareferencia.core.validation.ValidatorResult;
import org.lareferencia.core.worker.BaseBatchWorker;
import org.lareferencia.core.worker.IPaginator;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;

public class ValidationWorker extends BaseBatchWorker<OAIRecord, NetworkRunningContext> {
	
	private static Logger logger = LogManager.getLogger(ValidationWorker.class);

	NumberFormat percentajeFormat = NumberFormat.getPercentInstance();

	
	@Autowired
	NetworkRepository networkRepository;
	
	@Autowired
	private SnapshotLogService snapshotLogService;
	
	@Autowired 
	private IMetadataRecordStoreService metadataStoreService;
	
	
	private ITransformer transformer;
	private ITransformer secondaryTransformer;
		
	private IValidator validator;

	@Autowired
	private ValidationService validationManager;

	private Long snapshotId;
	

	private ArrayList<ValidationStatObservation> validationStatsObservations = new ArrayList<ValidationStatObservation>();
	
	@Autowired
	ValidationStatisticsService validationStatisticsService;
	
	
	// reusable objects
	private ValidatorResult reusableValidationResult;
	private Boolean wasTransformed;
	
	
	public ValidationWorker()  {
		super();
		
	}
	
	@Override
	public void preRun() {


		if ( ! validationStatisticsService.isServiceAvaliable() ) {
			logError("Validation Statistics Service is not available, can't run validation");
			this.stop();
			return;
		}
		
		// new reusable validation result
		reusableValidationResult = new ValidatorResult();
		
		// trata de conseguir la ultima cosecha válida o el revalida el lgk 
		this.snapshotId = metadataStoreService.findLastHarvestingSnapshot( runningContext.getNetwork() );

		if ( snapshotId != null ) {

			Long previousSnapshotId = metadataStoreService.getPreviousSnapshotId(snapshotId);

			logInfo("Starting Validation/Tranformation " + runningContext.toString() + "  -  snapshot: " + snapshotId + (isIncremental() ? " (incremental)" : " (full)"));
			/***
			 * Si es una validación incremental se crea un paginado que solo considera los untested, 
			 * para la validacion full se considera los no deleted, eso asegura que se vuelvan a evaluar los valid e invalid
			 * si las reglas cambiaron pueden cambiar su status
			 */

			if (isIncremental() && previousSnapshotId != null) {

				// if is the first incremental validation, copy the results from the previous snapshot
				if (metadataStoreService.getSnapshotStatus(snapshotId) == SnapshotStatus.HARVESTING_FINISHED_VALID) {

					try {
						this.copyAndUpdatePreviousValidationResults(previousSnapshotId, snapshotId);
					} catch (ValidationStatisticsException e) {
						logError("Error copying previous validation results from: " + previousSnapshotId + " to: " + snapshotId + " :: " + e.getMessage());
						this.stop();
					}

				}

				// set the paginator to the untested records, this means that already valid and invalid records will not be revalidated
				this.setPaginator(metadataStoreService.getUntestedRecordsPaginator(snapshotId));

			} else { /// si es full validation entonces trabajo sobre los no deleted

			   // delete previous validation results if any
				try {
					validationStatisticsService.deleteValidationStatsBySnapshotID(snapshotId);
				} catch (ValidationStatisticsException e) {
					logError("Error deleting previous validation results: " + e.getMessage());
					this.stop();
				}

				// set the paginator to the not deleted records, this means that all records will be revalidated
				this.setPaginator(metadataStoreService.getNotDeletedRecordsPaginator(snapshotId));
			}


		
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
				
				if (! isIncremental() ) { // si es validacion full hace un reset de los indicadores
					metadataStoreService.resetSnapshotValidationCounts(snapshotId);
				}
				
	
			} catch (ValidationException | MetadataRecordStoreException e) {
				logError(runningContext.toString() + ": " + e.getMessage());
				this.stop();
				return;
			}
			
			
			
			
			logger.debug("Starting - total pages: " + this.getTotalPages() );
			
		
		} else {
			logger.error("There is not a suitable snapshot for validation");
			this.setPaginator( null );
			this.stop();
		}
		
	}


	private void copyAndUpdatePreviousValidationResults(Long previousSnapshotId, Long snapshotId) throws ValidationStatisticsException {
		// copia los resultados de validación del snapshot anterior
		validationStatisticsService.copyValidationStatsObservationsFromTo(previousSnapshotId, snapshotId);

		// obtain paginator for deleted records
		IPaginator<OAIRecord> paginator = metadataStoreService.getDeletedRecordsPaginator(snapshotId);

		// if paginator is not null
		if (paginator != null) {

			int totalPages = paginator.getTotalPages();

			for (int actualPage = paginator.getStartingPage(); actualPage <= totalPages; actualPage++) {

				try {

					Page<OAIRecord> page = paginator.nextPage();

					// use record ids from page and call validation services delete
					page.getContent().stream().forEach(record -> {
						try {
							validationStatisticsService.deleteValidationStatsObservationByRecordIDAndSnapshotID(snapshotId, record.getId());
							logger.debug("Deleting validation results for record: " + record.getId() + " :: Snapshot: " + snapshotId);
						} catch (ValidationStatisticsException e) {
							throw new RuntimeException(e);
						}
					});

				} catch (Exception e) {
					throw new ValidationStatisticsException("Error cleaning deleted record validation" + e.getMessage(), e);
				}
			}

		} else {
		    logError("No deleted records paginator was found for snapshot: " + snapshotId);
			this.stop();
		}
	}
	
	@Override
	public void prePage() {
		
		validationStatsObservations.clear();
	}
	
	@Override
	public void processItem(OAIRecord record) {
		
		
		try {
		    // reset validation result
            reusableValidationResult.reset();

		
			// if is full revalidation set record as untested and not transformed
			if ( ! isIncremental() ) {
				record.setStatus(RecordStatus.UNTESTED);
				record.setTransformed(false);
			}
			
			logger.debug( "Validating record: " + record.getId() + " :: "+ record.getIdentifier() );
			logger.debug("Initial status: "  + record.getId() + " :: "+ record.getIdentifier() + "::" + record.getStatus() );
			wasTransformed = false;
			
			// carga la metadata original sin transformar
			logger.debug("Load metadata: "  + record.getId() + " :: "+ record.getIdentifier() );
			OAIRecordMetadata metadata = metadataStoreService.getOriginalMetadata(record);

			
			// si corresponde lo transforma

			logger.debug("Starting transformations: " + record.getId() + " :: "+ record.getIdentifier() );
			
			// transforma
			
			if ( transformer != null ) {
				logger.debug("Primary transformer: "  + record.getId() + " :: "+ record.getIdentifier() );
				wasTransformed |= transformer.transform(record, metadata);
			}
			
			if ( secondaryTransformer != null ) {
				logger.debug("Secondary transformer: "  + record.getId() + " :: "+ record.getIdentifier() );
				wasTransformed |= secondaryTransformer.transform(record, metadata);
			}
				
			logger.debug( record.getId() + " :: "+ record.getIdentifier() + "  Transformed: " + wasTransformed);


			
			
			// if validator is defined
			if ( validator != null ) {

				logger.debug("Validating: "  + record.getId() + " :: "+ record.getIdentifier() );
				
				// validación
				reusableValidationResult = validator.validate(metadata, reusableValidationResult);
				
				// update record validation status
				metadataStoreService.updateRecordStatus(record, reusableValidationResult.isValid() ? RecordStatus.VALID : RecordStatus.INVALID, wasTransformed);
					
				
			}
			else { // if no validator is set, then record is consired valid and the validation results are set to true
				
			    reusableValidationResult.setValid(true);

	            // update record validation status
				metadataStoreService.updateRecordStatus(record, RecordStatus.VALID, wasTransformed);

			}
			
			// Se almacenan las estadísticas de cosecha
            logger.debug("Storing diagnose "  + record.getId() + " :: "+ record.getIdentifier() );
            validationStatsObservations.add( validationStatisticsService.buildObservation(record, reusableValidationResult) );

			logger.debug( record.getId() + " :: "+ record.getIdentifier() + " final status: " + record.getStatus() );

			
			metadataStoreService.updatePublishedMetadata(record, metadata);

			
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
		
	}


	@Override
	public void postPage() {
		
		if ( validationStatisticsService.isServiceAvaliable() ) {
			validationStatisticsService.registerObservations(validationStatsObservations);
		} 
		
		metadataStoreService.saveSnapshot(snapshotId);

		
	}

	@Override
	public void postRun() {
		setSnapshotStatus(SnapshotStatus.VALID);
		logInfo("Finishing Validation/Transformation of " + runningContext.toString() );

	}

	@Override
	public String toString() {
		return "Transform/Validate(" + percentajeFormat.format(this.getCompletionRate()) + ")" + (isIncremental() ? "(I)" : "(F)") ;
	}
	
	private void setSnapshotStatus(SnapshotStatus status) {
		// metodo auxiliar para centralizar la forma de actualizar status
		metadataStoreService.updateSnapshotStatus(snapshotId, status);
		metadataStoreService.saveSnapshot(snapshotId);

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
