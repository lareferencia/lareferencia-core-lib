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

package org.lareferencia.core.harvester.workers;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.backend.domain.Network;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.backend.domain.SnapshotStatus;
import org.lareferencia.backend.domain.Validator;
import org.lareferencia.backend.services.SnapshotLogService;
import org.lareferencia.backend.services.ValidationService;
import org.lareferencia.core.harvester.HarvestingEvent;
import org.lareferencia.core.harvester.IHarvester;
import org.lareferencia.core.harvester.IHarvestingEventListener;
import org.lareferencia.core.metadata.IMetadataRecordStoreService;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.RecordStatus;
import org.lareferencia.core.util.date.DateHelper;
import org.lareferencia.core.validation.IValidator;
import org.lareferencia.core.validation.ValidationException;
import org.lareferencia.core.validation.ValidatorResult;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.IWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import lombok.Getter;
import lombok.Setter;


public class HarvestingWorker extends BaseWorker<NetworkRunningContext> implements IWorker<NetworkRunningContext>, IHarvestingEventListener {

	private static final String DEFAULT_GRANDULARITY = "YYYY-MM-DDThh:mm:ssZ";

	private static Logger logger = LogManager.getLogger(HarvestingWorker.class);
	
	@Getter
	@Setter
	private boolean fetchIdentifyParameters = false;
	
	@Autowired
	private PlatformTransactionManager transactionManager;
	
	@Autowired
	private ValidationService validationManager;
	
	private IValidator validator;

	
	public HarvestingWorker()  {
		super();
		setIncremental(false);
	}

	@Override
	public String toString() {
		return "Harvesting";
	}

	@Value("${harvester.max.retries}")
	private int MAX_RETRIES;

	@Autowired 
	private IMetadataRecordStoreService metadataStoreService;

	@Autowired
	private SnapshotLogService snapshotLogService;


	private IHarvester harvester;

	@Autowired
	public void setHarvester(IHarvester harvester) {
		this.harvester = harvester;

		// los eventos de harvesting serán manejados por el worker
		harvester.addEventListener(this);
	}
	
	private Long snapshotId;
	ValidatorResult validatorResult;


	private String from = null;


	public HarvestingWorker(Network network) {
		super( new NetworkRunningContext(network) );
	};
	
	
//	public NetworkSnapshot getSnapshot() {
//		return snapshotId;
//	}

	@Override
	public void stop() {

		logInfoMessage(runningContext.toString() + " Stop signal received - Harvesting is stopping");
		harvester.stop();
		
	
		metadataStoreService.updateSnapshotStatus(this.snapshotId, SnapshotStatus.HARVESTING_FINISHED_VALID);
		metadataStoreService.updateSnapshotEndDatestamp(snapshotId, LocalDateTime.now());
		metadataStoreService.saveSnapshot(snapshotId);
		
		
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
		
		logInfoMessage(runningContext.toString() + " Stopping worker");
		super.stop();		
	}

	/*
	 * 
	 * @throws Exception
	 */
	@Override
	public void run() {

		// create identify map for contain the information of identify request
		Map<String, String> identifyMap = null;

		// date granularity
		String granularity = DEFAULT_GRANDULARITY;

		// check if the url is valid by trying to connect to it with identify verb
		String originURL = runningContext.getNetwork().getOriginURL();


		// if fetch identify is true, will try to fetch the identify information
		//if ( runningContext.getNetwork().getBooleanPropertyValue("HARVEST_IDENTIFY_PARAMETERS") ) {
		if ( this.fetchIdentifyParameters ) {
			logger.debug("FETCH_IDENTIFY_PARAMETERS is true, fetching identify parameters");
			identifyMap =  harvester.identify(originURL);
			if ( identifyMap != null && identifyMap.containsKey("granularity") && identifyMap.get("granularity") != null
					&& !identifyMap.get("granularity").isEmpty() ) {

				logInfoMessage("Identify Granularity found: " + identifyMap.get("granularity"));
				granularity = identifyMap.get("granularity");
			} else {
				logInfoMessage("Identify Granularity not found, using default granularity: " + granularity);
			}
		} else { // if fetch identify is false, will use the default granularity
			logInfoMessage("Using default granularity: " + granularity);
		}


		LocalDateTime startDateStamp = LocalDateTime.now();
		validatorResult = new ValidatorResult();

		if (runningContext.getNetwork() != null) {
			
			Validator validatorModel = runningContext.getNetwork().getPrevalidator();
				
		    if ( validatorModel  != null ) {
		    	
				try {
					validator = validationManager.createValidatorFromModel( validatorModel );
					logInfoMessage(runningContext.toString() + " Prevalidator found and loaded: " + validatorModel.getName() + " - Harvested records will be filtered, and only valid ones will be counted and stored");
					
				} catch (ValidationException e) {
					logErrorMessage(runningContext.toString() + " Prevalidator found - error loading: " + validatorModel.getName());
				}
					
		    } 
		    
			// caso de harvesting incremental
			
			// fuerza el tratamiento full de la cosecha
			if ( runningContext.getNetwork().getBooleanPropertyValue("FORCE_FULL_HARVESTING") ) {
				logger.debug("Forcing full harvesting");
				this.setIncremental(false);			
			}

			logger.debug("Is incremental harvesting?: " + this.isIncremental() );
	
			if ( isIncremental() ) {
				
				// trata de conseguir la ultima cosecha válida , puede ser status 4 o 9 (harvested o valid)
				//snapshot = snapshotRepository.findLastHarvestedByNetworkID( runningContext.getNetwork().getId() );
				snapshotId = metadataStoreService.findLastHarvestingSnapshot( runningContext.getNetwork() );
					
				if ( snapshotId == null ) {
					logInfoMessage(runningContext.toString() + " There isn't any previous snapshot. Launching full harvesting instead ...");
				} 
				else { // caso cosecha full anterior
					
					LocalDateTime startDatestamp = metadataStoreService.getSnapshotStartDatestamp(snapshotId);
									
					from =  DateHelper.getDateTimeFormattedStringFromGranularity(startDatestamp,granularity); //DateHelper.getDateTimeMachineString(startDatestamp);

					metadataStoreService.updateSnapshotLastIncrementalDatestamp(snapshotId, startDatestamp);
									
					logInfoMessage(runningContext.toString() + " Starting incremental harvesting from date: " + from + " -- Snapshot: " + snapshotId);
				}			
			} 
			
			// if no snapshot then create a new one, full harvesting case
			if ( snapshotId == null ) {
				snapshotId = metadataStoreService.createSnapshot( runningContext.getNetwork() );
				metadataStoreService.updateSnapshotStartDatestamp(snapshotId, startDateStamp);						
			}
			
			
		} else {
			logErrorMessage("Network does't exists("+ runningContext.toString()+"), cannot continue. Breaking ...");
			return;
		}


		logInfoMessage("Harvesting is starting for "+ runningContext.toString()+"...");
				
		metadataStoreService.updateSnapshotStatus(this.snapshotId, SnapshotStatus.HARVESTING);
		metadataStoreService.saveSnapshot(snapshotId);

		
		// inicialización del harvester
		harvester.reset();
		harvestEntireNetwork();
		
		// Luego del harvesting el snapshot puede presentar estados diversos

		// si no fue detenido
		//if (snapshot.getStatus() != SnapshotStatus.HARVESTING_STOPPED) {
		
		
		if ( metadataStoreService.getSnapshotSize(snapshotId) < 1 ) {
			logErrorMessage( runningContext.toString() + " :: No records found !!");
			metadataStoreService.updateSnapshotStatus(this.snapshotId, SnapshotStatus.HARVESTING_FINISHED_ERROR);
		}
		
		// Si no generó errores, entonces terminó exitosa la cosecha
		if (metadataStoreService.getSnapshotStatus(snapshotId) != SnapshotStatus.HARVESTING_FINISHED_ERROR) {

			logInfoMessage( runningContext.toString() + " :: Harvesting ended succesfully.");

			metadataStoreService.updateSnapshotStatus(this.snapshotId, SnapshotStatus.HARVESTING_FINISHED_VALID);
			metadataStoreService.updateSnapshotStartDatestamp(snapshotId, startDateStamp);						
			metadataStoreService.updateSnapshotEndDatestamp(snapshotId, LocalDateTime.now());	
			metadataStoreService.saveSnapshot(snapshotId);



		} else {
			logErrorMessage (runningContext.toString() + " :: Harvesting ended with errors !!!");
		}
		//}

	}
	
	private Boolean metadataPassPrevalidation(OAIRecordMetadata metadata) throws ValidationException {
		
		if ( validator == null )
			return true;
		else {
			validatorResult = validator.validate(metadata, validatorResult);
			return validatorResult.isValid();
		}
		
	}


	/*************************************************************/

	private void harvestEntireNetwork() {
		// Ciclo principal de procesamiento, dado por la estructura de la red
		// nacional
		// Se recorren los orígenes

		String originURL = runningContext.getNetwork().getOriginURL();
		String metadataPrefix = runningContext.getNetwork().getMetadataPrefix();
		String metadataStoreSchema = runningContext.getNetwork().getMetadataStoreSchema();
		
		List<String> sets = runningContext.getNetwork().getSets();
		
		if ( originURL != null ) {
		
			// si tiene sets declarados solo va a cosechar
			if (sets != null && sets.size() > 0) {

				logInfoMessage("There are defined sets. Harvesting configured sets for "+ runningContext.toString());

				for (String set : sets) {

					logInfoMessage("Harvesting set: " + set + " for "+ runningContext.toString());

					harvester.harvest(originURL, set, metadataPrefix, metadataStoreSchema, from, null, null, MAX_RETRIES);
				}
			}
			// si no hay set declarado cosecha todo
			else {

				logInfoMessage("There aren't defined sets. Harvesting the entire collection for " + runningContext.toString());
				harvester.harvest(originURL, null, metadataPrefix, metadataStoreSchema, from, null, null, MAX_RETRIES);
			}
		}
	}

//	private void harvestEntireNetworkBySet() {
//		// Ciclo principal de procesamiento, dado por la estructura de la red
//		// nacional
//		// Se recorren los orígenes
//		for (OAIOrigin origin : runningContext.getNetwork().getOrigins()) {
//
//			List<String> setList = harvester.listSets(origin.getUri());
//
//			for (String setName : setList) {
//				logger.info("Harvesting: " + origin.getName() + " set: " +setName);
//				OAISet set = new OAISet();
//				set.setSpec(setName);
//				set.setName(setName);
//				harvester.harvest(origin, set, from, null, null, MAX_RETRIES);
//			}
//		}
//
//	}

	@Override
	public void harvestingEventOccurred(HarvestingEvent event) {
		
		DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
		transactionDefinition.setIsolationLevel(TransactionDefinition.ISOLATION_DEFAULT);
		
		TransactionStatus transactionStatus = transactionManager.getTransaction(transactionDefinition);


		logger.debug(runningContext.getNetwork().getName() + "  HarvestingEvent received: " + event.getStatus());

		switch (event.getStatus()) {

		case OK:
			
			
			// Registra si hubo errores en esta pagina
			if ( event.isRecordMissing() ) {
				logErrorMessage("Some record metadata is missing RT:" + event.getResumptionToken() + " identifiers: " + event.getMissingRecordsIdentifiers() );
			}
			
			// marca como borrados los identifiers reportados 
			if ( isIncremental()) {
				for ( String deletedRecordIdentifier : event.getDeletedRecordsIdentifiers() ) {
					
					OAIRecord record = metadataStoreService.findRecordByIdentifier(snapshotId, deletedRecordIdentifier); 
					
					// Si se encuentra se modifica el status del registro
					if (record != null) {
						logger.debug("Marking record as deleted: " + record.getId() + " : " + record.getIdentifier() );
						metadataStoreService.updateRecordStatus(record, RecordStatus.DELETED, null);
					}
				}
				
				// Se registra la lista de registros borrados
				if ( !event.getDeletedRecordsIdentifiers().isEmpty() )
					logInfoMessage( "Marked as deleted:" + event.getDeletedRecordsIdentifiers() + " for " + runningContext.toString() );
			
			}
			
			
			// Agrega los records al snapshot actual
			for (OAIRecordMetadata metadata : event.getRecords()) {

				try {
					
					OAIRecord record = null;
					Boolean isMetadataValidAccordingPrevalidator = metadataPassPrevalidation(metadata);
					
					// Si es cosecha incremental
					if ( isIncremental() ) {
						// busca si ya existe el registro
						//record = recordRepository.findOneBySnapshotIdAndIdentifier(snapshot.getId(), metadata.getIdentifier() );
						record = metadataStoreService.findRecordByIdentifier(snapshotId, metadata.getIdentifier() );

						// si existe lo actualiza o lo borra si no pasa el validador 
						if ( record != null  ) {
							logger.debug( "Updating record: " + record.getId() + " : " + record.getIdentifier() );
							
							// if the existing record no longer result valid in pre-valitatin must be marked as deleted, if is valid must be updated
							if ( isMetadataValidAccordingPrevalidator )
								metadataStoreService.updateOriginalMetadata(record, metadata);
							else {
								metadataStoreService.updateRecordStatus(record, RecordStatus.DELETED, null);
							}
						}
					} 
					
					// si no existe o no es incremental
					if ( record == null ) {
						// if metadata is valid according to prevalidator the store the records
						if ( isMetadataValidAccordingPrevalidator )						
							record = metadataStoreService.createRecord(snapshotId, metadata); 
					}
					
				} catch (Exception e) { //TODO: specialize this exception
					logErrorMessage("Metadata store error :: " + e.getMessage());
				}
			}
			
		
			// FIXME: Esto evita el problema con la restricción de resumption
			// token mayor a 255
			String resumptionToken = event.getResumptionToken();
			if (resumptionToken != null && resumptionToken.length() > 255)
				resumptionToken = resumptionToken.substring(0, 255);

			//snapshotId.setResumptionToken(resumptionToken);

			// Graba el status
			metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.HARVESTING);
			metadataStoreService.saveSnapshot(snapshotId);

			//setSnapshotStatus();

			break;
			
		case NO_MATCHING_QUERY:
			
			if ( isIncremental() ) {
				logInfoMessage("No new records found. Finishing incremental harvesting for "+ runningContext.toString());
				metadataStoreService.updateSnapshotStatus(this.snapshotId, SnapshotStatus.HARVESTING_FINISHED_VALID);
			}
			else {
				if ( metadataStoreService.getSnapshotSize(snapshotId) > 0 )
					metadataStoreService.updateSnapshotStatus(this.snapshotId, SnapshotStatus.HARVESTING_FINISHED_VALID);
				else
					metadataStoreService.updateSnapshotStatus(this.snapshotId, SnapshotStatus.HARVESTING_FINISHED_ERROR);

				logInfoMessage("No records found!!! at " + runningContext.toString());
			}
			
			break;

		case ERROR_RETRY:

			logErrorMessage("Retry:" + event.getMessage());
			metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.RETRYING);
			break;

		case ERROR_FATAL:

			logErrorMessage("Fatal Error:" + event.getMessage());
			metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.HARVESTING_FINISHED_ERROR);
			break;

		case STOP_SIGNAL_RECEIVED:

			logErrorMessage("Stop signal received:" + event.getMessage());
			if ( metadataStoreService.getSnapshotSize(snapshotId) > 0 )
				metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.HARVESTING_FINISHED_VALID);
			else
				metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.HARVESTING_FINISHED_ERROR);
			break;

		default:
			/**
			 * TODO: Definir que se hace en caso de eventos sin status conocido
			 */
			break;
		}
		
		metadataStoreService.updateSnapshotEndDatestamp(snapshotId, LocalDateTime.now());	
		metadataStoreService.saveSnapshot(snapshotId);
		transactionManager.commit(transactionStatus);

	}
	
	private void logErrorMessage(String message) {
		
		logger.error(message);
		snapshotLogService.addEntry(snapshotId, "ERROR: " + message);		

	}
	
	private void logInfoMessage(String message) {
		
		logger.info(message);
		snapshotLogService.addEntry(snapshotId, "INFO: " + message);		

	}



}
