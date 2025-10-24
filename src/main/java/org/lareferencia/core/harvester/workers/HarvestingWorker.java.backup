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
import org.lareferencia.core.metadata.MetadataRecordStoreException;
import org.lareferencia.core.metadata.OAIRecordMetadata;
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

	// this will hold the previous snapshot id when the harvesting is incremental
	private Long previousSnapshotId;


	ValidatorResult validatorResult;

	private String from = null;

	private Boolean bySetHarvesting = false;
	private  String currentSetSpec = null;


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

		// create a validator result for reusing the object in the loop
		validatorResult = new ValidatorResult();

		// start datestamp
		// LocalDateTime startDatestamp = null;
		LocalDateTime startDateStamp = LocalDateTime.now();
		LocalDateTime previusSnapshotStartDatestamp = null;

		// check if the url is valid by trying to connect to it with identify verb
		String originURL = runningContext.getNetwork().getOriginURL();


		// create the snapshot
		snapshotId = metadataStoreService.createSnapshot( runningContext.getNetwork() );

		// set the snapshot start datetime
		metadataStoreService.updateSnapshotStartDatestamp(snapshotId, startDateStamp);

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
			
			// forces full harvesting
			if ( runningContext.getNetwork().getBooleanPropertyValue("FORCE_FULL_HARVESTING") ) {
				logger.debug("Forcing full harvesting");
				this.setIncremental(false);			
			}

			logger.debug("Is incremental harvesting?: " + this.isIncremental() );
	
			if ( isIncremental() ) {
				
				// get the last good known snapshot if exists (the last one that finished with status VALID)
				previousSnapshotId = metadataStoreService.findLastGoodKnownSnapshot( runningContext.getNetwork() );
					
				if ( previousSnapshotId == null ) {
					logInfoMessage(runningContext.toString() + " There isn't any previous snapshot. Launching full harvesting instead ...");
					this.setIncremental(false);
				} 
				else { 	// if there is a previous snapshot, get the last incremental datestamp

					previusSnapshotStartDatestamp = metadataStoreService.getSnapshotStartDatestamp(previousSnapshotId);

					// set the from date for the harvester
					from =  DateHelper.getDateTimeFormattedStringFromGranularity(previusSnapshotStartDatestamp,granularity); //DateHelper.getDateTimeMachineString(startDatestamp);

					logInfoMessage(runningContext.toString() + " Setting up incremental harvesting from date: " + from + " based on snapshot: " + previousSnapshotId);
				}			
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
		runOAIPMHHarvesting();
		
		// when the harvesting is finished, check if there are errors

		// if the size of the snapshot is 0 and is not incremental, then there are no records harvested and the status should be HARVESTING_FINISHED_ERROR
		if ( metadataStoreService.getSnapshotSize(snapshotId) < 1 && ! isIncremental() ) {
			logErrorMessage( runningContext.toString() + " :: No records found !!");
			metadataStoreService.updateSnapshotStatus(this.snapshotId, SnapshotStatus.HARVESTING_FINISHED_ERROR);
		}
		
		// if the snapshot status is not HARVESTING_FINISHED_ERROR, then the harvesting finished successfully
		if (metadataStoreService.getSnapshotStatus(snapshotId) != SnapshotStatus.HARVESTING_FINISHED_ERROR) {

			if ( isIncremental() && previousSnapshotId != null  ) {
				// copy the previous snapshot records to the current snapshot avoiding duplicates and deleted records
				logInfoMessage(runningContext.toString() + " :: getting records from previous snapshot: " + previousSnapshotId);
				metadataStoreService.updateSnapshotLastIncrementalDatestamp(snapshotId, previusSnapshotStartDatestamp);
				metadataStoreService.setPreviousSnapshotId(snapshotId, previousSnapshotId);
				metadataStoreService.copyNotDeletedRecordsFromSnapshot(previousSnapshotId, snapshotId);
			}

			logInfoMessage( runningContext.toString() + " :: Harvesting ended successfully.");

			metadataStoreService.updateSnapshotStatus(this.snapshotId, SnapshotStatus.HARVESTING_FINISHED_VALID);
			metadataStoreService.updateSnapshotStartDatestamp(snapshotId, startDateStamp);						
			metadataStoreService.updateSnapshotEndDatestamp(snapshotId, LocalDateTime.now());	
			metadataStoreService.saveSnapshot(snapshotId);

		} else {
			logErrorMessage (runningContext.toString() + " :: Harvesting ended with errors !!!");
		}

	} // end of run method
	
	private Boolean metadataPassPrevalidation(OAIRecordMetadata metadata) throws ValidationException {
		
		if ( validator == null )
			return true;
		else {
			validatorResult = validator.validate(metadata, validatorResult);
			return validatorResult.isValid();
		}
		
	}


	/*************************************************************/

	private void runOAIPMHHarvesting() {

		String originURL = runningContext.getNetwork().getOriginURL();
		String metadataPrefix = runningContext.getNetwork().getMetadataPrefix();
		String metadataStoreSchema = runningContext.getNetwork().getMetadataStoreSchema();

		//String until = "2022-01-01";
		String until = null;

		List<String> sets = runningContext.getNetwork().getSets();
		
		if ( originURL != null ) {
		
			// si tiene sets declarados solo va a cosechar
			if (sets != null && sets.size() > 0) {

				bySetHarvesting = true;

				logInfoMessage("There are defined sets. Harvesting configured sets for "+ runningContext.toString());
				logInfoMessage("Please note that sets may not be disjoint, so the same record may be harvested more than once");

				for (String set : sets) {
					logInfoMessage("Harvesting set: " + set + " for "+ runningContext.toString());
					currentSetSpec = set;
					harvester.harvest(originURL, set, metadataPrefix, metadataStoreSchema, from, until,  null, MAX_RETRIES);
				}
			}
			// si no hay set declarado cosecha todo
			else {

				logInfoMessage("There aren't defined sets. Harvesting the entire collection for " + runningContext.toString());
				harvester.harvest(originURL, null, metadataPrefix, metadataStoreSchema, from, until, null, MAX_RETRIES);
			}
		}
	}


	@Override
	public void harvestingEventOccurred(HarvestingEvent event) {
		
		DefaultTransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
		transactionDefinition.setIsolationLevel(TransactionDefinition.ISOLATION_DEFAULT);
		
		TransactionStatus transactionStatus = transactionManager.getTransaction(transactionDefinition);

		logger.debug(runningContext.getNetwork().getName() + "  HarvestingEvent received: " + event.getStatus());

		switch (event.getStatus()) {

		case OK:

			// if some record is missing, log the error
			if ( event.isRecordMissing() ) {
				logErrorMessage("Some record metadata is missing RT:" + event.getResumptionToken() + " identifiers: " + event.getMissingRecordsIdentifiers() );
			}
			
			// if is an incremental harvesting, then get the deleted records
			if ( isIncremental() ) {

				try {

					OAIRecord record = null;

					// process the deleted records
					for (String deletedRecordIdentifier : event.getDeletedRecordsIdentifiers())
						record = metadataStoreService.createDeletedRecord(snapshotId, deletedRecordIdentifier, LocalDateTime.now());

					// log the deleted records
					if (!event.getDeletedRecordsIdentifiers().isEmpty())
						logger.debug("Stored as deleted:" + event.getDeletedRecordsIdentifiers() + " for " + runningContext.toString());

				} catch (MetadataRecordStoreException e) {
					logErrorMessage("Error storing deleted records for " + runningContext.toString() + " : " + e.getMessage());
				} catch (Exception e) {
					logErrorMessage("Unknown error storing deleted records for " + runningContext.toString() + " : " + e.getMessage());
				}

			}
			
			
			// add non delted records to the snapshot
			for (OAIRecordMetadata metadata : event.getRecords()) {

				try {

					OAIRecord record = null;

					// if the metadata pass the prevalidation, then store it
					if (metadataPassPrevalidation(metadata))
						record = metadataStoreService.createRecord(snapshotId, metadata);

				} catch (MetadataRecordStoreException e) {
					logErrorMessage("Error storing record " + metadata.getIdentifier() + " for " + runningContext.toString() + " : " + e.getMessage());
				} catch (ValidationException e) {
					logErrorMessage("Error prevalidating record " + metadata.getIdentifier() + " for " + runningContext.toString() + " : " + e.getMessage());
				} catch (Exception e) {
					logErrorMessage("Unknown record store error :: " + e.getMessage());
				}
			}
			
		
			// FIXME: this is a workaround for the resumption token length problem
			String resumptionToken = event.getResumptionToken();
			if (resumptionToken != null && resumptionToken.length() > 255)
				resumptionToken = resumptionToken.substring(0, 255);

			//snapshotId.setResumptionToken(resumptionToken);

			// stores the status of the snapshot
			metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.HARVESTING);
			metadataStoreService.saveSnapshot(snapshotId);

			break;
			
		case NO_MATCHING_QUERY:

			// if NO_MATCHING_QUERY is received, then
			if ( ! bySetHarvesting ) { // if is not by set harvesting, then the harvesting is finished with error

				if ( isIncremental() ) { // if is incremental, then the harvesting is marked as valid
					metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.HARVESTING_FINISHED_VALID);
					logInfoMessage("No records found in incremental harvesting" + runningContext.toString());

					//metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.EMPTY_INCREMENTAL);
				} else { // if is not incremental, then the harvesting is finished with error
					metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.HARVESTING_FINISHED_ERROR);
					logInfoMessage("No records found!!! at " + runningContext.toString());

				}

			}
			else { // if is by set harvesting, then the harvesting can be susscessful even when no records are found in this set
				metadataStoreService.updateSnapshotStatus(snapshotId, SnapshotStatus.HARVESTING_FINISHED_VALID);
				logInfoMessage("No records found for the set: " + currentSetSpec + " at " + runningContext.toString());
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
			// TODO: decide what to do in this case
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
