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

package org.lareferencia.core.worker.harvesting;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.SnapshotStatus;
import org.lareferencia.core.domain.Validator;
import org.lareferencia.core.repository.catalog.OAIRecord;
import org.lareferencia.core.repository.catalog.OAIRecordCatalogRepository;

import org.lareferencia.core.service.management.SnapshotLogService;
import org.lareferencia.core.service.validation.ValidationService;
import org.lareferencia.core.metadata.IMetadataStore;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.date.DateHelper;
import org.lareferencia.core.worker.validation.IValidator;
import org.lareferencia.core.worker.validation.ValidationException;
import org.lareferencia.core.worker.validation.ValidatorResult;
import org.lareferencia.core.worker.BaseWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

@Component("harvestingWorkerFlowable")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
/**
 * Worker de harvesting que usa almacenamiento SQLite para catálogo.
 * 
 * ARQUITECTURA:
 * - ISnapshotStore: Gestiona metadata de snapshots (SQL)
 * - OAIRecordCatalogRepository: Gestiona records en SQLite (catálogo por
 * snapshot)
 * - IMetadataStore: Gestiona XML de metadata (filesystem)
 * 
 * FLUJO:
 * 1. Crear snapshot (ISnapshotStore)
 * 2. Inicializar catálogo SQLite (copia del anterior si incremental)
 * 3. Por cada record harvested:
 * - Guardar XML en IMetadataStore
 * - Upsert OAIRecord en SQLite
 * - Incrementar contador en ISnapshotStore
 * 4. Cerrar catálogo SQLite
 * 5. Actualizar estado de snapshot
 * 
 * @author LA Referencia Team
 */
public class HarvestingWorker extends BaseWorker<NetworkRunningContext>
		implements IHarvestingEventListener {

	private static final String DEFAULT_GRANDULARITY = "YYYY-MM-DDThh:mm:ssZ";

	private static Logger logger = LogManager.getLogger(HarvestingWorker.class);

	@Getter
	@Setter
	private boolean fetchIdentifyParameters = false;

	@Autowired
	private ValidationService validationManager;

	@Autowired
	private ISnapshotStore snapshotStore;

	@Autowired
	private IMetadataStore metadataStore;

	@Autowired
	private OAIRecordCatalogRepository catalogRepository;

	@Autowired
	private SnapshotLogService snapshotLogService;

	/**
	 * Validator para records harvested.
	 */
	private IValidator validator;

	/**
	 * Crea un nuevo harvesting worker SQLite.
	 */
	public HarvestingWorker() {
		super();
		setIncremental(false);
	}

	@Override
	public String toString() {
		return "Harvesting (SQLite)";
	}

	@Value("${harvester.max.retries}")
	private int MAX_RETRIES;

	/**
	 * Instancia de harvester para fetch de records.
	 */
	private IHarvester harvester;

	/**
	 * Configura el harvester y registra este worker como event listener.
	 * 
	 * @param harvester el harvester a usar
	 */
	@Autowired
	public void setHarvester(IHarvester harvester) {
		this.harvester = harvester;
		// Los eventos de harvesting serán manejados por el worker
		harvester.addEventListener(this);
	}

	private Long snapshotId;
	private SnapshotMetadata snapshotMetadata;

	// ID del snapshot anterior (para harvesting incremental)
	private Long previousSnapshotId;

	ValidatorResult validatorResult;

	private String from = null;

	private Boolean bySetHarvesting = false;
	private String currentSetSpec = null;

	/**
	 * Crea un harvesting worker para una red específica.
	 * 
	 * @param network la red a cosechar
	 */
	public HarvestingWorker(Network network) {
		super(new NetworkRunningContext(network));
	}

	/**
	 * Detiene el proceso de harvesting.
	 * Solo señala la parada - el cleanup se hace en run() cuando termina.
	 */
	@Override
	public void stop() {
		logInfoMessage(runningContext.toString() + " Stop signal received - Harvesting is stopping");
		// Signal the harvester to stop - it will check this flag and terminate
		harvester.stop();
		// Signal the worker to stop - run() will detect this and cleanup properly
		super.stop();
	}

	/**
	 * Gets the current harvesting status.
	 * 
	 * @return status string with harvested record count
	 */
	@Override
	public String getStatus() {
		if (snapshotId != null && snapshotStore != null) {
			try {
				long recordCount = snapshotStore.getSnapshotSize(snapshotId);
				return "Harvested " + recordCount + " records";
			} catch (Exception e) {
				return "Harvesting...";
			}
		}
		return "Initializing...";
	}

	/**
	 * Ejecuta el proceso de harvesting.
	 */
	@Override
	public void run() {

		// Mapa de identify para contener información del request identify
		Map<String, String> identifyMap = null;

		// Granularidad de fecha
		String granularity = DEFAULT_GRANDULARITY;

		// Crear validator result para reutilizar el objeto en el loop
		validatorResult = new ValidatorResult();

		// Timestamps
		LocalDateTime previousSnapshotStartDatestamp = null;

		// URL de origen
		String originURL = runningContext.getNetwork().getOriginURL();

		// Crear el snapshot
		snapshotId = snapshotStore.createSnapshot(runningContext.getNetwork());
		snapshotMetadata = snapshotStore.getSnapshotMetadata(snapshotId);
		logInfoMessage("SNAPSHOT CREATED: id=" + snapshotId + " for network " + runningContext.toString());

		// El timestamp de inicio se establece en startHarvesting() automáticamente

		// Inicializar catálogo SQLite para escritura
		// Nota: previousSnapshotId se determina más adelante si es incremental
		// La inicialización real se hace después de determinar el modo

		// Fetch identify parameters si está habilitado
		if (this.fetchIdentifyParameters) {
			logger.debug("FETCH_IDENTIFY_PARAMETERS is true, fetching identify parameters");
			identifyMap = harvester.identify(originURL);
			if (identifyMap != null && identifyMap.containsKey("granularity") &&
					identifyMap.get("granularity") != null && !identifyMap.get("granularity").isEmpty()) {
				logInfoMessage("Identify Granularity found: " + identifyMap.get("granularity"));
				granularity = identifyMap.get("granularity");
			} else {
				logInfoMessage("Identify Granularity not found, using default granularity: " + granularity);
			}
		} else {
			logInfoMessage("Using default granularity: " + granularity);
		}

		if (runningContext.getNetwork() != null) {

			Validator validatorModel = runningContext.getNetwork().getPrevalidator();

			if (validatorModel != null) {

				try {
					validator = validationManager.createValidatorFromModel(validatorModel);
					logInfoMessage(runningContext.toString() + " Prevalidator found and loaded: " +
							validatorModel.getName() + " - Harvested records will be filtered");

				} catch (ValidationException e) {
					logErrorMessage(runningContext.toString() + " Prevalidator found - error loading: " +
							validatorModel.getName());
				}

			}

			// Caso de harvesting incremental

			// Forzar full harvesting si está configurado
			if (runningContext.getNetwork().getBooleanPropertyValue("FORCE_FULL_HARVESTING")) {
				logger.debug("Forcing full harvesting");
				this.setIncremental(false);
			}

			logger.debug("Is incremental harvesting?: " + this.isIncremental());

			if (isIncremental()) {

				// Obtener el último snapshot válido
				previousSnapshotId = snapshotStore.findLastGoodKnownSnapshot(runningContext.getNetwork());

				if (previousSnapshotId == null) {
					logInfoMessage(runningContext.toString() +
							" There isn't any previous snapshot. Launching full harvesting instead ...");
					this.setIncremental(false);
				} else {
					previousSnapshotStartDatestamp = snapshotStore.getSnapshotStartDatestamp(previousSnapshotId);

					// Configurar fecha from para el harvester
					from = DateHelper.getDateTimeFormattedStringFromGranularity(
							previousSnapshotStartDatestamp, granularity);

					logInfoMessage(runningContext.toString() +
							" Setting up incremental harvesting from date: " + from +
							" based on snapshot: " + previousSnapshotId);
				}
			}

		} else {
			logErrorMessage("Network doesn't exists(" + runningContext.toString() +
					"), cannot continue. Breaking ...");
			return;
		}

		logInfoMessage("Harvesting is starting for " + runningContext.toString() + "...");

		// Inicializar catálogo SQLite para escritura DESPUÉS de determinar
		// previousSnapshotId
		// Si es incremental, copia el catálogo del snapshot anterior
		try {
			catalogRepository.initializeSnapshot(snapshotMetadata, previousSnapshotId);
			if (previousSnapshotId != null) {
				// Para incremental: copiar el tamaño del snapshot anterior como base
				Integer previousSize = snapshotStore.getSnapshotSize(previousSnapshotId);
				if (previousSize != null && previousSize > 0) {
					snapshotStore.incrementSnapshotSizeBy(snapshotId, previousSize);
					logInfoMessage("CATALOG: Repository initialized - copied from snapshot " + previousSnapshotId +
							" with initial size: " + previousSize);
				} else {
					logInfoMessage("CATALOG: Repository initialized - copied from snapshot " + previousSnapshotId);
				}
			} else {
				logInfoMessage("CATALOG: Repository initialized (new catalog) - snapshot " + snapshotId);
			}
		} catch (Exception e) {
			logErrorMessage("CATALOG: Error initializing repository: " + e.getMessage());
			snapshotStore.markAsFailed(snapshotId);
			return;
		}

		snapshotStore.startHarvesting(this.snapshotId);

		// Inicialización del harvester
		harvester.reset();
		runOAIPMHHarvesting();

		// Cuando el harvesting termina, verificar si hay errores

		// Si el tamaño del snapshot es 0 y no es incremental, no hay records
		if (snapshotStore.getSnapshotSize(snapshotId) < 1 && !isIncremental()) {
			logErrorMessage(runningContext.toString() + " :: No records found !!");
			snapshotStore.markAsFailed(this.snapshotId);
		}

		// Si el status no es error, el harvesting terminó exitosamente
		if (snapshotStore.getSnapshotStatus(snapshotId) != SnapshotStatus.HARVESTING_FINISHED_ERROR) {

			if (isIncremental() && previousSnapshotId != null) {
				// El catálogo ya fue copiado durante la inicialización
				// Solo actualizar metadata del snapshot
				logInfoMessage(runningContext.toString() +
						" :: Catalog inherited from previous snapshot: " + previousSnapshotId);
				snapshotStore.updateSnapshotLastIncrementalDatestamp(snapshotId, previousSnapshotStartDatestamp);
				snapshotStore.setPreviousSnapshotId(snapshotId, previousSnapshotId);
			}

			logInfoMessage(runningContext.toString() + " :: Harvesting ended successfully.");

			snapshotStore.finishHarvesting(snapshotId);

		} else {
			logErrorMessage(runningContext.toString() + " :: Harvesting ended with errors !!!");
			snapshotStore.markAsFailed(snapshotId);
		}

		// Cerrar catálogo SQLite (flush final) - SE CIERRA SIEMPRE al final
		closeCatalogRepository();

	}

	/**
	 * Cierra el catálogo SQLite de forma segura.
	 */
	private void closeCatalogRepository() {
		if (catalogRepository != null && catalogRepository.hasActiveManager(snapshotId)) {
			try {
				long recordCount = catalogRepository.count(snapshotId);
				catalogRepository.finalizeSnapshot(snapshotId);
				logInfoMessage("CATALOG: Repository closed - " + recordCount + " records in catalog");
			} catch (Exception e) {
				logErrorMessage("CATALOG: Error closing repository: " + e.getMessage());
			}
		}
	}

	/**
	 * Verifica si el metadata pasa la prevalidación.
	 */
	private Boolean metadataPassPrevalidation(OAIRecordMetadata metadata) throws ValidationException {

		if (validator == null)
			return true;
		else {
			validatorResult = validator.validate(metadata, validatorResult);
			return validatorResult.isValid();
		}

	}

	/**
	 * Ejecuta el harvesting OAI-PMH.
	 */
	private void runOAIPMHHarvesting() {

		String originURL = runningContext.getNetwork().getOriginURL();
		String metadataPrefix = runningContext.getNetwork().getMetadataPrefix();
		String metadataStoreSchema = runningContext.getNetwork().getMetadataStoreSchema();

		String until = null;

		List<String> sets = runningContext.getNetwork().getSets();

		if (originURL != null) {

			// Si tiene sets declarados solo cosecha esos
			if (sets != null && sets.size() > 0) {

				bySetHarvesting = true;

				logInfoMessage("There are defined sets. Harvesting configured sets for " +
						runningContext.toString());
				logInfoMessage("Please note that sets may not be disjoint, " +
						"so the same record may be harvested more than once");

				for (String set : sets) {
					logInfoMessage("Harvesting set: " + set + " for " + runningContext.toString());
					currentSetSpec = set;
					harvester.harvest(originURL, set, metadataPrefix, metadataStoreSchema,
							from, until, null, MAX_RETRIES);
				}
			}
			// Si no hay set declarado cosecha todo
			else {
				logInfoMessage("There aren't defined sets. Harvesting the entire collection for " +
						runningContext.toString());
				harvester.harvest(originURL, null, metadataPrefix, metadataStoreSchema,
						from, until, null, MAX_RETRIES);
			}
		}
	}

	/**
	 * Maneja eventos de harvesting.
	 */
	@Override
	public void harvestingEventOccurred(HarvestingEvent event) {

		logger.debug(runningContext.getNetwork().getName() + "  HarvestingEvent received: " +
				event.getStatus());

		switch (event.getStatus()) {

			case OK:

				// Si algún record está missing, loguear el error
				if (event.isRecordMissing()) {
					logErrorMessage("Some record metadata is missing RT:" + event.getResumptionToken() +
							" identifiers: " + event.getMissingRecordsIdentifiers());
				}

				// Procesar records (batching handled by repository, buffering handled here by
				// event)
				// NOTA: Ya no hacemos tracking incremental del tamaño (sizeAdjustment)
				// El tamaño final se calculará al terminar el proceso consultando el catálogo

				// Lista para batch update
				java.util.List<OAIRecord> batchRecords = new java.util.ArrayList<>();

				if (isIncremental()) {
					try {
						// Procesar deleted records
						for (String deletedRecordIdentifier : event.getDeletedRecordsIdentifiers()) {
							batchRecords.add(createDeletedRecord(deletedRecordIdentifier, LocalDateTime.now()));
						}

						// Loguear deleted records
						if (!event.getDeletedRecordsIdentifiers().isEmpty()) {
							logger.debug("Stored as deleted:" + event.getDeletedRecordsIdentifiers() +
									" for " + runningContext.toString());
						}

					} catch (Exception e) {
						logErrorMessage("Error storing deleted records for " + runningContext.toString() +
								" : " + e.getMessage());
					}
				}

				// Agregar records no eliminados al snapshot
				for (OAIRecordMetadata metadata : event.getRecords()) {
					try {
						// Si el metadata pasa la prevalidación, almacenarlo
						if (metadataPassPrevalidation(metadata)) {
							batchRecords.add(createRecord(metadata));
						}
					} catch (ValidationException e) {
						logErrorMessage("Error prevalidating record " + metadata.getIdentifier() +
								" for " + runningContext.toString() + " : " + e.getMessage());
					} catch (Exception e) {
						logErrorMessage("Unknown record store error :: " + e.getMessage());
					}
				}

				// Batch upsert hacia el catálogo
				// Esto usará una sola transacción para todos los registros de la página
				if (!batchRecords.isEmpty()) {
					try {
						catalogRepository.upsertBatch(snapshotId, batchRecords);
						// Optimistic size update: assume all records are new/relevant for progress
						// display
						// The final exact count is corrected at finishHarvestingSuccessfully()
						snapshotStore.incrementSnapshotSizeBy(snapshotId, batchRecords.size());
					} catch (Exception e) {
						logErrorMessage("Error performing batch upsert: " + e.getMessage());
					}
				}

				// Snapshot update
				snapshotStore.updateHarvesting(snapshotId);

				break;

			case NO_MATCHING_QUERY:

				// Si NO_MATCHING_QUERY, entonces
				if (!bySetHarvesting) {

					if (isIncremental()) {
						finishHarvestingSuccessfully(); // Puede ser que no haya nuevos registros
						logInfoMessage("No records found in incremental harvesting" +
								runningContext.toString());
					} else {
						snapshotStore.markAsFailed(snapshotId);
						logInfoMessage("No records found!!! at " + runningContext.toString());
					}

				} else {
					finishHarvestingSuccessfully();
					logInfoMessage("No records found for the set: " + currentSetSpec + " at " +
							runningContext.toString());
				}

				break;

			case ERROR_RETRY:

				logErrorMessage("Retry:" + event.getMessage());
				snapshotStore.markAsRetrying(snapshotId);
				break;

			case ERROR_FATAL:

				logErrorMessage("Fatal Error:" + event.getMessage());
				snapshotStore.markAsFailed(snapshotId);
				break;

			case STOP_SIGNAL_RECEIVED:

				logErrorMessage("Stop signal received:" + event.getMessage());

				// Intentar finalizar correctamente si hay datos
				long currentCount = catalogRepository.countNotDeleted(snapshotId);
				if (currentCount > 0)
					finishHarvestingSuccessfully();
				else
					snapshotStore.markAsFailed(snapshotId);

				// Cerrar catálogo SQLite cuando se detiene el harvesting
				closeCatalogRepository();
				break;

			default:
				// TODO: decidir qué hacer en este caso
				break;
		}
	}

	/**
	 * Finaliza el harvesting exitosamente, actualizando el tamaño final.
	 */
	private void finishHarvestingSuccessfully() {
		// ACTUALIZAR TAMAÑO FINAL
		// Consultar el catálogo para obtener el conteo real exacto
		long finalCount = catalogRepository.countNotDeleted(snapshotId);
		snapshotStore.updateSnapshotSize(snapshotId, (int) finalCount);

		logInfoMessage("Harvesting finished. Final count: " + finalCount);

		snapshotStore.finishHarvesting(snapshotId);
	}

	/**
	 * Crea un record en SQLite a partir de metadata OAI.
	 * 
	 * @param metadata el metadata OAI a almacenar
	 * @return OAIRecord record creado
	 * @throws Exception si falla la creación
	 */
	private OAIRecord createRecord(OAIRecordMetadata metadata) throws Exception {

		// 1. Guardar XML en IMetadataStore y obtener hash
		String metadataStr = metadata.toString();
		String hash = metadataStore.storeAndReturnHash(snapshotMetadata, metadataStr);

		// 2. Crear OAIRecord para catálogo
		OAIRecord record = OAIRecord.create(
				metadata.getIdentifier(),
				metadata.getDatestamp(),
				hash,
				false // no deleted
		);

		return record;
	}

	/**
	 * Crea un deleted record marker en SQLite.
	 * 
	 * @param identifier el identificador OAI del record eliminado
	 * @param dateStamp  el timestamp de eliminación
	 * @return OAIRecord record eliminado creado
	 * @throws Exception si falla la creación
	 */
	private OAIRecord createDeletedRecord(String identifier, LocalDateTime dateStamp) throws Exception {

		// Crear OAIRecord con flag deleted=true
		OAIRecord record = OAIRecord.create(
				identifier,
				dateStamp,
				null, // sin hash (no hay metadata)
				true // deleted
		);

		return record;
	}

	/**
	 * Loguea un mensaje de error.
	 */
	private void logErrorMessage(String message) {
		logger.error(message);
		snapshotLogService.addEntry(snapshotId, "ERROR: " + message);
	}

	/**
	 * Loguea un mensaje informativo.
	 */
	private void logInfoMessage(String message) {
		logger.info(message);
		snapshotLogService.addEntry(snapshotId, "INFO: " + message);
	}
}
