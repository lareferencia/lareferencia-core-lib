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

package org.lareferencia.core.worker.validation;

import java.io.IOException;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.util.Iterator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lareferencia.core.repository.parquet.OAIRecord;
import org.lareferencia.core.repository.parquet.OAIRecordParquetRepository;
import org.lareferencia.core.repository.jpa.NetworkRepository;
import org.lareferencia.core.service.management.SnapshotLogService;
import org.lareferencia.core.service.validation.ValidationService;
import org.lareferencia.core.service.validation.IValidationStatisticsService;
import org.lareferencia.core.service.validation.ValidationStatisticsException;
import org.lareferencia.core.metadata.IMetadataStore;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadataParseException;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.worker.BaseIteratorWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Worker that performs validation and transformation of harvested OAI records
 * using Parquet storage.
 * <p>
 * This is the NEW Parquet-based implementation that uses:
 * -
 * {@link org.lareferencia.backend.repositories.parquet.OAIRecordParquetRepository}
 * for OAI records (Parquet)
 * - {@link ISnapshotStore} for snapshot metadata (SQL)
 * <p>
 * For the legacy SQL-based implementation, see {@link ValidationWorkerLegacy}.
 * <p>
 * This worker processes OAI records from Parquet storage in a network snapshot,
 * applying validation rules and metadata transformations. It supports both full
 * and
 * incremental validation modes.
 * <p>
 * The worker performs the following operations:
 * </p>
 * <ul>
 * <li>Reads OAI records from Parquet storage (streaming)</li>
 * <li>Validates metadata records against configured validation rules</li>
 * <li>Applies primary and secondary transformations to valid records</li>
 * <li>Tracks validation statistics and observations</li>
 * <li>Updates record status (stored in Parquet)</li>
 * <li>Writes validated/transformed records back to Parquet</li>
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
 * @see BaseParquetOAIRecordWorker
 */
@Component("validationWorkerFlowable")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ValidationWorker extends BaseIteratorWorker<OAIRecord, NetworkRunningContext> {

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

	@Autowired
	protected OAIRecordParquetRepository recordRepository;

	private ITransformer transformer;
	private ITransformer secondaryTransformer;
	private IValidator validator;

	@Autowired
	private ValidationService validationManager;

	@Autowired
	IValidationStatisticsService validationStatisticsService;

	// reusable objects
	private ValidatorResult reusableValidationResult;
	private Boolean wasTransformed;

	private SnapshotMetadata snapshotMetadata;

	/**
	 * Constructs a new validation worker.
	 */
	public ValidationWorker() {
		super();

	}

	@Override
	public void preRun() {

		// new reusable validation result
		reusableValidationResult = new ValidatorResult();

		// trata de conseguir la ultima cosecha válida o el revalida el lgk
		Long snapshotId = snapshotStore.findLastHarvestingSnapshot(runningContext.getNetwork());

		if (snapshotId != null) {

			// Cargar metadata completo del snapshot y asignarlo al campo del padre
			this.snapshotMetadata = snapshotStore.getSnapshotMetadata(snapshotId);

			try {
				Iterator<OAIRecord> it = recordRepository.getIterator(snapshotMetadata);
				this.setIterator(it, snapshotMetadata.getSize());
			} catch (IOException e) {
				logError("Error initializing OAIRecord iterator for snapshot " + snapshotId + ": " + e.getMessage());
				this.stop();
				return;
			}

			try {
				validationStatisticsService.deleteValidationStatsObservationsBySnapshotID(snapshotId);
			} catch (ValidationStatisticsException e) {
				logError("Error deleting previous validation results: " + e.getMessage());
				this.stop();
			}

			// INITIALIZE: Create fresh writers AFTER cleanup
			logInfo("Initializing validation statistics for snapshot: " + snapshotId);
			validationStatisticsService.initializeValidationForSnapshot(this.snapshotMetadata);
			logger.debug(
					"Detailed diagnose: " + runningContext.getNetwork().getBooleanPropertyValue("DETAILED_DIAGNOSE"));
			validationStatisticsService
					.setDetailedDiagnose(runningContext.getNetwork().getBooleanPropertyValue("DETAILED_DIAGNOSE"));

			try {

				if (runningContext.getNetwork().getValidator() != null) {
					validator = validationManager.createValidatorFromModel(runningContext.getNetwork().getValidator());
				} else {
					logInfo("No Validator was found for " + runningContext.toString() + "!!!");
				}

				if (runningContext.getNetwork().getTransformer() != null) {
					transformer = validationManager
							.createTransformerFromModel(runningContext.getNetwork().getTransformer());
				}

				if (runningContext.getNetwork().getSecondaryTransformer() != null) {
					secondaryTransformer = validationManager
							.createTransformerFromModel(runningContext.getNetwork().getSecondaryTransformer());
				}

				if (transformer == null && secondaryTransformer == null)
					logInfo("No transformers for " + runningContext.toString() + "!!!");

			} catch (ValidationException e) {
				logError(runningContext.toString() + ": " + e.getMessage());
				this.stop();
				return;
			}

		} else {
			logger.error("There is not a suitable snapshot for validation");
			this.stop();
		}

		logInfo("Starting Validation/Transformation of " + runningContext.toString());
		snapshotStore.startValidation(snapshotMetadata.getSnapshotId());
		snapshotStore.resetSnapshotValidationCounts(snapshotMetadata.getSnapshotId());

	}

	@Override
	public void prePage() {
		// No additional work needed here
	}

	@Override
	public void postPage() {
		// postPage is called after each page - counts are already updated in
		// processItem()
		// No additional work needed here
	}

	@Override
	public void processItem(OAIRecord record) {

		try {
			// reset validation result
			reusableValidationResult.reset();

			logger.debug("Validating record: " + record.getId() + " :: " + record.getIdentifier());
			logger.debug("Initial status: " + record.getId() + " :: " + record.getIdentifier() + "::");
			wasTransformed = false;

			// carga la metadata original sin transformar
			logger.debug("Load metadata: " + record.getId() + " :: " + record.getIdentifier());
			String metadataStr = metadataStoreService.getMetadata(snapshotMetadata, record.getOriginalMetadataHash());
			OAIRecordMetadata metadata = new OAIRecordMetadata(record.getIdentifier(), metadataStr);

			// si corresponde lo transforma

			logger.debug("Starting transformations: " + record.getId() + " :: " + record.getIdentifier());

			// transforma

			if (transformer != null) {
				logger.debug("Primary transformer: " + record.getId() + " :: " + record.getIdentifier());
				wasTransformed |= transformer.transform(this.snapshotMetadata, record, metadata);
			}

			if (secondaryTransformer != null) {
				logger.debug("Secondary transformer: " + record.getId() + " :: " + record.getIdentifier());
				wasTransformed |= secondaryTransformer.transform(this.snapshotMetadata, record, metadata);
			}

			logger.debug(record.getId() + " :: " + record.getIdentifier() + "  Transformed: " + wasTransformed);

			// if validator is defined
			if (validator != null) {

				logger.debug("Validating: " + record.getId() + " :: " + record.getIdentifier());

				// validación
				reusableValidationResult = validator.validate(metadata, reusableValidationResult);
				reusableValidationResult.setTransformed(wasTransformed);

			} else { // if no validator is set, then record is consired valid and the validation
						// results are set to true

				reusableValidationResult.setValid(true);
			}

			logger.debug(record.getId() + " :: " + record.getIdentifier() + " final status: ");

			// store metadata if needed and set publishedMetadataHash
			String publishedMetadataHash = record.getOriginalMetadataHash();

			// if transformed store the metadata, get hash and set datestamp as now
			if (wasTransformed) {
				publishedMetadataHash = metadataStoreService.storeAndReturnHash(snapshotMetadata, metadata.toString());
				record.setDatestamp(LocalDateTime.now());
			}
			// store publishedMetadataHash in validation result
			reusableValidationResult.setMetadataHash(publishedMetadataHash);

			// Se almacenan las estadísticas de cosecha
			logger.debug("Storing diagnose " + record.getId() + " :: " + record.getIdentifier());
			// validationStatsObservations.add(
			// validationStatisticsService.buildObservation(record,
			// reusableValidationResult) );
			validationStatisticsService.addObservation(snapshotMetadata, record, reusableValidationResult);

			// Update snapshot counts immediately after validation
			Long snapshotId = snapshotMetadata.getSnapshotId();
			if (reusableValidationResult.isValid()) {
				snapshotStore.incrementValidSize(snapshotId);
			}
			if (wasTransformed) {
				snapshotStore.incrementTransformedSize(snapshotId);
			}

		} catch (OAIRecordMetadataParseException e) {

			logError("Metadata parsing record ID: " + record.getId() + " oai_id: " + record.getIdentifier() + " :: "
					+ e.getMessage());
			// logger.debug( record.getOriginalXML());
			snapshotStore.finishHarvesting(snapshotMetadata.getSnapshotId());
			this.stop();

		} catch (ValidationException e) {
			logError("Validation error:" + runningContext.toString() + ": " + e.getMessage());
			snapshotStore.finishHarvesting(snapshotMetadata.getSnapshotId());
			this.stop();

		} catch (Exception e) {
			logError("Unknown validation error:" + runningContext.toString() + ": " + e.getMessage());
			snapshotStore.finishHarvesting(snapshotMetadata.getSnapshotId());
			this.stop();
		}

	}

	@Override
	public void postRun() {
		// Finalize validation and mark snapshot as complete
		try {
			validationStatisticsService.finalizeValidationForSnapshot(snapshotMetadata.getSnapshotId());
		} catch (Exception e) {
			logger.error("ERROR: Failed to finalize validation data for snapshot {}", snapshotMetadata.getSnapshotId(),
					e);
		}

		// Mark snapshot as validation complete - all counts already updated in
		// processItem()
		snapshotStore.finishValidation(snapshotMetadata.getSnapshotId());
		logInfo("Finishing Validation/Transformation of " + runningContext.toString());
	}

	@Override
	public String toString() {
		return "Transform/Validate(" + percentajeFormat.format(this.getCompletionRate()) + ")"
				+ (isIncremental() ? "(I)" : "(F)");
	}

	private void logError(String message) {
		logger.error(message);
		snapshotLogService.addEntry(snapshotMetadata.getSnapshotId(), "ERROR: " + message);
	}

	private void logInfo(String message) {
		logger.info(message);
		snapshotLogService.addEntry(snapshotMetadata.getSnapshotId(), "INFO: " + message);
	}

}
