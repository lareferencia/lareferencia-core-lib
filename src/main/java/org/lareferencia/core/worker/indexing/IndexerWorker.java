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

package org.lareferencia.core.worker.indexing;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.domain.OAIRecord;
import org.lareferencia.core.repository.validation.ValidationRecord;
import org.lareferencia.core.repository.validation.ValidationRecordPaginator;
import org.lareferencia.core.repository.validation.ValidationDatabaseManager;
import org.lareferencia.core.service.management.SnapshotLogService;
import org.lareferencia.core.service.validation.ValidationManifestStore;
import org.lareferencia.core.metadata.IMDFormatTransformer;
import org.lareferencia.core.metadata.IMetadataStore;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.MDFormatTranformationException;
import org.lareferencia.core.metadata.MDFormatTransformerService;
import org.lareferencia.core.metadata.MDTransformerParameterSetter;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadataParseException;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.util.date.DateHelper;
import org.lareferencia.core.util.IRecordFingerprintHelper;
import org.lareferencia.core.worker.BaseBatchWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

/**
 * Worker that indexes harvested records into Solr.
 * <p>
 * Transforms and indexes validated records with optional deletion support.
 * </p>
 * 
 * @author LA Referencia Team
 */
@Component("indexerWorkerFlowable")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class IndexerWorker extends BaseBatchWorker<ValidationRecord, NetworkRunningContext> {

	@Autowired
	private ISnapshotStore snapshotStore;

	@Autowired
	private IMetadataStore metadataStore;

	@Autowired
	private ValidationDatabaseManager dbManager;

	@Autowired
	private IRecordFingerprintHelper recordFingerprintHelper;

	private static Logger logger = LogManager.getLogger(IndexerWorker.class);

	@Autowired
	private SnapshotLogService snapshotLogService;

	@Autowired
	MDFormatTransformerService trfService;

	private IMDFormatTransformer metadataTransformer;

	private HttpSolrClient solrClient;

	@Getter
	private String solrUrl;

	private Long snapshotId;
	private SnapshotMetadata snapshotMetadata;
	@Autowired private ValidationManifestStore validationManifestStore;
	private boolean useIncrementalDelta;

	@Getter
	@Setter
	private String targetSchemaName;

	@Getter
	@Setter
	private String solrNetworkIDField;

	@Getter
	@Setter
	private String solrRecordIDField = "id";
	@Getter @Setter
	private String solrRecordIDValue;

	@Getter
	@Setter
	private boolean executeDeletion = false;

	@Getter
	@Setter
	private boolean executeIndexing = false;

	@Getter
	@Setter
	private boolean indexDeletedRecords = false;

	@Getter
	@Setter
	private boolean indexNetworkAttributes = false;

	@Getter
	@Setter
	private Map<String, List<String>> contentFiltersByFieldName = null;

	private StringBuffer stringBuffer;

	NumberFormat percentajeFormat = NumberFormat.getPercentInstance();

	/**
	 * Creates an indexer worker with the specified Solr URL.
	 * 
	 * @param solrURL the Solr server URL
	 */
	public IndexerWorker(String solrURL) {
		super();
		setSolrUrl(solrURL);
	}

	/** Rebuilds the Solr client when the installation-level worker configuration changes. */
	public synchronized void setSolrUrl(String solrUrl) {
		if (solrUrl == null || solrUrl.isBlank()) throw new IllegalArgumentException("Solr URL must not be blank");
		HttpSolrClient replacement = new HttpSolrClient.Builder(solrUrl.trim()).build();
		HttpSolrClient previous = this.solrClient;
		this.solrClient = replacement;
		this.solrUrl = solrUrl.trim();
		if (previous != null) try { previous.close(); } catch (java.io.IOException exception) { logger.warn("Could not close previous Solr client", exception); }
	}

	/**
	 * Initializes the indexer before processing starts.
	 */
	public void preRun() {

		if (executeDeletion && !executeIndexing) { // si es un borrado sin indexacion

			logger.debug("Executing index deletion (without indexing): " + runningContext.getNetwork().getAcronym());

			// Si es solo un borrado envia el mensaje de borrado
			delete(runningContext.getNetwork().getAcronym());
			// establece un paginador nulo para no recorrer los registros

		} else {

			// busca el lgk
			snapshotId = snapshotStore.findLastGoodKnownSnapshot(runningContext.getNetwork()); // snpshotRepository.findLastGoodKnowByNetworkID(runningContext.getNetwork().getId());

			if (snapshotId != null) { // solo si existe un lgk

				snapshotMetadata = snapshotStore.getSnapshotMetadata(snapshotId);
				useIncrementalDelta = isIncremental() && solrRecordIDValue != null && !solrRecordIDValue.isBlank()
						&& validationManifestStore.read(snapshotMetadata)
						.map(f -> "INCREMENTAL".equalsIgnoreCase(f.getScope())).orElse(false);
				if (isIncremental() && !useIncrementalDelta) logger.warn("Incremental identity or validation scope is missing; rebuilding index fully");

				if (!useIncrementalDelta) {
					logger.debug("Executing index deletion: " + runningContext.getNetwork().getAcronym());
				logInfo("Executing index deletion: " + runningContext.toString() + " (" + this.targetSchemaName + ")");
				delete(runningContext.getNetwork().getAcronym());
				}

				logger.debug("Full indexing (" + this.targetSchemaName + "): " + snapshotId);
				logInfo("Full indexing: " + runningContext.toString() + "(" + this.targetSchemaName + ")");

				// establece el transformador para indexación
				try {
					// Create paginator for validation records
					ValidationRecordPaginator paginator = new ValidationRecordPaginator(
							snapshotMetadata, dbManager, useIncrementalDelta);
					paginator.setPageSize(getPageSize());
					this.setPaginator(paginator);

					metadataTransformer = trfService
							.getMDTransformer(runningContext.getNetwork().getMetadataStoreSchema(), targetSchemaName);

					metadataTransformer.setParameter("networkAcronym", runningContext.getNetwork().getAcronym());
					metadataTransformer.setParameter("networkName", runningContext.getNetwork().getName());
					metadataTransformer.setParameter("institutionName",
							runningContext.getNetwork().getInstitutionName());
					metadataTransformer.setParameter("institutionAcronym",
							runningContext.getNetwork().getInstitutionAcronym());

					// Set parameters from network attributes
					if (indexNetworkAttributes)
						MDTransformerParameterSetter.setParametersFromMap(metadataTransformer, "attr_",
								runningContext.getNetwork().getAttributes());

				} catch (MDFormatTranformationException e) {
					logError("Metadata transformation configuration ERROR at indexing: " + runningContext.toString()
							+ " " + runningContext.getNetwork().getMetadataStoreSchema() + " >> " + targetSchemaName
							+ " error: " + e.getMessage());
					error();
				}

			} else {

				logError("Didn't find LGKSnapshot on the network: " + runningContext.toString());
				error();
			}

		}
	}

	public void prePage() {
		stringBuffer = new StringBuffer();
	}

	public void processItem(ValidationRecord record) {

		try {
			if (useIncrementalDelta && (record.isDeleted() || !record.isValid())) {
				OAIRecord identityRecord = new OAIRecord();
				identityRecord.setIdentifier(record.getIdentifier());
				String value = recordFingerprintHelper.getRecordIdValue(identityRecord, snapshotMetadata, solrRecordIDValue);
				this.sendUpdateToSolr("<delete><query>" + solrRecordIDField + ":" + ClientUtils.escapeQueryChars(value) + "</query></delete>");
				return;
			}

			OAIRecordMetadata metadata = new OAIRecordMetadata(record.getIdentifier(),
					metadataStore.getMetadata(snapshotMetadata, record.getPublishedMetadataHash()));

			// this filters the records by content, using the contentFiltersByFieldName map
			if (contentFiltersByFieldName != null) {

				logger.debug("Evaluating record content: " + record.getIdentifierHash());

				boolean allowIndexing = true;

				for (String fieldName : contentFiltersByFieldName.keySet()) {

					boolean isFieldValid = false;

					List<String> validValues = contentFiltersByFieldName.get(fieldName);

					for (String occr : metadata.getFieldOcurrences(fieldName)) {
						isFieldValid |= validValues.contains(occr);
					}

					allowIndexing &= isFieldValid;

					logger.debug("fieldName: " + isFieldValid);

				}

				if (!allowIndexing) // si no pertenece al conjunto de este indexador entonces debe ser rechazado
					return;

			} // end of content filtering

			// fingerprint del registro
			metadataTransformer.setParameter("fingerprint", runningContext.getNetwork().getAcronym() + "_"
					+ record.getIdentifierHash());

			// identifier del record
			metadataTransformer.setParameter("identifier", record.getIdentifier());

			// Stable numeric ID derived from the textual record fingerprint.
			OAIRecord identityRecord = new OAIRecord();
			identityRecord.setIdentifier(record.getIdentifier());
			metadataTransformer.setParameter("record_id",
					recordFingerprintHelper.getRecordIdLong(identityRecord, snapshotMetadata).toString());

			// metadata como string
			if (record.getDatestamp() != null)
				metadataTransformer.setParameter("timestamp",
						DateHelper.getDateTimeMachineString(record.getDatestamp()));

			// if the record is invalid, deleted or untested then set the deleted parameter
			// to true
			// this parameter is used to filter out deleted records in oai providers
			// this parameter is not used by frontends solr indices
			metadataTransformer
					.setParameter("deleted", Boolean.valueOf(!record.isValid()).toString());

			// metadata como string
			metadataTransformer.setParameter("metadata", metadata.toString());

			// if the record is valid or if it is a deleted record but indexDeletedRecords
			// is true then index it
			// indexDeletedRecords is used to index deleted records in the index, this is
			// used by oaipmh providers but not by frontends
			// frontend indexer should set indexDeletedRecords to false and oai provider
			// indexer should set it to true
			if (record.isValid()) {
				String recordStr = metadataTransformer.transformToString(metadata.getDOMDocument());
				stringBuffer.append(recordStr);

				logger.debug("Transformed record to be indexed: " + record.getIdentifierHash() + " :: "
						+ record.getIdentifier() + " :: " + recordStr);
				logger.debug("Indexed record size in bytes: " + recordStr.length());
			} else {
				logger.debug(
						"Record not indexed: " + record.getIdentifierHash() + " :: " + record.getIdentifier() + " :: "
								+ record.isValid());
			}

		} catch (MDFormatTranformationException e) {

			logError("Index::RecordID:" + record.getIdentifierHash() + " oai_id:" + record.getIdentifier()
					+ " transformation Error xslt with the schema: " + targetSchemaName + " :: " + e.getMessage());
			logger.debug(e.getMessage(), e);
			// logger.debug("Record Metadata: \n" + metadata.toString() + "\n\n");
			error();
		} catch (OAIRecordMetadataParseException e) {
			logError("Index::RecordID:" + record.getIdentifierHash() + " oai_id:" + record.getIdentifier()
					+ " error getting record metadata: :: " + e.getMessage());
		} catch (Exception e) {
			// Catches MetadataRecordStoreException and other exceptions
			logError("Index::RecordID:" + record.getIdentifierHash() + " oai_id:" + record.getIdentifier()
					+ " error: :: " + e.getMessage());
		}

	}

	public void postPage() {

		if (stringBuffer.length() > 0) {
			try {
				this.sendUpdateToSolr("<add>" + stringBuffer.toString() + "</add>");
			} catch (SolrServerException e) {
				logError("Issues whe connecting to SOLR: " + runningContext.toString() + ": " + e.getMessage());
				logger.debug(stringBuffer);
				solrRollback();
				error();

			} catch (IOException e) {
				logError("Issues when sending to SOLR - I/O: " + runningContext.toString()
						+ ": " + e.getMessage());
				logger.debug(stringBuffer);
				solrRollback();
				error();

			} catch (Exception e) {
				logError("Issues with the index process - Undetermined: " + runningContext.toString()
						+ ": " + e.getMessage());
				logger.debug(stringBuffer);
				solrRollback();
				error();
			}
		}
	}

	public void postRun() {

		try {
			// Paginator cleanup is handled by BaseBatchWorker

			postPage();

			this.sendUpdateToSolr("<commit/>");

			if (executeIndexing)
				snapshotStore.markAsIndexed(snapshotId);

			logInfo("Finishing Indexing: " + runningContext.toString() + "(" + this.targetSchemaName + ")");
			logInfo("Indexed documents in " + runningContext.getNetwork().getAcronym() + "::" + this.targetSchemaName
					+ " = " + this.queryForNetworkDocumentCount(runningContext.getNetwork().getAcronym()));

			logger.debug("Updates snapshot status to " + SnapshotIndexStatus.INDEXED);

		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError("Issues when commiting to SOLR: " + runningContext.toString() + ": " + e.getMessage());
			error();
		}
	}

	/******************* Auxiliares ********** */
	private void error() {
		// With new @Transactional pattern, simply stopping the worker will persist
		// the current snapshot state. Index status remains FAILED by default.
		// Paginator cleanup is handled by BaseBatchWorker
		this.stop();
	}

	private void logError(String message) {
		logger.error(message);
		snapshotLogService.addEntry(snapshotId, "ERROR: " + message);
	}

	private void logInfo(String message) {
		logger.info(message);
		snapshotLogService.addEntry(snapshotId, "INFO: " + message);
	}

	private void delete(String networkAcronym) {
		// Borrado de la red
		try {
			this.sendUpdateToSolr(
					"<delete><query>" + this.solrNetworkIDField + ":" + networkAcronym + "</query></delete>");
		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError("Issues when deleting index: " + runningContext.toString() + ": " + e.getMessage());
			error();
		}
	}

	private Long queryForNetworkDocumentCount(String networkAcronym) {

		try {
			return this.sendCountQueryToSolr(this.solrNetworkIDField + ":" + networkAcronym);

		} catch (Exception e) {
			logError("Issues when querying for network document count: " + runningContext.toString() + ": "
					+ e.getMessage());
			error();
		}
		return 0L;
	}

	private void solrRollback() {

		try {
			this.sendUpdateToSolr("<rollback/>");
		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError("Issues with rollback  " + runningContext.toString() + ": " + e.getMessage());
			error();
		}
	}

	private void sendUpdateToSolr(String data)
			throws SolrServerException, IOException, HttpSolrClient.RemoteSolrException {
		DirectXmlRequest request = new DirectXmlRequest("/update", data);
		solrClient.request(request);
	}

	private Long sendCountQueryToSolr(String queryString) {
		try {
			// Create select query
			SolrQuery query = new SolrQuery();
			query.setQuery(queryString);
			query.setRows(0);

			return solrClient.query(query).getResults().getNumFound();

		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError("Issues with query  " + runningContext.toString() + ": " + e.getMessage());
			error();
		}
		return 0L;
	}

	@Override
	public String toString() {
		return "Indexer[" + ((executeDeletion && !executeIndexing) ? "Delete:" : "") + targetSchemaName + "]("
				+ percentajeFormat.format(this.getCompletionRate()) + ")";
	}

}
