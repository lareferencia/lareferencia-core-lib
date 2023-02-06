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

package org.lareferencia.backend.workers.indexer;

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
import org.apache.solr.client.solrj.response.QueryResponse;
import org.lareferencia.backend.domain.OAIBitstream;
import org.lareferencia.backend.domain.SnapshotIndexStatus;
import org.lareferencia.backend.repositories.jpa.OAIBitstreamRepository;
import org.lareferencia.backend.services.SnapshotLogService;
import org.lareferencia.core.metadata.IMDFormatTransformer;
import org.lareferencia.backend.domain.OAIRecord;
import org.lareferencia.core.metadata.IMetadataRecordStoreService;
import org.lareferencia.core.metadata.MDFormatTranformationException;
import org.lareferencia.core.metadata.MDFormatTransformerService;
import org.lareferencia.core.metadata.MDTransformerParameterSetter;
import org.lareferencia.core.metadata.MetadataRecordStoreException;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadataParseException;
import org.lareferencia.core.metadata.RecordStatus;
import org.lareferencia.core.util.date.DateHelper;
import org.lareferencia.core.util.IRecordFingerprintHelper;
import org.lareferencia.core.worker.BaseBatchWorker;
import org.lareferencia.core.worker.IPaginator;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;

import lombok.Getter;
import lombok.Setter;

public class IndexerWorker extends BaseBatchWorker<OAIRecord, NetworkRunningContext> {

	@Autowired
	private OAIBitstreamRepository bitstreamRepository;

	@Autowired
	private IMetadataRecordStoreService metadataStoreService;

	private static Logger logger = LogManager.getLogger(IndexerWorker.class);

	@Autowired
	private IRecordFingerprintHelper fingerprintHelper;

	@Autowired
	private SnapshotLogService snapshotLogService;

	@Autowired
	MDFormatTransformerService trfService;

	private IMDFormatTransformer metadataTransformer;

	private HttpSolrClient solrClient;

	private Long snapshotId;

	@Getter
	@Setter
	private String targetSchemaName;

	@Getter
	@Setter
	private String solrNetworkIDField;

	@Getter
	@Setter
	private String solrRecordIDField = "id";

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

	public IndexerWorker(String solrURL) {
		super();

		this.solrClient = new HttpSolrClient.Builder(solrURL).build();
	}

	@Override
	public void preRun() {

		if (executeDeletion && !executeIndexing) { // si es un borrado sin indexacion

			logger.debug("Executing index deletion (without indexing): " + runningContext.getNetwork().getAcronym());

			// Si es solo un borrado envia el mensaje de borrado
			delete(runningContext.getNetwork().getAcronym());
			// establece un paginador nulo para no recorrer los registros
			this.setPaginator(null);

		} else {

			// busca el lgk
			snapshotId = metadataStoreService.findLastGoodKnownSnapshot(runningContext.getNetwork()); // snpshotRepository.findLastGoodKnowByNetworkID(runningContext.getNetwork().getId());

			if (snapshotId != null) { // solo si existe un lgk

				// si es incremental
				if (this.isIncremental()) {

					logger.debug("Incremental index (" + this.targetSchemaName + "): " + snapshotId);
					logInfo("Incremental index: "+ runningContext.toString() +" (" + this.targetSchemaName + ")");

					// establece una paginator para recorrer los registros que han sido actualizados
					// (validados) luego de la
					// fecha/hora de la ultima cosecha
					IPaginator<OAIRecord> paginator;
				
					try {
						paginator = metadataStoreService.getUpdatedRecordsPaginator(snapshotId);
						paginator.setPageSize(this.getPageSize());
						this.setPaginator(paginator);
						
					} catch (MetadataRecordStoreException e) {
						logError("Indexing incremental error - building updated record paginator ::" + e.getMessage());
						this.setPaginator(null);
						error();
					}
					

				} else { // si no es incremental

					logger.debug("Executing index deletion: " + runningContext.getNetwork().getAcronym());
					logInfo("Executing index deletion: "+ runningContext.toString() +" (" + this.targetSchemaName + ")");
					delete(runningContext.getNetwork().getAcronym());

					logger.debug("Full indexing (" + this.targetSchemaName + "): " + snapshotId);
					logInfo("Full indexing: "+ runningContext.toString() +"(" + this.targetSchemaName + ")");

					// establece una paginator para recorrer los registros que no sean inválidos ( validos, deleted y untested )
					IPaginator<OAIRecord> paginator = metadataStoreService.getNotInvalidRecordsPaginator(snapshotId);
					paginator.setPageSize(this.getPageSize());
					this.setPaginator(paginator);
				}

				// establece el transformador para indexación
				try {
					metadataTransformer = trfService.getMDTransformer(runningContext.getNetwork().getMetadataStoreSchema(), targetSchemaName);

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
					this.setPaginator(null);
					error();
				}

			} else {

				logError("Didn't find LGKSnapshot on the network: " + runningContext.toString());
				this.setPaginator(null);
				error();
			}

		}
	}

	@Override
	public void prePage() {
		stringBuffer = new StringBuffer();
	}

	@Override
	public void processItem(OAIRecord record) {

		try {

			OAIRecordMetadata metadata = metadataStoreService.getPublishedMetadata(record);

			if (contentFiltersByFieldName != null) { // esto filtra los registros por contenido, dejando solo los que
														// deben ser enviados al indice

				logger.debug("Evaluating record content: " + record.getId());

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

			} // fin del filtro por contenido

			// Se transforma y genera el string del registro

			// si esta indicado borrar los registros borrados o invalidos los elimina del
			// indice
			if (executeDeletion && (record.getStatus() == RecordStatus.DELETED
					|| record.getStatus() == RecordStatus.INVALID || record.getStatus() == RecordStatus.UNTESTED)) {

				logger.debug("Deleting record from solr index (" + record.getStatus() + "): " + record.getId());
				deleteRecord(record.getId().toString(), fingerprintHelper.getFingerprint(record));
			}

			// fingerprint del registro
			metadataTransformer.setParameter("fingerprint", fingerprintHelper.getFingerprint(record));

			// identifier del record
			metadataTransformer.setParameter("identifier", record.getIdentifier());

			// metadata como string
			metadataTransformer.setParameter("record_id", record.getId().toString());

			// metadata como string
			metadataTransformer.setParameter("timestamp", DateHelper.getDateTimeMachineString(record.getDatestamp()));

			metadataTransformer
					.setParameter("deleted",
							(new Boolean(record.getStatus() == RecordStatus.DELETED
									|| record.getStatus() == RecordStatus.INVALID
									|| record.getStatus() == RecordStatus.UNTESTED)).toString());

			// metadata como string
			metadataTransformer.setParameter("metadata", metadata.toString());

			if (runningContext.getNetwork().getBooleanPropertyValue("INDEX_FULLTEXT")) {

				/** Fulltext ***/
				StringBuilder fulltextStringBuilder = new StringBuilder();
				for (OAIBitstream bstream : bitstreamRepository.findByNetworkIdAndIdentifier(
						record.getSnapshot().getNetwork().getId(), record.getIdentifier())) {

					if (bstream.getFulltext() != null) {
						fulltextStringBuilder.append(bstream.getFulltext());
					}

				}
				metadataTransformer.setParameter("fulltext", fulltextStringBuilder.toString());

				/** Fulltext end ***/

			}

			// stringBuffer.append( metadataTransformer.transformToString(
			// record.getMetadata().getDOMDocument()) );

			// si no debe indexar registros borrados o invalidos los elimina del indice
			if (!indexDeletedRecords && (record.getStatus() == RecordStatus.DELETED
					|| record.getStatus() == RecordStatus.INVALID || record.getStatus() == RecordStatus.UNTESTED)) {

				logger.debug("Executing record deletion (" + record.getStatus() + "): " + record.getId());
				deleteRecord(record.getId().toString(), fingerprintHelper.getFingerprint(record));
			}

			// si es un record valido o si no es valido pero no debe remover los invalidos,
			// lo indexa
			if (record.getStatus() == RecordStatus.VALID || indexDeletedRecords) {
				String recordStr = metadataTransformer.transformToString(metadata.getDOMDocument());
				stringBuffer.append(recordStr);
				logger.debug("Indexed record size: " + recordStr.length());
			}

		} catch (MDFormatTranformationException e) {

			logError("Index::RecordID:" + record.getId() + " oai_id:" + record.getIdentifier()
					+ " transformation Error xslt with the schema: " + targetSchemaName + " :: " + e.getMessage());
			logger.debug(e.getMessage(), e);
			//logger.debug("Record Metadata: \n" + metadata.toString() + "\n\n");
			error();
		} catch (OAIRecordMetadataParseException e) {
			logError("Index::RecordID:" + record.getId() + " oai_id:" + record.getIdentifier()
			+ " error getting record metadata: :: " + e.getMessage());
		} catch (MetadataRecordStoreException e) {
			logError("Index::RecordID:" + record.getId() + " oai_id:" + record.getIdentifier()
			+ " error getting record metadata: :: " + e.getMessage());
		}

	}

	@Override
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

	@Override
	public void postRun() {

		try {
			this.sendUpdateToSolr("<commit/>");

			if (executeIndexing)
				metadataStoreService.updateSnapshotIndexStatus(snapshotId, SnapshotIndexStatus.INDEXED);
			else
				metadataStoreService.updateSnapshotIndexStatus(snapshotId, SnapshotIndexStatus.UNKNOWN);
				
			metadataStoreService.saveSnapshot(snapshotId);

			logInfo("Finishing Indexing: "+ runningContext.toString() + "(" + this.targetSchemaName + ")");
			logInfo("Indexed documents in " + runningContext.getNetwork().getAcronym() + " == " + this.queryForNetworkDocumentCount( runningContext.getNetwork().getAcronym() ) );

			logger.debug("Updates snapshot status to " + SnapshotIndexStatus.INDEXED);

		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError("Issues when commiting to SOLR: " + runningContext.toString() + ": " + e.getMessage());
			error();
		}
	}

	/******************* Auxiliares ********** */
	private void error() {
		if ( snapshotId != null) {
			metadataStoreService.updateSnapshotIndexStatus(snapshotId, SnapshotIndexStatus.FAILED);
			metadataStoreService.saveSnapshot(snapshotId);
		}
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

	private void deleteRecord(String id, String fingerprint) {
		// Borrado de la red
		try {
			this.sendUpdateToSolr("<delete><query>" + solrRecordIDField + ":" + id + "</query></delete>");

		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError("Issues deleting record: " + id + " :: " + fingerprint + " network: "
			        + runningContext.toString() + ": "
					+ e.getMessage());
			error();
		}
	}

	private Long queryForNetworkDocumentCount(String networkAcronym) {

		try {
			return this.sendCountQueryToSolr(this.solrNetworkIDField + ":" + networkAcronym );

		} catch (Exception e) {
			logError("Issues when querying for network document count: " + runningContext.toString() + ": " + e.getMessage());
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

	private Long sendCountQueryToSolr(String queryString ) {
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
