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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.repository.validation.ValidationRecord;
import org.lareferencia.core.repository.validation.ValidationRecordPaginator;
import org.lareferencia.core.repository.validation.ValidationDatabaseManager;
import org.lareferencia.core.service.management.SnapshotLogService;
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
import org.lareferencia.core.worker.BaseBatchWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.Setter;

/**
 * Worker that indexes harvested records into Solr with semantic enrichment.
 * <p>
 * Transforms and indexes validated records with optional deletion support.
 * This is a separate implementation for semantic indexing purposes.
 * </p>
 * 
 * @author LA Referencia Team
 */
@Component("semanticIndexerWorkerFlowable")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class SemanticIndexerWorker extends BaseBatchWorker<ValidationRecord, NetworkRunningContext> {

	@Autowired
	private ISnapshotStore snapshotStore;

	@Autowired
	private IMetadataStore metadataStore;

	@Autowired
	private ValidationDatabaseManager dbManager;

	private static Logger logger = LogManager.getLogger(SemanticIndexerWorker.class);

	@Autowired
	private SnapshotLogService snapshotLogService;

	@Autowired
	MDFormatTransformerService trfService;

	private IMDFormatTransformer metadataTransformer;

	private HttpSolrClient solrClient;
	
	// HTTP client for embedding API calls
	private HttpClient httpClient;
	private ObjectMapper objectMapper;

	private Long snapshotId;
	private SnapshotMetadata snapshotMetadata;

	// Counter for generating unique record IDs (incremented in processItem)
	private int recordCounter = 0;
	
	// Counters for embedding statistics
	private int embeddedRecordsCount = 0;
	private int failedEmbeddingsCount = 0;

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
	
	// ==================== Semantic Indexing Parameters ====================
	
	/**
	 * URL of the embedding API endpoint.
	 * Expected API format: POST with JSON body {"text": "..."} 
	 * Returns: {"embedding": [0.1, 0.2, ...]} (768-dimensional vector)
	 */
	@Getter
	@Setter
	private String embeddingApiUrl;
	
	/**
	 * Metadata field pattern to extract text for embedding generation.
	 * Supports wildcards, e.g., "dc.title.*" to get all title variations.
	 * Default: "dc.title.*"
	 */
	@Getter
	@Setter
	private String sourceFieldForEmbedding = "dc.title.*";
	
	/**
	 * Name of the Solr field where the embedding vector will be stored.
	 * Must be configured as DenseVectorField in Solr schema.
	 * Default: "semantic_vector"
	 */
	@Getter
	@Setter
	private String vectorFieldName = "semantic_vector";
	
	/**
	 * Timeout in seconds for embedding API calls.
	 * Default: 30 seconds
	 */
	@Getter
	@Setter
	private int embeddingApiTimeoutSeconds = 30;
	
	/**
	 * Whether to skip records that fail embedding generation.
	 * If true, records without embeddings are still indexed (without vector).
	 * If false, embedding failures cause the record to be skipped entirely.
	 * Default: true
	 */
	@Getter
	@Setter
	private boolean skipOnEmbeddingFailure = true;

	private StringBuffer stringBuffer;

	NumberFormat percentajeFormat = NumberFormat.getPercentInstance();

	/**
	 * Creates a semantic indexer worker with the specified Solr URL.
	 * 
	 * @param solrURL the Solr server URL
	 */
	public SemanticIndexerWorker(String solrURL) {
		super();
		this.solrClient = new HttpSolrClient.Builder(solrURL).build();
		this.httpClient = HttpClient.newBuilder()
				.connectTimeout(Duration.ofSeconds(10))
				.build();
		this.objectMapper = new ObjectMapper();
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

				logger.debug("Executing index deletion: " + runningContext.getNetwork().getAcronym());
				logInfo("Executing index deletion: " + runningContext.toString() + " (" + this.targetSchemaName + ")");
				delete(runningContext.getNetwork().getAcronym());

				logger.debug("Full semantic indexing (" + this.targetSchemaName + "): " + snapshotId);
				logInfo("Full semantic indexing: " + runningContext.toString() + "(" + this.targetSchemaName + ")");
				logInfo("Embedding API: " + embeddingApiUrl + " | Source field: " + sourceFieldForEmbedding + " | Vector field: " + vectorFieldName);

				// establece el transformador para indexación
				try {
					// Create paginator for validation records
					ValidationRecordPaginator paginator = new ValidationRecordPaginator(
							snapshotMetadata, dbManager);
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

		// Increment counter for unique ID generation
		recordCounter++;

		try {

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

			// record_id: use snapshot + counter for unique integer ID
			metadataTransformer.setParameter("record_id", generateRecordUniqueID(snapshotId).toString());

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
				
				// ==================== Semantic Embedding Generation ====================
				String textForEmbedding = extractTextForEmbedding(metadata);
				float[] embedding = null;
				
				if (textForEmbedding != null && !textForEmbedding.trim().isEmpty()) {
					try {
						embedding = generateEmbedding(textForEmbedding);
						if (embedding != null) {
							embeddedRecordsCount++;
							logger.debug("Generated embedding for record: " + record.getIdentifierHash() + 
									" | Text length: " + textForEmbedding.length() + 
									" | Vector dimension: " + embedding.length);
						}
					} catch (Exception e) {
						failedEmbeddingsCount++;
						logger.warn("Failed to generate embedding for record: " + record.getIdentifierHash() + 
								" | Error: " + e.getMessage());
						if (!skipOnEmbeddingFailure) {
							return; // Skip this record entirely
						}
					}
				} else {
					logger.debug("No text found for embedding in record: " + record.getIdentifierHash());
				}
				
				// Inject the vector field into the Solr document XML
				if (embedding != null) {
					recordStr = injectVectorField(recordStr, embedding);
				}
				// ==================== End Semantic Embedding ====================
				
				stringBuffer.append(recordStr);

				logger.debug("Transformed record to be indexed: " + record.getIdentifierHash() + " :: "
						+ record.getIdentifier());
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

			logInfo("Finishing Semantic Indexing: " + runningContext.toString() + "(" + this.targetSchemaName + ")");
			logInfo("Indexed documents in " + runningContext.getNetwork().getAcronym() + "::" + this.targetSchemaName
					+ " = " + this.queryForNetworkDocumentCount(runningContext.getNetwork().getAcronym()));
			logInfo("Embedding stats - Success: " + embeddedRecordsCount + " | Failed: " + failedEmbeddingsCount);

			logger.debug("Updates snapshot status to " + SnapshotIndexStatus.INDEXED);

		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError("Issues when commiting to SOLR: " + runningContext.toString() + ": " + e.getMessage());
			error();
		}
	}

	/******************* Auxiliares ********** */
	/**
	 * Generates a unique record ID by combining snapshot ID with counter.
	 * Uses bit shifting: snapshotId in bits 27-62, counter in bits 0-26.
	 * NOTE: Counter must be incremented before calling this method.
	 * 
	 * @param snapshotId the snapshot ID
	 * @return unique Long ID for the record
	 */
	private Long generateRecordUniqueID(Long snapshotId) {
		return (snapshotId << 27) | (recordCounter & 0x7FFFFFFFL);
	}

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
		return "SemanticIndexer[" + ((executeDeletion && !executeIndexing) ? "Delete:" : "") + targetSchemaName + "]("
				+ percentajeFormat.format(this.getCompletionRate()) + ")";
	}
	
	// ==================== Embedding Helper Methods ====================
	
	/**
	 * Extracts text from the configured source field for embedding generation.
	 * Concatenates multiple occurrences with spaces.
	 * 
	 * @param metadata the OAI record metadata
	 * @return concatenated text from source field, or null if empty
	 */
	private String extractTextForEmbedding(OAIRecordMetadata metadata) {
		List<String> occurrences = metadata.getFieldOcurrences(sourceFieldForEmbedding);
		if (occurrences == null || occurrences.isEmpty()) {
			return null;
		}
		return occurrences.stream()
				.filter(s -> s != null && !s.trim().isEmpty())
				.collect(Collectors.joining(" "));
	}
	
	/**
	 * Calls the embedding API to generate a vector for the given text.
	 * 
	 * Expected API contract:
	 * - Request: POST with JSON body {"text": "..."}
	 * - Response: JSON with {"embedding": [float, float, ...]}
	 * 
	 * @param text the text to embed
	 * @return the embedding vector (768 dimensions), or null on failure
	 * @throws IOException if API call fails
	 * @throws InterruptedException if API call is interrupted
	 */
	private float[] generateEmbedding(String text) throws IOException, InterruptedException {
		if (embeddingApiUrl == null || embeddingApiUrl.trim().isEmpty()) {
			logger.warn("Embedding API URL not configured, skipping embedding generation");
			return null;
		}
		
		// Prepare JSON request body
		String requestBody = objectMapper.writeValueAsString(Map.of("text", text));
		
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create(embeddingApiUrl))
				.header("Content-Type", "application/json")
				.timeout(Duration.ofSeconds(embeddingApiTimeoutSeconds))
				.POST(HttpRequest.BodyPublishers.ofString(requestBody))
				.build();
		
		HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
		
		if (response.statusCode() != 200) {
			throw new IOException("Embedding API returned status " + response.statusCode() + ": " + response.body());
		}
		
		// Parse response
		JsonNode jsonResponse = objectMapper.readTree(response.body());
		JsonNode embeddingNode = jsonResponse.get("embedding");
		
		if (embeddingNode == null || !embeddingNode.isArray()) {
			throw new IOException("Invalid embedding API response: missing 'embedding' array");
		}
		
		float[] embedding = new float[embeddingNode.size()];
		for (int i = 0; i < embeddingNode.size(); i++) {
			embedding[i] = (float) embeddingNode.get(i).asDouble();
		}
		
		return embedding;
	}
	
	/**
	 * Injects the vector field into the Solr document XML.
	 * Inserts a new field element before the closing </doc> tag.
	 * 
	 * @param recordXml the Solr document XML string
	 * @param embedding the embedding vector to inject
	 * @return the modified XML with the vector field
	 */
	private String injectVectorField(String recordXml, float[] embedding) {
		// Convert float array to Solr-compatible string format: [0.1, 0.2, ...]
		String vectorString = Arrays.toString(embedding);
		
		// Create the vector field XML element
		String vectorFieldXml = "<field name=\"" + vectorFieldName + "\">" + vectorString + "</field>";
		
		// Insert before the closing </doc> tag
		int closeDocIndex = recordXml.lastIndexOf("</doc>");
		if (closeDocIndex != -1) {
			return recordXml.substring(0, closeDocIndex) + vectorFieldXml + recordXml.substring(closeDocIndex);
		}
		
		// Fallback: if no </doc> found, append at the end
		logger.warn("Could not find </doc> tag in record XML, appending vector field at end");
		return recordXml + vectorFieldXml;
	}

}
