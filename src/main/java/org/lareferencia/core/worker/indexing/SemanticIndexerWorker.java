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
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.lareferencia.core.api.semantic.EmbeddingRequest;
import org.lareferencia.core.api.semantic.EmbeddingResponse;
import org.lareferencia.core.api.semantic.SemanticVectorAPI;
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
import org.springframework.beans.factory.annotation.Value;
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

    public static final int EMBEDDING_SUPPPORTED_NUMBER_OF_ARRAYS = 1;
    public static final int MAX_EMBEDDING_TEXT_LENGTH = 8000;
    private static Logger logger = LogManager.getLogger(SemanticIndexerWorker.class);

    @Autowired
	private ISnapshotStore snapshotStore;

	@Autowired
	private IMetadataStore metadataStore;

	@Autowired
	private ValidationDatabaseManager dbManager;

	@Autowired
	private SnapshotLogService snapshotLogService;

	@Autowired
	private MDFormatTransformerService metadataTransformerService;

	@Autowired
	private SemanticVectorAPI semanticVectorAPI;

	private IMDFormatTransformer metadataTransformer;

	private HttpSolrClient solrClient;

	@Value("${frontend.solr.url}")
    private String solrURL;

	@Value("${embedding.model.name}")
	private String embeddingModelName;

	@Value("${embedding.model.datatype}")
	private String embeddingModelDataType;

	@Value("${embedding.model.dimension}")
	private int embeddingModelDimension;

	@Value("${embedding.model.applicationId}")
	private String embeddingApplicationId;

	private Long snapshotId;
	private SnapshotMetadata snapshotMetadata;
	private int recordCounter = 0;
	private int embeddedRecordsCount = 0;
	private int failedEmbeddingsCount = 0;
    private StringBuffer linesToBeSentToIndexing;
    private NumberFormat percentajeFormat = NumberFormat.getPercentInstance();

    @Setter @Getter private String targetSchemaName;
	@Setter @Getter private String solrNetworkIDField;
	@Setter @Getter private boolean executeDeletion = false;
	@Setter @Getter private boolean executeIndexing = false;
	@Setter @Getter private boolean indexNetworkAttributes = false;
	@Setter @Getter private Map<String, List<String>> contentFiltersByFieldName = null;
	@Setter @Getter private String embeddingApiUrl;
	@Setter @Getter private String sourceFieldForEmbedding = "dc.title.*";
	@Setter @Getter private String vectorFieldName;
	@Setter @Getter private int embeddingApiTimeoutSeconds = 30;
	@Setter @Getter private boolean skipOnEmbeddingFailure = true;

	public SemanticIndexerWorker() {
		super();
	}

	@PostConstruct
    public void init() {
        this.solrClient = new HttpSolrClient.Builder(solrURL).build();
    }

	@Override
	public void preRun() {
		if (isDeletionOnlyMode()) {
			handleDeletionOnly();
			return;
		}
	
		if (!initializeSnapshot()) {
			return;
		}
	
		if (!prepareForIndexing()) {
			return;
		}
	
		setupPaginator();
	}

	@Override
	public void prePage() {
		linesToBeSentToIndexing = new StringBuffer();
	}

	@Override
	public void processItem(ValidationRecord record) {

		try {
			OAIRecordMetadata metadata = new OAIRecordMetadata(record.getIdentifier(),
					metadataStore.getMetadata(snapshotMetadata, record.getPublishedMetadataHash()));
	
			if (!passesContentFilter(metadata)) {
				logger.debug(MessageFormat.format("Record does not pass content filter: {0}", record.getIdentifier()));
				return;
			}
	
			setTransformerParameters(record, metadata);
	
			if (record.isValid()) {
				String recordXml = metadataTransformer.transformToString(metadata.getDOMDocument());
				String enrichedXml = enrichRecordWithEmbedding(recordXml, metadata, record.getIdentifierHash());
	
				if (enrichedXml != null) {
					linesToBeSentToIndexing.append(enrichedXml);
					logger.debug(MessageFormat.format("Transformed record to be indexed: {0} :: {1}", record.getIdentifierHash(), record.getIdentifier()));
					logger.debug(MessageFormat.format("Indexed record size in bytes: {0}", enrichedXml.length()));
				}
			} else {
				logger.debug(MessageFormat.format("Record not indexed: {0} :: {1} :: {2}", record.getIdentifierHash(), record.getIdentifier(), record.isValid()));
			}
		} catch (MDFormatTranformationException e) {
			logError(MessageFormat.format("Index::RecordID:{0} oai_id:{1} transformation Error xslt with the schema: {2} :: {3}", record.getIdentifierHash(), record.getIdentifier(), targetSchemaName, e.getMessage()));
			logger.debug(e.getMessage(), e);
			error();
		} catch (OAIRecordMetadataParseException e) {
			logError(MessageFormat.format("Index::RecordID:{0} oai_id:{1} error getting record metadata: :: {2}", record.getIdentifierHash(), record.getIdentifier(), e.getMessage()));
		} catch (Exception e) {
			logError(MessageFormat.format("Index::RecordID:{0} oai_id:{1} error: :: {2}", record.getIdentifierHash(), record.getIdentifier(), e.getMessage()));
		}
	}

	@Override
	public void postPage() {

		if (!linesToBeSentToIndexing.isEmpty()) {
			try {
				this.sendUpdateToSolr(MessageFormat.format("<add>{0}</add>", linesToBeSentToIndexing));
			} catch (SolrServerException e) {
				logError(MessageFormat.format("Issues whe connecting to SOLR: {0}: {1}", runningContext.toString(), e.getMessage()));
				logger.debug(linesToBeSentToIndexing);
				solrRollback();
				error();

			} catch (IOException e) {
				logError(MessageFormat.format("Issues when sending to SOLR - I/O: {0}: {1}", runningContext.toString(), e.getMessage()));
				logger.debug(linesToBeSentToIndexing);
				solrRollback();
				error();

			} catch (Exception e) {
				logError(MessageFormat.format("Issues with the index process - Undetermined: {0}: {1}", runningContext.toString(), e.getMessage()));
				logger.debug(linesToBeSentToIndexing);
				solrRollback();
				error();
			}
		}
	}

	@Override
	public void postRun() {

		try {

			postPage();

			this.sendUpdateToSolr("<commit/>");

			if (executeIndexing)
				snapshotStore.markAsIndexed(snapshotId);

			logInfo(MessageFormat.format("Finishing Semantic Indexing: {0}({1})", runningContext.toString(), this.targetSchemaName));
			logInfo(MessageFormat.format("Indexed documents in {0}::{1} = {2}", runningContext.getNetwork().getAcronym(), this.targetSchemaName, this.queryForNetworkDocumentCount(runningContext.getNetwork().getAcronym())));
			logInfo(MessageFormat.format("Embedding stats - Success: {0} | Failed: {1}", embeddedRecordsCount, failedEmbeddingsCount));

			logger.debug(MessageFormat.format("Updates snapshot status to {0}", SnapshotIndexStatus.INDEXED));

		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError(MessageFormat.format("Issues when commiting to SOLR: {0}: {1}", runningContext.toString(), e.getMessage()));
			error();
		}
	}


	private boolean isDeletionOnlyMode() {
		return executeDeletion && !executeIndexing;
	}

	private void handleDeletionOnly() {
		logger.debug(MessageFormat.format("Executing index deletion (without indexing): {0}", runningContext.getNetwork().getAcronym()));
		delete(runningContext.getNetwork().getAcronym());
	}
	
	private boolean initializeSnapshot() {
		snapshotId = snapshotStore.findLastGoodKnownSnapshot(runningContext.getNetwork());
		if (snapshotId == null) {
			logError(MessageFormat.format("Didn't find LGKSnapshot on the network: {0}", runningContext.toString()));
			error();
			return false;
		}
		snapshotMetadata = snapshotStore.getSnapshotMetadata(snapshotId);
		return true;
	}
	
	private boolean prepareForIndexing() {
		logger.debug(MessageFormat.format("Executing index deletion: {0}", runningContext.getNetwork().getAcronym()));
		logInfo(MessageFormat.format("Executing index deletion: {0} ({1})", runningContext.toString(), this.targetSchemaName));
	
		logger.debug(MessageFormat.format("Full semantic indexing ({0}): {1}", this.targetSchemaName, snapshotId));
		logInfo(MessageFormat.format("Full semantic indexing: {0}({1})", runningContext.toString(), this.targetSchemaName));
		logInfo(MessageFormat.format("Embedding API: {0} | Source field: {1} | Vector field: {2}", embeddingApiUrl, sourceFieldForEmbedding, vectorFieldName));
	
		return initializeTransformer();
	}
	
	private boolean initializeTransformer() {
		try {
			metadataTransformer = metadataTransformerService.getMDTransformer(runningContext.getNetwork().getMetadataStoreSchema(), targetSchemaName);
			metadataTransformer.setParameter("networkAcronym", runningContext.getNetwork().getAcronym());
			metadataTransformer.setParameter("networkName", runningContext.getNetwork().getName());
			metadataTransformer.setParameter("institutionName", runningContext.getNetwork().getInstitutionName());
			metadataTransformer.setParameter("institutionAcronym", runningContext.getNetwork().getInstitutionAcronym());
	
			if (indexNetworkAttributes) {
				MDTransformerParameterSetter.setParametersFromMap(metadataTransformer, "attr_", runningContext.getNetwork().getAttributes());
			}
			return true;
		} catch (MDFormatTranformationException e) {
			logError(MessageFormat.format("Metadata transformation configuration ERROR at indexing: {0} {1} >> {2} error: {3}", runningContext.toString(), runningContext.getNetwork().getMetadataStoreSchema(), targetSchemaName, e.getMessage()));
			error();
			return false;
		}
	}
	
	private void setupPaginator() {
		ValidationRecordPaginator paginator = new ValidationRecordPaginator(snapshotMetadata, dbManager);
		paginator.setPageSize(getPageSize());
		this.setPaginator(paginator);
	}
	
	private boolean passesContentFilter(OAIRecordMetadata metadata) {
		if (contentFiltersByFieldName == null) {
			return true;
		}
	
		logger.debug(MessageFormat.format("Evaluating record content: {0}", metadata.getIdentifier()));
	
		for (Map.Entry<String, List<String>> entry : contentFiltersByFieldName.entrySet()) {
			String fieldName = entry.getKey();
			List<String> validValues = entry.getValue();
			boolean fieldHasValidValue = false;
	
			for (String occurrence : metadata.getFieldOcurrences(fieldName)) {
				if (validValues.contains(occurrence)) {
					fieldHasValidValue = true;
					break;
				}
			}
	
			if (!fieldHasValidValue) {
				logger.debug(MessageFormat.format("Record fails filter on field: {0}", fieldName));
				return false;
			}
		}
		return true;
	}
	
	private void setTransformerParameters(ValidationRecord record, OAIRecordMetadata metadata) {
		metadataTransformer.setParameter("fingerprint", MessageFormat.format("{0}_{1}", runningContext.getNetwork().getAcronym(), record.getIdentifierHash()));
		metadataTransformer.setParameter("identifier", record.getIdentifier());
		metadataTransformer.setParameter("record_id", generateRecordUniqueID(snapshotId).toString());
	
		if (record.getDatestamp() != null) {
			metadataTransformer.setParameter("timestamp", DateHelper.getDateTimeMachineString(record.getDatestamp()));
		}
	
		metadataTransformer.setParameter("deleted", Boolean.valueOf(!record.isValid()).toString());
		metadataTransformer.setParameter("metadata", metadata.toString());
	}

	private String enrichRecordWithEmbedding(String recordXml, OAIRecordMetadata metadata, String recordIdentifier) {
		String textForEmbedding = extractTextForEmbedding(metadata);
		if (textForEmbedding == null || textForEmbedding.trim().isEmpty()) {
			logger.debug(MessageFormat.format("No text found for embedding in record: {0}", recordIdentifier));
			return recordXml;
		}

		try {
			EmbeddingResponse response = semanticVectorAPI.generateEmbedding(
                    new EmbeddingRequest(textForEmbedding,
                            embeddingModelName,
                            embeddingModelDataType,
                            embeddingModelDimension,
                            embeddingApplicationId));

			if (response != null && response.getData() != null && response.getData().size() == EMBEDDING_SUPPPORTED_NUMBER_OF_ARRAYS) {
				embeddedRecordsCount++;
				logger.debug(MessageFormat.format("Generated embedding for record: {0} | Text length: {1} | Vector dimension: {2}", recordIdentifier, textForEmbedding.length(), response.getData().size()));

                List<Float> embeddingAslist = response.getData().get(0).getEmbedding();
                Float[] floatObjectArray = embeddingAslist.toArray(new Float[0]);

				return injectVectorField(recordXml, floatObjectArray);
			}
		} catch (Exception e) {
			failedEmbeddingsCount++;
			logger.error(MessageFormat.format("Failed to generate embedding for record: {0} | Error: {1}", recordIdentifier, e.getMessage()));
			if (!skipOnEmbeddingFailure) {
				return null; // Signal to skip the record
			}
		}
		return recordXml;
	}

	/**
	 * Generates a unique record ID by combining snapshot ID with counter.
	 * Uses bit shifting: snapshotId in bits 27-62, counter in bits 0-26.
	 * NOTE: Counter must be incremented before calling this method.
	 * 
	 * @param snapshotId the snapshot ID
	 * @return unique Long ID for the record
	 */
	private Long generateRecordUniqueID(Long snapshotId) {
		return (snapshotId << 27) | (++recordCounter & 0x7FFFFFFFL);
	}

	private void error() {
		this.stop();
	}

	private void logError(String message) {
		logger.error(message);
		snapshotLogService.addEntry(snapshotId, MessageFormat.format("ERROR: {0}", message));
	}

	private void logInfo(String message) {
		logger.info(message);
		snapshotLogService.addEntry(snapshotId, MessageFormat.format("INFO: {0}", message));
	}

	private void delete(String networkAcronym) {
		// Borrado de la red
		try {
			this.sendUpdateToSolr(
					MessageFormat.format("<delete><query>{0}:{1}</query></delete>", this.solrNetworkIDField, networkAcronym));
		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError(MessageFormat.format("Issues when deleting index: {0}: {1}", runningContext.toString(), e.getMessage()));
			error();
		}
	}

	private Long queryForNetworkDocumentCount(String networkAcronym) {

		try {
			return this.sendCountQueryToSolr(MessageFormat.format("{0}:{1}", this.solrNetworkIDField, networkAcronym));

		} catch (Exception e) {
			logError(MessageFormat.format("Issues when querying for network document count: {0}: {1}", runningContext.toString(), e.getMessage()));
			error();
		}
		return 0L;
	}

	private void solrRollback() {

		try {
			this.sendUpdateToSolr("<rollback/>");
		} catch (SolrServerException | IOException | HttpSolrClient.RemoteSolrException e) {
			logError(MessageFormat.format("Issues with rollback  {0}: {1}", runningContext.toString(), e.getMessage()));
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
			logError(MessageFormat.format("Issues with query  {0}: {1}", runningContext.toString(), e.getMessage()));
			error();
		}
		return 0L;
	}

	@Override
	public String toString() {
		return MessageFormat.format("SemanticIndexer[{0}{1}]({2})", ((executeDeletion && !executeIndexing) ? "Delete:" : ""), targetSchemaName, percentajeFormat.format(this.getCompletionRate()));
	}
	

	/**
	 * Extracts text from the configured source field for embedding generation.
	 * Concatenates multiple occurrences with spaces.
	 * 
	 * @param metadata the OAI record metadata
	 * @return concatenated text from source field, or null if empty
	 */
    private String extractTextForEmbedding(OAIRecordMetadata metadata) {
        String text = Arrays.stream(sourceFieldForEmbedding.split(","))
                .map(String::trim)
                .map(metadata::getFieldOcurrences)
                .filter(Objects::nonNull)
                .flatMap(List::stream)
                .filter(metadataValue -> metadataValue != null && !metadataValue.trim().isEmpty())
                .collect(Collectors.joining(" "));

        if (text.length() <= MAX_EMBEDDING_TEXT_LENGTH) {
            return text;
        }

        // If it exceeds the limit, truncate to the limit and find the last space to avoid cutting a word
        String truncated = text.substring(0, MAX_EMBEDDING_TEXT_LENGTH);
        int lastSpaceIndex = truncated.lastIndexOf(' ');

        if (lastSpaceIndex != -1) {
            return truncated.substring(0, lastSpaceIndex).trim();
        }

        return truncated;
    }
	
	/**
	 * Injects the vector field into the Solr document XML.
	 * Inserts a new field element before the closing </doc> tag.
	 * 
	 * @param recordXml the Solr document XML string
	 * @param embedding the embedding vector to inject
	 * @return the modified XML with the vector field
	 */
	private String injectVectorField(String recordXml, Float[] embedding) {
        String vectorFieldXml = Arrays.stream(embedding).map(vectorValue ->
                MessageFormat.format("<field name=\"{0}\">{1}</field>", vectorFieldName, String.format(Locale.US, "%f", vectorValue))).collect(Collectors.joining());

		int closeDocIndex = recordXml.lastIndexOf("</doc>");
		if (closeDocIndex != -1) {
			return new StringBuilder(recordXml).insert(closeDocIndex, vectorFieldXml).toString();
		}
		
		logger.warn("Could not find </doc> tag in record XML, appending vector field at end");
		return new StringBuilder(recordXml).append(vectorFieldXml).toString();
	}

}
