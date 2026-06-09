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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.embedding.IEmbeddingService;
import org.lareferencia.core.embedding.chunks.ChunkingService;
import org.lareferencia.core.metadata.IMDFormatTransformer;
import org.lareferencia.core.metadata.IMetadataStore;
import org.lareferencia.core.metadata.ISnapshotStore;
import org.lareferencia.core.metadata.MDFormatTranformationException;
import org.lareferencia.core.metadata.MDFormatTransformerService;
import org.lareferencia.core.metadata.MDTransformerParameterSetter;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.metadata.OAIRecordMetadataParseException;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.repository.validation.ValidationDatabaseManager;
import org.lareferencia.core.repository.validation.ValidationRecord;
import org.lareferencia.core.repository.validation.ValidationRecordPaginator;
import org.lareferencia.core.service.management.SnapshotLogService;
import org.lareferencia.core.util.date.DateHelper;
import org.lareferencia.core.worker.BaseBatchWorker;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

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
	private IEmbeddingService embeddingService;

	@Autowired
	private ChunkingService chunkingService;

	private IMDFormatTransformer metadataTransformer;

	private HttpSolrClient solrClient;

	@Value("${frontend.solr.url}")
	private String solrURL;

	private Long snapshotId;
	private SnapshotMetadata snapshotMetadata;
	private int recordCounter = 0;
	private int embeddedRecordsCount = 0;
	private int emptyRecordsCount = 0;
	private int failedEmbeddingsCount = 0;
	private List<SolrInputDocument> documentsToBeIndexed;
	private NumberFormat percentajeFormat = NumberFormat.getPercentInstance();

	@Setter
	@Getter
	private String targetSchemaName;
	@Setter
	@Getter
	private String solrNetworkIDField;
	@Setter
	@Getter
	private boolean executeDeletion = false;
	@Setter
	@Getter
	private boolean executeIndexing = false;
	@Setter
	@Getter
	private boolean indexNetworkAttributes = false;
	@Setter
	@Getter
	private Map<String, List<String>> contentFiltersByFieldName = null;
	@Setter
	@Getter
	@Value("${embedding.api.url}")
	private String embeddingApiUrl;	@Setter
	@Getter
	@Value("${embedding.model.name}")
	private String embeddingModel;
	@Setter
	@Getter
	@Value("${embedding.title.field:dc.title.*}")
	private String titleFieldForEmbedding;
	@Setter
	@Getter
	@Value("${embedding.abstract.field:dc.description.*}")
	private String abstractFieldForEmbedding;
	@Setter
	@Getter
	@Value("${embedding.vector.field.name:vector_multivalued}")
	private String vectorFieldName;
	@Value("${embedding.use.multivalued.vector:false}")
	@Setter
	private boolean useMultiValuedVector;
    @Value("${embedding.title.standalone.indexing.min.words:5}")
    @Setter
    private int minTitleWordsForEmbedding;

	public SemanticIndexerWorker() {
		super();
	}

	@PostConstruct
	public void init() {
		this.solrClient = new HttpSolrClient.Builder(solrURL).build();
		logger.info(MessageFormat.format("SemanticIndexerWorker initialized with expected dimension: {0}",
				embeddingService.getEmbeddingDimension()));
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
		documentsToBeIndexed = new ArrayList<>();
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
				Document transformedDoc = metadataTransformer.transform(metadata.getDOMDocument());
				SolrInputDocument solrDoc = xmlDocumentToSolrInputDocument(transformedDoc);

				enrichRecordWithEmbedding(solrDoc, metadata);
				documentsToBeIndexed.add(solrDoc);
				logger.debug(MessageFormat.format("Transformed record to be indexed: {0} :: {1}", record.getIdentifierHash(),
						record.getIdentifier()));

			} else {
				logger.debug(MessageFormat.format("Record not indexed: {0} :: {1} :: {2}", record.getIdentifierHash(),
						record.getIdentifier(), record.isValid()));
			}
		} catch (MDFormatTranformationException e) {
			logError(
					MessageFormat.format("Index::RecordID:{0} oai_id:{1} transformation Error xslt with the schema: {2} :: {3}",
							record.getIdentifierHash(), record.getIdentifier(), targetSchemaName, e.getMessage()));
			logger.debug(e.getMessage(), e);
			error();
		} catch (OAIRecordMetadataParseException e) {
			logError(MessageFormat.format("Index::RecordID:{0} oai_id:{1} error getting record metadata: :: {2}",
					record.getIdentifierHash(), record.getIdentifier(), e.getMessage()));
		} catch (Exception e) {
			logError(MessageFormat.format("Index::RecordID:{0} oai_id:{1} error: :: {2}", record.getIdentifierHash(),
					record.getIdentifier(), e.getMessage()));
		}
	}

	@Override
	public void postPage() {

		if (documentsToBeIndexed != null && !documentsToBeIndexed.isEmpty()) {
			try {
				solrClient.add(documentsToBeIndexed);
			} catch (SolrServerException e) {
				logError(
						MessageFormat.format("Issues whe connecting to SOLR: {0}: {1}", runningContext.toString(), e.getMessage()));
				solrRollback();
				error();

			} catch (IOException e) {
				logError(MessageFormat.format("Issues when sending to SOLR - I/O: {0}: {1}", runningContext.toString(),
						e.getMessage()));
				solrRollback();
				error();

			} catch (Exception e) {
				logError(MessageFormat.format("Issues with the index process - Undetermined: {0}: {1}",
						runningContext.toString(), e.getMessage()));
				solrRollback();
				error();
			}
		}
	}

	@Override
	public void postRun() {

		try {

			postPage();

			solrClient.commit();

			if (executeIndexing)
				snapshotStore.markAsIndexed(snapshotId);

			logInfo(MessageFormat.format("Finishing Semantic Indexing: {0}({1})", runningContext.toString(),
					this.targetSchemaName));
			logInfo(MessageFormat.format("Indexed documents in {0}::{1} = {2}", runningContext.getNetwork().getAcronym(),
					this.targetSchemaName, this.queryForNetworkDocumentCount(runningContext.getNetwork().getAcronym())));
			logInfo(MessageFormat.format("Embedding stats: Success: {0} | Empty: {1} | Failed: {2}",
					(embeddedRecordsCount - (failedEmbeddingsCount + emptyRecordsCount)), emptyRecordsCount,
					failedEmbeddingsCount));

			logger.debug(MessageFormat.format("Updates snapshot status to {0}", SnapshotIndexStatus.INDEXED));

		} catch (SolrServerException | IOException e) {
			logError(
					MessageFormat.format("Issues when commiting to SOLR: {0}: {1}", runningContext.toString(), e.getMessage()));
			error();
		}
	}

	private boolean isDeletionOnlyMode() {
		return executeDeletion && !executeIndexing;
	}

	private void handleDeletionOnly() {
		logger.debug(MessageFormat.format("Executing index deletion (without indexing): {0}",
				runningContext.getNetwork().getAcronym()));
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
		logInfo(
				MessageFormat.format("Executing index deletion: {0} ({1})", runningContext.toString(), this.targetSchemaName));

		logger.debug(MessageFormat.format("Full semantic indexing ({0}): {1}", this.targetSchemaName, snapshotId));
		logInfo(MessageFormat.format("Full semantic indexing: {0}({1})", runningContext.toString(), this.targetSchemaName));
		logInfo(MessageFormat.format("Embedding API: {0} | Model: {1} | Title field: {2} | Abstract field: {3} | Vector field: {4} | MultiValued vector: {5}",
				embeddingApiUrl, embeddingModel, titleFieldForEmbedding, abstractFieldForEmbedding, vectorFieldName, useMultiValuedVector));

		return initializeTransformer();
	}

	private boolean initializeTransformer() {
		try {
			metadataTransformer = metadataTransformerService
					.getMDTransformer(runningContext.getNetwork().getMetadataStoreSchema(), targetSchemaName);
			metadataTransformer.setParameter("networkAcronym", runningContext.getNetwork().getAcronym());
			metadataTransformer.setParameter("networkName", runningContext.getNetwork().getName());
			metadataTransformer.setParameter("institutionName", runningContext.getNetwork().getInstitutionName());
			metadataTransformer.setParameter("institutionAcronym", runningContext.getNetwork().getInstitutionAcronym());

			if (indexNetworkAttributes) {
				MDTransformerParameterSetter.setParametersFromMap(metadataTransformer, "attr_",
						runningContext.getNetwork().getAttributes());
			}
			return true;
		} catch (MDFormatTranformationException e) {
			logError(
					MessageFormat.format("Metadata transformation configuration ERROR at indexing: {0} {1} >> {2} error: {3}",
							runningContext.toString(), runningContext.getNetwork().getMetadataStoreSchema(), targetSchemaName,
							e.getMessage()));
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
		metadataTransformer.setParameter("fingerprint",
				MessageFormat.format("{0}_{1}", runningContext.getNetwork().getAcronym(), record.getIdentifierHash()));
		metadataTransformer.setParameter("identifier", record.getIdentifier());
		metadataTransformer.setParameter("record_id", generateRecordUniqueID(snapshotId).toString());

		if (record.getDatestamp() != null) {
			metadataTransformer.setParameter("timestamp", DateHelper.getDateTimeMachineString(record.getDatestamp()));
		}

		metadataTransformer.setParameter("deleted", Boolean.valueOf(!record.isValid()).toString());
		metadataTransformer.setParameter("metadata", metadata.toString());
	}

	/**
	 * Solr DenseVector multivalued fields expect an array of vectors [[...], [...]]
	 * while single-valued fields expect a single vector [...].
	 */
	private void enrichRecordWithEmbedding(SolrInputDocument recordDoc, OAIRecordMetadata metadata) {
		String title = extractMetadataValue(metadata, titleFieldForEmbedding);
		if (title.isBlank()) {
            emptyRecordsCount++;
			return;
		}

        if (useMultiValuedVector) {
            String abstractText = extractMetadataValue(metadata, abstractFieldForEmbedding);
            List<String> textsToEmbedding = new ArrayList<>();
            int titleWordCount = title.trim().split("\\s+").length;
            if (titleWordCount < minTitleWordsForEmbedding) {
                logger.info(MessageFormat.format(
                        "Title: {0}, has {1} words, which is below the minimum threshold of {2} for standalone embedding. Skipping embedding for this record: {3}",
                        title, titleWordCount, minTitleWordsForEmbedding, metadata.getIdentifier()));
            } else {
                textsToEmbedding.add(title);
            }

			textsToEmbedding.addAll(chunkingService.chunkTitleAndAbstract(title, abstractText));

            if (textsToEmbedding.isEmpty()){
                emptyRecordsCount++;
                return;
            }

			embeddingService.embed(textsToEmbedding)
					.filter(vectors -> !vectors.isEmpty())
					.ifPresentOrElse(
							vectors -> recordDoc.setField(vectorFieldName, vectors),
							() -> logEmbeddingFailure(title));
		} else {
			embeddingService.embed(chunkingService.normalizeText(title))
					.filter(vector -> !vector.isEmpty())
					.ifPresentOrElse(
							vector -> recordDoc.setField(vectorFieldName, vector),
							() -> logEmbeddingFailure(title));
		}
		embeddedRecordsCount++;
	}

	private void logEmbeddingFailure(String title) {
		failedEmbeddingsCount++;
		logger.warn(MessageFormat.format("Failed to generate embedding for record: {0}", title));
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
		try {
			solrClient.deleteByQuery(MessageFormat.format("{0}:{1}", this.solrNetworkIDField, networkAcronym));
		} catch (SolrServerException | IOException e) {
			logError(MessageFormat.format("Issues when deleting index: {0}: {1}", runningContext.toString(), e.getMessage()));
			error();
		}
	}

	private Long queryForNetworkDocumentCount(String networkAcronym) {

		try {
			return this.sendCountQueryToSolr(MessageFormat.format("{0}:{1}", this.solrNetworkIDField, networkAcronym));

		} catch (Exception e) {
			logError(MessageFormat.format("Issues when querying for network document count: {0}: {1}",
					runningContext.toString(), e.getMessage()));
			error();
		}
		return 0L;
	}

	private void solrRollback() {

		try {
			solrClient.rollback();
		} catch (SolrServerException | IOException e) {
			logError(MessageFormat.format("Issues with rollback  {0}: {1}", runningContext.toString(), e.getMessage()));
			error();
		}
	}

	private Long sendCountQueryToSolr(String queryString) {
		try {
			SolrQuery query = new SolrQuery();
			query.setQuery(queryString);
			query.setRows(0);

			return solrClient.query(query).getResults().getNumFound();

		} catch (SolrServerException | IOException e) {
			logError(MessageFormat.format("Issues with query  {0}: {1}", runningContext.toString(), e.getMessage()));
			error();
		}
		return 0L;
	}

	@Override
	public String toString() {
		return MessageFormat.format("SemanticIndexer[{0}{1}]({2})",
				((executeDeletion && !executeIndexing) ? "Delete:" : ""), targetSchemaName,
				percentajeFormat.format(this.getCompletionRate()));
	}

	/**
	 * Extracts text from the configured source field for embedding generation.
	 * Concatenates multiple occurrences with spaces.
	 *
	 * @param metadata the OAI record metadata
	 * @return concatenated text from source field, or null if empty
	 */
	private String extractMetadataValue(OAIRecordMetadata metadata, String metadataField) {
		List<String> occurrences = metadata.getFieldOcurrences(metadataField);
		if (occurrences == null || occurrences.isEmpty()) {
			return "";
		}

		return occurrences.stream()
				.filter(Objects::nonNull)
				.filter(metadataValue -> !metadataValue.trim().isEmpty())
				.collect(Collectors.joining(" "));

    }

	private SolrInputDocument xmlDocumentToSolrInputDocument(Document doc) {
		SolrInputDocument solrDoc = new SolrInputDocument();
		NodeList fields = doc.getElementsByTagName("field");
		for (int i = 0; i < fields.getLength(); i++) {
			Element field = (Element) fields.item(i);
			String name = field.getAttribute("name");
			String value = field.getTextContent();
			solrDoc.addField(name, value);
		}
		return solrDoc;
	}

}
