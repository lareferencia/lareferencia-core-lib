package org.lareferencia.core.worker.indexing;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.lareferencia.core.metadata.IMDFormatTransformer;
import org.lareferencia.core.metadata.MDFormatTranformationException;
import org.lareferencia.core.metadata.OAIRecordMetadata;
import org.lareferencia.core.repository.validation.ValidationRecord;
import org.lareferencia.core.util.PathUtils;
import org.springframework.beans.factory.annotation.Value;

/** Frontend indexer that adds one precomputed vector to each matching bibliographic document. */
public class SQLiteVectorIndexerWorker extends IndexerWorker {
    @Value("${store.basepath:/tmp/data/}") private String storeBase;
    @Setter private String embeddingsDirectory = "embeddings";
    @Setter private String sqliteFilename = "";
    @Setter private String vectorFieldName = "semantic_vector";
    private final List<SolrInputDocument> documents = new ArrayList<>();
    private SQLiteVectorReader reader;
    private long withVector;
    private long withoutVector;

    public SQLiteVectorIndexerWorker(String solrUrl) {
        super(solrUrl);
    }

    @Override
    protected void prepareIndex() {
        String network = runningContext.getNetwork().getAcronym();
        Path root = Path.of(storeBase, PathUtils.sanitizeNetworkAcronym(network)).toAbsolutePath().normalize();
        Path directory = root.resolve(embeddingsDirectory).normalize();
        if (!directory.startsWith(root) || directory.equals(root)) {
            throw new IllegalArgumentException("Embedding directory must be inside " + root);
        }
        try {
            reader = new SQLiteVectorReader(directory, sqliteFilename, network);
            var options = new ModifiableSolrParams().set("showDefaults", true);
            var field = new SchemaRequest.Field(vectorFieldName, options).process(solrClient).getField();
            var type = new SchemaRequest.FieldType(field.get("type").toString()).process(solrClient)
                    .getFieldType().getAttributes();
            if (!"solr.DenseVectorField".equals(type.get("class"))
                    || !Boolean.parseBoolean(String.valueOf(field.get("indexed")))
                    || Boolean.parseBoolean(String.valueOf(field.get("multiValued")))
                    || !Integer.toString(reader.dimensions).equals(String.valueOf(type.get("vectorDimension")))) {
                throw new IOException("Expected indexed, single-valued DenseVectorField of dimension "
                        + reader.dimensions + " for " + vectorFieldName);
            }
        } catch (IOException | SQLException | SolrServerException e) {
            throw new IllegalStateException("Cannot index " + network + " using " + directory + ": " + e.getMessage(), e);
        }
    }

    @Override
    protected void queueDocument(ValidationRecord record, OAIRecordMetadata metadata,
            IMDFormatTransformer transformer) throws MDFormatTranformationException, IOException, SQLException {
        var doc = SolrDocuments.fromXml(transformer.transform(metadata.getDOMDocument()));
        try {
            var vector = reader.find(record.getIdentifierHash());
            if (vector.isEmpty()) {
                withoutVector++;
            } else {
                doc.setField(vectorFieldName, vector);
                withVector++;
            }
        } catch (IOException | SQLException e) {
            stop();
            throw e;
        }
        documents.add(doc);
    }

    @Override
    protected void flushPage() throws IOException, SolrServerException {
        if (!documents.isEmpty()) {
            solrClient.add(documents);
            documents.clear();
        }
    }

    @Override
    public synchronized void run() {
        try {
            super.run();
        } finally {
            // Cancellation and initialization failures can bypass postRun.
            if (reader != null) {
                try {
                    reader.close();
                } catch (SQLException e) {
                    LogManager.getLogger(getClass()).warn("Closing embedding database", e);
                }
                LogManager.getLogger(getClass()).info("Documents prepared with vector: {}, without vector: {}",
                        withVector, withoutVector);
            }
        }
    }
}
