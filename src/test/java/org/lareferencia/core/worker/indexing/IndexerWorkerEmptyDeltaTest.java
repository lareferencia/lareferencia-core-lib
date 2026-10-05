package org.lareferencia.core.worker.indexing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.request.DirectXmlRequest;
import org.apache.solr.common.SolrDocumentList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.IndexingResult;
import org.lareferencia.core.domain.SnapshotIndexStatus;
import org.lareferencia.core.metadata.*;
import org.lareferencia.core.repository.validation.ValidationDatabaseManager;
import org.lareferencia.core.service.management.SnapshotIndexingService;
import org.lareferencia.core.service.management.SnapshotLogService;
import org.lareferencia.core.service.validation.ValidationManifestStore;
import org.lareferencia.core.service.validation.ValidatorFingerprint;
import org.lareferencia.core.worker.NetworkRunningContext;
import org.mockito.ArgumentCaptor;
import org.springframework.test.util.ReflectionTestUtils;

class IndexerWorkerEmptyDeltaTest {
    @TempDir Path directory;

    @Test
    void emptyIncrementalDeltaCommitsSuccessfullyWithoutDeletingInheritedDocuments() throws Exception {
        Network network = new Network();
        network.setAcronym("UY");
        network.setMetadataStoreSchema("xoai");
        SnapshotMetadata metadata = new SnapshotMetadata();
        metadata.setSnapshotId(4152L);
        metadata.setNetwork(network);
        ValidationDatabaseManager databases = new ValidationDatabaseManager();
        ReflectionTestUtils.setField(databases, "basePath", directory.toString());
        try {
            databases.initializeSnapshot(metadata, List.of());
            try (var connection = databases.getDataSource(4152L).getConnection();
                    var statement = connection.createStatement()) {
                statement.executeUpdate("INSERT INTO record_validation "
                        + "(identifier_hash, identifier, is_valid, is_transformed, deleted, change_type) "
                        + "VALUES ('hash', 'oai:uy:1', 1, 1, 0, NULL)");
            }
            ValidationManifestStore manifests = new ValidationManifestStore();
            ReflectionTestUtils.setField(manifests, "basePath", directory.toString());
            ValidatorFingerprint fingerprint = new ValidatorFingerprint(2, "SHA-256", "validation-pipeline-v2", "hash");
            fingerprint.setScope("INCREMENTAL");
            manifests.write(metadata, fingerprint);

            IndexerWorker worker = new IndexerWorker("http://localhost:8983/solr/test");
            worker.setBeanName("frontendIndexerWorker");
            worker.setTargetSchemaName("vufind5");
            worker.setSolrRecordIDValue("fingerprint");
            worker.setSolrNetworkIDField("network_acronym");
            worker.setExecuteIndexing(true);
            worker.setIncremental(true);
            worker.setRunningContext(new NetworkRunningContext(network, "FRONTEND_INDEXING_ACTION", null));
            ISnapshotStore snapshots = mock(ISnapshotStore.class);
            when(snapshots.findLastGoodKnownSnapshot(network)).thenReturn(4152L);
            when(snapshots.getSnapshotMetadata(4152L)).thenReturn(metadata);
            MDFormatTransformerService transformations = mock(MDFormatTransformerService.class);
            when(transformations.getMDTransformer("xoai", "vufind5")).thenReturn(mock(IMDFormatTransformer.class));
            SnapshotIndexingService results = mock(SnapshotIndexingService.class);
            SnapshotLogService logs = mock(SnapshotLogService.class);
            HttpSolrClient solr = mock(HttpSolrClient.class);
            QueryResponse response = mock(QueryResponse.class);
            SolrDocumentList documents = new SolrDocumentList();
            documents.setNumFound(1);
            when(response.getResults()).thenReturn(documents);
            when(solr.query(any(SolrQuery.class))).thenReturn(response);
            ReflectionTestUtils.setField(worker, "snapshotStore", snapshots);
            ReflectionTestUtils.setField(worker, "dbManager", databases);
            ReflectionTestUtils.setField(worker, "validationManifestStore", manifests);
            ReflectionTestUtils.setField(worker, "trfService", transformations);
            ReflectionTestUtils.setField(worker, "indexingResults", results);
            ReflectionTestUtils.setField(worker, "snapshotLogService", logs);
            ReflectionTestUtils.setField(worker, "solrClient", solr);

            worker.run();

            assertNull(worker.getExecutionFailure());
            assertFalse(worker.isCancellationRequested());
            ArgumentCaptor<SolrRequest> request = ArgumentCaptor.forClass(SolrRequest.class);
            verify(solr, times(1)).request(request.capture());
            DirectXmlRequest update = assertInstanceOf(DirectXmlRequest.class, request.getValue());
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            update.getContentWriter("text/xml").write(body);
            assertEquals("<commit/>", body.toString(StandardCharsets.UTF_8));
            verify(results).record(eq(4152L), eq(0L), eq("frontendIndexerWorker"),
                    argThat((IndexingResult result) -> result.status() == SnapshotIndexStatus.INDEXED && result.error() == null));
            verify(logs).addEntry(eq(4152L), contains("Incremental indexing:"));
        } finally {
            databases.cleanup();
        }
    }

    @Test
    void aFlushedPageIsNotSubmittedAgainDuringFinalization() throws Exception {
        IndexerWorker worker = new IndexerWorker("http://localhost:8983/solr/test");
        HttpSolrClient solr = mock(HttpSolrClient.class);
        ReflectionTestUtils.setField(worker, "solrClient", solr);
        worker.prePage();
        ((StringBuffer) ReflectionTestUtils.getField(worker, "stringBuffer")).append("<doc/>");
        worker.postPage();
        worker.postPage();
        verify(solr, times(1)).request(any(SolrRequest.class));
    }
}
