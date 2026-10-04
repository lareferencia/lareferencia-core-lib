package org.lareferencia.core.worker.indexing;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.metadata.IMetadataStore;
import org.lareferencia.core.metadata.SnapshotMetadata;
import org.lareferencia.core.repository.validation.ValidationRecord;
import org.lareferencia.core.service.management.SnapshotLogService;
import org.springframework.test.util.ReflectionTestUtils;
import org.lareferencia.core.worker.NetworkRunningContext;

class IndexingWorkerErrorTest {
    @Test void failureToReadDiagnosticCountsDoesNotInvalidateACompletedCommit() throws Exception {
        for (BaseIndexerWorker worker : List.of(new IndexerWorker("http://localhost:8983/solr/test"), new SemanticIndexerWorker())) {
            HttpSolrClient solr = mock(HttpSolrClient.class);
            when(solr.query(any(SolrQuery.class))).thenThrow(new SolrServerException("Count unavailable"));
            ReflectionTestUtils.setField(worker, "solrClient", solr);
            ReflectionTestUtils.setField(worker, "snapshotLogService", mock(SnapshotLogService.class));
            worker.setRunningContext(new NetworkRunningContext(new Network()));
            if (worker instanceof IndexerWorker indexer) { indexer.prePage(); indexer.postRun(); }
            else { ((SemanticIndexerWorker) worker).postRun(); }
            assertNull(worker.getExecutionFailure());
            assertFalse(worker.isCancellationRequested());
            assertEquals(true, ReflectionTestUtils.getField(worker, "committed"));
        }
    }

    @Test void bothIndexersReportMetadataFailuresThatTheyHandleInsideProcessItem() throws Exception {
        for (BaseIndexerWorker worker : List.of(new IndexerWorker("http://localhost:8983/solr/test"), new SemanticIndexerWorker())) {
            IMetadataStore store = mock(IMetadataStore.class);
            ReflectionTestUtils.setField(worker, "metadataStore", store);
            ReflectionTestUtils.setField(worker, "snapshotMetadata", mock(SnapshotMetadata.class));
            ReflectionTestUtils.setField(worker, "snapshotLogService", mock(SnapshotLogService.class));
            when(store.getMetadata(any(), any())).thenThrow(new IllegalStateException("Metadata unavailable"));
            ValidationRecord record = mock(ValidationRecord.class);
            when(record.getIdentifier()).thenReturn("oai:test:1");
            when(record.getPublishedMetadataHash()).thenReturn("metadata-hash");
            worker.processItem(record);
            assertNotNull(worker.getExecutionFailure());
            assertTrue(worker.getExecutionFailure().contains("Metadata unavailable"));
        }
    }
}
