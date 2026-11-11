package org.lareferencia.core.worker.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.worker.NetworkRunningContext;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class FullUnindexerWorkerTest {
    @Test
    void verifiesDeletionAfterCommitting() throws Exception {
        Probe worker = new Probe(12, 0);
        worker.run();
        assertEquals("Completed", worker.getStatus());
        assertEquals(List.of("count", "<delete><query>network_acronym_str:CU</query></delete>",
                "<commit/>", "count"), worker.operations);
    }

    @Test
    void reportsFailureWhenDocumentsRemainAfterCommit() throws Exception {
        Probe worker = new Probe(12, 12);
        worker.run();
        assertTrue(worker.getStatus().startsWith("Failed:"));
        assertTrue(worker.getStatus().contains("12 documents remain"));
    }

    @Test
    void commitFailureDoesNotBecomeSuccessfulCompletion() throws Exception {
        Probe worker = new Probe(12, 0);
        worker.failCommit = true;
        worker.run();
        assertTrue(worker.getStatus().startsWith("Failed:"));
        assertTrue(worker.getStatus().contains("commit refused"));
        assertEquals(1, worker.operations.stream().filter("count"::equals).count());
    }

    private static class Probe extends FullUnindexerWorker {
        final List<String> operations = new ArrayList<>();
        boolean failCommit;

        Probe(long before, long after) throws Exception {
            super("http://localhost:8983/solr/biblio");
            solrClient.close();
            solrClient = mock(HttpSolrClient.class);
            when(solrClient.query(any(SolrQuery.class))).thenAnswer(invocation -> {
                SolrQuery query = invocation.getArgument(0);
                assertEquals("network_acronym_str:CU", query.getQuery());
                assertEquals(0, query.getRows());
                long count = operations.isEmpty() ? before : after;
                operations.add("count");
                SolrDocumentList documents = new SolrDocumentList();
                documents.setNumFound(count);
                QueryResponse response = mock(QueryResponse.class);
                when(response.getResults()).thenReturn(documents);
                return response;
            });
            Network network = new Network();
            network.setAcronym("CU");
            this.runningContext = new NetworkRunningContext(network);
            setSolrNetworkIDField("network_acronym_str");
        }

        @Override
        protected void sendUpdateToSolr(String xml) throws IOException {
            operations.add(xml);
            if (failCommit && xml.equals("<commit/>")) throw new IOException("commit refused");
        }
    }
}
