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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.lareferencia.core.worker.NetworkRunningContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("IndexerWorker Tests")
class IndexerWorkerTest {

    private IndexerWorker worker;
    private static final String TEST_SOLR_URL = "http://localhost:8983/solr/test-core";

    @BeforeEach
    void setUp() {
        worker = new IndexerWorker(TEST_SOLR_URL);
    }

    @Test
    @DisplayName("Should create IndexerWorker with Solr URL")
    void testConstructorWithSolrURL() {
        assertNotNull(worker);
        assertEquals("IndexerWorker", worker.getName());
    }

    @Test
    @DisplayName("Should set and get target schema name")
    void testTargetSchemaName() {
        worker.setTargetSchemaName("oai_dc");
        assertEquals("oai_dc", worker.getTargetSchemaName());
    }

    @Test
    @DisplayName("Should set and get Solr network ID field")
    void testSolrNetworkIDField() {
        worker.setSolrNetworkIDField("network_id");
        assertEquals("network_id", worker.getSolrNetworkIDField());
    }

    @Test
    @DisplayName("Should set and get Solr record ID field")
    void testSolrRecordIDField() {
        assertEquals("id", worker.getSolrRecordIDField()); // default
        
        worker.setSolrRecordIDField("custom_id");
        assertEquals("custom_id", worker.getSolrRecordIDField());
    }

    @Test
    @DisplayName("Should set and get execute deletion flag")
    void testExecuteDeletion() {
        assertFalse(worker.isExecuteDeletion()); // default
        
        worker.setExecuteDeletion(true);
        assertTrue(worker.isExecuteDeletion());
    }

    @Test
    @DisplayName("Should set and get execute indexing flag")
    void testExecuteIndexing() {
        assertFalse(worker.isExecuteIndexing()); // default
        
        worker.setExecuteIndexing(true);
        assertTrue(worker.isExecuteIndexing());
    }

    @Test
    @DisplayName("Should set and get index deleted records flag")
    void testIndexDeletedRecords() {
        assertFalse(worker.isIndexDeletedRecords()); // default
        
        worker.setIndexDeletedRecords(true);
        assertTrue(worker.isIndexDeletedRecords());
    }

    @Test
    @DisplayName("Should set and get index network attributes flag")
    void testIndexNetworkAttributes() {
        assertFalse(worker.isIndexNetworkAttributes()); // default
        
        worker.setIndexNetworkAttributes(true);
        assertTrue(worker.isIndexNetworkAttributes());
    }

    @Test
    @DisplayName("Should set and get content filters by field name")
    void testContentFiltersByFieldName() {
        assertNull(worker.getContentFiltersByFieldName()); // default
        
        Map<String, List<String>> filters = new HashMap<>();
        filters.put("dc.type", Arrays.asList("article", "thesis"));
        filters.put("dc.language", Arrays.asList("en", "es"));
        
        worker.setContentFiltersByFieldName(filters);
        assertEquals(filters, worker.getContentFiltersByFieldName());
        assertEquals(2, worker.getContentFiltersByFieldName().size());
    }

    @Test
    @DisplayName("Should handle empty filters map")
    void testEmptyFiltersMap() {
        Map<String, List<String>> emptyFilters = new HashMap<>();
        worker.setContentFiltersByFieldName(emptyFilters);
        
        assertNotNull(worker.getContentFiltersByFieldName());
        assertEquals(0, worker.getContentFiltersByFieldName().size());
    }

    @Test
    @DisplayName("Should handle null target schema name")
    void testNullTargetSchemaName() {
        worker.setTargetSchemaName(null);
        assertNull(worker.getTargetSchemaName());
    }

    @Test
    @DisplayName("Should set running context")
    void testSetRunningContext() {
        NetworkRunningContext context = createMockContext();
        worker.setRunningContext(context);
        assertEquals(context, worker.getRunningContext());
    }

    @Test
    @DisplayName("Should set incremental mode")
    void testIncrementalMode() {
        assertFalse(worker.isIncremental());
        
        worker.setIncremental(true);
        assertTrue(worker.isIncremental());
    }

    // DISABLED: IndexerWorker now extends BaseWorker instead of BaseBatchWorker
    // @Test
    // @DisplayName("Should extend BaseBatchWorker")
    // void testInheritance() {
    //     assertTrue(worker instanceof org.lareferencia.core.worker.BaseBatchWorker);
    // }

    // DISABLED: PageSize methods no longer exist in IndexerWorker
    // @Test
    // @DisplayName("Should set page size")
    // void testPageSize() {
    //     worker.setPageSize(200);
    //     assertEquals(200, worker.getPageSize());
    // }

    @Test
    @DisplayName("Should handle multiple schema types")
    void testMultipleSchemaTypes() {
        String[] schemas = {"oai_dc", "oai_openaire", "rdf", "datacite"};
        
        for (String schema : schemas) {
            worker.setTargetSchemaName(schema);
            assertEquals(schema, worker.getTargetSchemaName());
        }
    }

    @Test
    @DisplayName("Should configure all indexing flags together")
    void testAllIndexingFlags() {
        worker.setExecuteDeletion(true);
        worker.setExecuteIndexing(true);
        worker.setIndexDeletedRecords(true);
        worker.setIndexNetworkAttributes(true);
        
        assertTrue(worker.isExecuteDeletion());
        assertTrue(worker.isExecuteIndexing());
        assertTrue(worker.isIndexDeletedRecords());
        assertTrue(worker.isIndexNetworkAttributes());
    }

    // Helper method to create mock context without heavy dependencies
    private NetworkRunningContext createMockContext() {
        org.lareferencia.backend.domain.Network network = new org.lareferencia.backend.domain.Network();
        network.setAcronym("TEST");
        network.setName("Test Network");
        return new NetworkRunningContext(network);
    }
}
