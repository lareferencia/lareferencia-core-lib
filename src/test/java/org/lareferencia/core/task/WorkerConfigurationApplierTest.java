package org.lareferencia.core.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.ApplicationWorkerConfiguration;
import org.lareferencia.core.worker.indexing.IndexerWorker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

class WorkerConfigurationApplierTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void appliesPublishedPropertiesBeyondTheXmlDescriptor() {
        NetworkAction action = new NetworkAction();
        WorkerConfigurationDescriptor descriptor = new WorkerConfigurationDescriptor();
        descriptor.setKey("sample"); descriptor.setBeanName("sampleWorker");
        WorkerConfigurationProperty declared = new WorkerConfigurationProperty();
        declared.setName("solrUrl");
        descriptor.getProperties().add(declared);
        action.getWorkerConfigurations().add(descriptor);

        ObjectNode values = mapper.createObjectNode();
        values.put("solrUrl", "http://solr-new/core");
        values.put("targetSchemaName", "new-crosswalk");
        ApplicationWorkerConfiguration row = row("sample", "sampleWorker", values);
        properties(row).putObject("solrUrl").put("type", "string");
        properties(row).putObject("targetSchemaName").put("type", "string");

        SampleWorker target = new SampleWorker();
        applier("sample", row).apply(target, "legacy", action, "sampleWorker");

        assertEquals("http://solr-new/core", target.solrUrl);
        assertEquals("new-crosswalk", target.targetSchemaName);
    }

    @Test
    void appliesSolrUrlAndSchemaToTheRealIndexer() {
        ObjectNode values = mapper.createObjectNode();
        values.put("solrUrl", "http://solr-new/core");
        values.put("targetSchemaName", "new-crosswalk");
        ApplicationWorkerConfiguration row = row("frontendIndexer", "frontendIndexerWorker", values);
        properties(row).putObject("solrUrl").put("type", "string");
        properties(row).putObject("targetSchemaName").put("type", "string");

        NetworkAction action = new NetworkAction();
        WorkerConfigurationDescriptor descriptor = new WorkerConfigurationDescriptor();
        descriptor.setKey("frontendIndexer"); descriptor.setBeanName("frontendIndexerWorker");
        action.getWorkerConfigurations().add(descriptor);
        IndexerWorker indexer = new IndexerWorker("http://solr-original/core");

        applier("frontendIndexer", row).apply(indexer, "legacy", action, "frontendIndexerWorker");

        assertEquals("http://solr-new/core", indexer.getSolrUrl());
        assertEquals("new-crosswalk", indexer.getTargetSchemaName());
    }

    @Test
    void appliesPublishedPropertiesWithoutAnXmlDescriptor() {
        ObjectNode values = mapper.createObjectNode();
        values.put("enabled", true);
        ApplicationWorkerConfiguration row = row("sampleWorker", "sampleWorker", values);
        properties(row).putObject("enabled").put("type", "boolean");

        SampleWorker target = new SampleWorker();
        applier("sampleWorker", row).apply(target, "legacy", new NetworkAction(), "sampleWorker");

        assertTrue(target.enabled);
    }

    @Test
    void rejectsValuesOutsideThePublishedSchema() {
        ObjectNode values = mapper.createObjectNode();
        values.put("other", true);
        ApplicationWorkerConfiguration row = row("sampleWorker", "sampleWorker", values);
        properties(row).putObject("enabled").put("type", "boolean");

        SampleWorker target = new SampleWorker();
        assertThrows(ApplicationActionPolicyException.class,
                () -> applier("sampleWorker", row).apply(target, "legacy", new NetworkAction(), "sampleWorker"));
        assertFalse(target.other);
    }

    @Test
    void rejectsConfigurationForAnotherBean() {
        ApplicationWorkerConfiguration row = row("sampleWorker", "otherWorker", mapper.createObjectNode());

        assertThrows(ApplicationActionPolicyException.class,
                () -> applier("sampleWorker", row).apply(new SampleWorker(), "legacy", new NetworkAction(), "sampleWorker"));
    }

    private WorkerConfigurationApplier applier(String key, ApplicationWorkerConfiguration row) {
        ApplicationWorkerConfigurationService configurations = new ApplicationWorkerConfigurationService(null, mapper, null, null) {
            @Override public ApplicationWorkerConfiguration require(String engineType, String requestedKey) {
                assertEquals("legacy", engineType);
                assertEquals(key, requestedKey);
                return row;
            }
        };
        return new WorkerConfigurationApplier(configurations);
    }

    private ApplicationWorkerConfiguration row(String key, String beanName, ObjectNode values) {
        ApplicationWorkerConfiguration row = new ApplicationWorkerConfiguration();
        row.setWorkerKey(key);
        row.setAvailable(true);
        ObjectNode definition = mapper.createObjectNode();
        definition.put("beanName", beanName);
        definition.putObject("schema").putObject("properties");
        row.setDefinition(definition);
        row.setConfiguration(values);
        return row;
    }

    private ObjectNode properties(ApplicationWorkerConfiguration row) {
        return (ObjectNode) row.getDefinition().path("schema").path("properties");
    }

    public static class SampleWorker {
        private String solrUrl;
        private String targetSchemaName;
        private boolean enabled;
        private boolean other;
        public void setSolrUrl(String solrUrl) { this.solrUrl = solrUrl; }
        public void setTargetSchemaName(String targetSchemaName) { this.targetSchemaName = targetSchemaName; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public void setOther(boolean other) { this.other = other; }
    }
}
