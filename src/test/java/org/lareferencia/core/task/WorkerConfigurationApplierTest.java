package org.lareferencia.core.task;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

class WorkerConfigurationApplierTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void appliesOnlyPropertiesDeclaredForTheWorker() throws Exception {
        NetworkAction action = new NetworkAction();
        WorkerConfigurationProperty property = new WorkerConfigurationProperty();
        property.setName("enabled"); property.setType("boolean");
        WorkerConfigurationDescriptor worker = new WorkerConfigurationDescriptor();
        worker.setKey("sample"); worker.setBeanName("sampleWorker"); worker.getProperties().add(property);
        action.getWorkerConfigurations().add(worker);

        SampleWorker target = new SampleWorker();
        ApplicationWorkerConfigurationService configurations = new ApplicationWorkerConfigurationService(null, mapper, null, null) {
            @Override public com.fasterxml.jackson.databind.JsonNode configuration(String engineType, String key) {
                try { return mapper.readTree("{\"enabled\":true,\"ignored\":true}"); }
                catch (Exception exception) { throw new IllegalStateException(exception); }
            }
        };
        new WorkerConfigurationApplier(configurations).apply(target, "legacy", action, "sampleWorker");

        assertTrue(target.enabled);
        assertFalse(target.other);
    }

    public static class SampleWorker {
        private boolean enabled;
        private boolean other;
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public void setOther(boolean other) { this.other = other; }
    }
}
