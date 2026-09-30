package org.lareferencia.core.task;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;

/** Applies the same worker properties that the management API exposes for editing. */
@Service
public class WorkerConfigurationApplier {
    private final ApplicationWorkerConfigurationService configurations;

    public WorkerConfigurationApplier(ApplicationWorkerConfigurationService configurations) { this.configurations = configurations; }

    public void apply(Object worker, String engineType, NetworkAction action, String beanName) {
        WorkerConfigurationDescriptor descriptor = action.getWorkerConfigurations().stream()
                .filter(item -> beanName.equals(item.getBeanName())).findFirst().orElse(null);
        String key = descriptor == null || descriptor.getKey() == null || descriptor.getKey().isBlank()
                ? beanName : descriptor.getKey();
        var row = configurations.require(engineType, key);
        if (!row.isAvailable() || !beanName.equals(row.getDefinition().path("beanName").asText())) {
            throw new ApplicationActionPolicyException("WORKER_CONFIGURATION_INVALID",
                    "Worker configuration does not match the active bean: " + key);
        }
        JsonNode properties = row.getDefinition().path("schema").path("properties");
        JsonNode values = row.getConfiguration();
        if (!properties.isObject() || !values.isObject()) {
            throw new ApplicationActionPolicyException("WORKER_CONFIGURATION_INVALID",
                    "Worker configuration schema or values are invalid: " + key);
        }

        BeanWrapper wrapper = new BeanWrapperImpl(worker);
        var fields = values.fields();
        while (fields.hasNext()) {
            var field = fields.next();
            String name = field.getKey();
            JsonNode value = field.getValue();
            if (value == null || value.isNull()) continue;
            JsonNode property = properties.path(name);
            String type = property.path("type").asText();
            if (property.isMissingNode() || !wrapper.isWritableProperty(name)
                    || !(type.equals("boolean") || type.equals("integer") || type.equals("number") || type.equals("string"))) {
                throw new ApplicationActionPolicyException("WORKER_CONFIGURATION_INVALID",
                        "Worker property is not configurable: " + key + "." + name);
            }
            wrapper.setPropertyValue(name, primitive(value, type));
        }
    }

    private Object primitive(JsonNode value, String type) {
        return switch (type) {
            case "boolean" -> value.asBoolean();
            case "integer" -> value.asLong();
            case "number" -> value.asDouble();
            default -> value.asText();
        };
    }
}
