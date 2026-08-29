package org.lareferencia.core.task;

import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;

/** Applies only allow-listed configuration to a newly-created prototype worker. */
@Service
public class WorkerConfigurationApplier {
    private final ApplicationWorkerConfigurationService configurations;

    public WorkerConfigurationApplier(ApplicationWorkerConfigurationService configurations) { this.configurations = configurations; }

    public void apply(Object worker, String engineType, NetworkAction action, String beanName) {
        WorkerConfigurationDescriptor descriptor = action.getWorkerConfigurations().stream()
                .filter(item -> beanName.equals(item.getBeanName())).findFirst().orElse(null);
        if (descriptor == null) return;
        var values = configurations.configuration(engineType, descriptor.getKey());
        if (!values.isObject()) return;

        BeanWrapper wrapper = new BeanWrapperImpl(worker);
        for (WorkerConfigurationProperty property : descriptor.getProperties()) {
            JsonNode value = values.get(property.getName());
            if (value == null || value.isNull()) continue;
            if (!wrapper.isWritableProperty(property.getName())) {
                throw new ApplicationActionPolicyException("WORKER_CONFIGURATION_INVALID",
                        "Worker property is not writable: " + descriptor.getKey() + "." + property.getName());
            }
            wrapper.setPropertyValue(property.getName(), primitive(value, property.getType()));
        }
    }

    private Object primitive(JsonNode value, String type) {
        return switch (type) {
            case "boolean" -> value.asBoolean();
            case "integer" -> value.asInt();
            case "number" -> value.asDouble();
            default -> value.asText();
        };
    }
}
