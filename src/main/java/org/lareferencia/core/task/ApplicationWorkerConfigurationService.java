package org.lareferencia.core.task;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.lareferencia.core.domain.ApplicationWorkerConfiguration;
import org.lareferencia.core.repository.jpa.ApplicationWorkerConfigurationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Owns worker parameters at installation scope; network rows never participate. */
@Service
public class ApplicationWorkerConfigurationService {
    private final ApplicationWorkerConfigurationRepository repository;
    private final ObjectMapper mapper;
    private final ApplicationActionCatalogService validator;
    private final WorkerConfigurationIntrospector introspector;

    public ApplicationWorkerConfigurationService(ApplicationWorkerConfigurationRepository repository, ObjectMapper mapper,
            ApplicationActionCatalogService validator, WorkerConfigurationIntrospector introspector) { this.repository = repository; this.mapper = mapper; this.validator = validator; this.introspector = introspector; }

    @Transactional
    public void reconcile(String engineType, List<NetworkAction> actions, String updatedBy) {
        Map<String, WorkerConfigurationDescriptor> descriptors = new LinkedHashMap<>();
        Map<String, JsonNode> legacyValues = new LinkedHashMap<>();
        actions.forEach(action -> {
            Map<String, WorkerConfigurationDescriptor> declared = new LinkedHashMap<>();
            action.getWorkerConfigurations().forEach(item -> declared.put(item.getBeanName(), item));
            action.getWorkers().forEach(beanName -> {
                WorkerConfigurationDescriptor item = declared.get(beanName);
                String key = item == null || item.getKey() == null || item.getKey().isBlank() ? beanName : item.getKey();
                WorkerConfigurationDescriptor discovered = introspector.describe(key, beanName, item);
                WorkerConfigurationDescriptor previous = descriptors.putIfAbsent(key, discovered);
                if (previous != null && !previous.getBeanName().equals(discovered.getBeanName())) {
                    throw new IllegalStateException("Duplicate worker configuration key '" + key + "'");
            }
                JsonNode value = validator.require(engineType, action.getName()).getConfiguration().path("workers").path(key);
                if (value.isObject()) legacyValues.putIfAbsent(key, value);
            });
        });
        List<ApplicationWorkerConfiguration> existing = repository.findAllByEngineTypeOrderByWorkerKeyAsc(engineType);
        existing.forEach(row -> row.setAvailable(false)); repository.saveAll(existing);
        OffsetDateTime now = OffsetDateTime.now();
        descriptors.forEach((key, descriptor) -> {
            ApplicationWorkerConfiguration row = repository.findByEngineTypeAndWorkerKey(engineType, key)
                    .orElseGet(ApplicationWorkerConfiguration::new);
            if (row.getId() == null) {
                row.setEngineType(engineType); row.setWorkerKey(key);
                JsonNode migrated = legacyValues.get(key);
                row.setConfiguration(migrated != null ? migrated.deepCopy() : defaults(descriptor));
            }
            row.setAvailable(true); row.setDefinition(definition(descriptor)); row.setLastSeenAt(now); row.setUpdatedBy(updatedBy);
            validator.validateConfiguration(row.getDefinition().path("schema"), row.getConfiguration());
            repository.save(row);
        });
    }

    @Transactional(readOnly = true) public List<ApplicationWorkerConfiguration> list(String engineType) { return repository.findAllByEngineTypeOrderByWorkerKeyAsc(engineType); }
    @Transactional(readOnly = true) public ApplicationWorkerConfiguration require(String engineType, String key) {
        return repository.findByEngineTypeAndWorkerKey(engineType, key).orElseThrow(() -> new ApplicationActionPolicyException("WORKER_CONFIGURATION_NOT_FOUND", "Worker configuration '" + key + "' was not found"));
    }
    @Transactional(readOnly = true) public JsonNode configuration(String engineType, String key) { return require(engineType, key).getConfiguration().deepCopy(); }
    @Transactional public ApplicationWorkerConfiguration replace(String engineType, String key, JsonNode configuration, String updatedBy) {
        ApplicationWorkerConfiguration row = require(engineType, key);
        if (!row.isAvailable()) throw new ApplicationActionPolicyException("WORKER_CONFIGURATION_UNAVAILABLE", "Unavailable worker configuration cannot be updated");
        validator.validateConfiguration(row.getDefinition().path("schema"), configuration);
        row.setConfiguration(configuration.deepCopy()); row.setUpdatedBy(updatedBy); return repository.save(row);
    }

    private JsonNode defaults(WorkerConfigurationDescriptor descriptor) {
        ObjectNode result = mapper.createObjectNode();
        descriptor.getProperties().forEach(property -> {
            if (property.getDefaultValue() == null) return;
            switch (property.getType()) {
                case "boolean" -> result.put(property.getName(), Boolean.parseBoolean(property.getDefaultValue()));
                case "integer" -> result.put(property.getName(), Long.parseLong(property.getDefaultValue()));
                case "number" -> result.put(property.getName(), Double.parseDouble(property.getDefaultValue()));
                default -> result.put(property.getName(), property.getDefaultValue());
            }
        });
        return result;
    }
    private ObjectNode definition(WorkerConfigurationDescriptor descriptor) {
        ObjectNode result = mapper.createObjectNode(); result.put("key", descriptor.getKey()); result.put("beanName", descriptor.getBeanName());
        ObjectNode schema = result.putObject("schema"); schema.put("type", "object"); schema.put("additionalProperties", false);
        ObjectNode properties = schema.putObject("properties");
        descriptor.getProperties().forEach(property -> {
            ObjectNode p = properties.putObject(property.getName()); p.put("type", property.getType()); p.put("title", property.getDescription());
            if (property.getDefaultValue() == null) return;
            switch (property.getType()) {
                case "boolean" -> p.put("default", Boolean.parseBoolean(property.getDefaultValue()));
                case "integer" -> p.put("default", Long.parseLong(property.getDefaultValue()));
                case "number" -> p.put("default", Double.parseDouble(property.getDefaultValue()));
                default -> p.put("default", property.getDefaultValue());
            }
        });
        return result;
    }
}
