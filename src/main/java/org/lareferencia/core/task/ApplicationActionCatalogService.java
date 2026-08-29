package org.lareferencia.core.task;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.lareferencia.core.domain.ApplicationAction;
import org.lareferencia.core.repository.jpa.ApplicationActionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Service
public class ApplicationActionCatalogService {
    public static final int MAX_CONFIGURATION_BYTES = 64 * 1024;

    private final ApplicationActionRepository repository;
    private final ObjectMapper objectMapper;

    public ApplicationActionCatalogService(ApplicationActionRepository repository, ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public synchronized ReconciliationResult reconcile(String engineType, List<NetworkAction> discovered, String updatedBy) {
        Set<String> keys = new HashSet<>();
        for (NetworkAction action : discovered) {
            if (action.getName() == null || !keys.add(action.getName())) {
                throw new IllegalStateException("Duplicate action key for engine " + engineType + ": " + action.getName());
            }
        }

        boolean bootstrap = repository.countByEngineType(engineType) == 0;
        List<ApplicationAction> existing = repository.findAllByEngineTypeOrderByExecutionOrderAscActionKeyAsc(engineType);
        // Rows created before execution_order existed are marked -1 by the
        // migration. Use the discovery sequence exactly once to preserve the
        // configured legacy order rather than inventing an alphabetical one.
        boolean initializeExistingOrder = !bootstrap && !existing.isEmpty()
                && existing.stream().allMatch(row -> row.getExecutionOrder() < 0);
        int nextOrder = existing.stream().mapToInt(ApplicationAction::getExecutionOrder).max().orElse(-1) + 1;
        if (initializeExistingOrder) nextOrder = 0;
        existing.forEach(row -> row.setAvailable(false));
        repository.saveAll(existing);

        int created = 0;
        int updated = 0;
        OffsetDateTime now = OffsetDateTime.now();
        for (int position = 0; position < discovered.size(); position++) {
            NetworkAction descriptor = discovered.get(position);
            ApplicationAction row = repository.findByEngineTypeAndActionKey(engineType, descriptor.getName()).orElse(null);
            if (row == null) {
                row = new ApplicationAction();
                row.setEngineType(engineType);
                row.setActionKey(descriptor.getName());
                row.setEnabled(bootstrap);
                // The initial sequence preserves the executor's configured
                // sequence. Afterwards it is installation configuration.
                row.setExecutionOrder(bootstrap ? position : nextOrder++);
                row.setConfiguration(defaultConfiguration(descriptor));
                validateConfiguration(descriptor.getConfigurationSchema(), row.getConfiguration());
                created++;
            } else {
                if (initializeExistingOrder) row.setExecutionOrder(nextOrder++);
                updated++;
            }
            row.setAvailable(true);
            row.setDefinition(definition(descriptor));
            row.setLastSeenAt(now);
            row.setUpdatedBy(updatedBy);
            repository.save(row);
        }
        if (initializeExistingOrder) {
            for (ApplicationAction row : existing.stream()
                    .filter(row -> row.getExecutionOrder() < 0)
                    .sorted(Comparator.comparing(ApplicationAction::getActionKey)).toList()) {
                row.setExecutionOrder(nextOrder++);
                row.setUpdatedBy(updatedBy);
                repository.save(row);
            }
        }
        int unavailable = (int) repository.findAllByEngineTypeOrderByExecutionOrderAscActionKeyAsc(engineType).stream()
                .filter(row -> !row.isAvailable()).count();
        return new ReconciliationResult(engineType, bootstrap, created, updated, unavailable, List.of());
    }

    @Transactional(readOnly = true)
    public List<ApplicationAction> list(String engineType) {
        return repository.findAllByEngineTypeOrderByExecutionOrderAscActionKeyAsc(engineType);
    }

    /** One-time-compatible cleanup after worker values have been copied to their global rows. */
    @Transactional
    public void removeWorkerConfiguration(String engineType, List<NetworkAction> actions) {
        for (NetworkAction action : actions) {
            ApplicationAction row = require(engineType, action.getName());
            if (row.getConfiguration() != null && row.getConfiguration().isObject()
                    && row.getConfiguration().has("workers")) {
                ObjectNode clean = (ObjectNode) row.getConfiguration().deepCopy();
                clean.remove("workers"); row.setConfiguration(clean); repository.save(row);
            }
        }
    }

    @Transactional(readOnly = true)
    public ApplicationAction require(String engineType, String actionKey) {
        return repository.findByEngineTypeAndActionKey(engineType, actionKey)
                .orElseThrow(() -> new ApplicationActionPolicyException("ACTION_NOT_FOUND",
                        "Action '" + actionKey + "' was not found for engine " + engineType));
    }

    @Transactional(readOnly = true)
    public boolean isEffectivelyEnabled(String engineType, String actionKey) {
        return repository.findByEngineTypeAndActionKey(engineType, actionKey)
                .map(row -> state(row) == ApplicationActionState.ENABLED).orElse(false);
    }

    @Transactional(readOnly = true)
    public void requireEnabled(String engineType, String actionKey) {
        ApplicationAction row = require(engineType, actionKey);
        ApplicationActionState state = state(row);
        if (state != ApplicationActionState.ENABLED) {
            String code = state == ApplicationActionState.UNAVAILABLE ? "ACTION_UNAVAILABLE"
                    : state == ApplicationActionState.INVALID_CONFIGURATION ? "ACTION_CONFIGURATION_INVALID"
                            : "ACTION_DISABLED";
            throw new ApplicationActionPolicyException(code,
                    "Action '" + actionKey + "' cannot execute because its state is " + state);
        }
    }

    @Transactional
    public ApplicationAction replace(String engineType, String actionKey, boolean enabled, JsonNode configuration,
            String updatedBy) {
        ApplicationAction row = require(engineType, actionKey);
        if (enabled && !row.isAvailable()) {
            throw new ApplicationActionPolicyException("ACTION_UNAVAILABLE", "Unavailable actions cannot be enabled");
        }
        validateConfiguration(row.getDefinition().path("schema"), configuration);
        row.setEnabled(enabled);
        row.setConfiguration(configuration.deepCopy());
        row.setUpdatedBy(updatedBy);
        return repository.save(row);
    }

    /** Moves an available or unavailable catalogue item one position within its engine. */
    @Transactional
    public ApplicationAction move(String engineType, String actionKey, MoveDirection direction, String updatedBy) {
        List<ApplicationAction> rows = new ArrayList<>(list(engineType));
        int index = -1;
        for (int i = 0; i < rows.size(); i++) {
            if (rows.get(i).getActionKey().equals(actionKey)) { index = i; break; }
        }
        if (index < 0) throw new ApplicationActionPolicyException("ACTION_NOT_FOUND", "Action '" + actionKey + "' was not found for engine " + engineType);
        int target = direction == MoveDirection.UP ? index - 1 : index + 1;
        if (target >= 0 && target < rows.size()) {
            ApplicationAction moved = rows.remove(index);
            rows.add(target, moved);
        }
        for (int i = 0; i < rows.size(); i++) {
            ApplicationAction row = rows.get(i);
            row.setExecutionOrder(i);
            row.setUpdatedBy(updatedBy);
        }
        repository.saveAll(rows);
        return rows.stream().filter(row -> row.getActionKey().equals(actionKey)).findFirst().orElseThrow();
    }

    /** Applies the persisted action order to live descriptors. Unknown items stay last. */
    @Transactional(readOnly = true)
    public List<NetworkAction> order(String engineType, List<NetworkAction> descriptors) {
        Map<String, Integer> positions = new HashMap<>();
        list(engineType).forEach(row -> positions.put(row.getActionKey(), row.getExecutionOrder()));
        if (positions.isEmpty()) return descriptors;
        return descriptors.stream().sorted(Comparator
                .comparing((NetworkAction action) -> positions.get(action.getName()), Comparator.nullsLast(Comparator.naturalOrder()))
                .thenComparing(NetworkAction::getName, Comparator.nullsLast(Comparator.naturalOrder()))).toList();
    }

    public ApplicationActionState state(ApplicationAction row) {
        if (!row.isAvailable()) return ApplicationActionState.UNAVAILABLE;
        try {
            validateConfiguration(row.getDefinition().path("schema"), row.getConfiguration());
        } catch (ApplicationActionPolicyException exception) {
            return ApplicationActionState.INVALID_CONFIGURATION;
        }
        return row.isEnabled() ? ApplicationActionState.ENABLED : ApplicationActionState.DISABLED;
    }

    public void validateConfiguration(JsonNode schema, JsonNode configuration) {
        if (configuration == null || !configuration.isObject()) {
            invalid("configuration must be a JSON object");
        }
        if (configuration.toString().getBytes(StandardCharsets.UTF_8).length > MAX_CONFIGURATION_BYTES) {
            invalid("configuration exceeds 64 KiB");
        }
        if (schema == null || schema.isMissingNode() || schema.isNull() || schema.isEmpty()) {
            if (!configuration.isEmpty()) invalid("only an empty object is valid when no schema is available");
            return;
        }
        validateNode(schema, configuration, "configuration");
    }

    private void validateNode(JsonNode schema, JsonNode value, String path) {
        String type = schema.path("type").asText("");
        boolean validType = switch (type) {
            case "object" -> value.isObject(); case "array" -> value.isArray(); case "string" -> value.isTextual();
            case "integer" -> value.isIntegralNumber(); case "number" -> value.isNumber();
            case "boolean" -> value.isBoolean(); case "null" -> value.isNull(); default -> true;
        };
        if (!validType) invalid(path + " must be of type " + type);
        if (schema.has("enum")) {
            boolean matches = false;
            for (JsonNode allowed : schema.path("enum")) matches |= allowed.equals(value);
            if (!matches) invalid(path + " is not one of the allowed values");
        }
        if (value.isObject()) {
            Set<String> required = new HashSet<>();
            schema.path("required").forEach(node -> required.add(node.asText()));
            required.forEach(name -> { if (!value.has(name)) invalid(path + "." + name + " is required"); });
            JsonNode properties = schema.path("properties");
            value.fields().forEachRemaining(entry -> {
                JsonNode propertySchema = properties.path(entry.getKey());
                if (propertySchema.isMissingNode() && schema.path("additionalProperties").isBoolean()
                        && !schema.path("additionalProperties").asBoolean()) {
                    invalid(path + "." + entry.getKey() + " is not allowed");
                }
                if (!propertySchema.isMissingNode()) validateNode(propertySchema, entry.getValue(), path + "." + entry.getKey());
            });
        }
        if (value.isArray() && schema.has("items")) {
            for (int i = 0; i < value.size(); i++) validateNode(schema.path("items"), value.get(i), path + "[" + i + "]");
        }
    }

    private void invalid(String message) {
        throw new ApplicationActionPolicyException("ACTION_CONFIGURATION_INVALID", message);
    }

    private JsonNode defaultConfiguration(NetworkAction action) {
        JsonNode defaults = action.getDefaultConfiguration();
        return defaults != null && defaults.isObject() ? defaults.deepCopy() : objectMapper.createObjectNode();
    }

    private ObjectNode definition(NetworkAction action) {
        ObjectNode node = objectMapper.createObjectNode();
        node.put("name", action.getDescription());
        node.put("description", action.getDescription());
        node.put("incremental", action.isIncremental());
        node.put("schedulable", Boolean.TRUE.equals(action.getRunOnSchedule()));
        if (action.getVersion() != null) node.put("version", action.getVersion());
        if (action.getProcessKey() != null) node.put("processKey", action.getProcessKey());
        ArrayNode workers = node.putArray("workers");
        action.getWorkers().forEach(workers::add);
        ArrayNode properties = node.putArray("properties");
        action.getProperties().forEach(property -> properties.addObject().put("name", property.getName())
                .put("description", property.getDescription()));
        node.set("schema", action.getConfigurationSchema() == null ? inferredSchema(action) : action.getConfigurationSchema());
        node.set("uiSchema", action.getUiSchema() == null ? objectMapper.createObjectNode() : action.getUiSchema());
        return node;
    }

    private JsonNode inferredSchema(NetworkAction action) {
        if (action.getConfigurationProperties().isEmpty()) return objectMapper.createObjectNode();
        ObjectNode schema = objectMapper.createObjectNode();
        schema.put("type", "object");
        schema.put("additionalProperties", false);
        ObjectNode properties = schema.putObject("properties");
        action.getConfigurationProperties().forEach(property -> properties.putObject(property.getName())
                .put("type", "boolean")
                .put("title", property.getDescription())
                .put("default", false));
        return schema;
    }

    public record ReconciliationResult(String engineType, boolean bootstrap, int created, int updated,
            int unavailable, List<String> conflicts) { }

    public enum MoveDirection { UP, DOWN }
}
