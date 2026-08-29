package org.lareferencia.core.task;

import java.util.List;

import org.lareferencia.core.domain.ApplicationAction;
import org.lareferencia.core.domain.Network;
import org.lareferencia.core.domain.NetworkActionConfiguration;
import org.lareferencia.core.repository.jpa.NetworkActionConfigurationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Resolves installation defaults and per-network action policy without mutating legacy properties. */
@Service
public class NetworkActionConfigurationService {
    private final NetworkActionConfigurationRepository repository;
    private final ApplicationActionCatalogService catalog;
    private final ObjectMapper mapper;

    public NetworkActionConfigurationService(NetworkActionConfigurationRepository repository,
            ApplicationActionCatalogService catalog, ObjectMapper mapper) {
        this.repository = repository; this.catalog = catalog; this.mapper = mapper;
    }

    @Transactional
    public void reconcile(Network network, String engineType, List<NetworkAction> actions) {
        for (NetworkAction action : actions) {
            ApplicationAction application = catalog.require(engineType, action.getName());
            NetworkActionConfiguration row = repository.findByNetworkIdAndApplicationActionId(network.getId(), application.getId())
                    .orElseGet(NetworkActionConfiguration::new);
            boolean isNew = row.getId() == null;
            row.setNetwork(network); row.setApplicationAction(application);
            if (isNew) {
                row.setEnabled(true); // manual execution was available for every configured legacy action
                row.setScheduleEnabled(legacyScheduleEnabled(network, action));
                row.setConfiguration(mapper.createObjectNode());
            }
            ObjectNode configuration = row.getConfiguration() != null && row.getConfiguration().isObject()
                    ? (ObjectNode) row.getConfiguration().deepCopy() : mapper.createObjectNode();
            action.getConfigurationProperties().forEach(property -> {
                if (!configuration.has(property.getName())) {
                    configuration.put(property.getName(), network.getBooleanPropertyValue(property.getName()));
                }
            });
            row.setConfiguration(configuration); row.setUpdatedBy("system:legacy-migration");
            repository.save(row);
        }
    }

    @Transactional(readOnly = true)
    public boolean canExecute(Network network, String engineType, String actionKey) {
        ApplicationAction action = catalog.require(engineType, actionKey);
        return repository.findByNetworkIdAndApplicationActionId(network.getId(), action.getId())
                .map(NetworkActionConfiguration::isEnabled).orElse(true);
    }

    @Transactional(readOnly = true)
    public boolean canSchedule(Network network, String engineType, NetworkAction descriptor) {
        ApplicationAction action = catalog.require(engineType, descriptor.getName());
        return repository.findByNetworkIdAndApplicationActionId(network.getId(), action.getId())
                .map(row -> row.isEnabled() && row.isScheduleEnabled())
                .orElseGet(() -> legacyScheduleEnabled(network, descriptor));
    }

    @Transactional(readOnly = true)
    public List<NetworkActionConfiguration> list(Network network) { return repository.findAllByNetworkId(network.getId()); }

    @Transactional(readOnly = true)
    public NetworkActionConfiguration require(Network network, String engineType, String actionKey) {
        ApplicationAction application = catalog.require(engineType, actionKey);
        return repository.findByNetworkIdAndApplicationActionId(network.getId(), application.getId())
                .orElseThrow(() -> new ApplicationActionPolicyException("NETWORK_ACTION_NOT_FOUND", "Action is not configured for this network"));
    }

    @Transactional
    public NetworkActionConfiguration replace(Network network, String engineType, String actionKey, boolean enabled,
            boolean scheduleEnabled, JsonNode configuration, String updatedBy) {
        ApplicationAction application = catalog.require(engineType, actionKey);
        NetworkActionConfiguration row = repository.findByNetworkIdAndApplicationActionId(network.getId(), application.getId())
                .orElseGet(NetworkActionConfiguration::new);
        ObjectNode effective = mapper.createObjectNode();
        merge(effective, application.getConfiguration());
        merge(effective, configuration);
        catalog.validateConfiguration(application.getDefinition().path("schema"), effective);
        row.setNetwork(network); row.setApplicationAction(application); row.setEnabled(enabled);
        row.setScheduleEnabled(scheduleEnabled); row.setConfiguration(configuration.deepCopy()); row.setUpdatedBy(updatedBy);
        return repository.save(row);
    }

    @Transactional(readOnly = true)
    public JsonNode effectiveConfiguration(NetworkActionConfiguration row) {
        ObjectNode result = mapper.createObjectNode();
        merge(result, row.getApplicationAction().getConfiguration());
        merge(result, row.getConfiguration());
        return result;
    }

    @Transactional(readOnly = true)
    public JsonNode effectiveConfiguration(Network network, String engineType, String actionKey) {
        ApplicationAction application = catalog.require(engineType, actionKey);
        return repository.findByNetworkIdAndApplicationActionId(network.getId(), application.getId())
                .map(this::effectiveConfiguration)
                .orElseGet(() -> application.getConfiguration() == null
                        ? mapper.createObjectNode() : application.getConfiguration().deepCopy());
    }

    private boolean legacyScheduleEnabled(Network network, NetworkAction action) {
        if (!Boolean.TRUE.equals(action.getRunOnSchedule())) return false;
        if (Boolean.TRUE.equals(action.getAllwaysRunOnSchedule())) return true;
        return action.getProperties().stream().anyMatch(property -> network.getBooleanPropertyValue(property.getName()));
    }

    private void merge(ObjectNode target, JsonNode source) {
        if (source == null || !source.isObject()) return;
        source.fields().forEachRemaining(entry -> {
            if (entry.getValue().isObject() && target.path(entry.getKey()).isObject()) {
                merge((ObjectNode) target.path(entry.getKey()), entry.getValue());
            } else target.set(entry.getKey(), entry.getValue());
        });
    }
}
