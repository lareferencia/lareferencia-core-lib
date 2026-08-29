package org.lareferencia.core.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.lareferencia.core.domain.ApplicationAction;
import org.lareferencia.core.repository.jpa.ApplicationActionRepository;

import com.fasterxml.jackson.databind.ObjectMapper;

class ApplicationActionCatalogServiceTest {
    private final List<ApplicationAction> rows = new ArrayList<>();
    private final ApplicationActionRepository repository = inMemoryRepository();
    private final ObjectMapper mapper = new ObjectMapper();
    private final ApplicationActionCatalogService service = new ApplicationActionCatalogService(repository, mapper);

    @Test
    void reconcile_FirstEngineBootstrap_EnablesDiscoveredActions() {
        NetworkAction action = new NetworkAction();
        action.setName("harvesting");
        action.setDescription("Harvesting");

        var result = service.reconcile("legacy", List.of(action), "test");

        assertTrue(rows.get(0).isEnabled());
        assertTrue(rows.get(0).isAvailable());
        assertTrue(result.bootstrap());
        assertEquals(1, result.created());
        assertEquals(0, rows.get(0).getExecutionOrder());
    }

    @Test
    void move_ReordersTheOnlyExecutionSequence() {
        NetworkAction first = new NetworkAction(); first.setName("first");
        NetworkAction second = new NetworkAction(); second.setName("second");
        service.reconcile("legacy", List.of(first, second), "test");

        service.move("legacy", "second", ApplicationActionCatalogService.MoveDirection.UP, "admin");

        assertEquals(List.of("second", "first"), service.list("legacy").stream()
                .map(ApplicationAction::getActionKey).toList());
        assertEquals(0, service.require("legacy", "second").getExecutionOrder());
        assertEquals(1, service.require("legacy", "first").getExecutionOrder());
    }

    @Test
    void reconcile_DuplicateKey_FailsExplicitly() {
        NetworkAction first = new NetworkAction(); first.setName("same");
        NetworkAction second = new NetworkAction(); second.setName("same");
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> service.reconcile("flowable", List.of(first, second), "test"));
        assertTrue(error.getMessage().contains("Duplicate action key"));
    }

    @Test
    void validateConfiguration_WithoutSchema_OnlyAcceptsEmptyObject() throws Exception {
        service.validateConfiguration(mapper.createObjectNode(), mapper.createObjectNode());
        var error = assertThrows(ApplicationActionPolicyException.class,
                () -> service.validateConfiguration(mapper.createObjectNode(), mapper.readTree("{\"value\":1}")));
        assertEquals("ACTION_CONFIGURATION_INVALID", error.getCode());
    }

    @Test
    void reconcile_LegacyMixedProperties_PublishesOnlyExecutionModifiers() {
        NetworkProperty scheduleSwitch = new NetworkProperty();
        scheduleSwitch.setName("INDEX_FRONTEND");
        NetworkProperty modifier = new NetworkProperty();
        modifier.setName("INDEX_FULLTEXT");
        modifier.setDescription("Index full text?");
        NetworkAction action = new NetworkAction();
        action.setName("FRONTEND_INDEXING_ACTION");
        action.setProperties(List.of(scheduleSwitch, modifier));

        service.reconcile("legacy", List.of(action), "test");

        var schemaProperties = rows.get(0).getDefinition().path("schema").path("properties");
        assertTrue(schemaProperties.has("INDEX_FULLTEXT"));
        assertTrue(!schemaProperties.has("INDEX_FRONTEND"));
    }

    @SuppressWarnings("unchecked")
    private ApplicationActionRepository inMemoryRepository() {
        return (ApplicationActionRepository) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] { ApplicationActionRepository.class }, (proxy, method, args) -> switch (method.getName()) {
                    case "countByEngineType" -> rows.stream().filter(row -> args[0].equals(row.getEngineType())).count();
                    case "findAllByEngineTypeOrderByExecutionOrderAscActionKeyAsc" -> rows.stream()
                            .filter(row -> args[0].equals(row.getEngineType()))
                            .sorted(java.util.Comparator.comparingInt(ApplicationAction::getExecutionOrder)
                                    .thenComparing(ApplicationAction::getActionKey)).toList();
                    case "findByEngineTypeAndActionKey" -> rows.stream()
                            .filter(row -> args[0].equals(row.getEngineType()) && args[1].equals(row.getActionKey())).findFirst();
                    case "save" -> { ApplicationAction row = (ApplicationAction) args[0]; if (!rows.contains(row)) rows.add(row); yield row; }
                    case "saveAll" -> args[0];
                    case "toString" -> "InMemoryApplicationActionRepository";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
