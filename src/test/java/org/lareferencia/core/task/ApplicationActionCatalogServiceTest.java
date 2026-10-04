package org.lareferencia.core.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

        var result = service.reconcile("flowable", List.of(action), "test");

        assertTrue(rows.get(0).isEnabled());
        assertTrue(rows.get(0).isAvailable());
        assertTrue(result.bootstrap());
        assertEquals(1, result.created());
        assertEquals(0, rows.get(0).getExecutionOrder());
    }

    @Test
    void reconcile_EmptyLegacyCatalogue_UsesMigrationDefaultsBeforeUnknownActions() {
        service.reconcile("legacy", List.of(action("ZZ_CUSTOM"), action("VALIDATION_ACTION"),
                action("DARK_RECONCILE_ACTION"), action("AA_CUSTOM"), action("HARVESTING_ACTION"),
                action("DARK_STAGE_ACTION"), action("SEMANTIC_INDEXING_ACTION")), "system:startup");

        assertEquals(List.of("HARVESTING_ACTION", "DARK_STAGE_ACTION", "DARK_RECONCILE_ACTION",
                "VALIDATION_ACTION", "SEMANTIC_INDEXING_ACTION", "AA_CUSTOM", "ZZ_CUSTOM"),
                service.list("legacy").stream().map(ApplicationAction::getActionKey).toList());
        assertTrue(service.require("legacy", "HARVESTING_ACTION").isEnabled());
        assertTrue(service.require("legacy", "VALIDATION_ACTION").isEnabled());
        assertFalse(service.require("legacy", "DARK_STAGE_ACTION").isEnabled());
        assertFalse(service.require("legacy", "DARK_RECONCILE_ACTION").isEnabled());
        assertFalse(service.require("legacy", "SEMANTIC_INDEXING_ACTION").isEnabled());
        assertFalse(service.require("legacy", "AA_CUSTOM").isEnabled());
    }

    @Test
    void reconcile_RestartAndNewActions_PreservesCustomPolicyAndAppendsDisabled() {
        service.reconcile("legacy", List.of(action("HARVESTING_ACTION"), action("VALIDATION_ACTION")), "startup");
        service.move("legacy", "VALIDATION_ACTION", ApplicationActionCatalogService.MoveDirection.UP, "admin");
        var validation = service.require("legacy", "VALIDATION_ACTION");
        validation.setEnabled(false);

        service.reconcile("legacy", List.of(action("ZZ_CUSTOM"), action("XOAI_INDEXING_ACTION"),
                action("HARVESTING_ACTION"), action("AA_CUSTOM"), action("VALIDATION_ACTION")), "restart");

        assertEquals(List.of("VALIDATION_ACTION", "HARVESTING_ACTION", "AA_CUSTOM", "XOAI_INDEXING_ACTION", "ZZ_CUSTOM"),
                service.list("legacy").stream().map(ApplicationAction::getActionKey).toList());
        assertFalse(validation.isEnabled());
        assertFalse(service.require("legacy", "XOAI_INDEXING_ACTION").isEnabled());
        assertFalse(service.require("legacy", "AA_CUSTOM").isEnabled());
        assertFalse(service.require("legacy", "ZZ_CUSTOM").isEnabled());
    }

    @Test
    void reconcile_DisappearingAndReturningAction_KeepsItsPositionAndConfiguration() {
        service.reconcile("legacy", List.of(action("HARVESTING_ACTION"), action("VALIDATION_ACTION")), "startup");
        var harvesting = service.require("legacy", "HARVESTING_ACTION");
        var configuration = mapper.createObjectNode().put("custom", true);
        harvesting.setConfiguration(configuration);
        service.reconcile("legacy", List.of(action("VALIDATION_ACTION")), "restart");
        assertFalse(harvesting.isAvailable());
        service.reconcile("legacy", List.of(action("VALIDATION_ACTION"), action("HARVESTING_ACTION")), "restart");
        assertTrue(harvesting.isAvailable());
        assertTrue(harvesting.isEnabled());
        assertEquals(0, harvesting.getExecutionOrder());
        assertEquals(configuration, harvesting.getConfiguration());
        assertEquals(2, rows.size());
    }

    @Test
    void reconcile_UninitializedOrder_AppliesDefaultsOnceAndPreservesEnabledState() {
        service.reconcile("legacy", List.of(action("HARVESTING_ACTION"), action("VALIDATION_ACTION")), "startup");
        rows.forEach(row -> { row.setExecutionOrder(-1); row.setEnabled(false); });
        service.reconcile("legacy", List.of(action("VALIDATION_ACTION"), action("HARVESTING_ACTION")), "restart");
        assertEquals(0, service.require("legacy", "HARVESTING_ACTION").getExecutionOrder());
        assertEquals(1, service.require("legacy", "VALIDATION_ACTION").getExecutionOrder());
        assertTrue(rows.stream().noneMatch(ApplicationAction::isEnabled));
    }

    private NetworkAction action(String name) {
        NetworkAction action = new NetworkAction();
        action.setName(name);
        return action;
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
